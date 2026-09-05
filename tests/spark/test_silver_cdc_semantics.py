"""What the silver MERGE actually *does* to a table, executed on real Spark + Iceberg.

tests/test_silver_cdc.py pins the shape of the generated SQL; this file runs it. The three
defects it covers all produced valid-looking SQL and a green suite, so shape alone was never
going to catch them:

* a delete and a re-create of one key in a single batch resolved as a delete, because deletes
  ran as an unconditional second pass after the upsert merge;
* a replayed or out-of-order batch overwrote newer silver state, because the merge had no
  sequence guard;
* a Debezium tombstone -- the null-valued record that follows every delete -- was read as a
  malformed record and written to the dead-letter table.

Excluded from the default suite by the `spark` marker and run by its own CI job, which is
the only one that pays for a JVM and a ~300 MB pyspark install. Selected explicitly:
`pytest -m spark tests/spark`. The heavier nine-container chain stays under
`integration_cdc`.

These tests are mutation-checked: removing the LSN guard fails the two stale-state cases,
and removing the DELETE branch fails the delete case. A green run here means the guard is
doing work, not that the assertions are vacuous.
"""
from __future__ import annotations

import json
import pathlib
import shutil
import tempfile

import pytest

pytestmark = pytest.mark.spark

pyspark = pytest.importorskip("pyspark", reason="needs a Spark-capable runtime")

from pyspark.sql import SparkSession  # noqa: E402

REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
MICROS = 1_000_000


def _iceberg_runtime_coordinate() -> str:
    """Read the Iceberg runtime version the deployed jobs actually use.

    Hardcoding a version here would let these tests pass against a different Iceberg than
    production runs -- and the behaviour under test is that engine's MERGE semantics, which
    is exactly the thing a version bump could change. config/spark/Dockerfile's
    SPARK_PACKAGES is already the single source the runtimes are held to
    (tests/test_spark_jobs.py compares it against every job definition), so read it.
    """
    dockerfile = (REPO_ROOT / "config" / "spark" / "Dockerfile").read_text(encoding="utf-8")
    declared = next(
        line.split("=", 1)[1].strip().strip('"')
        for line in dockerfile.splitlines()
        if line.startswith("ARG SPARK_PACKAGES=")
    )
    coordinate = next(
        c for c in declared.split(",") if c.startswith("org.apache.iceberg:iceberg-spark-runtime")
    )
    return coordinate


ICEBERG_PACKAGE = _iceberg_runtime_coordinate()


def envelope(op, payment_id, lsn, *, status="authorized", amount="100.00", updated=1_760_000_000):
    """A Debezium envelope as bronze stores it (post PII masking)."""
    after = None if op == "d" else {
        "payment_id": payment_id, "merchant_id": 7, "amount": amount, "currency": "USD",
        "payment_method": "card", "payment_status": status, "country_code": "US",
        "created_at": 1_750_000_000 * MICROS, "updated_at": updated * MICROS,
    }
    # A Postgres delete carries only the key in `before` under default REPLICA IDENTITY.
    before = {"payment_id": payment_id} if op == "d" else None
    return json.dumps({"op": op, "before": before, "after": after, "source": {"lsn": lsn}})


@pytest.fixture(scope="module")
def spark():
    warehouse = tempfile.mkdtemp(prefix="iceberg-test-")
    session = (
        SparkSession.builder.appName("silver-cdc-semantics")
        .master("local[2]")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", warehouse)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
    shutil.rmtree(warehouse, ignore_errors=True)


@pytest.fixture
def silver(spark):
    """Fresh silver + DLQ per test, matching the DDL the job creates."""
    import payment_rules
    import silver_payments as job

    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.analytics")
    for table in (job.SILVER_TABLE, job.DLQ_TABLE):
        spark.sql(f"DROP TABLE IF EXISTS {table}")
    spark.sql(f"""
        CREATE TABLE {job.SILVER_TABLE} (
            payment_id BIGINT, merchant_id BIGINT, amount DECIMAL(12,2), currency STRING,
            payment_method STRING, payment_status STRING, country_code STRING,
            created_at TIMESTAMP, updated_at TIMESTAMP, source_lsn BIGINT,
            ingested_at TIMESTAMP
        ) USING iceberg
    """)
    spark.sql(f"""
        CREATE TABLE {job.DLQ_TABLE} (
            kafka_partition INT, kafka_offset BIGINT, kafka_value STRING, batch_id BIGINT,
            error_reason STRING, ingested_at TIMESTAMP
        ) USING iceberg
    """)
    assert list(payment_rules.SILVER_COLUMNS)  # the DDL above mirrors it
    return job


def batch(spark, envelopes):
    """Build a bronze micro-batch: (offset, value) pairs in Kafka order."""
    rows = [
        (str(i), value, "cdc.public.payments", 0, i, None)
        for i, value in enumerate(envelopes, start=1)
    ]
    df = spark.createDataFrame(
        rows,
        "kafka_key string, kafka_value string, kafka_topic string, "
        "kafka_partition int, kafka_offset bigint, kafka_timestamp timestamp",
    )
    return df


def silver_rows(spark, job):
    return {
        r["payment_id"]: r
        for r in spark.sql(
            f"SELECT payment_id, payment_status, source_lsn FROM {job.SILVER_TABLE}"
        ).collect()
    }


def dlq_reasons(spark, job):
    return [r["error_reason"] for r in spark.sql(f"SELECT error_reason FROM {job.DLQ_TABLE}").collect()]


# --------------------------------------------------------------------------------------

def test_delete_then_recreate_in_one_batch_leaves_the_row_present(spark, silver):
    """The old two-pass structure applied deletes last, so this finished deleted."""
    job = silver
    job._upsert_to_silver(batch(spark, [
        envelope("c", 1, lsn=100),
        envelope("d", 1, lsn=101),
        envelope("c", 1, lsn=102, status="pending"),
    ]), 1)

    rows = silver_rows(spark, job)
    assert 1 in rows, "delete-then-recreate resolved as a delete"
    assert rows[1]["payment_status"] == "pending"
    assert rows[1]["source_lsn"] == 102


def test_recreate_then_delete_in_one_batch_leaves_the_row_deleted(spark, silver):
    """The mirror case: highest LSN wins, and here that is the delete."""
    job = silver
    job._upsert_to_silver(batch(spark, [
        envelope("c", 2, lsn=200),
        envelope("d", 2, lsn=201),
    ]), 1)

    assert 2 not in silver_rows(spark, job)


def test_a_stale_batch_does_not_overwrite_newer_state(spark, silver):
    """A replay must converge, not regress -- this is what makes bronze replay safe."""
    job = silver
    job._upsert_to_silver(batch(spark, [envelope("c", 3, lsn=300, status="authorized")]), 1)
    # Replayed older event arriving in a later batch.
    job._upsert_to_silver(batch(spark, [envelope("u", 3, lsn=299, status="pending")]), 2)

    rows = silver_rows(spark, job)
    assert rows[3]["payment_status"] == "authorized", "older event overwrote newer state"
    assert rows[3]["source_lsn"] == 300


def test_a_stale_delete_does_not_remove_a_newer_row(spark, silver):
    """The guard sits on the delete branch too, not just the update branch."""
    job = silver
    job._upsert_to_silver(batch(spark, [envelope("c", 4, lsn=400)]), 1)
    job._upsert_to_silver(batch(spark, [envelope("d", 4, lsn=399)]), 2)

    assert 4 in silver_rows(spark, job), "a replayed delete removed a live row"


def test_a_newer_delete_removes_the_row(spark, silver):
    """The guard must not block legitimate deletes."""
    job = silver
    job._upsert_to_silver(batch(spark, [envelope("c", 5, lsn=500)]), 1)
    job._upsert_to_silver(batch(spark, [envelope("d", 5, lsn=501)]), 2)

    assert 5 not in silver_rows(spark, job)


def test_a_tombstone_is_not_a_dead_letter(spark, silver):
    """Debezium emits a null-valued record after every delete; it is not malformed."""
    job = silver
    job._upsert_to_silver(batch(spark, [
        envelope("c", 6, lsn=600),
        None,  # the tombstone
    ]), 1)

    assert "null_op" not in dlq_reasons(spark, job), "a valid tombstone landed in the DLQ"
    assert 6 in silver_rows(spark, job)


def test_a_genuinely_malformed_record_still_reaches_the_dlq(spark, silver):
    """Dropping tombstones must not also drop real corruption."""
    job = silver
    job._upsert_to_silver(batch(spark, [
        envelope("c", 7, lsn=700),
        '{"no_op_field": true}',
    ]), 1)

    assert "null_op" in dlq_reasons(spark, job)
    assert 7 in silver_rows(spark, job)


def test_an_unexpected_op_reaches_the_dlq(spark, silver):
    job = silver
    job._upsert_to_silver(batch(spark, [json.dumps({"op": "x", "after": None, "source": {"lsn": 800}})]), 1)

    assert "unexpected_op" in dlq_reasons(spark, job)
