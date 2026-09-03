"""End-to-end proof that the CDC chain actually works.

Postgres -> Debezium -> Kafka -> Spark -> Iceberg, asserted on real data rather than
inferred from configuration. Everything else in tests/ that touches this pipeline is a
static guard: tests/test_compose_manifest.py and tests/test_validate_k8s_manifests.py
parse manifests, and the Spark job tests run against fake session objects. Nothing
before this exercised config/connect/postgres-cdc.json at all.

Deliberately opt-in -- marked `integration_cdc` and excluded from the fast suite, since
it needs nine containers and roughly ten minutes.

    pytest -m integration_cdc tests/integration/test_cdc_chain.py

Two properties of the pipeline make this deterministic rather than a timeout race:

* Bronze and Silver both submit with `.trigger(availableNow=True)`, so they drain the
  available data and terminate. There is no streaming query to poll for quiescence.
* Gold is a plain batch INSERT OVERWRITE off Silver.

The only genuinely asynchronous step is Debezium's initial snapshot, which is polled for
with a bounded timeout below.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from decimal import Decimal

import pytest

from scripts import trino_http

pytestmark = pytest.mark.integration_cdc

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# The CDC chain only. The three images this repo builds (api, airflow, trino-exporter)
# take no part in it, which is why Build and this test run in parallel in CI.
#
# Trino is included as the query interface: scripts/trino_http.py already speaks to it
# and is covered by tests, so assertions stay ordinary Python here instead of becoming a
# bespoke Spark job whose own correctness nothing checks.
SERVICES = [
    "postgres",
    "kafka",
    "kafka-connect",
    "minio",
    "catalog-db",
    "iceberg-rest-migrate",
    "iceberg-rest",
    "spark",
    "trino",
]

# A row that cannot collide with config/postgres/init/002_seed_data.sql, whose country
# codes are AU BE CA CH DE ES FR GB NL US and whose payment ids stop at 9001. 'ZZ'
# isolates exactly one gold row, so the assertions never depend on seed volume.
PAYMENT_ID = 999_001
MERCHANT_ID = 1
SHOPPER_ID = 424_242
AMOUNT = Decimal("4242.42")
COUNTRY = "ZZ"
METHOD = "card"
STATUS = "authorized"

CONNECTOR_TIMEOUT = 180
SNAPSHOT_TIMEOUT = 180


def compose(*args: str, check: bool = True, timeout: int = 900) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT, check=check, timeout=timeout,
        capture_output=True, text=True,
    )


def psql(sql: str) -> str:
    """Run SQL as the pipeline's own Postgres user."""
    user = os.environ.get("POSTGRES_USER", "dataeng")
    database = os.environ.get("POSTGRES_DB", "payments")
    result = compose(
        "exec", "-T", "postgres",
        "psql", "-U", user, "-d", database, "-tAc", sql,
    )
    return result.stdout.strip()


def trino_env() -> dict[str, str]:
    """Trino is reachable on the published port from the runner, not by service DNS."""
    return {**os.environ, "TRINO_HTTP_HOST": "localhost", "TRINO_HTTP_PORT": "8080"}


def query(sql: str) -> list[list[object]]:
    return trino_http.run_statement(sql, env=trino_env())


def wait_until(predicate, timeout: int, description: str, interval: float = 3.0):
    """Bounded poll. An unbounded wait here is how this test would become a flake."""
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            last = predicate()
            if last:
                return last
        except Exception as exc:  # noqa: BLE001 - services come up mid-poll
            last = exc
        time.sleep(interval)
    raise AssertionError(f"Timed out after {timeout}s waiting for {description}. Last: {last!r}")


def dump_logs() -> None:
    for service in SERVICES:
        result = compose("logs", "--tail", "60", service, check=False, timeout=60)
        print(f"\n===== {service} =====\n{result.stdout}{result.stderr}")


@pytest.fixture(scope="module")
def cdc_chain():
    """Bring the chain up, seed one known payment, and run the medallion once."""
    if not shutil.which("docker"):
        pytest.skip("docker is not installed")
    probe = subprocess.run(
        ["docker", "info"], capture_output=True, text=True, check=False, timeout=30
    )
    if probe.returncode != 0:
        pytest.skip("the Docker daemon is not running")

    try:
        compose("up", "-d", "--wait", *SERVICES)

        # The medallion cannot write until object storage and the catalog are bootstrapped.
        # These are the DAG's first two tasks (init_object_store, init_catalog) and skipping
        # them fails deep inside Spark with "A warehouse 'payments' does not exist" rather
        # than anywhere near the actual cause.
        #
        # The two need DIFFERENT S3_ENDPOINT values, which is easy to get wrong:
        #
        #   init_object_store   CONNECTS to S3 to create the buckets, so from the runner it
        #                       needs the published port -- compose maps MinIO to 9002, not 9000.
        #   init_iceberg_catalog RECORDS the endpoint in the warehouse's storage profile for
        #                       Lakekeeper and Spark to use later. Both are in-network, so it
        #                       must store the service name. Passing localhost here would
        #                       register a warehouse no in-cluster engine can reach.
        s3_credentials = {
            "S3_ACCESS_KEY": os.environ.get("MINIO_ROOT_USER", "payments"),
            "S3_SECRET_KEY": os.environ.get("MINIO_ROOT_PASSWORD", "changeme"),
        }

        subprocess.run(
            ["python", "scripts/init_object_store.py"],
            cwd=REPO_ROOT, check=True, timeout=180, capture_output=True, text=True,
            env={**os.environ, **s3_credentials, "S3_ENDPOINT": "http://localhost:9002"},
        )
        subprocess.run(
            ["python", "scripts/init_iceberg_catalog.py"],
            cwd=REPO_ROOT, check=True, timeout=180, capture_output=True, text=True,
            env={
                **os.environ, **s3_credentials,
                "ICEBERG_REST_URL": "http://localhost:8181",
                "S3_ENDPOINT": "http://minio:9000",
            },
        )

        # register_connector.sh posts to localhost:8083, which docker-compose.yml already
        # publishes. It :?-guards both credentials, so they must be exported.
        env = {
            **os.environ,
            "POSTGRES_USER": os.environ.get("POSTGRES_USER", "dataeng"),
            "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "changeme"),
        }
        subprocess.run(
            ["bash", "scripts/register_connector.sh"],
            cwd=REPO_ROOT, check=True, env=env, capture_output=True, text=True, timeout=120,
        )

        # validate_connector.py defaults to the in-network name; from the runner only the
        # published port resolves. This is the CONNECT_URL override it exists for.
        def connector_running() -> bool:
            result = subprocess.run(
                ["python", "scripts/validate_connector.py"],
                cwd=REPO_ROOT, env={**env, "CONNECT_URL": "http://localhost:8083"},
                capture_output=True, text=True, timeout=60,
            )
            return result.returncode == 0

        wait_until(connector_running, CONNECTOR_TIMEOUT, "the Debezium connector to report RUNNING")

        psql(
            "INSERT INTO payments (payment_id, merchant_id, shopper_id, amount, currency, "
            "payment_method, payment_status, country_code, created_at, updated_at) VALUES "
            f"({PAYMENT_ID}, {MERCHANT_ID}, {SHOPPER_ID}, {AMOUNT}, 'EUR', '{METHOD}', "
            f"'{STATUS}', '{COUNTRY}', NOW(), NOW()) ON CONFLICT (payment_id) DO NOTHING;"
        )

        # Debezium's snapshot plus streaming capture is the one asynchronous hop.
        wait_until(
            lambda: int(psql(f"SELECT count(*) FROM payments WHERE payment_id = {PAYMENT_ID};")) == 1,
            SNAPSHOT_TIMEOUT,
            "the seeded payment to be visible in Postgres",
        )

        # run_local_job.py is the repo's own submission path, and tests/test_spark_jobs.py
        # asserts its arguments still match airflow/dags/spark_jobs.py and
        # k8s/base/spark.yaml. Hand-rolling spark-submit here would bypass that guard.
        for layer in ("bronze", "silver", "gold"):
            subprocess.run(
                ["python", "scripts/run_local_job.py", layer],
                cwd=REPO_ROOT, check=True, timeout=1800,
            )

        wait_until(
            lambda: query("SHOW TABLES FROM iceberg.analytics"),
            120,
            "Trino to see the Iceberg tables",
        )
        yield
    except Exception:
        dump_logs()
        raise
    finally:
        if os.environ.get("CDC_KEEP_STACK") != "1":
            compose("down", "-v", check=False, timeout=300)


def gold_row() -> list[object]:
    rows = query(
        "SELECT payment_count, gross_volume, auth_rate, country_code, payment_method "
        "FROM iceberg.analytics.payment_metrics_gold "
        f"WHERE country_code = '{COUNTRY}'"
    )
    assert rows, f"no gold row for the seeded country_code {COUNTRY!r}"
    assert len(rows) == 1, f"expected exactly one isolated gold row, got {len(rows)}"
    return rows[0]


def test_seeded_payment_reaches_gold(cdc_chain):
    """The whole point: a Postgres INSERT becomes an Iceberg gold aggregate."""
    count, volume, auth_rate, country, method = gold_row()
    assert int(count) == 1
    assert Decimal(str(volume)) == AMOUNT
    assert float(auth_rate) == pytest.approx(1.0), "the seeded payment is 'authorized'"
    assert country == COUNTRY
    assert method == METHOD


def test_gold_dimensions_are_not_null(cdc_chain):
    """A DQ assertion, not a smoke check -- sql/trino/validation_queries.sql asserts nothing."""
    rows = query(
        "SELECT count(*) FROM iceberg.analytics.payment_metrics_gold "
        "WHERE country_code IS NULL OR payment_method IS NULL OR payment_hour IS NULL"
    )
    assert int(rows[0][0]) == 0, "gold rows exist with null grouping dimensions"


def test_pii_is_hashed_before_bronze(cdc_chain):
    """bronze_from_kafka.py hashes shopper_id so PII never lands in the lakehouse.

    This is the rule duplicated verbatim into databricks/src/dlt_pipeline.py with nothing
    asserting the two copies agree. Here at least one of them is enforced.
    """
    rows = query(
        "SELECT kafka_value FROM iceberg.analytics.payments_bronze "
        f"WHERE kafka_value LIKE '%\"payment_id\":{PAYMENT_ID}%' LIMIT 1"
    )
    assert rows, "the seeded payment never reached bronze"

    envelope = json.loads(rows[0][0])
    after = envelope.get("after") or {}
    hashed = after.get("shopper_id")

    assert hashed != SHOPPER_ID, "shopper_id reached bronze unhashed"
    assert hashed != str(SHOPPER_ID), "shopper_id reached bronze unhashed"
    assert isinstance(hashed, str) and len(hashed) == 64, (
        f"shopper_id is not a SHA-256 hex digest: {hashed!r}"
    )


def test_silver_canonicalised_the_row(cdc_chain):
    """Silver upper-cases country_code and lower-cases method/status."""
    rows = query(
        "SELECT country_code, payment_method, payment_status FROM "
        f"iceberg.analytics.payments_silver WHERE payment_id = {PAYMENT_ID}"
    )
    assert rows, "the seeded payment never reached silver"
    country, method, status = rows[0]
    assert country == COUNTRY.upper()
    assert method == METHOD.lower()
    assert status == STATUS.lower()
