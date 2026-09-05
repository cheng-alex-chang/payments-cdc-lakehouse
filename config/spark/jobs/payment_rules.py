"""Canonical payment business rules, shared by every runtime.

These four definitions are the contract the medallion enforces: which field is PII and
how it is masked, and which payment methods and statuses are considered valid. They lived
in three places -- bronze_from_kafka.py, silver_payments.py, and (copied verbatim)
databricks/src/dlt_pipeline.py -- with nothing asserting the copies agreed. Adding a
payment method meant remembering all three.

Deliberately imports nothing beyond the standard library: no pyspark, no dlt. That is what
lets the Spark jobs, the tests, and the drift guard all import it, and what keeps the rules
testable in milliseconds without a session.

Ships to Kubernetes through the spark-jobs ConfigMap alongside common.py; the file list in
k8s/base/kustomization.yaml is asserted against this directory by
tests/test_payment_rules.py, so a new module here cannot be left out of the pods.

The Databricks DLT pipeline still inlines its own copy on purpose -- it is loaded from a
Volume where sibling imports are unreliable on Free Edition, which is why it is written
self-contained. That copy is no longer free to drift: test_payment_rules.py parses it and
fails if it disagrees with the values below.
"""
from __future__ import annotations

import hashlib
import json

# Hashed before writing to Bronze so PII never lands in the lakehouse.
PII_FIELDS = frozenset({"shopper_id"})

ALLOWED_PAYMENT_METHODS = (
    "apple_pay",
    "bank_transfer",
    "card",
    "google_pay",
    "paypal",
)

ALLOWED_PAYMENT_STATUSES = (
    "authorized",
    "cancelled",
    "chargeback",
    "failed",
    "pending",
    "refunded",
)


def mask_pii_fields(value: str | None) -> str | None:
    """Hash PII fields in both `before` and `after` sections of a Debezium envelope.

    Returns the input unchanged when it is not JSON: a malformed envelope is the
    dead-letter path's problem, not this function's, and raising here would fail the whole
    micro-batch over one bad record.
    """
    if value is None:
        return None
    try:
        envelope = json.loads(value)
        for section in ("before", "after"):
            if isinstance(envelope.get(section), dict):
                for field in PII_FIELDS:
                    if field in envelope[section] and envelope[section][field] is not None:
                        raw = str(envelope[section][field]).encode()
                        envelope[section][field] = hashlib.sha256(raw).hexdigest()
        return json.dumps(envelope)
    except (json.JSONDecodeError, TypeError):
        return value


# --------------------------------------------------------------------------------------
# CDC change application
# --------------------------------------------------------------------------------------
#
# Silver applies a Debezium change feed to a current-state table. Two properties have to
# hold, and both were violated by the two-pass "merge the upserts, then delete everything
# marked d" structure this replaces:
#
# * A change must never be applied over a NEWER one already in silver. Within-batch dedup
#   is not enough -- a replayed or out-of-order batch regresses state.
# * A delete and a re-create of the same key must resolve by sequence, not by which pass
#   runs last. The old code applied deletes after upserts unconditionally, so d-then-c
#   always finished deleted.
#
# The sequence token is the Postgres LSN from `$.source.lsn`, not `updated_at`: the LSN is
# strictly monotonic across the whole WAL, whereas updated_at ties (two rows written in one
# transaction share it) and kafka_offset is only ordered within a partition.
#
# The Databricks DLT pipeline deliberately does NOT share this. It applies changes with
# apply_changes(sequence_by=updated_at) over a static seed of op='r' snapshot envelopes
# that carry no `source` section at all (see databricks/src/seed_to_volume.py), so there is
# no LSN to sequence by and no out-of-order case to resolve.

CDC_UPSERT_OPS = ("c", "r", "u")  # create / snapshot-read / update -- all carry `after`
CDC_DELETE_OP = "d"               # carries `before`; `after` is null
CDC_OPS = ("c", "d", "r", "u")    # anything else is a dead-letter record

# Silver's column order, shared by the DDL and the MERGE so the two cannot drift apart.
SILVER_COLUMNS = (
    "payment_id",
    "merchant_id",
    "amount",
    "currency",
    "payment_method",
    "payment_status",
    "country_code",
    "created_at",
    "updated_at",
    "source_lsn",
    "ingested_at",
)


def silver_merge_sql(target: str, source: str, key: str = "payment_id") -> str:
    """Build the single sequenced MERGE that applies one CDC batch to silver.

    Three branches, and all three are load-bearing:

    * MATCHED + newer + ``d``       -> DELETE
    * MATCHED + newer + upsert op   -> UPDATE
    * NOT MATCHED + upsert op       -> INSERT

    The ``source_lsn`` guard sits on *both* MATCHED branches: a stale delete and a stale
    update must each lose to the row already in silver. The NOT MATCHED branch carries no
    LSN comparison -- there is nothing to compare against -- and excluding ``d`` there makes
    a delete for an absent row the no-op it should be, rather than inserting a tombstone
    row full of nulls.

    Columns are listed explicitly rather than using ``UPDATE SET *`` / ``INSERT *``: the
    source view carries an ``op`` column that the target does not have, and the star forms
    require the two schemas to match exactly.
    """
    upsert_ops = ", ".join(f"'{op}'" for op in CDC_UPSERT_OPS)
    assignments = ",\n".join(
        f"        t.{column} = s.{column}" for column in SILVER_COLUMNS if column != key
    )
    insert_columns = ", ".join(SILVER_COLUMNS)
    insert_values = ", ".join(f"s.{column}" for column in SILVER_COLUMNS)
    newer = "s.source_lsn > t.source_lsn"

    return (
        f"MERGE INTO {target} t\n"
        f"USING {source} s ON t.{key} = s.{key}\n"
        f"WHEN MATCHED AND {newer} AND s.op = '{CDC_DELETE_OP}' THEN DELETE\n"
        f"WHEN MATCHED AND {newer} AND s.op IN ({upsert_ops}) THEN UPDATE SET\n"
        f"{assignments}\n"
        f"WHEN NOT MATCHED AND s.op IN ({upsert_ops}) THEN INSERT ({insert_columns})\n"
        f"    VALUES ({insert_values})"
    )


# Dead-letter rows are keyed by their Kafka coordinates rather than appended blindly. The
# append-only version re-inserted the same rejects on every replay of a batch, so the table
# could not answer "is this error still happening?" -- the count grew with retries, not with
# problems.
DLQ_COLUMNS = (
    "kafka_partition",
    "kafka_offset",
    "kafka_value",
    "batch_id",
    "error_reason",
    "ingested_at",
)

DLQ_KEY = ("kafka_partition", "kafka_offset")


def dlq_merge_sql(target: str, source: str) -> str:
    """Build the MERGE that records dead-letter rows idempotently.

    A record's (partition, offset) is unique within a topic and stable across replays, so
    re-processing a batch updates the existing row instead of adding a duplicate.
    """
    on_clause = " AND ".join(f"t.{column} = s.{column}" for column in DLQ_KEY)
    assignments = ",\n".join(
        f"        t.{column} = s.{column}" for column in DLQ_COLUMNS if column not in DLQ_KEY
    )
    insert_columns = ", ".join(DLQ_COLUMNS)
    insert_values = ", ".join(f"s.{column}" for column in DLQ_COLUMNS)
    return (
        f"MERGE INTO {target} t\n"
        f"USING {source} s ON {on_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET\n"
        f"{assignments}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_columns})\n"
        f"    VALUES ({insert_values})"
    )


def bronze_duplicate_offsets_sql(table: str) -> str:
    """Count Kafka coordinates appearing more than once in bronze.

    Bronze is append-only and reads from ``startingOffsets=earliest``, so the streaming
    checkpoint is the only thing preventing a full re-consume. Checkpoints live in their own
    bucket, separate from the warehouse, which is good for blast radius and means losing that
    bucket replays the entire topic into an append-only table. (topic, partition, offset)
    identifies a Kafka record exactly, so a duplicate is proof that happened.
    """
    return (
        f"SELECT kafka_topic, kafka_partition, kafka_offset, COUNT(*) AS occurrences\n"
        f"FROM {table}\n"
        f"GROUP BY kafka_topic, kafka_partition, kafka_offset\n"
        f"HAVING COUNT(*) > 1"
    )
