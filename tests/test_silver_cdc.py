"""The silver CDC application contract.

Silver applies a Debezium change feed to a current-state table, and three defects lived in
how it did that:

* the MERGE had no sequence guard, so an out-of-order batch overwrote newer state;
* deletes ran as an unconditional second pass, so delete-then-recreate always finished
  deleted, whatever the sequence said;
* a Debezium tombstone (a real record with a null value, emitted after every delete) was
  read as a malformed record and written to the dead-letter table.

The fix replaces the two-pass structure with one sequenced MERGE. These tests pin the shape
of that MERGE, because the failure mode the fix invites is a half-migration -- keeping the
old deletes pass, or dropping the LSN guard from one branch -- which reintroduces exactly
the defect it replaces while looking finished.

Pure: no Spark session, no containers. The runtime semantics are covered by
test_silver_cdc_semantics.py (real Spark, skipped when pyspark is absent) and the chain is
covered end-to-end by tests/integration/test_cdc_chain.py.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import payment_rules

REPO_ROOT = Path(__file__).resolve().parents[1]
SILVER_JOB = REPO_ROOT / "config" / "spark" / "jobs" / "silver_payments.py"
CONNECTOR = REPO_ROOT / "config" / "connect" / "postgres-cdc.json"

MERGE = payment_rules.silver_merge_sql("iceberg.analytics.payments_silver", "_silver_changes")


def _branch(keyword: str) -> str:
    """Return the MERGE line introducing the branch that ends in `keyword`."""
    for line in MERGE.splitlines():
        if line.startswith("WHEN ") and keyword in line:
            return line
    raise AssertionError(f"no MERGE branch containing {keyword!r} in:\n{MERGE}")


# --------------------------------------------------------------------------------------
# the sequenced MERGE
# --------------------------------------------------------------------------------------

def test_merge_has_all_three_branches() -> None:
    """A half-migrated merge that drops one branch is the failure this fix invites."""
    assert _branch("DELETE").startswith("WHEN MATCHED")
    assert _branch("UPDATE SET").startswith("WHEN MATCHED")
    assert _branch("INSERT").startswith("WHEN NOT MATCHED")


def test_both_matched_branches_carry_the_sequence_guard() -> None:
    """A stale delete and a stale update must each lose to the row already in silver.

    Guarding only the update branch leaves replayed deletes able to remove a live row.
    """
    assert "s.source_lsn > t.source_lsn" in _branch("DELETE")
    assert "s.source_lsn > t.source_lsn" in _branch("UPDATE SET")


def test_not_matched_branch_excludes_deletes() -> None:
    """A delete for a key silver does not have is a no-op, not a row of nulls."""
    insert_branch = _branch("INSERT")
    assert "s.source_lsn" not in insert_branch, "nothing to compare an absent row against"
    assert f"'{payment_rules.CDC_DELETE_OP}'" not in insert_branch


def test_delete_branch_matches_only_the_delete_op() -> None:
    assert f"s.op = '{payment_rules.CDC_DELETE_OP}'" in _branch("DELETE")


def test_upsert_branches_name_every_upsert_op() -> None:
    """Dropping 'r' would silently discard the initial snapshot."""
    for op in payment_rules.CDC_UPSERT_OPS:
        assert f"'{op}'" in _branch("UPDATE SET")
        assert f"'{op}'" in _branch("INSERT")


def test_merge_joins_on_the_payment_key() -> None:
    assert "ON t.payment_id = s.payment_id" in MERGE


def test_op_sets_are_disjoint_and_cover_the_debezium_alphabet() -> None:
    assert payment_rules.CDC_DELETE_OP not in payment_rules.CDC_UPSERT_OPS
    assert set(payment_rules.CDC_OPS) == set(payment_rules.CDC_UPSERT_OPS) | {
        payment_rules.CDC_DELETE_OP
    }
    assert list(payment_rules.CDC_OPS) == sorted(payment_rules.CDC_OPS)


# --------------------------------------------------------------------------------------
# the column list the MERGE and the DDL share
# --------------------------------------------------------------------------------------

def test_ddl_and_merge_agree_on_silver_columns() -> None:
    """Explicit column lists mean the MERGE breaks if the DDL gains a column silently."""
    ddl = re.search(
        r"CREATE TABLE IF NOT EXISTS \{SILVER_TABLE\} \((.*?)\)\s*USING iceberg",
        SILVER_JOB.read_text(encoding="utf-8"),
        re.DOTALL,
    )
    assert ddl, "silver DDL not found"
    declared = [
        line.split()[0]
        for line in (raw.strip() for raw in ddl.group(1).splitlines())
        if line and not line.startswith("--")
    ]
    assert declared == list(payment_rules.SILVER_COLUMNS)


def test_silver_carries_the_sequence_token() -> None:
    """source_lsn is the sequence token; without it the guard cannot be evaluated."""
    assert "source_lsn" in payment_rules.SILVER_COLUMNS


# --------------------------------------------------------------------------------------
# the two-pass structure is gone
# --------------------------------------------------------------------------------------

def test_no_separate_delete_pass_remains() -> None:
    """`DELETE FROM ... WHERE payment_id IN (...)` after the merge is the old inversion."""
    source = SILVER_JOB.read_text(encoding="utf-8")
    assert "_silver_deletes" not in source
    assert "DELETE FROM" not in source


def test_no_unguarded_update_remains() -> None:
    source = SILVER_JOB.read_text(encoding="utf-8")
    assert "WHEN MATCHED     THEN UPDATE SET *" not in source
    assert "WHEN MATCHED THEN UPDATE SET *" not in source


def test_job_builds_its_merge_from_the_shared_builder() -> None:
    """Inlining the SQL again is how the branches drift apart."""
    source = SILVER_JOB.read_text(encoding="utf-8")
    assert "silver_merge_sql(SILVER_TABLE" in source


def test_lsn_is_read_from_the_source_block_not_the_row() -> None:
    """`before.updated_at` is null for deletes under default REPLICA IDENTITY."""
    source = SILVER_JOB.read_text(encoding="utf-8")
    assert '"$.source.lsn"' in source


# --------------------------------------------------------------------------------------
# tombstones
# --------------------------------------------------------------------------------------

def test_connector_does_not_emit_tombstones() -> None:
    """Debezium defaults this to true; the op='d' event already carries the delete."""
    config = json.loads(CONNECTOR.read_text(encoding="utf-8"))["config"]
    assert config["tombstones.on.delete"] == "false"


def test_null_valued_records_are_dropped_before_the_dlq_check() -> None:
    """Tombstones already in bronze must not keep landing in the dead-letter table.

    The filter has to run *before* the null-op check, or the DLQ write happens anyway.
    """
    source = SILVER_JOB.read_text(encoding="utf-8")
    tombstone_filter = source.index('col("kafka_value").isNotNull()')
    null_op_dlq = source.index('"null_op"')
    assert tombstone_filter < null_op_dlq


# --------------------------------------------------------------------------------------
# dead-letter idempotency
# --------------------------------------------------------------------------------------

def test_dlq_merges_on_kafka_coordinates() -> None:
    """Appending made the DLQ grow with retries instead of with problems."""
    merge = payment_rules.dlq_merge_sql("iceberg.analytics.payments_silver_dlq", "_rows")

    assert merge.startswith("MERGE INTO")
    assert "t.kafka_partition = s.kafka_partition" in merge
    assert "t.kafka_offset = s.kafka_offset" in merge
    assert "WHEN MATCHED THEN UPDATE SET" in merge
    assert "WHEN NOT MATCHED THEN INSERT" in merge


def test_dlq_key_columns_are_not_reassigned_on_update() -> None:
    merge = payment_rules.dlq_merge_sql("dlq", "_rows")
    update_block = merge.split("WHEN MATCHED THEN UPDATE SET")[1].split("WHEN NOT MATCHED")[0]
    for key in payment_rules.DLQ_KEY:
        assert f"t.{key} = s.{key}" not in update_block


def test_dlq_ddl_matches_the_shared_column_list() -> None:
    ddl = re.search(
        r"CREATE TABLE IF NOT EXISTS \{DLQ_TABLE\} \((.*?)\)\s*USING iceberg",
        SILVER_JOB.read_text(encoding="utf-8"),
        re.DOTALL,
    )
    assert ddl, "DLQ DDL not found"
    declared = [
        line.split()[0]
        for line in (raw.strip() for raw in ddl.group(1).splitlines())
        if line and not line.startswith("--")
    ]
    assert declared == list(payment_rules.DLQ_COLUMNS)


def test_dlq_write_no_longer_appends() -> None:
    source = SILVER_JOB.read_text(encoding="utf-8")
    assert ".append()" not in source
    assert "dlq_merge_sql(DLQ_TABLE" in source


# --------------------------------------------------------------------------------------
# bronze replay detection
# --------------------------------------------------------------------------------------

def test_bronze_duplicate_check_keys_on_the_full_kafka_coordinate() -> None:
    """Offset alone is not unique across partitions, and partition not across topics."""
    sql = payment_rules.bronze_duplicate_offsets_sql("iceberg.analytics.payments_bronze")

    assert "GROUP BY kafka_topic, kafka_partition, kafka_offset" in sql
    assert "HAVING COUNT(*) > 1" in sql


def test_bronze_job_fails_the_run_on_a_replay() -> None:
    """A silent doubling of event history is worse than a failed task."""
    bronze = (REPO_ROOT / "config" / "spark" / "jobs" / "bronze_from_kafka.py").read_text(
        encoding="utf-8"
    )
    assert "class BronzeReplayDetected" in bronze
    # The check has to run after the write, or it inspects the previous run's table.
    # Split rather than index: the function's own `def` precedes its call site.
    after_write = bronze.split("query.awaitTermination()")[1]
    assert "assert_no_replayed_offsets(spark)" in after_write
