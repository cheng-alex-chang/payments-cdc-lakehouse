"""Guards for Iceberg table maintenance (scripts/maintain_iceberg.py).

The statements themselves are one-liners; what is worth pinning is the *ordering* and the
retention window. Expiring snapshots before compacting makes the cleanup a no-op, and a retention
threshold shorter than Trino's minimum is rejected at runtime rather than at review time.
"""
from __future__ import annotations

from unittest.mock import patch

from scripts.maintain_iceberg import (
    DEFAULT_RETENTION,
    MANAGED_TABLES,
    expire_snapshots_sql,
    main,
    maintenance_statements,
    optimize_sql,
    remove_orphan_files_sql,
)

TABLE = "iceberg.analytics.payments_bronze"


def test_optimize_compacts_the_named_table() -> None:
    assert optimize_sql(TABLE) == f"ALTER TABLE {TABLE} EXECUTE optimize"


def test_expire_snapshots_carries_a_retention_threshold() -> None:
    # Without a threshold Trino applies its own default; stating it makes the retained window a
    # decision in the repo rather than a property of the deployment.
    sql = expire_snapshots_sql(TABLE)

    assert "expire_snapshots" in sql
    assert f"retention_threshold => '{DEFAULT_RETENTION}'" in sql


def test_remove_orphan_files_carries_a_retention_threshold() -> None:
    # The threshold is what stops the cleanup deleting files belonging to an in-flight commit.
    sql = remove_orphan_files_sql(TABLE)

    assert "remove_orphan_files" in sql
    assert f"retention_threshold => '{DEFAULT_RETENTION}'" in sql


def test_default_retention_meets_trinos_minimum() -> None:
    # Trino rejects anything under iceberg.expire_snapshots.min-retention (7 days by default)
    # rather than silently discarding time-travel history.
    assert DEFAULT_RETENTION == "7d"


def test_every_iceberg_table_is_maintained() -> None:
    statements = maintenance_statements()

    for table in MANAGED_TABLES:
        assert any(table in statement for statement in statements)
    assert len(statements) == len(MANAGED_TABLES) * 3


def test_all_three_medallion_tables_are_managed() -> None:
    # Gold is a full INSERT OVERWRITE so it churns snapshots the fastest; bronze accumulates the
    # most small files. Neither should be left out.
    assert MANAGED_TABLES == (
        "iceberg.analytics.payments_bronze",
        "iceberg.analytics.payments_silver",
        "iceberg.analytics.payment_metrics_gold",
    )


def test_compaction_runs_before_expiry_for_each_table() -> None:
    statements = maintenance_statements(tables=(TABLE,))

    optimize_at = next(i for i, s in enumerate(statements) if "optimize" in s)
    expire_at = next(i for i, s in enumerate(statements) if "expire_snapshots" in s)
    orphan_at = next(i for i, s in enumerate(statements) if "remove_orphan_files" in s)

    # optimize writes new files and a new snapshot; expiring first would leave the pre-compaction
    # files still referenced, so the orphan sweep would find nothing to reclaim.
    assert optimize_at < expire_at < orphan_at


def test_retention_override_reaches_every_statement_that_takes_one() -> None:
    statements = maintenance_statements(tables=(TABLE,), retention="30d")

    assert sum("'30d'" in statement for statement in statements) == 2
    assert not any(f"'{DEFAULT_RETENTION}'" in statement for statement in statements)


def test_main_executes_every_statement_in_order() -> None:
    with patch("scripts.maintain_iceberg.run_statement") as run:
        main()

    executed = [call.args[0] for call in run.call_args_list]
    assert executed == maintenance_statements()
