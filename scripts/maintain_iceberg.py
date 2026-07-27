"""Iceberg table maintenance: compaction, snapshot expiry, orphan-file cleanup.

Iceberg's write path is append-only at the file level. Bronze is fed by a streaming job that
commits on every micro-batch, so without maintenance the table accumulates two problems that both
degrade reads long before storage becomes an issue:

* **Small files.** Each commit writes new Parquet files sized by what arrived in that batch, not by
  what reads well. Thousands of tiny files mean thousands of open/seek operations per scan.
* **Snapshot and manifest growth.** Every commit adds a snapshot and a manifest. Query planning
  reads manifests to prune files, so an unbounded snapshot history makes *planning* slow even for
  queries that touch almost no data.

This is the actual cause behind most "Iceberg is slow" reports -- not the table format, and not the
catalog. Running maintenance on a schedule is the fix.

Order matters: optimize first (it writes new compacted files and a new snapshot), then expire old
snapshots, then remove files no longer referenced by any live snapshot.

The SQL builders are pure so the statements are unit-testable without a warehouse; execution
follows the same `docker exec` convention as scripts/publish_trino_tables.py and
scripts/validate_trino.py.
"""
from __future__ import annotations

import logging
import subprocess

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

MANAGED_TABLES = (
    "iceberg.analytics.payments_bronze",
    "iceberg.analytics.payments_silver",
    "iceberg.analytics.payment_metrics_gold",
)

# Trino refuses a retention below iceberg.expire_snapshots.min-retention (7 days by default)
# rather than silently destroying time-travel history. Keeping the default means the last week of
# snapshots stays queryable, which is what makes "read the table as of yesterday" work during an
# incident.
DEFAULT_RETENTION = "7d"


def optimize_sql(table: str) -> str:
    """Compact small files into larger ones. Iceberg's rewrite_data_files, spelled for Trino."""
    return f"ALTER TABLE {table} EXECUTE optimize"


def expire_snapshots_sql(table: str, retention: str = DEFAULT_RETENTION) -> str:
    """Drop snapshots older than the retention window, with their manifests."""
    return f"ALTER TABLE {table} EXECUTE expire_snapshots(retention_threshold => '{retention}')"


def remove_orphan_files_sql(table: str, retention: str = DEFAULT_RETENTION) -> str:
    """Delete files in the table's directory that no live snapshot references.

    These are left behind by failed or aborted writes. The retention threshold protects files
    belonging to a commit that is still in flight.
    """
    return f"ALTER TABLE {table} EXECUTE remove_orphan_files(retention_threshold => '{retention}')"


def maintenance_statements(
    tables: tuple[str, ...] = MANAGED_TABLES,
    retention: str = DEFAULT_RETENTION,
) -> list[str]:
    """Every maintenance statement, in dependency order, for each table.

    Per table: optimize, then expire_snapshots, then remove_orphan_files. Expiring before
    compacting would leave the pre-compaction files referenced by a snapshot that optimize is
    about to supersede, so the cleanup would find nothing to do.
    """
    statements: list[str] = []
    for table in tables:
        statements.append(optimize_sql(table))
        statements.append(expire_snapshots_sql(table, retention))
        statements.append(remove_orphan_files_sql(table, retention))
    return statements


def run_statement(statement: str) -> None:
    subprocess.run(
        f'docker exec dp-trino trino --execute "{statement}"',
        shell=True,
        check=True,
    )


def main() -> None:
    statements = maintenance_statements()
    LOGGER.info("Running %s Iceberg maintenance statements", len(statements))
    for statement in statements:
        LOGGER.info("%s", statement)
        run_statement(statement)
    LOGGER.info("Iceberg maintenance completed")


if __name__ == "__main__":  # pragma: no cover
    main()
