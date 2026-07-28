"""Verify the Iceberg tables are visible to Trino before anything queries them.

Talks to Trino over HTTP rather than shelling into a Compose container, so the same task runs
under Docker Compose and Kubernetes without a branch. See scripts/trino_http.py.
"""
from __future__ import annotations

import logging

# Airflow mounts these scripts as loose files under /opt/airflow/scripts with no package parent,
# so `from scripts import ...` resolves only when running from a repo checkout. Python puts the
# running script's own directory on sys.path, which makes the flat import work in the container.
try:
    from scripts import trino_http
except ImportError:  # pragma: no cover - exercised in the Airflow container, not in tests
    import trino_http

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

EXPECTED_TABLES = {"payments_bronze", "payments_silver", "payment_metrics_gold"}


def missing_tables(rows: list[list[object]]) -> set[str]:
    """SHOW TABLES returns one column; compare against what the medallion should have produced."""
    present = {str(row[0]) for row in rows if row}
    return EXPECTED_TABLES - present


def main() -> None:
    LOGGER.info("Verifying Iceberg tables are visible in Trino")
    rows = trino_http.run_statement("SHOW TABLES IN iceberg.analytics")

    missing = missing_tables(rows)
    if missing:
        # Previously this only echoed SHOW TABLES and exited 0 regardless, so a missing gold table
        # sailed through and surfaced later as an empty dashboard.
        raise SystemExit(f"Iceberg tables missing from Trino: {sorted(missing)}")

    LOGGER.info("Iceberg tables verified in Trino: %s", sorted(EXPECTED_TABLES))


if __name__ == "__main__":  # pragma: no cover
    main()
