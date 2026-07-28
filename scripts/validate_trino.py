"""Run the Trino validation queries that reconcile bronze, silver, and gold.

Talks to Trino over HTTP rather than shelling into a Compose container, so the same task runs
under Docker Compose and Kubernetes without a branch. See scripts/trino_http.py.
"""
from __future__ import annotations

import logging
from pathlib import Path

# See publish_trino_tables.py: Airflow mounts these as loose files with no package parent.
try:
    from scripts import trino_http
except ImportError:  # pragma: no cover - exercised in the Airflow container, not in tests
    import trino_http

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

# Airflow mounts the SQL at /opt/airflow/sql/trino; a checkout has it at the repo root. Preferring
# the mount keeps the container path authoritative without breaking a local run.
MOUNTED_SQL = Path("/opt/airflow/sql/trino/validation_queries.sql")
REPO_SQL = Path(__file__).resolve().parents[1] / "sql" / "trino" / "validation_queries.sql"


def validation_sql_path() -> Path:
    return MOUNTED_SQL if MOUNTED_SQL.is_file() else REPO_SQL


def main() -> None:
    path = validation_sql_path()
    LOGGER.info("Running Trino validation queries from %s", path)
    trino_http.run_script(path.read_text(encoding="utf-8"))
    LOGGER.info("Trino validation queries completed successfully")


if __name__ == "__main__":  # pragma: no cover
    main()
