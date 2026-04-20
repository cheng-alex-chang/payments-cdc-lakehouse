from __future__ import annotations

import logging
import os
import subprocess
from pathlib import Path


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    project_root = Path(__file__).resolve().parents[1]
    seed_sql = project_root / "config" / "postgres" / "init" / "002_seed_data.sql"
    sql_text = seed_sql.read_text(encoding="utf-8")

    postgres_user = os.getenv("POSTGRES_USER", "dataeng")
    postgres_db = os.getenv("POSTGRES_DB", "payments")
    postgres_password = os.getenv("POSTGRES_PASSWORD")

    command = ["docker", "exec"]
    if postgres_password:
        command.extend(["-e", f"PGPASSWORD={postgres_password}"])

    command.extend(
        [
            "-i",
            "dp-postgres",
            "psql",
            "-v",
            "ON_ERROR_STOP=1",
            "-U",
            postgres_user,
            "-d",
            postgres_db,
        ]
    )

    LOGGER.info("Loading demo data into %s/%s", "dp-postgres", postgres_db)
    subprocess.run(command, input=sql_text, text=True, check=True)
    LOGGER.info("Demo data load completed")


if __name__ == "__main__":  # pragma: no cover
    main()
