from __future__ import annotations

import logging
import subprocess


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Publishing Hive-backed tables in Trino")
    subprocess.run(
        "docker exec dp-trino trino --file /opt/project/sql/trino/create_hive_tables.sql",
        shell=True,
        check=True,
    )
    LOGGER.info("Finished publishing Hive-backed tables in Trino")


if __name__ == "__main__":  # pragma: no cover
    main()
