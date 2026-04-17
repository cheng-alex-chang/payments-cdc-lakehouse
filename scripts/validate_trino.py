from __future__ import annotations

import logging
import subprocess


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Running Trino validation queries")
    subprocess.run(
        "docker exec dp-trino trino --file /opt/project/sql/trino/validation_queries.sql",
        shell=True,
        check=True,
    )
    LOGGER.info("Trino validation queries completed successfully")


if __name__ == "__main__":  # pragma: no cover
    main()
