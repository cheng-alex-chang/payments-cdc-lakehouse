from __future__ import annotations

import logging
import subprocess


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def main() -> None:
    LOGGER.info("Verifying Iceberg tables are visible in Trino")
    subprocess.run(
        'docker exec dp-trino trino --execute "SHOW TABLES IN iceberg.analytics"',
        shell=True,
        check=True,
    )
    LOGGER.info("Iceberg tables verified in Trino")


if __name__ == "__main__":  # pragma: no cover
    main()
