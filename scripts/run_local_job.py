from __future__ import annotations

import logging
import subprocess
import sys


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

_ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
_KAFKA   = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

JOB_MAP = {
    "bronze": f"docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] --packages {_KAFKA},{_ICEBERG} /opt/project/config/spark/jobs/bronze_from_kafka.py",
    "silver": f"docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] --packages {_ICEBERG} /opt/project/config/spark/jobs/silver_payments.py",
    "gold":   f"docker exec dp-spark /opt/spark/bin/spark-submit --master local[*] --packages {_ICEBERG} /opt/project/config/spark/jobs/gold_metrics.py",
}


def main(job_name: str) -> None:
    command = JOB_MAP.get(job_name)
    if command is None:
        raise SystemExit(f"Unsupported job: {job_name}")

    LOGGER.info("Starting Spark job '%s'", job_name)
    LOGGER.info("Executing command: %s", command)
    subprocess.run(command, shell=True, check=True)
    LOGGER.info("Spark job '%s' completed successfully", job_name)


if __name__ == "__main__":  # pragma: no cover
    if len(sys.argv) != 2:
        raise SystemExit("Usage: run_local_job.py <bronze|silver|gold>")
    main(sys.argv[1])
