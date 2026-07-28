"""Submit one Spark medallion job under Docker Compose.

The Kubernetes path does not use this: there the DAG creates a pod per job with
KubernetesPodOperator (see airflow/dags/spark_jobs.py). This stays the Compose mechanism, where
Airflow has the Docker socket and there is no cluster to schedule against.

The submit arguments deliberately mirror airflow/dags/spark_jobs.py and the Job templates in
k8s/base/spark.yaml -- same master, same driver memory, same packages. `tests/test_spark_jobs.py`
asserts all three stay in step, because a Spark job that behaves differently depending on which
runtime launched it is exactly the drift this repo just spent a refactor eliminating.
"""
from __future__ import annotations

import logging
import subprocess
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

_ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
_KAFKA = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

# local[2] and an explicit heap, matching the cluster. Under local[*] the driver starts one task
# thread per core in a single JVM, and with no --driver-memory that JVM sizes its heap off the
# host rather than the container -- the combination that OOM-killed every spark-gold attempt.
SPARK_MASTER = "local[2]"
DRIVER_MEMORY = "1g"
JOBS_DIR = "/opt/project/config/spark/jobs"

JOB_SCRIPTS = {
    "bronze": ("bronze_from_kafka.py", f"{_KAFKA},{_ICEBERG}"),
    "silver": ("silver_payments.py", _ICEBERG),
    "gold": ("gold_metrics.py", _ICEBERG),
}


def submit_command(job_name: str) -> str:
    script, packages = JOB_SCRIPTS[job_name]
    return (
        "docker exec dp-spark /opt/spark/bin/spark-submit "
        f"--master {SPARK_MASTER} --driver-memory {DRIVER_MEMORY} "
        f"--conf spark.jars.ivy=/tmp/.ivy2 --packages {packages} {JOBS_DIR}/{script}"
    )


def main(job_name: str) -> None:
    if job_name not in JOB_SCRIPTS:
        raise SystemExit(f"Unsupported job: {job_name}")

    command = submit_command(job_name)
    LOGGER.info("Starting Spark job '%s'", job_name)
    LOGGER.info("Executing command: %s", command)
    subprocess.run(command, shell=True, check=True)
    LOGGER.info("Spark job '%s' completed successfully", job_name)


if __name__ == "__main__":  # pragma: no cover
    if len(sys.argv) != 2:
        raise SystemExit("Usage: run_local_job.py <bronze|silver|gold>")
    main(sys.argv[1])
