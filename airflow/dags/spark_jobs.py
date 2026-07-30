"""Spark job definitions and the per-runtime operator factory.

The pipeline submits the same three Spark jobs in both runtimes, but reaches them differently:

* **Compose** shells into the `dp-spark` container, because Airflow there has the Docker socket
  and there is no cluster to schedule against.
* **Kubernetes** creates a pod per job with KubernetesPodOperator. That is what makes the DAG the
  actual orchestrator on the cluster, rather than a parser whose tasks would fail if triggered
  while the real work happened through `kubectl patch`.

Only the submission mechanism differs. The job specs -- master, driver memory, packages, script
paths -- live here once, and `tests/test_spark_jobs.py` asserts the Compose path and the
Kubernetes Job templates in k8s/base/spark.yaml still agree with them.
"""
from __future__ import annotations

import os
from typing import Any

SPARK_IMAGE = "spark:3.5.8-python3"
JOBS_DIR = "/opt/project/config/spark/jobs"

# iceberg-aws-bundle carries the AWS SDK v2 that S3FileIO uses. hadoop-aws is still needed for
# the Structured Streaming checkpoints, which are plain s3a:// paths rather than Iceberg tables --
# it is pinned to 3.3.4 to match hadoop-client-api in spark:3.5.8-python3, because a mismatch
# there produces class-loading errors that look nothing like a version problem.
ICEBERG_PACKAGE = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
    ",org.apache.iceberg:iceberg-aws-bundle:1.6.1"
    ",org.apache.hadoop:hadoop-aws:3.3.4"
)
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8"

# local[2] rather than local[*]: local[*] starts one task thread per core inside a single JVM and
# each carries its own buffers. These datasets are small, so extra threads only multiply memory.
SPARK_MASTER = "local[2]"
# An explicit heap; otherwise the JVM sizes its default maximum off the host's total memory,
# ignoring the container it lives in, and gets OOM-killed.
DRIVER_MEMORY = "1g"

JOBS: dict[str, dict[str, str]] = {
    "bronze": {"script": "bronze_from_kafka.py", "packages": f"{KAFKA_PACKAGE},{ICEBERG_PACKAGE}"},
    "silver": {"script": "silver_payments.py", "packages": ICEBERG_PACKAGE},
    "gold": {"script": "gold_metrics.py", "packages": ICEBERG_PACKAGE},
}


def runtime() -> str:
    """Which runtime this DAG is orchestrating. Compose unless told otherwise."""
    return os.getenv("PIPELINE_RUNTIME", "compose").strip().lower()


def spark_submit_argv(layer: str) -> list[str]:
    """The spark-submit command for one medallion layer, identical in both runtimes."""
    job = JOBS[layer]
    return [
        "/opt/spark/bin/spark-submit",
        "--master", SPARK_MASTER,
        "--driver-memory", DRIVER_MEMORY,
        "--conf", "spark.jars.ivy=/tmp/.ivy2",
        "--packages", job["packages"],
        f"{JOBS_DIR}/{job['script']}",
    ]


def _kubernetes_operator(task_id: str, layer: str) -> Any:
    """A pod running one Spark job, mirroring the Job templates in k8s/base/spark.yaml.

    Imported lazily so a Compose deployment without the cncf-kubernetes provider can still parse
    this DAG.
    """
    from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
    from kubernetes.client import models as k8s

    return KubernetesPodOperator(
        task_id=task_id,
        name=f"spark-{layer}",
        namespace="data-pipeline",
        image=SPARK_IMAGE,
        cmds=spark_submit_argv(layer),
        # The airflow ServiceAccount is bound to the spark-job-runner Role, but the *pod* runs as
        # the spark ServiceAccount, matching the Job templates.
        service_account_name="spark",
        in_cluster=True,
        get_logs=True,
        # Keep a failed pod so its logs survive for inspection; clean up successful ones.
        on_finish_action="delete_succeeded_pod",
        # Airflow reuses a pod when name and parameters match; a fresh one per run avoids
        # inheriting a previous attempt's state.
        random_name_suffix=True,
        container_resources=k8s.V1ResourceRequirements(
            requests={"cpu": "500m", "memory": "1Gi"},
            limits={"memory": "2Gi"},
        ),
        # These pods are built here, not from the Job templates in k8s/base/spark.yaml, so
        # anything the jobs need in their environment has to be repeated in both places.
        # S3 credentials live here because the Iceberg S3FileIO client and the s3a checkpoint
        # filesystem both read the standard AWS_* variables.
        env_vars=[
            k8s.V1EnvVar(name="HOME", value="/tmp"),
            k8s.V1EnvVar(
                name="AWS_ACCESS_KEY_ID",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="platform-secrets", key="MINIO_ROOT_USER"
                    )
                ),
            ),
            k8s.V1EnvVar(
                name="AWS_SECRET_ACCESS_KEY",
                value_from=k8s.V1EnvVarSource(
                    secret_key_ref=k8s.V1SecretKeySelector(
                        name="platform-secrets", key="MINIO_ROOT_PASSWORD"
                    )
                ),
            ),
        ],
        volumes=[
            # Streaming checkpoints live on a volume, not in the warehouse bucket -- a REST
            # catalog scopes storage access per table, and a checkpoint belongs to no table.
            k8s.V1Volume(
                name="checkpoints",
                persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
                    claim_name="spark-checkpoints"
                ),
            ),
            k8s.V1Volume(
                name="spark-jobs",
                config_map=k8s.V1ConfigMapVolumeSource(name="spark-jobs"),
            ),
        ],
        volume_mounts=[
            k8s.V1VolumeMount(name="checkpoints", mount_path="/checkpoints"),
            k8s.V1VolumeMount(name="spark-jobs", mount_path=JOBS_DIR),
        ],
    )


def spark_task(task_id: str, layer: str) -> Any:
    """Build the right operator for the current runtime."""
    if layer not in JOBS:
        raise ValueError(f"unknown Spark layer: {layer}")

    if runtime() == "kubernetes":
        return _kubernetes_operator(task_id, layer)

    from airflow.operators.bash import BashOperator

    return BashOperator(
        task_id=task_id,
        bash_command=f"python /opt/airflow/scripts/run_local_job.py {layer}",
    )
