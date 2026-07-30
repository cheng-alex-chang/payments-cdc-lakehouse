from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

import spark_jobs  # dags/ is on sys.path inside Airflow
from alerts import notify_failure

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # Fires after retries are exhausted; webhook-or-warn, see alerts.py.
    "on_failure_callback": notify_failure,
}


with DAG(
    dag_id="payments_pipeline",
    default_args=default_args,
    description="Build bronze, silver, and gold payment datasets",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    # Airflow pauses new DAGs by default, and a paused DAG accepts `airflow dags trigger` while
    # never running it -- the run sits in `queued` with no error logged anywhere. That made the
    # trigger command in the README silently do nothing on a cluster built from scratch, because
    # every earlier verification had unpaused this DAG by hand in the UI and nothing recorded the
    # step. Safe to unpause only because `schedule=None`: nothing runs until someone triggers it.
    is_paused_upon_creation=False,
    tags=["payments", "spark", "cdc"],
) as dag:
    # Storage and catalog bootstrap, replacing init_hdfs. Both are idempotent, so a retry costs
    # nothing, and both speak HTTP to a service name that resolves in either runtime.
    init_object_store = BashOperator(
        task_id="init_object_store",
        bash_command="python /opt/airflow/scripts/init_object_store.py",
    )

    init_catalog = BashOperator(
        task_id="init_catalog",
        bash_command="python /opt/airflow/scripts/init_iceberg_catalog.py",
    )

    validate_connector = BashOperator(
        task_id="validate_connector",
        bash_command="python /opt/airflow/scripts/validate_connector.py",
        retries=0,
    )

    validate_schema = BashOperator(
        task_id="validate_schema",
        bash_command="python /opt/airflow/scripts/validate_schema.py",
        retries=0,
    )

    # Under Kubernetes these become KubernetesPodOperator tasks that create a pod per job; under
    # Compose they stay BashOperators that submit into the dp-spark container. Selected by the
    # PIPELINE_RUNTIME env var -- see airflow/dags/spark_jobs.py. Everything else in this DAG is
    # already runtime-neutral: the remaining tasks talk HTTP to service names that resolve in
    # both.
    bronze_load = spark_jobs.spark_task("bronze_load", "bronze")
    silver_transform = spark_jobs.spark_task("silver_transform", "silver")
    gold_transform = spark_jobs.spark_task("gold_transform", "gold")

    publish_trino_tables = BashOperator(
        task_id="publish_trino_tables",
        bash_command="python /opt/airflow/scripts/publish_trino_tables.py",
    )

    validate_trino = BashOperator(
        task_id="validate_trino",
        bash_command="python /opt/airflow/scripts/validate_trino.py",
    )

    # Compaction, snapshot expiry, and orphan cleanup. Bronze commits a snapshot per micro-batch,
    # so without this the table accumulates small files and an unbounded manifest history -- which
    # slows query *planning*, not just scanning. Runs last, and only once the data has been
    # validated: maintenance rewrites files, and doing that to a bad load just preserves it more
    # efficiently.
    maintain_iceberg = BashOperator(
        task_id="maintain_iceberg",
        bash_command="python /opt/airflow/scripts/maintain_iceberg.py",
        # Last in the chain deliberately: maintenance rewrites files, and doing that to a bad load
        # only preserves it more efficiently. Placing it after validate_trino means it runs on
        # data that has already reconciled.
        retries=1,
    )

    init_object_store >> init_catalog >> validate_connector >> validate_schema >> bronze_load >> silver_transform >> gold_transform >> publish_trino_tables >> validate_trino >> maintain_iceberg
