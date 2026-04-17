from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
}


with DAG(
    dag_id="payments_pipeline",
    default_args=default_args,
    description="Build bronze, silver, and gold payment datasets",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["payments", "spark", "cdc"],
) as dag:
    init_hdfs = BashOperator(
        task_id="init_hdfs",
        bash_command="python /opt/airflow/scripts/init_hdfs.py",
    )

    validate_connector = BashOperator(
        task_id="validate_connector",
        bash_command="python /opt/airflow/scripts/validate_connector.py",
    )

    bronze_load = BashOperator(
        task_id="bronze_load",
        bash_command="python /opt/airflow/scripts/run_local_job.py bronze",
    )

    silver_transform = BashOperator(
        task_id="silver_transform",
        bash_command="python /opt/airflow/scripts/run_local_job.py silver",
    )

    gold_transform = BashOperator(
        task_id="gold_transform",
        bash_command="python /opt/airflow/scripts/run_local_job.py gold",
    )

    publish_trino_tables = BashOperator(
        task_id="publish_trino_tables",
        bash_command="python /opt/airflow/scripts/publish_trino_tables.py",
    )

    validate_trino = BashOperator(
        task_id="validate_trino",
        bash_command="python /opt/airflow/scripts/validate_trino.py",
    )

    init_hdfs >> validate_connector >> bronze_load >> silver_transform >> gold_transform >> publish_trino_tables >> validate_trino
