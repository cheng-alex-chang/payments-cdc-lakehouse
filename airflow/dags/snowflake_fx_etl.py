"""Snowflake FX ELT DAG -- the batch + cloud-warehouse sibling of payments_pipeline.

Operator choices:
* TaskFlow ``@task`` for the Python S3 staging (boto3 work, runs in-process).
* ``SnowflakeOperator`` for the RAW load (COPY INTO), so the warehouse load runs through an
  Airflow **Connection** (``snowflake_default``) -- credentials live in Airflow's connection
  store / secrets backend, not in the DAG.
* ``BashOperator`` running **dbt** for the transform + tests: dbt owns the model DAG
  (ref()-derived ordering) and the data-quality gates, so Airflow just invokes
  ``dbt run`` / ``dbt test`` and fails the task on a non-zero exit. (astronomer-cosmos could
  render model-level tasks later; one task per dbt command keeps this image-light.)

Runtime prerequisites (Airflow worker image): ``apache-airflow-providers-snowflake`` and
``dbt-snowflake`` installed, a ``snowflake_default`` connection, ``SNOWFLAKE_*`` env vars for
dbt's profile (see snowflake_etl/dbt/profiles.yml), and ``SNOWFLAKE_LAKE_BUCKET`` /
``SNOWFLAKE_STAGE`` available to the workers.
"""
from __future__ import annotations

import os
import shlex
from datetime import date, datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

from alerts import notify_failure  # dags/ is on sys.path inside Airflow

from snowflake_etl.src import load_to_snowflake, stage_to_s3

SNOWFLAKE_CONN_ID = "snowflake_default"
# Deliberately not S3_BUCKET: payments_pipeline runs on these same workers and reads
# S3_BUCKET as the Iceberg warehouse bucket. Sharing the name points one of the two DAGs
# at the other's storage.
LAKE_BUCKET = os.getenv("SNOWFLAKE_LAKE_BUCKET", "payments-lake")
SNOWFLAKE_STAGE = os.getenv("SNOWFLAKE_STAGE", "PAYMENTS_LAKE_STAGE")
# Resolved relative to this file (repo root is two levels up from airflow/dags/) so the
# command works in any worker checkout layout; shell-quoted in case the path has spaces.
DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "snowflake_etl" / "dbt"
DBT_FLAGS = f"--project-dir {shlex.quote(str(DBT_PROJECT_DIR))} --profiles-dir {shlex.quote(str(DBT_PROJECT_DIR))}"

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


def _load_raw_sql() -> list[str]:
    """DDL + COPY statements for both RAW tables, partition templated to Airflow's {{ ds }}.

    Reuses the Phase-3 pure SQL builders; SnowflakeOperator renders the {{ ds }} at runtime.
    """
    statements: list[str] = []
    for dataset, table in load_to_snowflake.RAW_TABLES.items():
        statements.append(load_to_snowflake.create_raw_table_sql(table))
        statements.append(load_to_snowflake.copy_into_sql(table, SNOWFLAKE_STAGE, dataset, "{{ ds }}"))
    return statements


with DAG(
    dag_id="snowflake_fx_etl",
    default_args=default_args,
    description="Stage FX rates + payments to S3, load Snowflake RAW, dbt transform + test",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["payments", "snowflake", "s3", "fx", "elt", "dbt"],
) as dag:

    def _staged_to_xcom(staged: list) -> list[dict]:
        """Flatten StagedSnapshot into JSON XCom can carry (dates are not serializable)."""
        return [
            {
                "dataset": s.dataset,
                "run_id": s.run_id,
                "run_date": s.run_date.isoformat(),
                "expected_parts": s.part_count,
                "expected_rows": s.row_count,
            }
            for s in staged or []
        ]

    @task(task_id="stage_fx_rates")
    def stage_fx_rates(ds: str | None = None, run_id: str | None = None) -> list[dict]:
        args = ["--bucket", LAKE_BUCKET, "--datasets", "fx_rates"]
        if ds:
            args += ["--run-date", ds]
        if run_id:
            args += ["--run-id", run_id]
        return _staged_to_xcom(stage_to_s3.main(args))

    @task(task_id="stage_payments")
    def stage_payments(
        ds: str | None = None,
        run_id: str | None = None,
        data_interval_start=None,  # noqa: ANN001 - pendulum, injected by Airflow
        data_interval_end=None,  # noqa: ANN001
    ) -> list[dict]:
        args = ["--bucket", LAKE_BUCKET, "--datasets", "payments"]
        if ds:
            args += ["--run-date", ds]
        if run_id:
            args += ["--run-id", run_id]
        # Incremental watermark: extract only the payments updated inside this run's data
        # interval. On a manual trigger (schedule=None) start == end, which would select
        # nothing -- fall back to the full snapshot in that case.
        windowed = bool(
            data_interval_start and data_interval_end and data_interval_start < data_interval_end
        )
        if windowed:
            args += [
                "--updated-after", data_interval_start.strftime("%Y-%m-%d %H:%M:%S"),
                "--updated-before", data_interval_end.strftime("%Y-%m-%d %H:%M:%S"),
            ]
        staged = _staged_to_xcom(stage_to_s3.main(args))
        # A windowed run is a delta, not the state of the source. Recorded here because
        # only this task knows which mode it ran in, and stg_payments must never treat a
        # delta as a snapshot -- doing so would delete every payment outside the window.
        for row in staged:
            row["snapshot_type"] = (
                load_to_snowflake.SNAPSHOT_WINDOW if windowed else load_to_snowflake.SNAPSHOT_FULL
            )
        return staged

    @task(task_id="register_runs")
    def register_runs(*staged_groups: list[dict]) -> list[dict]:
        """Record each staged run with staged_at set and completed_at still NULL.

        Nothing downstream may read these runs yet: their data is in S3, not in RAW.
        """
        rows = [row for group in staged_groups for row in group]
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            for row in rows:
                load_to_snowflake.register_snapshot_run(
                    conn,
                    dataset=row["dataset"],
                    run_id=row["run_id"],
                    run_date=date.fromisoformat(row["run_date"]),
                    snapshot_type=row.get("snapshot_type", load_to_snowflake.SNAPSHOT_FULL),
                    expected_parts=row["expected_parts"],
                    expected_rows=row["expected_rows"],
                )
            conn.commit()
        finally:
            conn.close()
        return rows

    load_raw = SnowflakeOperator(
        task_id="load_raw",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql=_load_raw_sql(),
    )

    @task(task_id="validate_snapshot_load")
    def validate_snapshot_load(rows: list[dict]) -> None:
        """Reconcile RAW against what staging wrote, then stamp completed_at.

        This is the transition that makes a run visible to dbt. Splitting it from load_raw
        keeps the failure legible: a COPY that errors fails there, a COPY that returns but
        does not account for the staged data fails here.

        The COPY result is re-read from Snowflake's load history rather than passed down
        from load_raw, because SnowflakeOperator does not surface the result set.
        """
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            for row in rows:
                table = load_to_snowflake.RAW_TABLES[row["dataset"]]
                outcome = load_to_snowflake.copy_history_for_run(
                    conn, table=table, run_id=row["run_id"]
                )
                load_to_snowflake.reconcile_snapshot(
                    expected_parts=row["expected_parts"],
                    expected_rows=row["expected_rows"],
                    outcome=outcome,
                    snapshot_type=row.get("snapshot_type", load_to_snowflake.SNAPSHOT_FULL),
                )
                load_to_snowflake.complete_snapshot_run(
                    conn, dataset=row["dataset"], run_id=row["run_id"], outcome=outcome
                )
            conn.commit()
        finally:
            conn.close()

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"dbt run {DBT_FLAGS}",
    )

    # Data-quality gates: schema tests + the singular reconcile/identity tests. retries=0 --
    # a deterministic data-quality failure should page, not retry.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"dbt test {DBT_FLAGS}",
        retries=0,
    )

    staged_fx = stage_fx_rates()
    staged_payments = stage_payments()
    registered = register_runs(staged_fx, staged_payments)

    # register_runs -> load_raw -> validate_snapshot_load is the completion state machine:
    # S3 written, then RAW loaded, then the two reconciled. Only the last step sets
    # completed_at, and only a completed run is visible to stg_payments -- so a COPY that
    # fails or half-loads leaves the previous snapshot serving instead of reading as a
    # mass deletion.
    registered >> load_raw >> validate_snapshot_load(registered) >> dbt_run >> dbt_test
