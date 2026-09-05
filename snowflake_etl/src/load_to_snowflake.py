"""Load the S3-staged JSON into Snowflake RAW tables (Phase 3).

This is the ``L`` in ELT: it pulls the date-partitioned objects written by Phase 2
(``raw/<dataset>/dt=<run_date>/...jsonl``) off an external stage and ``COPY INTO`` a
landing table that stores each JSON line *untouched* in a single ``VARIANT`` column.
No reshaping happens here -- the transform (split VARIANT into typed columns, join FX to
payments, normalize to USD) is deferred to Snowflake SQL in Phase 4.

Design notes:
* **SQL builders are pure functions** (``create_raw_table_sql`` / ``copy_into_sql``) that
  return strings, so the statement shape is unit-testable without a live warehouse or even
  the Snowflake driver installed.
* **The driver is imported lazily** (only inside ``connect_from_env``), so the mocked unit
  tests -- and anyone running ``pytest`` after a plain clone -- never need
  ``snowflake-connector-python``. The real driver is only pulled in for the live load.
* **COPY INTO is not the idempotency mechanism.** Snowflake skips files it has already
  loaded *and that have not changed* -- a re-staged file with different contents gets a new
  checksum and is loaded again. Idempotency comes from the run-scoped prefixes staging
  writes (every run is a distinct set of files) plus the dedup in ``stg_payments``. A
  ``LOAD_SKIPPED`` is therefore a problem to report, not the happy path.
* **A load is not finished when COPY returns.** ``RAW.SNAPSHOT_RUNS`` tracks each run
  through staged -> loaded -> completed, and only a run whose loaded counts reconcile with
  what staging wrote is visible to dbt. See the section at the bottom of this module.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import logging
import os
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULT_PREFIX = "raw"
# Schema the external stage lives in (Terraform creates it in PAYMENTS.RAW). Used to
# qualify a bare stage name so the COPY does not depend on the session schema.
DEFAULT_STAGE_SCHEMA = "RAW"

# dataset name (matches the Phase-2 S3 prefix) -> fully-qualified RAW landing table.
RAW_TABLES = {
    "fx_rates": "RAW.RAW_FX_RATES",
    "payments": "RAW.RAW_PAYMENTS",
}


def create_raw_table_sql(table: str) -> str:
    """DDL for a VARIANT landing table; ``IF NOT EXISTS`` keeps the loader idempotent.

    ``raw`` holds each JSON line verbatim; ``source_file`` (from ``METADATA$FILENAME``) and
    ``loaded_at`` give lineage for debugging and replay auditing.
    """
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        f"  raw         VARIANT,\n"
        f"  source_file STRING,\n"
        f"  loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()\n"
        f")"
    )


def copy_into_sql(
    table: str,
    stage: str,
    dataset: str,
    run_date: dt.date | str,
    *,
    prefix: str = DEFAULT_PREFIX,
) -> str:
    """Build the ``COPY INTO`` that reads one date partition off the external stage.

    The stage location mirrors the Phase-2 key layout exactly
    (``@<stage>/raw/<dataset>/dt=<run_date>/``). ``$1`` is the whole JSON object parsed as a
    VARIANT; ``METADATA$FILENAME`` is captured for lineage. ``ON_ERROR = ABORT_STATEMENT``
    means a single malformed row fails the load loudly rather than silently dropping data.

    ``run_date`` may be a ``date`` (CLI/integration path) or a string -- the Airflow DAG passes
    the ``"{{ ds }}"`` template literal so SnowflakeOperator renders the partition at runtime.
    """
    run_date_str = run_date.isoformat() if isinstance(run_date, dt.date) else run_date
    # An unqualified stage name resolves against the *session* schema, not the stage's own.
    # The stage lives in RAW (Terraform puts it there) but callers legitimately connect with
    # SNOWFLAKE_SCHEMA=ANALYTICS -- dbt does -- and the COPY then fails with "Stage
    # PAYMENTS.ANALYTICS.PAYMENTS_LAKE_STAGE does not exist". Qualify it unless the caller
    # already did.
    qualified = stage if "." in stage else f"{DEFAULT_STAGE_SCHEMA}.{stage}"
    location = f"@{qualified}/{prefix}/{dataset}/dt={run_date_str}/"
    return (
        f"COPY INTO {table} (raw, source_file)\n"
        f"FROM (\n"
        f"  SELECT $1, METADATA$FILENAME\n"
        f"  FROM {location}\n"
        f")\n"
        f"FILE_FORMAT = (TYPE = JSON STRIP_OUTER_ARRAY = FALSE)\n"
        f"ON_ERROR = ABORT_STATEMENT"
    )


@dataclasses.dataclass(frozen=True)
class CopyOutcome:
    """What a COPY INTO actually did, per file and in total.

    The old helper summed ``rows_loaded`` and returned an int, which lost the two facts the
    snapshot contract needs: how many files were touched, and whether any of them were
    skipped. It also described ``LOAD_SKIPPED`` as the normal idempotency path, which is not
    how ``COPY INTO`` behaves -- a re-staged file whose contents changed gets a new checksum
    and is loaded again. Skips now mean something has gone wrong (most often a partition
    whose files are older than the 64-day load-metadata window), so they are reported rather
    than silently folded into a sum.
    """

    files: tuple[str, ...]
    rows_loaded: int
    skipped: tuple[str, ...]

    @property
    def file_count(self) -> int:
        return len(self.files)


def parse_copy_result(copy_result: list[tuple]) -> CopyOutcome:
    """Read a COPY INTO result set: one row per source file.

    Snowflake returns ``(file, status, rows_parsed, rows_loaded, ...)``. Short or malformed
    rows are ignored rather than raising -- ``reconcile_snapshot`` is what fails the run,
    and it fails on the numbers, not on the shape of this tuple.
    """
    files: list[str] = []
    skipped: list[str] = []
    total = 0
    for row in copy_result:
        if not row:
            continue
        name = str(row[0])
        files.append(name)
        status = str(row[1]).upper() if len(row) > 1 and row[1] is not None else ""
        if status == "LOAD_SKIPPED":
            skipped.append(name)
        if len(row) > 3 and isinstance(row[3], int):
            total += row[3]
    return CopyOutcome(files=tuple(files), rows_loaded=total, skipped=tuple(skipped))


def load_dataset(
    conn: Any,
    *,
    table: str,
    stage: str,
    dataset: str,
    run_date: dt.date,
    prefix: str = DEFAULT_PREFIX,
) -> CopyOutcome:
    """Ensure the landing table exists, COPY the partition in, report what landed."""
    cursor = conn.cursor()
    try:
        cursor.execute(create_raw_table_sql(table))
        cursor.execute(copy_into_sql(table, stage, dataset, run_date, prefix=prefix))
        result = cursor.fetchall()
    finally:
        cursor.close()
    outcome = parse_copy_result(result)
    LOGGER.info(
        "Loaded %d rows into %s from dataset %s (dt=%s) across %d file(s); %d skipped",
        outcome.rows_loaded, table, dataset, run_date, outcome.file_count, len(outcome.skipped),
    )
    return outcome


# --------------------------------------------------------------------------------------
# Snapshot completion state
# --------------------------------------------------------------------------------------
#
# A staging run is not usable because S3 has its files. It is usable when those files are
# provably in RAW. Keeping only an S3-side marker would let a run whose COPY failed or
# half-loaded advertise itself as the newest complete snapshot, and stg_payments -- which
# treats the newest complete snapshot as the whole current state of the source -- would
# read the missing rows as deletions.
#
# So the control table is a state machine, not a marker:
#
#     staged_at   -> every part object written
#     loaded_at   -> COPY INTO returned, actual counts recorded
#     completed_at -> actual counts reconcile with expected; safe to consume
#
# Only the last transition makes a run visible to dbt.

SNAPSHOT_RUNS_TABLE = "RAW.SNAPSHOT_RUNS"

# How far back to look for a run's COPY in the load history.
#
# Not 24 hours. A backfill of an older partition, or a run that sat queued behind a long
# retry chain, then finds no history and reconciliation reports an incomplete load for
# data that landed correctly. Seven days is comfortably inside the 14 days
# INFORMATION_SCHEMA.COPY_HISTORY retains, and the query is filtered to one run's files,
# so widening the window costs nothing.
COPY_HISTORY_LOOKBACK_HOURS = 24 * 7

SNAPSHOT_FULL = "full"
SNAPSHOT_WINDOW = "window"


def create_snapshot_runs_sql(table: str = SNAPSHOT_RUNS_TABLE) -> str:
    """DDL for the snapshot control table."""
    return (
        f"CREATE TABLE IF NOT EXISTS {table} (\n"
        f"  dataset        STRING,\n"
        f"  run_id         STRING,\n"
        f"  run_date       DATE,\n"
        f"  snapshot_type  STRING,\n"
        f"  expected_parts INTEGER,\n"
        f"  expected_rows  INTEGER,\n"
        f"  loaded_parts   INTEGER,\n"
        f"  loaded_rows    INTEGER,\n"
        f"  staged_at      TIMESTAMP_NTZ,\n"
        f"  loaded_at      TIMESTAMP_NTZ,\n"
        f"  completed_at   TIMESTAMP_NTZ\n"
        f")"
    )


def bootstrap_raw_objects(conn: Any, *, snapshot_runs_table: str = SNAPSHOT_RUNS_TABLE) -> list[str]:
    """Create every RAW object the dbt project reads, idempotently. Returns what it ensured.

    Terraform owns the *infrastructure* -- database, schemas, warehouse, role, grants, the
    storage integration and the external stage -- and deliberately not the tables, which are
    workload. That split works as long as something creates the tables before dbt reads them.

    The landing tables get created on the fly by ``load_dataset``, so a load bootstraps them.
    ``SNAPSHOT_RUNS`` does not have that luxury: ``stg_payments`` selects from it, so it must
    exist before *dbt* runs, and CI runs ``dbt build`` without ever going through the loader.
    Without this, a fresh account fails the rehearsal at "Object does not exist" on a table
    nothing had a reason to create yet.
    """
    ensured: list[str] = []
    cursor = conn.cursor()
    try:
        for table in RAW_TABLES.values():
            cursor.execute(create_raw_table_sql(table))
            ensured.append(table)
        cursor.execute(create_snapshot_runs_sql(snapshot_runs_table))
        ensured.append(snapshot_runs_table)
    finally:
        cursor.close()
    LOGGER.info("Bootstrapped RAW objects: %s", ", ".join(ensured))
    return ensured


def register_snapshot_run(
    conn: Any,
    *,
    dataset: str,
    run_id: str,
    run_date: dt.date,
    snapshot_type: str,
    expected_parts: int,
    expected_rows: int,
    table: str = SNAPSHOT_RUNS_TABLE,
) -> None:
    """Record a staged-but-not-yet-loaded run. ``completed_at`` stays NULL."""
    if snapshot_type not in (SNAPSHOT_FULL, SNAPSHOT_WINDOW):
        raise ValueError(f"snapshot_type must be {SNAPSHOT_FULL!r} or {SNAPSHOT_WINDOW!r}")
    cursor = conn.cursor()
    try:
        cursor.execute(create_snapshot_runs_sql(table))
        # MERGE, not INSERT: (dataset, run_id) is the identity of a staging run, and the
        # Airflow task that calls this carries retries=3. A plain insert turns any retry
        # after a successful write -- a connection dropped on the *response*, say -- into a
        # second row for one run, and complete_snapshot_run would then stamp both.
        cursor.execute(
            f"MERGE INTO {table} t\n"
            f"USING (SELECT %s AS dataset, %s AS run_id, %s AS run_date, %s AS snapshot_type, "
            f"%s AS expected_parts, %s AS expected_rows) s\n"
            f"  ON t.dataset = s.dataset AND t.run_id = s.run_id\n"
            f"WHEN MATCHED THEN UPDATE SET\n"
            f"    t.run_date = s.run_date, t.snapshot_type = s.snapshot_type,\n"
            f"    t.expected_parts = s.expected_parts, t.expected_rows = s.expected_rows,\n"
            f"    t.staged_at = CURRENT_TIMESTAMP()\n"
            f"WHEN NOT MATCHED THEN INSERT "
            f"(dataset, run_id, run_date, snapshot_type, expected_parts, expected_rows, staged_at)\n"
            f"    VALUES (s.dataset, s.run_id, s.run_date, s.snapshot_type, "
            f"s.expected_parts, s.expected_rows, CURRENT_TIMESTAMP())",
            (dataset, run_id, run_date.isoformat(), snapshot_type, expected_parts, expected_rows),
        )
    finally:
        cursor.close()
    LOGGER.info(
        "Registered %s run %s (%s): %d part(s), %d row(s) staged",
        dataset, run_id, snapshot_type, expected_parts, expected_rows,
    )


def copy_history_sql(table: str, run_id: str, *, lookback_hours: int = COPY_HISTORY_LOOKBACK_HOURS) -> str:
    """Query Snowflake's load history for the files belonging to one run.

    ``SnowflakeOperator`` does not surface a statement's result set, so the reconciliation
    step cannot be handed the COPY output directly. ``COPY_HISTORY`` is the same information
    read back afterwards, and it is authoritative -- it reports what the warehouse believes
    it loaded, not what the client thinks it asked for.

    Columns are projected in the order ``parse_copy_result`` expects
    (file, status, rows_parsed, rows_loaded), so the two paths share one parser.
    """
    safe_run_id = run_id.replace("'", "''")
    return (
        f"SELECT FILE_NAME, STATUS, ROW_PARSED, ROW_COUNT\n"
        f"FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(\n"
        f"  TABLE_NAME => '{table}',\n"
        f"  START_TIME => DATEADD(hours, -{lookback_hours}, CURRENT_TIMESTAMP())\n"
        f"))\n"
        f"WHERE FILE_NAME LIKE '%run={safe_run_id}/%'"
    )


def copy_history_for_run(
    conn: Any, *, table: str, run_id: str, lookback_hours: int = COPY_HISTORY_LOOKBACK_HOURS
) -> CopyOutcome:
    """Read back what the COPY actually landed for one run."""
    cursor = conn.cursor()
    try:
        cursor.execute(copy_history_sql(table, run_id, lookback_hours=lookback_hours))
        result = cursor.fetchall()
    finally:
        cursor.close()
    return parse_copy_result(result)


class SnapshotIncomplete(RuntimeError):
    """The COPY result does not account for everything staging wrote."""


def reconcile_snapshot(
    *,
    expected_parts: int,
    expected_rows: int,
    outcome: CopyOutcome,
    snapshot_type: str,
    allow_empty: bool = False,
) -> None:
    """Raise unless the load fully accounts for the staged snapshot.

    Pure, so the completion rule is testable without a warehouse. Three ways to fail:

    * fewer files loaded than staged -- a partial COPY;
    * fewer rows loaded than staged -- files present but not fully parsed;
    * any file skipped -- with per-run prefixes a skip is never the idempotency path, it
      means the load metadata considered the file already handled (the 64-day window) and
      the data silently did not arrive.

    A ``full`` snapshot reporting zero rows is refused separately. It is indistinguishable
    from an extract that broke, and because absence means deletion downstream, accepting it
    would empty the fact. ``allow_empty`` is the deliberate override for a genuinely empty
    source.
    """
    if outcome.skipped:
        raise SnapshotIncomplete(
            f"COPY skipped {len(outcome.skipped)} file(s): {list(outcome.skipped[:5])}. "
            "With per-run prefixes this is never a re-load; the data did not arrive."
        )
    if expected_parts and not outcome.file_count:
        raise SnapshotIncomplete(
            f"expected {expected_parts} part(s) but the load history has no record of this "
            "run at all. Either the COPY never ran, or the run is older than the history "
            "window (COPY_HISTORY_LOOKBACK_HOURS). Either way the load is unverified, so the "
            "run stays incomplete."
        )
    if outcome.file_count < expected_parts:
        raise SnapshotIncomplete(
            f"expected {expected_parts} part(s), COPY reported {outcome.file_count}"
        )
    if outcome.rows_loaded < expected_rows:
        raise SnapshotIncomplete(
            f"expected {expected_rows} row(s), COPY loaded {outcome.rows_loaded}"
        )
    if snapshot_type == SNAPSHOT_FULL and expected_rows == 0 and not allow_empty:
        raise SnapshotIncomplete(
            "refusing to complete a full snapshot with zero rows -- downstream reads "
            "absence as deletion. Pass allow_empty if the source really is empty."
        )


def complete_snapshot_run(
    conn: Any,
    *,
    dataset: str,
    run_id: str,
    outcome: CopyOutcome,
    table: str = SNAPSHOT_RUNS_TABLE,
) -> None:
    """Stamp ``loaded_at``/``completed_at`` once the load has been reconciled.

    Call only after ``reconcile_snapshot`` has passed: this is the transition that makes the
    run visible to ``stg_payments``.
    """
    cursor = conn.cursor()
    try:
        cursor.execute(
            f"UPDATE {table} SET loaded_parts = %s, loaded_rows = %s, "
            f"loaded_at = CURRENT_TIMESTAMP(), completed_at = CURRENT_TIMESTAMP() "
            f"WHERE dataset = %s AND run_id = %s AND completed_at IS NULL",
            (outcome.file_count, outcome.rows_loaded, dataset, run_id),
        )
    finally:
        cursor.close()
    LOGGER.info("Completed %s run %s: %d rows in RAW", dataset, run_id, outcome.rows_loaded)


def connect_from_env() -> Any:
    """Open a Snowflake connection from SNOWFLAKE_* env vars.

    Auth: if ``SNOWFLAKE_PRIVATE_KEY_PATH`` is set, use **key-pair** auth (the production
    path -- Snowflake is deprecating single-factor passwords); otherwise fall back to
    ``SNOWFLAKE_PASSWORD``. The private key never transits the environment, only its path.

    The driver is imported here -- not at module top -- so unit tests that only exercise the
    SQL builders never require ``snowflake-connector-python``.
    """
    import snowflake.connector  # lazy: keeps the mocked test suite driver-free

    auth: dict[str, Any] = {}
    private_key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
    if private_key_path:
        # expanduser: the connector reads the file verbatim and would not resolve '~'
        auth["private_key_file"] = os.path.expanduser(private_key_path)
    else:
        LOGGER.warning("SNOWFLAKE_PRIVATE_KEY_PATH not set; falling back to password auth")
        auth["password"] = os.environ["SNOWFLAKE_PASSWORD"]

    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.getenv("SNOWFLAKE_ROLE", "PAYMENTS_ETL_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "PAYMENTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "PAYMENTS"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
        **auth,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="COPY S3-staged JSON into Snowflake RAW tables.")
    parser.add_argument(
        "--stage",
        default=os.getenv("SNOWFLAKE_STAGE", "PAYMENTS_LAKE_STAGE"),
        help="Snowflake external stage name (created by Terraform in Phase 6)",
    )
    parser.add_argument(
        "--run-date",
        default=dt.date.today().isoformat(),
        help="Partition date to load (YYYY-MM-DD); defaults to today",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=sorted(RAW_TABLES),
        default=sorted(RAW_TABLES),
        help="Which datasets to load (default: all)",
    )
    parser.add_argument(
        "--bootstrap",
        action="store_true",
        help="Only create the RAW objects dbt reads (landing tables + SNAPSHOT_RUNS), then "
        "exit. Idempotent; run it once against a fresh account before dbt.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the DDL + COPY statements without connecting to Snowflake",
    )
    args = parser.parse_args(argv)
    run_date = dt.date.fromisoformat(args.run_date)

    if args.dry_run:
        for dataset in args.datasets:
            table = RAW_TABLES[dataset]
            print(f"-- {dataset} -> {table}")
            print(create_raw_table_sql(table) + ";")
            print(copy_into_sql(table, args.stage, dataset, run_date) + ";\n")
        print(create_snapshot_runs_sql() + ";")
        return

    if args.bootstrap:
        conn = connect_from_env()
        try:
            bootstrap_raw_objects(conn)
        finally:
            conn.close()
        return

    conn = connect_from_env()
    try:
        for dataset in args.datasets:
            load_dataset(
                conn,
                table=RAW_TABLES[dataset],
                stage=args.stage,
                dataset=dataset,
                run_date=run_date,
            )
    finally:
        conn.close()


if __name__ == "__main__":  # pragma: no cover
    main()
