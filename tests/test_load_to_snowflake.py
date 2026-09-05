from __future__ import annotations

import datetime as dt
import pytest
from unittest import mock

from snowflake_etl.src import load_to_snowflake as module


def test_create_raw_table_sql_is_idempotent_variant_landing() -> None:
    sql = module.create_raw_table_sql("RAW.RAW_FX_RATES")

    assert "CREATE TABLE IF NOT EXISTS RAW.RAW_FX_RATES" in sql  # safe to re-run
    assert "raw         VARIANT" in sql                          # whole JSON line, untyped
    assert "source_file STRING" in sql                           # lineage column
    assert "loaded_at   TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()" in sql


def test_copy_into_sql_targets_partitioned_stage_path() -> None:
    sql = module.copy_into_sql(
        "RAW.RAW_FX_RATES", "PAYMENTS_LAKE_STAGE", "fx_rates", dt.date(2026, 6, 29)
    )

    # Stage location must mirror the Phase-2 S3 key layout exactly.
    assert "@RAW.PAYMENTS_LAKE_STAGE/raw/fx_rates/dt=2026-06-29/" in sql
    assert "COPY INTO RAW.RAW_FX_RATES (raw, source_file)" in sql
    assert "SELECT $1, METADATA$FILENAME" in sql                 # raw VARIANT + lineage
    assert "TYPE = JSON" in sql
    assert "ON_ERROR = ABORT_STATEMENT" in sql                   # fail loud on bad rows


def test_copy_into_sql_accepts_templated_run_date() -> None:
    # The Airflow DAG passes "{{ ds }}" so SnowflakeOperator renders the partition at runtime.
    sql = module.copy_into_sql("RAW.RAW_PAYMENTS", "PAYMENTS_LAKE_STAGE", "payments", "{{ ds }}")
    assert "@RAW.PAYMENTS_LAKE_STAGE/raw/payments/dt={{ ds }}/" in sql


def test_parse_copy_result_reports_files_rows_and_skips() -> None:
    """Skips are surfaced, not summed away.

    With per-run prefixes every run stages a distinct set of files, so a LOAD_SKIPPED is
    never the re-load path -- it means the load metadata already accounted for the file (the
    64-day window) and the data silently did not arrive.
    """
    result = [
        ("fx-a.jsonl", "LOADED", 1536, 1536, 0, None),
        ("fx-b.jsonl", "LOAD_SKIPPED", None, None, None, None),
        ("fx-c.jsonl", "LOADED", 10, 10, 0, None),
    ]

    outcome = module.parse_copy_result(result)

    assert outcome.rows_loaded == 1546
    assert outcome.file_count == 3
    assert outcome.skipped == ("fx-b.jsonl",)


def test_reconcile_accepts_a_load_that_accounts_for_everything_staged() -> None:
    outcome = module.CopyOutcome(files=("p1", "p2"), rows_loaded=100, skipped=())

    module.reconcile_snapshot(
        expected_parts=2, expected_rows=100, outcome=outcome,
        snapshot_type=module.SNAPSHOT_FULL,
    )


@pytest.mark.parametrize(
    "expected_parts, expected_rows, outcome, match",
    [
        # A COPY that loaded fewer files than staging wrote: the classic partial load.
        (3, 100, module.CopyOutcome(("p1", "p2"), 100, ()), "expected 3 part"),
        # Files all present but not fully parsed.
        (2, 100, module.CopyOutcome(("p1", "p2"), 61, ()), "expected 100 row"),
        # A skip means the rows never arrived.
        (2, 100, module.CopyOutcome(("p1", "p2"), 100, ("p2",)), "skipped"),
    ],
)
def test_reconcile_refuses_an_incomplete_load(expected_parts, expected_rows, outcome, match) -> None:  # noqa: ANN001
    with pytest.raises(module.SnapshotIncomplete, match=match):
        module.reconcile_snapshot(
            expected_parts=expected_parts, expected_rows=expected_rows,
            outcome=outcome, snapshot_type=module.SNAPSHOT_FULL,
        )


def test_reconcile_refuses_an_empty_full_snapshot() -> None:
    """Downstream reads absence as deletion, so an empty full snapshot empties the fact.

    Indistinguishable from an extract that broke, so it takes a deliberate override.
    """
    empty = module.CopyOutcome(files=("p1",), rows_loaded=0, skipped=())

    with pytest.raises(module.SnapshotIncomplete, match="zero rows"):
        module.reconcile_snapshot(
            expected_parts=1, expected_rows=0, outcome=empty,
            snapshot_type=module.SNAPSHOT_FULL,
        )

    module.reconcile_snapshot(
        expected_parts=1, expected_rows=0, outcome=empty,
        snapshot_type=module.SNAPSHOT_FULL, allow_empty=True,
    )
    # A windowed run legitimately has nothing in it; it never drives deletion.
    module.reconcile_snapshot(
        expected_parts=1, expected_rows=0, outcome=empty,
        snapshot_type=module.SNAPSHOT_WINDOW,
    )


def test_register_rejects_an_unknown_snapshot_type() -> None:
    with pytest.raises(ValueError, match="snapshot_type"):
        module.register_snapshot_run(
            mock.MagicMock(), dataset="payments", run_id="r1",
            run_date=dt.date(2026, 6, 29), snapshot_type="delta",
            expected_parts=1, expected_rows=1,
        )


def test_completion_is_a_separate_step_from_registration() -> None:
    """completed_at means "in RAW", not "in S3" -- so it cannot be set at registration."""
    cursor = mock.MagicMock()
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    module.register_snapshot_run(
        conn, dataset="payments", run_id="r1", run_date=dt.date(2026, 6, 29),
        snapshot_type=module.SNAPSHOT_FULL, expected_parts=2, expected_rows=100,
    )

    # MERGE, not INSERT: the calling Airflow task has retries=3, so registering the same
    # run twice must update one row rather than create a second.
    register = [c.args[0] for c in cursor.execute.call_args_list if "MERGE INTO" in c.args[0]]
    assert len(register) == 1
    assert "t.dataset = s.dataset AND t.run_id = s.run_id" in register[0]
    assert "staged_at" in register[0]
    assert "completed_at" not in register[0]

    cursor.reset_mock()
    module.complete_snapshot_run(
        conn, dataset="payments", run_id="r1",
        outcome=module.CopyOutcome(("p1", "p2"), 100, ()),
    )
    update = cursor.execute.call_args_list[0].args[0]
    assert "completed_at = CURRENT_TIMESTAMP()" in update
    assert "completed_at IS NULL" in update  # never re-complete a finished run


def test_load_dataset_runs_ddl_then_copy_and_returns_rows() -> None:
    cursor = mock.MagicMock()
    cursor.fetchall.return_value = [("payments.jsonl", "LOADED", 50004, 50004, 0, None)]
    conn = mock.MagicMock()
    conn.cursor.return_value = cursor

    loaded = module.load_dataset(
        conn,
        table="RAW.RAW_PAYMENTS",
        stage="PAYMENTS_LAKE_STAGE",
        dataset="payments",
        run_date=dt.date(2026, 6, 29),
    )

    assert loaded.rows_loaded == 50004
    assert loaded.file_count == 1
    # DDL first, then COPY -- in that order.
    executed = [call.args[0] for call in cursor.execute.call_args_list]
    assert executed[0].startswith("CREATE TABLE IF NOT EXISTS RAW.RAW_PAYMENTS")
    assert executed[1].startswith("COPY INTO RAW.RAW_PAYMENTS")
    cursor.close.assert_called_once()  # cursor always closed, even though we asserted success


def test_connect_uses_keypair_when_private_key_path_set(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}
    connector = mock.MagicMock()
    connector.connect = lambda **kw: captured.update(kw) or "CONN"
    monkeypatch.setitem(__import__("sys").modules, "snowflake", mock.MagicMock(connector=connector))
    monkeypatch.setitem(__import__("sys").modules, "snowflake.connector", connector)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ORG-ACCT")
    monkeypatch.setenv("SNOWFLAKE_USER", "u")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/keys/rsa_key.p8")
    monkeypatch.delenv("SNOWFLAKE_PASSWORD", raising=False)

    assert module.connect_from_env() == "CONN"
    assert captured["private_key_file"] == "/keys/rsa_key.p8"
    assert "password" not in captured  # key-pair path never sends a password


def test_connect_falls_back_to_password_without_key(monkeypatch) -> None:  # noqa: ANN001
    captured: dict = {}
    connector = mock.MagicMock()
    connector.connect = lambda **kw: captured.update(kw) or "CONN"
    monkeypatch.setitem(__import__("sys").modules, "snowflake", mock.MagicMock(connector=connector))
    monkeypatch.setitem(__import__("sys").modules, "snowflake.connector", connector)
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "ORG-ACCT")
    monkeypatch.setenv("SNOWFLAKE_USER", "u")
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "pw")
    monkeypatch.delenv("SNOWFLAKE_PRIVATE_KEY_PATH", raising=False)

    assert module.connect_from_env() == "CONN"
    assert captured["password"] == "pw"
    assert "private_key_file" not in captured


def test_main_dry_run_prints_sql_without_connecting(capsys) -> None:  # noqa: ANN001
    # connect_from_env would raise if called (no creds / no driver); dry-run must not call it.
    with mock.patch.object(module, "connect_from_env", side_effect=AssertionError("connected!")):
        module.main(["--dry-run", "--datasets", "fx_rates", "--run-date", "2026-06-29"])

    out = capsys.readouterr().out
    assert "CREATE TABLE IF NOT EXISTS RAW.RAW_FX_RATES" in out
    assert "@RAW.PAYMENTS_LAKE_STAGE/raw/fx_rates/dt=2026-06-29/" in out


def test_copy_into_qualifies_a_bare_stage_name() -> None:
    """An unqualified stage resolves against the session schema, not its own.

    dbt connects with SNOWFLAKE_SCHEMA=ANALYTICS while the stage lives in RAW, so a bare
    name made the COPY fail with "PAYMENTS.ANALYTICS.PAYMENTS_LAKE_STAGE does not exist"
    depending entirely on who opened the connection.
    """
    sql = module.copy_into_sql("RAW.RAW_PAYMENTS", "PAYMENTS_LAKE_STAGE", "payments",
                               dt.date(2026, 6, 29))
    assert "@RAW.PAYMENTS_LAKE_STAGE/" in sql


def test_copy_into_leaves_an_already_qualified_stage_alone() -> None:
    sql = module.copy_into_sql("RAW.RAW_PAYMENTS", "OTHER.MY_STAGE", "payments",
                               dt.date(2026, 6, 29))
    assert "@OTHER.MY_STAGE/" in sql
    assert "RAW.OTHER" not in sql


def test_copy_history_window_outlives_a_backfill() -> None:
    """24h would fail a backfill of an older partition on missing history, not missing data."""
    assert module.COPY_HISTORY_LOOKBACK_HOURS >= 24 * 7
    # Snowflake's COPY_HISTORY only retains 14 days; a wider window would query nothing extra.
    assert module.COPY_HISTORY_LOOKBACK_HOURS <= 24 * 14
    assert f"-{module.COPY_HISTORY_LOOKBACK_HOURS}" in module.copy_history_sql("T", "r1")


def test_no_history_at_all_is_reported_as_unverified() -> None:
    """Distinct from a partial load: nothing was found, so nothing can be vouched for."""
    with pytest.raises(module.SnapshotIncomplete, match="no record of this run"):
        module.reconcile_snapshot(
            expected_parts=2, expected_rows=10,
            outcome=module.CopyOutcome(files=(), rows_loaded=0, skipped=()),
            snapshot_type=module.SNAPSHOT_FULL,
        )
