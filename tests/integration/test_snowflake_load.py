"""Real-Snowflake load checks (gated; skipped without SNOWFLAKE_* creds + driver).

The mocked sibling (tests/test_load_to_snowflake.py) proves the SQL builders and control flow
with a fake cursor and never needs the driver. This module exercises the live warehouse: a
bare connectivity smoke test, and -- once data has been staged and the external stage exists --
an actual COPY INTO.

Two reasons this lives in a separate, gated tier rather than CI:
* The Snowflake driver is heavy and only needed here, so it's an opt-in dependency
  (``pip install -r requirements-snowflake.txt``); ``importorskip`` skips cleanly without it.
* The free trial expires after 30 days, so this can never be a permanent CI gate -- it is a
  run-on-demand check for the cloud session.

Run with::

    pip install -r requirements-snowflake.txt
    SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=... \
    SNOWFLAKE_STAGE=PAYMENTS_LAKE_STAGE pytest -m integration tests/integration/test_snowflake_load.py
"""
from __future__ import annotations

import datetime as dt
import os

import pytest

pytestmark = pytest.mark.integration

# Connectivity needs account/user/password; the COPY also needs a stage pointing at staged data.
# Key-pair is the production auth path (Snowflake is deprecating single-factor passwords),
# and load_to_snowflake.connect_from_env prefers it. Gating these tests on SNOWFLAKE_PASSWORD
# alone meant they skipped silently on exactly the credentials a real deployment uses.
_CRED_VARS = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER")
_HAS_AUTH = bool(os.getenv("SNOWFLAKE_PASSWORD") or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"))
_HAS_CREDS = all(os.getenv(v) for v in _CRED_VARS) and _HAS_AUTH
_HAS_STAGE = _HAS_CREDS and bool(os.getenv("SNOWFLAKE_STAGE"))

_needs_creds = pytest.mark.skipif(
    not _HAS_CREDS, reason="set SNOWFLAKE_ACCOUNT/USER plus PASSWORD or PRIVATE_KEY_PATH for the live tests"
)
_needs_stage = pytest.mark.skipif(
    not _HAS_STAGE, reason="set SNOWFLAKE_STAGE (and creds) plus stage data to run the live COPY"
)


@_needs_creds
def test_can_connect_and_query_version() -> None:
    pytest.importorskip("snowflake.connector")
    from snowflake_etl.src import load_to_snowflake as loader

    conn = loader.connect_from_env()
    try:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT CURRENT_VERSION()")
            assert cursor.fetchone()[0]  # non-empty version string proves creds + warehouse
        finally:
            cursor.close()
    finally:
        conn.close()


@_needs_stage
def test_copy_fx_rates_into_raw_table() -> None:
    pytest.importorskip("snowflake.connector")
    from snowflake_etl.src import load_to_snowflake as loader

    run_date = dt.date.fromisoformat(os.getenv("RUN_DATE", dt.date.today().isoformat()))
    conn = loader.connect_from_env()
    try:
        loaded = loader.load_dataset(
            conn,
            table=loader.RAW_TABLES["fx_rates"],
            stage=os.environ["SNOWFLAKE_STAGE"],
            dataset="fx_rates",
            run_date=run_date,
        )
        # load_dataset reports a CopyOutcome, not a bare row count: the snapshot contract
        # needs to know how many files were touched and whether any were skipped, not just
        # a sum. A first run loads > 0 rows; a replay over an unchanged partition loads 0
        # and reports the files as skipped -- both are valid here.
        assert loaded.rows_loaded >= 0
        assert loaded.file_count >= len(loaded.skipped)
    finally:
        conn.close()
