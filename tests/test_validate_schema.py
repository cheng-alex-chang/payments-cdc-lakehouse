from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from scripts.validate_schema import (
    TRACKED_TABLES,
    check_columns,
    fetch_postgres_columns,
    main,
)

_PAYMENTS_SILVER, _PAYMENTS_EXCLUDED = TRACKED_TABLES["payments"]
_MERCHANTS_SILVER, _MERCHANTS_EXCLUDED = TRACKED_TABLES["merchants"]
_REFUNDS_SILVER, _REFUNDS_EXCLUDED = TRACKED_TABLES["refunds"]


# ---------------------------------------------------------------------------
# check_columns
# ---------------------------------------------------------------------------

def test_check_columns_passes_when_all_accounted_for() -> None:
    known = _PAYMENTS_SILVER | _PAYMENTS_EXCLUDED
    assert check_columns(known, _PAYMENTS_SILVER, _PAYMENTS_EXCLUDED) == []


def test_check_columns_detects_new_column() -> None:
    known = _PAYMENTS_SILVER | _PAYMENTS_EXCLUDED
    unmapped = check_columns(known | {"risk_score"}, _PAYMENTS_SILVER, _PAYMENTS_EXCLUDED)
    assert unmapped == ["risk_score"]


def test_check_columns_detects_multiple_new_columns() -> None:
    known = _PAYMENTS_SILVER | _PAYMENTS_EXCLUDED
    unmapped = check_columns(
        known | {"risk_score", "device_fingerprint"},
        _PAYMENTS_SILVER,
        _PAYMENTS_EXCLUDED,
    )
    assert unmapped == ["device_fingerprint", "risk_score"]


def test_check_columns_empty_when_subset() -> None:
    assert check_columns(frozenset({"payment_id", "amount"}), _PAYMENTS_SILVER, _PAYMENTS_EXCLUDED) == []


def test_check_columns_merchants_passes() -> None:
    known = _MERCHANTS_SILVER | _MERCHANTS_EXCLUDED
    assert check_columns(known, _MERCHANTS_SILVER, _MERCHANTS_EXCLUDED) == []


def test_check_columns_refunds_passes() -> None:
    known = _REFUNDS_SILVER | _REFUNDS_EXCLUDED
    assert check_columns(known, _REFUNDS_SILVER, _REFUNDS_EXCLUDED) == []


# ---------------------------------------------------------------------------
# fetch_postgres_columns
# ---------------------------------------------------------------------------

def test_fetch_postgres_columns_returns_column_names() -> None:
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("payment_id",), ("amount",), ("currency",)]
    mock_conn.cursor.return_value.__enter__ = lambda _: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("scripts.validate_schema.psycopg2.connect", return_value=mock_conn):
        result = fetch_postgres_columns("postgresql://user:pass@host:5432/payments", "payments")

    assert result == frozenset({"payment_id", "amount", "currency"})
    mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# main — end-to-end
# ---------------------------------------------------------------------------

def _all_known_columns(table: str) -> frozenset[str]:
    silver, excluded = TRACKED_TABLES[table]
    return silver | excluded


def test_main_passes_when_all_tables_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SOURCE_POSTGRES", "postgresql://u:p@host/db")
    monkeypatch.setattr(
        "scripts.validate_schema.fetch_postgres_columns",
        lambda _uri, table: _all_known_columns(table),
    )
    main()  # must not raise


def test_main_raises_on_unmapped_payments_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SOURCE_POSTGRES", "postgresql://u:p@host/db")
    monkeypatch.setattr(
        "scripts.validate_schema.fetch_postgres_columns",
        lambda _uri, table: _all_known_columns(table) | ({"risk_score"} if table == "payments" else frozenset()),
    )
    with pytest.raises(SystemExit, match="risk_score"):
        main()


def test_main_raises_on_unmapped_merchants_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SOURCE_POSTGRES", "postgresql://u:p@host/db")
    monkeypatch.setattr(
        "scripts.validate_schema.fetch_postgres_columns",
        lambda _uri, table: _all_known_columns(table) | ({"merchant_tier"} if table == "merchants" else frozenset()),
    )
    with pytest.raises(SystemExit, match="merchant_tier"):
        main()


def test_main_raises_on_unmapped_refunds_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SOURCE_POSTGRES", "postgresql://u:p@host/db")
    monkeypatch.setattr(
        "scripts.validate_schema.fetch_postgres_columns",
        lambda _uri, table: _all_known_columns(table) | ({"refund_status"} if table == "refunds" else frozenset()),
    )
    with pytest.raises(SystemExit, match="refund_status"):
        main()


def test_main_reports_all_drifted_tables(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SOURCE_POSTGRES", "postgresql://u:p@host/db")
    monkeypatch.setattr(
        "scripts.validate_schema.fetch_postgres_columns",
        lambda _uri, table: _all_known_columns(table) | {"new_col"},
    )
    with pytest.raises(SystemExit) as exc_info:
        main()
    msg = str(exc_info.value)
    assert "payments" in msg
    assert "merchants" in msg
    assert "refunds" in msg
