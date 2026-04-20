from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch

from scripts.validate_schema import (
    EXCLUDED_COLUMNS,
    SILVER_SOURCE_COLUMNS,
    check_columns,
    fetch_postgres_columns,
    main,
)


def test_check_columns_passes_when_all_accounted_for() -> None:
    known = SILVER_SOURCE_COLUMNS | EXCLUDED_COLUMNS
    assert check_columns(known) == []


def test_check_columns_detects_new_column() -> None:
    known = SILVER_SOURCE_COLUMNS | EXCLUDED_COLUMNS
    unmapped = check_columns(known | {"new_risk_score"})
    assert unmapped == ["new_risk_score"]


def test_check_columns_detects_multiple_new_columns() -> None:
    known = SILVER_SOURCE_COLUMNS | EXCLUDED_COLUMNS
    unmapped = check_columns(known | {"risk_score", "device_fingerprint"})
    assert unmapped == ["device_fingerprint", "risk_score"]


def test_check_columns_empty_when_subset() -> None:
    subset = frozenset({"payment_id", "amount"})
    assert check_columns(subset) == []


def test_fetch_postgres_columns_returns_column_names() -> None:
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.fetchall.return_value = [("payment_id",), ("amount",), ("currency",)]
    mock_conn.cursor.return_value.__enter__ = lambda _: mock_cursor
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    with patch("scripts.validate_schema.psycopg2.connect", return_value=mock_conn):
        result = fetch_postgres_columns("postgresql://user:pass@host:5432/payments")

    assert result == frozenset({"payment_id", "amount", "currency"})
    mock_conn.close.assert_called_once()


def test_main_passes_on_known_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SOURCE_POSTGRES", "postgresql://u:p@host/db")
    monkeypatch.setattr(
        "scripts.validate_schema.fetch_postgres_columns",
        lambda _uri: SILVER_SOURCE_COLUMNS | EXCLUDED_COLUMNS,
    )
    main()  # must not raise


def test_main_raises_on_unmapped_column(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AIRFLOW_CONN_SOURCE_POSTGRES", "postgresql://u:p@host/db")
    monkeypatch.setattr(
        "scripts.validate_schema.fetch_postgres_columns",
        lambda _uri: SILVER_SOURCE_COLUMNS | EXCLUDED_COLUMNS | {"risk_score"},
    )
    with pytest.raises(SystemExit, match="risk_score"):
        main()
