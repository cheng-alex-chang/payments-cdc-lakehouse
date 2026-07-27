"""Guards for Trino connection handling (api/src/trino_client.py).

The load-bearing property is that importing the API does not import the Trino driver. That is what
keeps `trino` out of requirements-ci.txt and lets the whole API suite run in CI with no driver and
no warehouse.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from api.src import trino_client

MODULE_SOURCE = Path(__file__).resolve().parents[1] / "api" / "src" / "trino_client.py"


def test_driver_is_not_imported_at_module_scope() -> None:
    # A top-level `import trino` would break every CI job, since the driver is intentionally not
    # installed there. Asserting on the source keeps the guard honest even when a previous test
    # has already pulled the driver into sys.modules locally.
    lines = MODULE_SOURCE.read_text(encoding="utf-8").splitlines()
    top_level_imports = [line for line in lines if line.startswith(("import ", "from "))]

    assert not any("trino" in line for line in top_level_imports)
    assert any(line.strip().startswith("import trino") for line in lines), "expected a lazy import"


def test_defaults_target_the_in_cluster_service() -> None:
    settings = trino_client.connection_settings(env={})

    assert settings == {
        "host": "trino",
        "port": 8080,
        "user": "api",
        "catalog": "iceberg",
        "schema": "analytics",
    }


def test_environment_overrides_every_default() -> None:
    settings = trino_client.connection_settings(
        env={
            "TRINO_HOST": "localhost",
            "TRINO_PORT": "9999",
            "TRINO_USER": "bench",
            "TRINO_CATALOG": "hive",
            "TRINO_SCHEMA": "staging",
        }
    )

    assert settings["host"] == "localhost"
    assert settings["port"] == 9999
    assert settings["user"] == "bench"
    assert settings["catalog"] == "hive"
    assert settings["schema"] == "staging"


def test_non_numeric_port_fails_loudly() -> None:
    # Better to fail at startup than to surface as a confusing driver error on first request.
    with pytest.raises(ValueError, match="TRINO_PORT"):
        trino_client.connection_settings(env={"TRINO_PORT": "eight-thousand"})


def test_run_query_binds_parameters_rather_than_formatting_them() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [("row",)]
    connection = MagicMock()
    connection.cursor.return_value = cursor

    rows = trino_client.run_query(connection, "SELECT ? ", ["value"])

    assert rows == [("row",)]
    cursor.execute.assert_called_once_with("SELECT ? ", ["value"])


def test_run_query_omits_the_parameter_list_when_there_is_nothing_to_bind() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    connection = MagicMock()
    connection.cursor.return_value = cursor

    trino_client.run_query(connection, "SELECT 1", [])

    cursor.execute.assert_called_once_with("SELECT 1")


def test_cursor_is_closed_even_when_the_query_raises() -> None:
    cursor = MagicMock()
    cursor.execute.side_effect = RuntimeError("query failed")
    connection = MagicMock()
    connection.cursor.return_value = cursor

    with pytest.raises(RuntimeError):
        trino_client.run_query(connection, "SELECT 1", [])

    cursor.close.assert_called_once()
