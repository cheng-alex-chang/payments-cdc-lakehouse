"""Trino connection handling for the serving API.

The `trino` driver is imported lazily inside `connect_from_env` rather than at module scope. That
is what lets every unit test import the app, the routers, and the SQL builders without the driver
installed and without a Trino server running -- the same pattern
`snowflake_etl/src/load_to_snowflake.py` uses for the Snowflake driver.
"""
from __future__ import annotations

import os
from typing import Any

DEFAULTS = {
    "TRINO_HOST": "trino",
    "TRINO_PORT": "8080",
    "TRINO_USER": "api",
    "TRINO_CATALOG": "iceberg",
    "TRINO_SCHEMA": "analytics",
}


def connection_settings(env: dict[str, str] | None = None) -> dict[str, Any]:
    """Resolve connection settings from the environment, applying defaults.

    Pure and env-injectable so the resolution rules are testable without touching os.environ.
    """
    source = os.environ if env is None else env
    settings = {key: source.get(key, default) for key, default in DEFAULTS.items()}

    port = settings["TRINO_PORT"]
    if not str(port).isdigit():
        raise ValueError(f"TRINO_PORT must be numeric, got {port!r}")

    return {
        "host": settings["TRINO_HOST"],
        "port": int(port),
        "user": settings["TRINO_USER"],
        "catalog": settings["TRINO_CATALOG"],
        "schema": settings["TRINO_SCHEMA"],
    }


def connect_from_env(env: dict[str, str] | None = None) -> Any:
    """Open a Trino connection. Imports the driver lazily -- see the module docstring."""
    import trino  # noqa: PLC0415

    settings = connection_settings(env)
    return trino.dbapi.connect(**settings)


def run_query(connection: Any, sql: str, params: list[Any] | None = None) -> list[tuple[Any, ...]]:
    """Execute one statement and return all rows.

    Parameters are bound by the driver rather than interpolated, so user-supplied filter values
    can never alter the statement's shape.
    """
    cursor = connection.cursor()
    try:
        if params:
            cursor.execute(sql, params)
        else:
            cursor.execute(sql)
        return cursor.fetchall()
    finally:
        cursor.close()
