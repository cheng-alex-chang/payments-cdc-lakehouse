"""Minimal Trino client over its HTTP protocol, for pipeline scripts.

The pipeline scripts used to reach Trino with `docker exec dp-trino trino --execute ...`, which
names a Docker Compose container. That works in exactly one runtime. Kubernetes has no
`dp-trino`, so every Trino-touching Airflow task was Compose-only -- which is why the cluster ran
its Spark jobs through `kubectl patch` instead of through the DAG.

Talking to Trino over HTTP removes the branch entirely: Airflow resolves `trino:8080` by service
name under Compose and by Service DNS under Kubernetes, with identical code. The environment
contract matches api/src/trino_client.py so both halves of the repo are configured the same way.

`requests` rather than the `trino` driver deliberately: requests is already a CI dependency, and
the driver is intentionally absent there (see requirements-api.txt) so that the API's lazy-import
guarantee stays testable.

The protocol is a poll loop. POST the SQL to /v1/statement, then follow `nextUri` until it stops
coming back, accumulating rows and watching for an `error` object -- Trino reports failures in the
response body with HTTP 200, so a status check alone would silently pass over a failed query.
"""
from __future__ import annotations

import logging
import os
import time
from typing import Any

import requests

LOGGER = logging.getLogger(__name__)

# TRINO_HTTP_PORT, not TRINO_PORT. Kubernetes injects Docker-link-style variables for every
# Service in the namespace, so a Service named `trino` makes TRINO_PORT="tcp://10.96.15.110:8080"
# in every pod. Any name of the form <SERVICE>_PORT is effectively reserved, and the collision is
# invisible under Compose -- this only failed once the DAG ran on the cluster.
DEFAULTS = {
    "TRINO_HTTP_HOST": "trino",
    "TRINO_HTTP_PORT": "8080",
    "TRINO_USER": "airflow",
    "TRINO_CATALOG": "iceberg",
    "TRINO_SCHEMA": "analytics",
}

# Trino answers a poll immediately when results are ready and asks the client to back off when
# they are not; this is the pause between polls in the latter case.
POLL_SECONDS = 0.2
REQUEST_TIMEOUT = 60


def settings(env: dict[str, str] | None = None) -> dict[str, str]:
    source = os.environ if env is None else env
    resolved = {key: source.get(key, default) for key, default in DEFAULTS.items()}
    if not str(resolved["TRINO_HTTP_PORT"]).isdigit():
        raise ValueError(f"TRINO_HTTP_PORT must be numeric, got {resolved['TRINO_HTTP_PORT']!r}")
    return resolved


def statement_url(env: dict[str, str] | None = None) -> str:
    resolved = settings(env)
    return f"http://{resolved['TRINO_HTTP_HOST']}:{resolved['TRINO_HTTP_PORT']}/v1/statement"


def request_headers(env: dict[str, str] | None = None) -> dict[str, str]:
    resolved = settings(env)
    return {
        "X-Trino-User": resolved["TRINO_USER"],
        "X-Trino-Catalog": resolved["TRINO_CATALOG"],
        "X-Trino-Schema": resolved["TRINO_SCHEMA"],
    }


def split_statements(script: str) -> list[str]:
    """Split a .sql file into individual statements.

    The HTTP protocol accepts one statement per request, unlike the CLI's --file. Comment-only
    fragments are dropped so a trailing comment does not become an empty statement.
    """
    statements: list[str] = []
    for chunk in script.split(";"):
        lines = [
            line for line in chunk.splitlines()
            if line.strip() and not line.strip().startswith("--")
        ]
        if lines:
            statements.append("\n".join(lines).strip())
    return statements


def collect_rows(pages: list[dict[str, Any]]) -> list[list[Any]]:
    """Flatten the `data` blocks Trino returns across polls.

    Rows arrive spread over however many pages the coordinator chose; only some carry `data`.
    """
    rows: list[list[Any]] = []
    for page in pages:
        rows.extend(page.get("data") or [])
    return rows


def raise_for_query_error(page: dict[str, Any]) -> None:
    """Trino reports query failures in the body with HTTP 200, so this has to be explicit."""
    error = page.get("error")
    if not error:
        return
    message = error.get("message", "unknown Trino error")
    name = error.get("errorName", "")
    raise RuntimeError(f"Trino query failed [{name}]: {message}")


def run_statement(
    sql: str,
    env: dict[str, str] | None = None,
    session: Any | None = None,
) -> list[list[Any]]:
    """Execute one statement and return its rows."""
    http = session if session is not None else requests
    response = http.post(
        statement_url(env), data=sql.encode("utf-8"),
        headers=request_headers(env), timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    page = response.json()

    pages = [page]
    raise_for_query_error(page)

    while page.get("nextUri"):
        time.sleep(POLL_SECONDS)
        response = http.get(page["nextUri"], timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        page = response.json()
        raise_for_query_error(page)
        pages.append(page)

    return collect_rows(pages)


def run_script(script: str, env: dict[str, str] | None = None, session: Any | None = None) -> None:
    """Execute every statement in a .sql file, in order."""
    for statement in split_statements(script):
        LOGGER.info("%s", statement.splitlines()[0][:120])
        run_statement(statement, env=env, session=session)
