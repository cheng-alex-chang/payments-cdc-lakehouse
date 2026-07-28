"""Guards for the scripts' Trino HTTP client (scripts/trino_http.py).

This module exists so pipeline tasks stop naming a Docker Compose container. The properties worth
pinning are the ones whose failure is silent: Trino reports query errors in the response body with
HTTP 200, and its rows arrive spread across however many polls the coordinator chose.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from scripts import trino_http


class FakeResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class FakeSession:
    """Replays a scripted sequence of Trino protocol pages."""

    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = list(pages)
        self.posted: list[tuple[str, bytes, dict[str, str]]] = []
        self.gets: list[str] = []

    def post(self, url: str, data: bytes, headers: dict[str, str], timeout: int) -> FakeResponse:
        self.posted.append((url, data, headers))
        return FakeResponse(self._pages.pop(0))

    def get(self, url: str, timeout: int) -> FakeResponse:
        self.gets.append(url)
        return FakeResponse(self._pages.pop(0))


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(trino_http.time, "sleep", lambda _seconds: None)


def test_scripts_import_trino_http_in_a_container_safe_way() -> None:
    """Airflow mounts these scripts as loose files with no `scripts` package parent.

    A bare `from scripts import trino_http` resolves in a repo checkout and raises ImportError in
    the container -- which is exactly how it failed the first time the DAG ran on Kubernetes.
    Each consumer needs the flat-import fallback.
    """
    root = Path(__file__).resolve().parents[1] / "scripts"
    for name in ("publish_trino_tables.py", "validate_trino.py", "maintain_iceberg.py"):
        source = (root / name).read_text(encoding="utf-8")

        assert "from scripts import trino_http" in source, name
        assert "import trino_http" in source.split("except ImportError")[1], name


def test_defaults_target_the_in_cluster_service() -> None:
    # "trino" resolves to the Compose service name and the Kubernetes Service DNS name alike --
    # that identity is the whole point of this module.
    assert trino_http.statement_url(env={}) == "http://trino:8080/v1/statement"


def test_environment_overrides_host_and_port() -> None:
    url = trino_http.statement_url(env={"TRINO_HTTP_HOST": "localhost", "TRINO_HTTP_PORT": "9090"})

    assert url == "http://localhost:9090/v1/statement"


def test_non_numeric_port_fails_loudly() -> None:
    with pytest.raises(ValueError, match="TRINO_HTTP_PORT"):
        trino_http.statement_url(env={"TRINO_HTTP_PORT": "eighty-eighty"})


def test_headers_carry_user_catalog_and_schema() -> None:
    headers = trino_http.request_headers(env={})

    assert headers["X-Trino-User"] == "airflow"
    assert headers["X-Trino-Catalog"] == "iceberg"
    assert headers["X-Trino-Schema"] == "analytics"


def test_query_error_raises_even_though_the_response_was_http_200() -> None:
    # The trap this module exists to avoid: Trino signals failure in the body, so checking the
    # status code alone reports a failed query as a successful pipeline task.
    page = {"error": {"errorName": "TABLE_NOT_FOUND", "message": "line 1:15: Table not found"}}

    with pytest.raises(RuntimeError, match="TABLE_NOT_FOUND"):
        trino_http.raise_for_query_error(page)


def test_absent_error_key_is_not_a_failure() -> None:
    trino_http.raise_for_query_error({"data": [[1]]})


def test_rows_are_collected_across_every_poll() -> None:
    # Only some pages carry `data`; dropping the later ones would silently truncate results.
    pages = [
        {"columns": [{"name": "n"}]},
        {"data": [[1], [2]]},
        {},
        {"data": [[3]]},
    ]

    assert trino_http.collect_rows(pages) == [[1], [2], [3]]


def test_run_statement_follows_next_uri_until_it_stops() -> None:
    session = FakeSession([
        {"nextUri": "http://trino:8080/v1/statement/x/1"},
        {"data": [["payments_bronze"]], "nextUri": "http://trino:8080/v1/statement/x/2"},
        {"data": [["payments_silver"]]},
    ])

    rows = trino_http.run_statement("SHOW TABLES", env={}, session=session)

    assert rows == [["payments_bronze"], ["payments_silver"]]
    assert session.gets == [
        "http://trino:8080/v1/statement/x/1",
        "http://trino:8080/v1/statement/x/2",
    ]


def test_run_statement_posts_the_sql_as_the_body() -> None:
    session = FakeSession([{"data": [[1]]}])

    trino_http.run_statement("SELECT 1", env={}, session=session)

    url, body, headers = session.posted[0]
    assert url == "http://trino:8080/v1/statement"
    assert body == b"SELECT 1"
    assert headers["X-Trino-User"] == "airflow"


def test_run_statement_raises_on_an_error_page_mid_poll() -> None:
    session = FakeSession([
        {"nextUri": "http://trino:8080/v1/statement/x/1"},
        {"error": {"errorName": "EXCEEDED_MEMORY_LIMIT", "message": "Query exceeded memory"}},
    ])

    with pytest.raises(RuntimeError, match="EXCEEDED_MEMORY_LIMIT"):
        trino_http.run_statement("SELECT 1", env={}, session=session)


def test_script_is_split_into_individual_statements() -> None:
    # The HTTP protocol takes one statement per request, unlike the CLI's --file.
    script = "SELECT 1;\nSELECT 2;\n"

    assert trino_http.split_statements(script) == ["SELECT 1", "SELECT 2"]


def test_comment_only_fragments_are_dropped() -> None:
    # A trailing comment after the final semicolon would otherwise become an empty statement and
    # make Trino reject the request.
    script = "-- reconcile the layers\nSELECT 1;\n-- done\n"

    assert trino_http.split_statements(script) == ["SELECT 1"]


def test_inline_comments_above_a_statement_are_stripped_but_sql_survives() -> None:
    script = "-- count bronze\nSELECT count(*)\nFROM t;"

    assert trino_http.split_statements(script) == ["SELECT count(*)\nFROM t"]


def test_run_script_executes_each_statement_in_order(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(
        trino_http, "run_statement",
        lambda sql, env=None, session=None: calls.append(sql),
    )

    trino_http.run_script("SELECT 1;\nSELECT 2;", env={})

    assert calls == ["SELECT 1", "SELECT 2"]
