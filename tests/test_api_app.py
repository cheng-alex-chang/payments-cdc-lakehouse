"""Endpoint guards for the gold serving API (api/src/app.py).

Runs entirely offline. Instead of stubbing GoldRepository wholesale, these tests inject a fake
DBAPI connection underneath the real repository, so the code actually under test -- SQL building,
snapshot caching, row mapping, cursor emission -- is the production code path. Only the driver is
fake.
"""
from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any

import pytest
from fastapi.testclient import TestClient

from api.src.app import GoldRepository, app, get_repository
from api.src.cache import SnapshotCache

COUNTRIES = ("NL", "US")
METHODS = ("card", "ideal")


def _gold_rows(hours: int = 6) -> list[tuple[Any, ...]]:
    """Deterministic gold rows in SORT_KEYS order (payment_hour, country_code, payment_method)."""
    base = datetime(2026, 3, 1, 0, 0, 0)
    rows: list[tuple[Any, ...]] = []
    for hour in range(hours):
        for country in COUNTRIES:
            for method in METHODS:
                rows.append(
                    (
                        base + timedelta(hours=hour),
                        country,
                        method,
                        10 + hour,
                        Decimal(f"{100 + hour}.50"),
                        0.9,
                    )
                )
    return rows


class FakeCursor:
    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state
        self._rows: list[tuple[Any, ...]] = []

    def execute(self, sql: str, params: list[Any] | None = None) -> None:
        self._state["queries"].append((sql, params or []))
        if "$snapshots" in sql:
            self._rows = [(self._state["snapshot_id"],)] if self._state["snapshot_id"] else []
        elif "count(*) AS bucket_count" in sql:
            self._rows = [self._state["summary_row"]]
        else:
            self._rows = self._state["page_fn"](sql, params or [])

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        return None


class FakeConnection:
    def __init__(self, state: dict[str, Any]) -> None:
        self._state = state

    def cursor(self) -> FakeCursor:
        return FakeCursor(self._state)

    def close(self) -> None:
        return None


def _paged(rows: list[tuple[Any, ...]]) -> Any:
    """Emulate the keyset page the warehouse would return for a given SQL + params."""

    def page_fn(sql: str, params: list[Any]) -> list[tuple[Any, ...]]:
        limit = int(sql.rsplit("LIMIT ", 1)[1].strip())
        candidates = rows
        if "payment_hour > ?" in sql:
            after = (params[-3], params[-2], params[-1])
            candidates = [row for row in rows if (row[0], row[1], row[2]) > after]
        return candidates[:limit]

    return page_fn


def _client(
    rows: list[tuple[Any, ...]] | None = None,
    snapshot_id: str | None = "snap-1",
    summary_row: tuple[Any, ...] = (24, 240, Decimal("2412.00"), 0.9),
    connection_error: Exception | None = None,
) -> tuple[TestClient, dict[str, Any], GoldRepository]:
    state: dict[str, Any] = {
        "queries": [],
        "snapshot_id": snapshot_id,
        "summary_row": summary_row,
        "page_fn": _paged(rows if rows is not None else _gold_rows()),
    }

    def factory() -> FakeConnection:
        if connection_error is not None:
            raise connection_error
        return FakeConnection(state)

    repository = GoldRepository(connection_factory=factory, cache=SnapshotCache())
    app.dependency_overrides[get_repository] = lambda: repository
    client = TestClient(app)
    return client, state, repository


@pytest.fixture(autouse=True)
def _clear_overrides() -> Any:
    yield
    app.dependency_overrides.clear()


def test_hourly_returns_rows_and_the_snapshot_they_came_from() -> None:
    client, _, _ = _client()

    body = client.get("/v1/metrics/hourly", params={"limit": 5}).json()

    assert len(body["data"]) == 5
    assert body["snapshot_id"] == "snap-1"
    assert body["data"][0]["country_code"] == "NL"
    # Money crosses the wire as a string, not a float.
    assert body["data"][0]["gross_volume"] == "100.50"


def test_filters_reach_the_warehouse_as_predicates() -> None:
    client, state, _ = _client()

    client.get("/v1/metrics/hourly", params={"country_code": "NL", "payment_method": "ideal"})

    sql, params = state["queries"][-1]
    assert "country_code = ?" in sql
    assert "payment_method = ?" in sql
    assert "NL" in params and "ideal" in params


def test_last_page_has_no_next_cursor() -> None:
    client, _, _ = _client(rows=_gold_rows(hours=1))  # exactly 4 rows

    body = client.get("/v1/metrics/hourly", params={"limit": 10}).json()

    assert len(body["data"]) == 4
    assert body["next_cursor"] is None


def test_pagination_walks_every_row_without_gaps_or_duplicates() -> None:
    rows = _gold_rows(hours=5)  # 20 rows
    client, _, _ = _client(rows=rows)

    seen: list[tuple[str, str, str]] = []
    cursor = None
    for _ in range(20):  # bounded so a broken cursor cannot loop forever
        params = {"limit": 3}
        if cursor:
            params["cursor"] = cursor
        body = client.get("/v1/metrics/hourly", params=params).json()
        seen.extend(
            (item["payment_hour"], item["country_code"], item["payment_method"])
            for item in body["data"]
        )
        cursor = body["next_cursor"]
        if cursor is None:
            break

    assert cursor is None, "pagination did not terminate"
    assert len(seen) == len(rows)
    assert len(set(seen)) == len(rows), "a row was returned on more than one page"


def test_malformed_cursor_is_a_client_error() -> None:
    client, _, _ = _client()

    response = client.get("/v1/metrics/hourly", params={"cursor": "not-a-cursor"})

    assert response.status_code == 400
    assert "cursor" in response.json()["detail"]


def test_inverted_time_window_is_rejected() -> None:
    client, _, _ = _client()

    response = client.get(
        "/v1/metrics/hourly",
        params={"start": "2026-03-02T00:00:00", "end": "2026-03-01T00:00:00"},
    )

    assert response.status_code == 400


@pytest.mark.parametrize("limit", [0, 1001])
def test_limit_is_range_checked(limit: int) -> None:
    client, _, _ = _client()

    assert client.get("/v1/metrics/hourly", params={"limit": limit}).status_code == 422


def test_country_code_must_be_two_characters() -> None:
    client, _, _ = _client()

    assert client.get("/v1/metrics/hourly", params={"country_code": "NLD"}).status_code == 422


def test_repeat_request_is_served_from_cache_without_requerying() -> None:
    client, state, _ = _client()

    client.get("/v1/metrics/hourly", params={"limit": 5})
    after_first = len(state["queries"])
    client.get("/v1/metrics/hourly", params={"limit": 5})
    after_second = len(state["queries"])

    # The snapshot lookup still runs (it is how staleness is detected); the page query does not.
    assert after_second - after_first == 1


def test_new_snapshot_invalidates_the_cached_page() -> None:
    client, state, _ = _client()
    client.get("/v1/metrics/hourly", params={"limit": 5})

    state["snapshot_id"] = "snap-2"
    before = len(state["queries"])
    body = client.get("/v1/metrics/hourly", params={"limit": 5}).json()

    assert body["snapshot_id"] == "snap-2"
    assert len(state["queries"]) - before == 2, "expected a snapshot lookup plus a fresh page query"


def test_summary_reports_weighted_totals() -> None:
    client, _, _ = _client(summary_row=(24, 240, Decimal("2412.00"), 0.875))

    body = client.get("/v1/metrics/summary").json()

    assert body["bucket_count"] == 24
    assert body["payment_count"] == 240
    assert body["gross_volume"] == "2412.00"
    assert body["auth_rate"] == 0.875


def test_summary_of_an_empty_window_returns_nulls_not_an_error() -> None:
    client, _, _ = _client(summary_row=(0, 0, Decimal("0.00"), None))

    body = client.get("/v1/metrics/summary").json()

    assert body["bucket_count"] == 0
    assert body["auth_rate"] is None


def test_health_does_not_depend_on_trino() -> None:
    # Liveness must not fail on a warehouse outage, or Kubernetes restarts healthy pods.
    client, state, _ = _client(connection_error=RuntimeError("trino down"))

    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert state["queries"] == []


def test_ready_reports_degraded_when_trino_is_unreachable() -> None:
    client, _, _ = _client(connection_error=RuntimeError("connection refused"))

    body = client.get("/v1/ready").json()

    assert body["status"] == "degraded"
    assert body["trino_reachable"] is False
    assert "connection refused" in body["detail"]


def test_ready_is_ok_when_trino_answers() -> None:
    client, _, _ = _client()

    body = client.get("/v1/ready").json()

    assert body["status"] == "ok"
    assert body["trino_reachable"] is True


def test_metrics_endpoint_exposes_prometheus_text() -> None:
    client, _, _ = _client()
    client.get("/v1/metrics/hourly", params={"limit": 1})

    response = client.get("/metrics")

    assert response.status_code == 200
    assert "text/plain" in response.headers["content-type"]
    assert "payments_api_requests_total" in response.text
    assert "payments_api_request_duration_seconds" in response.text
    assert "payments_api_cache_events_total" in response.text


def test_committed_openapi_schema_matches_the_app() -> None:
    """docs/openapi.json is the published contract; a drifted copy is worse than none.

    Regenerate after any route or model change:
        python3 -c "import json,pathlib;from api.src.app import app;\
pathlib.Path('docs/openapi.json').write_text(json.dumps(app.openapi(),indent=2,sort_keys=True)+chr(10))"
    """
    import json
    from pathlib import Path

    committed = json.loads(
        (Path(__file__).resolve().parents[1] / "docs" / "openapi.json").read_text(encoding="utf-8")
    )
    live = json.loads(json.dumps(app.openapi(), sort_keys=True))

    assert committed["paths"].keys() == live["paths"].keys()
    assert committed["components"]["schemas"].keys() == live["components"]["schemas"].keys()


def test_response_model_matches_the_documented_gold_contract() -> None:
    """The response shape must track the gold table's columns in docs/design.md.

    Gold is "hourly aggregates per country_code and payment_method -- payment_count,
    exact-precision gross_volume, auth_rate". A field added to the table and forgotten here means
    the API silently under-serves the data it claims to expose.
    """
    schema = app.openapi()["components"]["schemas"]["HourlyMetric"]["properties"]

    assert set(schema) == {
        "payment_hour",
        "country_code",
        "payment_method",
        "payment_count",
        "gross_volume",
        "auth_rate",
    }


def test_openapi_schema_documents_the_public_endpoints() -> None:
    client, _, _ = _client()

    paths = client.get("/openapi.json").json()["paths"]

    assert "/v1/metrics/hourly" in paths
    assert "/v1/metrics/summary" in paths
    assert "/v1/health" in paths
    # /metrics is Prometheus scrape surface, not part of the public contract.
    assert "/metrics" not in paths


def test_console_is_served_from_the_api_root() -> None:
    client, _, _ = _client()

    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    # Same-origin relative paths: the page must not hard-code a host, or it breaks the moment it is
    # reached through a port-forward or behind a gateway on a different address.
    assert "/v1/metrics/hourly" in response.text
    assert "http://localhost:8000" not in response.text


def test_console_stays_out_of_the_machine_contract() -> None:
    """The page is a consumer of the API, not part of its published surface."""
    client, _, _ = _client()

    assert "/" not in client.get("/openapi.json").json()["paths"]


def test_console_uses_the_cursor_rather_than_offset_paging() -> None:
    """The page exists partly to demonstrate keyset pagination; offset paging would undercut it."""
    client, _, _ = _client()

    body = client.get("/").text

    assert "next_cursor" in body
    assert "offset" not in body.lower()
