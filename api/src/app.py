"""Read-only serving API over the gold Iceberg table.

Closes the loop the rest of the platform leaves open: Postgres -> CDC -> Iceberg -> Trino gets the
data to an analyst, but nothing serves it back to an application. This does, over the same gold
table Grafana charts.

Design notes worth knowing before editing:

* Filters compile into SQL. Trino prunes Iceberg files by the days(payment_hour) partitioning, so
  narrowing in the warehouse is the difference between reading a few files and reading all of them.
* Pagination is keyset, not OFFSET. OFFSET makes the engine read and discard every skipped row, so
  page 500 costs 500 pages of work; a cursor costs the same as page 1.
* Caching is keyed on the gold table's Iceberg snapshot id, so invalidation is exact rather than a
  TTL guess. See api/src/cache.py.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query, Response
from fastapi.responses import HTMLResponse

from api.src import metrics
from api.src.cache import SnapshotCache
from api.src.models import (
    CursorError,
    HealthResponse,
    HourlyMetric,
    HourlyMetricsPage,
    MetricsSummary,
    ReadyResponse,
    decode_cursor,
    encode_cursor,
)
from api.src.queries import (
    Cursor,
    MetricsFilter,
    current_snapshot_sql,
    hourly_metrics_sql,
    summary_sql,
)
from api.src.trino_client import connect_from_env, run_query

MAX_PAGE_SIZE = 1000
DEFAULT_PAGE_SIZE = 100

# Read once at import rather than per request: it is a fixed asset baked into the image, and
# re-reading it on every load would put disk I/O in the hot path for no benefit.
CONSOLE_HTML = (Path(__file__).resolve().parent / "console.html").read_text(encoding="utf-8")


class GoldRepository:
    """Executes the SQL builders against Trino, with snapshot-scoped caching."""

    def __init__(
        self,
        connection_factory: Any = connect_from_env,
        cache: SnapshotCache | None = None,
    ) -> None:
        self._connection_factory = connection_factory
        self._cache = cache if cache is not None else SnapshotCache()

    @property
    def cache(self) -> SnapshotCache:
        return self._cache

    def _query(self, sql: str, params: list[Any]) -> list[tuple[Any, ...]]:
        # Trino's DBAPI connection is a thin HTTP client, so a connection per request is cheap and
        # avoids holding server-side state between them.
        connection = self._connection_factory()
        try:
            rows = run_query(connection, sql, params)
        except Exception:
            metrics.TRINO_QUERIES.labels(outcome="error").inc()
            raise
        else:
            metrics.TRINO_QUERIES.labels(outcome="ok").inc()
            return rows
        finally:
            close = getattr(connection, "close", None)
            if callable(close):
                close()

    def current_snapshot(self) -> str | None:
        """Newest gold snapshot id, or None when the table has no snapshots yet."""
        sql, params = current_snapshot_sql()
        rows = self._query(sql, params)
        return str(rows[0][0]) if rows and rows[0][0] is not None else None

    def cached(self, key: str, snapshot_id: str | None, build: Any) -> Any:
        hit = self._cache.get(snapshot_id, key)
        metrics.record_cache(hit is not None)
        if hit is not None:
            return hit

        value = build()
        self._cache.put(snapshot_id, key, value)
        metrics.CACHE_ENTRIES.set(len(self._cache))
        return value

    def hourly_rows(
        self, filters: MetricsFilter, limit: int, after: Cursor | None
    ) -> list[tuple[Any, ...]]:
        # One extra row is the has-more signal; a COUNT(*) would double the warehouse work.
        sql, params = hourly_metrics_sql(filters, limit + 1, after)
        return self._query(sql, params)

    def summary_row(self, filters: MetricsFilter) -> tuple[Any, ...]:
        sql, params = summary_sql(filters)
        rows = self._query(sql, params)
        return rows[0] if rows else (0, 0, Decimal("0.00"), None)


_repository = GoldRepository()


def get_repository() -> GoldRepository:
    """FastAPI dependency. Tests override this to inject a stubbed repository."""
    return _repository


app = FastAPI(
    title="Payments Gold API",
    version="1.0.0",
    description=(
        "Read-only access to hourly payment metrics from the gold Iceberg table "
        "(`iceberg.analytics.payment_metrics_gold`), served through Trino."
    ),
)

RepositoryDep = Annotated[GoldRepository, Depends(get_repository)]


def _cache_key(prefix: str, filters: MetricsFilter, limit: int | None, cursor: str | None) -> str:
    return "|".join(
        [
            prefix,
            filters.start.isoformat() if filters.start else "",
            filters.end.isoformat() if filters.end else "",
            filters.country_code or "",
            filters.payment_method or "",
            str(limit or ""),
            cursor or "",
        ]
    )


@app.get("/v1/metrics/hourly", response_model=HourlyMetricsPage, tags=["metrics"])
def hourly_metrics(
    repository: RepositoryDep,
    start: Annotated[datetime | None, Query(description="Inclusive lower bound on payment_hour.")] = None,
    end: Annotated[datetime | None, Query(description="Exclusive upper bound on payment_hour.")] = None,
    country_code: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
    payment_method: Annotated[str | None, Query(max_length=64)] = None,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
    cursor: Annotated[str | None, Query(description="Opaque token from a previous page.")] = None,
) -> HourlyMetricsPage:
    with metrics.observe("hourly"):
        if start is not None and end is not None and end <= start:
            raise HTTPException(status_code=400, detail="end must be after start")

        after = None
        if cursor is not None:
            try:
                after = decode_cursor(cursor)
            except CursorError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc

        filters = MetricsFilter(
            start=start, end=end, country_code=country_code, payment_method=payment_method
        )
        snapshot_id = repository.current_snapshot()
        key = _cache_key("hourly", filters, limit, cursor)
        rows = repository.cached(key, snapshot_id, lambda: repository.hourly_rows(filters, limit, after))

        has_more = len(rows) > limit
        page = rows[:limit]
        data = [
            HourlyMetric(
                payment_hour=row[0],
                country_code=row[1],
                payment_method=row[2],
                payment_count=row[3],
                gross_volume=row[4],
                auth_rate=row[5],
            )
            for row in page
        ]

        next_cursor = None
        if has_more and page:
            last = page[-1]
            next_cursor = encode_cursor(
                Cursor(payment_hour=last[0], country_code=last[1], payment_method=last[2])
            )

        return HourlyMetricsPage(data=data, next_cursor=next_cursor, snapshot_id=snapshot_id)


@app.get("/v1/metrics/summary", response_model=MetricsSummary, tags=["metrics"])
def metrics_summary(
    repository: RepositoryDep,
    start: Annotated[datetime | None, Query()] = None,
    end: Annotated[datetime | None, Query()] = None,
    country_code: Annotated[str | None, Query(min_length=2, max_length=2)] = None,
    payment_method: Annotated[str | None, Query(max_length=64)] = None,
) -> MetricsSummary:
    with metrics.observe("summary"):
        if start is not None and end is not None and end <= start:
            raise HTTPException(status_code=400, detail="end must be after start")

        filters = MetricsFilter(
            start=start, end=end, country_code=country_code, payment_method=payment_method
        )
        snapshot_id = repository.current_snapshot()
        key = _cache_key("summary", filters, None, None)
        row = repository.cached(key, snapshot_id, lambda: repository.summary_row(filters))

        return MetricsSummary(
            bucket_count=row[0],
            payment_count=row[1],
            gross_volume=row[2],
            auth_rate=row[3],
            snapshot_id=snapshot_id,
        )


@app.get("/v1/health", response_model=HealthResponse, tags=["ops"])
def health() -> HealthResponse:
    """Liveness only -- deliberately does not touch Trino, so a warehouse outage cannot cause
    Kubernetes to restart an otherwise healthy pod."""
    return HealthResponse(status="ok")


@app.get("/v1/ready", response_model=ReadyResponse, tags=["ops"])
def ready(repository: RepositoryDep) -> ReadyResponse:
    """Readiness: can this instance actually serve? Requires Trino to answer."""
    try:
        repository.current_snapshot()
    except Exception as exc:
        return ReadyResponse(status="degraded", trino_reachable=False, detail=str(exc)[:200])
    return ReadyResponse(status="ok", trino_reachable=True)


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def console() -> HTMLResponse:
    """A single-page consumer of this API, served from the API itself.

    The platform had no reader for its own serving tier: Grafana queries Trino directly, and
    Prometheus only scrapes /metrics. This is the thing the gold endpoints exist for -- an internal
    ops view of volume and authorization rate, built entirely from /v1/metrics/*.

    Served from this origin rather than as a separate site so it needs no CORS policy, no second
    deployment, and no network egress. It is excluded from the OpenAPI schema for the same reason
    /metrics is: the machine contract is the /v1 surface, not the page that happens to render it.
    """
    return HTMLResponse(CONSOLE_HTML)


@app.get("/metrics", include_in_schema=False)
def prometheus_metrics() -> Response:
    payload, content_type = metrics.render()
    return Response(content=payload, media_type=content_type)
