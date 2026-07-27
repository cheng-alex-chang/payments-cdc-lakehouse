"""Prometheus instrumentation for the serving API.

Metrics register on a module-private CollectorRegistry rather than prometheus_client's global
default. Two reasons: the default registry raises "Duplicated timeseries" if the module is
imported twice under pytest, and a private registry keeps the API's series separate from anything
else that might be collected in the same process.
"""
from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager

from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge, Histogram, generate_latest

REGISTRY = CollectorRegistry()

REQUESTS = Counter(
    "payments_api_requests_total",
    "API requests by endpoint and outcome.",
    labelnames=("endpoint", "status"),
    registry=REGISTRY,
)

REQUEST_SECONDS = Histogram(
    "payments_api_request_duration_seconds",
    "Wall-clock request latency by endpoint.",
    labelnames=("endpoint",),
    # Buckets are tuned for a warehouse-backed read path: a cache hit lands in single-digit
    # milliseconds, a cold Trino scan in hundreds. The default buckets bunch everything
    # interesting into one bin.
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
    registry=REGISTRY,
)

CACHE_EVENTS = Counter(
    "payments_api_cache_events_total",
    "Snapshot-cache lookups by result.",
    labelnames=("result",),
    registry=REGISTRY,
)

CACHE_ENTRIES = Gauge(
    "payments_api_cache_entries",
    "Entries currently held in the snapshot cache.",
    registry=REGISTRY,
)

TRINO_QUERIES = Counter(
    "payments_api_trino_queries_total",
    "Statements issued to Trino by outcome. A cache hit issues none, so the ratio of this to "
    "payments_api_requests_total is the cache's real effect.",
    labelnames=("outcome",),
    registry=REGISTRY,
)


@contextmanager
def observe(endpoint: str) -> Iterator[dict[str, str]]:
    """Time a request and record its outcome.

    Yields a mutable dict so the caller can set the status label; on an unhandled exception the
    status is recorded as "error" and the exception re-raised.
    """
    outcome = {"status": "ok"}
    started = time.perf_counter()
    try:
        yield outcome
    except Exception:
        outcome["status"] = "error"
        raise
    finally:
        REQUEST_SECONDS.labels(endpoint=endpoint).observe(time.perf_counter() - started)
        REQUESTS.labels(endpoint=endpoint, status=outcome["status"]).inc()


def record_cache(hit: bool) -> None:
    CACHE_EVENTS.labels(result="hit" if hit else "miss").inc()


def render() -> tuple[bytes, str]:
    """Return the Prometheus exposition payload and its content type."""
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
