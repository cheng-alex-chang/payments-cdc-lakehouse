# Payments Gold API

A read-only HTTP tier over the gold Iceberg table (`iceberg.analytics.payment_metrics_gold`),
served through Trino.

The rest of the platform moves data *inward*: Postgres → CDC → Kafka → Spark → Iceberg → Trino.
That gets an analyst to a SQL prompt and Grafana to a chart, but nothing hands the results back to
an application. This closes that loop.

## Endpoints

| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/v1/metrics/hourly` | Hourly gold rows; filterable, cursor-paginated |
| `GET` | `/v1/metrics/summary` | Roll-up totals for a window |
| `GET` | `/v1/health` | Liveness — never touches Trino |
| `GET` | `/v1/ready` | Readiness — requires Trino |
| `GET` | `/metrics` | Prometheus scrape |
| `GET` | `/docs` | Interactive OpenAPI UI |

```bash
curl 'http://localhost:8000/v1/metrics/hourly?country_code=NL&limit=5'
curl 'http://localhost:8000/v1/metrics/summary?start=2026-03-01T00:00:00'
```

## Four decisions worth knowing

**Filters compile to SQL, not to Python.** Gold is `PARTITIONED BY (days(payment_hour))`, so a
bounded time range lets Iceberg prune files before anything is read. Fetching rows and filtering
them in the process would turn every request into a full-table scan — the reason to put a query
engine underneath is to let it do the narrowing.

**Pagination is keyset, not `OFFSET`.** `OFFSET` makes the engine read and discard every skipped
row, so page 500 costs five hundred pages of work. The cursor encodes the last
`(payment_hour, country_code, payment_method)` seen, so every page costs the same as the first.
The token is opaque base64 — clients echo it back rather than constructing it.

**The cache is keyed on the Iceberg snapshot id, not a TTL.** A TTL guesses: too short and it does
nothing, too long and it serves data that has already been replaced. The gold job commits a new
snapshot on every `INSERT OVERWRITE`, so keying entries on that id makes invalidation exact. A
cached response cannot outlive the data it was built from, and it never expires early while the
data is unchanged. Because gold is a full atomic replace, one new snapshot drops the whole cache.

**Money is a string on the wire.** `gross_volume` is `DECIMAL(18,2)`. Serialized as a JSON number
it reaches a browser as an IEEE double, reintroducing precisely the rounding the warehouse type
exists to prevent.

## Layout

```text
api/src/
├── app.py            FastAPI app, routes, GoldRepository
├── queries.py        pure SQL builders -> (sql, params)
├── trino_client.py   connect_from_env(), lazy driver import
├── cache.py          snapshot-scoped LRU
├── models.py         response models + cursor codec
└── metrics.py        Prometheus instrumentation
```

## Running it

Under Compose and Kubernetes the API starts with the rest of the platform. Standalone:

```bash
pip install -r requirements-ci.txt -r requirements-api.txt
TRINO_HOST=localhost uvicorn api.src.app:app --reload --port 8000
```

Configuration is environment-only: `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG`,
`TRINO_SCHEMA` (see `api/src/trino_client.py` for defaults).

## Tests

```bash
pytest tests/test_api_*.py
```

The suite runs with **no Trino driver installed and no warehouse reachable**. `trino_client.py`
imports the driver lazily inside `connect_from_env`, so the tests inject a fake DBAPI connection
underneath the real `GoldRepository` — SQL building, snapshot caching, row mapping and cursor
emission are all the production code path; only the driver is fake. That is why `trino` lives in
`requirements-api.txt` rather than `requirements-ci.txt`: keeping it out of CI is what proves the
lazy import still works.

## Benchmark

```bash
bash scripts/bench_api.sh
```

Reports p50/p95/p99 latency cold vs. warm cache. Results in [docs/api-benchmark.md](../docs/api-benchmark.md).
