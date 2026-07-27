"""SQL builders for the gold serving API.

Every function here is pure: it returns `(sql, params)` and never opens a connection, so the exact
query shape is unit-testable with no Trino running. The app layer is the only thing that executes
them.

Filters compile into `WHERE` predicates rather than being applied in Python. That is the whole
reason Trino sits underneath: the gold table is `PARTITIONED BY (days(payment_hour))`, so a bounded
time range lets Iceberg prune files before any data is read. Pulling rows into the process and
filtering them there would throw that away and turn every request into a full-table scan.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

GOLD_TABLE = "iceberg.analytics.payment_metrics_gold"

# Trino exposes Iceberg metadata as `"<table>$snapshots"`. The current snapshot id is the cache
# key for every response built from this table -- see api/src/cache.py.
SNAPSHOTS_TABLE = 'iceberg.analytics."payment_metrics_gold$snapshots"'

# Ordering key for keyset pagination. Must stay in step with `Cursor` and the ORDER BY clause;
# together they are what makes a cursor stable across requests.
SORT_KEYS = ("payment_hour", "country_code", "payment_method")

SELECT_COLUMNS = (
    "payment_hour",
    "country_code",
    "payment_method",
    "payment_count",
    "gross_volume",
    "auth_rate",
)


@dataclass(frozen=True)
class MetricsFilter:
    """User-supplied filters. Every field is optional; None means "do not constrain"."""

    start: datetime | None = None
    end: datetime | None = None
    country_code: str | None = None
    payment_method: str | None = None


@dataclass(frozen=True)
class Cursor:
    """The last row a client received, i.e. where the next page resumes."""

    payment_hour: datetime
    country_code: str
    payment_method: str


def _filter_predicates(filters: MetricsFilter) -> tuple[list[str], list[object]]:
    clauses: list[str] = []
    params: list[object] = []

    # `end` is exclusive so adjacent windows tile without double-counting the boundary hour.
    if filters.start is not None:
        clauses.append("payment_hour >= ?")
        params.append(filters.start)
    if filters.end is not None:
        clauses.append("payment_hour < ?")
        params.append(filters.end)
    if filters.country_code is not None:
        clauses.append("country_code = ?")
        params.append(filters.country_code)
    if filters.payment_method is not None:
        clauses.append("payment_method = ?")
        params.append(filters.payment_method)

    return clauses, params


def _keyset_predicate(after: Cursor) -> tuple[str, list[object]]:
    """The portable spelling of `(a, b, c) > (?, ?, ?)`.

    Written out as nested OR groups rather than a row comparison so no CAST is needed around the
    bound timestamp parameter. Semantically identical: strictly-after in SORT_KEYS order.
    """
    clause = (
        "(payment_hour > ?"
        " OR (payment_hour = ? AND country_code > ?)"
        " OR (payment_hour = ? AND country_code = ? AND payment_method > ?))"
    )
    params: list[object] = [
        after.payment_hour,
        after.payment_hour,
        after.country_code,
        after.payment_hour,
        after.country_code,
        after.payment_method,
    ]
    return clause, params


def hourly_metrics_sql(
    filters: MetricsFilter,
    limit: int,
    after: Cursor | None = None,
) -> tuple[str, list[object]]:
    """One page of hourly gold rows, ordered by SORT_KEYS.

    Callers should request `limit + 1` rows and treat the extra one as the has-more signal rather
    than issuing a second COUNT query.
    """
    clauses, params = _filter_predicates(filters)

    if after is not None:
        keyset_clause, keyset_params = _keyset_predicate(after)
        clauses.append(keyset_clause)
        params.extend(keyset_params)

    where = f"WHERE {' AND '.join(clauses)}\n" if clauses else ""
    order_by = ", ".join(SORT_KEYS)
    columns = ", ".join(SELECT_COLUMNS)

    # LIMIT is interpolated rather than bound: Trino does not accept a parameter there. It is an
    # int the request model has already range-checked, so there is nothing injectable left.
    sql = f"SELECT {columns}\nFROM {GOLD_TABLE}\n{where}ORDER BY {order_by}\nLIMIT {int(limit)}"
    return sql, params


def summary_sql(filters: MetricsFilter) -> tuple[str, list[object]]:
    """Roll-up totals for the filtered window."""
    clauses, params = _filter_predicates(filters)
    where = f"\nWHERE {' AND '.join(clauses)}" if clauses else ""

    # auth_rate must be weighted by payment_count. Averaging the per-bucket rates directly would
    # give an unweighted mean of means -- an hour with 3 payments would count as much as one with
    # 30,000. NULLIF keeps an empty window returning NULL instead of dividing by zero.
    sql = (
        "SELECT\n"
        "    count(*) AS bucket_count,\n"
        "    coalesce(sum(payment_count), 0) AS payment_count,\n"
        "    coalesce(sum(gross_volume), CAST(0 AS DECIMAL(18,2))) AS gross_volume,\n"
        "    sum(auth_rate * payment_count) / NULLIF(sum(payment_count), 0) AS auth_rate\n"
        f"FROM {GOLD_TABLE}{where}"
    )
    return sql, params


def current_snapshot_sql() -> tuple[str, list[object]]:
    """The gold table's newest Iceberg snapshot id -- the cache key for every response."""
    sql = f"SELECT snapshot_id\nFROM {SNAPSHOTS_TABLE}\nORDER BY committed_at DESC\nLIMIT 1"
    return sql, []
