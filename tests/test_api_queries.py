"""Static guards for the serving API's SQL builders (api/src/queries.py).

These functions are pure by design so the emitted SQL is assertable without Trino. What matters
is not that the strings look a particular way but that the *pushdown* survives: filters must
become WHERE predicates, pagination must stay keyset, and the summary roll-up must weight the
authorization rate. Each of those is a correctness or performance property a refactor could drop
silently.
"""
from __future__ import annotations

from datetime import datetime

from api.src.queries import (
    GOLD_TABLE,
    SELECT_COLUMNS,
    SORT_KEYS,
    Cursor,
    MetricsFilter,
    current_snapshot_sql,
    hourly_metrics_sql,
    summary_sql,
)

START = datetime(2026, 3, 1, 0, 0, 0)
END = datetime(2026, 3, 2, 0, 0, 0)


def test_unfiltered_page_emits_no_where_clause() -> None:
    sql, params = hourly_metrics_sql(MetricsFilter(), limit=10)

    assert "WHERE" not in sql
    assert params == []


def test_filters_become_bound_where_predicates() -> None:
    filters = MetricsFilter(start=START, end=END, country_code="NL", payment_method="ideal")

    sql, params = hourly_metrics_sql(filters, limit=50)

    assert "WHERE" in sql
    assert "payment_hour >= ?" in sql
    # Exclusive upper bound so adjacent windows tile without double-counting the boundary hour.
    assert "payment_hour < ?" in sql
    assert "country_code = ?" in sql
    assert "payment_method = ?" in sql
    assert params == [START, END, "NL", "ideal"]


def test_values_are_bound_never_interpolated() -> None:
    # A quote-bearing value must land in params, not in the statement text.
    filters = MetricsFilter(payment_method="' OR 1=1 --")

    sql, params = hourly_metrics_sql(filters, limit=5)

    assert "OR 1=1" not in sql
    assert params == ["' OR 1=1 --"]


def test_select_lists_columns_explicitly() -> None:
    sql, _ = hourly_metrics_sql(MetricsFilter(), limit=1)

    # SELECT * would read every column off disk and silently change the response shape if the
    # gold schema gained one.
    assert "SELECT *" not in sql
    for column in SELECT_COLUMNS:
        assert column in sql
    assert GOLD_TABLE in sql


def test_page_is_ordered_by_the_pagination_key() -> None:
    sql, _ = hourly_metrics_sql(MetricsFilter(), limit=1)

    # A cursor is only stable if the ORDER BY matches the key it encodes.
    assert f"ORDER BY {', '.join(SORT_KEYS)}" in sql


def test_limit_is_inlined_as_an_integer() -> None:
    sql, params = hourly_metrics_sql(MetricsFilter(), limit=250)

    # Trino rejects a parameter in LIMIT, so it is interpolated; the request model range-checks it.
    assert "LIMIT 250" in sql
    assert 250 not in params


def test_cursor_adds_a_strictly_after_predicate() -> None:
    after = Cursor(payment_hour=START, country_code="NL", payment_method="ideal")

    sql, params = hourly_metrics_sql(MetricsFilter(), limit=10, after=after)

    # Keyset, not OFFSET: the engine seeks rather than reading and discarding skipped rows.
    assert "OFFSET" not in sql
    assert "payment_hour > ?" in sql
    assert "country_code > ?" in sql
    assert "payment_method > ?" in sql
    assert params == [START, START, "NL", START, "NL", "ideal"]


def test_cursor_and_filter_params_stay_in_positional_order() -> None:
    filters = MetricsFilter(start=START, country_code="NL")
    after = Cursor(payment_hour=END, country_code="US", payment_method="card")

    sql, params = hourly_metrics_sql(filters, limit=10, after=after)

    # Filters are appended before the keyset clause; a mismatch here binds values to the wrong
    # placeholders and returns quietly wrong rows.
    assert params == [START, "NL", END, END, "US", END, "US", "card"]
    assert sql.index("payment_hour >= ?") < sql.index("payment_hour > ?")


def test_summary_weights_auth_rate_by_payment_count() -> None:
    sql, _ = summary_sql(MetricsFilter())

    # avg(auth_rate) would be an unweighted mean of means: an hour with 3 payments would count
    # as much as one with 30,000.
    assert "sum(auth_rate * payment_count)" in sql
    assert "NULLIF(sum(payment_count), 0)" in sql
    assert "avg(auth_rate)" not in sql


def test_summary_applies_the_same_filters() -> None:
    filters = MetricsFilter(start=START, country_code="NL")

    sql, params = summary_sql(filters)

    assert "payment_hour >= ?" in sql
    assert "country_code = ?" in sql
    assert params == [START, "NL"]


def test_snapshot_query_reads_the_iceberg_metadata_table() -> None:
    sql, params = current_snapshot_sql()

    # Trino exposes Iceberg metadata as "<table>$snapshots"; newest commit wins.
    assert "payment_metrics_gold$snapshots" in sql
    assert "ORDER BY committed_at DESC" in sql
    assert "LIMIT 1" in sql
    assert params == []
