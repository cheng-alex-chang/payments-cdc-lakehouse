from __future__ import annotations

import logging
import os
import sys
from urllib.parse import urlparse

import psycopg2


# Per-table schema contract: (silver_source_columns, excluded_columns).
# silver_source_columns: extracted from Debezium's $.after and written to silver.
# excluded_columns: deliberately not propagated — document the reason for each.
# Any Postgres column in neither set triggers a failure until a developer
# either maps it into silver or adds it to excluded_columns with a reason.
TRACKED_TABLES: dict[str, tuple[frozenset[str], frozenset[str]]] = {
    "payments": (
        frozenset({
            "payment_id",
            "merchant_id",
            "amount",
            "currency",
            "payment_method",
            "payment_status",
            "country_code",
            "created_at",
            "updated_at",
        }),
        frozenset({
            "shopper_id",   # PII; not required for payment analytics
        }),
    ),
    "merchants": (
        frozenset(),        # no silver layer yet; all columns tracked for drift only
        frozenset({
            "merchant_id",
            "merchant_name",
            "country_code",
            "category",
            "created_at",
        }),
    ),
    "refunds": (
        frozenset(),        # no silver layer yet; all columns tracked for drift only
        frozenset({
            "refund_id",
            "payment_id",
            "refund_amount",
            "refund_reason",
            "created_at",
        }),
    ),
}

SCHEMA = "public"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def fetch_postgres_columns(conn_uri: str, table: str) -> frozenset[str]:
    parsed = urlparse(conn_uri)
    conn = psycopg2.connect(
        host=parsed.hostname,
        port=parsed.port or 5432,
        dbname=parsed.path.lstrip("/"),
        user=parsed.username,
        password=parsed.password,
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
                """,
                (SCHEMA, table),
            )
            return frozenset(row[0] for row in cur.fetchall())
    finally:
        conn.close()


def check_columns(
    postgres_columns: frozenset[str],
    silver_source_columns: frozenset[str],
    excluded_columns: frozenset[str],
) -> list[str]:
    known = silver_source_columns | excluded_columns
    return sorted(postgres_columns - known)


def main() -> None:
    conn_uri = os.environ["AIRFLOW_CONN_SOURCE_POSTGRES"]
    failures: dict[str, list[str]] = {}

    for table, (silver_cols, excluded_cols) in TRACKED_TABLES.items():
        LOGGER.info("Checking schema for %s.%s", SCHEMA, table)
        postgres_columns = fetch_postgres_columns(conn_uri, table)
        LOGGER.info("%s columns: %s", table, sorted(postgres_columns))
        unmapped = check_columns(postgres_columns, silver_cols, excluded_cols)
        if unmapped:
            failures[table] = unmapped

    if failures:
        lines = [
            f"  {SCHEMA}.{table}: {cols}" for table, cols in failures.items()
        ]
        raise SystemExit(
            f"Schema drift detected in {len(failures)} table(s):\n"
            + "\n".join(lines)
            + "\nAdd each column to the table's silver_source_columns (and update the "
            "silver job) or excluded_columns (with a reason) in scripts/validate_schema.py."
        )

    LOGGER.info("Schema check passed for all %d tracked table(s)", len(TRACKED_TABLES))


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        LOGGER.exception("Schema validation failed")
        print(str(exc), file=sys.stderr)
        raise
