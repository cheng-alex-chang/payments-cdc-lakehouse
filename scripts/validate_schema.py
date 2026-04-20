from __future__ import annotations

import logging
import os
import sys
from urllib.parse import urlparse

import psycopg2


# Columns extracted from Debezium's $.after and written to silver.
SILVER_SOURCE_COLUMNS: frozenset[str] = frozenset({
    "payment_id",
    "merchant_id",
    "amount",
    "currency",
    "payment_method",
    "payment_status",
    "country_code",
    "created_at",
    "updated_at",
})

# Postgres columns deliberately not propagated to silver.
# Document the reason for each exclusion here so the intent is explicit.
EXCLUDED_COLUMNS: frozenset[str] = frozenset({
    "shopper_id",   # PII; not required for payment analytics
})

PAYMENTS_TABLE = "payments"
PAYMENTS_SCHEMA = "public"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def fetch_postgres_columns(conn_uri: str) -> frozenset[str]:
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
                (PAYMENTS_SCHEMA, PAYMENTS_TABLE),
            )
            return frozenset(row[0] for row in cur.fetchall())
    finally:
        conn.close()


def check_columns(postgres_columns: frozenset[str]) -> list[str]:
    known = SILVER_SOURCE_COLUMNS | EXCLUDED_COLUMNS
    return sorted(postgres_columns - known)


def main() -> None:
    conn_uri = os.environ["AIRFLOW_CONN_SOURCE_POSTGRES"]
    LOGGER.info("Fetching column list for %s.%s", PAYMENTS_SCHEMA, PAYMENTS_TABLE)
    postgres_columns = fetch_postgres_columns(conn_uri)
    LOGGER.info("Postgres columns: %s", sorted(postgres_columns))

    unmapped = check_columns(postgres_columns)
    if unmapped:
        raise SystemExit(
            f"Schema drift detected — {len(unmapped)} unmapped column(s) in "
            f"{PAYMENTS_SCHEMA}.{PAYMENTS_TABLE}: {unmapped}. "
            "Add each column to SILVER_SOURCE_COLUMNS (and update the silver job) "
            "or to EXCLUDED_COLUMNS (with a reason) in scripts/validate_schema.py."
        )

    LOGGER.info("Schema check passed — all %d column(s) accounted for", len(postgres_columns))


if __name__ == "__main__":  # pragma: no cover
    try:
        main()
    except SystemExit:
        raise
    except Exception as exc:
        LOGGER.exception("Schema validation failed")
        print(str(exc), file=sys.stderr)
        raise
