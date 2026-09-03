"""Data-quality assertions for a freshly deployed medallion.

Distinct from scripts/validate_trino.py, which runs sql/trino/validation_queries.sql and
reports success as long as the queries *execute*. Those queries are three unasserted
SELECTs -- two counts and a GROUP BY -- so nothing in them can fail on wrong data, only
on a missing table. That is a smoke check, and calling it data quality would overstate it.

This module asserts values. It runs against Trino over HTTP, so the same checks work
against Compose and against a Kubernetes deployment reached through a port-forward; see
scripts/trino_http.py for the endpoint contract.
"""
from __future__ import annotations

import logging

try:
    from scripts import trino_http
except ImportError:  # pragma: no cover - flat import path inside the Airflow container
    import trino_http

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

BRONZE = "iceberg.analytics.payments_bronze"
SILVER = "iceberg.analytics.payments_silver"
GOLD = "iceberg.analytics.payment_metrics_gold"

# Silver canonicalises these, so anything outside the sets means the transform regressed.
ALLOWED_METHODS = ("apple_pay", "bank_transfer", "card", "google_pay", "paypal")
ALLOWED_STATUSES = ("authorized", "cancelled", "chargeback", "failed", "pending", "refunded")


def scalar(rows: list[list[object]]) -> int:
    """First column of the first row as an int; 0 when the query returned nothing."""
    if not rows or not rows[0]:
        return 0
    return int(rows[0][0])


def quoted(values: tuple[str, ...]) -> str:
    return ", ".join(f"'{value}'" for value in values)


def checks() -> list[tuple[str, str, str]]:
    """(name, sql, failure message). Each SQL returns a count that must be zero...

    ...except the population checks, which must be non-zero. `expectation` in the name
    says which: `no_` prefixed checks expect 0, `has_` prefixed expect more than 0.
    """
    return [
        (
            "has_bronze_rows",
            f"SELECT count(*) FROM {BRONZE}",
            "bronze is empty -- CDC delivered nothing",
        ),
        (
            "has_silver_rows",
            f"SELECT count(*) FROM {SILVER}",
            "silver is empty -- the bronze -> silver transform produced no rows",
        ),
        (
            "has_gold_rows",
            f"SELECT count(*) FROM {GOLD}",
            "gold is empty -- the silver -> gold aggregation produced no rows",
        ),
        (
            "no_null_gold_dimensions",
            f"SELECT count(*) FROM {GOLD} WHERE country_code IS NULL "
            "OR payment_method IS NULL OR payment_hour IS NULL",
            "gold rows exist with null grouping dimensions",
        ),
        (
            "no_nonpositive_gold_counts",
            f"SELECT count(*) FROM {GOLD} WHERE payment_count <= 0",
            "gold rows exist with a non-positive payment_count",
        ),
        (
            "no_out_of_range_auth_rate",
            f"SELECT count(*) FROM {GOLD} WHERE auth_rate < 0 OR auth_rate > 1",
            "auth_rate is a mean of a 0/1 indicator and cannot fall outside [0, 1]",
        ),
        (
            "no_unknown_payment_methods",
            f"SELECT count(*) FROM {SILVER} WHERE payment_method NOT IN ({quoted(ALLOWED_METHODS)})",
            "silver contains a payment_method outside the canonical set",
        ),
        (
            "no_unknown_payment_statuses",
            f"SELECT count(*) FROM {SILVER} WHERE payment_status NOT IN ({quoted(ALLOWED_STATUSES)})",
            "silver contains a payment_status outside the canonical set",
        ),
        (
            "no_null_silver_keys",
            f"SELECT count(*) FROM {SILVER} WHERE payment_id IS NULL OR merchant_id IS NULL",
            "silver rows exist with null keys",
        ),
        (
            "no_duplicate_silver_payment_ids",
            f"SELECT count(*) FROM (SELECT payment_id FROM {SILVER} "
            "GROUP BY payment_id HAVING count(*) > 1)",
            "silver holds duplicate payment_id rows -- the SCD-1 upsert regressed",
        ),
        (
            "no_unhashed_pii_in_bronze",
            # bronze_from_kafka.py replaces shopper_id with a 64-char SHA-256 digest.
            # A bare integer still in the envelope means PII reached the lakehouse.
            # \s* around the colon: the converter's spacing is not this check's business,
            # and a pattern that assumes compact JSON would quietly match nothing.
            f"SELECT count(*) FROM {BRONZE} "
            "WHERE regexp_like(kafka_value, '\"shopper_id\"\\s*:\\s*[0-9]')",
            "unhashed shopper_id found in bronze -- PII masking regressed",
        ),
    ]


def evaluate(name: str, count: int) -> bool:
    """`has_` checks require a positive count; everything else requires zero."""
    return count > 0 if name.startswith("has_") else count == 0


def main(env: dict[str, str] | None = None) -> None:
    failures: list[str] = []

    for name, sql, message in checks():
        count = scalar(trino_http.run_statement(sql, env=env))
        if evaluate(name, count):
            LOGGER.info("PASS %-32s (%d)", name, count)
        else:
            LOGGER.error("FAIL %-32s (%d) -- %s", name, count, message)
            failures.append(f"{name}: {message} (count={count})")

    if failures:
        for failure in failures:
            LOGGER.error("%s", failure)
        raise SystemExit(f"{len(failures)} data-quality check(s) failed")

    print(f"All {len(checks())} data-quality checks passed")


if __name__ == "__main__":  # pragma: no cover
    main()
