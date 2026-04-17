from decimal import Decimal

from config.spark.jobs.common import (
    canonicalize_country_code,
    canonicalize_text,
    compute_auth_rate,
    normalize_payment,
)


def test_compute_auth_rate_handles_empty_input() -> None:
    assert compute_auth_rate([]) == Decimal("0")


def test_compute_auth_rate_counts_authorized_rows() -> None:
    rows = [
        {"payment_status": "authorized"},
        {"payment_status": "failed"},
        {"payment_status": "authorized"},
    ]

    assert compute_auth_rate(rows) == Decimal("0.6667")


def test_normalize_payment_shapes_record() -> None:
    record = {
        "payment_id": "1",
        "merchant_id": "2",
        "shopper_id": "3",
        "amount": "42.50",
        "currency": "eur",
        "payment_method": "Card",
        "payment_status": "AUTHORIZED",
        "country_code": "nl",
        "created_at": "2026-01-01T00:00:00",
        "updated_at": "2026-01-01T00:00:05",
    }

    normalized = normalize_payment(record)

    assert normalized["payment_id"] == 1
    assert normalized["currency"] == "EUR"
    assert normalized["payment_method"] == "card"
    assert normalized["payment_status"] == "authorized"
    assert normalized["country_code"] == "NL"


def test_canonicalize_text_normalizes_spacing_and_case() -> None:
    assert canonicalize_text(" Apple Pay ") == "apple_pay"


def test_canonicalize_country_code_uppercases_value() -> None:
    assert canonicalize_country_code(" gb ") == "GB"
