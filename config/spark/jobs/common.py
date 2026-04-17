from __future__ import annotations

from decimal import Decimal
from typing import Iterable


def compute_auth_rate(records: Iterable[dict]) -> Decimal:
    rows = list(records)
    if not rows:
        return Decimal("0")

    authorized = sum(1 for row in rows if row.get("payment_status") == "authorized")
    return (Decimal(authorized) / Decimal(len(rows))).quantize(Decimal("0.0001"))


def canonicalize_text(value: str) -> str:
    return str(value).strip().lower().replace(" ", "_")


def canonicalize_country_code(value: str) -> str:
    return str(value).strip().upper()


def normalize_payment(record: dict) -> dict:
    return {
        "payment_id": int(record["payment_id"]),
        "merchant_id": int(record["merchant_id"]),
        "shopper_id": int(record["shopper_id"]),
        "amount": float(record["amount"]),
        "currency": str(record["currency"]).upper(),
        "payment_method": canonicalize_text(record["payment_method"]),
        "payment_status": canonicalize_text(record["payment_status"]),
        "country_code": canonicalize_country_code(record["country_code"]),
        "created_at": record["created_at"],
        "updated_at": record["updated_at"],
    }
