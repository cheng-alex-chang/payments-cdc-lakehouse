"""Wire representations for the serving API: response models and the pagination cursor codec."""
from __future__ import annotations

import base64
import binascii
import json
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, field_serializer

from api.src.queries import Cursor


class CursorError(ValueError):
    """Raised when a client-supplied cursor is malformed. The app maps this to HTTP 400."""


def encode_cursor(cursor: Cursor) -> str:
    """Serialize a cursor to an opaque base64url token.

    Opaque by intent: clients must treat it as a token and echo it back, not construct one. The
    payload is not signed -- a tampered cursor can only move the read position within a read-only
    table, so integrity protection would buy nothing here.
    """
    payload = {
        "h": cursor.payment_hour.isoformat(),
        "c": cursor.country_code,
        "m": cursor.payment_method,
    }
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.urlsafe_b64encode(raw).decode("ascii")


def decode_cursor(token: str) -> Cursor:
    """Parse a cursor token, rejecting anything malformed rather than silently resetting."""
    try:
        raw = base64.urlsafe_b64decode(token.encode("ascii"))
        payload = json.loads(raw)
        return Cursor(
            payment_hour=datetime.fromisoformat(payload["h"]),
            country_code=payload["c"],
            payment_method=payload["m"],
        )
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
        raise CursorError(f"malformed cursor: {exc}") from exc


class HourlyMetric(BaseModel):
    """One gold row: the hourly aggregate for a country and payment method."""

    payment_hour: datetime
    country_code: str
    payment_method: str
    payment_count: int
    gross_volume: Decimal
    auth_rate: float

    # gross_volume is money, stored as DECIMAL(18,2). Serializing it as a JSON number would hand
    # it to a JavaScript client as an IEEE double and reintroduce the rounding the warehouse type
    # exists to prevent, so it goes over the wire as a string.
    @field_serializer("gross_volume")
    def _serialize_gross_volume(self, value: Decimal) -> str:
        return str(value)


class HourlyMetricsPage(BaseModel):
    """A page of hourly metrics plus the token for the next one."""

    data: list[HourlyMetric]
    next_cursor: str | None = Field(
        default=None,
        description="Opaque token for the next page; null when this is the last page.",
    )
    snapshot_id: str | None = Field(
        default=None,
        description="Iceberg snapshot id of the gold table this page was read from.",
    )


class MetricsSummary(BaseModel):
    """Roll-up totals across the filtered window."""

    bucket_count: int = Field(description="Number of hourly gold rows matched.")
    payment_count: int
    gross_volume: Decimal
    auth_rate: float | None = Field(
        default=None,
        description="Payment-count-weighted authorization rate; null when the window is empty.",
    )
    snapshot_id: str | None = None

    @field_serializer("gross_volume")
    def _serialize_gross_volume(self, value: Decimal) -> str:
        return str(value)


class HealthResponse(BaseModel):
    status: str


class ReadyResponse(BaseModel):
    status: str
    trino_reachable: bool
    detail: str | None = None
