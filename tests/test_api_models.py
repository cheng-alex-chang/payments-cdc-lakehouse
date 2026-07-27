"""Guards for the API's wire representations (api/src/models.py).

Two things matter here: a cursor must survive a round trip exactly (otherwise pagination silently
skips or repeats rows), and money must not be serialized as a JSON number.
"""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal

import pytest

from api.src.models import CursorError, HourlyMetric, MetricsSummary, decode_cursor, encode_cursor
from api.src.queries import Cursor


def test_cursor_round_trips_exactly() -> None:
    original = Cursor(
        payment_hour=datetime(2026, 3, 1, 14, 0, 0),
        country_code="NL",
        payment_method="ideal",
    )

    assert decode_cursor(encode_cursor(original)) == original


def test_cursor_token_is_url_safe() -> None:
    # It travels as a query parameter; '+' and '/' from standard base64 would need escaping.
    token = encode_cursor(
        Cursor(payment_hour=datetime(2026, 3, 1), country_code="GB", payment_method="card")
    )

    assert "+" not in token
    assert "/" not in token


@pytest.mark.parametrize(
    "token",
    [
        "not-base64!!",
        "",
        "YWJjZA==",  # valid base64, not JSON
        "eyJoIjogIm5vdC1hLWRhdGUifQ==",  # JSON, unparseable timestamp
        "eyJjIjogIk5MIn0=",  # JSON, missing required keys
    ],
)
def test_malformed_cursor_is_rejected(token: str) -> None:
    # Rejecting beats silently restarting from page 1: a client walking pages would otherwise
    # loop forever without ever seeing an error.
    with pytest.raises(CursorError):
        decode_cursor(token)


def test_gross_volume_serializes_as_a_string() -> None:
    # DECIMAL(18,2) through a JSON number becomes an IEEE double in the browser, reintroducing
    # exactly the rounding the warehouse type exists to prevent.
    metric = HourlyMetric(
        payment_hour=datetime(2026, 3, 1),
        country_code="NL",
        payment_method="ideal",
        payment_count=3,
        gross_volume=Decimal("12345678901234.56"),
        auth_rate=0.75,
    )

    dumped = metric.model_dump(mode="json")

    assert dumped["gross_volume"] == "12345678901234.56"
    assert isinstance(dumped["gross_volume"], str)


def test_summary_allows_a_null_auth_rate_for_an_empty_window() -> None:
    summary = MetricsSummary(
        bucket_count=0, payment_count=0, gross_volume=Decimal("0.00"), auth_rate=None
    )

    assert summary.model_dump(mode="json")["auth_rate"] is None
