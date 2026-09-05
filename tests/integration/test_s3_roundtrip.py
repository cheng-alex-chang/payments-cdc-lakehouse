"""Real-S3 round-trip for the Phase-2 stager (gated; skipped without AWS creds).

The mocked sibling (tests/test_stage_to_s3.py, moto) proves the serialization + key layout
logic offline and always runs. This one proves the *real* boto3 PUT/GET path against a live
bucket -- the thing moto can't certify. It is opt-in: it stays SKIPPED unless both
AWS credentials and SNOWFLAKE_LAKE_BUCKET are present, so CI and a plain ``pytest`` clone stay green.

Run it in the cloud session with, e.g.::

    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... AWS_REGION=us-east-1 \
    SNOWFLAKE_LAKE_BUCKET=my-payments-lake pytest -m integration tests/integration/test_s3_roundtrip.py
"""
from __future__ import annotations

import datetime as dt
import json
import os
import uuid

import pytest

pytestmark = pytest.mark.integration

_HAS_AWS = bool(os.getenv("AWS_ACCESS_KEY_ID")) and bool(os.getenv("SNOWFLAKE_LAKE_BUCKET"))
_skip = pytest.mark.skipif(
    not _HAS_AWS, reason="set AWS_* credentials and SNOWFLAKE_LAKE_BUCKET to run the real S3 round-trip"
)


@_skip
def test_stage_dataset_roundtrips_through_real_s3() -> None:
    from snowflake_etl.src import stage_to_s3

    bucket = os.environ["SNOWFLAKE_LAKE_BUCKET"]
    client = stage_to_s3.s3_client_from_env()
    records = [{"rate_date": "2026-06-29", "currency": "EUR", "rate_to_usd": 1.08}]
    # Unique prefix so concurrent/repeat runs never clobber each other or real data.
    prefix = f"itest/{uuid.uuid4().hex[:8]}"

    staged = stage_to_s3.stage_dataset(
        client, bucket, "fx_rates", records, run_id="itest-roundtrip",
        run_date=dt.date(2026, 6, 29), prefix=prefix,
    )
    try:
        assert staged.row_count == 1
        assert staged.part_count == 1
        # Run-scoped prefix: two runs on one day no longer overwrite each other, which is
        # what lets stg_payments tell one snapshot from another.
        assert staged.prefix == f"{prefix}/fx_rates/dt=2026-06-29/run=itest-roundtrip/"
        assert staged.parts[0] == f"{staged.prefix}part-00001.jsonl"
        body = client.get_object(Bucket=bucket, Key=staged.parts[0])["Body"].read().decode("utf-8")
        assert json.loads(body.strip())["currency"] == "EUR"
    finally:
        for key in staged.parts:  # leave the bucket as we found it
            client.delete_object(Bucket=bucket, Key=key)
