from __future__ import annotations

import datetime as dt
import decimal
import json

import boto3
import pytest
from moto import mock_aws

from snowflake_etl.src import stage_to_s3 as module

BUCKET = "test-payments-lake"


@pytest.fixture
def s3_client():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


def test_chunks_handle_decimal_and_datetime() -> None:
    records = [
        {
            "payment_id": 1,
            "amount": decimal.Decimal("149.99"),
            "created_at": dt.datetime(2025, 6, 30, 12, 0, 0),
        }
    ]

    (body, count), = module.iter_jsonl_chunks(records)

    assert count == 1
    parsed = json.loads(body.decode("utf-8").strip())
    assert parsed["amount"] == "149.99"  # Decimal -> string preserves exact money precision
    assert parsed["created_at"] == "2025-06-30T12:00:00"  # datetime -> ISO-8601


def test_serialize_raises_on_unsupported_type() -> None:
    with pytest.raises(TypeError):
        list(module.iter_jsonl_chunks([{"x": object()}]))


def test_chunking_bounds_memory_by_rows_not_extract_size() -> None:
    """The extractor streams; staging must not re-materialize the whole result to undo it."""
    records = ({"payment_id": i} for i in range(250))

    chunks = list(module.iter_jsonl_chunks(records, chunk_rows=100))

    assert [count for _, count in chunks] == [100, 100, 50]
    lines = [line for body, _ in chunks for line in body.decode("utf-8").splitlines()]
    assert len(lines) == 250
    assert json.loads(lines[0])["payment_id"] == 0
    assert json.loads(lines[-1])["payment_id"] == 249


def test_chunk_rows_must_be_positive() -> None:
    with pytest.raises(ValueError):
        list(module.iter_jsonl_chunks([{"a": 1}], chunk_rows=0))


def test_stage_dataset_writes_parts_under_a_run_prefix(s3_client) -> None:  # noqa: ANN001
    records = [{"payment_id": i} for i in range(5)]

    staged = module.stage_dataset(
        s3_client, BUCKET, "payments", records,
        run_id="manual__2026-06-29T10:00:00+00:00",
        run_date=dt.date(2026, 6, 29),
        chunk_rows=2,
    )

    assert staged.row_count == 5
    assert staged.part_count == 3          # 2 + 2 + 1
    assert staged.prefix == "raw/payments/dt=2026-06-29/run=manual__2026-06-29T10-00-00-00-00/"
    assert staged.parts[0].endswith("part-00001.jsonl")
    assert staged.parts[-1].endswith("part-00003.jsonl")

    body = s3_client.get_object(Bucket=BUCKET, Key=staged.parts[0])["Body"].read().decode("utf-8")
    assert [json.loads(line)["payment_id"] for line in body.splitlines()] == [0, 1]


def test_two_runs_on_one_day_do_not_overwrite_each_other(s3_client) -> None:  # noqa: ANN001
    """The old layout wrote one key per dataset per day, so a rerun clobbered the first.

    Both loads then reached RAW under the same METADATA$FILENAME, leaving nothing able to
    tell one snapshot from another -- which is what blocks delete detection downstream.
    """
    day = dt.date(2026, 6, 29)
    first = module.stage_dataset(
        s3_client, BUCKET, "payments", [{"payment_id": 1}, {"payment_id": 2}],
        run_id="run-a", run_date=day,
    )
    second = module.stage_dataset(
        s3_client, BUCKET, "payments", [{"payment_id": 1}], run_id="run-b", run_date=day,
    )

    assert first.prefix != second.prefix
    assert set(first.parts).isdisjoint(second.parts)
    keys = {o["Key"] for o in s3_client.list_objects_v2(Bucket=BUCKET, Prefix="raw/")["Contents"]}
    assert set(first.parts) | set(second.parts) <= keys


def test_an_empty_extract_still_produces_a_well_formed_run(s3_client) -> None:  # noqa: ANN001
    """Otherwise "wrote nothing" and "died before the first part" look identical in S3."""
    staged = module.stage_dataset(
        s3_client, BUCKET, "payments", [], run_id="run-empty", run_date=dt.date(2026, 6, 29)
    )

    assert staged.row_count == 0
    assert staged.part_count == 1
    assert s3_client.get_object(Bucket=BUCKET, Key=staged.parts[0])["Body"].read() == b""


def test_run_id_is_sanitized_for_the_key() -> None:
    assert module.sanitize_run_id("manual__2026-06-29T10:00:00+00:00") == "manual__2026-06-29T10-00-00-00-00"
    assert module.sanitize_run_id("scheduled__2026-06-29") == "scheduled__2026-06-29"
    with pytest.raises(ValueError):
        module.sanitize_run_id("///")


def test_main_stages_only_requested_datasets(s3_client, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(module, "s3_client_from_env", lambda: s3_client)
    monkeypatch.setitem(
        module.DATASET_FACTORIES,
        "fx_rates",
        lambda: [{"rate_date": "2026-06-29", "currency": "EUR", "rate_to_usd": 1.08}],
    )
    payments_called = {"v": False}

    def payments_factory() -> list:
        payments_called["v"] = True  # must NOT run when only fx_rates is requested
        return []

    monkeypatch.setitem(module.DATASET_FACTORIES, "payments", payments_factory)

    module.main(["--bucket", BUCKET, "--datasets", "fx_rates", "--run-date", "2026-06-29"])

    keys = [obj["Key"] for obj in s3_client.list_objects_v2(Bucket=BUCKET).get("Contents", [])]
    assert any("raw/fx_rates/dt=2026-06-29/" in k for k in keys)  # honors --run-date partition
    assert not any("raw/payments/" in k for k in keys)
    assert payments_called["v"] is False  # the unselected source never extracted


def test_main_passes_incremental_window_to_payments(s3_client, monkeypatch) -> None:  # noqa: ANN001
    monkeypatch.setattr(module, "s3_client_from_env", lambda: s3_client)
    captured: dict = {}

    def payments_factory(updated_after=None, updated_before=None):  # noqa: ANN001
        captured["window"] = (updated_after, updated_before)
        return [{"payment_id": 1, "amount": "9.99"}]

    monkeypatch.setitem(module.DATASET_FACTORIES, "payments", payments_factory)

    module.main([
        "--bucket", BUCKET, "--datasets", "payments", "--run-date", "2026-06-30",
        "--updated-after", "2026-06-30 00:00:00", "--updated-before", "2026-07-01 00:00:00",
    ])

    assert captured["window"] == ("2026-06-30 00:00:00", "2026-07-01 00:00:00")
