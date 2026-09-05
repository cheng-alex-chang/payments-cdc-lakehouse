"""Stage extracted records to AWS S3 as newline-delimited JSON (Phase 2).

Consumes the Phase-1 extractor streams and writes them as chunked part objects under a
prefix that identifies the run:

    raw/<dataset>/dt=<run_date>/run=<run_id>/part-00001.jsonl

which a Snowflake external stage + ``COPY INTO`` reads a whole ``dt=`` partition at a time
(Phase 3). Two properties come from that layout, and neither is cosmetic:

* **Bounded memory.** One object per chunk of rows, streamed, so peak usage tracks the chunk
  size rather than the extract size.
* **Run identity.** Every run has its own prefix, so ``METADATA$FILENAME`` in RAW says which
  run a row came from. Without that, ``stg_payments`` cannot tell one snapshot from another
  and so cannot treat the newest one as the current state of the source -- which is what
  makes deleted payments disappear downstream.

**The bucket is ``SNOWFLAKE_LAKE_BUCKET``, never ``S3_BUCKET``.** Both DAGs run on one
Airflow deployment and therefore one environment, and ``S3_BUCKET`` already means the
local lakehouse's Iceberg warehouse bucket there (``scripts/init_object_store.py``,
``scripts/init_iceberg_catalog.py``, default ``warehouse``). One name for two buckets
points whichever DAG is configured second at the other's storage: the catalog bootstrap
fails validating a bucket MinIO does not have, or staging writes payment extracts into
the warehouse bucket.

This is also where the serialization the extractors deferred happens: ``Decimal`` money values
are written as JSON *strings* to preserve exact NUMERIC precision (never as floats), and
timestamps as ISO-8601.
"""
from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import decimal
import json
import logging
import os
import re
from collections.abc import Iterable, Iterator
from typing import Any

import boto3

from snowflake_etl.src import extract_fx_rates, extract_payments

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULT_PREFIX = "raw"
# Rows per part object. Bounded so peak memory is a function of this, not of extract size.
DEFAULT_CHUNK_ROWS = 100_000


def _json_default(value: Any) -> str:
    """Serialize the non-JSON-native types the extractors emit."""
    if isinstance(value, decimal.Decimal):
        return str(value)  # exact precision for money -- never coerce to float
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    raise TypeError(f"Cannot serialize value of type {type(value).__name__}")


def iter_jsonl_chunks(
    records: Iterable[dict], *, chunk_rows: int = DEFAULT_CHUNK_ROWS
) -> Iterator[tuple[bytes, int]]:
    """Yield ``(body, row_count)`` per part, never holding more than ``chunk_rows`` rows.

    This is the whole point of the part layout. The extractor streams through a server-side
    cursor in bounded memory; rendering the result into one buffer and then encoding it
    undid that twice over -- the full text, plus a full bytes copy -- so a 5M-row extract
    peaked at roughly twice the dataset in RAM and could take the Airflow worker down with
    it. (``put_object`` also caps a single object at 5 GB.)
    """
    if chunk_rows < 1:
        raise ValueError("chunk_rows must be at least 1")

    lines: list[str] = []
    for record in records:
        lines.append(json.dumps(record, default=_json_default))
        if len(lines) >= chunk_rows:
            yield ("\n".join(lines) + "\n").encode("utf-8"), len(lines)
            lines = []
    if lines:
        yield ("\n".join(lines) + "\n").encode("utf-8"), len(lines)


@dataclasses.dataclass(frozen=True)
class StagedSnapshot:
    """What one staging run wrote, and what the loader must find to call it complete."""

    dataset: str
    run_id: str
    run_date: dt.date
    prefix: str
    parts: tuple[str, ...]
    row_count: int

    @property
    def part_count(self) -> int:
        return len(self.parts)


def sanitize_run_id(run_id: str) -> str:
    """Reduce an Airflow run_id to characters that read back cleanly out of an S3 key.

    Airflow hands out ids like ``manual__2026-09-04T10:00:00+00:00``. The colons and plus
    survive in an S3 key but make the key awkward to quote and to match; staging normalizes
    them so ``REGEXP_SUBSTR(source_file, 'run=([^/]+)')`` in stg_payments stays trivial.
    """
    cleaned = re.sub(r"[^A-Za-z0-9_-]+", "-", run_id).strip("-")
    if not cleaned:
        raise ValueError(f"run_id {run_id!r} has no usable characters")
    return cleaned


def snapshot_prefix(dataset: str, run_date: dt.date, run_id: str, *, prefix: str = DEFAULT_PREFIX) -> str:
    return f"{prefix}/{dataset}/dt={run_date.isoformat()}/run={sanitize_run_id(run_id)}/"


def stage_dataset(
    s3_client: Any,
    bucket: str,
    dataset: str,
    records: Iterable[dict],
    *,
    run_id: str,
    run_date: dt.date | None = None,
    prefix: str = DEFAULT_PREFIX,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
) -> StagedSnapshot:
    """Stream ``records`` into one object per chunk under this run's own prefix.

    The run id is part of the key on purpose. The old layout wrote one deterministic object
    per dataset per day, so a same-day rerun overwrote it and both COPY loads landed in RAW
    carrying the identical ``METADATA$FILENAME`` -- nothing downstream could tell run 1 from
    run 2, which is what blocks treating the newest snapshot as the current source state.
    """
    run_date = run_date or dt.date.today()
    key_prefix = snapshot_prefix(dataset, run_date, run_id, prefix=prefix)

    parts: list[str] = []
    total = 0
    for body, rows in iter_jsonl_chunks(records, chunk_rows=chunk_rows):
        key = f"{key_prefix}part-{len(parts) + 1:05d}.jsonl"
        s3_client.put_object(
            Bucket=bucket, Key=key, Body=body, ContentType="application/x-ndjson"
        )
        parts.append(key)
        total += rows

    if not parts:
        # An empty extract still has to produce a well-formed run, or "staging wrote
        # nothing" and "staging died before its first part" look identical in S3. The
        # loader decides whether a zero-row snapshot is acceptable; staging does not.
        key = f"{key_prefix}part-00001.jsonl"
        s3_client.put_object(Bucket=bucket, Key=key, Body=b"", ContentType="application/x-ndjson")
        parts.append(key)

    LOGGER.info(
        "Staged %d %s rows to s3://%s/%s as %d part(s)", total, dataset, bucket, key_prefix, len(parts)
    )
    return StagedSnapshot(
        dataset=dataset,
        run_id=sanitize_run_id(run_id),
        run_date=run_date,
        prefix=key_prefix,
        parts=tuple(parts),
        row_count=total,
    )


def s3_client_from_env() -> Any:
    """Standard boto3 S3 client; credentials/region come from the usual AWS_* env / profile."""
    return boto3.client("s3", region_name=os.getenv("AWS_REGION", "us-east-1"))


def _fx_records() -> Iterable[dict]:
    start, end = extract_fx_rates.default_window()
    return (dataclasses.asdict(rate) for rate in extract_fx_rates.fetch_fx_rates(start, end))


def _payment_records(
    updated_after: str | None = None, updated_before: str | None = None
) -> Iterable[dict]:
    conn = extract_payments.connect_from_env()
    try:
        yield from extract_payments.fetch_payments(
            conn, updated_after=updated_after, updated_before=updated_before
        )
    finally:
        conn.close()


# dataset name -> zero-arg factory. Called lazily so staging one source never triggers the
# other's extract (e.g. staging fx_rates alone won't open a Postgres connection).
DATASET_FACTORIES = {
    "fx_rates": _fx_records,
    "payments": _payment_records,
}


def main(argv: list[str] | None = None) -> list[StagedSnapshot] | None:
    parser = argparse.ArgumentParser(description="Stage FX rates + payments to S3.")
    # See the module docstring: this is SNOWFLAKE_LAKE_BUCKET, never S3_BUCKET.
    parser.add_argument(
        "--bucket",
        default=os.getenv("SNOWFLAKE_LAKE_BUCKET"),
        help="Target S3 bucket for the raw lake (env: SNOWFLAKE_LAKE_BUCKET)",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=sorted(DATASET_FACTORIES),
        default=sorted(DATASET_FACTORIES),
        help="Which datasets to extract + stage (default: all). Lets the DAG run them in parallel.",
    )
    parser.add_argument(
        "--run-date",
        default=None,
        help="Partition date YYYY-MM-DD (default: today). The DAG passes Airflow's {{ ds }}.",
    )
    parser.add_argument(
        "--updated-after",
        default=None,
        help="Incremental watermark: only payments with updated_at >= this (the DAG passes "
        "Airflow's data_interval_start). Omit for a full-snapshot extract.",
    )
    parser.add_argument(
        "--updated-before",
        default=None,
        help="Upper bound of the change window (exclusive); pairs with --updated-after.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Unique id for this staging run; becomes the run=<id> key segment. The DAG "
        "passes Airflow's {{ run_id }}. Defaults to a UTC timestamp for CLI use.",
    )
    parser.add_argument(
        "--chunk-rows",
        type=int,
        default=DEFAULT_CHUNK_ROWS,
        help=f"Rows per part object (default {DEFAULT_CHUNK_ROWS}); bounds staging memory",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Serialize from real sources and print a sample, without touching S3",
    )
    args = parser.parse_args(argv)
    run_date = dt.date.fromisoformat(args.run_date) if args.run_date else None
    run_id = args.run_id or dt.datetime.now(dt.timezone.utc).strftime("cli-%Y%m%dT%H%M%SZ")

    def records_for(name: str) -> Iterable[dict]:
        # The change window only applies to payments; FX is windowed by --start/--end
        # in its own extractor and always staged in full here.
        if name == "payments":
            return DATASET_FACTORIES[name](
                updated_after=args.updated_after, updated_before=args.updated_before
            )
        return DATASET_FACTORIES[name]()

    if args.dry_run:
        for name in args.datasets:
            rows = 0
            parts = 0
            preview: list[str] = []
            # Consumed chunk by chunk, like the real path -- a dry run that buffered the
            # whole extract would hide the very problem the chunking exists to fix.
            for body, count in iter_jsonl_chunks(records_for(name), chunk_rows=args.chunk_rows):
                rows += count
                parts += 1
                if not preview:
                    preview = body.decode("utf-8").splitlines()[:2]
            LOGGER.info("[dry-run] %s: %d rows across %d part(s)", name, rows, parts)
            for line in preview:
                print(f"{name}: {line}")
        return

    if not args.bucket:
        raise SystemExit("--bucket (or SNOWFLAKE_LAKE_BUCKET) is required when not in --dry-run")
    client = s3_client_from_env()
    staged = [
        stage_dataset(
            client,
            args.bucket,
            name,
            records_for(name),
            run_id=run_id,
            run_date=run_date,
            chunk_rows=args.chunk_rows,
        )
        for name in args.datasets
    ]
    # Returned so the Airflow task can register expected_parts / expected_rows for this run;
    # without them the loader has nothing to reconcile the COPY result against.
    return staged


if __name__ == "__main__":  # pragma: no cover
    main()
