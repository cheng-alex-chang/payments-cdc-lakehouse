"""Create the S3 buckets the pipeline writes into.

The object-storage replacement for init_hdfs.py, and the same idea: speak the service's own HTTP
API rather than shelling into a container, so one Airflow task covers both runtimes. `minio:9000`
resolves as a Compose service name and a Kubernetes Service DNS name alike.

boto3 is used rather than the MinIO client because it is already a dependency (requirements-ci.txt,
for the Snowflake S3 staging) and because it is the *real* S3 API -- pointing it at AWS instead of
MinIO is a change of endpoint and credentials, nothing else. Committing to the MinIO SDK would tie
the pipeline to MinIO.

`CreateBucket` on an existing bucket raises rather than succeeding quietly, which is the one place
this differs from WebHDFS MKDIRS. Both `BucketAlreadyOwnedByYou` and `BucketAlreadyExists` are
treated as success so the Airflow task can retry without special-casing.
"""
from __future__ import annotations

import logging
import os
from typing import Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULTS = {
    # S3_ENDPOINT rather than MINIO_ENDPOINT: the pipeline talks S3, and the only thing that
    # changes when this points at AWS is the value.
    "S3_ENDPOINT": "http://minio:9000",
    "S3_REGION": "us-east-1",
    "S3_ACCESS_KEY": "",
    "S3_SECRET_KEY": "",
}

# Only the warehouse. Streaming checkpoints deliberately live on a volume rather than in object
# storage -- see checkpoint_path() in config/spark/jobs/common.py for why a governed bucket cannot
# hold them.
BUCKETS = ("warehouse", "checkpoints")


def settings(env: dict[str, str] | None = None) -> dict[str, str]:
    """Resolve connection settings, applying defaults. Pure and env-injectable for testing."""
    source = os.environ if env is None else env
    resolved = {key: source.get(key, default) for key, default in DEFAULTS.items()}

    for required in ("S3_ACCESS_KEY", "S3_SECRET_KEY"):
        if not resolved[required]:
            raise ValueError(f"{required} must be set")

    endpoint = resolved["S3_ENDPOINT"]
    if not endpoint.startswith(("http://", "https://")):
        raise ValueError(f"S3_ENDPOINT must include a scheme, got {endpoint!r}")

    return resolved


def build_client(env: dict[str, str] | None = None) -> Any:
    """Open an S3 client. Imports boto3 lazily so `settings` stays testable without it."""
    import boto3  # noqa: PLC0415

    resolved = settings(env)
    return boto3.client(
        "s3",
        endpoint_url=resolved["S3_ENDPOINT"],
        aws_access_key_id=resolved["S3_ACCESS_KEY"],
        aws_secret_access_key=resolved["S3_SECRET_KEY"],
        region_name=resolved["S3_REGION"],
    )


def make_bucket(client: Any, bucket: str) -> bool:
    """Create a bucket, treating "already there" as success. Returns True if it was created."""
    try:
        client.create_bucket(Bucket=bucket)
        return True
    except Exception as exc:  # noqa: BLE001 -- botocore's exception classes are generated
        name = type(exc).__name__
        if name in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            return False
        raise


def main() -> None:
    client = build_client()
    LOGGER.info("Ensuring %s S3 buckets exist", len(BUCKETS))
    for bucket in BUCKETS:
        created = make_bucket(client, bucket)
        LOGGER.info("  %s %s", "created" if created else "exists ", bucket)
    LOGGER.info("Object store layout ready")


if __name__ == "__main__":  # pragma: no cover
    main()
