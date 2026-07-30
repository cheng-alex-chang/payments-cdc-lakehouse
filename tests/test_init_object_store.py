"""Tests for the S3 bucket bootstrap that replaces init_hdfs.py."""
from __future__ import annotations

import pytest

from scripts import init_object_store as module


def test_settings_apply_defaults_for_the_cluster_endpoint() -> None:
    resolved = module.settings({"S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s"})

    # minio:9000 resolves as a Compose service name and a Kubernetes Service DNS name alike,
    # which is what keeps one Airflow task working in both runtimes.
    assert resolved["S3_ENDPOINT"] == "http://minio:9000"
    assert resolved["S3_REGION"] == "us-east-1"


@pytest.mark.parametrize("missing", ["S3_ACCESS_KEY", "S3_SECRET_KEY"])
def test_settings_reject_missing_credentials(missing: str) -> None:
    env = {"S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s"}
    env[missing] = ""

    with pytest.raises(ValueError, match=missing):
        module.settings(env)


def test_settings_reject_an_endpoint_without_a_scheme() -> None:
    # boto3 accepts endpoint_url without a scheme and then fails at connect time with a much
    # less obvious error, so this is caught up front.
    with pytest.raises(ValueError, match="scheme"):
        module.settings({"S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s", "S3_ENDPOINT": "minio:9000"})


class _Client:
    def __init__(self, raises: Exception | None = None) -> None:
        self.raises = raises
        self.created: list[str] = []

    def create_bucket(self, Bucket: str) -> None:  # noqa: N803 -- boto3's parameter name
        if self.raises is not None:
            raise self.raises
        self.created.append(Bucket)


def _error(name: str) -> Exception:
    """botocore generates exception classes at runtime; match on the class name as the code does."""
    return type(name, (Exception,), {})()


def test_make_bucket_creates_when_absent() -> None:
    client = _Client()

    assert module.make_bucket(client, "warehouse") is True
    assert client.created == ["warehouse"]


@pytest.mark.parametrize("name", ["BucketAlreadyOwnedByYou", "BucketAlreadyExists"])
def test_make_bucket_is_idempotent(name: str) -> None:
    """Re-running the task must be safe -- MKDIRS was idempotent and this has to match."""
    client = _Client(raises=_error(name))

    assert module.make_bucket(client, "warehouse") is False


def test_make_bucket_propagates_real_failures() -> None:
    # An idempotency shortcut that swallows every error would turn a credentials or networking
    # problem into a silently successful task.
    client = _Client(raises=_error("AccessDenied"))

    with pytest.raises(Exception) as caught:  # noqa: PT011 -- botocore's classes are generated
        module.make_bucket(client, "warehouse")
    assert type(caught.value).__name__ == "AccessDenied"


def test_buckets_separate_table_data_from_streaming_checkpoints() -> None:
    # Resetting Spark checkpoints must never be able to touch Iceberg data.
    assert module.BUCKETS == ("warehouse", "checkpoints")


def test_boto3_is_imported_lazily() -> None:
    """Same rule as the Trino driver: importing the module must not require the SDK."""
    import inspect

    source = inspect.getsource(module)
    assert "import boto3" in source
    assert not source.startswith("import boto3")
    # The import lives inside build_client, so `settings` stays testable without boto3 installed.
    assert "import boto3" in inspect.getsource(module.build_client)
