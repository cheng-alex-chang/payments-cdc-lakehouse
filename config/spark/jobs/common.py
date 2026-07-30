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


# --- Iceberg catalog wiring -------------------------------------------------------------------
#
# This block used to be copy-pasted identically into all three Spark jobs. Three copies of the
# same connection details is the drift class this project keeps rediscovering, so it lives here
# once and the jobs call it.
#
# The catalog is an Iceberg REST service and the storage is S3. Neither Spark nor the jobs know
# anything about MinIO: the endpoint is a value, so pointing this at real S3 changes configuration
# rather than code.
#
# io-impl is S3FileIO -- Iceberg's own AWS SDK v2 client -- rather than the Hadoop S3A connector.
# A filesystem-backed catalog would force s3a:// because it resolves its warehouse directory
# through the Hadoop FileSystem API, but a REST catalog does not live on a filesystem, so that
# constraint disappears along with the hadoop-aws dependency.

ICEBERG_CATALOG = "iceberg"


def iceberg_settings(env: dict | None = None) -> dict:
    """Resolve the catalog and storage settings. Pure, so the wiring is testable without Spark."""
    import os

    source = os.environ if env is None else env
    return {
        "uri": source.get("ICEBERG_REST_URI", "http://iceberg-rest:8181/catalog"),
        "warehouse": source.get("ICEBERG_WAREHOUSE", "payments"),
        "s3_endpoint": source.get("S3_ENDPOINT", "http://minio:9000"),
        "s3_region": source.get("S3_REGION", "us-east-1"),
    }


def configure_iceberg(builder, env: dict | None = None):
    """Apply the Iceberg REST + S3 configuration to a SparkSession builder."""
    settings = iceberg_settings(env)
    prefix = f"spark.sql.catalog.{ICEBERG_CATALOG}"
    return (
        builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.type", "rest")
        .config(f"{prefix}.uri", settings["uri"])
        .config(f"{prefix}.warehouse", settings["warehouse"])
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.endpoint", settings["s3_endpoint"])
        # MinIO addresses buckets by path, not DNS subdomain. Real S3 accepts path style too.
        .config(f"{prefix}.s3.path-style-access", "true")
        # Remote signing off. With it on, the catalog signs every S3FileIO request -- including
        # the Structured Streaming offset files, which belong to no table, so the catalog rejects
        # them with "Table does not exist ... at location s3://checkpoints/...". Engines
        # authenticate to S3 directly with the credentials in the environment instead.
        .config(f"{prefix}.s3.remote-signing-enabled", "false")
        .config(f"{prefix}.client.region", settings["s3_region"])
        # Structured Streaming checkpoints are plain s3a:// paths, not Iceberg tables, so they go
        # through the Hadoop S3A connector and need their own endpoint settings. The Iceberg
        # catalog's s3.* keys do not apply to it -- a distinction that costs a failed job to learn.
        .config("spark.hadoop.fs.s3a.endpoint", settings["s3_endpoint"])
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        # No explicit credentials provider: hadoop-aws's default chain already tries the
        # environment (TemporaryAWSCredentialsProvider, SimpleAWSCredentialsProvider,
        # EnvironmentVariableCredentialsProvider, IAMInstanceCredentialsProvider), and naming a
        # provider by hand invites the wrong package -- the env-var one lives in the AWS SDK
        # namespace, not Hadoop's.
    )


def checkpoint_path(layer: str, env: dict | None = None) -> str:
    """Structured Streaming checkpoint location for a layer.

    Kept in its own bucket so resetting checkpoints can never touch table data -- the same reason
    init_object_store.py creates two buckets rather than one.
    """
    import os

    source = os.environ if env is None else env
    bucket = source.get("S3_CHECKPOINT_BUCKET", "checkpoints")
    return f"s3a://{bucket}/{layer}"
