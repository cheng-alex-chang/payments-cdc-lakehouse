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
# Table IO goes through Iceberg's own S3FileIO. Streaming checkpoints do not -- they are plain
# s3a:// paths in a separate bucket, handled by the Hadoop S3A connector, which is why hadoop-aws
# stays on the classpath. See checkpoint_path() below for why the two are kept apart.

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
        # S3FileIO -- Iceberg's own S3 client, on the AWS SDK v2, rather than routing table IO
        # through the Hadoop FileSystem abstraction. It is the current default for Iceberg on
        # object storage and the reason iceberg-aws-bundle is in the package list.
        #
        # Getting here took a wrong turn worth recording. Iceberg's micro-batch streaming source
        # writes its offset log through the *table's* FileIO, and under a REST catalog with remote
        # signing on, that FileIO is handed a signer endpoint bound to one table id -- so the
        # checkpoint is rejected as a location that is not that table. The apparent fix was to
        # abandon S3FileIO for HadoopFileIO. The real fix was to turn remote signing off on the
        # *catalog*, which scripts/init_iceberg_catalog.py now does explicitly; setting it on the
        # engine achieves nothing, because the REST spec gives server config precedence.
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.endpoint", settings["s3_endpoint"])
        # MinIO addresses buckets by path, not DNS subdomain. Real S3 accepts path style too.
        .config(f"{prefix}.s3.path-style-access", "true")
        # Remote signing off. This line alone does nothing -- the server's value wins -- and the
        # setting that actually takes effect is applied to the warehouse by
        # scripts/init_iceberg_catalog.py. It is kept because it states the engine's expectation:
        # if someone re-enables signing on the catalog, the mismatch is visible here rather than
        # only in a streaming job that fails eleven minutes into a run.
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
    """Structured Streaming checkpoint location for a layer -- its own bucket, not the warehouse.

    Checkpoints deliberately do *not* live in the warehouse bucket, and the reason is the sharpest
    thing this migration taught.

    Iceberg's micro-batch source writes its offset log through the *table's* FileIO. Under a REST
    catalog with remote signing on, that FileIO is handed per-table configuration -- `loadTable`
    returns a signer endpoint bound to one table id and a credential scoped to that table's own
    prefix. So a checkpoint written through it gets signed as if it belonged to the table, and the
    catalog correctly refuses a location that is not that table:

        Table does not exist ... at location `s3://checkpoints/silver/sources/0/offsets/0`

    Signing is therefore disabled on the warehouse itself (scripts/init_iceberg_catalog.py); doing
    it on the engine has no effect, because the REST spec gives the server's config precedence.

    That fixes the error, but the separation stays regardless: a checkpoint is *engine state*, not
    lakehouse data, and it has no business inside a governed warehouse. Two buckets also mean
    resetting a stream can never touch table data. This never arose on HDFS, where checkpoints and
    tables shared one ungoverned filesystem.

    The path is `s3a://`, not `s3://`: this one goes through the Hadoop S3A connector rather than
    S3FileIO, since it is a plain filesystem write with no table behind it.
    """
    import os

    source = os.environ if env is None else env
    bucket = source.get("S3_CHECKPOINT_BUCKET", "checkpoints")
    return f"s3a://{bucket}/{layer}"
