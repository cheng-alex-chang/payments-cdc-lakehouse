# Databricks notebook source
# MAGIC %md
# MAGIC # Payments medallion as a Lakeflow Declarative Pipeline (DLT)
# MAGIC
# MAGIC Declarative port of the medallion contract from `config/spark/jobs/*`:
# MAGIC
# MAGIC - **Bronze** — Auto Loader ingests the Debezium envelopes from the Volume; PII is masked.
# MAGIC - **Silver** — parsed/canonicalized change feed, upserted with **AUTO CDC**
# MAGIC   (`apply_changes`, SCD type 1, deletes via `op='d'`); the data-quality rules are
# MAGIC   **expectations** (`expect_all_or_drop`) so DLT tracks pass/drop counts and lineage.
# MAGIC - **Gold** — hourly metrics materialized view over Silver.
# MAGIC
# MAGIC DLT manages checkpoints, incremental state, and the dependency DAG; tables publish to
# MAGIC the pipeline's target catalog/schema (`workspace.analytics`). Self-contained on purpose
# MAGIC (no sibling imports) to avoid Free Edition's FUSE read flakiness.

# COMMAND ----------

import hashlib
import json

import dlt
from pyspark.sql.functions import (
    avg, coalesce, col, count, current_timestamp, date_trunc, expr,
    from_unixtime, get_json_object, lit, lower, regexp_replace, sum as spark_sum,
    trim, udf, upper, when,
)
from pyspark.sql.types import StringType

CATALOG, SCHEMA, VOLUME = "workspace", "analytics", "landing"
SEED_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/payments"
CDC_TOPIC = "cdc.public.payments"
PII_FIELDS = {"shopper_id"}

ALLOWED_PAYMENT_METHODS = ("apple_pay", "bank_transfer", "card", "google_pay", "paypal")
ALLOWED_PAYMENT_STATUSES = ("authorized", "cancelled", "chargeback", "failed", "pending", "refunded")


def mask_pii_fields(value):
    if value is None:
        return None
    try:
        envelope = json.loads(value)
        for section in ("before", "after"):
            if isinstance(envelope.get(section), dict):
                for field in PII_FIELDS:
                    if field in envelope[section] and envelope[section][field] is not None:
                        raw = str(envelope[section][field]).encode()
                        envelope[section][field] = hashlib.sha256(raw).hexdigest()
        return json.dumps(envelope)
    except (json.JSONDecodeError, TypeError):
        return value


mask_udf = udf(mask_pii_fields, StringType())


def _after(field):
    return get_json_object(col("kafka_value"), f"$.after.{field}")


def _before(field):
    return get_json_object(col("kafka_value"), f"$.before.{field}")


# COMMAND ----------

# MAGIC %md ## Bronze -- raw Debezium envelopes via Auto Loader (PII masked)

# COMMAND ----------

@dlt.table(
    name="payments_bronze",
    comment="Raw Debezium envelopes ingested from the landing Volume; shopper_id hashed.",
    table_properties={"quality": "bronze"},
)
def payments_bronze():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "text")
        .load(SEED_DIR)
        .select(
            get_json_object(col("value"), "$.after.payment_id").alias("kafka_key"),
            col("value").alias("kafka_value"),
            lit(CDC_TOPIC).alias("kafka_topic"),
            coalesce(
                get_json_object(col("value"), "$.after.payment_id"),
                get_json_object(col("value"), "$.before.payment_id"),
            ).cast("long").alias("kafka_offset"),
            current_timestamp().alias("kafka_timestamp"),
        )
        .withColumn("kafka_value", mask_udf(col("kafka_value")))
    )


# COMMAND ----------

# MAGIC %md ## Silver -- parsed change feed, AUTO CDC upsert, DQ expectations

# COMMAND ----------

@dlt.view(name="payments_silver_changes")
def payments_silver_changes():
    op = get_json_object(col("kafka_value"), "$.op")
    return (
        dlt.read_stream("payments_bronze")
        .filter(op.isin("c", "u", "r", "d"))
        .select(
            op.alias("op"),
            coalesce(_after("payment_id"), _before("payment_id")).cast("long").alias("payment_id"),
            _after("merchant_id").cast("long").alias("merchant_id"),
            _after("amount").cast("decimal(12,2)").alias("amount"),
            upper(trim(_after("currency"))).alias("currency"),
            regexp_replace(lower(trim(_after("payment_method"))), r"\s+", "_").alias("payment_method"),
            regexp_replace(lower(trim(_after("payment_status"))), r"\s+", "_").alias("payment_status"),
            upper(trim(_after("country_code"))).alias("country_code"),
            from_unixtime(coalesce(_after("created_at"), _before("created_at")).cast("double") / 1_000_000).cast("timestamp").alias("created_at"),
            from_unixtime(coalesce(_after("updated_at"), _before("updated_at")).cast("double") / 1_000_000).cast("timestamp").alias("updated_at"),
        )
    )


_methods = ", ".join(f"'{m}'" for m in ALLOWED_PAYMENT_METHODS)
_statuses = ", ".join(f"'{s}'" for s in ALLOWED_PAYMENT_STATUSES)

dlt.create_streaming_table(
    name="payments_silver",
    comment="Validated, deduplicated payments (SCD type 1 via AUTO CDC).",
    table_properties={"quality": "silver"},
    expect_all_or_drop={
        "valid_payment_id": "payment_id IS NOT NULL",
        "non_negative_amount": "amount >= 0",
        "valid_currency": "currency RLIKE '^[A-Z]{3}$'",
        "valid_country": "country_code RLIKE '^[A-Z]{2}$'",
        "known_method": f"payment_method IN ({_methods})",
        "known_status": f"payment_status IN ({_statuses})",
        "ordered_timestamps": "updated_at >= created_at",
    },
)

dlt.apply_changes(
    target="payments_silver",
    source="payments_silver_changes",
    keys=["payment_id"],
    sequence_by=col("updated_at"),
    apply_as_deletes=expr("op = 'd'"),
    except_column_list=["op"],
    stored_as_scd_type=1,
)


# COMMAND ----------

# MAGIC %md ## Gold -- hourly metrics by country and payment method

# COMMAND ----------

@dlt.table(
    name="payment_metrics_gold",
    comment="Hourly payment metrics by country and method.",
    table_properties={"quality": "gold"},
)
def payment_metrics_gold():
    return (
        dlt.read("payments_silver")
        .groupBy(
            date_trunc("hour", col("created_at")).alias("payment_hour"),
            col("country_code"),
            col("payment_method"),
        )
        .agg(
            count(lit(1)).alias("payment_count"),
            # gross_volume is ATTEMPTED volume -- every status, matching the Spark gold job.
            # authorized_volume is the authorized subset. Both are current-state, not
            # lifecycle: a payment now at `refunded` still counts toward gross_volume.
            spark_sum("amount").cast("decimal(18,2)").alias("gross_volume"),
            spark_sum(
                when(col("payment_status") == "authorized", col("amount")).otherwise(0)
            ).cast("decimal(18,2)").alias("authorized_volume"),
            avg(when(col("payment_status") == "authorized", 1.0).otherwise(0.0)).alias("auth_rate"),
        )
    )
