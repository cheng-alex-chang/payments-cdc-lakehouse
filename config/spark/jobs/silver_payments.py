from __future__ import annotations

import logging

from common import configure_iceberg, checkpoint_path
from payment_rules import (
    ALLOWED_PAYMENT_METHODS,
    ALLOWED_PAYMENT_STATUSES,
    CDC_DELETE_OP,
    CDC_OPS,
    CDC_UPSERT_OPS,
    dlq_merge_sql,
    silver_merge_sql,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    count,
    col,
    current_timestamp,
    from_unixtime,
    get_json_object,
    lit,
    lower,
    regexp_replace,
    row_number,
    sum as spark_sum,
    trim,
    upper,
)
from pyspark.sql.window import Window


BRONZE_TABLE    = "iceberg.analytics.payments_bronze"
SILVER_TABLE    = "iceberg.analytics.payments_silver"
DLQ_TABLE       = "iceberg.analytics.payments_silver_dlq"
CHECKPOINT_PATH = checkpoint_path("silver")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def _write_to_dlq(records: DataFrame, batch_id: int, reason: str) -> None:
    """Record rejects keyed by their Kafka coordinates, so a replay updates rather than adds.

    The append-only version re-inserted the same rows every time a batch was reprocessed,
    which made the table grow with retries instead of with problems.
    """
    spark = records.sparkSession
    (
        records
        .select(
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_value"),
            lit(batch_id).cast("long").alias("batch_id"),
            lit(reason).alias("error_reason"),
            current_timestamp().alias("ingested_at"),
        )
        .createOrReplaceTempView("_silver_dlq_rows")
    )
    spark.sql(dlq_merge_sql(DLQ_TABLE, "_silver_dlq_rows"))
    LOGGER.warning("DLQ batch=%s reason=%s", batch_id, reason)


def _after(field: str):
    return get_json_object(col("kafka_value"), f"$.after.{field}")


def _before(field: str):
    return get_json_object(col("kafka_value"), f"$.before.{field}")


def _micros_to_timestamp(column):
    return from_unixtime(column.cast("double") / 1_000_000).cast("timestamp")


def _build_changes(batch_df: DataFrame) -> DataFrame:
    """Project one CDC batch into a single change set covering all four ops.

    Deletes are part of this set, not a separate pass. Their payload lives in `before`
    rather than `after`, so the identity and timestamp columns coalesce across the two
    sections; every other column stays null on a delete row and is never read, because the
    MERGE's DELETE branch does not reference it.

    `payment_id` and `source_lsn` are the only fields a delete row must supply, which is
    what makes this robust to REPLICA IDENTITY: under Postgres' default, a delete's `before`
    carries only the primary key, so sequencing on `before.updated_at` would sequence on
    null. The LSN comes from `$.source`, which is always populated.
    """
    projected = batch_df.select(
        get_json_object(col("kafka_value"), "$.op").alias("op"),
        get_json_object(col("kafka_value"), "$.source.lsn").cast("long").alias("source_lsn"),
        coalesce(_after("payment_id"), _before("payment_id")).cast("long").alias("payment_id"),
        _after("merchant_id").cast("long").alias("merchant_id"),
        _after("amount").cast("decimal(12,2)").alias("amount"),
        upper(trim(_after("currency"))).alias("currency"),
        regexp_replace(lower(trim(_after("payment_method"))), r"\s+", "_").alias("payment_method"),
        regexp_replace(lower(trim(_after("payment_status"))), r"\s+", "_").alias("payment_status"),
        upper(trim(_after("country_code"))).alias("country_code"),
        _micros_to_timestamp(coalesce(_after("created_at"), _before("created_at"))).alias("created_at"),
        _micros_to_timestamp(coalesce(_after("updated_at"), _before("updated_at"))).alias("updated_at"),
        col("kafka_offset").alias("_kafka_offset"),
        current_timestamp().alias("ingested_at"),
    )
    # Multiple CDC events for one payment can land in a single batch (replay, back-to-back
    # updates, delete-then-recreate). Keep the highest-LSN event per payment_id and let the
    # MERGE decide what it means -- ordering by op, or applying deletes in a second pass,
    # is what made d-then-c resolve as a delete regardless of sequence. kafka_offset only
    # breaks ties among equal LSNs (a snapshot writes one LSN across many rows).
    latest_per_key = Window.partitionBy("payment_id").orderBy(
        col("source_lsn").desc_nulls_last(), col("_kafka_offset").desc()
    )
    return (
        projected
        .withColumn("_rn", row_number().over(latest_per_key))
        .filter(col("_rn") == 1)
        .drop("_rn", "_kafka_offset")
    )


def _validate_upserts(upserts: DataFrame) -> None:
    quality_metrics = (
        upserts
        .select(
            spark_sum(col("payment_id").isNull().cast("int")).alias("null_payment_id"),
            spark_sum(col("merchant_id").isNull().cast("int")).alias("null_merchant_id"),
            spark_sum(col("amount").isNull().cast("int")).alias("null_amount"),
            spark_sum((col("amount") < 0).cast("int")).alias("negative_amount"),
            spark_sum(col("currency").isNull().cast("int")).alias("null_currency"),
            spark_sum((~col("currency").rlike("^[A-Z]{3}$")).cast("int")).alias("invalid_currency"),
            spark_sum(col("payment_method").isNull().cast("int")).alias("null_payment_method"),
            spark_sum((~col("payment_method").isin(*ALLOWED_PAYMENT_METHODS)).cast("int")).alias("invalid_payment_method"),
            spark_sum(col("payment_status").isNull().cast("int")).alias("null_payment_status"),
            spark_sum((~col("payment_status").isin(*ALLOWED_PAYMENT_STATUSES)).cast("int")).alias("invalid_payment_status"),
            spark_sum(col("country_code").isNull().cast("int")).alias("null_country_code"),
            spark_sum((~col("country_code").rlike("^[A-Z]{2}$")).cast("int")).alias("invalid_country_code"),
            spark_sum(col("created_at").isNull().cast("int")).alias("null_created_at"),
            spark_sum(col("updated_at").isNull().cast("int")).alias("null_updated_at"),
            spark_sum((col("updated_at") < col("created_at")).cast("int")).alias("updated_before_created"),
            # Without an LSN the MERGE's sequence guard cannot order this row against
            # silver, so it would be silently dropped by `s.source_lsn > t.source_lsn`.
            spark_sum(col("source_lsn").isNull().cast("int")).alias("null_source_lsn"),
        )
        .collect()[0]
        .asDict()
    )

    duplicate_payment_ids = (
        upserts
        .groupBy("payment_id")
        .agg(count("*").alias("row_count"))
        .filter((col("payment_id").isNotNull()) & (col("row_count") > 1))
        .count()
    )

    failures = {name: value for name, value in quality_metrics.items() if value}
    if duplicate_payment_ids:
        failures["duplicate_payment_ids"] = duplicate_payment_ids

    if failures:
        raise ValueError(f"Silver data quality checks failed: {failures}")


def _upsert_to_silver(batch_df: DataFrame, batch_id: int) -> None:
    spark = batch_df.sparkSession

    # A Debezium tombstone is a real record with a null value, emitted after every delete
    # when tombstones.on.delete is on. It is not malformed and does not belong in the DLQ --
    # the op='d' event that precedes it already carries the delete. The connector now sets
    # tombstones.on.delete=false, so this only drains the ones already sitting in bronze
    # and keeps the DLQ clean if anyone turns the setting back on.
    records = batch_df.filter(col("kafka_value").isNotNull())

    op_col = get_json_object(col("kafka_value"), "$.op")
    malformed = records.filter(op_col.isNull())
    if not malformed.isEmpty():
        _write_to_dlq(malformed, batch_id, "null_op")

    known_ops = records.filter(op_col.isNotNull())
    unexpected = known_ops.filter(~op_col.isin(*CDC_OPS))
    if not unexpected.isEmpty():
        _write_to_dlq(unexpected, batch_id, "unexpected_op")

    processable = known_ops.filter(op_col.isin(*CDC_OPS))

    changes = _build_changes(processable)
    if changes.isEmpty():
        return

    # Deletes carry only payment_id and source_lsn; the data-quality rules describe a
    # payment's columns, so they apply to the upsert rows alone.
    upserts = changes.filter(col("op").isin(*CDC_UPSERT_OPS))
    if not upserts.isEmpty():
        _validate_upserts(upserts)

    deletes_in_batch = changes.filter(col("op") == CDC_DELETE_OP).count()
    if deletes_in_batch:
        LOGGER.info("batch=%s applying %d delete(s) to silver", batch_id, deletes_in_batch)

    # One sequenced MERGE for all four ops. See payment_rules.silver_merge_sql for why the
    # LSN guard sits on both MATCHED branches and why deletes are not a separate pass.
    changes.createOrReplaceTempView("_silver_changes")
    spark.sql(silver_merge_sql(SILVER_TABLE, "_silver_changes"))


def main() -> None:
    LOGGER.info("Starting silver transformation from %s", BRONZE_TABLE)
    spark = (
        configure_iceberg(
            SparkSession.builder.appName("silver-payments")
        )
        .getOrCreate()
    )

    # Keyed on (kafka_partition, kafka_offset) so replaying a batch updates its rejects
    # instead of appending a second copy. Column order mirrors payment_rules.DLQ_COLUMNS.
    #
    # MIGRATION: the coordinate columns are new. IF NOT EXISTS leaves an existing DLQ on the
    # old schema and the MERGE then fails on the unknown columns; add them with ALTER TABLE
    # or drop the table -- it is diagnostic, not a source of truth.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {DLQ_TABLE} (
            kafka_partition INT,
            kafka_offset    BIGINT,
            kafka_value     STRING,
            batch_id        BIGINT,
            error_reason    STRING,
            ingested_at     TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(ingested_at))
    """)

    # Column order mirrors payment_rules.SILVER_COLUMNS, which also drives the MERGE's
    # explicit column lists; tests/test_silver_cdc.py asserts the two agree.
    #
    # MIGRATION: source_lsn is new. IF NOT EXISTS leaves an already-created silver table on
    # the old schema, and the MERGE then fails on the unknown column. An existing deployment
    # needs `ALTER TABLE ... ADD COLUMN source_lsn BIGINT` (existing rows get NULL, so the
    # first change per key wins on `s.source_lsn > t.source_lsn` being NULL-false -- backfill
    # with 0 if you want those rows to be beatable) or a silver rebuild from bronze.
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_TABLE} (
            payment_id     BIGINT,
            merchant_id    BIGINT,
            amount         DECIMAL(12,2),
            currency       STRING,
            payment_method STRING,
            payment_status STRING,
            country_code   STRING,
            created_at     TIMESTAMP,
            updated_at     TIMESTAMP,
            source_lsn     BIGINT,
            ingested_at    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(created_at))
    """)

    query = (
        spark.readStream
        .format("iceberg")
        .load(BRONZE_TABLE)
        .writeStream
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .foreachBatch(_upsert_to_silver)
        .start()
    )
    query.awaitTermination()
    LOGGER.info("Silver streaming job completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
