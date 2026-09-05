from __future__ import annotations

import logging
import os

from common import configure_iceberg, checkpoint_path
from payment_rules import bronze_duplicate_offsets_sql, mask_pii_fields
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC     = "cdc.public.payments"
BRONZE_TABLE    = "iceberg.analytics.payments_bronze"
CHECKPOINT_PATH = checkpoint_path("bronze")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


class BronzeReplayDetected(RuntimeError):
    """Bronze holds the same Kafka record twice -- the topic was re-consumed."""


def assert_no_replayed_offsets(spark) -> None:  # noqa: ANN001 - SparkSession
    """Fail the run if any (topic, partition, offset) appears in bronze more than once.

    Bronze is append-only and reads from ``startingOffsets=earliest``, so the only thing
    standing between a restart and re-consuming the whole topic is the streaming checkpoint
    -- which deliberately lives in its own bucket, away from the warehouse. That separation
    is right for blast radius, and it also means losing the checkpoint bucket duplicates the
    entire event history into an append-only table.

    Detecting it costs one aggregate and turns a silent doubling into a failed task. The
    recovery is not automatic and must not be: see the recovery contract in
    docs/architecture-lakehouse.md. Kafka retention is a window, not an archive, so a rebuild
    from the topic is only complete while the affected offsets are still retained.
    """
    duplicates = spark.sql(bronze_duplicate_offsets_sql(BRONZE_TABLE))
    offending = duplicates.count()
    if offending:
        sample = [row.asDict() for row in duplicates.limit(5).collect()]
        raise BronzeReplayDetected(
            f"{offending} Kafka coordinate(s) appear more than once in {BRONZE_TABLE}: "
            f"{sample}. Bronze has re-consumed the topic -- most likely the checkpoint at "
            f"{CHECKPOINT_PATH} was lost. Do not simply re-run: follow the recovery contract "
            f"in docs/architecture-lakehouse.md."
        )
    LOGGER.info("Bronze offset uniqueness verified for %s", BRONZE_TABLE)


def main() -> None:
    LOGGER.info("Starting bronze Kafka ingestion from topic '%s'", KAFKA_TOPIC)
    spark = (
        configure_iceberg(
            SparkSession.builder.appName("bronze-from-kafka")
        )
        .getOrCreate()
    )

    # mask_pii_fields runs inside a Python UDF, so it executes in a worker process rather
    # than the driver. While it was defined in this file cloudpickle serialized its body;
    # imported from a module it serializes a reference instead, and the worker fails with
    # ModuleNotFoundError because only the submitted script's directory is on the driver's
    # path. addPyFile ships the module to the executors.
    #
    # Resolved next to this file so it works in every runtime: Compose bind-mounts the repo
    # at /opt/project, and the Kubernetes Jobs mount the spark-jobs ConfigMap at the same
    # path, with payment_rules.py alongside.
    spark.sparkContext.addPyFile(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "payment_rules.py")
    )

    spark.sql("CREATE DATABASE IF NOT EXISTS iceberg.analytics")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
            kafka_key       STRING,
            kafka_value     STRING,
            kafka_topic     STRING,
            kafka_partition INT,
            kafka_offset    BIGINT,
            kafka_timestamp TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(kafka_timestamp))
    """)

    mask_udf = udf(mask_pii_fields, StringType())

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(
            col("key").cast("string").alias("kafka_key"),
            col("value").cast("string").alias("kafka_value"),
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("kafka_value", mask_udf(col("kafka_value")))
    )

    query = (
        stream.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(availableNow=True)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .toTable(BRONZE_TABLE)
    )
    query.awaitTermination()

    assert_no_replayed_offsets(spark)

    LOGGER.info("Bronze streaming job completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
