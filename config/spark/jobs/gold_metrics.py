from __future__ import annotations

import logging

from common import configure_iceberg
from pyspark.sql import SparkSession


SILVER_TABLE = "iceberg.analytics.payments_silver"
GOLD_TABLE   = "iceberg.analytics.payment_metrics_gold"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)


def _build_spark_session() -> SparkSession:
    return (
        configure_iceberg(
            SparkSession.builder.appName("gold-metrics")
        )
        .getOrCreate()
    )


def main() -> None:
    LOGGER.info("Starting gold aggregation from %s", SILVER_TABLE)
    spark = _build_spark_session()

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            payment_hour   TIMESTAMP,
            country_code   STRING,
            payment_method STRING,
            payment_count     BIGINT,
            gross_volume      DECIMAL(18,2),
            authorized_volume DECIMAL(18,2),
            auth_rate         DOUBLE
        )
        USING iceberg
        PARTITIONED BY (days(payment_hour))
    """)

    # Metric definitions -- the SQL was always valid; what was missing was saying what it
    # means, and "gross volume" is ambiguous enough that readers disagree about it.
    #
    #   gross_volume      total ATTEMPTED payment amount, across every payment_status:
    #                     authorized, failed, cancelled, pending, refunded, chargeback.
    #                     $100 authorized + $200 failed + $300 cancelled = $600.
    #   authorized_volume the subset whose status is authorized. $100 in that example.
    #   auth_rate         authorized count / attempted count, at this group's grain.
    #
    # These describe CURRENT state, not lifecycle: silver holds one row per payment at its
    # latest version, so a payment now sitting at `refunded` still contributes to
    # gross_volume, and authorized_volume counts payments authorized *now* rather than every
    # payment ever authorized. "Ever-authorized volume" needs event history and cannot come
    # from this table. Net settlement volume needs refunds, which have no medallion yet.
    #
    # Full idempotent recompute from silver. INSERT OVERWRITE atomically replaces every
    # row, so hours whose payments were all deleted from silver drop out of gold cleanly.
    # Gold reads only silver (linear bronze -> silver -> gold lineage); it never touches
    # bronze or the raw Debezium envelope.
    spark.sql(f"""
        INSERT OVERWRITE TABLE {GOLD_TABLE}
        SELECT
            date_trunc('hour', created_at)                                     AS payment_hour,
            country_code,
            payment_method,
            count(*)                                                            AS payment_count,
            CAST(sum(amount) AS DECIMAL(18,2))                                  AS gross_volume,
            CAST(sum(CASE WHEN payment_status = 'authorized' THEN amount ELSE 0 END)
                 AS DECIMAL(18,2))                                              AS authorized_volume,
            avg(CASE WHEN payment_status = 'authorized' THEN 1.0 ELSE 0.0 END) AS auth_rate
        FROM {SILVER_TABLE}
        GROUP BY 1, 2, 3
    """)

    LOGGER.info("Gold aggregation completed")
    spark.stop()


if __name__ == "__main__":  # pragma: no cover
    main()
