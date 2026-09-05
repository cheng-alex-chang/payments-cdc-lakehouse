-- Grain: one row per payment (latest version) in the CURRENT state of the source.
--
-- Scoped to one snapshot, and that scoping is the whole delete story. RAW accumulates every
-- run's rows forever; deduping across all of them by payment_id keeps the last version ever
-- seen, so a payment deleted in Postgres simply stops arriving and lives on in the fact and
-- the marts indefinitely. Nothing catches it -- the row-count reconciliation sees the ghost
-- on both sides and agrees.
--
-- Reading only the newest *completed* full snapshot makes absence mean deletion. Three parts
-- of that matter:
--   * newest    -- RAW.SNAPSHOT_RUNS.completed_at orders the runs.
--   * completed -- completed_at is set only after COPY INTO reconciled against what staging
--                  wrote (see snowflake_etl/src/load_to_snowflake.py). A half-loaded run
--                  would otherwise read as a mass deletion.
--   * full      -- a windowed run is a delta, not the state of the source; treating one as a
--                  snapshot would delete every payment outside its window.
-- Filtering by dt= instead would not work: same-day reruns share the date partition, so both
-- runs' rows sit inside it.
--
-- QUALIFY still runs, because one snapshot can legitimately contain a payment twice if the
-- extract straddled an update.
--
-- amount was serialized as a JSON *string* to preserve exact money precision; cast straight
-- back to NUMBER(12,2), never through a float.
-- shopper_id is PII: SHA-256 hashed here so the raw customer id never propagates past
-- staging, matching the streaming/DLT bronze masking (sha256 of the id string) -- both
-- pipelines then expose the same tokenized identifier.
-- MIGRATION: this changes shopper_id's type (numeric -> 64-char text). fct_payments_usd is
-- incremental, so deploying this over an existing build needs a one-time
-- `dbt run --full-refresh` -- Snowflake can't alter the populated numeric column in place.
WITH latest_snapshot AS (
    SELECT run_id
    FROM {{ source('raw', 'snapshot_runs') }}
    WHERE dataset = 'payments'
      AND snapshot_type = 'full'
      AND completed_at IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (ORDER BY completed_at DESC) = 1
)
SELECT
    raw:payment_id::INTEGER        AS payment_id,
    raw:merchant_id::INTEGER       AS merchant_id,
    SHA2(raw:shopper_id::INTEGER::STRING, 256) AS shopper_id,
    raw:amount::NUMBER(12, 2)      AS amount,
    raw:currency::STRING           AS currency,
    raw:payment_method::STRING     AS payment_method,
    raw:payment_status::STRING     AS payment_status,
    raw:country_code::STRING       AS country_code,
    raw:created_at::TIMESTAMP_NTZ  AS created_at,
    raw:updated_at::TIMESTAMP_NTZ  AS updated_at
FROM {{ source('raw', 'raw_payments') }}
WHERE REGEXP_SUBSTR(source_file, 'run=([^/]+)', 1, 1, 'e', 1)
      = (SELECT run_id FROM latest_snapshot)
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw:payment_id::INTEGER
    ORDER BY raw:updated_at::TIMESTAMP_NTZ DESC, loaded_at DESC
) = 1
