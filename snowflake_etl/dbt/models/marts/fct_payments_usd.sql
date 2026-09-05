-- Grain: one row per payment, normalized to USD.
-- usd_amount = amount * rate_to_usd is THE business deliverable -- it makes the 6 currencies
-- summable. LEFT JOIN (not INNER) on purpose: an unmatched payment must survive with a NULL
-- usd_amount so the not_null test catches it loudly, rather than being silently dropped.
--
-- Materialized as a full rebuild, not incrementally. This used to be
-- `materialized='incremental'` filtered on `updated_at >= (SELECT MAX(updated_at) FROM
-- {{ this }})`, which had three problems, all of which disappear here rather than being
-- patched:
--
--   * Deletes never propagated. An incremental fact only ever learns about rows that appear;
--     a payment deleted at source stopped arriving and stayed in the fact forever.
--   * Late records were dropped. A payment whose updated_at fell below the fact's own high
--     watermark failed the filter and was never folded in -- loud for a new payment_id
--     (counts diverge), silent for an update to an existing one.
--   * One bad timestamp froze everything. A row with updated_at in 2099 set the watermark to
--     2099, after which no normal change could pass the filter.
--
-- The fix is upstream: stg_payments now resolves to the newest completed *full* snapshot, so
-- it is already the current state of the source. Rebuilding from it makes the fact a pure
-- function of that snapshot -- deletes drop out, late rows arrive, and there is no watermark
-- left to poison. Deriving ingestion progress from business data was the underlying mistake.
--
-- Cost: this rescans the snapshot each run instead of only changed rows. At this dataset's
-- size that is the right trade for losing three correctness bugs. If the fact outgrows a
-- full rebuild, the replacement is an incremental MERGE keyed on the snapshot run_id from
-- RAW.SNAPSHOT_RUNS -- pipeline lineage -- never on MAX(updated_at) again.
SELECT
    p.payment_id,
    p.merchant_id,
    p.shopper_id,
    p.currency,
    p.country_code,
    p.payment_method,
    p.payment_status,
    p.amount,
    d.rate_to_usd,
    ROUND(p.amount * d.rate_to_usd, 2) AS usd_amount,
    d.is_filled                        AS fx_rate_filled,
    p.created_at,
    p.created_at::DATE                 AS created_date,
    p.updated_at                       AS updated_at
FROM {{ ref('stg_payments') }} p
LEFT JOIN {{ ref('dim_fx_rates') }} d
    ON d.currency = p.currency
   AND d.rate_date = p.created_at::DATE
