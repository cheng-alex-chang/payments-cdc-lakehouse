-- Impossible timestamps, caught at the door.
--
-- A payment dated in the future used to be structurally dangerous: fct_payments_usd derived
-- its incremental watermark from MAX(updated_at), so one row at 2099-01-01 froze the model
-- and no later change could pass the filter. The fact is now a full rebuild from the current
-- snapshot, so a bad timestamp can no longer stop the pipeline -- but it still corrupts
-- created_date, the monthly grain in agg_payments_by_currency, and any window a consumer
-- builds on it.
--
-- One day of slack absorbs clock skew between Postgres and the warehouse without letting a
-- genuinely wrong year through.
SELECT payment_id, created_at, updated_at
FROM {{ ref('stg_payments') }}
WHERE updated_at > DATEADD('day', 1, CURRENT_TIMESTAMP())
   OR created_at > DATEADD('day', 1, CURRENT_TIMESTAMP())
