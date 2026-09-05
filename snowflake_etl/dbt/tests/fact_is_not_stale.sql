-- State parity, not row-count parity.
--
-- fact_reconciles_to_payments compares COUNT(stg) to COUNT(fact), which catches a payment
-- the incremental filter never inserted but says nothing about one it failed to UPDATE:
-- both sides still hold the same number of rows while the fact carries an older version.
-- That is exactly the shape of the two watermark defects -- a late-arriving record behind
-- MAX(updated_at), and a poisoned future watermark that freezes every later change -- and
-- it is invisible to every other test in this project. usd_payments_unchanged does not see
-- it either: a stale row is internally consistent, since its old amount still equals its
-- old usd_amount.
--
-- Returns the offending payments (= test failure) rather than just asserting divergence,
-- so a failure names the rows to investigate.
--
-- Standing guard, not an active detector: the fact is now a full rebuild from the current
-- snapshot, so it cannot go stale by construction. This test is what fails if anyone
-- reintroduces an incremental materialization without also solving late arrivals.
SELECT
    s.payment_id,
    s.updated_at AS staged_updated_at,
    f.updated_at AS fact_updated_at
FROM {{ ref('stg_payments') }} s
JOIN {{ ref('fct_payments_usd') }} f
    ON f.payment_id = s.payment_id
WHERE f.updated_at < s.updated_at
