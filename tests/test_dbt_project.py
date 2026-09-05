"""Static guards for the dbt project (snowflake_etl/dbt).

`dbt parse` in CI proves the project compiles; these tests pin the *invariants* that a future
edit could silently drop -- the forward-fill, the LEFT-join-don't-drop contract, the
validation gates, and the no-secrets-in-profile rule.
"""
from __future__ import annotations

from pathlib import Path

import yaml

DBT_DIR = Path(__file__).resolve().parents[1] / "snowflake_etl" / "dbt"
MODELS = DBT_DIR / "models"


def _read(relpath: str) -> str:
    return (DBT_DIR / relpath).read_text(encoding="utf-8")


def _sql_only(relpath: str) -> str:
    """Read a model with `--` comment lines stripped.

    Needed for assertions of the form "this construct is gone": these models explain in
    their header comments what they replaced, so a naive substring check finds the very
    thing it is asserting the absence of.
    """
    return "\n".join(
        line for line in _read(relpath).splitlines() if not line.lstrip().startswith("--")
    )


def test_expected_models_and_tests_exist() -> None:
    for relpath in (
        "models/staging/stg_payments.sql",
        "models/staging/stg_fx_rates.sql",
        "models/marts/dim_date.sql",
        "models/marts/dim_fx_rates.sql",
        "models/marts/fct_payments_usd.sql",
        "models/marts/agg_payments_by_currency.sql",
        "models/schema.yml",
        "models/sources.yml",
        "tests/fact_reconciles_to_payments.sql",
        "tests/fact_is_not_stale.sql",
        "tests/no_future_timestamps.sql",
        "tests/no_null_or_zero_fx_rate.sql",
        "tests/usd_payments_unchanged.sql",
        "tests/dim_fx_rates_grain_unique.sql",
    ):
        assert (DBT_DIR / relpath).is_file(), relpath


def test_reconciliation_checks_state_not_just_row_count() -> None:
    """COUNT(stg) == COUNT(fact) is blind to a fact row the incremental failed to update.

    Both watermark defects (a late record behind MAX(updated_at), a poisoned future
    watermark) leave the counts equal while the fact holds an older version of the row, so
    the count test alone cannot verify any fix to them.
    """
    counts = _read("tests/fact_reconciles_to_payments.sql")
    assert "COUNT(*)" in counts                                  # the cheap first gate stays

    stale = _read("tests/fact_is_not_stale.sql")
    assert "{{ ref('stg_payments') }}" in stale
    assert "{{ ref('fct_payments_usd') }}" in stale
    # The comparison that makes it a state check rather than a cardinality check.
    assert "f.updated_at < s.updated_at" in stale
    # Failure output has to name the rows, not just signal divergence.
    assert "s.payment_id" in stale


def test_staging_dedups_and_marts_use_refs() -> None:
    stg = _read("models/staging/stg_payments.sql")
    assert "QUALIFY ROW_NUMBER()" in stg                      # snapshot dedup survives
    assert "{{ source('raw', 'raw_payments') }}" in stg       # reads the declared source

    fct = _read("models/marts/fct_payments_usd.sql")
    # LEFT JOIN keeps unmatched payments so the not_null test catches them (vs. silent drop).
    assert "LEFT JOIN {{ ref('dim_fx_rates') }}" in fct
    assert "ROUND(p.amount * d.rate_to_usd, 2) AS usd_amount" in fct


def test_staging_hashes_pii_and_types_fx_as_fixed_scale() -> None:
    # shopper_id is PII: the Snowflake batch path must SHA-256 hash it (matching the
    # streaming/DLT contract) so the raw customer id is never exposed in the fact.
    stg = _read("models/staging/stg_payments.sql")
    assert "SHA2(raw:shopper_id::INTEGER::STRING, 256) AS shopper_id" in stg
    assert "raw:shopper_id::INTEGER        AS shopper_id" not in stg  # no un-hashed passthrough

    # FX rate is money: fixed-scale NUMBER, never FLOAT (float drifts at the cent level once
    # multiplied through amount * rate in the fact).
    fx = _read("models/staging/stg_fx_rates.sql")
    assert "raw:rate_to_usd::NUMBER(18, 8) AS rate_to_usd" in fx
    assert "::FLOAT" not in fx


def test_fct_derives_no_watermark_from_business_data() -> None:
    """The fact rebuilds from the current snapshot instead of chasing MAX(updated_at).

    The incremental watermark this replaces dropped late-arriving rows, never learned about
    deletes, and froze outright if one source row carried a future timestamp. Reintroducing
    any of it would bring all three back, so the absence is worth asserting.
    """
    fct = _sql_only("models/marts/fct_payments_usd.sql")
    assert "materialized='incremental'" not in fct
    assert "is_incremental()" not in fct
    assert "MAX(updated_at)" not in fct
    # Inherits marts' `table` materialization, so a run is a full rebuild from staging.
    assert "{{ ref('stg_payments') }}" in fct


def test_staging_resolves_one_completed_full_snapshot() -> None:
    """Delete propagation depends on all three conditions, not just the newest run."""
    stg = _read("models/staging/stg_payments.sql")
    assert "{{ source('raw', 'snapshot_runs') }}" in stg
    assert "completed_at IS NOT NULL" in stg          # half-loaded run is not the truth
    assert "snapshot_type = 'full'" in stg            # a windowed delta is not a snapshot
    assert "ORDER BY completed_at DESC" in stg        # newest wins
    # dt= alone cannot separate same-day reruns; the run id must come out of the filename.
    assert "REGEXP_SUBSTR(source_file, 'run=([^/]+)'" in stg


def test_snapshot_runs_is_a_declared_source() -> None:
    sources = yaml.safe_load(_read("models/sources.yml"))
    raw = next(s for s in sources["sources"] if s["name"] == "raw")
    assert "snapshot_runs" in {t["name"] for t in raw["tables"]}


def test_dim_fx_rates_forward_fills_gaps() -> None:
    dim = _read("models/marts/dim_fx_rates.sql")
    assert "LAST_VALUE(rate_to_usd) IGNORE NULLS" in dim   # carry last known rate forward
    assert "FIRST_VALUE(rate_to_usd) IGNORE NULLS" in dim  # cover the leading edge
    assert "is_filled" in dim                              # gaps flagged, not hidden
    assert "{{ ref('dim_date') }}" in dim                  # spine comes from the conformed dim


def test_dim_date_owns_the_calendar_spine() -> None:
    dim = _read("models/marts/dim_date.sql")
    assert "GENERATOR" in dim                              # the one spine in the project
    assert "is_weekend" in dim
    # dim_date is the only model that generates a calendar; everyone else refs it.
    for other in ("models/marts/dim_fx_rates.sql", "models/marts/fct_payments_usd.sql"):
        assert "GENERATOR" not in _read(other), other


def test_schema_declares_the_validation_gates() -> None:
    schema = yaml.safe_load(_read("models/schema.yml"))
    tests_by_model = {
        model["name"]: {
            column["name"]: column.get("tests", [])
            for column in model.get("columns", [])
        }
        for model in schema["models"]
    }

    fct = tests_by_model["fct_payments_usd"]
    assert "not_null" in fct["usd_amount"]                   # no unmatched USD amount
    assert {"unique", "not_null"} <= set(fct["payment_id"])  # grain: one row per payment


def test_profiles_use_env_vars_only() -> None:
    profile = _read("profiles.yml")
    for field in ("account", "user", "password"):
        # every credential field is env_var-driven -- no literal secrets in git
        assert f"env_var('SNOWFLAKE_{field.upper()}'" in profile

    parsed = yaml.safe_load(profile)
    output = parsed["payments_fx"]["outputs"]["trial"]
    assert output["password"].startswith("{{ env_var(")

    # The key-pair target authenticates by key path only -- no password field at all.
    keypair = parsed["payments_fx"]["outputs"]["trial_keypair"]
    assert "password" not in keypair
    assert keypair["private_key_path"].startswith("{{ env_var(")


def test_materializations_match_the_old_runner() -> None:
    project = yaml.safe_load(_read("dbt_project.yml"))
    models = project["models"]["payments_fx"]
    # Staging defaults to a view (env_var override lets the scale benchmark flip it to a
    # table); marts are always tables.
    assert models["staging"]["+materialized"] == "{{ env_var('DBT_STAGING_MATERIALIZED', 'view') }}"
    assert models["marts"]["+materialized"] == "table"
