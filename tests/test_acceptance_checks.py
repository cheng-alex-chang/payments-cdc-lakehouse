"""Tests for the deployment data-quality checks.

The SQL runs against a real warehouse, so what is testable here is the logic around it:
that `has_` and `no_` checks are evaluated in opposite directions, that a failing check
actually fails the run, and that the canonical value sets stay in step with the Spark
transform that produces them.
"""
from __future__ import annotations

import pytest

from scripts import acceptance_checks


def test_every_check_is_named_for_its_expectation():
    """The name encodes the direction, so a typo would silently invert a check."""
    for name, _, _ in acceptance_checks.checks():
        assert name.startswith(("has_", "no_")), f"{name} does not declare an expectation"


def test_population_checks_require_rows():
    for name in ("has_bronze_rows", "has_silver_rows", "has_gold_rows"):
        assert acceptance_checks.evaluate(name, 5) is True
        assert acceptance_checks.evaluate(name, 0) is False, f"{name} passed on an empty table"


def test_violation_checks_require_zero():
    assert acceptance_checks.evaluate("no_null_gold_dimensions", 0) is True
    assert acceptance_checks.evaluate("no_null_gold_dimensions", 1) is False


def test_scalar_handles_an_empty_result():
    assert acceptance_checks.scalar([]) == 0
    assert acceptance_checks.scalar([[]]) == 0
    assert acceptance_checks.scalar([[7]]) == 7


def warehouse(counts: dict[str, int] | None = None):
    """A fake Trino keyed on check NAME rather than SQL shape.

    Matching on substrings of the SQL is too fragile: no_duplicate_silver_payment_ids has
    no WHERE clause, so a naive "does it have a WHERE" heuristic silently misclassifies
    it as a population check.
    """
    overrides = counts or {}
    by_sql = {sql: name for name, sql, _ in acceptance_checks.checks()}

    def fake_run(sql, env=None):
        name = by_sql[sql]
        if name in overrides:
            return [[overrides[name]]]
        return [[3]] if name.startswith("has_") else [[0]]

    return fake_run


def test_all_checks_pass_when_the_warehouse_is_healthy(monkeypatch):
    """has_ checks see rows, violation checks see none."""
    monkeypatch.setattr(acceptance_checks.trino_http, "run_statement", warehouse())
    acceptance_checks.main()


def test_a_violation_fails_the_run(monkeypatch):
    monkeypatch.setattr(
        acceptance_checks.trino_http, "run_statement",
        warehouse({"no_null_gold_dimensions": 4}),
    )
    with pytest.raises(SystemExit, match="1 data-quality check"):
        acceptance_checks.main()


def test_unhashed_pii_fails_the_run(monkeypatch):
    """The check that matters most: PII masking regressing must stop the deployment."""
    monkeypatch.setattr(
        acceptance_checks.trino_http, "run_statement",
        warehouse({"no_unhashed_pii_in_bronze": 1}),
    )
    with pytest.raises(SystemExit, match="1 data-quality check"):
        acceptance_checks.main()


def test_an_empty_warehouse_fails_the_run(monkeypatch):
    """The failure mode a smoke check misses: queries execute fine against no data."""
    monkeypatch.setattr(acceptance_checks.trino_http, "run_statement", lambda sql, env=None: [[0]])
    with pytest.raises(SystemExit, match="3 data-quality check"):
        acceptance_checks.main()


def test_canonical_sets_match_the_shared_payment_rules():
    """config/spark/jobs/payment_rules.py is the source of truth for these values.

    The DQ check asserts silver contains nothing outside them, so if the two lists drift
    the check either passes on bad data or fails on good data.
    """
    import payment_rules

    assert tuple(acceptance_checks.ALLOWED_METHODS) == payment_rules.ALLOWED_PAYMENT_METHODS
    assert tuple(acceptance_checks.ALLOWED_STATUSES) == payment_rules.ALLOWED_PAYMENT_STATUSES
