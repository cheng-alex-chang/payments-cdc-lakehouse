"""Guards for the API latency benchmark (scripts/bench_api.py).

The percentile maths is the part worth testing: a benchmark that quietly reports a wrong p99 is
worse than no benchmark, because the number ends up in documentation and on a resume.
"""
from __future__ import annotations

import pytest

from scripts.bench_api import LatencyReport, format_table, percentile, summarize


def test_percentile_of_an_empty_series_is_zero() -> None:
    assert percentile([], 0.5) == 0.0


def test_percentile_of_a_single_sample_is_that_sample() -> None:
    assert percentile([42.0], 0.5) == 42.0
    assert percentile([42.0], 0.99) == 42.0


def test_median_uses_nearest_rank() -> None:
    # Nearest-rank on 1..10 puts p50 at the 5th value. Interpolating would report 5.5, a latency
    # that was never actually measured.
    values = [float(n) for n in range(1, 11)]

    assert percentile(values, 0.50) == 5.0


def test_p99_reports_an_observed_value_not_an_extrapolation() -> None:
    values = [float(n) for n in range(1, 101)]

    assert percentile(values, 0.99) == 99.0
    assert percentile(values, 1.0) == 100.0


def test_percentile_ignores_input_order() -> None:
    assert percentile([9.0, 1.0, 5.0, 3.0, 7.0], 0.5) == 5.0


def test_high_percentile_of_a_short_series_lands_on_the_maximum() -> None:
    # With 3 samples there is no 99th percentile to speak of; reporting the max is the honest
    # answer and must never index past the end of the list.
    assert percentile([1.0, 2.0, 3.0], 0.99) == 3.0


@pytest.mark.parametrize("fraction", [0.0, -0.1, 1.5])
def test_percentile_rejects_a_fraction_outside_the_unit_interval(fraction: float) -> None:
    with pytest.raises(ValueError, match="fraction"):
        percentile([1.0], fraction)


def test_summarize_reports_every_percentile_and_the_error_count() -> None:
    report = summarize("warm", [float(n) for n in range(1, 101)], errors=3)

    assert report.label == "warm"
    assert report.samples == 100
    assert report.errors == 3
    assert report.p50_ms == 50.0
    assert report.p95_ms == 95.0
    assert report.p99_ms == 99.0
    assert report.max_ms == 100.0


def test_summarize_survives_a_scenario_where_every_request_failed() -> None:
    report = summarize("all-errors", [], errors=10)

    assert report.samples == 0
    assert report.errors == 10
    assert report.max_ms == 0.0


def test_format_table_includes_a_row_per_scenario() -> None:
    rendered = format_table(
        [
            LatencyReport("cold cache (first call)", 3, 0, 900.0, 950.0, 950.0, 950.0),
            LatencyReport("hourly (warm cache)", 100, 0, 4.0, 6.0, 9.0, 12.0),
        ]
    )

    assert "cold cache (first call)" in rendered
    assert "hourly (warm cache)" in rendered
    assert "p99 ms" in rendered
