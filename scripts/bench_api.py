"""Latency benchmark for the gold serving API.

Reports percentiles rather than means. A mean hides the shape of a cache-fronted read path
entirely: a run that is 90% cache hits at 4 ms and 10% cold Trino scans at 900 ms has a
respectable-looking mean and a p99 that tells the truth.

Cold and warm are measured separately because they are different systems. Cold exercises Trino,
Iceberg file pruning, and the HDFS read path; warm exercises the snapshot cache and almost nothing
else. Reporting them together would just blend two distributions.

    python3 scripts/bench_api.py --base-url http://localhost:8000 --requests 100
"""
from __future__ import annotations

import argparse
import json
import statistics
import time
import urllib.error
import urllib.request
from dataclasses import dataclass


@dataclass(frozen=True)
class LatencyReport:
    label: str
    samples: int
    errors: int
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float


def percentile(values: list[float], fraction: float) -> float:
    """Nearest-rank percentile.

    Deliberately not statistics.quantiles: with a handful of samples, interpolating between
    neighbours invents a latency that was never observed. Nearest-rank always reports a real
    measurement, which is what you want when the number you publish has to be defensible.
    """
    if not values:
        return 0.0
    if not 0.0 < fraction <= 1.0:
        raise ValueError(f"fraction must be in (0, 1], got {fraction}")

    ordered = sorted(values)
    rank = max(1, min(len(ordered), -(-len(ordered) * fraction // 1)))
    return ordered[int(rank) - 1]


def summarize(label: str, latencies_ms: list[float], errors: int) -> LatencyReport:
    return LatencyReport(
        label=label,
        samples=len(latencies_ms),
        errors=errors,
        p50_ms=round(percentile(latencies_ms, 0.50), 2),
        p95_ms=round(percentile(latencies_ms, 0.95), 2),
        p99_ms=round(percentile(latencies_ms, 0.99), 2),
        max_ms=round(max(latencies_ms), 2) if latencies_ms else 0.0,
    )


def time_request(url: str, timeout: float = 30.0) -> float | None:
    """Return elapsed milliseconds, or None if the request failed."""
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            response.read()
    except (urllib.error.URLError, TimeoutError, OSError):
        return None
    return (time.perf_counter() - started) * 1000.0


def run_series(url: str, count: int) -> tuple[list[float], int]:
    latencies: list[float] = []
    errors = 0
    for _ in range(count):
        elapsed = time_request(url)
        if elapsed is None:
            errors += 1
        else:
            latencies.append(elapsed)
    return latencies, errors


def format_table(reports: list[LatencyReport]) -> str:
    header = f"{'scenario':<28}{'n':>6}{'err':>6}{'p50 ms':>10}{'p95 ms':>10}{'p99 ms':>10}{'max ms':>10}"
    lines = [header, "-" * len(header)]
    for report in reports:
        lines.append(
            f"{report.label:<28}{report.samples:>6}{report.errors:>6}"
            f"{report.p50_ms:>10}{report.p95_ms:>10}{report.p99_ms:>10}{report.max_ms:>10}"
        )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a table")
    args = parser.parse_args()

    scenarios = [
        ("hourly (warm cache)", f"{args.base_url}/v1/metrics/hourly?limit={args.limit}"),
        ("hourly filtered", f"{args.base_url}/v1/metrics/hourly?limit={args.limit}&country_code=NL"),
        ("summary", f"{args.base_url}/v1/metrics/summary"),
    ]

    reports: list[LatencyReport] = []

    # Cold: the very first call to an untouched cache key pays the full Trino round trip. Measured
    # once per scenario, because the second call is by definition no longer cold.
    cold_latencies: list[float] = []
    cold_errors = 0
    for _, url in scenarios:
        elapsed = time_request(f"{url}&_cold=1" if "?" in url else f"{url}?_cold=1")
        if elapsed is None:
            cold_errors += 1
        else:
            cold_latencies.append(elapsed)
    reports.append(summarize("cold cache (first call)", cold_latencies, cold_errors))

    for label, url in scenarios:
        time_request(url)  # prime the cache so the series measures steady state
        latencies, errors = run_series(url, args.requests)
        reports.append(summarize(label, latencies, errors))

    if args.json:
        print(json.dumps([report.__dict__ for report in reports], indent=2))
    else:
        print(format_table(reports))
        total = sum(report.samples for report in reports)
        if total:
            all_latencies = [report.p50_ms for report in reports]
            print(f"\n{total} timed requests; median of scenario medians "
                  f"{round(statistics.median(all_latencies), 2)} ms")


if __name__ == "__main__":  # pragma: no cover
    main()
