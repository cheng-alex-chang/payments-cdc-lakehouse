"""Guards the CI test split against silent gaps.

.github/workflows/ci.yml builds its job matrix from tests/buckets.yml. Hard-coded file
lists have one dangerous failure mode: add tests/test_new_feature.py, forget to assign
it, and CI keeps passing while never running it. Coverage would dip, but the 80% gate
has enough headroom to absorb a small file, so nothing would fail.

This test closes that hole by asserting the union of every bucket is exactly the fast
suite -- no orphans, no duplicates, no paths pointing at files that no longer exist.

It follows the idiom already used by tests/test_validate_k8s_manifests.py, which asserts
scripts/k8s_verify.sh's hand-written workload lists still match the manifests.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
BUCKETS_FILE = REPO_ROOT / "tests" / "buckets.yml"

# tests/integration/ is the slow tier: it needs real services or cloud credentials and is
# selected by marker, not by these buckets. Everything directly under tests/ is fast.
FAST_SUITE = {f"tests/{path.name}" for path in (REPO_ROOT / "tests").glob("test_*.py")}


@pytest.fixture(scope="module")
def config() -> dict:
    with BUCKETS_FILE.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


@pytest.fixture(scope="module")
def buckets(config) -> dict:
    return config["buckets"]


def assigned(buckets: dict) -> list[str]:
    return [path for bucket in buckets.values() for path in bucket["paths"]]


def test_every_fast_test_is_assigned(buckets):
    """A new test file with no bucket would never run in CI."""
    orphans = FAST_SUITE - set(assigned(buckets))
    assert not orphans, (
        f"Test files in no CI bucket: {sorted(orphans)}. "
        f"Add them to a bucket in {BUCKETS_FILE.relative_to(REPO_ROOT)}."
    )


def test_no_bucket_references_a_missing_file(buckets):
    """A renamed or deleted test would make its bucket's pytest invocation fail."""
    phantoms = set(assigned(buckets)) - FAST_SUITE
    assert not phantoms, f"Buckets reference files that do not exist: {sorted(phantoms)}"


def test_no_file_is_assigned_twice(buckets):
    """Double-assignment would run the tests twice and double-count their coverage."""
    seen: dict[str, str] = {}
    duplicates = []
    for name, bucket in buckets.items():
        for path in bucket["paths"]:
            if path in seen:
                duplicates.append(f"{path} in both '{seen[path]}' and '{name}'")
            seen[path] = name
    assert not duplicates, f"Test files assigned to more than one bucket: {duplicates}"


def test_every_bucket_declares_paths(buckets):
    """The workflow matrix reads this key; a missing one fails at runtime, not here."""
    assert buckets, "buckets.yml declares no buckets"
    for name, bucket in buckets.items():
        assert bucket.get("paths"), f"bucket '{name}' declares no paths"


def test_coverage_sources_exist(config):
    """A stale source makes `--cov=` measure nothing and quietly lowers the total."""
    sources = config["coverage_sources"]
    assert sources, "coverage_sources is empty"
    missing = [s for s in sources if not (REPO_ROOT / s).exists()]
    assert not missing, f"Coverage sources that do not exist: {missing}"


def test_coverage_sources_match_coveragerc(config):
    """.coveragerc and the CI source list must not drift apart again.

    They already did once: .coveragerc listed airflow/dags while the CI --cov flags did
    not, so a local `pytest --cov` and CI measured different code against one 80% gate.
    """
    declared = set(config["coverage_sources"])
    coveragerc = (REPO_ROOT / ".coveragerc").read_text(encoding="utf-8")
    block = coveragerc.split("source =", 1)[1].split("[report]", 1)[0]
    listed = {line.strip() for line in block.splitlines() if line.strip()}
    assert listed <= declared, (
        f".coveragerc measures sources CI does not: {sorted(listed - declared)}"
    )


def test_inventory_test_is_itself_assigned(buckets):
    """This file is part of the fast suite, so it must live in a bucket like any other."""
    assert "tests/test_bucket_inventory.py" in assigned(buckets)
