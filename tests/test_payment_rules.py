"""The shared payment rules, and the two guards that keep their copies honest.

config/spark/jobs/payment_rules.py is the canonical definition. Two things could still
silently diverge from it, and each gets a test here:

* The Kubernetes pods only receive the files listed in k8s/base/kustomization.yaml's
  spark-jobs ConfigMap. A module added to the directory but not to that list imports fine
  locally and under Compose (which bind-mounts the repo) and fails with ImportError only
  inside the cluster.
* databricks/src/dlt_pipeline.py inlines its own copy on purpose -- it is loaded from a
  Volume where sibling imports are unreliable on Free Edition. That copy cannot be
  replaced with an import, so instead it is held to behavioural equivalence.
"""
from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import pytest
import yaml

import payment_rules

REPO_ROOT = Path(__file__).resolve().parents[1]
JOBS_DIR = REPO_ROOT / "config" / "spark" / "jobs"
DLT_FILE = REPO_ROOT / "databricks" / "src" / "dlt_pipeline.py"
KUSTOMIZATION = REPO_ROOT / "k8s" / "base" / "kustomization.yaml"

# Envelopes chosen to cover every branch of the masking logic, so behavioural equivalence
# between the two implementations means something.
VECTORS = [
    None,
    "not json at all",
    "",
    json.dumps({"after": {"shopper_id": 424242, "amount": "10.00"}}),
    json.dumps({"before": {"shopper_id": 7}, "after": {"shopper_id": 8}}),
    json.dumps({"after": {"shopper_id": None, "amount": "1.00"}}),
    json.dumps({"after": {"amount": "1.00"}}),
    json.dumps({"before": None, "after": {"shopper_id": "0"}}),
    json.dumps({"op": "d", "before": {"shopper_id": 99}, "after": None}),
]


# --------------------------------------------------------------------------------------
# the rules themselves
# --------------------------------------------------------------------------------------

def test_shopper_id_is_hashed_not_dropped():
    """Bronze keeps the field so downstream joins still work; only the value changes."""
    out = json.loads(payment_rules.mask_pii_fields(json.dumps({"after": {"shopper_id": 42}})))
    hashed = out["after"]["shopper_id"]
    assert hashed == hashlib.sha256(b"42").hexdigest()
    assert len(hashed) == 64


def test_both_envelope_sections_are_masked():
    """A Debezium update carries the old row in `before` -- masking only `after` leaks it."""
    payload = json.dumps({"before": {"shopper_id": 1}, "after": {"shopper_id": 2}})
    out = json.loads(payment_rules.mask_pii_fields(payload))
    assert out["before"]["shopper_id"] == hashlib.sha256(b"1").hexdigest()
    assert out["after"]["shopper_id"] == hashlib.sha256(b"2").hexdigest()


def test_non_pii_fields_are_untouched():
    payload = json.dumps({"after": {"shopper_id": 1, "amount": "9.99", "currency": "EUR"}})
    out = json.loads(payment_rules.mask_pii_fields(payload))
    assert out["after"]["amount"] == "9.99"
    assert out["after"]["currency"] == "EUR"


@pytest.mark.parametrize("value", [None, "not json", "", "{unclosed"])
def test_unparseable_input_passes_through(value):
    """A malformed envelope belongs in the DLQ, not raised mid-micro-batch."""
    assert payment_rules.mask_pii_fields(value) == value


def test_a_null_pii_value_is_left_alone():
    payload = json.dumps({"after": {"shopper_id": None}})
    assert json.loads(payment_rules.mask_pii_fields(payload))["after"]["shopper_id"] is None


def test_allowed_sets_are_sorted_and_unique():
    """Sorted so a diff adding a value is one line, and duplicates cannot hide."""
    for values in (payment_rules.ALLOWED_PAYMENT_METHODS, payment_rules.ALLOWED_PAYMENT_STATUSES):
        assert list(values) == sorted(values)
        assert len(set(values)) == len(values)


# --------------------------------------------------------------------------------------
# guard 1: every job module actually reaches the pods
# --------------------------------------------------------------------------------------

def test_configmap_ships_every_spark_job_module():
    """A module missing from the ConfigMap fails only inside the cluster, at import time."""
    with KUSTOMIZATION.open(encoding="utf-8") as handle:
        kustomization = yaml.safe_load(handle)

    generator = next(
        g for g in kustomization["configMapGenerator"] if g["name"] == "spark-jobs"
    )
    shipped = {entry.split("=", 1)[0] for entry in generator["files"]}
    on_disk = {p.name for p in JOBS_DIR.glob("*.py") if p.name != "__init__.py"}

    assert shipped == on_disk, (
        f"spark-jobs ConfigMap and {JOBS_DIR.relative_to(REPO_ROOT)} disagree. "
        f"Missing from the ConfigMap: {sorted(on_disk - shipped)}. "
        f"Listed but absent from disk: {sorted(shipped - on_disk)}."
    )


# --------------------------------------------------------------------------------------
# guard 2: the Databricks copy cannot drift
# --------------------------------------------------------------------------------------

def dlt_namespace() -> dict:
    """Execute only the rule definitions from the DLT notebook.

    The module cannot be imported: it does `import dlt` and builds a pipeline at module
    scope. So the AST is parsed and only the constant assignments and the masking function
    are compiled -- they depend on nothing but hashlib and json.
    """
    tree = ast.parse(DLT_FILE.read_text(encoding="utf-8"))
    wanted = {"PII_FIELDS", "ALLOWED_PAYMENT_METHODS", "ALLOWED_PAYMENT_STATUSES"}

    nodes: list[ast.stmt] = []
    for node in tree.body:
        if isinstance(node, ast.Assign):
            names = {t.id for t in node.targets if isinstance(t, ast.Name)}
            if names & wanted:
                nodes.append(node)
        elif isinstance(node, ast.FunctionDef) and node.name == "mask_pii_fields":
            nodes.append(node)

    namespace: dict = {"hashlib": hashlib, "json": json}
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(DLT_FILE), "exec"), namespace)
    return namespace


def test_dlt_still_declares_the_rules():
    """If the notebook is restructured, the rest of this guard would silently pass."""
    ns = dlt_namespace()
    for name in ("PII_FIELDS", "ALLOWED_PAYMENT_METHODS", "ALLOWED_PAYMENT_STATUSES"):
        assert name in ns, f"{name} not found in {DLT_FILE.name}"
    assert callable(ns.get("mask_pii_fields"))


def test_dlt_constants_match_the_canonical_rules():
    ns = dlt_namespace()
    assert set(ns["PII_FIELDS"]) == set(payment_rules.PII_FIELDS)
    assert tuple(ns["ALLOWED_PAYMENT_METHODS"]) == payment_rules.ALLOWED_PAYMENT_METHODS
    assert tuple(ns["ALLOWED_PAYMENT_STATUSES"]) == payment_rules.ALLOWED_PAYMENT_STATUSES


@pytest.mark.parametrize("value", VECTORS)
def test_dlt_masking_behaves_identically(value):
    """Behavioural equivalence, not text comparison.

    The two implementations are allowed to look different -- the notebook copy has no type
    hints -- but they must produce the same output for the same envelope.
    """
    assert dlt_namespace()["mask_pii_fields"](value) == payment_rules.mask_pii_fields(value)
