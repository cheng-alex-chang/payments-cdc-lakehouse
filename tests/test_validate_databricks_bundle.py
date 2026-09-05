"""Tests for the credential-free Databricks bundle structural validator.

Each negative builds a synthetic bundle under tmp_path, so the tests prove the validator
actually rejects broken input rather than only accepting the repo's current one.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from scripts import validate_databricks_bundle as validator

REPO_BUNDLE = Path(__file__).resolve().parents[1] / "databricks"


def write_bundle(root, bundle: dict, resources: dict[str, dict] | None = None) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "databricks.yml").write_text(yaml.safe_dump(bundle), encoding="utf-8")
    resource_dir = root / "resources"
    resource_dir.mkdir(exist_ok=True)
    for name, document in (resources or {}).items():
        (resource_dir / name).write_text(yaml.safe_dump(document), encoding="utf-8")


MINIMAL = {
    "bundle": {"name": "test-bundle"},
    "include": ["resources/*.yml"],
    "targets": {"dev": {"mode": "development", "default": True}},
}


def job_resource(task: dict) -> dict:
    """One job with a single task -- keeps the nesting out of each test."""
    return {"resources": {"jobs": {"j": {"tasks": [task]}}}}


def test_repo_bundle_is_valid():
    """The real bundle in this repo passes -- the check CI actually runs on every PR."""
    validator.main()


def test_minimal_bundle_passes(tmp_path):
    write_bundle(tmp_path, MINIMAL, {"a.yml": {"resources": {"jobs": {"j": {"name": "j"}}}}})
    validator.main(tmp_path)


def test_missing_bundle_file(tmp_path):
    with pytest.raises(SystemExit, match="bundle file not found"):
        validator.main(tmp_path)


def test_missing_bundle_name(tmp_path):
    write_bundle(tmp_path, {**MINIMAL, "bundle": {}}, {"a.yml": {"resources": {}}})
    with pytest.raises(SystemExit, match="bundle.name is missing"):
        validator.main(tmp_path)


def test_no_targets(tmp_path):
    write_bundle(tmp_path, {**MINIMAL, "targets": {}}, {"a.yml": {"resources": {}}})
    with pytest.raises(SystemExit, match="no targets declared"):
        validator.main(tmp_path)


def test_multiple_default_targets(tmp_path):
    bundle = {**MINIMAL, "targets": {"dev": {"default": True}, "prod": {"default": True}}}
    write_bundle(tmp_path, bundle, {"a.yml": {"resources": {}}})
    with pytest.raises(SystemExit, match="more than one default target"):
        validator.main(tmp_path)


def test_include_matching_nothing(tmp_path):
    """A rename that orphans an include silently stops deploying that resource."""
    write_bundle(tmp_path, {**MINIMAL, "include": ["resources/*.yaml"]}, {"a.yml": {"resources": {}}})
    with pytest.raises(SystemExit, match="matched no files"):
        validator.main(tmp_path)


def test_non_mapping_yaml(tmp_path):
    tmp_path.mkdir(parents=True, exist_ok=True)
    (tmp_path / "databricks.yml").write_text("- not\n- a mapping\n", encoding="utf-8")
    with pytest.raises(SystemExit, match="expected a YAML mapping"):
        validator.main(tmp_path)


def test_resource_file_without_resources_key(tmp_path):
    write_bundle(tmp_path, MINIMAL, {"a.yml": {"something_else": {}}})
    with pytest.raises(SystemExit, match="1 error"):
        validator.main(tmp_path)


def test_missing_notebook_path(tmp_path):
    """A renamed notebook the job still points at."""
    task = {"notebook_task": {"notebook_path": "../src/gone.py"}}
    write_bundle(tmp_path, MINIMAL, {"a.yml": job_resource(task)})
    with pytest.raises(SystemExit, match="1 error"):
        validator.main(tmp_path)


def test_existing_notebook_path_passes(tmp_path):
    task = {"notebook_task": {"notebook_path": "../src/here.py"}}
    write_bundle(tmp_path, MINIMAL, {"a.yml": job_resource(task)})
    src = tmp_path / "src"
    src.mkdir()
    (src / "here.py").write_text("# notebook\n", encoding="utf-8")
    validator.main(tmp_path)


def test_undeclared_resource_reference(tmp_path, caplog):
    """The summary is on SystemExit; the specific offender is logged."""
    task = {"pipeline_task": {"pipeline_id": "${resources.pipelines.ghost.id}"}}
    write_bundle(tmp_path, MINIMAL, {"a.yml": job_resource(task)})
    with caplog.at_level("ERROR"), pytest.raises(SystemExit, match="1 error"):
        validator.main(tmp_path)
    assert "undeclared resource pipelines.ghost" in caplog.text


def test_declared_resource_reference_passes(tmp_path):
    task = {"pipeline_task": {"pipeline_id": "${resources.pipelines.p.id}"}}
    document = job_resource(task)
    document["resources"]["pipelines"] = {"p": {"name": "p"}}
    write_bundle(tmp_path, MINIMAL, {"a.yml": document})
    validator.main(tmp_path)


def test_templated_path_is_not_checked_on_disk(tmp_path):
    """A ${var}-templated path resolves at deploy time, not statically."""
    task = {"notebook_task": {"notebook_path": "${workspace.root}/x.py"}}
    write_bundle(tmp_path, MINIMAL, {"a.yml": job_resource(task)})
    validator.main(tmp_path)


def test_dev_and_prod_publish_to_different_schemas():
    """Free Edition has one workspace, so the schema is what separates the two targets.

    Both targets used to publish to workspace.analytics, which meant a prod deploy
    overwrote the tables dev was using -- the reason the cloud release was pinned to
    manual. The workspace path was never the collision: mode: production already roots
    itself at /Shared/.bundle/<name>/<target>, distinct from dev's per-user path.
    """
    bundle = yaml.safe_load((REPO_BUNDLE / "databricks.yml").read_text(encoding="utf-8"))
    targets = bundle["targets"]

    schemas = {name: t["variables"]["target_schema"] for name, t in targets.items()}
    assert len(set(schemas.values())) == len(schemas), (
        f"targets must not share a schema in a single-workspace tier: {schemas}"
    )

    pipeline = yaml.safe_load(
        (REPO_BUNDLE / "resources" / "payments.pipeline.yml").read_text(encoding="utf-8")
    )["resources"]["pipelines"]["payments_dlt"]
    # Hardcoding the schema again would silently reunite the targets.
    assert pipeline["schema"] == "${var.target_schema}"
    assert "target_schema" in bundle["variables"]


def test_production_targets_pin_their_root_path():
    """`mode: production` refuses to infer a root_path -- the CLI errors without one.

    The bundle shipped without it and `bundle validate -t prod` had been failing since the
    target was written. Nothing caught it: the repo's own validator does not model CLI
    semantics, and the workflow that runs the real `bundle validate` is manual-only and had
    never been dispatched. This is the cheap static half of that check.
    """
    bundle = yaml.safe_load((REPO_BUNDLE / "databricks.yml").read_text(encoding="utf-8"))

    for name, target in bundle["targets"].items():
        if target.get("mode") != "production":
            continue
        root_path = target.get("workspace", {}).get("root_path")
        assert root_path, (
            f"target {name!r} uses mode: production but sets no workspace.root_path; "
            "`databricks bundle validate` fails on this."
        )
        # Keeping the target in the path is what stops prod landing on dev's deployment
        # in a single-workspace tier.
        assert "${bundle.target}" in root_path
