"""Guards for the Spark job definitions shared across runtimes (airflow/dags/spark_jobs.py).

A Spark job now has three descriptions: the Airflow factory here, the Compose submit command in
scripts/run_local_job.py, and the Job templates in k8s/base/spark.yaml. Three descriptions of one
thing is exactly the shape that produced the k8s/base/config/ drift, so these tests assert they
cannot disagree about master, driver memory, packages, or which script runs.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
LAYERS = ("bronze", "silver", "gold")


def _load_spark_jobs():
    """Load airflow/dags/spark_jobs.py by path.

    Importing it as `airflow.dags.spark_jobs` would put a directory named `airflow` on the import
    path and shadow the real Airflow package.
    """
    path = REPO_ROOT / "airflow" / "dags" / "spark_jobs.py"
    spec = importlib.util.spec_from_file_location("repo_spark_jobs", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


spark_jobs = _load_spark_jobs()


def _k8s_job_commands() -> dict[str, list[str]]:
    """The container command from each spark-* Job template, keyed by medallion layer."""
    commands: dict[str, list[str]] = {}
    for doc in yaml.safe_load_all((REPO_ROOT / "k8s" / "base" / "spark.yaml").read_text()):
        if not doc or doc.get("kind") != "Job":
            continue
        name = doc["metadata"]["name"]
        if not name.startswith("spark-"):
            continue
        layer = name.removeprefix("spark-")
        commands[layer] = doc["spec"]["template"]["spec"]["containers"][0]["command"]
    return commands


@pytest.mark.parametrize("layer", LAYERS)
def test_kubernetes_job_template_matches_the_shared_definition(layer: str) -> None:
    # Byte-for-byte: the Job template and the operator factory must submit the same job.
    assert _k8s_job_commands()[layer] == spark_jobs.spark_submit_argv(layer)


@pytest.mark.parametrize("layer", LAYERS)
def test_compose_submit_command_matches_the_shared_definition(layer: str) -> None:
    from scripts import run_local_job

    command = run_local_job.submit_command(layer)

    assert f"--master {spark_jobs.SPARK_MASTER}" in command
    assert f"--driver-memory {spark_jobs.DRIVER_MEMORY}" in command
    assert spark_jobs.JOBS[layer]["packages"] in command
    assert spark_jobs.JOBS[layer]["script"] in command


def test_no_runtime_uses_an_unbounded_driver() -> None:
    """local[*] with no --driver-memory is what OOM-killed every spark-gold attempt.

    The driver JVM sizes its heap off the host rather than its container, and local[*] multiplies
    that by one task thread per core.
    """
    from scripts import run_local_job

    assert spark_jobs.SPARK_MASTER != "local[*]"
    for layer in LAYERS:
        argv = spark_jobs.spark_submit_argv(layer)
        assert "local[*]" not in argv
        assert "--driver-memory" in argv
        assert "local[*]" not in run_local_job.submit_command(layer)


def test_runtime_defaults_to_compose(monkeypatch: pytest.MonkeyPatch) -> None:
    # Kubernetes must opt in. Defaulting the other way would make a plain `docker compose up`
    # try to create pods against a cluster that is not there.
    monkeypatch.delenv("PIPELINE_RUNTIME", raising=False)

    assert spark_jobs.runtime() == "compose"


@pytest.mark.parametrize("value", ["kubernetes", "Kubernetes", "  KUBERNETES  "])
def test_runtime_recognises_kubernetes_regardless_of_casing(
    monkeypatch: pytest.MonkeyPatch, value: str
) -> None:
    monkeypatch.setenv("PIPELINE_RUNTIME", value)

    assert spark_jobs.runtime() == "kubernetes"


def test_unknown_layer_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown Spark layer"):
        spark_jobs.spark_task("bad", "platinum")


def test_compose_runtime_builds_a_bash_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPELINE_RUNTIME", raising=False)

    class FakeBashOperator:
        def __init__(self, task_id: str, bash_command: str) -> None:
            self.task_id = task_id
            self.bash_command = bash_command

    bash_module = types.ModuleType("airflow.operators.bash")
    bash_module.BashOperator = FakeBashOperator
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))
    monkeypatch.setitem(sys.modules, "airflow.operators", types.ModuleType("airflow.operators"))
    monkeypatch.setitem(sys.modules, "airflow.operators.bash", bash_module)

    task = spark_jobs.spark_task("bronze_load", "bronze")

    assert task.task_id == "bronze_load"
    assert task.bash_command == "python /opt/airflow/scripts/run_local_job.py bronze"


def test_kubernetes_operator_is_imported_lazily() -> None:
    """A Compose deployment without the cncf-kubernetes provider must still parse the DAG.

    Asserted on the source rather than by import, because the provider happens to be absent from
    this test environment either way and that would make the check pass vacuously.
    """
    source = (REPO_ROOT / "airflow" / "dags" / "spark_jobs.py").read_text(encoding="utf-8")
    top_level = [line for line in source.splitlines() if line.startswith(("import ", "from "))]

    assert not any("cncf" in line or "kubernetes" in line for line in top_level)
    assert "from airflow.providers.cncf.kubernetes.operators.pod import" in source


def test_kubernetes_pods_run_as_the_spark_service_account() -> None:
    # The airflow ServiceAccount may create pods; the pods themselves run as spark, matching the
    # Job templates. k8s/base/spark.yaml binds both to the spark-job-runner Role.
    source = (REPO_ROOT / "airflow" / "dags" / "spark_jobs.py").read_text(encoding="utf-8")

    assert 'service_account_name="spark"' in source
    assert "in_cluster=True" in source
    assert "get_logs=True" in source


def test_rbac_grants_pod_log_access_for_streaming_task_logs() -> None:
    # Without pods/log the operator runs blind: a Spark failure surfaces only as a non-zero exit
    # with no driver output in the Airflow task log.
    rules: list[dict] = []
    for doc in yaml.safe_load_all((REPO_ROOT / "k8s" / "base" / "spark.yaml").read_text()):
        if doc and doc.get("kind") == "Role" and doc["metadata"]["name"] == "spark-job-runner":
            rules = doc["rules"]

    resources = {resource for rule in rules for resource in rule["resources"]}
    assert "pods" in resources
    assert "pods/log" in resources
