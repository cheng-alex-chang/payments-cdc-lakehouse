"""Structural guards for docker-compose.yml.

Every other surface in this repo had something asserting it stayed true. Compose did not, and it
is the runtime nobody re-ran during the storage migration -- so a `depends_on` pointing at a
service deleted in Phase 4 survived all the way to the end of Phase 5, in a state where
`docker compose config` exited 1 and `docker compose up` could start nothing at all.

These parse the file rather than shelling out to `docker compose`, so they run in CI with no
daemon, no images, and no .env -- the same constraint every other test here works under.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
VERIFY_SCRIPT = REPO_ROOT / "scripts" / "k8s_verify.sh"

# Compose services with no Kubernetes workload of the same name, each for a structural reason
# rather than an oversight. Anything else appearing here means the two runtimes have diverged.
COMPOSE_ONLY_SERVICES = {
    # Kubernetes runs the Lakekeeper migration as an init container inside the iceberg-rest pod.
    # Compose has no init containers, so it becomes a one-shot service instead.
    "iceberg-rest-migrate",
    # The Kubernetes equivalent is a Job, not a workload the verify script waits on.
    "airflow-init",
    # On Kubernetes the DAG creates a pod per Spark job (KubernetesPodOperator). Compose has no
    # cluster to schedule against, so it keeps one idle container to exec into.
    "spark",
}


@pytest.fixture(scope="module")
def compose() -> dict:
    return yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))


@pytest.fixture(scope="module")
def services(compose: dict) -> dict:
    return compose["services"]


def test_every_depends_on_names_a_defined_service(services: dict) -> None:
    """The Phase 4 bug, exactly.

    Deleting the `hive-metastore` service left `trino` depending on it. Compose refuses to load a
    project whose `depends_on` names an undefined service, so this was not a degraded start -- it
    was no start at all, for every service in the file, on the runtime the README offers as the
    two-minute quickstart.
    """
    defined = set(services)
    dangling: list[str] = []

    for name, service in services.items():
        requires = service.get("depends_on") or {}
        # Both spellings are legal: a list of names, or a mapping of name -> condition.
        targets = requires if isinstance(requires, list) else list(requires)
        dangling += [f"{name} -> {t}" for t in targets if t not in defined]

    assert not dangling, f"depends_on references undefined services: {dangling}"


def test_every_declared_volume_is_mounted(compose: dict, services: dict) -> None:
    """A named volume nothing mounts is a claim that survives its own reason.

    `spark_checkpoints` outlived the design that needed it: once checkpoints moved to
    `s3a://checkpoints/`, the volume stayed declared and mounted while never being written again.
    On Kubernetes the same leftover was a 2 Gi PersistentVolumeClaim.
    """
    declared = set(compose.get("volumes") or {})

    mounted = set()
    for service in services.values():
        for mount in service.get("volumes") or []:
            # Named volumes are "name:/path"; bind mounts start with . or / and are not volumes.
            source = mount.split(":")[0] if isinstance(mount, str) else mount.get("source", "")
            if source in declared:
                mounted.add(source)

    assert declared == mounted, f"declared but never mounted: {sorted(declared - mounted)}"


def test_every_service_pins_an_image_or_builds_one(services: dict) -> None:
    """No floating tags. A service that pins nothing is a service that changes under you."""
    unpinned = [
        name
        for name, service in services.items()
        if "build" not in service
        and (":" not in service.get("image", "") or service.get("image", "").endswith(":latest"))
    ]

    assert not unpinned, f"services without a pinned image: {unpinned}"


def _kubernetes_workloads() -> set[str]:
    """Workload names from scripts/k8s_verify.sh, which is itself guarded against the manifests.

    Reading them here rather than restating them keeps this a derived comparison: there is no
    third copy of the service list to drift.
    """
    text = VERIFY_SCRIPT.read_text(encoding="utf-8")
    names: set[str] = set()
    for array in ("STATEFULSETS", "DEPLOYMENTS"):
        match = re.search(rf"^{array}=\(([^)]*)\)", text, re.MULTILINE)
        assert match, f"{array} not found in {VERIFY_SCRIPT.name}"
        names |= set(match.group(1).split())
    return names


def test_compose_ships_every_kubernetes_workload(services: dict) -> None:
    """The two runtimes must offer the same services under the same names.

    Compose had no `iceberg-rest` at all after the migration, while the README claimed a task
    could address `iceberg-rest:8181` in either runtime. Nothing checked, so nothing complained.
    """
    missing = _kubernetes_workloads() - set(services)

    assert not missing, f"Kubernetes workloads with no Compose service: {sorted(missing)}"


def test_compose_only_services_are_documented(services: dict) -> None:
    """The other direction, so a Compose-only service cannot quietly appear."""
    extra = set(services) - _kubernetes_workloads() - COMPOSE_ONLY_SERVICES

    assert not extra, f"Compose services with no Kubernetes counterpart or reason: {sorted(extra)}"


def test_services_needing_storage_credentials_have_them_in_both_runtimes() -> None:
    """Credentials added on one runtime and forgotten on the other.

    Trino reads S3 credentials from `AWS_*` in its environment -- `iceberg.properties` carries the
    endpoint and region but deliberately no secrets. The Kubernetes Deployment supplied them and
    the Compose service did not, so every Trino query against a table failed with
    "Error processing metadata for table analytics.payments_bronze" while Spark wrote the same
    tables happily.

    The expectation is derived from the manifests rather than listed here: whichever workloads
    Kubernetes gives `AWS_ACCESS_KEY_ID`, the matching Compose services must have it too. Adding it
    to a new workload on one side alone fails this test.
    """
    needs: set[str] = set()
    for manifest in (REPO_ROOT / "k8s" / "base").glob("*.yaml"):
        for doc in yaml.safe_load_all(manifest.read_text(encoding="utf-8")):
            if not isinstance(doc, dict) or doc.get("kind") not in {"Deployment", "StatefulSet"}:
                continue
            spec = doc["spec"]["template"]["spec"]
            for container in spec.get("containers", []):
                names = {env["name"] for env in container.get("env") or []}
                if "AWS_ACCESS_KEY_ID" in names:
                    needs.add(doc["metadata"]["name"])

    assert needs, "expected at least one Kubernetes workload to carry AWS credentials"

    services = yaml.safe_load(COMPOSE_PATH.read_text(encoding="utf-8"))["services"]
    missing = [
        name
        for name in sorted(needs)
        if name in services
        and "AWS_ACCESS_KEY_ID" not in (services[name].get("environment") or {})
    ]

    assert not missing, f"Kubernetes gives these AWS credentials, Compose does not: {missing}"


def test_env_example_defines_every_interpolated_variable() -> None:
    """`${VAR}` in the file is a promise .env.example has to keep.

    `CATALOG_ENCRYPTION_KEY` reached docker-compose.yml through the catalog service without ever
    reaching .env.example, which turns a first-run `docker compose up` into an empty-string
    password and an error from Lakekeeper rather than from Compose.
    """
    text = COMPOSE_PATH.read_text(encoding="utf-8")
    referenced = set(re.findall(r"\$\{([A-Z0-9_]+)[:}-]", text))

    example = (REPO_ROOT / ".env.example").read_text(encoding="utf-8")
    defined = {
        line.split("=", 1)[0].strip()
        for line in example.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }

    assert not referenced - defined, f"used in Compose, absent from .env.example: {sorted(referenced - defined)}"


def test_iceberg_vectorization_is_not_set_per_runtime() -> None:
    """One value for every runtime, or the acceptance suite stops testing production.

    Iceberg's vectorized Parquet reader aborts the JVM on arm64 when a scan feeds a shuffle,
    so config/spark/jobs/common.py disables it by default. The override exists to re-enable
    it once an image ships a fixed Arrow/JVM combination -- but set in Compose or in the
    Kubernetes manifests alone, it silently splits the read path between what CI exercises
    and what runs in the cluster, and this crash is native: it takes the driver down rather
    than failing a task.

    So the value may be changed, and must be changed everywhere at once. Setting it in one
    runtime and not the other is what this forbids.
    """
    sources = [COMPOSE_PATH, *(REPO_ROOT / "k8s").rglob("*.yaml"), *(REPO_ROOT / "k8s").rglob("*.yml")]
    setters = [
        path.relative_to(REPO_ROOT)
        for path in sources
        if path.is_file() and "ICEBERG_VECTORIZATION" in path.read_text(encoding="utf-8")
    ]

    assert not setters, (
        "ICEBERG_VECTORIZATION is set in "
        f"{[str(p) for p in setters]} but not in every runtime. Either drop it and let "
        "config/spark/jobs/common.py hold the single default, or set the same value in all "
        "of them -- a per-runtime split means CI no longer exercises the production read path."
    )
