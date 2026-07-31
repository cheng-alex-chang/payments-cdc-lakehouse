from __future__ import annotations

import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


DEFAULT_OVERLAY = Path("k8s/overlays/local")


@dataclass(frozen=True)
class K8sObject:
    kind: str
    name: str
    namespace: str | None
    raw: dict[str, Any]


REQUIRED_OBJECTS = {
    ("Namespace", "data-pipeline"),
    ("StatefulSet", "postgres"),
    ("StatefulSet", "catalog-db"),
    ("StatefulSet", "kafka"),
    ("Deployment", "trino"),
    ("Deployment", "minio"),
    ("Deployment", "iceberg-rest"),
    ("Deployment", "kafka-connect"),
    ("Deployment", "airflow-webserver"),
    ("Deployment", "airflow-scheduler"),
    ("Deployment", "api"),
    ("Deployment", "prometheus"),
    ("Deployment", "grafana"),
    ("Job", "register-postgres-cdc"),
    ("Job", "spark-bronze"),
    ("Job", "spark-silver"),
    ("Job", "spark-gold"),
}


def render_kustomize(overlay: Path = DEFAULT_OVERLAY) -> str:
    # The base kustomization generates ConfigMaps from the repo's real config files rather than
    # from a copy, so its file paths reach outside the kustomization root. Kustomize blocks that
    # by default; LoadRestrictionsNone permits it. scripts/k8s_up.sh renders the same way.
    result = subprocess.run(
        ["kubectl", "kustomize", "--load-restrictor=LoadRestrictionsNone", str(overlay)],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def parse_objects(manifest_text: str) -> list[K8sObject]:
    objects: list[K8sObject] = []
    for document in yaml.safe_load_all(manifest_text):
        if not document:
            continue
        metadata = document.get("metadata", {})
        objects.append(
            K8sObject(
                kind=document["kind"],
                name=metadata["name"],
                namespace=metadata.get("namespace"),
                raw=document,
            )
        )
    return objects


def find_missing_required(objects: list[K8sObject]) -> list[tuple[str, str]]:
    present = {(obj.kind, obj.name) for obj in objects}
    return sorted(REQUIRED_OBJECTS - present)


def find_unsuspended_template_jobs(objects: list[K8sObject]) -> list[str]:
    template_jobs = {
        "register-postgres-cdc",
        "seed-demo-data",
        "spark-bronze",
        "spark-silver",
        "spark-gold",
    }
    return sorted(
        obj.name
        for obj in objects
        if obj.kind == "Job"
        and obj.name in template_jobs
        and obj.raw.get("spec", {}).get("suspend") is not True
    )


def find_namespaced_workload_gaps(objects: list[K8sObject]) -> list[str]:
    workload_kinds = {"Deployment", "StatefulSet", "Job"}
    return sorted(
        f"{obj.kind}/{obj.name}"
        for obj in objects
        if obj.kind in workload_kinds and obj.namespace != "data-pipeline"
    )


def _container_env_names(container: dict[str, Any]) -> set[str]:
    return {env["name"] for env in container.get("env", []) if "name" in env}


def _container_command_text(container: dict[str, Any]) -> str:
    command = container.get("command", [])
    if isinstance(command, list):
        return "\n".join(str(part) for part in command)
    return str(command)


def find_airflow_migration_wait_gaps(objects: list[K8sObject]) -> list[str]:
    gaps: list[str] = []
    for obj in objects:
        if obj.kind != "Deployment" or obj.name not in {"airflow-webserver", "airflow-scheduler"}:
            continue
        init_names = {
            container.get("name")
            for container in obj.raw.get("spec", {}).get("template", {}).get("spec", {}).get("initContainers", [])
        }
        if "wait-for-airflow-migrations" not in init_names:
            gaps.append(f"Deployment/{obj.name}")
    return sorted(gaps)


def find_airflow_dag_directory_mounts(objects: list[K8sObject]) -> list[str]:
    # Airflow's DAG walker follows the ConfigMap atomic-update `..data` symlink and crashes
    # the DagFileProcessor with a recursive-loop error, so the dags ConfigMap must be mounted
    # file-by-file via subPath, not as a directory at /opt/airflow/dags.
    airflow_workloads = {"airflow-init", "airflow-webserver", "airflow-scheduler"}
    gaps: list[str] = []
    for obj in objects:
        if obj.name not in airflow_workloads:
            continue
        pod_spec = obj.raw.get("spec", {}).get("template", {}).get("spec", {})
        for container in pod_spec.get("containers", []):
            for mount in container.get("volumeMounts", []):
                if mount.get("name") == "airflow-dags" and "subPath" not in mount:
                    gaps.append(f"{obj.kind}/{obj.name}/{container.get('name')}")
    return sorted(gaps)


def find_dead_service_precondition_envs(objects: list[K8sObject]) -> list[str]:
    gaps: list[str] = []
    for obj in objects:
        pod_spec = obj.raw.get("spec", {}).get("template", {}).get("spec", {})
        for container in pod_spec.get("containers", []):
            if "SERVICE_PRECONDITION" in _container_env_names(container):
                gaps.append(f"{obj.kind}/{obj.name}/{container.get('name')}")
    return sorted(gaps)


def find_trino_memory_config_gaps(objects: list[K8sObject]) -> list[str]:
    for obj in objects:
        if obj.kind == "ConfigMap" and obj.name == "trino-etc":
            config = obj.raw.get("data", {}).get("config.properties", "")
            # query.max-total-memory-per-node is defunct in Trino 477+; do not require it.
            required = {
                "query.max-memory",
                "query.max-memory-per-node",
                "memory.heap-headroom-per-node",
            }
            missing = sorted(setting for setting in required if setting not in config)
            return [f"ConfigMap/trino-etc:{setting}" for setting in missing]
    return ["ConfigMap/trino-etc"]


def find_connector_retry_gaps(objects: list[K8sObject]) -> list[str]:
    for obj in objects:
        if obj.kind == "Job" and obj.name == "register-postgres-cdc":
            containers = obj.raw.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
            command_text = "\n".join(_container_command_text(container) for container in containers)
            if "except urllib.error.HTTPError as create_exc" not in command_text or "create_exc.code == 409" not in command_text:
                return ["Job/register-postgres-cdc"]
            return []
    return ["Job/register-postgres-cdc"]


def validate(objects: list[K8sObject]) -> list[str]:
    errors: list[str] = []
    missing = find_missing_required(objects)
    if missing:
        errors.append(f"Missing required objects: {missing}")

    unsuspended = find_unsuspended_template_jobs(objects)
    if unsuspended:
        errors.append(f"Template jobs must be suspended by default: {unsuspended}")

    namespace_gaps = find_namespaced_workload_gaps(objects)
    if namespace_gaps:
        errors.append(f"Workloads missing data-pipeline namespace: {namespace_gaps}")

    airflow_gaps = find_airflow_migration_wait_gaps(objects)
    if airflow_gaps:
        errors.append(f"Airflow workloads missing migration wait init container: {airflow_gaps}")

    service_precondition_gaps = find_dead_service_precondition_envs(objects)
    if service_precondition_gaps:
        errors.append(f"Containers use ignored SERVICE_PRECONDITION env: {service_precondition_gaps}")

    airflow_dag_mount_gaps = find_airflow_dag_directory_mounts(objects)
    if airflow_dag_mount_gaps:
        errors.append(f"Airflow DAG ConfigMap must be mounted via subPath, not as a directory: {airflow_dag_mount_gaps}")

    trino_memory_gaps = find_trino_memory_config_gaps(objects)
    if trino_memory_gaps:
        errors.append(f"Trino memory config is incomplete: {trino_memory_gaps}")

    connector_retry_gaps = find_connector_retry_gaps(objects)
    if connector_retry_gaps:
        errors.append(f"Connector registration create path is not retry-safe: {connector_retry_gaps}")

    return errors


def main() -> None:
    objects = parse_objects(render_kustomize())
    errors = validate(objects)
    if errors:
        raise SystemExit("\n".join(errors))
    print(f"Validated {len(objects)} Kubernetes objects")


if __name__ == "__main__":  # pragma: no cover
    main()
