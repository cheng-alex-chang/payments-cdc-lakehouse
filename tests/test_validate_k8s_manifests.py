from __future__ import annotations

import re
import subprocess
from pathlib import Path
from xml.etree import ElementTree

import pytest
import yaml

from scripts import validate_k8s_manifests as module


MANIFEST = """
apiVersion: v1
kind: Namespace
metadata:
  name: data-pipeline
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: data-pipeline
---
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-bronze
  namespace: data-pipeline
spec:
  suspend: true
"""


def test_parse_objects_extracts_identity_and_namespace() -> None:
    objects = module.parse_objects(MANIFEST)

    assert objects[0] == module.K8sObject(
        kind="Namespace",
        name="data-pipeline",
        namespace=None,
        raw=objects[0].raw,
    )
    assert objects[1].kind == "StatefulSet"
    assert objects[1].name == "postgres"
    assert objects[1].namespace == "data-pipeline"


def test_find_missing_required_reports_absent_objects() -> None:
    objects = module.parse_objects(MANIFEST)

    missing = module.find_missing_required(objects)

    assert ("StatefulSet", "catalog-db") in missing
    assert ("StatefulSet", "postgres") not in missing


def test_find_unsuspended_template_jobs_flags_only_template_jobs() -> None:
    manifest = """
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-bronze
  namespace: data-pipeline
spec: {}
---
apiVersion: batch/v1
kind: Job
metadata:
  name: airflow-init
  namespace: data-pipeline
spec: {}
"""

    unsuspended = module.find_unsuspended_template_jobs(module.parse_objects(manifest))

    assert unsuspended == ["spark-bronze"]


def test_find_namespaced_workload_gaps_reports_missing_namespace() -> None:
    manifest = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: trino
---
apiVersion: v1
kind: Service
metadata:
  name: trino
"""

    gaps = module.find_namespaced_workload_gaps(module.parse_objects(manifest))

    assert gaps == ["Deployment/trino"]


def test_find_airflow_migration_wait_gaps_flags_missing_init_container() -> None:
    manifest = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: data-pipeline
spec:
  template:
    spec:
      initContainers:
        - name: wait-for-airflow-postgres
"""

    gaps = module.find_airflow_migration_wait_gaps(module.parse_objects(manifest))

    assert gaps == ["Deployment/airflow-webserver"]


def test_find_dead_service_precondition_envs_flags_ignored_env() -> None:
    manifest = """
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: datanode
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: datanode
          env:
            - name: SERVICE_PRECONDITION
              value: namenode:9870
"""

    gaps = module.find_dead_service_precondition_envs(module.parse_objects(manifest))

    assert gaps == ["StatefulSet/datanode/datanode"]


def test_find_airflow_dag_directory_mounts_flags_missing_subpath() -> None:
    manifest = """
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: scheduler
          volumeMounts:
            - name: airflow-dags
              mountPath: /opt/airflow/dags
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: webserver
          volumeMounts:
            - name: airflow-dags
              mountPath: /opt/airflow/dags/payments_pipeline.py
              subPath: payments_pipeline.py
"""

    gaps = module.find_airflow_dag_directory_mounts(module.parse_objects(manifest))

    assert gaps == ["Deployment/airflow-scheduler/scheduler"]


def test_find_trino_memory_config_gaps_requires_per_node_caps() -> None:
    manifest = """
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-etc
  namespace: data-pipeline
data:
  config.properties: |
    coordinator=true
"""

    gaps = module.find_trino_memory_config_gaps(module.parse_objects(manifest))

    assert "ConfigMap/trino-etc:memory.heap-headroom-per-node" in gaps
    assert "ConfigMap/trino-etc:query.max-memory-per-node" in gaps


def test_find_connector_retry_gaps_requires_create_http_error_handling() -> None:
    manifest = """
apiVersion: batch/v1
kind: Job
metadata:
  name: register-postgres-cdc
  namespace: data-pipeline
spec:
  suspend: true
  template:
    spec:
      containers:
        - name: register
          command:
            - python
            - -c
            - urllib.request.urlopen(req)
"""

    gaps = module.find_connector_retry_gaps(module.parse_objects(manifest))

    assert gaps == ["Job/register-postgres-cdc"]


def test_validate_combines_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "REQUIRED_OBJECTS", {("StatefulSet", "postgres")})
    objects = module.parse_objects(
        """
apiVersion: batch/v1
kind: Job
metadata:
  name: spark-gold
  namespace: default
spec: {}
"""
    )

    errors = module.validate(objects)

    assert "Missing required objects" in errors[0]
    assert "Template jobs must be suspended" in errors[1]
    assert "Workloads missing data-pipeline namespace" in errors[2]


def test_render_kustomize_invokes_kubectl(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[list[str]] = []

    def fake_run(command, check, capture_output, text):  # noqa: ANN001
        calls.append(command)
        return subprocess.CompletedProcess(command, 0, stdout="kind: List\n")

    monkeypatch.setattr(module.subprocess, "run", fake_run)

    rendered = module.render_kustomize(Path("k8s/overlays/local"))

    assert rendered == "kind: List\n"
    # The load-restrictor flag is load-bearing, not cosmetic: the base kustomization generates
    # ConfigMaps from the repo's real config files, whose paths sit outside the kustomization
    # root. Without it kubectl refuses to render at all.
    assert calls == [
        ["kubectl", "kustomize", "--load-restrictor=LoadRestrictionsNone", "k8s/overlays/local"]
    ]


def test_main_exits_when_validation_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(module, "render_kustomize", lambda: MANIFEST)

    with pytest.raises(SystemExit, match="Missing required objects"):
        module.main()


def test_main_prints_success(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    specialized_objects = {
        ("Deployment", "airflow-webserver"),
        ("Deployment", "airflow-scheduler"),
        ("Job", "hdfs-init"),
        ("Job", "register-postgres-cdc"),
    }
    required_manifest = "\n---\n".join(
        f"""
apiVersion: v1
kind: {kind}
metadata:
  name: {name}
  namespace: data-pipeline
spec:
  suspend: true
"""
        for kind, name in module.REQUIRED_OBJECTS
        if (kind, name) not in specialized_objects
    )
    manifest = required_manifest + """
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-webserver
  namespace: data-pipeline
spec:
  template:
    spec:
      initContainers:
        - name: wait-for-airflow-migrations
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: airflow-scheduler
  namespace: data-pipeline
spec:
  template:
    spec:
      initContainers:
        - name: wait-for-airflow-migrations
---
apiVersion: batch/v1
kind: Job
metadata:
  name: hdfs-init
  namespace: data-pipeline
spec:
  template:
    spec:
      containers:
        - name: hdfs-init
          command:
            - hdfs dfsadmin -report
            - Live datanodes
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: trino-etc
  namespace: data-pipeline
data:
  config.properties: |
    query.max-memory=256MB
    query.max-memory-per-node=128MB
    memory.heap-headroom-per-node=128MB
---
apiVersion: batch/v1
kind: Job
metadata:
  name: register-postgres-cdc
  namespace: data-pipeline
spec:
  suspend: true
  template:
    spec:
      containers:
        - name: register
          command:
            - except urllib.error.HTTPError as create_exc
            - create_exc.code == 409
"""
    monkeypatch.setattr(module, "render_kustomize", lambda: manifest)

    module.main()

    assert "Validated" in capsys.readouterr().out


# --- Anti-drift guards -------------------------------------------------------------------
#
# k8s/base/config/ used to be a hand-maintained copy of the repo's config, and it silently
# decayed a full architectural refactor behind the Compose path (the gold job still read from
# bronze; the seed was 120 rows instead of 50,000). The copy is gone -- Kustomize now reads the
# real files -- and these tests exist so it cannot come back.

REPO_ROOT = Path(__file__).resolve().parents[1]
K8S_BASE = REPO_ROOT / "k8s" / "base"
BASE_KUSTOMIZATION = K8S_BASE / "kustomization.yaml"

# The only files allowed to differ between the Compose and Kubernetes runtimes. Each needs a
# genuine Kubernetes reason (pod memory ceilings, bind-host settings) documented in its header.
EXPECTED_OVERRIDES = {"trino-config.properties", "trino-jvm.config"}


def _generator_file_paths() -> list[str]:
    """Every `key=path` entry across the base kustomization's configMapGenerator."""
    spec = yaml.safe_load(BASE_KUSTOMIZATION.read_text(encoding="utf-8"))
    paths: list[str] = []
    for generator in spec["configMapGenerator"]:
        for entry in generator.get("files", []):
            _, _, path = entry.partition("=")
            paths.append(path)
    return paths


def test_kustomization_references_no_forked_config_tree() -> None:
    # A copy under k8s/base/config/ is exactly the failure mode that caused the drift.
    forked_tree = K8S_BASE / "config"
    offenders = [
        path for path in _generator_file_paths() if forked_tree in (K8S_BASE / path).resolve().parents
    ]

    assert offenders == [], f"generator paths point back into a forked config tree: {offenders}"
    assert not forked_tree.exists(), "k8s/base/config/ is back; generators must read the real files"


def test_every_generator_file_path_resolves_to_an_existing_file() -> None:
    # Paths are relative to the kustomization's own directory. A rename upstream would otherwise
    # surface as a failed `kubectl apply` twenty minutes into a cluster spin-up.
    missing = [path for path in _generator_file_paths() if not (K8S_BASE / path).resolve().is_file()]

    assert missing == [], f"configMapGenerator references files that do not exist: {missing}"


def test_generators_read_the_same_files_compose_uses() -> None:
    # Everything except the three documented overrides must escape k8s/ into the repo root, so
    # both runtimes load byte-identical config.
    non_override = [path for path in _generator_file_paths() if not path.startswith("overrides/")]

    assert non_override, "expected shared config paths"
    assert all(path.startswith("../../") for path in non_override)


def test_overrides_directory_holds_only_the_documented_files() -> None:
    # Stops overrides/ from quietly growing into a new shadow tree.
    present = {path.name for path in (K8S_BASE / "overrides").iterdir() if path.is_file()}

    assert present == EXPECTED_OVERRIDES


def test_every_xml_document_is_well_formed() -> None:
    """XML forbids a double hyphen inside a comment, and it is very easy to type.

    This has now cost real time twice. A malformed hdfs-site.xml crash-looped the NameNode twenty
    minutes into a cluster spin-up, with a stack trace naming a Woodstox parser rather than the file
    that was edited. Later the same mistake made docs/favicon.svg unparseable.

    HDFS and Hive are gone, so no XML *config* remains -- but SVG is XML, and the repo has several
    hand-authored ones. The check is kept and widened rather than deleted, because the bug class
    outlived the files that first triggered it. It passes vacuously if a category disappears; that
    is the intended behaviour, not a gap.
    """
    documents = sorted(REPO_ROOT.glob("config/**/*.xml"))
    documents += sorted((K8S_BASE / "overrides").glob("*.xml"))
    documents += sorted(REPO_ROOT.glob("docs/*.svg")) + sorted(REPO_ROOT.glob("docs/images/*.svg"))

    assert documents, "expected some hand-authored XML or SVG to check"
    for path in documents:
        ElementTree.parse(path)  # raises ParseError on malformed input


def test_each_override_documents_why_it_differs() -> None:
    for name in EXPECTED_OVERRIDES:
        text = (K8S_BASE / "overrides" / name).read_text(encoding="utf-8")

        assert "override of config/" in text, f"{name} does not name the file it overrides"
        assert "docs/kubernetes.md" in text, f"{name} does not point at the rationale doc"


def test_airflow_dags_configmap_ships_the_alerts_module() -> None:
    # payments_pipeline.py does `from alerts import notify_failure`; without alerts.py in the
    # ConfigMap the DagFileProcessor cannot import the DAG at all.
    paths = _generator_file_paths()

    assert "../../airflow/dags/payments_pipeline.py" in paths
    assert "../../airflow/dags/alerts.py" in paths


def test_airflow_deployments_mount_both_dag_files() -> None:
    # The ConfigMap is mounted per-file via subPath (a directory mount makes Airflow's DAG walker
    # follow the `..data` symlink into a recursive loop), so each file needs its own mount.
    text = (K8S_BASE / "airflow.yaml").read_text(encoding="utf-8")

    assert text.count("subPath: payments_pipeline.py") == text.count("subPath: alerts.py")
    assert text.count("subPath: alerts.py") == 3, "expected webserver, scheduler, and init mounts"


def test_grafana_provisioning_is_generated_from_the_shared_config() -> None:
    # Kubernetes Grafana used to mount nothing but an emptyDir, so the cluster came up with no
    # datasources and no dashboards while Compose had both. A monitoring tier that only works on
    # the non-production runtime is worse than none.
    paths = _generator_file_paths()

    assert "../../config/grafana/provisioning/datasources/prometheus.yml" in paths
    assert "../../config/grafana/provisioning/dashboards/dashboards.yml" in paths
    assert "../../config/grafana/dashboards/pipeline-output.json" in paths
    assert "../../config/grafana/dashboards/platform-overview.json" in paths


def test_grafana_deployment_mounts_provisioning_and_installs_the_trino_plugin() -> None:
    text = (K8S_BASE / "observability.yaml").read_text(encoding="utf-8")

    # dashboards.yml points the file provider at /var/lib/grafana/dashboards, so the JSON must
    # land exactly there or the provider silently finds nothing.
    assert "mountPath: /etc/grafana/provisioning/datasources" in text
    assert "mountPath: /etc/grafana/provisioning/dashboards" in text
    assert "mountPath: /var/lib/grafana/dashboards" in text
    # "Payments Gold (Trino)" is a plugin datasource; without the plugin every gold panel is blank.
    assert "GF_INSTALL_PLUGINS" in text
    assert "trino-datasource" in text


def test_kafka_readiness_probe_allows_time_for_a_jvm_to_start() -> None:
    """The Kafka probe shells out to kafka-broker-api-versions, which boots a JVM.

    timeoutSeconds defaults to 1. Measured cold-start for that tool is ~1s idle and longer under
    load, so with the default the probe times out against a healthy broker and kafka-0 never goes
    Ready -- Kafka serves traffic while every dependent rollout blocks waiting for it. The other
    exec probes in this repo run pg_isready, a millisecond-scale binary, so they are fine as-is.
    """
    spec = yaml.safe_load_all((K8S_BASE / "kafka.yaml").read_text(encoding="utf-8"))
    stateful_sets = [doc for doc in spec if doc and doc.get("kind") == "StatefulSet"]
    containers = [
        container
        for sts in stateful_sets
        for container in sts["spec"]["template"]["spec"]["containers"]
        if container["name"] == "kafka"
    ]

    assert containers, "expected a kafka container"
    probe = containers[0]["readinessProbe"]
    assert "exec" in probe, "guard assumes an exec probe; revisit if this became httpGet"
    assert probe.get("timeoutSeconds", 1) >= 10


def _memory_mib(value: str) -> int:
    text = str(value)
    if text.endswith("Gi"):
        return int(text[:-2]) * 1024
    if text.endswith("Mi"):
        return int(text[:-2])
    raise ValueError(f"unhandled memory unit: {value}")


def _workload_containers() -> list[tuple[str, str, str, dict]]:
    """(kind, workload, container, resources) for every container in the base manifests."""
    found: list[tuple[str, str, str, dict]] = []
    for path in sorted(K8S_BASE.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if not doc or doc.get("kind") not in {"Deployment", "StatefulSet", "Job"}:
                continue
            for container in doc["spec"]["template"]["spec"]["containers"]:
                found.append(
                    (doc["kind"], doc["metadata"]["name"], container["name"],
                     container.get("resources") or {})
                )
    return found


def test_every_container_declares_a_memory_request_and_limit() -> None:
    """Undeclared resources are how a single-node cluster eats itself.

    With no request the scheduler treats a pod as free and packs the node; with no limit a JVM
    sizes its heap off total node memory rather than its own cgroup. Observed on this repo before
    the fix: Trino OOM-killed 15 times, spark-gold OOM-killed on every attempt, hive-metastore
    crash-looping -- while the node reported 290Mi requested and no memory pressure.
    """
    gaps = [
        f"{kind}/{workload}/{container}"
        for kind, workload, container, resources in _workload_containers()
        if not (resources.get("requests") or {}).get("memory")
        or not (resources.get("limits") or {}).get("memory")
    ]

    assert gaps == [], f"containers without memory requests/limits: {gaps}"


def test_steady_state_footprint_fits_a_single_node_dev_cluster() -> None:
    """Long-running workloads plus one Spark job must fit a modest Docker allocation.

    Spark jobs are suspended by default and run one at a time, and the *-init Jobs are one-shot,
    so the meaningful figure is the always-on set plus the largest single batch job -- not the sum
    of every container in the repo. This guard is what stops a newly added service from quietly
    making `bash scripts/k8s_up.sh` unschedulable on a laptop.
    """
    containers = _workload_containers()

    steady = sum(
        _memory_mib(resources["requests"]["memory"])
        for kind, _, _, resources in containers
        if kind in {"Deployment", "StatefulSet"}
    )
    largest_job = max(
        _memory_mib(resources["requests"]["memory"])
        for kind, _, _, resources in containers
        if kind == "Job"
    )

    # 8 GiB against a 12 GiB Docker allocation, leaving headroom for the kubelet, system pods,
    # and the burst between a pod's request and its limit. The full platform plus Spark does not
    # fit in 8 GiB of Docker memory -- docs/kubernetes.md states the requirement.
    assert steady + largest_job <= 8192, (
        f"steady state {steady} Mi + largest job {largest_job} Mi exceeds the dev-cluster budget"
    )


def test_shipped_scripts_have_their_local_imports_shipped_too() -> None:
    """A script in the ConfigMap is useless if the module it imports was left behind.

    `trino_http.py` was omitted on the first pass, so publish_trino_tables, validate_trino, and
    maintain_iceberg all reached the cluster importing a module that was not there -- invisible
    to every test until the DAG genuinely ran on Kubernetes.
    """
    shipped = {
        Path(path).name
        for path in _generator_file_paths()
        if path.startswith("../../scripts/")
    }

    assert shipped, "expected pipeline scripts in the ConfigMap"
    for name in sorted(shipped):
        source = (REPO_ROOT / "scripts" / name).read_text(encoding="utf-8")
        for line in source.splitlines():
            stripped = line.strip()
            if not stripped.startswith("from scripts import "):
                continue
            for imported in stripped.removeprefix("from scripts import ").split(","):
                module = f"{imported.strip()}.py"
                assert module in shipped, f"{name} imports {module}, which is not in the ConfigMap"


def test_no_config_env_var_collides_with_kubernetes_service_variables() -> None:
    """Kubernetes reserves `<SERVICE>_PORT` in every pod's environment.

    For each Service in the namespace, Kubernetes injects Docker-link-style variables --
    `TRINO_PORT=tcp://10.96.15.110:8080`, `TRINO_SERVICE_HOST`, `TRINO_SERVICE_PORT`. Naming our
    own config variable `TRINO_PORT` meant the pipeline read a URI where it expected a number.
    Invisible under Compose, and it only failed once the DAG ran on the cluster.
    """
    services = {
        doc["metadata"]["name"]
        for path in sorted(K8S_BASE.glob("*.yaml"))
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8"))
        if doc and doc.get("kind") == "Service"
    }
    reserved = {f"{name.upper().replace('-', '_')}_PORT" for name in services}

    sources = list((REPO_ROOT / "scripts").glob("*.py")) + list((REPO_ROOT / "api" / "src").glob("*.py"))
    offenders = [
        f"{path.name}:{name}"
        for path in sources
        for name in reserved
        # Quoted usage means we read or set it as config; a bare mention in prose is fine.
        if f'"{name}"' in path.read_text(encoding="utf-8")
    ]

    assert offenders == [], f"config env vars shadowed by Kubernetes Service variables: {offenders}"


def test_airflow_connection_env_exists_in_both_runtimes() -> None:
    """Every AIRFLOW_CONN_* the Compose Airflow defines must exist on Kubernetes too.

    The DAG's tasks read these to reach the source database. Compose had
    AIRFLOW_CONN_SOURCE_POSTGRES from the start and Kubernetes never did, so validate_schema
    failed with a bare KeyError the first time the DAG genuinely ran on the cluster -- invisible
    until then, because the Kubernetes path drove Spark through `kubectl patch` rather than
    through Airflow.
    """
    compose = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    kubernetes = (K8S_BASE / "airflow.yaml").read_text(encoding="utf-8")

    compose_conns = {
        line.split(":")[0].strip()
        for line in compose.splitlines()
        if line.strip().startswith("AIRFLOW_CONN_")
    }

    assert compose_conns, "expected Compose to define at least one Airflow connection"
    missing = [name for name in compose_conns if name not in kubernetes]
    assert missing == [], f"Airflow connections defined only under Compose: {missing}"


def test_spark_jobs_declare_memory_requests_and_limits() -> None:
    """An unbounded Spark driver takes the whole node with it.

    With no requests the scheduler cannot reason about the pod, and with no limit the JVM sizes
    its heap off the node's total memory rather than the pod's, so the kernel eventually OOM-kills
    it -- taking whichever container it picks, not necessarily Spark. Observed directly: every
    spark-gold attempt was OOMKilled while the node reported only 290Mi requested.
    """
    docs = [
        doc
        for doc in yaml.safe_load_all((K8S_BASE / "spark.yaml").read_text(encoding="utf-8"))
        if doc and doc.get("kind") == "Job"
    ]

    assert len(docs) == 3, "expected bronze, silver, and gold jobs"
    for job in docs:
        container = job["spec"]["template"]["spec"]["containers"][0]
        resources = container.get("resources", {})
        assert resources.get("requests", {}).get("memory"), f"{job['metadata']['name']}: no request"
        assert resources.get("limits", {}).get("memory"), f"{job['metadata']['name']}: no limit"

        command = container["command"]
        # local[*] would start one task thread per core inside the single driver JVM.
        assert "local[*]" not in command
        assert "--driver-memory" in command



def test_snowflake_dag_is_excluded_from_the_cluster() -> None:
    # snowflake_fx_etl.py imports SnowflakeOperator and snowflake_etl.src, neither of which is in
    # the cluster Airflow image. Shipping it would break scheduler DAG parsing outright.
    assert not any("snowflake_fx_etl" in path for path in _generator_file_paths())


def _workloads_in_manifests() -> dict[str, set[str]]:
    """Every Deployment and StatefulSet name declared under k8s/base, keyed by lowercase kind."""
    found: dict[str, set[str]] = {"statefulset": set(), "deployment": set()}
    for path in sorted(K8S_BASE.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if not doc:
                continue
            kind = doc.get("kind", "").lower()
            if kind in found:
                found[kind].add(doc["metadata"]["name"])
    return found


def _workloads_checked_by_verify_script() -> dict[str, set[str]]:
    """The STATEFULSETS and DEPLOYMENTS bash arrays in scripts/k8s_verify.sh."""
    text = (REPO_ROOT / "scripts" / "k8s_verify.sh").read_text(encoding="utf-8")
    checked: dict[str, set[str]] = {}
    for variable, kind in (("STATEFULSETS", "statefulset"), ("DEPLOYMENTS", "deployment")):
        match = re.search(rf"^{variable}=\(([^)]*)\)", text, re.MULTILINE)
        assert match, f"{variable} array not found in scripts/k8s_verify.sh"
        checked[kind] = set(match.group(1).split())
    return checked


def test_verify_script_covers_every_workload() -> None:
    # k8s_verify.sh drifted behind the manifests once already: it silently skipped the API, the
    # Airflow scheduler, and Kafka, so it reported success on a cluster missing the tier that
    # serves data and the one that orchestrates it. A hand-maintained list that must match another
    # list stays correct only if something asserts the match.
    manifests = _workloads_in_manifests()
    checked = _workloads_checked_by_verify_script()

    for kind in ("statefulset", "deployment"):
        unverified = manifests[kind] - checked[kind]
        assert not unverified, (
            f"{kind}s in k8s/base but not checked by scripts/k8s_verify.sh: {sorted(unverified)}"
        )
        phantom = checked[kind] - manifests[kind]
        assert not phantom, (
            f"{kind}s checked by scripts/k8s_verify.sh but absent from k8s/base: {sorted(phantom)}"
        )


def test_verify_script_checks_readiness_not_mere_existence() -> None:
    # `kubectl get deployment api` exits 0 while every pod is in CrashLoopBackOff, so the original
    # existence-only script passed on a fully broken cluster.
    text = (REPO_ROOT / "scripts" / "k8s_verify.sh").read_text(encoding="utf-8")
    assert "rollout status" in text

    # Strip comments first -- the script's own header quotes `kubectl get deployment api` while
    # explaining why that check is worthless, and prose is not what this test is about.
    commands = "\n".join(
        line for line in text.splitlines() if not line.lstrip().startswith("#")
    )
    for weak in ("get statefulset ", "get deployment "):
        assert weak not in commands, f"existence-only check {weak!r} is back in k8s_verify.sh"


def test_documented_port_forwards_match_service_ports() -> None:
    """Every `kubectl port-forward` in docs/kubernetes.md must name a real Service port.

    The first draft of that section published Airflow as `8088:8088`; the Service listens on 8080,
    so every command copied from the docs would have failed. Prose that names a port is another
    hand-maintained copy of the manifests, so it gets the same treatment as the verify script.
    """
    service_ports: dict[str, set[int]] = {}
    for path in sorted(K8S_BASE.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if doc and doc.get("kind") == "Service":
                name = doc["metadata"]["name"]
                service_ports[name] = {port["port"] for port in doc["spec"]["ports"]}

    doc_text = (REPO_ROOT / "docs" / "kubernetes.md").read_text(encoding="utf-8")
    commands = re.findall(
        r"port-forward\s+-n\s+data-pipeline\s+svc/(\S+)\s+(\d+):(\d+)", doc_text
    )
    assert commands, "no port-forward commands found in docs/kubernetes.md"

    for service, _local, remote in commands:
        assert service in service_ports, f"docs reference svc/{service}, which k8s/base does not define"
        assert int(remote) in service_ports[service], (
            f"docs forward svc/{service} to port {remote}, but that Service exposes "
            f"{sorted(service_ports[service])}"
        )


def _seed_job() -> dict:
    for doc in yaml.safe_load_all((K8S_BASE / "seed-demo-data.yaml").read_text(encoding="utf-8")):
        if doc and doc.get("kind") == "Job" and doc["metadata"]["name"] == "seed-demo-data":
            return doc
    raise AssertionError("seed-demo-data Job not found")


def test_seed_job_replays_the_same_configmap_the_database_mounts() -> None:
    # The point of the Job is to reload the *real* seed. If it carried its own copy of the SQL --
    # inline, or from a second ConfigMap -- it would be one more hand-maintained duplicate, and it
    # would eventually seed something the database never contained.
    job = _seed_job()
    postgres = next(
        doc
        for doc in yaml.safe_load_all((K8S_BASE / "postgres.yaml").read_text(encoding="utf-8"))
        if doc and doc.get("kind") == "StatefulSet"
    )

    def configmap_names(spec: dict) -> set[str]:
        return {
            volume["configMap"]["name"]
            for volume in spec["template"]["spec"].get("volumes", [])
            if "configMap" in volume
        }

    assert "postgres-init" in configmap_names(job["spec"])
    assert configmap_names(job["spec"]) <= configmap_names(postgres["spec"]), (
        "the seed Job must not mount config the source database does not"
    )


def test_seed_job_ships_suspended_and_fails_loudly() -> None:
    job = _seed_job()
    # Unsuspended, it would run at cluster creation -- racing the StatefulSet's own init.
    assert job["spec"]["suspend"] is True

    container = job["spec"]["template"]["spec"]["containers"][0]
    script = container["command"][-1]
    # Without ON_ERROR_STOP, psql prints errors and still exits 0, so a Job that seeded nothing
    # would report Complete.
    assert "ON_ERROR_STOP=1" in script
    # Same image as the server, so psql cannot be a version behind the database it writes to.
    assert container["image"] == postgres_image()


def postgres_image() -> str:
    for doc in yaml.safe_load_all((K8S_BASE / "postgres.yaml").read_text(encoding="utf-8")):
        if doc and doc.get("kind") == "StatefulSet":
            return doc["spec"]["template"]["spec"]["containers"][0]["image"]
    raise AssertionError("postgres StatefulSet not found")


def test_every_workload_declares_a_readiness_probe() -> None:
    """Unprobed workloads make scripts/k8s_verify.sh report readiness it never checked.

    `kubectl rollout status` is satisfied when the desired replicas are *available*, and without a
    readinessProbe a container counts as available the instant it starts. Six workloads -- both
    Airflow halves, Prometheus, Grafana, and the two exporters -- had none, so the verify script
    would call the platform ready while Airflow was still parsing DAGs.
    """
    missing = []
    for path in sorted(K8S_BASE.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if not doc or doc.get("kind") not in ("Deployment", "StatefulSet"):
                continue
            for container in doc["spec"]["template"]["spec"]["containers"]:
                if "readinessProbe" not in container:
                    missing.append(f"{doc['kind']}/{doc['metadata']['name']}/{container['name']}")

    assert not missing, f"workloads without a readinessProbe: {missing}"


def test_every_probe_sets_timeout_seconds_explicitly() -> None:
    """No probe may run on the implicit 1-second default, whatever its type.

    This started as an exec-only rule, on the theory that only a spawned process could be too slow.
    A live cluster disproved that: the API's readiness probe hits /v1/ready, which queries Trino,
    and the event log recorded `context deadline exceeded` during startup. Warm it answers in 20ms,
    so nothing in a test would have caught it -- and three slow probes in a row would have pulled a
    healthy API out of its Service endpoints.

    The rule that survives is simpler than "exec probes are slow": a probe reaching anything beyond
    its own process needs a stated budget, and writing it down forces the author to think about
    what the check actually costs. Kafka (15s) and the Airflow scheduler (20s) are the extremes.
    """
    implicit = []
    for path in sorted(K8S_BASE.glob("*.yaml")):
        for doc in yaml.safe_load_all(path.read_text(encoding="utf-8")):
            if not doc or doc.get("kind") not in ("Deployment", "StatefulSet"):
                continue
            for container in doc["spec"]["template"]["spec"]["containers"]:
                for kind in ("readinessProbe", "livenessProbe", "startupProbe"):
                    probe = container.get(kind)
                    if probe and "timeoutSeconds" not in probe:
                        implicit.append(f"{doc['metadata']['name']}/{container['name']}/{kind}")

    assert not implicit, f"probes relying on the 1s default timeoutSeconds: {implicit}"


def test_container_images_are_pinned_to_a_version() -> None:
    """`:latest` makes a deployment unreproducible -- two clusters built a week apart differ.

    Covers both runtimes from one place: the Kustomize base and the Compose file.
    """
    sources = list(K8S_BASE.glob("*.yaml")) + [REPO_ROOT / "docker-compose.yml"]
    unpinned = []
    for path in sources:
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip().lstrip("- ")
            if not stripped.startswith("image:"):
                continue
            image = stripped.split("image:", 1)[1].strip().strip("\"'")
            if "${" in image:  # Compose interpolation, resolved at run time
                continue
            tag = image.rsplit(":", 1)[1] if ":" in image.rsplit("/", 1)[-1] else ""
            if tag in ("", "latest"):
                unpinned.append(f"{path.name}:{number} {image}")

    assert not unpinned, f"unpinned images: {unpinned}"


def test_shell_scripts_reference_no_deleted_paths() -> None:
    """A script that builds a deleted directory fails twenty minutes into a cluster spin-up.

    scripts/k8s_up.sh kept building local/hive-metastore:dev after config/hive-metastore/ was
    removed, and nothing caught it -- the manifests rendered fine, every test passed, and the
    failure only appeared on the next cold start. Paths referenced by shell scripts are as much a
    dependency as an import.
    """
    import re

    for script in sorted((REPO_ROOT / "scripts").glob("*.sh")):
        text = script.read_text(encoding="utf-8")
        for match in re.finditer(r"(?:^|\s)((?:config|drivers|k8s|airflow|sql)/[A-Za-z0-9_./-]+)", text):
            candidate = match.group(1).rstrip(".,;:")
            assert (REPO_ROOT / candidate).exists(), (
                f"{script.name} references {candidate}, which does not exist"
            )
