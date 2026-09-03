# Kubernetes

Kubernetes is this platform's production-shaped deployment target — nothing ships to production on
Docker Compose. Compose stays as the fast inner loop because it starts in seconds, which is the
same split most teams actually run. Both runtimes honour the data contracts in [design.md](design.md),
and both load the *same* config files, so neither can drift from the other.

## Current Scope

The current Kubernetes path has a full local-platform manifest set:

- `scripts/k8s_up.sh` creates a local `kind` cluster (via the `kind` CLI).
- Kustomize applies the `data-pipeline` namespace.
- Kustomize generates shared ConfigMaps and Secrets from the existing repo config files and local overlay values.
- Kubernetes defines source Postgres, the catalog database, MinIO object storage, the Iceberg REST catalog, Trino, Kafka (KRaft)/Kafka Connect, connector registration, Spark bronze/silver/gold job templates, Airflow, the gold serving API, and Prometheus/exporters/Grafana.
- The warehouse and checkpoint buckets are created by the DAG's `init_object_store` task over the S3 API; `init_catalog` then registers the warehouse with the catalog. Both are idempotent.
- Connector registration, the demo re-seed, and Spark job manifests are suspended by default so they do not run before their dependencies are Ready.

## One Source of Config

The Kustomize base generates its ConfigMaps directly from the repo's real files — `config/`,
`airflow/dags/`, `scripts/`, and `sql/trino/` — the same ones Docker Compose bind-mounts. Editing a
Spark job or the Postgres seed updates both runtimes at once.

This used to be a hand-maintained copy under `k8s/base/config/`, and it did exactly what copies do:
it decayed. The gold Spark job there still ran a superseded architecture that read from **bronze**,
contradicting the linear `bronze → silver → gold` lineage documented in [design.md](design.md), and
the Postgres seed produced 120 rows instead of 50,000. The copy is gone.

Because those paths sit outside the kustomization root, rendering needs an explicit opt-in:

```bash
kubectl kustomize --load-restrictor=LoadRestrictionsNone k8s/overlays/local
```

`kubectl apply -k` has no such flag, so `scripts/k8s_up.sh` renders and pipes into `kubectl apply -f -`.
The documented tradeoff is that the kustomization is no longer relocatable — irrelevant here, since
it is repo-local and never vendored into another tree.

### The two legitimate overrides

Two files genuinely must differ on Kubernetes, and they live in `k8s/base/overrides/` with a
header comment in each explaining why. There were three until the storage migration removed
`hdfs-site.xml` — a third of that directory existed only to make the NameNode bind correctly
inside a pod network.

| File | Why it differs |
|------|----------------|
| `trino-jvm.config` | `-Xmx512M` instead of `-Xmx2G`; kind packs the whole platform onto one node, so a 2 GB Trino heap starves the Spark and Kafka pods |
| `trino-config.properties` | Adds four query-memory ceilings for the same reason; without them Trino sizes pools off total container memory and is OOMKilled under Iceberg scans |

`tests/test_validate_k8s_manifests.py` guards all of this: no generator path may point back into a
forked config tree, every referenced file must exist, `overrides/` must contain exactly these two
files, and each must document its rationale. The drift cannot silently return.

## Reaching a UI

The kind cluster reserves no host ports, so each UI is reached with its own port-forward. Each
command runs in the foreground until interrupted:

```bash
kubectl port-forward -n data-pipeline svc/airflow-webserver 8088:8080   # Airflow
kubectl port-forward -n data-pipeline svc/trino 8080:8080               # Trino
kubectl port-forward -n data-pipeline svc/api 8000:8000                 # Gold API (/docs)
kubectl port-forward -n data-pipeline svc/minio 9001:9001               # MinIO console
kubectl port-forward -n data-pipeline svc/iceberg-rest 8181:8181        # Iceberg REST catalog
kubectl port-forward -n data-pipeline svc/prometheus 9090:9090          # Prometheus
kubectl port-forward -n data-pipeline svc/grafana 3001:3000             # Grafana
```

The left-hand port matches the Compose URL in the [README](../README.md#docker-compose-fast-inner-loop)
so the same bookmark works in both runtimes; the right-hand port is what the Service actually
listens on. Two differ: Airflow serves 8080 in the pod (Compose publishes it as 8088, and 8080 is
already Trino's), and Grafana serves 3000 (Compose publishes 3001).

## Runtime Status

The manifest set renders and is structurally validated with:

```bash
python3 scripts/validate_k8s_manifests.py
```

That command prints the object count it validated. The manifest set renders and validates clean, and the Kubernetes path has
been verified end to end on a local `kind` cluster running the *current* architecture — gold as
`INSERT OVERWRITE` from silver, the full ~50k-row seed, and ConfigMaps generated from the repo's
real files:

- all core pods Ready in the `data-pipeline` namespace
- warehouse and checkpoint bucket creation, then catalog warehouse registration
- source Postgres seed validation (row counts reconcile end to end)
- Debezium connector registration with connector and task `RUNNING`
- Airflow scheduler parsing the `payments_pipeline` DAG with no import errors (each DAG file is mounted via `subPath` — `payments_pipeline.py` and the `alerts.py` module it imports — so the walker never follows the `..data` symlink into a recursive loop)
- Bronze, Silver, and Gold Spark Jobs completing successfully
- Trino queries over Iceberg reconciling exactly: bronze = silver = `sum(payment_count)` in gold = **50,004**, matching the source Postgres row count, across 8,764 hourly buckets
- the gold serving API answering against that data (`/v1/metrics/summary` returning the same 50,004) with its snapshot-keyed cache registering hits
- Grafana coming up with all three provisioned datasources — including the Trino *plugin* datasource — and both dashboards

## Orchestration

Airflow drives the pipeline on the cluster, not just on Compose. That distinction used to be
hollow: every task was a `BashOperator` running a script that shelled into a Compose container by
name (`docker exec dp-trino`, `docker exec dp-spark`). Airflow on
Kubernetes parsed the DAG but its tasks would have failed if triggered, so the cluster ran its
Spark jobs through `kubectl patch` on suspended Job templates instead.

Two changes closed that:

**Most tasks became runtime-neutral rather than branching.** Trino work goes over Trino's HTTP
protocol (`scripts/trino_http.py`), storage setup over the S3 API (`scripts/init_object_store.py`),
and catalog setup over HTTP (`scripts/init_iceberg_catalog.py`). All address services by name —
`trino:8080`, `minio:9000`, `iceberg-rest:8181` — which resolves as a Compose service
name and a Kubernetes Service DNS name alike. No branch, no duplicated logic; the same code runs
in both.

**Only Spark submission genuinely differs**, because Compose has no cluster to schedule against.
`airflow/dags/spark_jobs.py` returns a `KubernetesPodOperator` when `PIPELINE_RUNTIME=kubernetes`
and a `BashOperator` otherwise. The Kubernetes deployments set that variable; Compose leaves it
unset. The job specs — master, driver memory, packages, script paths — live in one place, and
`tests/test_spark_jobs.py` asserts the Compose command and the Job templates in
`k8s/base/spark.yaml` still agree with them byte for byte.

The pods run as the `spark` ServiceAccount; the `airflow` ServiceAccount is bound to the same
`spark-job-runner` Role so it may create them. That Role also grants `pods/log`, which is what
lets `KubernetesPodOperator` stream the Spark driver's output into the Airflow task log rather
than reporting a bare non-zero exit.

```bash
kubectl exec -n data-pipeline deploy/airflow-scheduler -- \
  airflow dags trigger payments_pipeline -r manual-1
```

What remains is Spark's own shape: jobs run as a single `local[2]` driver rather than a
production-like driver/executor split. That is a scale limitation, not a runtime gap.

## Resource Declarations

Every container declares a memory request and limit. This is not boilerplate: before it was added,
Trino was OOM-killed 15 times, `spark-gold` was OOM-killed on every attempt, and Hive Metastore (since removed)
crash-looped — while the node reported 290Mi requested and `MemoryPressure=False`. With no request
the scheduler treats a pod as free and packs the node; with no limit a JVM sizes its heap off total
node memory rather than its own cgroup, so the kernel picks a victim that may not even be the
offender.

`tests/test_validate_k8s_manifests.py` enforces two things: that no container is missing a request
or limit, and that the always-on set plus the largest single batch job stays inside a ~6.5 GiB
budget, so a newly added service cannot quietly make `scripts/k8s_up.sh` unschedulable on a laptop.

## Prerequisites

- Docker Desktop, with **at least 12 GiB** allocated (Settings → Resources → Memory)
- kind
- kubectl

The memory figure is a real requirement, not a suggestion. Steady-state requests come to ~5.8 GiB
and a Spark job adds 1 GiB, so the platform needs roughly 6.8 GiB reserved before the kubelet,
system pods, and the burst between each pod's request and its limit. That figure predates the
storage migration, which cut roughly 3 GiB from declared limits by removing HDFS and Hive
Metastore; the platform now fits comfortably under it.
`tests/test_validate_k8s_manifests.py` keeps the declared footprint inside that budget.

## Start the Local Cluster

```bash
bash scripts/k8s_up.sh
```

The script runs (idempotently — safe to re-run):

```bash
# create the cluster only if it does not already exist
kind get clusters | grep -qx data-pipeline \
  || kind create cluster --name data-pipeline \
       --config k8s/kind-config.yaml --kubeconfig .kind/kubeconfig --wait 120s

# Jobs have immutable pod templates, so delete any existing ones before re-applying
KUBECONFIG=.kind/kubeconfig kubectl delete jobs --all -n data-pipeline --ignore-not-found

# Render with the load restrictor relaxed (the generators read repo-root config), then apply
KUBECONFIG=.kind/kubeconfig kubectl kustomize \
  --load-restrictor=LoadRestrictionsNone k8s/overlays/local \
  | KUBECONFIG=.kind/kubeconfig kubectl apply -f -
```

The script also builds and loads the four local images into the kind cluster: Airflow, the serving API, the Trino exporter, and Spark (upstream `spark:3.5.8-python3` with the medallion's `--packages` tree pre-resolved -- see `config/spark/Dockerfile`).

Cluster passwords come from `k8s/overlays/local/secrets.env` (gitignored). On first run the script seeds it from `secrets.env.example` with placeholder values — edit it to change local credentials; real values never land in git, mirroring the Compose `.env` / `.env.example` pattern.

## Inspect

```bash
export KUBECONFIG=.kind/kubeconfig
kubectl get ns
kubectl get configmaps -n data-pipeline
kubectl get secrets -n data-pipeline
kubectl get deployments,statefulsets,jobs,pods,svc,pvc -n data-pipeline
python3 scripts/validate_k8s_manifests.py
```

Expected baseline result after dependencies pull and start:

```text
statefulset.apps/postgres        1/1
statefulset.apps/catalog-db      1/1
persistentvolumeclaim/...        Bound
```

You can verify the seeded source table with:

```bash
kubectl exec -n data-pipeline postgres-0 -- \
  psql -U dataeng -d payments -c "SELECT COUNT(*) AS payments_count FROM payments;"
```

The seed loads a configurable volume (default ~50k payments spread over 12 months); Bronze, Silver, and Gold then reconcile to the same count.

Connector registration and Spark job templates are suspended by default. Start or recreate them only after their dependencies are Ready.

```bash
kubectl patch job register-postgres-cdc -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/register-postgres-cdc -n data-pipeline --timeout=180s

kubectl patch job spark-bronze -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/spark-bronze -n data-pipeline --timeout=600s

kubectl patch job spark-silver -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/spark-silver -n data-pipeline --timeout=600s

kubectl patch job spark-gold -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/spark-gold -n data-pipeline --timeout=600s
```

`seed-demo-data` is suspended for the same reason, but is not part of startup — the database seeds
itself on an empty data directory. Unsuspend it only to replay the seed into a cluster that is
already running:

```bash
kubectl patch job seed-demo-data -n data-pipeline -p '{"spec":{"suspend":false}}'
kubectl wait --for=condition=complete job/seed-demo-data -n data-pipeline --timeout=120s
```

Kubernetes Jobs have immutable pod templates, so re-running it a second time needs
`kubectl delete job seed-demo-data -n data-pipeline` first, then a re-apply.

Validate the serving layer with:

```bash
kubectl exec -n data-pipeline deploy/trino -- \
  trino --execute "SELECT count(*) FROM iceberg.analytics.payments_bronze"
kubectl exec -n data-pipeline deploy/trino -- \
  trino --execute "SELECT count(*) FROM iceberg.analytics.payments_silver"
kubectl exec -n data-pipeline deploy/trino -- \
  trino --execute "SELECT count(*), sum(payment_count) FROM iceberg.analytics.payment_metrics_gold"
```

## Stop

```bash
bash scripts/k8s_down.sh
```

## Keeping This Page True

Every list on this page duplicates something that lives in the manifests, and a duplicate that
nothing checks is how the last round of drift happened. So:

- Add a workload here only after its manifest exists and `bash scripts/k8s_up.sh` creates it, and
  add it to `scripts/k8s_verify.sh` in the same change — `tests/test_validate_k8s_manifests.py`
  fails if the script and `k8s/base/` disagree, and if a port-forward above names a port the
  Service does not expose.
- Prefer dependency-focused verification over broad claims: that the catalog reaches its
  database, that Trino queries Iceberg metadata, that Kafka Connect holds the Debezium connector.
- Update [design.md](design.md) when runtime behavior or a limitation changes, not for manifest
  inventory churn.
