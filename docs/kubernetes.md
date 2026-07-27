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
- Kubernetes defines source Postgres, metastore database, HDFS NameNode/DataNode, Hive Metastore, Trino, Kafka (KRaft)/Kafka Connect, connector registration, Spark bronze/silver/gold job templates, Airflow, the gold serving API, and Prometheus/exporters/Grafana.
- An `hdfs-init` Job prepares local warehouse and checkpoint directories for Spark writes.
- Connector registration and Spark job manifests are suspended by default so they do not run before their dependencies are Ready.

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

### The three legitimate overrides

Three files genuinely must differ on Kubernetes, and they live in `k8s/base/overrides/` with a
header comment in each explaining why:

| File | Why it differs |
|------|----------------|
| `hdfs-site.xml` | NameNode must bind `0.0.0.0` rather than the Service DNS name, and DataNode registration's reverse-DNS check must be off — pod IPs have no matching PTR record |
| `trino-jvm.config` | `-Xmx512M` instead of `-Xmx2G`; kind packs the whole platform onto one node, so a 2 GB Trino heap starves the Spark, Kafka, and HDFS pods |
| `trino-config.properties` | Adds four query-memory ceilings for the same reason; without them Trino sizes pools off total container memory and is OOMKilled under Iceberg scans |

`tests/test_validate_k8s_manifests.py` guards all of this: no generator path may point back into a
forked config tree, every referenced file must exist, `overrides/` must contain exactly these three
files, and each must document its rationale. The drift cannot silently return.

The kind cluster does not reserve application ports up front. As services are added, use `kubectl port-forward` for the specific UI or API you want to inspect.

## Runtime Status

The manifest set renders and is structurally validated with:

```bash
python3 scripts/validate_k8s_manifests.py
```

The manifest set renders and structurally validates clean (60 objects), and the Kubernetes path has
been verified end to end on a local `kind` cluster running the *current* architecture — gold as
`INSERT OVERWRITE` from silver, the full ~50k-row seed, and ConfigMaps generated from the repo's
real files:

- all core pods Ready in the `data-pipeline` namespace
- HDFS warehouse/checkpoint initialization
- source Postgres seed validation (row counts reconcile end to end)
- Debezium connector registration with connector and task `RUNNING`
- Airflow scheduler parsing the `payments_pipeline` DAG with no import errors (each DAG file is mounted via `subPath` — `payments_pipeline.py` and the `alerts.py` module it imports — so the walker never follows the `..data` symlink into a recursive loop)
- Bronze, Silver, and Gold Spark Jobs completing successfully
- Trino queries over Iceberg reconciling exactly: bronze = silver = `sum(payment_count)` in gold = **50,004**, matching the source Postgres row count, across 8,764 hourly buckets
- the gold serving API answering against that data (`/v1/metrics/summary` returning the same 50,004) with its snapshot-keyed cache registering hits
- Grafana coming up with all three provisioned datasources — including the Trino *plugin* datasource — and both dashboards

## Resource Declarations

Every container declares a memory request and limit. This is not boilerplate: before it was added,
Trino was OOM-killed 15 times, `spark-gold` was OOM-killed on every attempt, and Hive Metastore
crash-looped — while the node reported 290Mi requested and `MemoryPressure=False`. With no request
the scheduler treats a pod as free and packs the node; with no limit a JVM sizes its heap off total
node memory rather than its own cgroup, so the kernel picks a victim that may not even be the
offender.

`tests/test_validate_k8s_manifests.py` enforces two things: that no container is missing a request
or limit, and that the always-on set plus the largest single batch job stays inside a ~6.5 GiB
budget, so a newly added service cannot quietly make `scripts/k8s_up.sh` unschedulable on a laptop.

## Prerequisites

- Docker Desktop
- kind
- kubectl

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

The script also builds and loads the local Airflow, Hive Metastore, and Trino exporter images into the kind cluster.

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
statefulset.apps/metastore-db    1/1
statefulset.apps/namenode        1/1
statefulset.apps/datanode        1/1
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

## Next Runtime Hardening Steps

The manifests exist for the full local stack and the manual runtime smoke path succeeds. Harden the operating workflow in this order:

1. Add dependency-aware readiness waits to `scripts/k8s_verify.sh`.
2. Convert the suspended Debezium and Spark Jobs into explicit run commands or verification steps.
3. Replace Docker-oriented Airflow helper scripts with Kubernetes-aware equivalents.
4. Add service-specific port-forward examples for Airflow, Trino, the gold API, Prometheus, and Grafana.
5. Move Spark jobs from local-mode Job templates toward Kubernetes-managed Spark driver/executor pods.

The goal is for Kubernetes to mirror the existing Compose architecture first. After that, Spark jobs can move from local-mode Job templates to Kubernetes-managed Spark driver/executor pods.

## Documentation Rules For New Manifests

Keep this page accurate as the Kubernetes path grows:

- List workloads under current scope only after the manifest exists and `bash scripts/k8s_up.sh` creates it.
- Keep port-forward commands service-specific, because the kind cluster does not reserve fixed application ports.
- Prefer dependency-focused verification steps over broad claims. For example, check that Hive Metastore can reach its database, Trino can query Iceberg metadata, Kafka Connect has the Debezium connector, and Airflow can trigger the same bronze/silver/gold sequence used locally.
- Update [design.md](design.md) only when runtime behavior or limitations change, not for manifest inventory churn.
