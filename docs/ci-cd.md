# CI/CD

The pipeline that gates and ships this repository. It replaced six independent validation
jobs that ran in parallel, depended on nothing, and stopped at validation — no registry,
no build, no deployment, no security scanning.

## Two tracks, not one chain

This project deploys two different things, and the pipeline says so rather than pretending
otherwise:

| Track | Workloads | Artifact | Target |
|---|---|---|---|
| **Lakehouse** | Spark, API, Airflow, Kafka/Debezium, Iceberg, Trino | Docker images | Ephemeral kind |
| **Cloud analytics** | DLT pipeline, Snowflake, dbt | A repo revision | Databricks + Snowflake |

They implement related logic but run different workloads and ship different artifacts.
kind is **acceptance**, not a dev environment that promotes into Databricks — nothing is
carried out of it. Modelling the two as one `dev → prod` chain would make "promote the
same artifact" untrue, because the cloud track has no image to promote.

```
                    CI gates
             lint · tests · coverage
                       │
         ┌─────────────┴─────────────┐
         ↓                           ↓
      Build                       CDC E2E
  3 images → GHCR @digest    Postgres→Debezium→Kafka
                                →Spark→Iceberg
         └─────────────┬─────────────┘
                       ↓
               K8s acceptance (ephemeral kind)
                       ↓
                 Smoke + data quality
                       ↓
                  ci-complete
```

Build and CDC run in parallel: none of the three built images take part in the CDC chain,
so gating one on the other would cost minutes for nothing.

## Workflows

| File | Purpose |
|---|---|
| `ci.yml` | Everything on a PR: validate, test, build, CDC, acceptance |
| `release-cloud.yml` | `main` only: Databricks, Snowflake, dbt, behind approval |
| `security.yml` | CodeQL, Trivy, pip-audit, gitleaks; also weekly |
| `_python-tests.yml` | Reusable: one test bucket |
| `_docker-build.yml` | Reusable: build, publish, record digest |
| `_deploy-k8s.yml` | Reusable: kind acceptance |

GitHub Actions does not process subdirectories under `.github/workflows/`, so reusable
workflows live alongside the rest with a `_` prefix.

## Decisions worth knowing

**Tests split for attribution, not speed.** The suite is ~330 tests in about a second. Six
buckets exist so a failure names the component that broke, not to save time.

**All buckets measure the same coverage sources.** Narrowing sources per bucket was
measured at a combined 83% against 87% for the full list — the difference is real coverage
being discarded, since `test_pipeline_runtime.py` genuinely exercises `scripts/`.
Splitting the suite should attribute failures, not quietly harden the gate by four points.

**Coverage fragments are named via `COVERAGE_FILE`.** A plain `pytest --cov` run erases
pre-existing `.coverage.*` siblings on startup, so renaming afterwards only survives
because each bucket gets its own runner. Naming the file up front makes the same sequence
reproducible locally.

**Deployments reference digests, not tags.** A tag can be repointed; a digest cannot.

**Kubernetes Jobs are activated, not recreated.** Every operational Job ships
`suspend: true`, so `kubectl apply` creates them without firing them. CI unsuspends
`register-postgres-cdc`, then `seed-demo-data`, then bronze/silver/gold in order. This
exercises the orchestration contract the manifests already define instead of inventing a
CI-only execution path.

**Resources are not trimmed for CI.** Always-on workloads request 4.6Gi and the Jobs run
one at a time, so peak is ~5.6Gi on a 16GB runner — not the 8.1Gi that summing every
workload suggests.

**Smoke and data quality are different things.** `validate_trino.py` runs
`sql/trino/validation_queries.sql`, which is three unasserted `SELECT`s: it proves the
queries execute, not that the data is right. `scripts/acceptance_checks.py` asserts values
— non-empty layers, no null grouping dimensions, `auth_rate` within `[0,1]`, canonical
method/status sets, no duplicate silver keys, and no unhashed `shopper_id` in bronze.

**`databricks bundle validate` authenticates.** It is not a credential-free schema check:
the `dev` target is `mode: development`, which resolves workspace identity. So PRs run
`scripts/validate_databricks_bundle.py` (structural, no network) and only `main` runs the
authenticated command — where a missing secret **fails** rather than reporting green.

## Fork pull requests

A fork's `GITHUB_TOKEN` is read-only and has no `packages: write`.

| Stage | Fork PR |
|---|---|
| Lint, tests, coverage | runs |
| CDC integration | runs — needs no registry and no secrets |
| Image build | runs with `push: false` |
| GHCR publish, k8s acceptance | **skipped** |
| Authenticated bundle validate | **skipped** |

`ci-complete` is the single required check. It distinguishes an intentional skip from an
accidental one: the three above are allowed to skip, and any *other* skip fails the build
— that is the failure mode an aggregator usually hides.

## Branch protection

Require exactly one check: **`ci-complete`**. Requiring individual jobs would mean editing
protection rules whenever the graph changes, and would have to encode the fork skip rules
in repository settings instead of in reviewed code.

## Running things locally

```bash
pytest                                    # fast suite, ~1s, no Docker
pytest tests/test_bucket_inventory.py     # buckets still cover every test file
pytest -m integration_cdc tests/integration/test_cdc_chain.py   # needs Docker
bash scripts/k8s_up.sh && bash scripts/k8s_verify.sh
```

Reproducing the coverage gate:

```bash
COVERAGE_FILE=.coverage.api pytest tests/test_api_*.py --cov=api/src --cov-report=
# ... one run per bucket ...
coverage combine && coverage report --fail-under=80
```

## The pod model

The three `_*.yml` workflows are written to be repo-agnostic and are the intended
extraction point:

```
org/platform-ci/.github/workflows/python-tests.yml@v1
org/platform-ci/.github/workflows/docker-build.yml@v1
org/platform-ci/.github/workflows/k8s-acceptance.yml@v1
```

Same-repository reusable workflows are factoring. Moving them to a separate versioned
repository is what turns the pattern into centralized platform standards with
decentralized pod ownership: the platform team owns the templates and their release
cadence, and the payments pod owns which versions it consumes, its own test buckets,
manifests, and deployment configuration.

Until that repository exists, the split is structural rather than organizational — worth
being clear about, since the directory layout alone can look like more separation than it
actually provides.
