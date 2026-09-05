# CI/CD

The pipeline that gates and ships this repository. It replaced six independent validation
jobs that ran in parallel, depended on nothing, and stopped at validation — no registry,
no build, no deployment, no security scanning.

## Two tracks, not one chain

This project deploys two different things, and the pipeline says so rather than pretending
otherwise:

| Track | Workloads | Artifact | Target |
|---|---|---|---|
| **Lakehouse** | Spark, API, Airflow, Kafka/Debezium, Iceberg, Trino | Docker images (4) | Ephemeral kind |
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
  4 images → GHCR @digest    Postgres→Debezium→Kafka
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
| `release-cloud.yml` | Databricks, Snowflake, dbt. Dispatch, or a push to `main` touching a cloud asset (see below) |

### Test tiers

Three, separated by what they cost to run — each excluded from the one below it so a
heavy dependency never becomes a silent skip in the fast suite:

| Tier | Selected by | Needs | Where |
|---|---|---|---|
| fast | default | nothing | the `test` bucket matrix |
| spark | `-m spark tests/spark` | JDK 17 + `requirements-spark.txt` | `spark-semantics` job |
| cdc | `-m integration_cdc` | nine containers | `cdc-integration` job |

The `spark` tier exists because the silver MERGE's correctness is behaviour, not text.
Delete-then-recreate ordering, stale-replay convergence and tombstone handling all
produced valid SQL and a green fast suite while being wrong. Those tests are
mutation-checked — remove the LSN guard and the two stale-state cases fail; remove the
`THEN DELETE` branch and the delete case fails — so a green run means the guard is doing
work rather than that the assertions are vacuous.
| `security.yml` | CodeQL, Trivy, pip-audit, gitleaks; also weekly |

The three reusable workflows now live in
[`cheng-alex-chang/platform-ci`](https://github.com/cheng-alex-chang/platform-ci), pinned
at `@v1`: `python-tests.yml`, `docker-build.yml`, `k8s-acceptance.yml`.

## Decisions worth knowing

**Tests split for attribution, not speed.** The suite is ~357 tests in about a second. Six
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

**Spark's dependencies are baked into an image — for supply-chain reasons, not speed.**
Measured, baking took the medallion step from 2.6 to 2.3 minutes and added ~0.3 minutes to
a cached build: roughly a wash. What it does buy is that acceptance no longer re-resolves
four Maven coordinates and their transitive tree from the public internet on every run, so
a slow or unavailable Maven Central can no longer fail the deploy gate — and the Spark
runtime stops being the one component acceptance deployed from a mutable upstream tag while
the other three were promoted by digest.

Mounting a hostPath at `spark.jars.ivy` is the obvious alternative and does not work:
kubelet creates a `DirectoryOrCreate` hostPath as `root:root 0755` while the Job runs as
the image's UID 185, so Ivy dies with `FileNotFoundException` and the Job crash-loops.
Compose only gets away with it because its `spark` service sets `user: "0:0"`.

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

## When the cloud release runs

`workflow_dispatch`, plus a push to `main` that touches a cloud asset —
`infra/terraform/**`, `databricks/**`, `snowflake_etl/dbt/**`, `snowflake_etl/src/**`, or
the workflow itself. There is no approval gate; the `prod` environment no longer requires a
reviewer.

Both original blockers are resolved:

- ~~**No live Snowflake account.**~~ There is one, and the full chain is verified against it:
  chunked staging to S3, `COPY INTO` through the storage integration, run registration and
  reconciliation, `dbt build`, and a source delete propagating to the marts.
- ~~**No separate Databricks workspace.**~~ Free Edition still provides one, but the
  workspace was never the collision — the *target schema* was. The bundle takes a
  `target_schema` variable (`analytics_dev` / `analytics`) and both targets now deploy side
  by side. One permissions boundary and one pool of serverless compute, so this is a working
  single-workspace arrangement, not isolation.

### Why the trigger is path-filtered rather than every push

This release is not a cheap no-op. It is a Terraform apply against two clouds plus a full
`dbt build` into `ANALYTICS`, and every apply is another chance to meet state drift. Firing
on every push would deploy production on a docs typo, would deploy on each Dependabot merge,
and — when the Snowflake trial lapses — would turn every merge to `main` red while deploying
nothing. That last one is precisely the failure this workflow was made manual to escape.
Scoping to the paths that change what is deployed keeps the drift protection without any of
it.

This is the opposite conclusion from *Why the expensive jobs are not path-filtered* below,
and deliberately so — the two cases differ in both respects that mattered there. Those are
**required checks on a pull request**, where a filtered-out job never reports at all and the
PR waits forever on a check that will not arrive; this is a **push trigger**, where not
firing simply means no deploy. And those jobs are read-only verification, cheap to over-run;
this one applies infrastructure and rebuilds a warehouse schema, where over-running has a
cost and a blast radius.

### Why running unattended is defensible now, and was not before

The Terraform inputs fail closed. They did not always. An earlier approved run planned this
against production:

```
Plan: 0 to add, 3 to change, 1 to destroy
  snowflake_grant_account_role.etl_to_users["..."] will be destroyed
  url = "s3://payments-lake-alexchang-2026/" -> "s3://payments-lake-changeme/"
```

The workflow supplied neither `TF_VAR_s3_bucket` nor `TF_VAR_etl_role_users`, so both fell
back to defaults: repoint the external stage at a bucket that does not exist, and destroy
the grant the pipeline authenticates with. It failed only because the OIDC role was missing
an unrelated S3 permission — luck, not a safeguard. Neither variable has a default now, and
`tests/test_terraform_inputs.py` fails if one comes back. **If that regresses, restore the
required reviewer on the `prod` environment before touching this trigger.**

### What the manual gate was worth

Three of the four dispatched runs failed, each on a distinct bug that no local testing had
surfaced: `mode: production` requiring an explicit `root_path`, the Databricks stack's
remote state holding no resources while the workspace held both, and the Terraform inputs
above. Worth remembering the next time a gate looks like ceremony.

## Two invariants worth knowing about

**Iceberg vectorization is one value everywhere.** The vectorized Parquet reader aborts the
JVM on arm64 whenever a scan feeds a shuffle (`free(): invalid pointer`, SIGABRT) — and every
medallion stage groups over an Iceberg table, so this is not a corner case. It is disabled in
`config/spark/jobs/common.py` and deliberately *not* set per runtime;
`tests/test_compose_manifest.py` fails if Compose or the Kubernetes manifests set it, because
a per-runtime split means CI stops exercising the production read path. Re-enable it with
`ICEBERG_VECTORIZATION=true` once an image ships a fixed Arrow/JVM combination — everywhere at
once, with the acceptance suite as the gate.

**Bootstrap stacks are hand-applied but not hand-remembered.** `infra/terraform/github-oidc`
creates the OIDC provider and the role `release-cloud.yml` assumes, which CI cannot create for
itself. Its state lives in the shared S3 bucket under `github-oidc/` like every other stack —
not locally. A human applying it authenticates as themselves and already has bucket access, so
there is no circularity, and losing a laptop does not make Terraform forget the role exists.

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

## Why the expensive jobs are not path-filtered

Every pull request, dependency bumps included, runs the CDC chain and a kind acceptance
cluster. Skipping those for `requirements-*.txt`-only changes looks like free savings. It
is not, for two reasons:

- **It would skip the thing that changed.** `config/api/Dockerfile` installs
  `requirements-ci.txt` and `requirements-api.txt`, so a pydantic or uvicorn bump lands in
  the deployed `payments-api` image — and acceptance is what runs `bench_api.py` and the
  smoke checks against it.
- **A path-filtered required check never reports.** It does not pass or fail; it simply
  never arrives, and the pull request waits forever on a check that will not come.

The pile-up that prompted the question was nine simultaneous Dependabot PRs, not the
per-PR cost. That is a Dependabot configuration problem, and it is fixed there: updates
are grouped into at most four PRs a week, split by concern so a failure is still
bisectable. If this ever becomes a team repository, the next step is a merge queue —
batch the PRs and run the expensive suite once per batch — not a weaker gate.

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

The mechanics of the three expensive stages live in
[`cheng-alex-chang/platform-ci`](https://github.com/cheng-alex-chang/platform-ci) and are
consumed at `@v1`:

```
cheng-alex-chang/platform-ci/.github/workflows/python-tests.yml@v1
cheng-alex-chang/platform-ci/.github/workflows/docker-build.yml@v1
cheng-alex-chang/platform-ci/.github/workflows/k8s-acceptance.yml@v1
```

`actions/checkout` inside those workflows resolves against the **calling** repository at the
calling commit, so `requirements-ci.txt`, `k8s/overlays/ci` and `scripts/k8s_verify.sh` stay
this repo's business. That is the split: the platform owns how a stage runs, the pod owns
what it runs against, its own buckets, manifests and deployment configuration, and which
version it consumes.

Same-repository reusable workflows were only factoring. Pinning a versioned external
reference is what makes the ownership real — the pod upgrades when it chooses rather than
inheriting whatever the platform merged.

Worth stating plainly: there is one consumer today, so this is largely a demonstration of
the model. The pattern earns its keep with several pods sharing the mechanics; with one it
costs a second repository to keep in step.
