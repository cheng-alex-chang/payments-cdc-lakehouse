#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-data-pipeline}"
KUBECONFIG_PATH="${ROOT_DIR}/.kind/kubeconfig"

for command in docker kind kubectl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

cd "${ROOT_DIR}"

# The overlay reads passwords from a gitignored secrets.env (see secrets.env.example).
# Seed it from the example on first run so the overlay can render.
SECRETS_ENV="${ROOT_DIR}/k8s/overlays/local/secrets.env"
if [ ! -f "${SECRETS_ENV}" ]; then
  cp "${SECRETS_ENV}.example" "${SECRETS_ENV}"
  echo "Created k8s/overlays/local/secrets.env from the example (placeholder passwords)." >&2
  echo "Edit it to change local cluster credentials; the file stays untracked." >&2
fi

mkdir -p .kind
kind get clusters | grep -qx "${CLUSTER_NAME}" \
  || kind create cluster --name "${CLUSTER_NAME}" \
       --config "${ROOT_DIR}/k8s/kind-config.yaml" \
       --kubeconfig "${KUBECONFIG_PATH}" --wait 120s
docker build -t local/data-pipeline-airflow:dev -f config/airflow/Dockerfile .
docker build -t local/trino-exporter:dev config/trino-exporter
docker build -t local/payments-api:dev -f config/api/Dockerfile .
docker build -t local/payments-spark:dev -f config/spark/Dockerfile .
kind load docker-image local/data-pipeline-airflow:dev --name data-pipeline
kind load docker-image local/trino-exporter:dev --name data-pipeline
kind load docker-image local/payments-api:dev --name data-pipeline
kind load docker-image local/payments-spark:dev --name data-pipeline

# Jobs have immutable pod templates, so `kubectl apply` over an existing Job
# fails ("field is immutable"). Delete any existing Jobs first so re-running this
# script recreates them cleanly. Harmless on a fresh cluster (nothing to delete).
KUBECONFIG="${KUBECONFIG_PATH}" kubectl delete jobs --all -n data-pipeline \
  --ignore-not-found --wait=false 2>/dev/null || true
# The base kustomization generates its ConfigMaps from the repo's real config files (the same
# ones Compose bind-mounts) rather than from a copy, so the two runtimes cannot drift. Those
# paths sit outside the kustomization root, which Kustomize refuses by default, so render with
# --load-restrictor=LoadRestrictionsNone and pipe into apply. `kubectl apply -k` has no such
# flag. The documented tradeoff is that the kustomization is no longer relocatable, which costs
# nothing here: it is repo-local and never vendored into another tree.
KUBECONFIG="${KUBECONFIG_PATH}" kubectl kustomize \
  --load-restrictor=LoadRestrictionsNone k8s/overlays/local \
  | KUBECONFIG="${KUBECONFIG_PATH}" kubectl apply -f -

echo
echo "Local Kubernetes foundation is ready."
echo "Use: export KUBECONFIG=${KUBECONFIG_PATH}"
echo "Then: kubectl get all -n data-pipeline"
