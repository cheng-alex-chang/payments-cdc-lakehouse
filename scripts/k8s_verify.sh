#!/usr/bin/env bash
# Verify the local Kubernetes deployment is *ready*, not merely *created*.
#
# `kubectl get deployment api` exits 0 while every one of its pods sits in CrashLoopBackOff, so an
# existence check reports success on a completely broken cluster. Each workload below is checked
# with `rollout status`, which blocks until the desired replicas are actually available and fails
# when they are not.
#
# The two lists must cover every Deployment and StatefulSet under k8s/base. They are written out
# rather than derived so a failure names a workload you can go look at -- and
# tests/test_validate_k8s_manifests.py asserts they still match the manifests, so a new workload
# cannot ship unverified.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KUBECONFIG_PATH="${ROOT_DIR}/.kind/kubeconfig"
NAMESPACE=data-pipeline

# Cold starts pull images and wait on dependency probes; override for a slower machine.
TIMEOUT="${VERIFY_TIMEOUT:-300s}"

STATEFULSETS=(airflow-postgres catalog-db kafka postgres)
DEPLOYMENTS=(airflow-scheduler airflow-webserver api grafana iceberg-rest kafka-connect minio prometheus statsd-exporter trino trino-exporter)

for command in kubectl; do
  if ! command -v "${command}" >/dev/null 2>&1; then
    echo "Missing required command: ${command}" >&2
    exit 1
  fi
done

KUBECTL=(kubectl --kubeconfig "${KUBECONFIG_PATH}")

"${KUBECTL[@]}" get namespace "${NAMESPACE}" >/dev/null
"${KUBECTL[@]}" get configmap trino-catalog -n "${NAMESPACE}" >/dev/null
"${KUBECTL[@]}" get secret platform-secrets -n "${NAMESPACE}" >/dev/null

failed=()

# `rollout status` exits non-zero on timeout, which `set -e` would turn into an early exit after
# the first bad workload. Collect failures instead so one run reports the whole picture.
check() {
  local kind="$1" name="$2"
  if "${KUBECTL[@]}" rollout status "${kind}/${name}" -n "${NAMESPACE}" \
      --timeout="${TIMEOUT}" >/dev/null 2>&1; then
    printf '  ready      %s/%s\n' "${kind}" "${name}"
  else
    printf '  NOT READY  %s/%s\n' "${kind}" "${name}"
    failed+=("${kind}/${name}")
  fi
}

echo "Checking ${#STATEFULSETS[@]} StatefulSets and ${#DEPLOYMENTS[@]} Deployments (timeout ${TIMEOUT} each):"
for name in "${STATEFULSETS[@]}"; do
  check statefulset "${name}"
done
for name in "${DEPLOYMENTS[@]}"; do
  check deployment "${name}"
done

if ((${#failed[@]} > 0)); then
  echo
  echo "${#failed[@]} workload(s) not ready: ${failed[*]}" >&2
  echo "Inspect with: kubectl get pods -n ${NAMESPACE}" >&2
  exit 1
fi

echo
echo "All ${#STATEFULSETS[@]} StatefulSets and ${#DEPLOYMENTS[@]} Deployments are ready."
