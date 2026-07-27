#!/usr/bin/env bash
# Latency benchmark for the gold serving API. Thin wrapper around scripts/bench_api.py so the
# percentile maths stays in Python where it is unit-tested (tests/test_bench_api.py).
#
#   bash scripts/bench_api.sh                              # defaults: localhost:8000, 100 requests
#   BASE_URL=http://api:8000 REQUESTS=500 bash scripts/bench_api.sh
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BASE_URL="${BASE_URL:-http://localhost:8000}"
REQUESTS="${REQUESTS:-100}"

PYTHON="${PYTHON:-python3}"
if [ -x "${ROOT_DIR}/.venv/bin/python" ]; then
  PYTHON="${ROOT_DIR}/.venv/bin/python"
fi

# Do NOT point this at a `kubectl port-forward` tunnel. port-forward is a single userspace TCP
# relay and collapses under sustained load: a run through one reported 11 errors in 60 requests
# and a 6-second p99, while the same API answered every sequential call in 56-314 ms. Benchmark
# against Compose (port published directly) or from inside the cluster against the Service.
# See docs/api-benchmark.md.

# Fail fast with a useful message rather than a wall of connection errors.
if ! curl -fsS "${BASE_URL}/v1/health" >/dev/null 2>&1; then
  echo "API is not reachable at ${BASE_URL} -- start it with 'docker compose up -d api'." >&2
  exit 1
fi

exec "${PYTHON}" "${ROOT_DIR}/scripts/bench_api.py" \
  --base-url "${BASE_URL}" --requests "${REQUESTS}" "$@"
