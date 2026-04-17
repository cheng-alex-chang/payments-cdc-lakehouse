#!/usr/bin/env bash
set -euo pipefail

python3 - <<'PY'
import json
import urllib.error
import urllib.request

with open("config/connect/postgres-cdc.json", "r", encoding="utf-8") as handle:
    payload = json.load(handle)

name = payload["name"]
config = payload["config"]
base_url = "http://localhost:8083/connectors"
headers = {"Content-Type": "application/json"}

try:
    request = urllib.request.Request(
        f"{base_url}/{name}/config",
        data=json.dumps(config).encode("utf-8"),
        headers=headers,
        method="PUT",
    )
    with urllib.request.urlopen(request) as response:
        print(response.read().decode("utf-8"))
except urllib.error.HTTPError as exc:
    if exc.code != 404:
        raise

    request = urllib.request.Request(
        base_url,
        data=json.dumps(payload).encode("utf-8"),
        headers=headers,
        method="POST",
    )
    with urllib.request.urlopen(request) as response:
        print(response.read().decode("utf-8"))
PY
