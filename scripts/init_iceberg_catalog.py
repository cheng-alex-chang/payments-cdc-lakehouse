"""Bootstrap the Iceberg REST catalog and register the warehouse.

The catalog equivalent of init_object_store.py, and idempotent for the same reason: it runs as an
Airflow task and as a Kubernetes Job, and both may retry.

Two steps, because Lakekeeper separates them:

* **Bootstrap** initialises the server itself. It is a one-time action; a second call returns a
  4xx, which is treated as success rather than an error.
* **Warehouse registration** tells the catalog where tables live -- the S3 bucket, endpoint, and
  the credentials it will vend to engines. This is the piece Hive Metastore had no concept of:
  engines ask the catalog for storage access instead of each holding permanent keys.

Speaks HTTP to `iceberg-rest:8181`, which resolves as a Compose service name and a Kubernetes
Service DNS name alike, so one task covers both runtimes.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULTS = {
    "ICEBERG_REST_URL": "http://iceberg-rest:8181",
    "ICEBERG_WAREHOUSE": "payments",
    "S3_BUCKET": "warehouse",
    "S3_ENDPOINT": "http://minio:9000",
    "S3_REGION": "us-east-1",
    "S3_ACCESS_KEY": "",
    "S3_SECRET_KEY": "",
}

REQUEST_TIMEOUT = 30


def settings(env: dict[str, str] | None = None) -> dict[str, str]:
    """Resolve configuration, applying defaults. Pure and env-injectable for testing."""
    source = os.environ if env is None else env
    resolved = {key: source.get(key, default) for key, default in DEFAULTS.items()}

    for required in ("S3_ACCESS_KEY", "S3_SECRET_KEY"):
        if not resolved[required]:
            raise ValueError(f"{required} must be set")

    return resolved


def warehouse_payload(resolved: dict[str, str]) -> dict[str, Any]:
    """The storage profile and credential the catalog stores for this warehouse."""
    return {
        "warehouse-name": resolved["ICEBERG_WAREHOUSE"],
        "storage-profile": {
            "type": "s3",
            "bucket": resolved["S3_BUCKET"],
            "key-prefix": "iceberg",
            "endpoint": resolved["S3_ENDPOINT"],
            "region": resolved["S3_REGION"],
            # MinIO serves buckets as a path, not a DNS subdomain, so virtual-host addressing
            # (bucket.host) does not resolve. Real S3 accepts path style too.
            "path-style-access": True,
            "flavor": "s3-compat",
            "sts-enabled": False,
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": resolved["S3_ACCESS_KEY"],
            "aws-secret-access-key": resolved["S3_SECRET_KEY"],
        },
    }


def bootstrap(base_url: str, session: Any | None = None) -> bool:
    """Initialise the server. Returns True if this call did it, False if it was already done."""
    http = session if session is not None else requests
    info = http.get(f"{base_url}/management/v1/info", timeout=REQUEST_TIMEOUT)
    info.raise_for_status()
    if info.json().get("bootstrapped"):
        return False

    response = http.post(
        f"{base_url}/management/v1/bootstrap",
        json={"accept-terms-of-use": True},
        timeout=REQUEST_TIMEOUT,
    )
    # A race between two replicas, or a retry after a partial failure, lands here.
    if response.status_code in (400, 409):
        return False
    response.raise_for_status()
    return True


def warehouse_exists(base_url: str, name: str, session: Any | None = None) -> bool:
    http = session if session is not None else requests
    response = http.get(f"{base_url}/management/v1/warehouse", timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    warehouses = response.json().get("warehouses", [])
    return any(w.get("name") == name for w in warehouses)


def create_warehouse(
    base_url: str, payload: dict[str, Any], session: Any | None = None
) -> None:
    http = session if session is not None else requests
    response = http.post(
        f"{base_url}/management/v1/warehouse", json=payload, timeout=REQUEST_TIMEOUT
    )
    if response.status_code == 409:
        return
    if not response.ok:
        # The body carries why the storage profile was rejected, which the status code does not.
        raise RuntimeError(
            f"warehouse creation failed ({response.status_code}): {response.text[:400]}"
        )


def main() -> None:
    resolved = settings()
    base_url = resolved["ICEBERG_REST_URL"].rstrip("/")
    name = resolved["ICEBERG_WAREHOUSE"]

    LOGGER.info("  %s server bootstrap", "performed" if bootstrap(base_url) else "already done:")

    if warehouse_exists(base_url, name):
        LOGGER.info("  exists   warehouse %s", name)
    else:
        create_warehouse(base_url, warehouse_payload(resolved))
        LOGGER.info("  created  warehouse %s", name)

    LOGGER.info("Iceberg REST catalog ready")


if __name__ == "__main__":  # pragma: no cover
    main()
