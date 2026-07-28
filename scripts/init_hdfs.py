"""Create the HDFS directories the pipeline writes into.

Uses WebHDFS over HTTP rather than shelling into a Compose container, so the same task runs under
Docker Compose and Kubernetes without a branch. `namenode:9870` resolves as a Compose service name
and as a Kubernetes Service DNS name alike -- Compose's own namenode healthcheck already curls
that exact URL.

MKDIRS is idempotent: creating a directory that already exists returns `{"boolean": true}` rather
than an error, so re-running is safe and the Airflow task can retry without special-casing.
"""
from __future__ import annotations

import logging
import os
from typing import Any

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
LOGGER = logging.getLogger(__name__)

DEFAULTS = {
    "HDFS_NAMENODE_HOST": "namenode",
    "HDFS_NAMENODE_HTTP_PORT": "9870",
    # The Hadoop images run as root and there is no Kerberos in this platform, so WebHDFS takes
    # the caller's identity from the query string.
    "HDFS_USER": "root",
}

DIRECTORIES = (
    "/data/bronze",
    "/data/silver",
    "/data/gold",
    "/warehouse",
    "/warehouse/analytics.db",
    "/checkpoints/bronze",
    "/checkpoints/silver",
)

REQUEST_TIMEOUT = 30


def settings(env: dict[str, str] | None = None) -> dict[str, str]:
    source = os.environ if env is None else env
    resolved = {key: source.get(key, default) for key, default in DEFAULTS.items()}
    port = resolved["HDFS_NAMENODE_HTTP_PORT"]
    if not str(port).isdigit():
        raise ValueError(f"HDFS_NAMENODE_HTTP_PORT must be numeric, got {port!r}")
    return resolved


def mkdirs_url(path: str, env: dict[str, str] | None = None) -> str:
    resolved = settings(env)
    return (
        f"http://{resolved['HDFS_NAMENODE_HOST']}:{resolved['HDFS_NAMENODE_HTTP_PORT']}"
        f"/webhdfs/v1{path}?op=MKDIRS&user.name={resolved['HDFS_USER']}"
    )


def raise_for_webhdfs_error(payload: dict[str, Any], path: str) -> None:
    """WebHDFS reports failures as a RemoteException in the body, so check it explicitly."""
    exception = payload.get("RemoteException")
    if isinstance(exception, dict):
        raise RuntimeError(
            f"WebHDFS MKDIRS failed for {path}: "
            f"{exception.get('exception')}: {exception.get('message')}"
        )
    if payload.get("boolean") is False:
        raise RuntimeError(f"WebHDFS MKDIRS returned false for {path}")


def make_directory(
    path: str, env: dict[str, str] | None = None, session: Any | None = None
) -> None:
    http = session if session is not None else requests
    response = http.put(mkdirs_url(path, env), timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    raise_for_webhdfs_error(response.json(), path)


def main() -> None:
    LOGGER.info("Creating %s HDFS directories via WebHDFS", len(DIRECTORIES))
    for path in DIRECTORIES:
        make_directory(path)
        LOGGER.info("  ok %s", path)
    LOGGER.info("HDFS layout ready")


if __name__ == "__main__":  # pragma: no cover
    main()
