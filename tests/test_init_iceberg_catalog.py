"""Tests for the Iceberg REST catalog bootstrap."""
from __future__ import annotations

import pytest

from scripts import init_iceberg_catalog as module


class _Response:
    def __init__(self, status: int = 200, payload: dict | None = None, text: str = "") -> None:
        self.status_code = status
        self.ok = 200 <= status < 300
        self._payload = payload or {}
        self.text = text

    def json(self) -> dict:
        return self._payload

    def raise_for_status(self) -> None:
        if not self.ok:
            raise RuntimeError(f"HTTP {self.status_code}")


class _Session:
    def __init__(self, get_map: dict, post_map: dict) -> None:
        self.get_map, self.post_map = get_map, post_map
        self.posted: list[str] = []

    def get(self, url: str, **_: object) -> _Response:
        return self.get_map[url.rsplit("/v1/", 1)[-1]]

    def post(self, url: str, **_: object) -> _Response:
        key = url.rsplit("/v1/", 1)[-1]
        self.posted.append(key)
        return self.post_map[key]


BASE = "http://iceberg-rest:8181"


def test_settings_require_storage_credentials() -> None:
    with pytest.raises(ValueError, match="S3_ACCESS_KEY"):
        module.settings({"S3_SECRET_KEY": "s"})


def test_bootstrap_skips_when_already_done() -> None:
    session = _Session({"info": _Response(payload={"bootstrapped": True})}, {})

    assert module.bootstrap(BASE, session) is False
    assert session.posted == []


def test_bootstrap_runs_when_needed() -> None:
    session = _Session(
        {"info": _Response(payload={"bootstrapped": False})}, {"bootstrap": _Response(204)}
    )

    assert module.bootstrap(BASE, session) is True
    assert session.posted == ["bootstrap"]


@pytest.mark.parametrize("status", [400, 409])
def test_bootstrap_treats_already_bootstrapped_as_success(status: int) -> None:
    """Two replicas racing, or a retry after a partial failure, must not fail the task."""
    session = _Session(
        {"info": _Response(payload={"bootstrapped": False})}, {"bootstrap": _Response(status)}
    )

    assert module.bootstrap(BASE, session) is False


def test_warehouse_exists_matches_by_name() -> None:
    session = _Session(
        {"warehouse": _Response(payload={"warehouses": [{"name": "payments"}]})}, {}
    )

    assert module.warehouse_exists(BASE, "payments", session) is True
    assert module.warehouse_exists(BASE, "other", session) is False


def test_create_warehouse_tolerates_conflict() -> None:
    session = _Session({}, {"warehouse": _Response(409)})

    module.create_warehouse(BASE, {}, session)  # must not raise


def test_create_warehouse_surfaces_the_response_body() -> None:
    # The status code alone never says *why* a storage profile was rejected.
    session = _Session({}, {"warehouse": _Response(400, text="bad endpoint")})

    with pytest.raises(RuntimeError, match="bad endpoint"):
        module.create_warehouse(BASE, {}, session)


def test_warehouse_payload_uses_path_style_addressing() -> None:
    """MinIO serves buckets as a path; virtual-host addressing does not resolve against it."""
    payload = module.warehouse_payload(
        module.settings({"S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s"})
    )

    assert payload["storage-profile"]["path-style-access"] is True
    assert payload["storage-profile"]["endpoint"] == "http://minio:9000"
    assert payload["storage-credential"]["aws-access-key-id"] == "k"
