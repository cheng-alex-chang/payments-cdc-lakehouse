"""Guards for the snapshot-scoped response cache (api/src/cache.py).

The cache's whole claim is that invalidation is *exact*: entries are keyed to an Iceberg snapshot
id, so a rebuilt gold table can never be served from cache. These tests pin that claim, because a
cache that occasionally serves stale financial aggregates is worse than no cache at all.
"""
from __future__ import annotations

from api.src.cache import SnapshotCache


def test_first_lookup_is_a_miss() -> None:
    cache = SnapshotCache()

    assert cache.get("snap-1", "key") is None
    assert cache.misses == 1
    assert cache.hits == 0


def test_value_is_returned_within_the_same_snapshot() -> None:
    cache = SnapshotCache()
    cache.put("snap-1", "key", ["row"])

    assert cache.get("snap-1", "key") == ["row"]
    assert cache.hits == 1


def test_new_snapshot_invalidates_every_entry() -> None:
    # The gold job is a full INSERT OVERWRITE, so one new snapshot replaces the whole table --
    # ageing entries out individually would leave stale rows readable.
    cache = SnapshotCache()
    cache.put("snap-1", "a", ["old-a"])
    cache.put("snap-1", "b", ["old-b"])

    assert cache.get("snap-2", "a") is None
    assert cache.get("snap-2", "b") is None
    assert len(cache) == 0


def test_reverting_to_a_previous_snapshot_does_not_resurrect_entries() -> None:
    cache = SnapshotCache()
    cache.put("snap-1", "key", ["v1"])
    cache.get("snap-2", "key")  # rolls the cache over to snap-2

    assert cache.get("snap-1", "key") is None


def test_unknown_snapshot_id_is_treated_as_its_own_scope() -> None:
    # current_snapshot() returns None for a table with no snapshots yet; that must not collide
    # with a real snapshot's entries.
    cache = SnapshotCache()
    cache.put(None, "key", ["from-empty-table"])

    assert cache.get(None, "key") == ["from-empty-table"]
    assert cache.get("snap-1", "key") is None


def test_least_recently_used_entry_is_evicted_first() -> None:
    cache = SnapshotCache(max_entries=2)
    cache.put("snap-1", "a", 1)
    cache.put("snap-1", "b", 2)
    cache.get("snap-1", "a")  # 'a' becomes most recently used
    cache.put("snap-1", "c", 3)

    assert cache.get("snap-1", "b") is None
    assert cache.get("snap-1", "a") == 1
    assert cache.get("snap-1", "c") == 3
    assert cache.evictions == 1


def test_cache_never_exceeds_its_bound() -> None:
    cache = SnapshotCache(max_entries=8)
    for index in range(50):
        cache.put("snap-1", f"key-{index}", index)

    assert len(cache) == 8


def test_hit_ratio_is_zero_before_any_lookup() -> None:
    assert SnapshotCache().hit_ratio() == 0.0


def test_hit_ratio_reflects_lookups() -> None:
    cache = SnapshotCache()
    cache.put("snap-1", "key", 1)
    cache.get("snap-1", "key")
    cache.get("snap-1", "missing")

    assert cache.hit_ratio() == 0.5


def test_clear_drops_entries_and_snapshot_scope() -> None:
    cache = SnapshotCache()
    cache.put("snap-1", "key", 1)

    cache.clear()

    assert len(cache) == 0
    assert cache.snapshot_id is None
