"""Response cache keyed on the gold table's Iceberg snapshot id.

A TTL cache guesses: too short and it does nothing, too long and it serves data that has already
been replaced. Iceberg removes the guess. `payment_metrics_gold` is rebuilt by an `INSERT OVERWRITE`
that commits a new snapshot every run, so keying entries on that snapshot id makes invalidation
exact -- an entry can never outlive the data it was built from, and it never expires early while
the data is unchanged.

The gold job is a full atomic replace, so a snapshot change invalidates *everything*: the cache
drops all entries at once rather than ageing them out individually.
"""
from __future__ import annotations

import threading
from collections import OrderedDict
from typing import Any


class SnapshotCache:
    """Bounded LRU whose entire contents are scoped to one Iceberg snapshot id."""

    def __init__(self, max_entries: int = 256) -> None:
        self._entries: OrderedDict[str, Any] = OrderedDict()
        self._snapshot_id: str | None = None
        self._max_entries = max_entries
        # FastAPI runs sync endpoints in a threadpool, so more than one request can touch the
        # cache at once. The lock keeps the snapshot swap and the LRU bookkeeping atomic.
        self._lock = threading.Lock()
        self.hits = 0
        self.misses = 0
        self.evictions = 0

    @property
    def snapshot_id(self) -> str | None:
        return self._snapshot_id

    def __len__(self) -> int:
        return len(self._entries)

    def _reset_locked(self, snapshot_id: str | None) -> None:
        self._entries.clear()
        self._snapshot_id = snapshot_id

    def get(self, snapshot_id: str | None, key: str) -> Any | None:
        """Return the cached value, or None on a miss.

        A snapshot id different from the cached one drops every entry before looking up, so a
        stale value can never be returned.
        """
        with self._lock:
            if snapshot_id != self._snapshot_id:
                self._reset_locked(snapshot_id)
                self.misses += 1
                return None

            if key not in self._entries:
                self.misses += 1
                return None

            self._entries.move_to_end(key)
            self.hits += 1
            return self._entries[key]

    def put(self, snapshot_id: str | None, key: str, value: Any) -> None:
        with self._lock:
            if snapshot_id != self._snapshot_id:
                self._reset_locked(snapshot_id)

            self._entries[key] = value
            self._entries.move_to_end(key)

            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
                self.evictions += 1

    def clear(self) -> None:
        with self._lock:
            self._reset_locked(None)

    def hit_ratio(self) -> float:
        total = self.hits + self.misses
        return self.hits / total if total else 0.0
