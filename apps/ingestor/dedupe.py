from __future__ import annotations

import time
from collections import OrderedDict


class IdempotencyFilter:
    def __init__(self, ttl_seconds: int = 1200, max_size: int = 200000) -> None:
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._cache: OrderedDict[str, float] = OrderedDict()

    def seen_recently(self, key: str) -> bool:
        now = time.time()
        self._evict(now)
        if key in self._cache:
            self._cache.move_to_end(key)
            return True
        self._cache[key] = now
        self._cache.move_to_end(key)
        if len(self._cache) > self.max_size:
            self._cache.popitem(last=False)
        return False

    def _evict(self, now: float) -> None:
        cutoff = now - self.ttl_seconds
        while self._cache:
            oldest_key, oldest_at = next(iter(self._cache.items()))
            if oldest_at >= cutoff:
                break
            self._cache.pop(oldest_key, None)
