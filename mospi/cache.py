"""
In-memory metadata cache for MoSPI API responses.
TTL-based caching to avoid hitting the government API on every describe_dataset call.
"""

import time
from typing import Any, Optional


class MetadataCache:
    """Simple in-memory cache with TTL expiry."""

    def __init__(self, ttl_seconds: int = 3600):
        self._cache: dict[str, Any] = {}
        self._timestamps: dict[str, float] = {}
        self._ttl = ttl_seconds

    def get(self, key: str) -> Optional[Any]:
        if key not in self._cache:
            return None
        if self._is_expired(key):
            del self._cache[key]
            del self._timestamps[key]
            return None
        return self._cache[key]

    def set(self, key: str, value: Any) -> None:
        self._cache[key] = value
        self._timestamps[key] = time.time()

    def _is_expired(self, key: str) -> bool:
        return time.time() - self._timestamps.get(key, 0) > self._ttl

    def clear(self) -> None:
        self._cache.clear()
        self._timestamps.clear()


# Global cache instance
metadata_cache = MetadataCache()
