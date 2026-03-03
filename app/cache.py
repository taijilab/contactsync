import os
from typing import Optional

REDIS_URL = os.getenv("REDIS_URL", "")

_redis_client = None


def _get_redis():
    """Lazy-init Redis client. Returns None if Redis is unavailable."""
    global _redis_client
    if _redis_client is not None:
        return _redis_client
    if not REDIS_URL:
        return None
    try:
        import redis

        _redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        _redis_client.ping()
        return _redis_client
    except Exception:
        _redis_client = None
        return None


def cache_get(key: str) -> Optional[str]:
    r = _get_redis()
    if r is None:
        return None
    try:
        return r.get(key)
    except Exception:
        return None


def cache_set(key: str, value: str, ttl_seconds: int = 60) -> None:
    r = _get_redis()
    if r is None:
        return
    try:
        r.setex(key, ttl_seconds, value)
    except Exception:
        pass


def cache_delete(key: str) -> None:
    r = _get_redis()
    if r is None:
        return
    try:
        r.delete(key)
    except Exception:
        pass


def cache_delete_pattern(pattern: str) -> None:
    r = _get_redis()
    if r is None:
        return
    try:
        keys = r.keys(pattern)
        if keys:
            r.delete(*keys)
    except Exception:
        pass
