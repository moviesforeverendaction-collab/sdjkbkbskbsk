"""
Optional Redis cache for file metadata.

If REDIS_URL is not configured the module becomes a passthrough —
all calls fall back to MongoDB silently.  Redis is not required.
"""

import json
from typing import Optional

from bot.config import Config

_redis = None
CACHE_TTL = 3600  # 1 hour


async def _get_redis():
    global _redis
    if _redis is not None:
        return _redis
    if not Config.REDIS_URL:
        return None
    try:
        import redis.asyncio as aioredis
        _redis = await aioredis.from_url(
            Config.REDIS_URL,
            encoding="utf-8",
            decode_responses=True,
            socket_connect_timeout=3,
        )
        await _redis.ping()
    except Exception:
        _redis = None
    return _redis


async def cache_get(token: str) -> Optional[dict]:
    r = await _get_redis()
    if r is None:
        return None
    try:
        raw = await r.get(f"file:{token}")
        return json.loads(raw) if raw else None
    except Exception:
        return None


async def cache_set(token: str, data: dict) -> None:
    r = await _get_redis()
    if r is None:
        return
    try:
        await r.setex(f"file:{token}", CACHE_TTL, json.dumps(data))
    except Exception:
        pass


async def cache_delete(token: str) -> None:
    r = await _get_redis()
    if r is None:
        return
    try:
        await r.delete(f"file:{token}")
    except Exception:
        pass
