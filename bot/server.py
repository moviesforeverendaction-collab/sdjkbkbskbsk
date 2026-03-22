"""
aiohttp web server.

Routes:
  GET /dl/{token}   — stream file (God Speed from disk or dual-client Telegram)
  GET /info/{token} — JSON metadata
  GET /health       — Railway health check
"""

import asyncio
import logging
import time
from collections import defaultdict
from pathlib import Path

from aiohttp import web

from bot import database, cache
from bot.client_pool import ClientPool
from bot.config import Config
from bot.stream import (
    iter_file, iter_cached_file, cache_path,
    parse_range_header,
)

log = logging.getLogger(__name__)

_rate_counters: dict[str, list[float]] = defaultdict(list)


def _is_rate_limited(ip: str) -> bool:
    now = time.monotonic()
    _rate_counters[ip] = [t for t in _rate_counters[ip] if now - t < 60.0]
    if len(_rate_counters[ip]) >= Config.RATE_LIMIT_PER_MINUTE:
        return True
    _rate_counters[ip].append(now)
    return False


async def handle_download(request: web.Request) -> web.StreamResponse:
    token = request.match_info["token"]
    ip = (
        request.headers.get("X-Forwarded-For", request.remote or "unknown")
        .split(",")[0].strip()
    )

    if _is_rate_limited(ip):
        raise web.HTTPTooManyRequests(text="Too many requests.")

    # Metadata lookup
    file_meta = await cache.cache_get(token)
    if file_meta is not None:
        if file_meta.get("expires_at") and int(time.time()) > file_meta["expires_at"]:
            await cache.cache_delete(token)
            file_meta = None

    if file_meta is None:
        file_meta = await database.get_file(token)
        if file_meta is None:
            raise web.HTTPNotFound(text="File not found or link expired.")
        await cache.cache_set(token, file_meta)

    file_size:  int = file_meta["file_size"]
    file_name:  str = file_meta["file_name"]
    mime_type:  str = file_meta.get("mime_type", "application/octet-stream")
    message_id: int = file_meta["message_id"]
    god_speed_ready: bool = file_meta.get("god_speed_ready", False)

    known_size   = file_size > 0
    range_header = request.headers.get("Range") if known_size else None
    start, end   = parse_range_header(range_header, file_size) if known_size else (0, 0)
    content_len  = (end - start + 1) if known_size else None

    headers = {
        "Content-Type":        mime_type,
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Accept-Ranges":       "bytes" if known_size else "none",
        "Cache-Control":       "no-store",
        "X-Accel-Buffering":   "no",
    }
    if known_size:
        headers["Content-Length"] = str(content_len)
    if range_header and known_size:
        headers["Content-Range"] = f"bytes {start}-{end}/{file_size}"

    status = 206 if (range_header and known_size) else 200
    resp = web.StreamResponse(status=status, headers=headers)
    await resp.prepare(request)

    pool: ClientPool = request.app["pool"]
    bytes_sent = 0
    completed  = False

    try:
        # ── God Speed path: serve from Railway disk ──────────────────────────
        cached = cache_path(token)
        if god_speed_ready and cached.exists():
            async for chunk in iter_cached_file(cached, offset=start):
                if known_size and bytes_sent >= content_len:
                    break
                if known_size:
                    remaining = content_len - bytes_sent
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                await resp.write(chunk)
                bytes_sent += len(chunk)
            completed = True

        # ── Dual client path ─────────────────────────────────────────────────
        elif pool.count >= 2:
            c1 = pool.get()
            c2 = pool.get()
            async for chunk in iter_file(c1, message_id, Config.CHANNEL_ID,
                                          offset=start, client2=c2):
                if known_size and bytes_sent >= content_len:
                    break
                if known_size:
                    remaining = content_len - bytes_sent
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                await resp.write(chunk)
                bytes_sent += len(chunk)
            completed = True

        # ── Single client fallback ────────────────────────────────────────────
        else:
            c1 = pool.get()
            async for chunk in iter_file(c1, message_id, Config.CHANNEL_ID,
                                          offset=start):
                if known_size and bytes_sent >= content_len:
                    break
                if known_size:
                    remaining = content_len - bytes_sent
                    if len(chunk) > remaining:
                        chunk = chunk[:remaining]
                await resp.write(chunk)
                bytes_sent += len(chunk)
            completed = True

    except (ConnectionResetError, asyncio.CancelledError, ConnectionError):
        pass
    except Exception as exc:
        msg = str(exc).lower()
        if "connection lost" not in msg and "connection reset" not in msg:
            log.error("Stream error token=%s: %s", token, exc)

    if completed:
        asyncio.create_task(database.increment_downloads(token))

    return resp


async def handle_info(request: web.Request) -> web.Response:
    token = request.match_info["token"]
    file_meta = await cache.cache_get(token)
    if file_meta is not None:
        if file_meta.get("expires_at") and int(time.time()) > file_meta["expires_at"]:
            await cache.cache_delete(token)
            file_meta = None
    if file_meta is None:
        file_meta = await database.get_file(token)
        if file_meta is None:
            raise web.HTTPNotFound(text="File not found.")
        await cache.cache_set(token, file_meta)
    safe = {k: file_meta[k] for k in ("file_name", "file_size", "mime_type", "created_at", "downloads")}
    safe["god_speed"] = file_meta.get("god_speed_ready", False)
    return web.json_response(safe)


async def handle_health(request: web.Request) -> web.Response:
    pool: ClientPool = request.app["pool"]
    db_ok = await database.ping()
    return web.json_response({
        "status":  "ok",
        "clients": pool.count,
        "db":      db_ok,
        "mode":    "dual" if pool.count >= 2 else "single",
    })


def make_app(pool: ClientPool) -> web.Application:
    app = web.Application(client_max_size=1)
    app["pool"] = pool
    app.router.add_get("/dl/{token}", handle_download)
    app.router.add_get("/info/{token}", handle_info)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/", lambda r: web.Response(text="FileStreamBot running."))
    return app


async def start_server(pool: ClientPool) -> web.AppRunner:
    app = make_app(pool)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
    await site.start()
    log.info("Web server listening on port %d", Config.PORT)
    return runner
