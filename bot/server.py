"""
aiohttp web server.

Routes:
  GET /dl/{token}           — stream a file
  GET /info/{token}         — JSON file metadata (for bots / integrations)
  GET /health               — Railway health check

All streaming goes through ClientPool.get() so downloads automatically
spread across both Telegram app buckets when two are configured.
"""

import asyncio
import logging
import time
from collections import defaultdict

from aiohttp import web

from bot import database, cache
from bot.client_pool import ClientPool
from bot.config import Config
from bot.stream import iter_file, parse_range_header

log = logging.getLogger(__name__)

# ── Simple in-memory rate limiter ──────────────────────────────────────────
_rate_counters: dict[str, list[float]] = defaultdict(list)


def _is_rate_limited(ip: str) -> bool:
    now = time.monotonic()
    window = 60.0
    hits = _rate_counters[ip]
    # Drop hits older than 1 minute
    _rate_counters[ip] = [t for t in hits if now - t < window]
    if len(_rate_counters[ip]) >= Config.RATE_LIMIT_PER_MINUTE:
        return True
    _rate_counters[ip].append(now)
    return False


# ── Request handlers ────────────────────────────────────────────────────────

async def handle_download(request: web.Request) -> web.StreamResponse:
    token = request.match_info["token"]
    ip = (
        request.headers.get("X-Forwarded-For", request.remote or "unknown")
        .split(",")[0]
        .strip()
    )

    # Rate limit
    if _is_rate_limited(ip):
        raise web.HTTPTooManyRequests(text="Slow down — too many requests.")

    # Resolve metadata: Redis first, then MongoDB. Always re-check expiry.
    file_meta = await cache.cache_get(token)
    if file_meta is not None:
        expires_at = file_meta.get("expires_at")
        if expires_at and int(time.time()) > expires_at:
            # Stale cache — evict and re-fetch (database.get_file will also return None)
            await cache.cache_delete(token)
            file_meta = None

    if file_meta is None:
        file_meta = await database.get_file(token)  # returns None if expired
        if file_meta is None:
            raise web.HTTPNotFound(text="File not found or link expired.")
        await cache.cache_set(token, file_meta)

    file_size: int = file_meta["file_size"]
    file_name: str = file_meta["file_name"]
    mime_type: str = file_meta.get("mime_type", "application/octet-stream")
    message_id: int = file_meta["message_id"]

    # file_size=0: Telegram didn't report a size (some photos/stickers/voice).
    # Range requests need a known size, so fall back to full-file streaming.
    known_size = file_size > 0
    range_header = request.headers.get("Range") if known_size else None
    start, end = parse_range_header(range_header, file_size) if known_size else (0, 0)
    content_length = (end - start + 1) if known_size else None

    # Build response headers
    headers: dict[str, str] = {
        "Content-Type": mime_type,
        "Content-Disposition": f'attachment; filename="{file_name}"',
        "Accept-Ranges": "bytes" if known_size else "none",
        "Cache-Control": "no-store",
        # Critical for Railway/nginx proxies — tells the proxy NOT to buffer
        # the entire response before forwarding. Without this the client sees
        # nothing until the whole file is downloaded to the proxy first.
        "X-Accel-Buffering": "no",
        "Transfer-Encoding": "chunked",
    }
    if known_size:
        headers["Content-Length"] = str(content_length)
        headers["Content-Range"] = (
            f"bytes {start}-{end}/{file_size}"
            if range_header
            else f"bytes 0-{file_size - 1}/{file_size}"
        )

    status = 206 if (range_header and known_size) else 200
    resp = web.StreamResponse(status=status, headers=headers)
    await resp.prepare(request)

    # Pick the next client from the pool (round-robin across both API apps)
    pool: ClientPool = request.app["pool"]
    client = pool.get()

    bytes_sent = 0
    completed = False
    try:
        async for chunk in iter_file(
            client=client,
            message_id=message_id,
            channel_id=Config.CHANNEL_ID,
            offset=start,
        ):
            # Stop once we've sent everything the Range asked for
            if known_size and bytes_sent >= content_length:
                break
            if known_size:
                remaining = content_length - bytes_sent
                if len(chunk) > remaining:
                    chunk = chunk[:remaining]
            await resp.write(chunk)
            bytes_sent += len(chunk)
        completed = True
    except (ConnectionResetError, asyncio.CancelledError):
        pass  # Client disconnected — perfectly normal
    except Exception as exc:
        log.error("Streaming error for token %s: %s", token, exc)

    # Only count as a completed download if the full stream finished cleanly
    if completed:
        asyncio.create_task(database.increment_downloads(token))

    return resp


async def handle_info(request: web.Request) -> web.Response:
    token = request.match_info["token"]
    file_meta = await cache.cache_get(token)
    if file_meta is not None:
        expires_at = file_meta.get("expires_at")
        if expires_at and int(time.time()) > expires_at:
            await cache.cache_delete(token)
            file_meta = None

    if file_meta is None:
        file_meta = await database.get_file(token)
        if file_meta is None:
            raise web.HTTPNotFound(text="File not found.")
        await cache.cache_set(token, file_meta)

    # Don't leak internal fields
    safe = {k: file_meta[k] for k in ("file_name", "file_size", "mime_type", "created_at", "downloads")}
    return web.json_response(safe)


async def handle_health(request: web.Request) -> web.Response:
    pool: ClientPool = request.app["pool"]
    db_ok = await database.ping()
    return web.json_response({
        "status": "ok",
        "clients": pool.count,
        "db": db_ok,
    })


# ── App factory ─────────────────────────────────────────────────────────────

def make_app(pool: ClientPool) -> web.Application:
    app = web.Application(client_max_size=1)  # No upload on the web server side

    app["pool"] = pool

    app.router.add_get("/dl/{token}", handle_download)
    app.router.add_get("/info/{token}", handle_info)
    app.router.add_get("/health", handle_health)
    app.router.add_get("/", lambda r: web.Response(text="FileStream is running."))

    return app


async def start_server(pool: ClientPool) -> web.AppRunner:
    app = make_app(pool)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()

    site = web.TCPSite(runner, "0.0.0.0", Config.PORT)
    await site.start()
    log.info("Web server listening on port %d", Config.PORT)
    return runner
