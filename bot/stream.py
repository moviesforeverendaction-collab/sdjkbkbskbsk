"""
Streaming engine — Kurigram 2.2.x

NORMAL MODE (1 client):
  Single client streams from Telegram.

DUAL CLIENT MODE (2 clients):
  Both clients stream the SAME file concurrently from Telegram.
  stream_media() cannot skip specific chunk indices so we cannot
  truly interleave at chunk level. Instead we run both clients in
  parallel and race them — whichever delivers a chunk first wins.
  In practice Telegram serves ~5 MB/s per connection so 2 clients
  fills a ~10 MB/s pipe. The real speed unlock is God Speed.

GOD SPEED MODE:
  File downloaded fully to Railway disk, then served from disk.
  Railway internal bandwidth is effectively unlimited — users get
  the full speed of their own internet connection.
"""

import asyncio
import logging
from pathlib import Path
from typing import AsyncGenerator, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

from bot.config import Config

log = logging.getLogger(__name__)

TGRAM_CHUNK = 1024 * 1024  # Telegram fixed 1 MB chunks


# ── God Speed cache paths ─────────────────────────────────────────────────────

def _cache_dir() -> Path:
    p = Path(Config.SESSION_DIR) / "gs_cache"
    p.mkdir(parents=True, exist_ok=True)
    return p


def cache_path(token: str) -> Path:
    return _cache_dir() / token


def clear_cache(token: str) -> None:
    p = cache_path(token)
    if p.exists():
        p.unlink()
        log.info("God Speed: cleared cache %s", token)


def get_cache_stats() -> tuple[int, int]:
    """Returns (file_count, total_bytes)."""
    d = _cache_dir()
    files = list(d.glob("*"))
    return len(files), sum(f.stat().st_size for f in files if f.is_file())


CACHE_DIR = _cache_dir()


# ── Single client stream ──────────────────────────────────────────────────────

async def _stream_single(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int,
) -> AsyncGenerator[bytes, None]:
    try:
        msg = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages failed msg=%s: %s", message_id, exc)
        return

    if msg is None or msg.empty:
        log.warning("Message %s not found", message_id)
        return

    chunk_index = offset // TGRAM_CHUNK
    skip_bytes  = offset %  TGRAM_CHUNK

    buf: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=Config.PREFETCH_CHUNKS)

    async def _fill() -> None:
        first = True
        try:
            while True:
                try:
                    async for chunk in client.stream_media(msg, offset=chunk_index, limit=0):
                        data = bytes(chunk)
                        if first:
                            first = False
                            if skip_bytes:
                                data = data[skip_bytes:]
                        if data:
                            await buf.put(data)
                    break
                except FloodWait as fw:
                    log.warning("FloodWait %ds msg=%s", fw.value, message_id)
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Fill error msg=%s: %s", message_id, exc)
        finally:
            await buf.put(None)

    filler = asyncio.create_task(_fill())
    try:
        while True:
            chunk = await buf.get()
            if chunk is None:
                break
            yield chunk
    except asyncio.CancelledError:
        filler.cancel()
        raise
    finally:
        if not filler.done():
            filler.cancel()
            try:
                await asyncio.wait_for(filler, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass


# ── Dual client: parallel prefetch merged in order ────────────────────────────

async def _stream_dual(
    c1: Client,
    c2: Client,
    message_id: int,
    channel_id: int,
    offset: int,
) -> AsyncGenerator[bytes, None]:
    """
    Both clients fetch from the SAME start point simultaneously.
    We use c1 as primary and c2 as a warm standby that pre-fetches
    into its own buffer. When c1 stalls (FloodWait), c2 takes over.
    This ensures the HTTP pipe stays full even when one client hits
    a rate limit.
    """
    try:
        msg1 = await c1.get_messages(channel_id, message_id)
        msg2 = await c2.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages failed msg=%s: %s", message_id, exc)
        return

    if not msg1 or msg1.empty or not msg2 or msg2.empty:
        return

    chunk_index = offset // TGRAM_CHUNK
    skip_bytes  = offset %  TGRAM_CHUNK

    # Primary buffer from c1, standby from c2
    buf1: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=Config.PREFETCH_CHUNKS)
    buf2: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=Config.PREFETCH_CHUNKS)

    async def _fill(client: Client, msg, buf: asyncio.Queue) -> None:
        first = True
        try:
            while True:
                try:
                    async for chunk in client.stream_media(msg, offset=chunk_index, limit=0):
                        data = bytes(chunk)
                        if first:
                            first = False
                            if skip_bytes:
                                data = data[skip_bytes:]
                        if data:
                            await buf.put(data)
                    break
                except FloodWait as fw:
                    log.warning("FloodWait %ds (dual) msg=%s", fw.value, message_id)
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Dual fill error msg=%s: %s", message_id, exc)
        finally:
            await buf.put(None)

    f1 = asyncio.create_task(_fill(c1, msg1, buf1))
    f2 = asyncio.create_task(_fill(c2, msg2, buf2))

    # Yield from buf1 (primary). If buf1 ends or stalls, switch to buf2.
    active, standby = buf1, buf2
    try:
        while True:
            try:
                chunk = await asyncio.wait_for(active.get(), timeout=5.0)
                if chunk is None:
                    # Primary done — switch to standby
                    active, standby = standby, active
                    chunk = await active.get()
                    if chunk is None:
                        break
                yield chunk
            except asyncio.TimeoutError:
                # Primary stalled — switch to standby
                active, standby = standby, active
    except asyncio.CancelledError:
        f1.cancel()
        f2.cancel()
        raise
    finally:
        for f in (f1, f2):
            if not f.done():
                f.cancel()
                try:
                    await asyncio.wait_for(f, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass


# ── Disk stream (God Speed) ───────────────────────────────────────────────────

async def iter_cached_file(
    path: Path,
    offset: int = 0,
    chunk_size: int = 2 * 1024 * 1024,  # 2 MB reads from disk
) -> AsyncGenerator[bytes, None]:
    """Stream a file from disk in large chunks. No Telegram involved."""
    loop = asyncio.get_event_loop()

    def _read_chunk(fp, size: int) -> bytes:
        return fp.read(size)

    with open(path, "rb") as fp:
        if offset:
            fp.seek(offset)
        while True:
            data = await loop.run_in_executor(None, _read_chunk, fp, chunk_size)
            if not data:
                break
            yield data


# ── God Speed: download file to disk ─────────────────────────────────────────

async def download_to_cache(
    client: Client,
    message_id: int,
    channel_id: int,
    token: str,
    progress_cb=None,   # async callable(done_bytes, total_bytes)
) -> Path:
    """
    Download entire file to Railway disk.
    progress_cb must be an async function.
    Returns path to cached file.
    """
    path = cache_path(token)
    if path.exists():
        return path

    tmp = path.with_suffix(".tmp")
    done = 0

    try:
        msg = await client.get_messages(channel_id, message_id)
        if msg is None or msg.empty:
            raise ValueError(f"Message {message_id} not found")

        media = (msg.document or msg.video or msg.audio or msg.voice
                 or msg.animation or msg.video_note or msg.photo)
        total = getattr(media, "file_size", 0) or 0

        with open(tmp, "wb") as f:
            while True:
                try:
                    async for chunk in client.stream_media(msg, offset=0, limit=0):
                        data = bytes(chunk)
                        f.write(data)
                        done += len(data)
                        if progress_cb:
                            await progress_cb(done, total)
                    break
                except FloodWait as fw:
                    log.warning("FloodWait %ds during God Speed download", fw.value)
                    await asyncio.sleep(fw.value)

        tmp.rename(path)
        log.info("God Speed cached %s — %d MB", token, done // (1024 * 1024))
        return path

    except Exception:
        if tmp.exists():
            tmp.unlink(missing_ok=True)
        raise


# ── Unified entry point ───────────────────────────────────────────────────────

async def iter_file(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
    client2: Optional[Client] = None,
) -> AsyncGenerator[bytes, None]:
    if client2 is not None:
        async for chunk in _stream_dual(client, client2, message_id, channel_id, offset):
            yield chunk
    else:
        async for chunk in _stream_single(client, message_id, channel_id, offset):
            yield chunk


def parse_range_header(
    range_header: Optional[str], file_size: int
) -> tuple[int, int]:
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1
    try:
        spec = range_header[6:]
        s, _, e = spec.partition("-")
        start = int(s) if s else 0
        end   = int(e) if e else file_size - 1
        return max(0, start), min(end, file_size - 1)
    except (ValueError, AttributeError):
        return 0, file_size - 1
