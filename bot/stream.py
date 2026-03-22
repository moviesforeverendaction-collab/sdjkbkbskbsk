"""
Streaming engine — two modes:

NORMAL MODE:
  Single client streams from Telegram → HTTP client.
  Bottleneck: Telegram's ~5-10 MB/s per connection.

DUAL STRIPE MODE (when 2 clients available):
  Client 1 fetches even chunks (0, 2, 4...)
  Client 2 fetches odd chunks  (1, 3, 5...)
  Both run in parallel — merged in order before HTTP write.
  Effective throughput: up to 2× single client speed.

GOD SPEED MODE:
  File is first fully downloaded to Railway disk (/data/cache/).
  Then served directly from disk at Railway's full network speed.
  Railway can push files at 500+ MB/s from disk vs 10 MB/s from Telegram.
  Triggered manually by user via toggle — not automatic (large files = disk cost).
"""

import asyncio
import logging
import os
from pathlib import Path
from typing import AsyncGenerator, Optional, Callable

from pyrogram import Client
from pyrogram.errors import FloodWait

from bot.config import Config

log = logging.getLogger(__name__)

TGRAM_CHUNK = 1024 * 1024  # 1 MB fixed chunk size in Telegram


# ── Single-client streaming ───────────────────────────────────────────────────

async def iter_file_single(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
) -> AsyncGenerator[bytes, None]:
    """Stream from one client. Used in normal mode or when only 1 API app."""
    try:
        message = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages failed msg=%s: %s", message_id, exc)
        return

    if message is None or message.empty:
        log.warning("Message %s not found", message_id)
        return

    chunk_index = offset // TGRAM_CHUNK
    skip_bytes  = offset %  TGRAM_CHUNK
    buffer: asyncio.Queue[Optional[bytes]] = asyncio.Queue(maxsize=Config.PREFETCH_CHUNKS)

    async def _fill() -> None:
        first = True
        try:
            while True:
                try:
                    async for chunk in client.stream_media(message, offset=chunk_index, limit=0):
                        data = bytes(chunk)
                        if first:
                            first = False
                            if skip_bytes:
                                data = data[skip_bytes:]
                        if data:
                            await buffer.put(data)
                    break
                except FloodWait as fw:
                    log.warning("FloodWait %ds msg=%s", fw.value, message_id)
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Fill error msg=%s: %s", message_id, exc)
        finally:
            await buffer.put(None)

    filler = asyncio.create_task(_fill())
    try:
        while True:
            chunk = await buffer.get()
            if chunk is None:
                break
            yield chunk
    except asyncio.CancelledError:
        filler.cancel()
        try:
            await asyncio.wait_for(filler, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        raise
    finally:
        if not filler.done():
            filler.cancel()
            try:
                await asyncio.wait_for(filler, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass


# ── Dual-client stripe streaming ──────────────────────────────────────────────

async def iter_file_dual(
    client1: Client,
    client2: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
) -> AsyncGenerator[bytes, None]:
    """
    Stripe download across two clients for ~2× speed.
    client1 fetches even chunks, client2 fetches odd chunks.
    Chunks are re-ordered and yielded sequentially.
    """
    try:
        msg1 = await client1.get_messages(channel_id, message_id)
        msg2 = await client2.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages failed msg=%s: %s", message_id, exc)
        return

    if msg1 is None or msg1.empty or msg2 is None or msg2.empty:
        log.warning("Message %s not found for dual stream", message_id)
        return

    start_chunk = offset // TGRAM_CHUNK
    skip_bytes  = offset %  TGRAM_CHUNK

    # Ordered output buffer — chunks arrive out of order, we sort them
    # We use two queues: one per client, and merge in order
    q1: asyncio.Queue[Optional[tuple[int, bytes]]] = asyncio.Queue(maxsize=Config.PREFETCH_CHUNKS)
    q2: asyncio.Queue[Optional[tuple[int, bytes]]] = asyncio.Queue(maxsize=Config.PREFETCH_CHUNKS)

    async def _fetch(client: Client, msg, queue, start: int, step: int) -> None:
        """Fetch every `step`-th chunk starting from `start`."""
        idx = start
        first = True
        try:
            while True:
                try:
                    async for chunk in client.stream_media(msg, offset=idx, limit=step):
                        data = bytes(chunk)
                        if first and skip_bytes and idx == start_chunk:
                            data = data[skip_bytes:]
                            first = False
                        if data:
                            await queue.put((idx, data))
                        idx += step
                    break
                except FloodWait as fw:
                    log.warning("FloodWait %ds (dual) msg=%s", fw.value, message_id)
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Dual fill error msg=%s client=%s: %s", message_id, start, exc)
        finally:
            await queue.put(None)

    # client1 → even chunks (start_chunk, start_chunk+2, ...)
    # client2 → odd  chunks (start_chunk+1, start_chunk+3, ...)
    t1 = asyncio.create_task(_fetch(client1, msg1, q1, start_chunk,     2))
    t2 = asyncio.create_task(_fetch(client2, msg2, q2, start_chunk + 1, 2))

    pending: dict[int, bytes] = {}  # out-of-order buffer
    next_idx = start_chunk
    done1 = done2 = False

    try:
        while not (done1 and done2) or pending:
            # Try to yield next expected chunk from pending buffer first
            if next_idx in pending:
                yield pending.pop(next_idx)
                next_idx += 1
                continue

            # Collect from whichever queue has data
            tasks = []
            if not done1:
                tasks.append(asyncio.create_task(q1.get()))
            if not done2:
                tasks.append(asyncio.create_task(q2.get()))

            if not tasks:
                break

            done_set, pending_set = await asyncio.wait(
                tasks, return_when=asyncio.FIRST_COMPLETED
            )
            # Cancel the ones we didn't consume
            for t in pending_set:
                t.cancel()

            for done_task in done_set:
                result = done_task.result()
                if result is None:
                    # whichever queue sent None is done
                    if not done1:
                        done1 = True
                    else:
                        done2 = True
                else:
                    idx, data = result
                    if idx == next_idx:
                        yield data
                        next_idx += 1
                    else:
                        pending[idx] = data

    except asyncio.CancelledError:
        t1.cancel()
        t2.cancel()
        raise
    finally:
        for t in (t1, t2):
            if not t.done():
                t.cancel()
                try:
                    await asyncio.wait_for(t, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass


# ── God Speed: download to disk, serve from disk ──────────────────────────────

CACHE_DIR = Path(Config.SESSION_DIR) / "godspeed_cache"


def _ensure_cache_dir() -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)


def cache_path(token: str) -> Path:
    return CACHE_DIR / token


async def download_to_cache(
    client: Client,
    message_id: int,
    channel_id: int,
    token: str,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> Path:
    """
    Download the full file to disk for God Speed serving.
    Returns the path to the cached file.
    progress_cb(bytes_done, total_bytes) called every chunk.
    """
    _ensure_cache_dir()
    path = cache_path(token)

    if path.exists():
        return path  # already cached

    tmp_path = path.with_suffix(".tmp")
    bytes_done = 0

    try:
        msg = await client.get_messages(channel_id, message_id)
        total = getattr(msg.document or msg.video or msg.audio or
                        msg.voice or msg.animation or msg.photo, "file_size", 0) or 0

        with open(tmp_path, "wb") as f:
            while True:
                try:
                    async for chunk in client.stream_media(msg, offset=0, limit=0):
                        data = bytes(chunk)
                        f.write(data)
                        bytes_done += len(data)
                        if progress_cb:
                            progress_cb(bytes_done, total)
                    break
                except FloodWait as fw:
                    log.warning("FloodWait %ds during cache download", fw.value)
                    await asyncio.sleep(fw.value)

        tmp_path.rename(path)
        log.info("God Speed: cached %s (%d MB)", token, bytes_done // (1024 * 1024))
        return path
    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink()
        raise exc


def clear_cache(token: str) -> None:
    p = cache_path(token)
    if p.exists():
        p.unlink()
        log.info("God Speed: cleared cache for %s", token)


async def iter_cached_file(
    path: Path,
    offset: int = 0,
    chunk_size: int = 2 * 1024 * 1024,   # 2 MB read chunks from disk = max speed
) -> AsyncGenerator[bytes, None]:
    """Serve a file from disk in large chunks. This is the fast path."""
    loop = asyncio.get_event_loop()

    def _read_sync():
        chunks = []
        with open(path, "rb") as f:
            f.seek(offset)
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                chunks.append(data)
        return chunks

    # Run in thread pool so we don't block the event loop
    chunks = await loop.run_in_executor(None, _read_sync)
    for chunk in chunks:
        yield chunk


# ── Unified entry point ───────────────────────────────────────────────────────

async def iter_file(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
    client2: Optional[Client] = None,
) -> AsyncGenerator[bytes, None]:
    """
    Auto-selects streaming strategy:
    - dual stripe if client2 provided
    - single stream otherwise
    """
    if client2 is not None:
        async for chunk in iter_file_dual(client, client2, message_id, channel_id, offset):
            yield chunk
    else:
        async for chunk in iter_file_single(client, message_id, channel_id, offset):
            yield chunk


def parse_range_header(
    range_header: Optional[str], file_size: int
) -> tuple[int, int]:
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1
    try:
        spec = range_header[6:]
        start_s, _, end_s = spec.partition("-")
        start = int(start_s) if start_s else 0
        end   = int(end_s)   if end_s   else file_size - 1
        return max(0, start), min(end, file_size - 1)
    except (ValueError, AttributeError):
        return 0, file_size - 1
