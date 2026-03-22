"""
Streaming engine — Kurigram 2.2.x

CHUNK SIZE: Telegram fixed at 1 MB per chunk.

GOD SPEED DOWNLOAD:
  Uses multiple independent Pyrogram clients, each opening their own
  MTProto connection to Telegram's media DC.
  
  Each worker calls get_messages() independently then stream_media()
  with a specific offset (chunk index) and limit (number of chunks).
  
  With 2 API apps × 8 workers per app = 16 parallel connections.
  Telegram DC can sustain ~3-8 MB/s per connection.
  Expected: 16 × 4 MB/s avg = ~64 MB/s theoretical peak.
  Real-world Railway↔Telegram: 20-60 MB/s.

STANDARD STREAMING:
  Dual-client hot-standby for ~10-20 MB/s to HTTP clients.
"""

import asyncio
import logging
from pathlib import Path
from typing import AsyncGenerator, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

from bot.config import Config

log = logging.getLogger(__name__)

TGRAM_CHUNK = 1024 * 1024  # Telegram fixed 1 MB


# ── Cache helpers ─────────────────────────────────────────────────────────────

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

def get_cache_stats() -> tuple[int, int]:
    d = _cache_dir()
    files = [f for f in d.iterdir() if f.is_file() and not f.suffix.startswith(".seg") and not f.suffix == ".tmp"]
    return len(files), sum(f.stat().st_size for f in files)

CACHE_DIR = _cache_dir()


# ── Single client stream ──────────────────────────────────────────────────────

async def _stream_single(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
) -> AsyncGenerator[bytes, None]:
    try:
        msg = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages failed msg=%s: %s", message_id, exc)
        return
    if msg is None or msg.empty:
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


# ── Dual client hot-standby stream ────────────────────────────────────────────

async def _stream_dual(
    c1: Client, c2: Client,
    message_id: int, channel_id: int,
    offset: int = 0,
) -> AsyncGenerator[bytes, None]:
    """Both clients stream. c1 is primary, c2 takes over if c1 stalls."""
    try:
        msg1 = await c1.get_messages(channel_id, message_id)
        msg2 = await c2.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages dual failed: %s", exc)
        return
    if not msg1 or msg1.empty or not msg2 or msg2.empty:
        return

    chunk_index = offset // TGRAM_CHUNK
    skip_bytes  = offset %  TGRAM_CHUNK
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
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Dual fill error: %s", exc)
        finally:
            await buf.put(None)

    f1 = asyncio.create_task(_fill(c1, msg1, buf1))
    f2 = asyncio.create_task(_fill(c2, msg2, buf2))
    active, standby = buf1, buf2

    try:
        while True:
            try:
                chunk = await asyncio.wait_for(active.get(), timeout=5.0)
                if chunk is None:
                    active, standby = standby, active
                    chunk = await active.get()
                    if chunk is None:
                        break
                yield chunk
            except asyncio.TimeoutError:
                active, standby = standby, active
    except asyncio.CancelledError:
        f1.cancel(); f2.cancel()
        raise
    finally:
        for f in (f1, f2):
            if not f.done():
                f.cancel()
                try:
                    await asyncio.wait_for(f, timeout=2.0)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    pass


# ── Disk stream (God Speed serve) ─────────────────────────────────────────────

async def iter_cached_file(
    path: Path,
    offset: int = 0,
    chunk_size: int = 4 * 1024 * 1024,  # 4 MB disk reads
) -> AsyncGenerator[bytes, None]:
    loop = asyncio.get_event_loop()
    with open(path, "rb") as fp:
        if offset:
            fp.seek(offset)
        while True:
            data = await loop.run_in_executor(None, fp.read, chunk_size)
            if not data:
                break
            yield data


# ── God Speed: parallel download to disk ──────────────────────────────────────

async def download_to_cache(
    client: Client,
    message_id: int,
    channel_id: int,
    token: str,
    progress_cb=None,
    client2: Optional[Client] = None,
) -> Path:
    """
    Download file to Railway disk in parallel.

    Architecture:
      - 8 workers per available client (up to 16 total with 2 API apps)
      - Each worker gets its own message reference + stream from a specific offset
      - Workers write to segment files, assembled in order at the end
      - Non-blocking disk I/O via run_in_executor
    """
    path = cache_path(token)
    if path.exists():
        return path

    tmp = path.with_suffix(".tmp")
    loop = asyncio.get_event_loop()

    try:
        # Fetch message via each available client
        msg1 = await client.get_messages(channel_id, message_id)
        if msg1 is None or msg1.empty:
            raise ValueError(f"Message {message_id} not found")

        msg2 = None
        if client2 is not None:
            try:
                msg2 = await client2.get_messages(channel_id, message_id)
                if msg2 and msg2.empty:
                    msg2 = None
            except Exception as exc:
                log.warning("God Speed: client2 get_messages failed: %s", exc)

        media = (msg1.document or msg1.video or msg1.audio or msg1.voice
                 or msg1.animation or msg1.video_note or msg1.photo)
        total_bytes = getattr(media, "file_size", 0) or 0

        # Unknown size — sequential fallback
        if total_bytes == 0:
            done = 0
            with open(tmp, "wb") as f:
                while True:
                    try:
                        async for chunk in client.stream_media(msg1, offset=0, limit=0):
                            data = bytes(chunk)
                            f.write(data)
                            done += len(data)
                            if progress_cb:
                                await progress_cb(done, 0)
                        break
                    except FloodWait as fw:
                        await asyncio.sleep(fw.value)
            tmp.rename(path)
            return path

        total_chunks = (total_bytes + TGRAM_CHUNK - 1) // TGRAM_CHUNK

        # Build worker list: 8 workers per client
        WORKERS_PER_CLIENT = 8
        worker_pool = [(client, msg1)] * WORKERS_PER_CLIENT
        if msg2 is not None:
            worker_pool += [(client2, msg2)] * WORKERS_PER_CLIENT

        n_workers = len(worker_pool)
        cpw = (total_chunks + n_workers - 1) // n_workers  # chunks per worker

        done_bytes = [0]
        seg_map: dict[int, Path] = {}

        async def _worker(idx: int, cli: Client, msg, start: int, end: int) -> None:
            if start >= total_chunks or start >= end:
                return
            end = min(end, total_chunks)
            count = end - start
            seg_path = tmp.parent / f"{tmp.stem}.seg{idx:03d}"
            seg_map[idx] = seg_path

            with open(seg_path, "wb") as f:
                retries = 0
                while True:
                    try:
                        async for raw in cli.stream_media(msg, offset=start, limit=count):
                            data = bytes(raw)
                            await loop.run_in_executor(None, f.write, data)
                            done_bytes[0] += len(data)
                            if progress_cb:
                                await progress_cb(done_bytes[0], total_bytes)
                        break
                    except FloodWait as fw:
                        log.warning("God Speed FloodWait %ds worker=%d", fw.value, idx)
                        await asyncio.sleep(fw.value)
                    except Exception as exc:
                        retries += 1
                        if retries >= 3:
                            log.error("God Speed worker=%d failed after 3 retries: %s", idx, exc)
                            raise
                        log.warning("God Speed worker=%d retry %d: %s", idx, retries, exc)
                        await asyncio.sleep(2)

        tasks = []
        for i, (cli, msg) in enumerate(worker_pool):
            s = i * cpw
            e = s + cpw
            if s >= total_chunks:
                break
            tasks.append(asyncio.create_task(_worker(i, cli, msg, s, e)))

        await asyncio.gather(*tasks)

        # Assemble in order
        def _assemble() -> None:
            with open(tmp, "wb") as out:
                for idx in sorted(seg_map.keys()):
                    seg = seg_map[idx]
                    if seg.exists():
                        with open(seg, "rb") as s:
                            while True:
                                buf = s.read(8 * 1024 * 1024)  # 8 MB copy buffer
                                if not buf:
                                    break
                                out.write(buf)
                        seg.unlink()

        await loop.run_in_executor(None, _assemble)
        tmp.rename(path)
        log.info("God Speed cached token=%s size=%dMB", token, total_bytes >> 20)
        return path

    except Exception:
        # Clean up all segment and tmp files
        for p in tmp.parent.glob(f"{tmp.stem}*"):
            try:
                p.unlink()
            except Exception:
                pass
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
