"""
Streaming engine — Kurigram 2.2.x

stream_media(offset, limit):
  offset = chunk index to start from (1 chunk = 1 MB)
  limit  = number of chunks to fetch (0 = all)

Speed strategy:
  - PREFETCH_CHUNKS=8 keeps 8 MB queued ahead of the HTTP writer
  - bytes(chunk) ensures we own the buffer before the MTProto layer recycles it
  - filler task is properly awaited on cancel to avoid "Task destroyed" warnings
"""

import asyncio
import logging
from typing import AsyncGenerator, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

from bot.config import Config

log = logging.getLogger(__name__)

TGRAM_CHUNK = 1024 * 1024  # Telegram's fixed 1 MB chunk size


async def iter_file(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
) -> AsyncGenerator[bytes, None]:
    """
    Yields file bytes from Telegram.
    offset is a BYTE offset (from HTTP Range); converted to chunk index internally.
    """
    try:
        message = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages failed msg=%s: %s", message_id, exc)
        return

    if message is None or message.empty:
        log.warning("Message %s not found in channel", message_id)
        return

    chunk_index = offset // TGRAM_CHUNK
    skip_bytes  = offset %  TGRAM_CHUNK

    # Generous buffer — keeps the HTTP pipe full while Telegram fetches next chunk.
    # Each slot = 1 MB, so PREFETCH_CHUNKS=8 = 8 MB pre-queued in RAM.
    buffer: asyncio.Queue[Optional[bytes]] = asyncio.Queue(
        maxsize=Config.PREFETCH_CHUNKS
    )

    async def _fill() -> None:
        first = True
        try:
            while True:
                try:
                    async for chunk in client.stream_media(
                        message,
                        offset=chunk_index,
                        limit=0,
                    ):
                        data = bytes(chunk)  # own the buffer before MTProto recycles it
                        if first:
                            first = False
                            if skip_bytes:
                                data = data[skip_bytes:]
                        if data:
                            await buffer.put(data)
                    break  # clean finish
                except FloodWait as fw:
                    log.warning("FloodWait %ds on msg %s", fw.value, message_id)
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Fill error msg=%s: %s", message_id, exc)
        finally:
            await buffer.put(None)  # always signal end

    filler = asyncio.create_task(_fill())
    try:
        while True:
            chunk = await buffer.get()
            if chunk is None:
                break
            yield chunk
    except asyncio.CancelledError:
        filler.cancel()
        # Properly await the cancellation so the task is not left pending
        try:
            await asyncio.wait_for(asyncio.shield(filler), timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        raise
    finally:
        if not filler.done():
            filler.cancel()
            # Drain the buffer so the filler task can exit cleanly
            try:
                await asyncio.wait_for(filler, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass


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
