"""
Streaming engine — Kurigram (Pyrogram fork).

stream_media() API (Kurigram 2.2.x):
  offset (int) — number of 1 MB chunks to skip (NOT a byte offset)
  limit  (int) — number of chunks to stream; 0 = stream everything

We receive a byte offset from the HTTP Range header and convert:
  chunk_index = byte_offset // 1_048_576
  skip_bytes  = byte_offset %  1_048_576  (trim the first chunk)
"""

import asyncio
import logging
from typing import AsyncGenerator, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

from bot.config import Config

log = logging.getLogger(__name__)

# Telegram's fixed internal chunk size — always 1 MiB, cannot be changed.
TGRAM_CHUNK = 1024 * 1024  # 1 MB


async def iter_file(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,            # byte offset from HTTP Range header
) -> AsyncGenerator[bytes, None]:
    """
    Async generator that yields file bytes from Telegram.
    Handles HTTP Range byte offsets by converting to chunk index.
    """
    try:
        message = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("get_messages failed for msg %s: %s", message_id, exc)
        return

    if message is None or message.empty:
        log.warning("Message %s not found in channel", message_id)
        return

    chunk_index = offset // TGRAM_CHUNK   # which 1 MB chunk to start from
    skip_bytes  = offset %  TGRAM_CHUNK   # bytes to trim from the first chunk

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
                        limit=0,    # 0 = all chunks from offset onward
                    ):
                        if first:
                            first = False
                            if skip_bytes:
                                chunk = chunk[skip_bytes:]
                        if chunk:
                            await buffer.put(bytes(chunk))
                    break   # stream finished cleanly
                except FloodWait as fw:
                    log.warning("FloodWait %ds on msg %s — waiting", fw.value, message_id)
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Stream fill error for msg %s: %s", message_id, exc)
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
        raise
    finally:
        if not filler.done():
            filler.cancel()


def parse_range_header(
    range_header: Optional[str], file_size: int
) -> tuple[int, int]:
    """
    Parse HTTP Range header → (start_byte, end_byte).
    Returns (0, file_size-1) when header is absent or invalid.
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1
    try:
        spec = range_header[6:]                     # strip "bytes="
        start_s, _, end_s = spec.partition("-")
        start = int(start_s) if start_s else 0
        end   = int(end_s)   if end_s   else file_size - 1
        return max(0, start), min(end, file_size - 1)
    except (ValueError, AttributeError):
        return 0, file_size - 1
