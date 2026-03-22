"""
Streaming engine using Kurigram (Pyrogram fork).

stream_media() parameters:
  offset (int) — chunk index to start from (NOT byte offset)
  limit  (int) — number of chunks to stream (0 = all)

We convert the HTTP Range byte offset to chunk index + intra-chunk skip.
"""

import asyncio
import logging
from typing import AsyncGenerator, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

from bot.config import Config

log = logging.getLogger(__name__)

# Kurigram/Pyrogram internal chunk size is always 1 MB (1024 * 1024)
# regardless of what we pass. We use this for offset math.
TGRAM_CHUNK_SIZE = 1024 * 1024


async def iter_file(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
    chunk_size: int = Config.CHUNK_SIZE,
) -> AsyncGenerator[bytes, None]:
    """
    Stream a Telegram file as an async generator of bytes.

    offset = byte offset from HTTP Range header.
    Internally converts to chunk index for stream_media().
    """
    try:
        message = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("Failed to fetch message %s: %s", message_id, exc)
        return

    if message is None or message.empty:
        log.warning("Message %s not found in channel", message_id)
        return

    # Convert byte offset → chunk index + intra-chunk skip bytes
    chunk_index = offset // TGRAM_CHUNK_SIZE
    skip_bytes  = offset % TGRAM_CHUNK_SIZE

    buffer: asyncio.Queue[Optional[bytes]] = asyncio.Queue(
        maxsize=Config.PREFETCH_CHUNKS
    )

    async def _fill_buffer() -> None:
        first_chunk = True
        try:
            while True:
                try:
                    async for chunk in client.stream_media(
                        message,
                        offset=chunk_index,
                        limit=0,        # 0 = stream everything from offset
                    ):
                        # Trim the first chunk to honour the byte offset
                        if first_chunk:
                            first_chunk = False
                            if skip_bytes:
                                chunk = chunk[skip_bytes:]
                        if chunk:
                            await buffer.put(bytes(chunk))
                    break  # finished cleanly
                except FloodWait as fw:
                    log.warning(
                        "FloodWait %ss on msg %s — retrying",
                        fw.value, message_id
                    )
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Buffer fill error for msg %s: %s", message_id, exc)
        finally:
            await buffer.put(None)  # sentinel — always signals end

    filler = asyncio.create_task(_fill_buffer())
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
    Parse HTTP Range header into (start, end) byte positions.
    Returns (0, file_size-1) when no Range header is present.
    """
    if not range_header or not range_header.startswith("bytes="):
        return 0, file_size - 1

    try:
        spec = range_header[len("bytes="):]
        start_str, _, end_str = spec.partition("-")
        start = int(start_str) if start_str else 0
        end   = int(end_str)   if end_str   else file_size - 1
        end   = min(end, file_size - 1)
        start = max(0, start)
        return start, end
    except (ValueError, AttributeError):
        return 0, file_size - 1
