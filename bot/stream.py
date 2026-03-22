"""
Streaming engine.

Pulls file data from Telegram via Pyrogram's stream_media(),
pipes it chunk by chunk to the aiohttp response writer.
"""

import asyncio
import logging
from typing import AsyncGenerator, Optional

from pyrogram import Client
from pyrogram.errors import FloodWait

from bot.config import Config

log = logging.getLogger(__name__)


async def iter_file(
    client: Client,
    message_id: int,
    channel_id: int,
    offset: int = 0,
    chunk_size: int = Config.CHUNK_SIZE,
) -> AsyncGenerator[bytes, None]:
    """
    Stream a Telegram file as an async generator of bytes.

    offset is a BYTE offset (from HTTP Range header).
    Pyrogram's stream_media() takes a chunk-index offset, so we convert:
      chunk_index = offset // chunk_size
      skip_bytes  = offset % chunk_size  (partial first chunk)
    """
    try:
        message = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("Failed to fetch message %s: %s", message_id, exc)
        return

    if message is None or message.empty:
        log.warning("Message %s not found in channel", message_id)
        return

    # Convert byte offset → chunk index + intra-chunk skip
    chunk_index = offset // chunk_size
    skip_bytes  = offset % chunk_size

    buffer: asyncio.Queue[Optional[bytes]] = asyncio.Queue(
        maxsize=Config.PREFETCH_CHUNKS
    )

    async def _fill_buffer() -> None:
        first = True
        try:
            while True:
                try:
                    async for chunk in client.stream_media(
                        message,
                        offset=chunk_index,
                        chunk_size=chunk_size,
                    ):
                        if first and skip_bytes:
                            # Trim the partial first chunk to honour byte offset
                            chunk = chunk[skip_bytes:]
                            first = False
                        else:
                            first = False
                        if chunk:
                            await buffer.put(chunk)
                    break
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
            await buffer.put(None)  # sentinel

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
