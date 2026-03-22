"""
Streaming engine.

Pulls file data from Telegram via Pyrogram's stream_media(),
and yields it chunk by chunk to the aiohttp response writer.

Accepts any client from ClientPool — the pool handles round-robin
so download load spreads across both API app buckets automatically.
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

    Args:
        client:      Pyrogram bot client (single session).
        message_id:  ID of the message in the storage channel.
        channel_id:  ID of the private storage channel.
        offset:      Byte offset (for range requests / resume support).
        chunk_size:  Bytes per chunk yielded to the HTTP response.
    """
    try:
        message = await client.get_messages(channel_id, message_id)
    except Exception as exc:
        log.error("Failed to fetch message %s: %s", message_id, exc)
        return

    if message is None or message.empty:
        log.warning("Message %s not found in channel", message_id)
        return

    # Pre-read buffer: keeps the HTTP pipe full between Telegram round-trips.
    buffer: asyncio.Queue[Optional[bytes]] = asyncio.Queue(
        maxsize=Config.PREFETCH_CHUNKS
    )

    async def _fill_buffer() -> None:
        try:
            while True:
                try:
                    async for chunk in client.stream_media(
                        message, offset=offset, chunk_size=chunk_size
                    ):
                        await buffer.put(chunk)
                    break  # stream finished cleanly
                except FloodWait as fw:
                    # Back off exactly as long as Telegram asks, then retry.
                    log.warning("FloodWait %ss on msg %s — retrying after sleep", fw.value, message_id)
                    await asyncio.sleep(fw.value)
        except asyncio.CancelledError:
            pass
        except Exception as exc:
            log.error("Error filling buffer for msg %s: %s", message_id, exc)
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
        range_spec = range_header[len("bytes="):]
        start_str, _, end_str = range_spec.partition("-")
        start = int(start_str) if start_str else 0
        end = int(end_str) if end_str else file_size - 1
        end = min(end, file_size - 1)
        start = max(0, start)
        return start, end
    except (ValueError, AttributeError):
        return 0, file_size - 1
