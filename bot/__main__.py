"""
Entry point.  Run with:  python -m bot
"""

import asyncio
import logging
import signal
import sys

import uvloop

from bot.config import Config
from bot.client_pool import ClientPool
from bot.database import ensure_indexes, delete_expired_files
from bot.handlers import register_handlers
from bot.server import start_server

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)


async def _cleanup_loop() -> None:
    """
    Belt-and-suspenders sweep for expired links.
    MongoDB TTL handles most deletions, but this catches any that slip through
    during the up-to-60s TTL window.  Runs every hour.
    """
    while True:
        await asyncio.sleep(3600)
        try:
            deleted = await delete_expired_files()
            if deleted:
                log.info("Cleanup sweep: removed %d expired link(s).", deleted)
        except Exception as exc:
            log.warning("Cleanup sweep error: %s", exc)


async def main() -> None:
    Config.validate()

    log.info("Starting FileStreamBot...")
    log.info("  API apps configured: %d", len(Config.api_pairs()))
    log.info("  Chunk size: %d KB", Config.CHUNK_SIZE // 1024)
    log.info("  Prefetch chunks: %d", Config.PREFETCH_CHUNKS)

    # Set up MongoDB indexes (including the TTL index on expires_at)
    await ensure_indexes()
    log.info("  MongoDB indexes ready.")

    # Start Telegram client pool (1 or 2 clients depending on env vars)
    pool = ClientPool()
    await pool.start()

    # Force Pyrogram to resolve and cache the storage channel peer.
    # Without this, the first forward() call fails with "Peer id invalid"
    # because Pyrogram hasn't seen this channel in the current session yet.
    try:
        chat = await pool.primary().get_chat(Config.CHANNEL_ID)
        log.info("  Storage channel: %s (id=%s)", chat.title, chat.id)
    except Exception as exc:
        log.error(
            "  Could not resolve storage channel %s: %s\n"
            "  Make sure the bot is an ADMIN of that channel.",
            Config.CHANNEL_ID, exc
        )
        await pool.stop()
        return

    # Register all bot handlers on the primary client
    register_handlers(pool.primary())

    # Start aiohttp web server
    runner = await start_server(pool)

    # Start hourly expired-link cleanup in the background
    cleanup_task = asyncio.create_task(_cleanup_loop())

    log.info("Bot is running. Press Ctrl+C to stop.")

    stop = asyncio.Event()

    def _handle_signal(*_):
        log.info("Shutdown signal received.")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    await stop.wait()

    log.info("Shutting down...")
    cleanup_task.cancel()
    await runner.cleanup()
    await pool.stop()
    log.info("Bye!")


if __name__ == "__main__":
    # uvloop.run() installs uvloop as the event loop policy and runs main().
    # This must be the outermost call — never call uvloop.install() after
    # an event loop has already started (e.g. inside an async function).
    uvloop.run(main())
