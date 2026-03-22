import asyncio
import logging
import platform
import signal
import sys
import time

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

_BOOT_TIME = time.time()


async def _send_startup_message(pool: ClientPool, chat: object) -> None:
    """Send a detailed startup report to the storage channel."""
    import pyrogram
    pairs = Config.api_pairs()
    uptime_ts = int(_BOOT_TIME)

    text = (
        "🟢 **FileStreamBot Started**\n"
        "─────────────────────────\n"
        f"🤖 **Channel:** {getattr(chat, 'title', '?')} (`{getattr(chat, 'id', '?')}`)\n"
        f"🔑 **API Apps:** {len(pairs)} client(s) active\n"
        f"📦 **Chunk Size:** {Config.CHUNK_SIZE // 1024} KB\n"
        f"⚡ **Prefetch:** {Config.PREFETCH_CHUNKS} chunks\n"
        f"🌐 **Base URL:** `{Config.BASE_URL}`\n"
        f"🗄 **DB:** `{Config.DB_NAME}`\n"
        f"🔴 **Redis:** {'enabled' if Config.REDIS_URL else 'disabled'}\n"
        f"👑 **Owners:** {len(Config.OWNER_IDS)} configured\n"
        f"🔒 **Upload restricted:** {'yes' if Config.OWNER_ONLY_UPLOAD else 'no (anyone can upload)'}\n"
        "─────────────────────────\n"
        f"🐍 Python `{platform.python_version()}` · "
        f"Pyrogram `{pyrogram.__version__}`\n"
        f"🖥 `{platform.system()} {platform.release()}`\n"
        f"🕐 Started at `{time.strftime('%Y-%m-%d %H:%M:%S UTC', time.gmtime(uptime_ts))}`"
    )

    try:
        await pool.primary().send_message(Config.CHANNEL_ID, text)
        log.info("  Startup message sent to channel.")
    except Exception as exc:
        log.warning("  Could not send startup message to channel: %s", exc)


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

    # Resolve the storage channel peer so Pyrogram caches the access hash.
    # On a fresh session, get_chat() fails on private channels the bot hasn't
    # interacted with. We fall back to walking dialogs to find it.
    chat = None
    try:
        chat = await pool.primary().get_chat(Config.CHANNEL_ID)
        log.info("  Storage channel resolved: %s (id=%s)", chat.title, chat.id)
    except Exception:
        log.info("  Channel not in peer cache — scanning dialogs...")
        try:
            async for dialog in pool.primary().get_dialogs():
                if dialog.chat and dialog.chat.id == Config.CHANNEL_ID:
                    chat = dialog.chat
                    log.info("  Storage channel found: %s", chat.title)
                    break
        except Exception as exc:
            log.warning("  Dialog scan error: %s", exc)

    if chat is None:
        log.error(
            "  Could not resolve storage channel %s.\n"
            "  Fix: open your channel in Telegram, go to the bot's profile\n"
            "  and send any message from the bot to the channel manually.\n"
            "  This forces Telegram to register the bot<->channel relationship.\n"
            "  Then redeploy.",
            Config.CHANNEL_ID,
        )
        await pool.stop()
        return

    # Register all bot handlers on the primary client
    register_handlers(pool.primary())

    # Start aiohttp web server
    runner = await start_server(pool)

    # Send startup report to the storage channel
    await _send_startup_message(pool, chat)

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
