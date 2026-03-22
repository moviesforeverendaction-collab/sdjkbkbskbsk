# !! MUST be the very first import — patches pyrogram before anything else loads
import bot.patches  # noqa: F401

import asyncio
import logging
import platform
import signal
import sys
import time

import pyrogram
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


async def _cleanup_loop() -> None:
    while True:
        await asyncio.sleep(3600)
        try:
            deleted = await delete_expired_files()
            if deleted:
                log.info("Cleanup sweep: removed %d expired link(s).", deleted)
        except Exception as exc:
            log.warning("Cleanup sweep error: %s", exc)


async def _send_startup_log(pool: ClientPool, chat: object) -> None:
    pairs = Config.api_pairs()
    now = time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime(_BOOT_TIME))
    boot_secs = round(time.time() - _BOOT_TIME, 1)

    text = (
        "🟢 **FileStreamBot Started**\n"
        "─────────────────────────\n"
        f"🤖 **Channel:** {getattr(chat, 'title', '?')} (`{getattr(chat, 'id', '?')}`)\n"
        f"🔑 **API Apps:** {len(pairs)} active\n"
        f"📦 **Chunk size:** {Config.CHUNK_SIZE // 1024} KB\n"
        f"⚡ **Prefetch:** {Config.PREFETCH_CHUNKS} chunks\n"
        f"🌐 **Base URL:** `{Config.BASE_URL}`\n"
        f"🗄 **DB:** `{Config.DB_NAME}`\n"
        f"🔴 **Redis:** {'enabled' if Config.REDIS_URL else 'disabled'}\n"
        f"👑 **Owners:** {len(Config.OWNER_IDS)} configured\n"
        f"🔒 **Upload open:** {'no' if Config.OWNER_ONLY_UPLOAD else 'yes (anyone)'}\n"
        "─────────────────────────\n"
        f"🐍 Python `{platform.python_version()}` · "
        f"Pyrogram `{pyrogram.__version__}`\n"
        f"🖥 `{platform.system()} {platform.release()}`\n"
        f"🕐 `{now}` · booted in {boot_secs}s"
    )

    try:
        await pool.primary().send_message(Config.CHANNEL_ID, text)
        log.info("  Startup log sent to channel.")
    except Exception as exc:
        log.warning("  Could not send startup log: %s", exc)


async def main() -> None:
    Config.validate()

    log.info("Starting FileStreamBot...")
    log.info("  Pyrogram MIN_CHANNEL_ID patched to: %s", pyrogram.utils.MIN_CHANNEL_ID)
    log.info("  API apps configured: %d", len(Config.api_pairs()))
    log.info("  Chunk size: %d KB", Config.CHUNK_SIZE // 1024)
    log.info("  Prefetch chunks: %d", Config.PREFETCH_CHUNKS)

    await ensure_indexes()
    log.info("  MongoDB indexes ready.")

    pool = ClientPool()
    await pool.start()

    try:
        chat = await pool.primary().get_chat(Config.CHANNEL_ID)
        log.info("  Storage channel: %s (id=%s)", chat.title, chat.id)
    except Exception as exc:
        log.error(
            "  Could not access storage channel %s: %s\n"
            "  Make sure the bot is an ADMIN of the channel.",
            Config.CHANNEL_ID, exc
        )
        await pool.stop()
        return

    register_handlers(pool.primary())

    # Give handlers access to client2 for parallel God Speed downloads
    if pool.count >= 2:
        import bot.handlers as _h
        _h._GOD_SPEED_CLIENT2 = pool._clients[1]
        log.info("  God Speed parallel download: 2 clients × 4 workers = 8 connections")

    runner = await start_server(pool)

    await _send_startup_log(pool, chat)

    cleanup_task = asyncio.create_task(_cleanup_loop())

    log.info("Bot is running.")

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
    uvloop.run(main())
