"""
Telegram bot handlers.

ALL handlers live inside the single register_handlers() function.
This is required — Pyrogram registers handlers via decorators that
capture the client reference, so they MUST be defined inside the
function that receives the client argument.

Flow:
  1. User sends file
  2. Bot asks expiry via inline keyboard
  3. User picks expiry
  4. If God Speed ON  → download to disk, then send link
     If God Speed OFF → forward to channel, send link immediately
"""

import asyncio
import logging
import time

from pyrogram import Client, filters
from pyrogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

from bot import database, cache
from bot.config import Config
from bot.database import EXPIRY_OPTIONS

log = logging.getLogger(__name__)

# Pending uploads waiting for expiry selection
# (user_id, msg_id) → (Message, monotonic_timestamp)
_pending: dict[tuple[int, int], tuple[Message, float]] = {}
_PENDING_TTL = 600  # 10 minutes


def _is_owner(uid: int) -> bool:
    return uid in Config.OWNER_IDS


def _evict_stale() -> None:
    cutoff = time.monotonic() - _PENDING_TTL
    stale = [k for k, (_, ts) in _pending.items() if ts < cutoff]
    for k in stale:
        _pending.pop(k, None)


def _fmt_size(b: int) -> str:
    if b >= 1 << 30: return f"{b/(1<<30):.2f} GB"
    if b >= 1 << 20: return f"{b/(1<<20):.2f} MB"
    if b >= 1 << 10: return f"{b/(1<<10):.1f} KB"
    return f"{b} B"


def _fmt_expiry(expires_in, expires_at) -> str:
    if not expires_in or not expires_at:
        return "Never expires"
    delta = expires_at - int(time.time())
    if delta <= 0:
        return "Expired"
    h, r = divmod(delta, 3600)
    m = r // 60
    if h >= 24:
        return f"Expires in {h//24}d {h%24}h"
    return f"Expires in {h}h {m}m"


def _main_kb(gs: bool) -> InlineKeyboardMarkup:
    icon = "⚡ God Speed: ON  ✅" if gs else "🔵 God Speed: OFF  ❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(icon, callback_data="toggle_gs")],
        [
            InlineKeyboardButton("📖 Help", callback_data="show_help"),
            InlineKeyboardButton("⚙️ Settings", callback_data="show_settings"),
        ],
    ])


def _expiry_kb(gs: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton("1 Hour",   callback_data="exp:1h"),
            InlineKeyboardButton("6 Hours",  callback_data="exp:6h"),
        ],
        [
            InlineKeyboardButton("12 Hours", callback_data="exp:12h"),
            InlineKeyboardButton("1 Day",    callback_data="exp:1d"),
        ],
        [
            InlineKeyboardButton("3 Days",   callback_data="exp:3d"),
            InlineKeyboardButton("7 Days",   callback_data="exp:7d"),
        ],
        [InlineKeyboardButton("30 Days",     callback_data="exp:30d")],
        [InlineKeyboardButton("♾️ Never",   callback_data="exp:never")],
        [InlineKeyboardButton(
            f"Mode: {'⚡ God Speed' if gs else '🔵 Standard'}",
            callback_data="toggle_gs_expiry"
        )],
    ]
    return InlineKeyboardMarkup(rows)


def _settings_kb(gs: bool) -> InlineKeyboardMarkup:
    icon = "⚡ God Speed: ON  ✅" if gs else "⚡ God Speed: OFF  ❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(icon, callback_data="toggle_gs")],
        [InlineKeyboardButton("🔙 Back", callback_data="show_main")],
    ])


def register_handlers(client: Client) -> None:
    """Register ALL bot handlers. Must be called once after client.start()."""

    # ── /start ────────────────────────────────────────────────────────────────
    @client.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, msg: Message) -> None:
        uid = msg.from_user.id
        name = msg.from_user.first_name or "there"
        gs = await database.is_god_speed_enabled(uid)
        mode = "⚡ **God Speed ON**" if gs else "🔵 **Standard Mode**"
        await msg.reply_text(
            f"👋 **Hey {name}!**\n\n"
            "Send me any file — I'll give you a blazing fast download link.\n\n"
            f"Current mode: {mode}\n\n"
            "**⚡ God Speed** — pre-downloads to Railway, serves at full bandwidth\n"
            "**🔵 Standard** — streams directly from Telegram (~10-20 MB/s)",
            reply_markup=_main_kb(gs),
        )

    # ── /help ─────────────────────────────────────────────────────────────────
    @client.on_message(filters.command("help") & filters.private)
    async def cmd_help(_, msg: Message) -> None:
        await msg.reply_text(
            "📖 **How to use**\n\n"
            "1. Send any file (video, doc, audio, photo)\n"
            "2. Choose how long the link lasts\n"
            "3. Share the link — anyone can download, no login needed\n\n"
            "**⚡ God Speed Mode**\n"
            "Bot downloads your file to Railway first, then serves it at "
            "Railway's full speed. Best for large files.\n\n"
            "**🔵 Standard Mode**\n"
            "Dual-client Telegram streaming. Fast but capped by Telegram.\n\n"
            "**Commands**\n"
            "`/godspeed` — toggle God Speed\n"
            "`/settings` — settings panel\n"
            "`/delete <token>` — delete a link (owners)\n"
            "`/stats` — bot status (owners)\n"
            "`/cache` — God Speed cache info (owners)"
        )

    # ── /godspeed ─────────────────────────────────────────────────────────────
    @client.on_message(filters.command("godspeed") & filters.private)
    async def cmd_godspeed(_, msg: Message) -> None:
        uid = msg.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        new_gs = not gs
        await database.set_god_speed(uid, new_gs)
        icon = "⚡" if new_gs else "🔵"
        status = "ON" if new_gs else "OFF"
        note = (
            "_Files will be pre-cached to Railway disk before link is sent._"
            if new_gs else
            "_Files will stream directly from Telegram (dual-client)._"
        )
        await msg.reply_text(
            f"{icon} **God Speed {status}**\n\n{note}",
            reply_markup=_settings_kb(new_gs),
        )

    # ── /settings ─────────────────────────────────────────────────────────────
    @client.on_message(filters.command("settings") & filters.private)
    async def cmd_settings(_, msg: Message) -> None:
        uid = msg.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await msg.reply_text(
            "⚙️ **Settings**\n\n"
            f"God Speed: {'⚡ ON' if gs else '❌ OFF'}",
            reply_markup=_settings_kb(gs),
        )

    # ── /delete (owner) ───────────────────────────────────────────────────────
    @client.on_message(filters.command("delete") & filters.private)
    async def cmd_delete(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("⛔ Owners only.")
            return
        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2:
            await msg.reply_text("Usage: `/delete <token>`")
            return
        token = parts[1].strip()
        deleted = await database.delete_file(token)
        await cache.cache_delete(token)
        from bot.stream import clear_cache
        clear_cache(token)
        await msg.reply_text(f"{'✅ Deleted' if deleted else '❌ Not found'}: `{token}`")

    # ── /stats (owner) ────────────────────────────────────────────────────────
    @client.on_message(filters.command("stats") & filters.private)
    async def cmd_stats(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("⛔ Owners only.")
            return
        db_ok = await database.ping()
        from bot.stream import get_cache_stats
        n_files, total_bytes = get_cache_stats()
        await msg.reply_text(
            "📊 **Bot Stats**\n\n"
            f"DB: {'✅ connected' if db_ok else '❌ DOWN'}\n"
            f"Base URL: `{Config.BASE_URL}`\n"
            f"API apps: `{len(Config.api_pairs())}`\n"
            f"Chunk size: `{Config.CHUNK_SIZE // 1024} KB`\n"
            f"Prefetch: `{Config.PREFETCH_CHUNKS} chunks`\n\n"
            f"⚡ **God Speed Cache**\n"
            f"Files: `{n_files}`\n"
            f"Size: `{_fmt_size(total_bytes)}`"
        )

    # ── /cache (owner) ────────────────────────────────────────────────────────
    @client.on_message(filters.command("cache") & filters.private)
    async def cmd_cache(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("⛔ Owners only.")
            return
        from bot.stream import get_cache_stats
        n_files, total_bytes = get_cache_stats()
        await msg.reply_text(
            f"⚡ **God Speed Cache**\n\n"
            f"Files: `{n_files}`\n"
            f"Size: `{_fmt_size(total_bytes)}`",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Clear All", callback_data="clear_cache")
            ]])
        )

    # ── Callbacks ─────────────────────────────────────────────────────────────

    @client.on_callback_query(filters.regex("^toggle_gs$"))
    async def cb_toggle_gs(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        new_gs = not gs
        await database.set_god_speed(uid, new_gs)
        icon = "⚡" if new_gs else "🔵"
        await cb.answer(f"{icon} God Speed {'ON' if new_gs else 'OFF'}")
        try:
            # Try to refresh the keyboard in-place
            text = cb.message.text or ""
            if "Settings" in text:
                await cb.message.edit_reply_markup(_settings_kb(new_gs))
            else:
                await cb.message.edit_reply_markup(_main_kb(new_gs))
        except Exception:
            pass

    @client.on_callback_query(filters.regex("^toggle_gs_expiry$"))
    async def cb_toggle_gs_expiry(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        new_gs = not gs
        await database.set_god_speed(uid, new_gs)
        icon = "⚡" if new_gs else "🔵"
        await cb.answer(f"{icon} God Speed {'ON' if new_gs else 'OFF'}")
        try:
            await cb.message.edit_reply_markup(_expiry_kb(new_gs))
        except Exception:
            pass

    @client.on_callback_query(filters.regex("^show_help$"))
    async def cb_show_help(_, cb: CallbackQuery) -> None:
        await cb.answer()
        await cb.message.reply_text(
            "Send any file → pick expiry → get link.\n"
            "Use /godspeed to toggle max speed mode."
        )

    @client.on_callback_query(filters.regex("^show_settings$"))
    async def cb_show_settings(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await cb.answer()
        await cb.message.edit_text(
            "⚙️ **Settings**\n\n"
            f"God Speed: {'⚡ ON' if gs else '❌ OFF'}",
            reply_markup=_settings_kb(gs),
        )

    @client.on_callback_query(filters.regex("^show_main$"))
    async def cb_show_main(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await cb.answer()
        await cb.message.edit_text(
            "Send me any file to get a download link.",
            reply_markup=_main_kb(gs),
        )

    @client.on_callback_query(filters.regex("^clear_cache$"))
    async def cb_clear_cache(_, cb: CallbackQuery) -> None:
        if not _is_owner(cb.from_user.id):
            await cb.answer("Owners only.", show_alert=True)
            return
        from bot.stream import _cache_dir
        cleared = 0
        d = _cache_dir()
        for f in d.glob("*"):
            if f.is_file():
                f.unlink()
                cleared += 1
        await cb.answer(f"Cleared {cleared} cached files.", show_alert=True)
        await cb.message.edit_text(f"✅ Cleared {cleared} cached files from God Speed cache.")

    # ── File received → ask expiry ────────────────────────────────────────────

    @client.on_message(filters.private & filters.media)
    async def handle_media(_, msg: Message) -> None:
        uid = msg.from_user.id
        if Config.OWNER_ONLY_UPLOAD and not _is_owner(uid):
            await msg.reply_text("⛔ Only the bot owner can upload files.")
            return
        _evict_stale()
        _pending[(uid, msg.id)] = (msg, time.monotonic())
        gs = await database.is_god_speed_enabled(uid)
        mode = "⚡ God Speed — will pre-cache to Railway" if gs else "🔵 Standard — dual Telegram stream"
        await msg.reply_text(
            f"📁 **File received!**\n\nMode: {mode}\n\n"
            "⏱ **How long should the link last?**",
            reply_markup=_expiry_kb(gs),
        )

    # ── Expiry picked → generate link ─────────────────────────────────────────

    @client.on_callback_query(filters.regex(r"^exp:(.+)$"))
    async def handle_expiry(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        expiry_key = cb.matches[0].group(1)

        if expiry_key not in EXPIRY_OPTIONS:
            await cb.answer("Invalid option.", show_alert=True)
            return

        expires_in = EXPIRY_OPTIONS[expiry_key]
        _evict_stale()

        pending_key = None
        for (u, m) in sorted(_pending.keys(), reverse=True):
            if u == uid:
                pending_key = (u, m)
                break

        if pending_key is None:
            await cb.answer("Session expired — please resend your file.", show_alert=True)
            return

        file_msg, _ = _pending.pop(pending_key)
        gs = await database.is_god_speed_enabled(uid)

        await cb.answer("Processing...")
        status = await cb.message.edit_text("⏳ Forwarding to storage...")

        try:
            forwarded = await file_msg.forward(Config.CHANNEL_ID)
            fwd_media = (
                forwarded.document or forwarded.video or forwarded.audio
                or forwarded.voice or forwarded.video_note or forwarded.animation
                or forwarded.photo or forwarded.sticker
            )

            if fwd_media is None:
                await status.edit_text("❌ Could not read file from Telegram. Try again.")
                return

            file_id        = fwd_media.file_id
            file_unique_id = fwd_media.file_unique_id
            file_size      = getattr(fwd_media, "file_size", 0) or 0
            mime_type      = getattr(fwd_media, "mime_type", "application/octet-stream") or "application/octet-stream"
            file_name      = (
                getattr(fwd_media, "file_name", None)
                or getattr(fwd_media, "title", None)
                or f"file_{forwarded.id}"
            )

            token = await database.save_file(
                file_id=file_id, file_unique_id=file_unique_id,
                file_name=file_name, file_size=file_size,
                mime_type=mime_type, message_id=forwarded.id,
                uploader_id=uid, expires_in=expires_in,
            )

            expiry_label = {
                "1h": "1 Hour",   "6h": "6 Hours",  "12h": "12 Hours",
                "1d": "1 Day",    "3d": "3 Days",    "7d":  "7 Days",
                "30d": "30 Days", "never": "Never",
            }.get(expiry_key, expiry_key)

            doc = await database.get_file(token)
            expiry_str = _fmt_expiry(
                doc.get("expires_in") if doc else expires_in,
                doc.get("expires_at") if doc else None,
            )

            link = f"{Config.BASE_URL}/dl/{token}"

            if gs:
                # God Speed: show status, kick off background download
                await status.edit_text(
                    f"⚡ **God Speed: Caching to Railway...**\n\n"
                    f"📄 `{file_name}`\n"
                    f"📦 {_fmt_size(file_size)}\n\n"
                    "_Downloading to Railway disk. Link ready when done._"
                )
                asyncio.create_task(_god_speed_task(
                    client, forwarded.id, token, file_name,
                    file_size, expiry_label, expiry_str, link, status
                ))
            else:
                # Standard: link is ready now
                await status.edit_text(
                    f"✅ **Link Ready!**\n\n"
                    f"📄 `{file_name}`\n"
                    f"📦 {_fmt_size(file_size)}\n"
                    f"⏱ {expiry_label} ({expiry_str})\n"
                    f"🔵 Standard streaming\n\n"
                    f"🔗 {link}\n\n"
                    "_Direct download — no account needed_",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⚡ Try God Speed next time", callback_data="toggle_gs")
                    ]])
                )

        except Exception as exc:
            log.error("File processing error user=%s: %s", uid, exc)
            await status.edit_text("❌ Something went wrong. Please try again.")


async def _god_speed_task(
    client: Client,
    message_id: int,
    token: str,
    file_name: str,
    file_size: int,
    expiry_label: str,
    expiry_str: str,
    link: str,
    status_msg,
) -> None:
    """Background task: download to disk, then update the message with the link."""
    from bot.stream import download_to_cache

    start_time = time.time()
    last_edit  = [0.0]   # use list so inner async func can mutate it

    async def _progress(done: int, total: int) -> None:
        now = time.time()
        if now - last_edit[0] < 3.0:
            return
        last_edit[0] = now
        pct   = (done / total * 100) if total > 0 else 0
        speed = done / max(now - start_time, 0.001) / (1024 * 1024)
        try:
            await status_msg.edit_text(
                f"⚡ **God Speed: Caching...**\n\n"
                f"📄 `{file_name}`\n"
                f"📊 {pct:.1f}%  —  {_fmt_size(done)} / {_fmt_size(total)}\n"
                f"🚀 {speed:.1f} MB/s (Railway download)\n\n"
                "_Almost ready..._"
            )
        except Exception:
            pass  # edit might fail if message was deleted

    try:
        await download_to_cache(
            client=client,
            message_id=message_id,
            channel_id=Config.CHANNEL_ID,
            token=token,
            progress_cb=_progress,
        )
        await database.mark_god_speed_ready(token)

        elapsed = time.time() - start_time
        speed   = file_size / max(elapsed, 0.001) / (1024 * 1024)

        await status_msg.edit_text(
            f"⚡ **God Speed Ready!**\n\n"
            f"📄 `{file_name}`\n"
            f"📦 {_fmt_size(file_size)}\n"
            f"⏱ {expiry_label} ({expiry_str})\n"
            f"🚀 Cached in {elapsed:.1f}s at {speed:.1f} MB/s\n\n"
            f"🔗 {link}\n\n"
            "_Serving at Railway full speed — no account needed_"
        )

    except Exception as exc:
        log.error("God Speed task failed token=%s: %s", token, exc)
        # Fall back to standard link
        try:
            await status_msg.edit_text(
                f"⚠️ **God Speed failed — standard link ready**\n\n"
                f"📄 `{file_name}`\n"
                f"📦 {_fmt_size(file_size)}\n"
                f"⏱ {expiry_label} ({expiry_str})\n\n"
                f"🔗 {link}\n\n"
                "_Standard streaming mode_"
            )
        except Exception:
            pass
