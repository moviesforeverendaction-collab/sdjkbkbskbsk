"""
Telegram bot handlers — full UI with inline buttons.

Commands:
  /start       — welcome + main menu
  /help        — usage guide
  /godspeed    — toggle God Speed mode
  /settings    — user settings panel
  /delete      — owner: remove a link
  /stats       — owner: bot stats
  /cache       — owner: manage god speed cache

God Speed mode:
  When ON  → bot downloads file to Railway disk first, then returns a link
             that serves at Railway's full network speed (no Telegram bottleneck)
  When OFF → standard Telegram streaming (~10 MB/s dual client)
"""

import asyncio
import logging
import time
from pathlib import Path

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

_pending: dict[tuple[int, int], tuple[Message, float]] = {}
_PENDING_TTL = 600


def _is_owner(uid: int) -> bool:
    return uid in Config.OWNER_IDS


def _evict_stale() -> None:
    cutoff = time.monotonic() - _PENDING_TTL
    for k in [k for k, (_, ts) in _pending.items() if ts < cutoff]:
        _pending.pop(k, None)


# ── Keyboard builders ──────────────────────────────────────────────────────────

def _main_menu_kb(god_speed: bool) -> InlineKeyboardMarkup:
    gs_label = "⚡ God Speed: ON" if god_speed else "🐢 God Speed: OFF"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(gs_label, callback_data="toggle_godspeed")],
        [InlineKeyboardButton("📖 How to use", callback_data="show_help"),
         InlineKeyboardButton("⚙️ Settings", callback_data="show_settings")],
    ])


def _expiry_kb(god_speed: bool) -> InlineKeyboardMarkup:
    labels = {
        "1h": "1 Hour",  "6h": "6 Hours",
        "12h": "12 Hours", "1d": "1 Day",
        "3d": "3 Days",  "7d": "7 Days",
        "30d": "30 Days", "never": "♾️ Never",
    }
    buttons, row = [], []
    for key, label in labels.items():
        if key == "never":
            if row:
                buttons.append(row)
            buttons.append([InlineKeyboardButton(label, callback_data=f"exp:{key}")])
            row = []
        else:
            row.append(InlineKeyboardButton(label, callback_data=f"exp:{key}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
    if row:
        buttons.append(row)
    # Mode indicator at bottom
    mode = "⚡ God Speed ON" if god_speed else "🔵 Standard Mode"
    buttons.append([InlineKeyboardButton(f"Mode: {mode}", callback_data="noop")])
    return InlineKeyboardMarkup(buttons)


def _settings_kb(god_speed: bool) -> InlineKeyboardMarkup:
    gs_label = "⚡ God Speed: ON  ✅" if god_speed else "⚡ God Speed: OFF  ❌"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(gs_label, callback_data="toggle_godspeed")],
        [InlineKeyboardButton("🔙 Back", callback_data="show_main")],
    ])


def _format_size(size: int) -> str:
    if size >= 1024 * 1024 * 1024:
        return f"{size / (1024**3):.2f} GB"
    if size >= 1024 * 1024:
        return f"{size / (1024**2):.2f} MB"
    if size >= 1024:
        return f"{size / 1024:.1f} KB"
    return f"{size} B"


def _format_expiry(expires_in, expires_at) -> str:
    if expires_in is None or expires_at is None:
        return "Never expires"
    delta = expires_at - int(time.time())
    if delta <= 0:
        return "Expired"
    h, rem = divmod(delta, 3600)
    m = rem // 60
    if h >= 24:
        return f"Expires in {h // 24}d {h % 24}h"
    return f"Expires in {h}h {m}m"


def register_handlers(client: Client) -> None:

    # ── /start ────────────────────────────────────────────────────────────────
    @client.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, msg: Message) -> None:
        uid = msg.from_user.id
        name = msg.from_user.first_name or "there"
        gs = await database.is_god_speed_enabled(uid)

        await msg.reply_text(
            f"👋 **Hey {name}!**\n\n"
            "Send me any file and I'll give you a blazing fast download link.\n\n"
            "**⚡ God Speed Mode**\n"
            "Toggle this ON to download the file to Railway's servers first. "
            "Then your link serves at Railway's full speed — no Telegram bottleneck.\n\n"
            "**🔵 Standard Mode**\n"
            "Dual-client streaming directly from Telegram (~10-20 MB/s).\n\n"
            f"Current mode: {'⚡ **God Speed ON**' if gs else '🔵 **Standard**'}",
            reply_markup=_main_menu_kb(gs),
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
            "\n\n_Files will be pre-downloaded to Railway for maximum speed._"
            if new_gs else
            "\n\n_Files will stream directly from Telegram (dual-client mode)._"
        )
        await msg.reply_text(
            f"{icon} **God Speed {status}**{note}",
            reply_markup=_settings_kb(new_gs),
        )

    # ── /settings ─────────────────────────────────────────────────────────────
    @client.on_message(filters.command("settings") & filters.private)
    async def cmd_settings(_, msg: Message) -> None:
        uid = msg.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await msg.reply_text(
            "⚙️ **Settings**\n\n"
            f"**God Speed:** {'⚡ ON' if gs else '❌ OFF'}\n"
            "_Toggle to pre-download files to Railway for max speed._",
            reply_markup=_settings_kb(gs),
        )

    # ── /help ─────────────────────────────────────────────────────────────────
    @client.on_message(filters.command("help") & filters.private)
    async def cmd_help(_, msg: Message) -> None:
        await msg.reply_text(
            "📖 **How to use**\n\n"
            "1. Send any file (video, doc, audio, photo)\n"
            "2. Pick how long the link should last\n"
            "3. Share the link — anyone downloads, no login needed\n\n"
            "**⚡ God Speed Mode** (recommended for large files)\n"
            "The bot downloads your file to Railway's disk first, then "
            "serves it at Railway's full bandwidth. Much faster for the "
            "end user. Use `/godspeed` to toggle.\n\n"
            "**🔵 Standard Mode**\n"
            "Dual-client direct Telegram streaming. Faster setup but "
            "download speed capped by Telegram's rate limits.\n\n"
            "**Commands**\n"
            "`/godspeed` — toggle God Speed mode\n"
            "`/settings` — settings panel\n"
            "`/delete <token>` — delete a link (owners)\n"
            "`/stats` — bot status (owners)\n"
            "`/cache` — cache stats (owners)"
        )

    # ── Callback: toggle god speed ────────────────────────────────────────────
    @client.on_callback_query(filters.regex("^toggle_godspeed$"))
    async def cb_toggle_gs(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        new_gs = not gs
        await database.set_god_speed(uid, new_gs)
        icon = "⚡" if new_gs else "🔵"
        status = "ON" if new_gs else "OFF"
        await cb.answer(f"{icon} God Speed {status}", show_alert=False)
        # Refresh the keyboard in context
        try:
            if cb.message.text and "Settings" in cb.message.text:
                await cb.message.edit_reply_markup(_settings_kb(new_gs))
            else:
                await cb.message.edit_reply_markup(_main_menu_kb(new_gs))
        except Exception:
            pass

    @client.on_callback_query(filters.regex("^show_help$"))
    async def cb_help(_, cb: CallbackQuery) -> None:
        await cb.answer()
        await cb.message.reply_text(
            "Send any file → pick expiry → get download link.\n"
            "Use /godspeed for max speed mode."
        )

    @client.on_callback_query(filters.regex("^show_settings$"))
    async def cb_settings(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await cb.answer()
        await cb.message.edit_text(
            "⚙️ **Settings**\n\n"
            f"**God Speed:** {'⚡ ON' if gs else '❌ OFF'}\n"
            "_Toggle to pre-download files to Railway for max speed._",
            reply_markup=_settings_kb(gs),
        )

    @client.on_callback_query(filters.regex("^show_main$"))
    async def cb_main(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await cb.answer()
        await cb.message.edit_text(
            "Send me any file to get a download link.",
            reply_markup=_main_menu_kb(gs),
        )

    @client.on_callback_query(filters.regex("^noop$"))
    async def cb_noop(_, cb: CallbackQuery) -> None:
        await cb.answer()

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
        mode_text = (
            "⚡ **God Speed ON** — file will be pre-downloaded to Railway for max speed."
            if gs else
            "🔵 **Standard Mode** — dual-client Telegram streaming."
        )

        await msg.reply_text(
            f"📁 Got your file!\n\n{mode_text}\n\n"
            "⏱ **How long should the download link last?**",
            reply_markup=_expiry_kb(gs),
        )

    # ── Expiry picked → process ───────────────────────────────────────────────
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
            await cb.answer("Session expired — resend your file.", show_alert=True)
            return

        file_msg, _ = _pending.pop(pending_key)
        gs = await database.is_god_speed_enabled(uid)

        await cb.answer("Processing...")
        status_msg = await cb.message.edit_text("⏳ Forwarding file to storage...")

        try:
            forwarded = await file_msg.forward(Config.CHANNEL_ID)
            fwd_media = (
                forwarded.document or forwarded.video or forwarded.audio
                or forwarded.voice or forwarded.video_note or forwarded.animation
                or forwarded.photo or forwarded.sticker
            )

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
                "1h": "1 Hour",  "6h": "6 Hours", "12h": "12 Hours",
                "1d": "1 Day",   "3d": "3 Days",   "7d": "7 Days",
                "30d": "30 Days", "never": "Never",
            }.get(expiry_key, expiry_key)

            doc = await database.get_file(token)
            expiry_str = _format_expiry(doc.get("expires_in") if doc else expires_in,
                                         doc.get("expires_at") if doc else None)

            link = f"{Config.BASE_URL}/dl/{token}"

            if gs:
                # God Speed: pre-download to Railway disk
                await status_msg.edit_text(
                    f"⚡ **God Speed: Downloading to Railway...**\n\n"
                    f"📄 `{file_name}`\n"
                    f"📦 {_format_size(file_size)}\n\n"
                    "_This may take a moment. The link will be ready once downloaded._"
                )
                # Kick off background cache download
                asyncio.create_task(
                    _god_speed_download(
                        client, forwarded.id, token, file_name,
                        file_size, expiry_label, expiry_str, link, status_msg
                    )
                )
            else:
                # Standard mode: link ready immediately
                await status_msg.edit_text(
                    f"✅ **Link Ready!**\n\n"
                    f"📄 `{file_name}`\n"
                    f"📦 {_format_size(file_size)}\n"
                    f"⏱ {expiry_label}  ({expiry_str})\n"
                    f"🔵 Standard streaming mode\n\n"
                    f"🔗 {link}\n\n"
                    "_Direct download — no account needed_",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⚡ Switch to God Speed", callback_data="toggle_godspeed")
                    ]])
                )

        except Exception as exc:
            log.error("Error processing file for user %s: %s", uid, exc)
            await status_msg.edit_text("❌ Something went wrong. Please try again.")


async def _god_speed_download(
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
    """Background task: download file to Railway disk, update message when done."""
    from bot.stream import download_to_cache

    last_update = time.time()
    start_time = time.time()

    def _progress(done: int, total: int) -> None:
        nonlocal last_update
        now = time.time()
        if now - last_update < 3:  # update every 3s
            return
        last_update = now
        if total > 0:
            pct = done / total * 100
            speed = done / (now - start_time) / (1024 * 1024)
            asyncio.create_task(status_msg.edit_text(
                f"⚡ **God Speed: Caching...**\n\n"
                f"📄 `{file_name}`\n"
                f"📊 {pct:.1f}%  —  {_format_size(done)} / {_format_size(total)}\n"
                f"🚀 {speed:.1f} MB/s\n\n"
                "_Almost ready..._"
            ))

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
        await status_msg.edit_text(
            f"⚡ **God Speed Ready!**\n\n"
            f"📄 `{file_name}`\n"
            f"📦 {_format_size(file_size)}\n"
            f"⏱ {expiry_label}  ({expiry_str})\n"
            f"⚡ Cached in {elapsed:.1f}s — serving at Railway full speed\n\n"
            f"🔗 {link}\n\n"
            "_Direct download at maximum speed — no account needed_",
        )
    except Exception as exc:
        log.error("God Speed cache failed for %s: %s", token, exc)
        await status_msg.edit_text(
            f"⚠️ **God Speed failed — falling back to standard link**\n\n"
            f"📄 `{file_name}`\n"
            f"📦 {_format_size(file_size)}\n"
            f"⏱ {expiry_label}  ({expiry_str})\n\n"
            f"🔗 {link}\n\n"
            "_Standard streaming mode_"
        )


# ── Owner commands ─────────────────────────────────────────────────────────────

def _register_owner_commands(client: Client) -> None:

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

    @client.on_message(filters.command("stats") & filters.private)
    async def cmd_stats(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("⛔ Owners only.")
            return
        db_ok = await database.ping()
        from bot.stream import CACHE_DIR
        cache_files = len(list(CACHE_DIR.glob("*"))) if CACHE_DIR.exists() else 0
        cache_size  = sum(f.stat().st_size for f in CACHE_DIR.glob("*") if f.is_file()) if CACHE_DIR.exists() else 0
        await msg.reply_text(
            "📊 **Bot Stats**\n\n"
            f"DB: {'✅' if db_ok else '❌'}\n"
            f"Base URL: `{Config.BASE_URL}`\n"
            f"API apps: `{len(Config.api_pairs())}`\n"
            f"Chunk size: `{Config.CHUNK_SIZE // 1024} KB`\n"
            f"Prefetch: `{Config.PREFETCH_CHUNKS} chunks`\n\n"
            f"⚡ **God Speed Cache**\n"
            f"Files cached: `{cache_files}`\n"
            f"Cache size: `{_format_size(cache_size)}`"
        )

    @client.on_message(filters.command("cache") & filters.private)
    async def cmd_cache(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("⛔ Owners only.")
            return
        from bot.stream import CACHE_DIR
        if not CACHE_DIR.exists():
            await msg.reply_text("No God Speed cache directory found.")
            return
        files = list(CACHE_DIR.glob("*"))
        total_size = sum(f.stat().st_size for f in files if f.is_file())
        await msg.reply_text(
            f"⚡ **God Speed Cache**\n\n"
            f"Cached files: `{len(files)}`\n"
            f"Total size: `{_format_size(total_size)}`\n\n"
            "Use `/clearcache` to wipe all cached files.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("🗑 Clear All Cache", callback_data="owner_clear_cache")
            ]])
        )

    @client.on_callback_query(filters.regex("^owner_clear_cache$"))
    async def cb_clear_cache(_, cb: CallbackQuery) -> None:
        if not _is_owner(cb.from_user.id):
            await cb.answer("Owners only.", show_alert=True)
            return
        from bot.stream import CACHE_DIR
        cleared = 0
        if CACHE_DIR.exists():
            for f in CACHE_DIR.glob("*"):
                if f.is_file():
                    f.unlink()
                    cleared += 1
        await cb.answer(f"Cleared {cleared} files.", show_alert=True)
        await cb.message.edit_text(f"✅ Cleared {cleared} cached files.")


def register_handlers(client: Client) -> None:
    # All handlers registered above via decorators when this module loads
    _register_owner_commands(client)
