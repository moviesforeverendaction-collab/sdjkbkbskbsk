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

_pending: dict[tuple[int, int], tuple[Message, float]] = {}
_PENDING_TTL = 600

# Set by __main__ after pool starts
_GOD_SPEED_CLIENT2 = None


def _is_owner(uid: int) -> bool:
    return uid in Config.OWNER_IDS


def _evict_stale() -> None:
    cutoff = time.monotonic() - _PENDING_TTL
    for k in [k for k, (_, ts) in _pending.items() if ts < cutoff]:
        _pending.pop(k, None)


def _sz(b: int) -> str:
    if b >= 1 << 30: return f"{b/(1<<30):.1f} GB"
    if b >= 1 << 20: return f"{b/(1<<20):.1f} MB"
    if b >= 1 << 10: return f"{b/(1<<10):.0f} KB"
    return f"{b} B"


def _expiry_str(expires_in, expires_at) -> str:
    if not expires_in or not expires_at:
        return "never"
    d = expires_at - int(time.time())
    if d <= 0:
        return "expired"
    h, r = divmod(d, 3600)
    if h >= 24:
        return f"{h//24}d {h%24}h"
    return f"{h}h {r//60}m"


# ── Keyboards ─────────────────────────────────────────────────────────────────

def _main_kb(gs: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "⚡ God Speed  ON" if gs else "🔵 God Speed  OFF",
            callback_data="toggle_gs"
        )],
        [
            InlineKeyboardButton("📖 Help", callback_data="show_help"),
            InlineKeyboardButton("⚙️ Settings", callback_data="show_settings"),
        ],
    ])


def _expiry_kb(gs: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1 hour",   callback_data="exp:1h"),
            InlineKeyboardButton("6 hours",  callback_data="exp:6h"),
            InlineKeyboardButton("12 hours", callback_data="exp:12h"),
        ],
        [
            InlineKeyboardButton("1 day",    callback_data="exp:1d"),
            InlineKeyboardButton("3 days",   callback_data="exp:3d"),
            InlineKeyboardButton("7 days",   callback_data="exp:7d"),
        ],
        [
            InlineKeyboardButton("30 days",  callback_data="exp:30d"),
            InlineKeyboardButton("Never",    callback_data="exp:never"),
        ],
        [InlineKeyboardButton(
            "⚡ God Speed on  —  tap to turn off" if gs else "🔵 Standard mode  —  tap for God Speed",
            callback_data="toggle_gs_expiry"
        )],
    ])


def _settings_kb(gs: bool) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(
            "⚡ God Speed is ON  —  tap to turn off" if gs else "🔵 God Speed is OFF  —  tap to turn on",
            callback_data="toggle_gs"
        )],
        [InlineKeyboardButton("← Back", callback_data="show_main")],
    ])


# ── Register all handlers ─────────────────────────────────────────────────────

def register_handlers(client: Client) -> None:

    @client.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, msg: Message) -> None:
        uid = msg.from_user.id
        name = msg.from_user.first_name or "there"
        gs = await database.is_god_speed_enabled(uid)
        await msg.reply_text(
            f"Hey {name} 👋\n\n"
            "Send me any file and I'll give you a direct download link "
            "you can share with anyone — no login, no waiting.\n\n"
            f"You're on {'⚡ God Speed' if gs else '🔵 Standard'} mode right now.\n\n"
            "God Speed downloads the file to my server first, then serves it "
            "at full speed. Standard streams directly from Telegram.",
            reply_markup=_main_kb(gs),
        )

    @client.on_message(filters.command("help") & filters.private)
    async def cmd_help(_, msg: Message) -> None:
        await msg.reply_text(
            "Here's how it works:\n\n"
            "1. Send me a file\n"
            "2. Pick how long the link should stay alive\n"
            "3. Share the link — anyone can download it directly\n\n"
            "God Speed mode pre-downloads your file to Railway servers. "
            "Once it's there, downloads go out at full bandwidth — "
            "easily 50-100 MB/s for your users. Regular mode streams "
            "straight from Telegram, which tops out around 10-15 MB/s.\n\n"
            "Commands you can use:\n"
            "/godspeed — toggle God Speed on or off\n"
            "/settings — open settings\n"
            "/delete <token> — remove a link (owner only)\n"
            "/stats — bot info (owner only)"
        )

    @client.on_message(filters.command("godspeed") & filters.private)
    async def cmd_godspeed(_, msg: Message) -> None:
        uid = msg.from_user.id
        gs = not await database.is_god_speed_enabled(uid)
        await database.set_god_speed(uid, gs)
        if gs:
            await msg.reply_text(
                "⚡ God Speed is now ON.\n\n"
                "Your next file will be downloaded to Railway first. "
                "It takes a minute or two depending on size, but then "
                "anyone downloading it gets full speed.",
                reply_markup=_settings_kb(gs),
            )
        else:
            await msg.reply_text(
                "🔵 God Speed is now OFF.\n\n"
                "Files stream directly from Telegram from now on.",
                reply_markup=_settings_kb(gs),
            )

    @client.on_message(filters.command("settings") & filters.private)
    async def cmd_settings(_, msg: Message) -> None:
        uid = msg.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await msg.reply_text(
            f"Settings\n\nGod Speed: {'ON ⚡' if gs else 'OFF 🔵'}",
            reply_markup=_settings_kb(gs),
        )

    @client.on_message(filters.command("delete") & filters.private)
    async def cmd_delete(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("You need to be an owner to delete links.")
            return
        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2:
            await msg.reply_text("Usage: /delete <token>")
            return
        token = parts[1].strip()
        deleted = await database.delete_file(token)
        await cache.cache_delete(token)
        from bot.stream import clear_cache
        clear_cache(token)
        await msg.reply_text(
            f"Done, link {token} is gone." if deleted
            else f"Couldn't find a link with token {token}."
        )

    @client.on_message(filters.command("stats") & filters.private)
    async def cmd_stats(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("Owner only command.")
            return
        db_ok = await database.ping()
        from bot.stream import get_cache_stats
        n, total = get_cache_stats()
        await msg.reply_text(
            f"Bot status\n\n"
            f"Database: {'connected' if db_ok else 'DOWN'}\n"
            f"URL: {Config.BASE_URL}\n"
            f"API apps: {len(Config.api_pairs())}\n"
            f"God Speed workers: {len(Config.api_pairs()) * 8} parallel connections\n"
            f"Prefetch: {Config.PREFETCH_CHUNKS} chunks\n\n"
            f"God Speed cache: {n} files ({_sz(total)})"
        )

    @client.on_message(filters.command("cache") & filters.private)
    async def cmd_cache(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("Owner only command.")
            return
        from bot.stream import get_cache_stats
        n, total = get_cache_stats()
        await msg.reply_text(
            f"God Speed cache\n\n{n} files using {_sz(total)}",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("Clear all cached files", callback_data="clear_cache")
            ]])
        )

    # ── Callbacks ─────────────────────────────────────────────────────────────

    @client.on_callback_query(filters.regex("^toggle_gs$"))
    async def cb_toggle_gs(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = not await database.is_god_speed_enabled(uid)
        await database.set_god_speed(uid, gs)
        await cb.answer("⚡ God Speed ON" if gs else "🔵 Standard mode")
        try:
            if cb.message.text and "Settings" in cb.message.text:
                await cb.message.edit_reply_markup(_settings_kb(gs))
            else:
                await cb.message.edit_reply_markup(_main_kb(gs))
        except Exception:
            pass

    @client.on_callback_query(filters.regex("^toggle_gs_expiry$"))
    async def cb_toggle_gs_expiry(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = not await database.is_god_speed_enabled(uid)
        await database.set_god_speed(uid, gs)
        await cb.answer("⚡ God Speed ON" if gs else "🔵 Standard mode")
        try:
            await cb.message.edit_reply_markup(_expiry_kb(gs))
        except Exception:
            pass

    @client.on_callback_query(filters.regex("^show_help$"))
    async def cb_show_help(_, cb: CallbackQuery) -> None:
        await cb.answer()
        await cb.message.reply_text(
            "Send me any file, pick how long the link should last, "
            "and share it. Use /godspeed for maximum speed."
        )

    @client.on_callback_query(filters.regex("^show_settings$"))
    async def cb_show_settings(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await cb.answer()
        await cb.message.edit_text(
            f"Settings\n\nGod Speed: {'ON ⚡' if gs else 'OFF 🔵'}",
            reply_markup=_settings_kb(gs),
        )

    @client.on_callback_query(filters.regex("^show_main$"))
    async def cb_show_main(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        gs = await database.is_god_speed_enabled(uid)
        await cb.answer()
        await cb.message.edit_text(
            "Send me a file to get a download link.",
            reply_markup=_main_kb(gs),
        )

    @client.on_callback_query(filters.regex("^clear_cache$"))
    async def cb_clear_cache(_, cb: CallbackQuery) -> None:
        if not _is_owner(cb.from_user.id):
            await cb.answer("Owner only.", show_alert=True)
            return
        from bot.stream import _cache_dir
        cleared = 0
        for f in _cache_dir().glob("*"):
            if f.is_file():
                f.unlink()
                cleared += 1
        await cb.answer(f"Cleared {cleared} files.", show_alert=True)
        await cb.message.edit_text(f"Done. Removed {cleared} cached files.")

    # ── File received ──────────────────────────────────────────────────────────

    @client.on_message(filters.private & filters.media)
    async def handle_media(_, msg: Message) -> None:
        uid = msg.from_user.id
        if Config.OWNER_ONLY_UPLOAD and not _is_owner(uid):
            await msg.reply_text("Only the bot owner can upload files right now.")
            return
        _evict_stale()
        _pending[(uid, msg.id)] = (msg, time.monotonic())
        gs = await database.is_god_speed_enabled(uid)
        await msg.reply_text(
            f"Got it. How long should the link stay active?\n\n"
            f"{'⚡ God Speed is on — file will be cached to Railway first.' if gs else '🔵 Standard mode — link is ready instantly.'}",
            reply_markup=_expiry_kb(gs),
        )

    # ── Expiry picked ──────────────────────────────────────────────────────────

    @client.on_callback_query(filters.regex(r"^exp:(.+)$"))
    async def handle_expiry(_, cb: CallbackQuery) -> None:
        uid = cb.from_user.id
        key = cb.matches[0].group(1)

        if key not in EXPIRY_OPTIONS:
            await cb.answer("Invalid option.", show_alert=True)
            return

        expires_in = EXPIRY_OPTIONS[key]
        _evict_stale()

        pending_key = None
        for (u, m) in sorted(_pending.keys(), reverse=True):
            if u == uid:
                pending_key = (u, m)
                break

        if pending_key is None:
            await cb.answer("Session timed out — please resend your file.", show_alert=True)
            return

        file_msg, _ = _pending.pop(pending_key)
        gs = await database.is_god_speed_enabled(uid)

        await cb.answer()
        status = await cb.message.edit_text("Forwarding to storage...")

        try:
            forwarded = await file_msg.forward(Config.CHANNEL_ID)
            fwd_media = (
                forwarded.document or forwarded.video or forwarded.audio
                or forwarded.voice or forwarded.video_note or forwarded.animation
                or forwarded.photo or forwarded.sticker
            )

            if fwd_media is None:
                await status.edit_text("Couldn't read the file from Telegram. Please try again.")
                return

            file_size = getattr(fwd_media, "file_size", 0) or 0
            file_name = (
                getattr(fwd_media, "file_name", None)
                or getattr(fwd_media, "title", None)
                or f"file_{forwarded.id}"
            )
            mime_type = getattr(fwd_media, "mime_type", "application/octet-stream") or "application/octet-stream"

            token = await database.save_file(
                file_id=fwd_media.file_id,
                file_unique_id=fwd_media.file_unique_id,
                file_name=file_name,
                file_size=file_size,
                mime_type=mime_type,
                message_id=forwarded.id,
                uploader_id=uid,
                expires_in=expires_in,
            )

            expiry_labels = {
                "1h": "1 hour", "6h": "6 hours", "12h": "12 hours",
                "1d": "1 day",  "3d": "3 days",   "7d": "7 days",
                "30d": "30 days", "never": "never",
            }
            exp_label = expiry_labels.get(key, key)
            link = f"{Config.BASE_URL}/dl/{token}"

            if gs:
                await status.edit_text(
                    f"Downloading to Railway... 0%\n\n"
                    f"{file_name}  ·  {_sz(file_size)}\n"
                    f"Hang tight, this makes your users' downloads super fast."
                )
                asyncio.create_task(_god_speed_task(
                    client, forwarded.id, token,
                    file_name, file_size, exp_label, link, status
                ))
            else:
                doc = await database.get_file(token)
                exp_str = _expiry_str(
                    doc.get("expires_in") if doc else expires_in,
                    doc.get("expires_at") if doc else None,
                )
                await status.edit_text(
                    f"Here's your link 🔗\n\n"
                    f"{link}\n\n"
                    f"{file_name}\n"
                    f"{_sz(file_size)}  ·  expires in {exp_str}\n\n"
                    f"Anyone can download this directly, no account needed.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("⚡ Use God Speed next time", callback_data="toggle_gs")
                    ]])
                )

        except Exception as exc:
            log.error("File processing error uid=%s: %s", uid, exc)
            await status.edit_text("Something went wrong. Please try again.")


async def _god_speed_task(
    client: Client,
    message_id: int,
    token: str,
    file_name: str,
    file_size: int,
    exp_label: str,
    link: str,
    status_msg,
) -> None:
    from bot.stream import download_to_cache

    start = time.time()
    last_edit = [0.0]

    async def _progress(done: int, total: int) -> None:
        now = time.time()
        if now - last_edit[0] < 2.5:
            return
        last_edit[0] = now
        elapsed = max(now - start, 0.001)
        speed = done / elapsed / (1 << 20)
        pct = (done / total * 100) if total > 0 else 0
        eta_s = int((total - done) / (done / elapsed)) if done > 0 and total > 0 else 0
        eta = f"{eta_s//60}m {eta_s%60}s" if eta_s >= 60 else f"{eta_s}s"
        try:
            await status_msg.edit_text(
                f"Downloading to Railway...  {pct:.0f}%\n\n"
                f"{file_name}  ·  {_sz(done)} of {_sz(total)}\n"
                f"{speed:.1f} MB/s  ·  {eta} left"
            )
        except Exception:
            pass

    try:
        await download_to_cache(
            client=client,
            message_id=message_id,
            channel_id=Config.CHANNEL_ID,
            token=token,
            progress_cb=_progress,
            client2=_GOD_SPEED_CLIENT2,
        )
        await database.mark_god_speed_ready(token)

        elapsed = time.time() - start
        avg_speed = file_size / max(elapsed, 0.001) / (1 << 20)

        await status_msg.edit_text(
            f"Ready — here's your link ⚡\n\n"
            f"{link}\n\n"
            f"{file_name}  ·  {_sz(file_size)}\n"
            f"Expires in {exp_label}  ·  cached in {elapsed:.0f}s at {avg_speed:.0f} MB/s\n\n"
            f"Downloads go out at full Railway speed now."
        )
    except Exception as exc:
        log.error("God Speed task failed token=%s: %s", token, exc)
        try:
            await status_msg.edit_text(
                f"God Speed failed, but here's a standard link:\n\n"
                f"{link}\n\n"
                f"{file_name}  ·  {_sz(file_size)}  ·  expires in {exp_label}"
            )
        except Exception:
            pass
