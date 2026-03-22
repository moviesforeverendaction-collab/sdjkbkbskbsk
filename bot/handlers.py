"""
Telegram bot handlers.

Flow when user sends a file:
  1. Bot receives file
  2. Bot asks: "How long should this link last?" (inline keyboard)
  3. User picks expiry
  4. Bot forwards file to storage channel, saves metadata, replies with link

Commands:
  /start   - welcome
  /help    - usage
  /delete  - owner: remove a link by token
  /stats   - owner: bot status
"""

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

# Pending uploads: (user_id, file_msg_id) -> (Message, timestamp)
# Evicted if user never picks an expiry within 10 minutes.
_pending: dict[tuple[int, int], tuple[Message, float]] = {}
_PENDING_TTL = 600  # 10 minutes


def _evict_stale_pending() -> None:
    """Remove pending entries older than _PENDING_TTL."""
    cutoff = time.monotonic() - _PENDING_TTL
    stale = [k for k, (_, ts) in _pending.items() if ts < cutoff]
    for k in stale:
        _pending.pop(k, None)


def _is_owner(user_id: int) -> bool:
    return user_id in Config.OWNER_IDS


def _expiry_keyboard() -> InlineKeyboardMarkup:
    """Build the inline keyboard for expiry selection."""
    labels = {
        "1h":    "1 Hour",
        "6h":    "6 Hours",
        "12h":   "12 Hours",
        "1d":    "1 Day",
        "3d":    "3 Days",
        "7d":    "7 Days",
        "30d":   "30 Days",
        "never": "Never expires",
    }
    buttons = []
    row = []
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
    return InlineKeyboardMarkup(buttons)


def _format_expiry(expires_in, expires_at) -> str:
    if expires_in is None or expires_at is None:
        return "Never expires"
    delta = expires_at - int(time.time())
    if delta <= 0:
        return "Expired"
    h, rem = divmod(delta, 3600)
    m = rem // 60
    if h >= 24:
        d = h // 24
        return f"Expires in {d}d {h % 24}h"
    return f"Expires in {h}h {m}m"


def register_handlers(client: Client) -> None:

    @client.on_message(filters.command("start") & filters.private)
    async def cmd_start(_, msg: Message) -> None:
        name = msg.from_user.first_name or "there"
        await msg.reply_text(
            f"Hi {name}!\n\n"
            "Send me any file and I will give you a fast direct download link.\n"
            "You choose how long the link stays alive — from 1 hour to forever.\n"
            "The link and its file are deleted automatically when it expires.\n\n"
            "Use /help to see all commands."
        )

    @client.on_message(filters.command("help") & filters.private)
    async def cmd_help(_, msg: Message) -> None:
        await msg.reply_text(
            "How to use:\n"
            "1. Send any file (video, document, audio, photo)\n"
            "2. Pick how long the link should last\n"
            "3. Share the link — anyone can download, no account needed\n\n"
            "The link expires and the file is removed automatically.\n\n"
            "Commands:\n"
            "/delete <token> — remove a link now (owners only)\n"
            "/stats — bot status (owners only)"
        )

    # ── Step 1: File received → ask for expiry ─────────────────────────────
    @client.on_message(filters.private & filters.media)
    async def handle_media(_, msg: Message) -> None:
        user_id = msg.from_user.id

        if Config.OWNER_ONLY_UPLOAD and not _is_owner(user_id):
            await msg.reply_text("Only the bot owner can upload files.")
            return

        # Evict any entries the user never acted on before storing the new one
        _evict_stale_pending()
        _pending[(user_id, msg.id)] = (msg, time.monotonic())

        await msg.reply_text(
            "How long should this download link last?\n\n"
            "The link and the file will be automatically deleted when it expires.",
            reply_markup=_expiry_keyboard(),
        )

    # ── Step 2: User picks expiry → forward + generate link ───────────────
    @client.on_callback_query(filters.regex(r"^exp:(.+)$"))
    async def handle_expiry_pick(_, cb: CallbackQuery) -> None:
        user_id = cb.from_user.id
        expiry_key = cb.matches[0].group(1)

        if expiry_key not in EXPIRY_OPTIONS:
            await cb.answer("Invalid option.", show_alert=True)
            return

        expires_in = EXPIRY_OPTIONS[expiry_key]

        # Find the most recent pending file for this user
        _evict_stale_pending()
        pending_key = None
        for (uid, mid) in sorted(_pending.keys(), reverse=True):
            if uid == user_id:
                pending_key = (uid, mid)
                break

        if pending_key is None:
            await cb.answer(
                "Session expired — please resend your file.", show_alert=True
            )
            return

        file_msg, _ = _pending.pop(pending_key)

        await cb.answer("Generating your link...")
        await cb.message.edit_text("Processing...")

        try:
            forwarded = await file_msg.forward(Config.CHANNEL_ID)

            fwd_media = (
                forwarded.document
                or forwarded.video
                or forwarded.audio
                or forwarded.voice
                or forwarded.video_note
                or forwarded.animation
                or forwarded.photo
                or forwarded.sticker
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
                file_id=file_id,
                file_unique_id=file_unique_id,
                file_name=file_name,
                file_size=file_size,
                mime_type=mime_type,
                message_id=forwarded.id,
                uploader_id=user_id,
                expires_in=expires_in,
            )

            doc = await database.get_file(token)
            expiry_str = _format_expiry(
                doc.get("expires_in") if doc else expires_in,
                doc.get("expires_at") if doc else None,
            )

            link = f"{Config.BASE_URL}/dl/{token}"
            size_mb = file_size / (1024 * 1024)

            expiry_label = {
                "1h": "1 Hour", "6h": "6 Hours", "12h": "12 Hours",
                "1d": "1 Day",  "3d": "3 Days",  "7d":  "7 Days",
                "30d": "30 Days", "never": "Never",
            }.get(expiry_key, expiry_key)

            await cb.message.edit_text(
                f"Link ready!\n\n"
                f"File: {file_name}\n"
                f"Size: {size_mb:.2f} MB\n"
                f"Expires: {expiry_label}  ({expiry_str})\n\n"
                f"{link}\n\n"
                f"Direct download — no account needed."
            )

        except Exception as exc:
            log.error("Error processing file for user %s: %s", user_id, exc)
            await cb.message.edit_text("Something went wrong. Please try again.")

    # ── Owner: delete a link manually ──────────────────────────────────────
    @client.on_message(filters.command("delete") & filters.private)
    async def cmd_delete(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("Only the bot owner can delete links.")
            return

        parts = msg.text.split(maxsplit=1)
        if len(parts) < 2:
            await msg.reply_text("Usage: /delete <token>")
            return

        token = parts[1].strip()
        deleted = await database.delete_file(token)
        await cache.cache_delete(token)

        if deleted:
            await msg.reply_text(f"Link deleted: {token}")
        else:
            await msg.reply_text(f"Token not found: {token}")

    # ── Owner: stats ───────────────────────────────────────────────────────
    @client.on_message(filters.command("stats") & filters.private)
    async def cmd_stats(_, msg: Message) -> None:
        if not _is_owner(msg.from_user.id):
            await msg.reply_text("Owners only.")
            return

        db_ok = await database.ping()
        await msg.reply_text(
            f"Bot Stats\n\n"
            f"DB: {'connected' if db_ok else 'DOWN'}\n"
            f"Base URL: {Config.BASE_URL}\n"
            f"API apps: {len(Config.api_pairs())}\n"
            f"Chunk size: {Config.CHUNK_SIZE // 1024} KB"
        )
