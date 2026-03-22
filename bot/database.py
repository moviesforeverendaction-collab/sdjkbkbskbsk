import secrets
import time
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient

from bot.config import Config


_client: Optional[AsyncIOMotorClient] = None

# Expiry options shown to users (label -> seconds, None = never)
EXPIRY_OPTIONS: dict[str, Optional[int]] = {
    "1h":    3600,
    "6h":    21600,
    "12h":   43200,
    "1d":    86400,
    "3d":    259200,
    "7d":    604800,
    "30d":   2592000,
    "never": None,
}


def _db():
    global _client
    if _client is None:
        _client = AsyncIOMotorClient(Config.MONGO_URI)
    return _client[Config.DB_NAME]


async def ping() -> bool:
    try:
        await _db().command("ping")
        return True
    except Exception:
        return False


async def ensure_indexes() -> None:
    """
    Create all required indexes once at startup.

    The TTL index on `expires_at` makes MongoDB auto-delete expired
    documents — no cron job needed. MongoDB checks every 60 seconds.
    Documents with expires_at=None are NOT deleted (null is ignored by TTL).
    """
    col = _db()["files"]
    await col.create_index("token", unique=True, background=True)
    await col.create_index("file_unique_id", background=True)
    await col.create_index("uploader_id", background=True)

    # TTL index: delete document when current time passes expires_at.
    # expireAfterSeconds=0 means "delete exactly at expires_at".
    # Docs where expires_at is null or missing are skipped automatically.
    await col.create_index(
        "expires_at",
        expireAfterSeconds=0,
        background=True,
        sparse=True,    # sparse=True skips documents where field is null/missing
    )


def _make_token(length: int = 24) -> str:
    """Cryptographically random URL-safe token."""
    return secrets.token_urlsafe(length)


async def save_file(
    file_id: str,
    file_unique_id: str,
    file_name: str,
    file_size: int,
    mime_type: str,
    message_id: int,
    uploader_id: int,
    expires_in: Optional[int],   # seconds from now, or None = never
) -> str:
    """
    Store file metadata. Returns the download token.

    expires_in=None  -> link never expires
    expires_in=3600  -> link dies after 1 hour

    Dedup only applies for "never" links with the same file_unique_id.
    Timed links always get a fresh token so the expiry is exactly as chosen.
    """
    col = _db()["files"]

    now = int(time.time())
    expires_at: Optional[int] = (now + expires_in) if expires_in else None

    # Dedup only for never-expiring links
    if expires_at is None:
        existing = await col.find_one(
            {"file_unique_id": file_unique_id, "expires_at": None}
        )
        if existing:
            return existing["token"]

    token = _make_token()
    doc = {
        "token":          token,
        "file_id":        file_id,
        "file_unique_id": file_unique_id,
        "file_name":      file_name,
        "file_size":      file_size,
        "mime_type":      mime_type,
        "message_id":     message_id,
        "uploader_id":    uploader_id,
        "created_at":     now,
        "expires_in":     expires_in,    # stored for display
        "expires_at":     expires_at,    # None or unix timestamp
        "downloads":      0,
    }
    await col.insert_one(doc)
    return token


async def get_file(token: str) -> Optional[dict]:
    """
    Fetch file metadata by token.
    Returns None if not found OR if the link has expired.
    Double-checks expiry here because MongoDB TTL has up to 60s lag.
    """
    doc = await _db()["files"].find_one({"token": token}, {"_id": 0})
    if doc is None:
        return None

    expires_at = doc.get("expires_at")
    if expires_at and int(time.time()) > expires_at:
        return None

    return doc


async def increment_downloads(token: str) -> None:
    await _db()["files"].update_one(
        {"token": token}, {"$inc": {"downloads": 1}}
    )


async def delete_file(token: str) -> bool:
    result = await _db()["files"].delete_one({"token": token})
    return result.deleted_count > 0


async def delete_expired_files() -> int:
    """
    Manual sweep for any expired docs MongoDB TTL hasn't cleaned yet.
    Belt-and-suspenders — run this hourly.
    Returns count of deleted docs.
    """
    now = int(time.time())
    result = await _db()["files"].delete_many(
        {"expires_at": {"$lte": now}}
    )
    return result.deleted_count


async def user_file_count(uploader_id: int) -> int:
    return await _db()["files"].count_documents({"uploader_id": uploader_id})
