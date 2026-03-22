import secrets
import time
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient
from bot.config import Config

_client: Optional[AsyncIOMotorClient] = None

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
    col = _db()["files"]
    await col.create_index("token", unique=True, background=True)
    await col.create_index("file_unique_id", background=True)
    await col.create_index("uploader_id", background=True)
    await col.create_index("expires_at", expireAfterSeconds=0, background=True, sparse=True)

    # User settings collection
    ucol = _db()["users"]
    await ucol.create_index("user_id", unique=True, background=True)


def _make_token(length: int = 24) -> str:
    return secrets.token_urlsafe(length)


async def save_file(
    file_id: str,
    file_unique_id: str,
    file_name: str,
    file_size: int,
    mime_type: str,
    message_id: int,
    uploader_id: int,
    expires_in: Optional[int],
) -> str:
    col = _db()["files"]
    now = int(time.time())
    expires_at = (now + expires_in) if expires_in else None

    if expires_at is None:
        existing = await col.find_one({"file_unique_id": file_unique_id, "expires_at": None})
        if existing:
            return existing["token"]

    token = _make_token()
    await col.insert_one({
        "token":          token,
        "file_id":        file_id,
        "file_unique_id": file_unique_id,
        "file_name":      file_name,
        "file_size":      file_size,
        "mime_type":      mime_type,
        "message_id":     message_id,
        "uploader_id":    uploader_id,
        "created_at":     now,
        "expires_in":     expires_in,
        "expires_at":     expires_at,
        "downloads":      0,
        "god_speed_ready": False,
    })
    return token


async def get_file(token: str) -> Optional[dict]:
    doc = await _db()["files"].find_one({"token": token}, {"_id": 0})
    if doc is None:
        return None
    expires_at = doc.get("expires_at")
    if expires_at and int(time.time()) > expires_at:
        return None
    return doc


async def mark_god_speed_ready(token: str) -> None:
    await _db()["files"].update_one({"token": token}, {"$set": {"god_speed_ready": True}})


async def increment_downloads(token: str) -> None:
    await _db()["files"].update_one({"token": token}, {"$inc": {"downloads": 1}})


async def delete_file(token: str) -> bool:
    result = await _db()["files"].delete_one({"token": token})
    return result.deleted_count > 0


async def delete_expired_files() -> int:
    now = int(time.time())
    result = await _db()["files"].delete_many({"expires_at": {"$lte": now}})
    return result.deleted_count


async def user_file_count(uploader_id: int) -> int:
    return await _db()["files"].count_documents({"uploader_id": uploader_id})


# ── User settings ─────────────────────────────────────────────────────────────

async def get_user_settings(user_id: int) -> dict:
    doc = await _db()["users"].find_one({"user_id": user_id}, {"_id": 0})
    if doc is None:
        return {"user_id": user_id, "god_speed": False}
    return doc


async def set_god_speed(user_id: int, enabled: bool) -> None:
    await _db()["users"].update_one(
        {"user_id": user_id},
        {"$set": {"god_speed": enabled}},
        upsert=True,
    )


async def is_god_speed_enabled(user_id: int) -> bool:
    settings = await get_user_settings(user_id)
    return settings.get("god_speed", False)
