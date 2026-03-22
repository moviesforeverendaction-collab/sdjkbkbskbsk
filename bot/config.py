import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    # ── Telegram ──────────────────────────────────────────────────
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")

    # Primary Telegram app credentials — create at https://my.telegram.org
    API_ID: int = int(os.environ.get("API_ID", "0"))
    API_HASH: str = os.environ.get("API_HASH", "")

    # Secondary Telegram app credentials (OPTIONAL).
    # Create a second app at my.telegram.org with a different app name.
    # Both apps use the same BOT_TOKEN — the bot account is still one.
    # Each app has its own rate-limit bucket, so two apps = 2× download bandwidth.
    API_ID_2: int = int(os.environ.get("API_ID_2", "0") or "0")
    API_HASH_2: str = os.environ.get("API_HASH_2", "") or ""

    # Derived: list of (api_id, api_hash) pairs that are actually configured.
    # Used by the client pool — automatically 1 or 2 clients.
    @classmethod
    def api_pairs(cls) -> list[tuple[int, str]]:
        pairs = [(cls.API_ID, cls.API_HASH)]
        if cls.API_ID_2 and cls.API_HASH_2:
            pairs.append((cls.API_ID_2, cls.API_HASH_2))
        return pairs

    # The private channel where files are forwarded for storage.
    # Must be the full ID including -100 prefix, e.g. -1003144372708
    # Get it by forwarding a message from your channel to @userinfobot
    CHANNEL_ID: int = int(os.environ.get("CHANNEL_ID", "0").strip() or "0")

    # Bot owner user IDs (comma-separated), e.g. "123456789,987654321"
    OWNER_IDS: list[int] = [
        int(x.strip())
        for x in os.environ.get("OWNER_IDS", "").split(",")
        if x.strip().lstrip("-").isdigit() and int(x.strip()) > 0
    ]

    # ── Server ────────────────────────────────────────────────────
    # Railway injects PORT automatically; fallback to 8080 locally.
    PORT: int = int(os.environ.get("PORT", "8080"))

    # Public base URL of your Railway deployment.
    # Example: https://my-bot-production.up.railway.app
    BASE_URL: str = os.environ.get("BASE_URL", "").rstrip("/")

    # ── Database ──────────────────────────────────────────────────
    # Railway injects MONGO_URL; fallback to MONGO_URI for other providers
    MONGO_URI: str = os.environ.get("MONGO_URL") or os.environ.get("MONGO_URI", "")
    DB_NAME: str = os.environ.get("DB_NAME", "filestreambot")

    # ── Redis (optional but recommended) ─────────────────────────
    REDIS_URL: str = os.environ.get("REDIS_URL", "")

    # ── Streaming tuning ──────────────────────────────────────────
    # 1 MB chunks — sweet spot for single-session Telegram streaming.
    # Increasing beyond 2 MB gives diminishing returns and risks flood waits.
    CHUNK_SIZE: int = int(os.environ.get("CHUNK_SIZE", str(1024 * 1024)))

    # How many chunks to pre-read ahead into memory before the client reads them.
    # Keeps the pipe full; 3 is safe for Railway's RAM.
    PREFETCH_CHUNKS: int = int(os.environ.get("PREFETCH_CHUNKS", "3"))

    # Max concurrent streaming connections per server worker.
    MAX_CONCURRENT: int = int(os.environ.get("MAX_CONCURRENT", "20"))

    # ── Safety / rate-limiting ────────────────────────────────────
    # Max downloads per unique IP per minute (simple in-memory counter).
    RATE_LIMIT_PER_MINUTE: int = int(os.environ.get("RATE_LIMIT_PER_MINUTE", "10"))

    # If True, only OWNER_IDS can upload files.  Set False to allow all users.
    OWNER_ONLY_UPLOAD: bool = os.environ.get("OWNER_ONLY_UPLOAD", "false").lower() == "true"

    # ── Validation ────────────────────────────────────────────────
    @classmethod
    def validate(cls) -> None:
        missing = []
        for field in ("BOT_TOKEN", "API_ID", "API_HASH", "CHANNEL_ID", "BASE_URL"):
            val = getattr(cls, field)
            if not val or val in (0, "0", ""):
                missing.append(field)
        # Check Mongo separately since it can come from either var name
        if not cls.MONGO_URI:
            missing.append("MONGO_URL or MONGO_URI")
        if missing:
            raise EnvironmentError(
                f"Missing required environment variables: {', '.join(missing)}\n"
                "See .env.example for reference."
            )
