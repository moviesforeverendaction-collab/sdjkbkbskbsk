# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip && \
    pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

WORKDIR /app

COPY --from=builder /install /usr/local
COPY bot/ ./bot/

# Create non-root user
RUN useradd -r -u 1001 -s /sbin/nologin appuser

# /data — mount a Railway Volume here to persist Pyrogram session files.
# Without this, every container restart = fresh Telegram auth = FloodWait risk.
# If no Volume is mounted, /data still exists and works (but sessions reset on redeploy).
RUN mkdir -p /data /tmp && \
    chown -R appuser:appuser /app /data /tmp && \
    chmod 755 /data

ENV PORT=8080
ENV SESSION_DIR=/data
EXPOSE 8080

USER appuser

CMD ["python", "-m", "bot"]
