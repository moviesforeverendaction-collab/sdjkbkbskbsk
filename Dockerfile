# ── Build stage ───────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /app

# Install build deps needed for TgCrypto and uvloop C extensions
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

# Copy installed packages from builder — keeps final image lean
COPY --from=builder /install /usr/local

# Copy source
COPY bot/ ./bot/

# /tmp is used for Pyrogram session files (ephemeral — fine for bots)
# No other writable volume needed.
RUN mkdir -p /tmp && chmod 1777 /tmp

# Railway injects PORT; default 8080 for local runs
ENV PORT=8080
EXPOSE 8080

# Non-root user for safety
RUN useradd -r -u 1001 -s /sbin/nologin appuser && \
    chown -R appuser:appuser /app /tmp
USER appuser

CMD ["python", "-m", "bot"]
