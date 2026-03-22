"""
Client pool — manages 1 or 2 Pyrogram bot clients.

Why two clients?
  Each API_ID/API_HASH pair you register at my.telegram.org is a separate
  Telegram "application" with its own independent rate-limit bucket.
  Both clients share the same bot account (same BOT_TOKEN), so there is
  zero risk of account bans — you are literally using Telegram's intended
  multi-app system.

  Result: 2 apps × ~5 MB/s per app = ~10 MB/s effective streaming bandwidth,
  distributed automatically across concurrent download requests.

Usage:
    pool = ClientPool()
    await pool.start()
    client = pool.get()          # round-robin pick
    await pool.stop()
"""

import asyncio
import itertools
import logging
from typing import Optional

from pyrogram import Client

from bot.config import Config

log = logging.getLogger(__name__)


class ClientPool:
    def __init__(self) -> None:
        self._clients: list[Client] = []
        self._cycle = None                  # itertools.cycle set after start()
        self._lock = asyncio.Lock()

    async def start(self) -> None:
        """Start all configured Pyrogram clients."""
        pairs = Config.api_pairs()
        log.info("Starting %d Telegram client(s)...", len(pairs))

        for idx, (api_id, api_hash) in enumerate(pairs, start=1):
            name = f"client_{idx}"
            client = Client(
                name=name,
                api_id=api_id,
                api_hash=api_hash,
                bot_token=Config.BOT_TOKEN,
                # Store session files in /tmp — Railway ephemeral disk is fine
                # because bots re-auth automatically on each restart.
                workdir="/tmp",
                # Limit Pyrogram's own connection pool — keeps RAM lean.
                max_concurrent_transmissions=4,
            )
            await client.start()
            self._clients.append(client)
            log.info("  ✓ Client %d started (api_id=%s)", idx, api_id)

        if not self._clients:
            raise RuntimeError("No Telegram clients started — check API_ID / API_HASH.")

        self._cycle = itertools.cycle(self._clients)
        log.info("Client pool ready (%d client(s)).", len(self._clients))

    async def stop(self) -> None:
        """Gracefully stop all clients."""
        for client in self._clients:
            try:
                await client.stop()
            except Exception as exc:
                log.warning("Error stopping client: %s", exc)
        self._clients.clear()
        log.info("Client pool stopped.")

    def get(self) -> Client:
        """
        Return the next client in round-robin order.
        Each call advances the cycle, so concurrent downloads spread evenly.
        """
        if not self._cycle:
            raise RuntimeError("ClientPool.start() has not been called.")
        return next(self._cycle)

    @property
    def count(self) -> int:
        return len(self._clients)

    def primary(self) -> Client:
        """Always return the first client (used for the bot's command handling)."""
        if not self._clients:
            raise RuntimeError("ClientPool not started.")
        return self._clients[0]
