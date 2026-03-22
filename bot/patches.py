"""
Apply patches to third-party libraries before anything else runs.
Import this module at the very top of __main__.py.

Pyrogram 2.0.106 hardcodes MIN_CHANNEL_ID = -1002147483647.
Telegram now issues channel IDs more negative than that limit
(e.g. -1003144372708), causing "Peer id invalid" errors locally
before Pyrogram even contacts Telegram. This patch extends the limit.
Reference: https://github.com/pyrogram/pyrogram/issues/1327
"""

import pyrogram.utils

pyrogram.utils.MIN_CHANNEL_ID = -1007852516352
pyrogram.utils.MIN_CHAT_ID    = -999999999999
