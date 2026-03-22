"""
Patches applied before any Pyrogram/Kurigram code runs.

Kurigram 2.2.x still inherits MIN_CHANNEL_ID from Pyrogram's utils.
Telegram now issues channel IDs more negative than -1002147483647,
so we extend the limit to cover all current and future channel IDs.
"""
import pyrogram.utils

pyrogram.utils.MIN_CHANNEL_ID = -1007852516352
pyrogram.utils.MIN_CHAT_ID    = -999999999999
