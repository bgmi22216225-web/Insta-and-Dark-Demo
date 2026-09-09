"""
Configuration — loads all settings from environment variables.
Used by Railway.app / .env locally.

Runs 2 bots (Insta Demo + Dark Demo) in ONE process.
Each bot has its OWN: BOT_TOKEN, DATABASE_URL, CHANNEL_ID.
Everything else (admin, contact, timings, limits) is SHARED.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── Shared across both bots ────────────────────────────────────────────────
ADMIN_ID: int      = int(os.environ["ADMIN_ID"])
CONTACT_ADMIN: str = os.environ.get("CONTACT_ADMIN", "https://t.me/youradmin")

VIDEO_DELETE_SECONDS: int     = 2 * 60           # 2 minutes
BROADCAST_DELETE_SECONDS: int = 6 * 60 * 60      # 6 hours
CYCLE_DAYS: int                = 7                # 7-day video cycle
VIDEOS_PER_SESSION: int        = 10

# ── Bot 1: Insta Demo ──────────────────────────────────────────────────────
BOT_TOKEN_INSTA: str    = os.environ["BOT_TOKEN_INSTA"]
DATABASE_URL_INSTA: str = os.environ["DATABASE_URL_INSTA"]
CHANNEL_ID_INSTA: int   = int(os.environ["CHANNEL_ID_INSTA"])

# ── Bot 2: Dark Demo ────────────────────────────────────────────────────────
BOT_TOKEN_DARK: str    = os.environ["BOT_TOKEN_DARK"]
DATABASE_URL_DARK: str = os.environ["DATABASE_URL_DARK"]
CHANNEL_ID_DARK: int   = int(os.environ["CHANNEL_ID_DARK"])

# ── Profile registry — bot.py loops over this ───────────────────────────────
PROFILES = [
    {
        "name": "insta",
        "bot_token": BOT_TOKEN_INSTA,
        "database_url": DATABASE_URL_INSTA,
        "channel_id": CHANNEL_ID_INSTA,
    },
    {
        "name": "dark",
        "bot_token": BOT_TOKEN_DARK,
        "database_url": DATABASE_URL_DARK,
        "channel_id": CHANNEL_ID_DARK,
    },
]
