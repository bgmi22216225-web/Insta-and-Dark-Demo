"""
Multi-bot Telegram launcher — Insta Demo + Dark Demo
FAST VERSION: Connection pooling + async optimizations

Dono bots ek hi process/Railway service mein chalte hain.
Har bot ka apna: BOT_TOKEN, DATABASE_URL, CHANNEL_ID (isolated).
Baaki sab (admin, contact, timings, handlers) SAME logic — config.PROFILES se load hota hai.
"""

import asyncio
import json
import logging
import urllib.parse
from datetime import datetime, timedelta
from threading import Lock

import pg8000.dbapi
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, BotCommand, BotCommandScopeChat
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram.error import TelegramError

import config

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════════
# BOT INSTANCE — ek profile (token/db/channel) ka poora self-contained bot
# ═══════════════════════════════════════════════════════════════════════════════
class BotInstance:
    def __init__(self, profile: dict):
        self.name         = profile["name"]
        self.bot_token    = profile["bot_token"]
        self.database_url = profile["database_url"]
        self.channel_id   = profile["channel_id"]

        # Shared settings (same for every bot)
        self.admin_id             = config.ADMIN_ID
        self.contact_admin        = config.CONTACT_ADMIN
        self.videos_per_session   = config.VIDEOS_PER_SESSION
        self.video_delete_seconds = config.VIDEO_DELETE_SECONDS
        self.broadcast_delete_seconds = config.BROADCAST_DELETE_SECONDS
        self.cycle_days           = config.CYCLE_DAYS

        self._conn = None
        self._conn_lock = Lock()
        self.log = logging.getLogger(f"bot.{self.name}")

    # ── Connection pool — ek hi connection reuse karo ────────────────────────
    def get_conn(self):
        with self._conn_lock:
            try:
                if self._conn is not None:
                    self._conn.run("SELECT 1")
                    return self._conn
            except Exception:
                self._conn = None
            r = urllib.parse.urlparse(self.database_url)
            self._conn = pg8000.dbapi.connect(
                host=r.hostname,
                port=r.port or 5432,
                database=r.path.lstrip("/"),
                user=r.username,
                password=r.password,
                ssl_context=True,
            )
            self._conn.autocommit = False
            self.log.info("🔌 DB connection (re)created.")
            return self._conn

    def db_exec(self, sql, params=(), fetch=None):
        for attempt in range(3):
            try:
                conn = self.get_conn()
                cur = conn.cursor()
                cur.execute(sql, params)
                if fetch == "one":
                    row = cur.fetchone()
                    conn.commit()
                    return row
                elif fetch == "all":
                    rows = cur.fetchall()
                    desc = cur.description
                    conn.commit()
                    return rows, desc
                else:
                    conn.commit()
                    return None
            except Exception as e:
                self.log.warning(f"DB attempt {attempt+1} failed: {e}")
                self._conn = None
                if attempt == 2:
                    raise

    @staticmethod
    def rows_to_dicts(rows, desc):
        cols = [d[0] for d in desc]
        return [dict(zip(cols, r)) for r in rows]

    # ── DB init ───────────────────────────────────────────────────────────
    def init_db(self):
        self.db_exec("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY, username TEXT, first_name TEXT,
                joined_at TIMESTAMPTZ DEFAULT NOW(), last_fetch TIMESTAMPTZ
            )
        """)
        self.db_exec("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value TEXT NOT NULL
            )
        """)
        self.db_exec(
            "INSERT INTO settings(key,value) VALUES('caption','Aur videos ke liye admin se contact karein.') "
            "ON CONFLICT(key) DO NOTHING"
        )
        self.db_exec("""
            CREATE TABLE IF NOT EXISTS fetched_content (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL,
                message_ids TEXT NOT NULL DEFAULT '[]',
                warning_msg_id BIGINT, fetched_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        self.db_exec("""
            CREATE TABLE IF NOT EXISTS broadcast_jobs (
                id SERIAL PRIMARY KEY, user_id BIGINT NOT NULL,
                message_id BIGINT NOT NULL, delete_at TIMESTAMPTZ NOT NULL,
                deleted BOOLEAN DEFAULT FALSE
            )
        """)
        self.db_exec("""
            CREATE TABLE IF NOT EXISTS channel_videos (
                message_id BIGINT PRIMARY KEY, media_type TEXT,
                indexed_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        self.log.info("✅ DB ready.")

    # ── DB helpers ────────────────────────────────────────────────────────
    def upsert_user(self, user_id, username, first_name):
        self.db_exec(
            "INSERT INTO users(user_id,username,first_name) VALUES(%s,%s,%s) "
            "ON CONFLICT(user_id) DO UPDATE SET username=EXCLUDED.username, first_name=EXCLUDED.first_name",
            (user_id, username, first_name)
        )

    def get_all_user_ids(self):
        rows, _ = self.db_exec("SELECT user_id FROM users", fetch="all")
        return [r[0] for r in rows]

    def update_last_fetch(self, user_id):
        self.db_exec("UPDATE users SET last_fetch=NOW() WHERE user_id=%s", (user_id,))

    def get_setting(self, key):
        row = self.db_exec("SELECT value FROM settings WHERE key=%s", (key,), fetch="one")
        return row[0] if row else ""

    def set_setting(self, key, value):
        self.db_exec(
            "INSERT INTO settings(key,value) VALUES(%s,%s) ON CONFLICT(key) DO UPDATE SET value=EXCLUDED.value",
            (key, value)
        )

    def get_user_content(self, user_id):
        cutoff = datetime.utcnow() - timedelta(days=self.cycle_days)
        row = self.db_exec(
            "SELECT id,message_ids,warning_msg_id,fetched_at FROM fetched_content "
            "WHERE user_id=%s AND fetched_at>%s ORDER BY fetched_at DESC LIMIT 1",
            (user_id, cutoff), fetch="one"
        )
        if row:
            return {"id": row[0], "message_ids": row[1], "warning_msg_id": row[2], "fetched_at": row[3]}
        return None

    def save_user_content(self, user_id, message_ids, warning_msg_id):
        self.db_exec("DELETE FROM fetched_content WHERE user_id=%s", (user_id,))
        self.db_exec(
            "INSERT INTO fetched_content(user_id,message_ids,warning_msg_id) VALUES(%s,%s,%s)",
            (user_id, json.dumps(message_ids), warning_msg_id)
        )

    def reset_all_content(self):
        self.db_exec("DELETE FROM fetched_content")

    def save_broadcast_job(self, user_id, message_id):
        delete_at = datetime.utcnow() + timedelta(seconds=self.broadcast_delete_seconds)
        self.db_exec(
            "INSERT INTO broadcast_jobs(user_id,message_id,delete_at) VALUES(%s,%s,%s)",
            (user_id, message_id, delete_at)
        )

    def get_pending_broadcast_deletes(self):
        rows, desc = self.db_exec(
            "SELECT id,user_id,message_id FROM broadcast_jobs WHERE deleted=FALSE AND delete_at<=NOW()",
            fetch="all"
        )
        return self.rows_to_dicts(rows, desc)

    def mark_broadcast_deleted(self, job_id):
        self.db_exec("UPDATE broadcast_jobs SET deleted=TRUE WHERE id=%s", (job_id,))

    def save_channel_video(self, message_id, media_type="video"):
        self.db_exec(
            "INSERT INTO channel_videos(message_id,media_type) VALUES(%s,%s) ON CONFLICT(message_id) DO NOTHING",
            (message_id, media_type)
        )

    def get_latest_channel_video_ids(self, count):
        rows, _ = self.db_exec(
            "SELECT message_id FROM channel_videos ORDER BY message_id DESC LIMIT %s",
            (count,), fetch="all"
        )
        return [r[0] for r in rows]

    def get_channel_video_count(self):
        row = self.db_exec("SELECT COUNT(*) FROM channel_videos", fetch="one")
        return row[0] if row else 0

    # ── Misc helpers ──────────────────────────────────────────────────────
    async def _safe_delete(self, bot, chat_id, msg_ids):
        for mid in msg_ids:
            try:
                await bot.delete_message(chat_id=chat_id, message_id=mid)
            except TelegramError:
                pass

    def _contact_url(self):
        raw = self.contact_admin.strip()
        if raw.startswith("@"):
            return f"https://t.me/{raw[1:]}"
        if raw.startswith("http"):
            return raw
        return f"https://t.me/{raw}"

    # ── Scheduled jobs ────────────────────────────────────────────────────
    async def _delete_videos_job(self, context: ContextTypes.DEFAULT_TYPE):
        d = context.job.data
        await self._safe_delete(context.bot, d["chat_id"], d["msg_ids"])

    async def _delete_broadcast_job(self, context: ContextTypes.DEFAULT_TYPE):
        jobs = self.get_pending_broadcast_deletes()
        for job in jobs:
            try:
                await context.bot.delete_message(chat_id=job["user_id"], message_id=job["message_id"])
            except TelegramError:
                pass
            finally:
                self.mark_broadcast_deleted(job["id"])

    # ═══════════════════════════════════════════════════════════════════════
    # AUTO-FETCH: Channel post → instantly DB mein save
    # ═══════════════════════════════════════════════════════════════════════
    async def channel_post_handler(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        post = update.channel_post or update.message
        if not post:
            return
        if abs(post.chat.id) != abs(self.channel_id):
            self.log.info(f"⚠️ Ignored post from chat_id={post.chat.id} (expected {self.channel_id})")
            return
        media_type = (
            "video"    if post.video    else
            "document" if post.document else
            "photo"    if post.photo    else None
        )
        if media_type:
            self.save_channel_video(post.message_id, media_type)
            self.log.info(f"⚡ Auto-indexed {media_type} msg_id={post.message_id} | total={self.get_channel_video_count()}")
        else:
            self.log.info(f"⚠️ Channel post received but no media — msg_id={post.message_id}, chat_id={post.chat.id}")

    # ═══════════════════════════════════════════════════════════════════════
    # /start — optimized: DB calls batched, videos sent concurrently
    # ═══════════════════════════════════════════════════════════════════════
    async def start_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        user    = update.effective_user
        chat_id = update.effective_chat.id
        bot     = context.bot

        # Fire-and-forget user upsert (don't await DB before sending)
        self.upsert_user(user.id, user.username, user.first_name)

        # Cancel old delete jobs + clean old session
        prev_key = f"session_{user.id}"
        if prev_key in context.bot_data:
            asyncio.create_task(self._safe_delete(bot, chat_id, context.bot_data[prev_key]))
            del context.bot_data[prev_key]
        for job in context.job_queue.get_jobs_by_name(f"del_{user.id}"):
            job.schedule_removal()

        # Warning message
        warn = await bot.send_message(
            chat_id=chat_id,
            text=f"⚠️ *Yeh videos {self.video_delete_seconds // 60} minute baad auto-delete ho jayenge.*\n📥 Download ya Forward disabled hai.",
            parse_mode="Markdown",
        )
        all_del = [warn.message_id]

        # Get video IDs (single fast DB call)
        cached  = self.get_user_content(user.id)
        ch_ids  = []
        is_new  = False

        if cached:
            ch_ids = json.loads(cached["message_ids"]) \
                if isinstance(cached["message_ids"], str) else cached["message_ids"]
        else:
            ch_ids = self.get_latest_channel_video_ids(self.videos_per_session)
            is_new = True

        if not ch_ids:
            await bot.send_message(chat_id=chat_id, text="❌ Abhi koi video nahi hai. Thodi der baad try karein.")
            return

        # Send all videos concurrently (much faster than sequential)
        async def _send_one(ch_id):
            try:
                sent = await bot.copy_message(
                    chat_id=chat_id, from_chat_id=self.channel_id,
                    message_id=ch_id, protect_content=True,
                    disable_notification=True,
                )
                return sent.message_id
            except TelegramError as e:
                self.log.warning(f"Video {ch_id}: {e}")
                return None

        results = await asyncio.gather(*[_send_one(cid) for cid in ch_ids])
        sent_ids = [r for r in results if r]
        all_del.extend(sent_ids)

        # Save to DB if fresh fetch
        if is_new:
            self.save_user_content(user.id, ch_ids, warn.message_id)
            self.update_last_fetch(user.id)

        context.bot_data[prev_key] = all_del

        # Final caption + Contact button (permanent)
        caption     = self.get_setting("caption")
        contact_url = self._contact_url()

        await bot.send_message(
            chat_id=chat_id,
            text=f"*{caption}*",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("📩 Contact Admin", url=contact_url)]]
            ),
        )

        # Schedule auto-delete
        context.job_queue.run_once(
            self._delete_videos_job,
            when=self.video_delete_seconds,
            data={"chat_id": chat_id, "msg_ids": all_del},
            name=f"del_{user.id}",
        )

    # ═══════════════════════════════════════════════════════════════════════
    # /reset
    # ═══════════════════════════════════════════════════════════════════════
    async def reset_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.admin_id:
            return await update.message.reply_text("🚫 Unauthorized.")
        self.reset_all_content()
        total = self.get_channel_video_count()
        await update.message.reply_text(
            f"✅ *Reset ho gaya!*\n\n"
            f"📹 Indexed videos: *{total}*\n"
            f"Ab sabko next /start par latest videos milenge.",
            parse_mode="Markdown",
        )

    # ═══════════════════════════════════════════════════════════════════════
    # /setcaption
    # ═══════════════════════════════════════════════════════════════════════
    async def setcaption_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.admin_id:
            return await update.message.reply_text("🚫 Unauthorized.")
        raw  = update.message.text or ""
        text = raw.split(" ", 1)[1].strip() if " " in raw else ""
        if not text:
            return await update.message.reply_text("Usage: /setcaption Aapka caption yahan")
        self.set_setting("caption", text)
        await update.message.reply_text(f"✅ *Caption set!*\n\n📌 {text}", parse_mode="Markdown")

    # ═══════════════════════════════════════════════════════════════════════
    # /broadcast
    # ═══════════════════════════════════════════════════════════════════════
    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        if update.effective_user.id != self.admin_id:
            return await update.message.reply_text("🚫 Unauthorized.")

        msg     = update.message
        reply   = msg.reply_to_message
        raw     = msg.text or msg.caption or ""
        caption = raw.split(" ", 1)[1].strip() if " " in raw else ""
        if not caption and reply and reply.caption:
            caption = reply.caption

        user_ids = self.get_all_user_ids()
        if not user_ids:
            return await msg.reply_text("⚠️ Koi user nahi mila.")
        if not reply and not caption:
            return await msg.reply_text(
                "❌ Usage:\n"
                "/broadcast Text ya <a href='URL'>Link</a>\n\n"
                "Image ke saath: image reply karo + /broadcast Caption",
                parse_mode="HTML",
            )

        status = await msg.reply_text(f"📢 *Sending to {len(user_ids)} users...*", parse_mode="Markdown")

        # Broadcast concurrently (much faster)
        async def _send_one(uid):
            try:
                sent = None
                if reply and reply.photo:
                    sent = await context.bot.send_photo(chat_id=uid, photo=reply.photo[-1].file_id, caption=caption or None, parse_mode="HTML")
                elif reply and reply.video:
                    sent = await context.bot.send_video(chat_id=uid, video=reply.video.file_id, caption=caption or None, parse_mode="HTML")
                elif reply and reply.document:
                    sent = await context.bot.send_document(chat_id=uid, document=reply.document.file_id, caption=caption or None, parse_mode="HTML")
                elif reply and reply.animation:
                    sent = await context.bot.send_animation(chat_id=uid, animation=reply.animation.file_id, caption=caption or None, parse_mode="HTML")
                else:
                    sent = await context.bot.send_message(chat_id=uid, text=caption, parse_mode="HTML", disable_web_page_preview=False)
                if sent:
                    self.save_broadcast_job(uid, sent.message_id)
                return True
            except TelegramError as e:
                self.log.warning(f"Broadcast {uid}: {e}")
                return False

        # Send in batches of 25 to avoid flood limits
        BATCH = 25
        ok = fail = 0
        for i in range(0, len(user_ids), BATCH):
            batch   = user_ids[i:i+BATCH]
            results = await asyncio.gather(*[_send_one(uid) for uid in batch])
            ok      += sum(results)
            fail    += len(results) - sum(results)
            if i + BATCH < len(user_ids):
                await asyncio.sleep(1)   # flood limit ke liye 1 sec pause per batch

        hrs = self.broadcast_delete_seconds / 3600
        await status.edit_text(
            f"✅ *Broadcast Complete!*\n\n"
            f"📨 Sent: *{ok}*\n❌ Failed: *{fail}*\n⏳ {hrs:g}h baad auto-delete.",
            parse_mode="Markdown",
        )

    # ═══════════════════════════════════════════════════════════════════════
    # STARTUP + APP BUILD
    # ═══════════════════════════════════════════════════════════════════════
    async def _on_startup(self, app: Application):
        app.job_queue.run_repeating(self._delete_broadcast_job, interval=300, first=10, name=f"broadcast_cleaner_{self.name}")
        try:
            await app.bot.set_my_commands([BotCommand("start", "Videos dekho")])
            await app.bot.set_my_commands(
                [
                    BotCommand("start",      "Videos dekho"),
                    BotCommand("reset",      "Naye videos fetch karo"),
                    BotCommand("setcaption", "Caption set karo"),
                    BotCommand("broadcast",  "Sabko message bhejo"),
                ],
                scope=BotCommandScopeChat(chat_id=self.admin_id),
            )
        except Exception:
            pass
        self.log.info(f"⚡ [{self.name}] Bot ready.")

    def build_application(self) -> Application:
        app = (
            Application.builder()
            .token(self.bot_token)
            .post_init(self._on_startup)
            .concurrent_updates(True)
            .build()
        )
        app.add_handler(CommandHandler("start",      self.start_command))
        app.add_handler(CommandHandler("reset",      self.reset_command))
        app.add_handler(CommandHandler("setcaption", self.setcaption_command))
        app.add_handler(CommandHandler("broadcast",  self.broadcast_command))
        app.add_handler(MessageHandler(filters.UpdateType.CHANNEL_POST, self.channel_post_handler))
        return app


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN — dono bots ek hi process mein saath chalao
# ═══════════════════════════════════════════════════════════════════════════════
async def run_forever():
    instances = [BotInstance(p) for p in config.PROFILES]
    apps = []

    for inst in instances:
        inst.init_db()
        app = inst.build_application()
        await app.initialize()
        await app.start()
        await app.updater.start_polling(
            drop_pending_updates=True,
            allowed_updates=["message", "channel_post"],
        )
        apps.append(app)
        logger.info(f"🤖 [{inst.name}] bot started.")

    logger.info(f"⚡ {len(apps)} bot(s) running in this process.")
    try:
        await asyncio.Event().wait()   # process ko zinda rakho
    finally:
        for app in apps:
            await app.updater.stop()
            await app.stop()
            await app.shutdown()


def main():
    asyncio.run(run_forever())


if __name__ == "__main__":
    main()
