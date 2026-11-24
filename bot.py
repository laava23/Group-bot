import os
import re
import time
import html
import random
import signal
import asyncio
import logging
import threading
import urllib.request

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from contextlib import contextmanager

import psycopg2

from telegram import Update, ChatPermissions, MessageEntity
from telegram.error import TelegramError, RetryAfter, InvalidToken
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    ChatMemberHandler,
    ContextTypes,
    filters,
)


# ==========================================
# CONFIGURATION
# ==========================================

# strip() removes accidental spaces / quotes from copy-paste
BOT_TOKEN = os.environ["BOT_TOKEN"].strip().strip('"').strip("'").strip()
DATABASE_URL = os.environ["DATABASE_URL"].strip()
PORT = int(os.environ.get("PORT", "10000"))

IST = ZoneInfo("Asia/Kolkata")

BOT_MESSAGE_TTL_HOURS = 6          # bot messages auto-delete after this
SETTINGS_CACHE_SECONDS = 60
ADMIN_CACHE_SECONDS = 300

# kind: (enabled, limit per day, mute seconds)
DEFAULTS = {
    "link": (True, 3, 24 * 3600),
    "img": (False, 5, 24 * 3600),
    "text": (False, 20, 3600),
}

KIND_ALIASES = {
    "link": "link", "links": "link",
    "img": "img", "image": "img", "images": "img",
    "photo": "img", "photos": "img",
    "text": "text", "msg": "text", "message": "text", "messages": "text",
}

KIND_LABEL = {
    "link": "link message(s)",
    "img": "image(s)",
    "text": "text message(s)",
}

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    level=logging.INFO,
)

# httpx logs the full URL, which contains the bot token. Keep it quiet.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

logger = logging.getLogger("bot")

if not re.fullmatch(r"\d{6,12}:[A-Za-z0-9_-]{35}", BOT_TOKEN):
    logger.warning(
        "BOT_TOKEN format looks wrong (expected digits:35 characters). "
        "Re-copy it from BotFather."
    )

HELP_TEXT = (
    "Link & Spam Guard - Help\n"
    "\n"
    "Members: limits are counted per day (IST). "
    "Admins are never warned or muted.\n"
    "\n"
    "ADMIN COMMANDS\n"
    "\n"
    "/setwarn [type] [limit] [mute time]\n"
    "  type = link, img or text\n"
    "  time = 30m, 6h, 1d (m = minutes, h = hours, d = days)\n"
    "  Examples:\n"
    "  /setwarn link 3 24h\n"
    "  -> 3 links allowed per day, the 4th link = 24 hour mute\n"
    "  /setwarn img 5 12h\n"
    "  -> 5 images allowed per day, the 6th image = 12 hour mute\n"
    "  /setwarn text 30 1h\n"
    "  -> 30 text messages allowed per day, the 31st = 1 hour mute\n"
    "  /setwarn img off\n"
    "  -> turns that check off\n"
    "\n"
    "/removewarn\n"
    "  Reply to a member's message with /removewarn to clear that "
    "member's warnings for today. You can also use /removewarn @username\n"
    "\n"
    "/warnlist - who has warnings today\n"
    "/settings - current limits\n"
    "/help - this message\n"
    "\n"
    "NOTES\n"
    "- Make the bot admin with 'Delete messages' and 'Restrict members'.\n"
    f"- Bot messages are deleted automatically after {BOT_MESSAGE_TTL_HOURS} hours.\n"
    "- A new warning replaces the member's previous warning message.\n"
    "- Text warnings start only when the member is close to the limit."
)

# in-memory caches
settings_cache = {}   # chat_id -> (time, settings)
admin_cache = {}      # chat_id -> (time, set of admin ids)


# ==========================================
# HEALTH SERVER + KEEP-ALIVE (RENDER)
# ==========================================

class HealthHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Bot is running")

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        pass


def start_health_server():
    server = ThreadingHTTPServer(("0.0.0.0", PORT), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logger.info("Health server started on port %s", PORT)


def start_keepalive():
    url = os.environ.get("RENDER_EXTERNAL_URL")

    if not url:
        return

    def loop():
        while True:
            time.sleep(600)
            try:
                urllib.request.urlopen(url, timeout=20).read()
            except Exception:
                pass

    threading.Thread(target=loop, daemon=True).start()
    logger.info("Keep-alive pinger started")


# ==========================================
# DATABASE (blocking code, always called via to_thread)
# ==========================================

def db_connect():
    last_error = None

    for attempt in range(4):
        try:
            return psycopg2.connect(DATABASE_URL, connect_timeout=15)
        except psycopg2.OperationalError as error:
            last_error = error
            time.sleep(2 * (attempt + 1))

    raise last_error


@contextmanager
def database():
    conn = db_connect()

    try:
        with conn:
            yield conn
    finally:
        conn.close()


def today_ist():
    return datetime.now(IST).date()


def setup_database():
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    chat_id BIGINT NOT NULL,
                    kind TEXT NOT NULL,
                    enabled BOOLEAN NOT NULL,
                    limit_count INTEGER NOT NULL,
                    mute_seconds INTEGER NOT NULL,
                    PRIMARY KEY (chat_id, kind)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS counts (
                    chat_id BIGINT NOT NULL,
                    user_id BIGINT NOT NULL,
                    kind TEXT NOT NULL,
                    day DATE NOT NULL,
                    cnt INTEGER NOT NULL DEFAULT 0,
                    username TEXT,
                    PRIMARY KEY (chat_id, user_id, kind, day)
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS bot_messages (
                    chat_id BIGINT NOT NULL,
                    message_id BIGINT NOT NULL,
                    user_id BIGINT,
                    kind TEXT NOT NULL,
                    delete_at TIMESTAMPTZ NOT NULL,
                    PRIMARY KEY (chat_id, message_id)
                )
            """)

    logger.info("Database connected successfully")


def load_settings(chat_id):
    result = {
        kind: {"enabled": v[0], "limit": v[1], "mute": v[2]}
        for kind, v in DEFAULTS.items()
    }

    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT kind, enabled, limit_count, mute_seconds
                FROM settings WHERE chat_id = %s
            """, (chat_id,))

            for kind, enabled, limit_count, mute_seconds in cur.fetchall():
                if kind in result:
                    result[kind] = {
                        "enabled": enabled,
                        "limit": limit_count,
                        "mute": mute_seconds,
                    }

    return result


def save_setting(chat_id, kind, enabled, limit_count, mute_seconds):
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO settings
                    (chat_id, kind, enabled, limit_count, mute_seconds)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, kind) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    limit_count = EXCLUDED.limit_count,
                    mute_seconds = EXCLUDED.mute_seconds
            """, (chat_id, kind, enabled, limit_count, mute_seconds))


def increment_count(chat_id, user_id, kind, username):
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO counts (chat_id, user_id, kind, day, cnt, username)
                VALUES (%s, %s, %s, %s, 1, %s)
                ON CONFLICT (chat_id, user_id, kind, day) DO UPDATE SET
                    cnt = counts.cnt + 1,
                    username = EXCLUDED.username
                RETURNING cnt
            """, (chat_id, user_id, kind, today_ist(), username))

            return cur.fetchone()[0]


def reset_counts(chat_id, user_id, kind=None):
    with database() as conn:
        with conn.cursor() as cur:
            if kind:
                cur.execute("""
                    DELETE FROM counts
                    WHERE chat_id = %s AND user_id = %s
                    AND day = %s AND kind = %s
                """, (chat_id, user_id, today_ist(), kind))
            else:
                cur.execute("""
                    DELETE FROM counts
                    WHERE chat_id = %s AND user_id = %s AND day = %s
                """, (chat_id, user_id, today_ist()))


def get_warn_list(chat_id):
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id, username, kind, cnt
                FROM counts
                WHERE chat_id = %s AND day = %s AND cnt > 0
                ORDER BY cnt DESC
            """, (chat_id, today_ist()))

            return cur.fetchall()


def find_user_id_by_username(chat_id, username):
    username = username.lstrip("@").lower()

    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT user_id FROM counts
                WHERE chat_id = %s AND lower(username) = %s
                ORDER BY day DESC LIMIT 1
            """, (chat_id, "@" + username))

            row = cur.fetchone()

            return row[0] if row else None


def add_bot_message(chat_id, message_id, user_id, kind):
    delete_at = datetime.now(timezone.utc) + timedelta(hours=BOT_MESSAGE_TTL_HOURS)

    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO bot_messages
                    (chat_id, message_id, user_id, kind, delete_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (chat_id, message_id) DO NOTHING
            """, (chat_id, message_id, user_id, kind, delete_at))


def pop_user_warnings(chat_id, user_id):
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM bot_messages
                WHERE chat_id = %s AND user_id = %s AND kind = 'warn'
                RETURNING message_id
            """, (chat_id, user_id))

            return [row[0] for row in cur.fetchall()]


def pop_due_messages():
    with database() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                DELETE FROM bot_messages
                WHERE delete_at <= now()
                RETURNING chat_id, message_id
            """)

            return cur.fetchall()


# ==========================================
# HELPERS
# ==========================================

async def get_settings_cached(chat_id):
    cached = settings_cache.get(chat_id)

    if cached and time.time() - cached[0] < SETTINGS_CACHE_SECONDS:
        return cached[1]

    data = await asyncio.to_thread(load_settings, chat_id)
    settings_cache[chat_id] = (time.time(), data)

    return data


def parse_duration(text):
    match = re.fullmatch(r"(\d+)\s*([mhd])", text.strip().lower())

    if not match:
        return None

    seconds = int(match.group(1)) * {"m": 60, "h": 3600, "d": 86400}[match.group(2)]

    if seconds < 60 or seconds > 365 * 86400:
        return None

    return seconds


def format_duration(seconds):
    if seconds % 86400 == 0:
        return f"{seconds // 86400} day(s)"

    if seconds % 3600 == 0:
        return f"{seconds // 3600} hour(s)"

    return f"{max(1, seconds // 60)} minute(s)"


def mention(user):
    name = html.escape(user.full_name or "member")

    return f'<a href="tg://user?id={user.id}">{name}</a>'


def username_or_name(user):
    if user.username:
        return "@" + user.username

    return user.full_name


def message_has_link(message):
    text = message.text or message.caption or ""

    if re.search(r"(https?://|www\.|t\.me/|telegram\.me/)", text, re.IGNORECASE):
        return True

    entities = message.entities or message.caption_entities or []

    return any(
        e.type in (MessageEntity.URL, MessageEntity.TEXT_LINK)
        for e in entities
    )


def detect_kind(message):
    if message_has_link(message):
        return "link"

    if message.photo:
        return "img"

    if message.text:
        return "text"

    return None


async def safe_delete(bot, chat_id, message_id):
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except TelegramError:
        pass


async def user_is_admin(bot, chat, message, user, fresh=False):
    # Admin posting anonymously
    if message and message.sender_chat and message.sender_chat.id == chat.id:
        return True

    if fresh:
        try:
            member = await bot.get_chat_member(chat_id=chat.id, user_id=user.id)
            return member.status in ("administrator", "creator")
        except TelegramError as error:
            logger.warning("Admin check failed: %s", error)
            return False

    cached = admin_cache.get(chat.id)

    if not cached or time.time() - cached[0] > ADMIN_CACHE_SECONDS:
        try:
            admins = await bot.get_chat_administrators(chat_id=chat.id)
            ids = {a.user.id for a in admins}
            admin_cache[chat.id] = (time.time(), ids)
        except TelegramError as error:
            logger.warning("Could not load admins: %s", error)
            ids = cached[1] if cached else set()
    else:
        ids = cached[1]

    return user.id in ids


async def send_temp(context, chat_id, text, *, html_mode=True,
                    user_id=None, kind="info", reply_to=None):
    """Sends a bot message that is deleted automatically after 6 hours."""

    parse_mode = "HTML" if html_mode else None

    if reply_to is not None:
        sent = await reply_to.reply_text(
            text, parse_mode=parse_mode, disable_web_page_preview=True
        )
    else:
        sent = await context.bot.send_message(
            chat_id=chat_id, text=text, parse_mode=parse_mode,
            disable_web_page_preview=True,
        )

    try:
        await asyncio.to_thread(
            add_bot_message, chat_id, sent.message_id, user_id, kind
        )
    except Exception as error:
        logger.warning("Could not schedule message deletion: %s", error)

    return sent


async def clear_user_warnings(bot, chat_id, user_id):
    try:
        ids = await asyncio.to_thread(pop_user_warnings, chat_id, user_id)
    except Exception as error:
        logger.warning("Could not load old warnings: %s", error)
        return

    for message_id in ids:
        await safe_delete(bot, chat_id, message_id)


async def require_admin(update, context):
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message

    if not chat or not user or not message:
        return False

    if await user_is_admin(context.bot, chat, message, user, fresh=True):
        return True

    await send_temp(
        context, chat.id, "You are not admin.",
        kind="info", reply_to=message,
    )

    return False


# ==========================================
# MAIN MODERATION
# ==========================================

async def check_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat or not user or user.is_bot:
        return

    kind = detect_kind(message)

    if not kind:
        return

    settings = await get_settings_cached(chat.id)
    rule = settings[kind]

    if not rule["enabled"]:
        return

    if await user_is_admin(context.bot, chat, message, user):
        return

    limit = rule["limit"]

    count = await asyncio.to_thread(
        increment_count, chat.id, user.id, kind, username_or_name(user)
    )

    await clear_user_warnings(context.bot, chat.id, user.id)

    # Within the limit: warning only
    if count <= limit:
        remaining = limit - count
        label = KIND_LABEL[kind]

        # Text warnings only when close to the limit (avoids spam)
        if kind == "text" and remaining > 3:
            return

        if remaining > 0:
            text = (
                f"⚠️ {mention(user)}, you have sent {count}/{limit} {label} today.\n"
                f"You can send {remaining} more today."
            )
        else:
            text = (
                f"⚠️ {mention(user)}, you reached the daily limit "
                f"({limit}/{limit}) for {label}.\n"
                f"The next one will result in a {format_duration(rule['mute'])} mute."
            )

        await send_temp(
            context, chat.id, text, user_id=user.id, kind="warn"
        )

        return

    # Over the limit: delete the message and mute
    try:
        await message.delete()
    except TelegramError as error:
        logger.error("Could not delete message (is bot admin?): %s", error)

    try:
        until = datetime.now(timezone.utc) + timedelta(seconds=rule["mute"])

        await context.bot.restrict_chat_member(
            chat_id=chat.id,
            user_id=user.id,
            permissions=ChatPermissions(can_send_messages=False),
            until_date=until,
        )

        await asyncio.to_thread(reset_counts, chat.id, user.id, kind)

        await send_temp(
            context, chat.id,
            f"🔇 {mention(user)} exceeded the daily {kind} limit and has been "
            f"muted for {format_duration(rule['mute'])}.",
            kind="mute",
        )

        logger.info("User %s muted in chat %s (%s)", user.id, chat.id, kind)

    except TelegramError as error:
        logger.error("Could not mute user %s: %s", user.id, error)

        await send_temp(
            context, chat.id,
            "I could not mute this member. Please make me admin with "
            "'Delete messages' and 'Restrict members' permissions.",
            kind="info",
        )


# ==========================================
# COMMANDS
# ==========================================

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message

    if not chat or not message:
        return

    await send_temp(
        context, chat.id, HELP_TEXT,
        html_mode=False, kind="info", reply_to=message,
    )


async def setwarn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message

    if not await require_admin(update, context):
        return

    args = context.args

    usage = (
        "Usage:\n"
        "/setwarn link 3 24h\n"
        "/setwarn img 5 12h\n"
        "/setwarn text 30 1h\n"
        "/setwarn img off\n\n"
        "Send /help for details."
    )

    async def reply(text):
        await send_temp(
            context, chat.id, text, html_mode=False,
            kind="info", reply_to=message,
        )

    if not args:
        await reply(usage)
        return

    kind = KIND_ALIASES.get(args[0].lower())

    if not kind:
        await reply("Type must be: link, img or text.\n\n" + usage)
        return

    settings = await get_settings_cached(chat.id)
    current = settings[kind]

    # /setwarn img off
    if len(args) >= 2 and args[1].lower() in ("off", "disable", "0"):
        await asyncio.to_thread(
            save_setting, chat.id, kind, False, current["limit"], current["mute"]
        )
        settings_cache.pop(chat.id, None)
        await reply(f"{kind} check is now OFF.")
        return

    if len(args) < 2 or not args[1].isdigit():
        await reply(usage)
        return

    limit = int(args[1])

    if not 1 <= limit <= 1000:
        await reply("Limit must be between 1 and 1000.")
        return

    mute = current["mute"]

    if len(args) >= 3:
        mute = parse_duration(args[2])

        if mute is None:
            await reply("Mute time must look like 30m, 6h or 1d (min 1m, max 365d).")
            return

    await asyncio.to_thread(save_setting, chat.id, kind, True, limit, mute)
    settings_cache.pop(chat.id, None)

    await reply(
        f"Saved: {kind} limit = {limit} per day. "
        f"Next one after that = {format_duration(mute)} mute."
    )


async def settings_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message

    if not await require_admin(update, context):
        return

    settings = await get_settings_cached(chat.id)

    lines = ["Current limits (per day):"]

    for kind in ("link", "img", "text"):
        rule = settings[kind]

        if rule["enabled"]:
            lines.append(
                f"{kind}: {rule['limit']} allowed, then "
                f"{format_duration(rule['mute'])} mute"
            )
        else:
            lines.append(f"{kind}: OFF")

    await send_temp(
        context, chat.id, "\n".join(lines),
        html_mode=False, kind="info", reply_to=message,
    )


async def warnlist(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message

    if not await require_admin(update, context):
        return

    rows = await asyncio.to_thread(get_warn_list, chat.id)

    if not rows:
        await send_temp(
            context, chat.id, "No members have warnings today.",
            html_mode=False, kind="info", reply_to=message,
        )
        return

    lines = ["Today's warnings:"]

    for user_id, username, kind, cnt in rows:
        lines.append(f"{html.escape(username or str(user_id))}: {cnt} {kind}")

    await send_temp(
        context, chat.id, "\n".join(lines), kind="info", reply_to=message
    )


async def removewarn(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    message = update.effective_message

    if not await require_admin(update, context):
        return

    target_id = None
    target_label = None

    if message.reply_to_message and message.reply_to_message.from_user:
        target = message.reply_to_message.from_user

        if target.is_bot:
            return

        target_id = target.id
        target_label = mention(target)

    elif context.args:
        arg = context.args[0]

        if arg.lstrip("@").isdigit():
            target_id = int(arg.lstrip("@"))
        else:
            target_id = await asyncio.to_thread(
                find_user_id_by_username, chat.id, arg
            )

        target_label = html.escape(arg)

    if not target_id:
        await send_temp(
            context, chat.id,
            "Reply to a member's message with /removewarn, "
            "or use /removewarn @username",
            html_mode=False, kind="info", reply_to=message,
        )
        return

    await asyncio.to_thread(reset_counts, chat.id, target_id)
    await clear_user_warnings(context.bot, chat.id, target_id)

    await send_temp(
        context, chat.id,
        f"Today's warnings have been removed for {target_label}.",
        kind="info", reply_to=message,
    )


# ==========================================
# WELCOME MESSAGE WHEN BOT IS ADDED
# ==========================================

async def welcome_new_bot(update: Update, context: ContextTypes.DEFAULT_TYPE):
    change = update.my_chat_member

    if not change or change.chat.type not in ("group", "supergroup"):
        return

    old_status = change.old_chat_member.status
    new_status = change.new_chat_member.status

    if old_status in ("left", "kicked") and new_status in ("member", "administrator"):
        settings_cache.pop(change.chat.id, None)

      await send_temp(
    context, change.chat.id,
    "👋 Hello! I'm Group Link Guardian, created by @Ox_bluee.\n\n"
    "I help keep your group safe by controlling unwanted links, "
    "images and spam.\n\n"
    "Please make me an admin with 'Delete Messages' and "
    "'Restrict Members' permissions.\n\n"
    "📌 Admins: Send /help to explore all commands.",
    html_mode=False, kind="info",
)


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE):
    if isinstance(context.error, RetryAfter):
        logger.warning("Telegram asked to slow down: %s", context.error)
        return

    logger.error("Error while processing an update:", exc_info=context.error)


# ==========================================
# BACKGROUND CLEANUP (deletes bot messages after 6 hours)
# ==========================================

async def cleanup_loop(app):
    while True:
        await asyncio.sleep(60)

        try:
            due = await asyncio.to_thread(pop_due_messages)

            for chat_id, message_id in due:
                await safe_delete(app.bot, chat_id, message_id)
                await asyncio.sleep(0.05)

        except Exception as error:
            logger.warning("Cleanup failed: %s", error)


# ==========================================
# SAFE STARTUP (never hammers Telegram on 429)
# ==========================================

def seconds_from(retry_after):
    if isinstance(retry_after, timedelta):
        return retry_after.total_seconds()

    return float(retry_after)


async def retry_forever(label, func):
    delay = 30

    while True:
        try:
            result = await func()
            logger.info("%s: OK", label)
            return result

        except InvalidToken:
            logger.critical(
                "%s: token is INVALID. Fix BOT_TOKEN and redeploy.", label
            )
            raise SystemExit(1)

        except RetryAfter as error:
            wait = max(seconds_from(error.retry_after) + random.uniform(5, 20), 60)
            logger.warning("%s: rate limited (429). Waiting %.0f s", label, wait)
            await asyncio.sleep(wait)

        except Exception as error:
            wait = delay + random.uniform(0, 10)
            logger.warning("%s: failed (%s). Retrying in %.0f s", label, error, wait)
            await asyncio.sleep(wait)
            delay = min(delay * 2, 900)


async def run():
    logger.info("Starting Telegram moderation bot...")

    start_health_server()
    start_keepalive()

    await asyncio.to_thread(setup_database)

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .connect_timeout(30)
        .read_timeout(30)
        .write_timeout(30)
        .pool_timeout(30)
        .get_updates_connect_timeout(30)
        .get_updates_read_timeout(30)
        .build()
    )

    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("start", help_command, filters.ChatType.PRIVATE))
    app.add_handler(CommandHandler("setwarn", setwarn, filters.ChatType.GROUPS))
    app.add_handler(CommandHandler("removewarn", removewarn, filters.ChatType.GROUPS))
    app.add_handler(CommandHandler("warnlist", warnlist, filters.ChatType.GROUPS))
    app.add_handler(CommandHandler("settings", settings_command, filters.ChatType.GROUPS))

    app.add_handler(
        ChatMemberHandler(welcome_new_bot, ChatMemberHandler.MY_CHAT_MEMBER)
    )

    app.add_handler(
        MessageHandler(
            filters.UpdateType.MESSAGE
            & filters.ChatType.GROUPS
            & (filters.TEXT | filters.CAPTION | filters.PHOTO)
            & ~filters.COMMAND,
            check_message,
        )
    )

    app.add_error_handler(error_handler)

    await retry_forever("Connect to Telegram", app.initialize)

    await app.start()

    await retry_forever(
        "Start polling",
        lambda: app.updater.start_polling(
            allowed_updates=["message", "my_chat_member"],
            drop_pending_updates=False,
            bootstrap_retries=0,
            poll_interval=1,
        ),
    )

    cleanup_task = asyncio.create_task(cleanup_loop(app))

    logger.info("Bot is ready and polling.")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:
            pass

    await stop.wait()

    logger.info("Shutting down...")
    cleanup_task.cancel()
    await app.updater.stop()
    await app.stop()
    await app.shutdown()


if __name__ == "__main__":
    asyncio.run(run())
