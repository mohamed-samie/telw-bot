import os
import re
import sqlite3
import logging
import hashlib
import time
import tempfile
import subprocess
import urllib.request
import subprocess
import tempfile
import shutil
from datetime import datetime
from urllib.parse import urlparse

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, ContextTypes,
    ConversationHandler, MessageHandler, filters
)

# ---------------- Logging ----------------
logging.basicConfig(
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    level=logging.INFO
)

# ---------------- Env ----------------
TOKEN = os.getenv("BOT_TOKEN", "").strip()
STORAGE_CHAT_ID = int(os.getenv("STORAGE_CHAT_ID", "0"))  # should be -100...
ADMIN_IDS = set(int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit())

# DB always next to this file (fixed path)
DB_PATH = os.path.join(os.path.dirname(__file__), "db.sqlite")

# ---------------- Tunables ----------------
MAX_SHEIKH_LEN = 60
MAX_MONTH_LEN = 30
CAPTION_MAX_LEN = 900

# unified batch size (10 as requested)
BATCH = 10

# Anti-spam
RATE_LIMIT_WINDOW_SEC = 60 * 60
RATE_LIMIT_MAX_UPLOADS = 20

# Browse paging
SHEIKHS_PAGE_SIZE = 12

# Link upload
URL_MAX_BYTES = 60 * 1024 * 1024  # 60MB (adjust if needed)
URL_TIMEOUT_SEC = 30

# ffmpeg silence trimming (simple + effective)
# NOTE: requires ffmpeg installed on the machine.
FFMPEG_SILENCE_FILTER = (
    "silenceremove=start_periods=1:start_duration=0.25:start_threshold=-45dB:"
    "stop_periods=1:stop_duration=0.8:stop_threshold=-45dB"
)

# Period presets for faster upload UX (optional)
PERIOD_PRESETS = [
    ("رمضان", "preset:period:رمضان"),
    ("تراويح", "preset:period:تراويح"),
    ("تهجد", "preset:period:تهجد"),
    ("قيام", "preset:period:قيام"),
]


# ---------------- Utils ----------------
def to_arabic_digits(s: str) -> str:
    trans = str.maketrans("0123456789", "٠١٢٣٤٥٦٧٨٩")
    return s.translate(trans)


def from_arabic_digits(s: str) -> str:
    trans = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
    return s.translate(trans)


def normalize_ar(text: str) -> str:
    if not text:
        return ""
    t = text.strip()
    t = re.sub(r"\s+", " ", t)
    t = t.replace("ـ", "")
    t = t.replace("أ", "ا").replace("إ", "ا").replace("آ", "ا")
    t = t.replace("ى", "ي")
    return t


def short_year(y: int) -> str:
    # 2026 -> "٢٦"
    yy = str(y)[-2:]
    return to_arabic_digits(yy)


def sheikh_key(name: str) -> str:
    # callback_data must be short
    return hashlib.sha1(name.encode("utf-8")).hexdigest()[:10]


def safe_caption(text: str) -> str:
    t = (text or "").strip()
    if len(t) > CAPTION_MAX_LEN:
        t = t[:CAPTION_MAX_LEN]
    return t


def is_url(s: str) -> bool:
    if not s:
        return False
    s = s.strip()
    if not (s.startswith("http://") or s.startswith("https://")):
        return False
    try:
        u = urlparse(s)
        return bool(u.scheme and u.netloc)
    except Exception:
        return False


def parse_period(text: str):
    """
    Expected: "<month/season> - <year>" e.g. "رمضان - ٢٠٢٦" or "رمضان-2026"
    Returns: (month_label, year_int) or None
    """
    if not text:
        return None
    t = text.strip().replace("—", "-").replace("–", "-")
    parts = [p.strip() for p in t.split("-")]
    if len(parts) < 2:
        return None

    month_label = normalize_ar(parts[0])[:MAX_MONTH_LEN]
    year_str = from_arabic_digits(parts[1])
    year_str = "".join(ch for ch in year_str if ch.isdigit())
    if len(month_label) < 2 or len(year_str) != 4:
        return None

    try:
        year = int(year_str)
        if year < 1900 or year > 2100:
            return None
    except Exception:
        return None

    return month_label, year


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def _dt_to_ts(s: str) -> float:
    try:
        dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        return dt.timestamp()
    except Exception:
        return 0.0
def ffmpeg_exists() -> bool:
    return shutil.which("ffmpeg") is not None


def clean_audio_ffmpeg(in_path: str, out_path: str) -> None:
    audio_filter = (
        "highpass=f=80,"
        "lowpass=f=12000,"
        "afftdn=nf=-25,"
        "silenceremove=start_periods=1:start_duration=0.4:start_threshold=-35dB:"
        "stop_periods=1:stop_duration=0.6:stop_threshold=-35dB,"
        "loudnorm=I=-16:TP=-1.5:LRA=11"
    )

    cmd = [
        "ffmpeg", "-y",
        "-i", in_path,
        "-vn",
        "-af", audio_filter,
        "-c:a", "libmp3lame", "-q:a", "4",
        out_path
    ]

    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"ffmpeg failed:\n{p.stderr[-2000:]}")

# ---------------- DB ----------------
def init_db():
    conn = db()
    cur = conn.cursor()

    cur.execute("PRAGMA journal_mode=WAL;")
    cur.execute("PRAGMA synchronous=NORMAL;")

    cur.execute("""
    CREATE TABLE IF NOT EXISTS approved_uploaders (
        user_id INTEGER PRIMARY KEY,
        approved_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)

    cur.execute("""
    CREATE TABLE IF NOT EXISTS sheikh_lookup (
        skey TEXT PRIMARY KEY,
        name TEXT UNIQUE NOT NULL
    )
    """)

    # Core recordings table (with migrations)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS recordings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sheikh TEXT NOT NULL,
        period_month TEXT NOT NULL,
        period_year INTEGER NOT NULL,
        storage_message_id INTEGER NOT NULL,
        media_type TEXT NOT NULL, -- voice/audio
        uploader_id INTEGER NOT NULL DEFAULT 0,
        storage_chat_id INTEGER NOT NULL DEFAULT 0,
        featured INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)

    # User last sheikh (quick access)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS user_last (
        user_id INTEGER PRIMARY KEY,
        sheikh TEXT NOT NULL,
        updated_at TEXT NOT NULL DEFAULT (datetime('now'))
    )
    """)

    # User favorites
    cur.execute("""
    CREATE TABLE IF NOT EXISTS favorites (
        user_id INTEGER NOT NULL,
        sheikh TEXT NOT NULL,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        PRIMARY KEY (user_id, sheikh)
    )
    """)

    # Lightweight migrations for older db.sqlite
    cur.execute("PRAGMA table_info(recordings)")
    cols = {r["name"] for r in cur.fetchall()}
    if "uploader_id" not in cols:
        cur.execute("ALTER TABLE recordings ADD COLUMN uploader_id INTEGER NOT NULL DEFAULT 0")
    if "storage_chat_id" not in cols:
        cur.execute("ALTER TABLE recordings ADD COLUMN storage_chat_id INTEGER NOT NULL DEFAULT 0")
    if "featured" not in cols:
        cur.execute("ALTER TABLE recordings ADD COLUMN featured INTEGER NOT NULL DEFAULT 0")
    if "period_year" not in cols:
        # If your very old DB doesn't have period_year at all, you should delete db.sqlite.
        # We won't attempt destructive migrations automatically.
        logging.warning("DB is missing period_year. Delete db.sqlite to recreate schema cleanly.")

    # Indexes
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rec_sheikh ON recordings(sheikh)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rec_period ON recordings(period_year, period_month)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rec_created ON recordings(created_at)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rec_featured ON recordings(featured)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_rec_storage_msg ON recordings(storage_message_id)")

    conn.commit()
    conn.close()


def is_approved(user_id: int) -> bool:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM approved_uploaders WHERE user_id = ?", (user_id,))
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def approve_user(user_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO approved_uploaders (user_id) VALUES (?)", (user_id,))
    conn.commit()
    conn.close()


def upsert_sheikh_key(name: str) -> str:
    k = sheikh_key(name)
    conn = db()
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO sheikh_lookup (skey, name) VALUES (?, ?)", (k, name))
    conn.commit()
    conn.close()
    return k


def get_sheikh_name_by_key(k: str) -> str:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT name FROM sheikh_lookup WHERE skey = ?", (k,))
    row = cur.fetchone()
    conn.close()
    return row["name"] if row else ""


def add_recording(sheikh: str, month_label: str, year: int, storage_message_id: int, media_type: str, uploader_id: int, storage_chat_id: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO recordings (sheikh, period_month, period_year, storage_message_id, media_type, uploader_id, storage_chat_id)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (sheikh, month_label, year, storage_message_id, media_type, uploader_id, storage_chat_id))
    conn.commit()
    conn.close()


def delete_by_storage_message_id(storage_message_id: int) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM recordings WHERE storage_message_id = ?", (storage_message_id,))
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def delete_last_recording() -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT id FROM recordings ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        conn.close()
        return 0
    last_id = row["id"]
    cur.execute("DELETE FROM recordings WHERE id = ?", (last_id,))
    conn.commit()
    conn.close()
    return 1


def delete_recordings_by_sheikh(sheikh: str) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("DELETE FROM recordings WHERE sheikh = ?", (sheikh,))
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def rename_sheikh(old: str, new: str) -> int:
    old_n = normalize_ar(old)
    new_n = normalize_ar(new)
    if not old_n or not new_n or old_n == new_n:
        return 0

    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE recordings SET sheikh = ? WHERE sheikh = ?", (new_n, old_n))
    n = cur.rowcount

    # move favorites + user_last
    cur.execute("UPDATE OR IGNORE favorites SET sheikh = ? WHERE sheikh = ?", (new_n, old_n))
    cur.execute("UPDATE user_last SET sheikh = ?, updated_at = datetime('now') WHERE sheikh = ?", (new_n, old_n))

    # update lookup
    try:
        new_k = sheikh_key(new_n)
        cur.execute("INSERT OR IGNORE INTO sheikh_lookup (skey, name) VALUES (?, ?)", (new_k, new_n))
        cur.execute("DELETE FROM sheikh_lookup WHERE name = ?", (old_n,))
    except Exception:
        pass

    conn.commit()
    conn.close()
    return n


def set_last_sheikh(user_id: int, sheikh: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO user_last (user_id, sheikh, updated_at)
        VALUES (?, ?, datetime('now'))
        ON CONFLICT(user_id) DO UPDATE SET sheikh=excluded.sheikh, updated_at=datetime('now')
    """, (user_id, sheikh))
    conn.commit()
    conn.close()


def get_last_sheikh(user_id: int) -> str:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT sheikh FROM user_last WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row["sheikh"] if row else ""


def is_favorite(user_id: int, sheikh: str) -> bool:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM favorites WHERE user_id = ? AND sheikh = ?", (user_id, sheikh))
    ok = cur.fetchone() is not None
    conn.close()
    return ok


def toggle_favorite(user_id: int, sheikh: str) -> bool:
    """
    Returns True if now favorited, False if removed.
    """
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM favorites WHERE user_id = ? AND sheikh = ?", (user_id, sheikh))
    exists = cur.fetchone() is not None
    if exists:
        cur.execute("DELETE FROM favorites WHERE user_id = ? AND sheikh = ?", (user_id, sheikh))
        conn.commit()
        conn.close()
        return False
    cur.execute("INSERT OR IGNORE INTO favorites (user_id, sheikh) VALUES (?, ?)", (user_id, sheikh))
    conn.commit()
    conn.close()
    return True


def list_favorites(user_id: int, limit: int = 20, offset: int = 0):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT sheikh
        FROM favorites
        WHERE user_id = ?
        ORDER BY created_at DESC
        LIMIT ? OFFSET ?
    """, (user_id, limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows


def count_favorites(user_id: int) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM favorites WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return int(row["c"]) if row else 0


def set_featured_by_storage_message_id(storage_message_id: int, featured: int) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("UPDATE recordings SET featured = ? WHERE storage_message_id = ?", (1 if featured else 0, storage_message_id))
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def set_featured_last(featured: int) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT storage_message_id FROM recordings ORDER BY id DESC LIMIT 1")
    row = cur.fetchone()
    if not row:
        conn.close()
        return 0
    mid = int(row["storage_message_id"])
    cur.execute("UPDATE recordings SET featured = ? WHERE storage_message_id = ?", (1 if featured else 0, mid))
    n = cur.rowcount
    conn.commit()
    conn.close()
    return n


def uploads_in_window(user_id: int, window_sec: int) -> int:
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT created_at
        FROM recordings
        WHERE uploader_id = ?
        ORDER BY id DESC
        LIMIT 200
    """, (user_id,))
    rows = cur.fetchall()
    conn.close()

    now = time.time()
    n = 0
    for r in rows:
        ts = _dt_to_ts(r["created_at"])
        if ts and (now - ts <= window_sec):
            n += 1
    return n


def list_sheikhs_with_counts(offset: int = 0, limit: int = 40, q: str = ""):
    conn = db()
    cur = conn.cursor()
    if q:
        like = f"%{q}%"
        cur.execute("""
            SELECT sheikh, COUNT(*) AS cnt
            FROM recordings
            WHERE sheikh LIKE ?
            GROUP BY sheikh
            ORDER BY cnt DESC, sheikh ASC
            LIMIT ? OFFSET ?
        """, (like, limit, offset))
    else:
        cur.execute("""
            SELECT sheikh, COUNT(*) AS cnt
            FROM recordings
            GROUP BY sheikh
            ORDER BY cnt DESC, sheikh ASC
            LIMIT ? OFFSET ?
        """, (limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows


def count_sheikhs(q: str = "") -> int:
    conn = db()
    cur = conn.cursor()
    if q:
        like = f"%{q}%"
        cur.execute("""
            SELECT COUNT(*) AS c FROM (
                SELECT sheikh
                FROM recordings
                WHERE sheikh LIKE ?
                GROUP BY sheikh
            )
        """, (like,))
    else:
        cur.execute("""
            SELECT COUNT(*) AS c FROM (
                SELECT sheikh
                FROM recordings
                GROUP BY sheikh
            )
        """)
    row = cur.fetchone()
    conn.close()
    return int(row["c"]) if row else 0


def get_sheikh_stats(sheikh: str):
    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) AS c FROM recordings WHERE sheikh = ?", (sheikh,))
    total = int(cur.fetchone()["c"])
    cur.execute("SELECT COUNT(DISTINCT period_year) AS c FROM recordings WHERE sheikh = ?", (sheikh,))
    years = int(cur.fetchone()["c"])
    conn.close()
    return total, years


def list_periods_for_sheikh(sheikh: str, limit: int = 20):
    """
    Returns rows: (period_month, period_year, cnt) ordered newest first.
    """
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT period_month AS m, period_year AS y, COUNT(*) AS cnt
        FROM recordings
        WHERE sheikh = ?
        GROUP BY m, y
        ORDER BY y DESC, cnt DESC, m ASC
        LIMIT ?
    """, (sheikh, limit))
    rows = cur.fetchall()
    conn.close()
    return rows


def list_recordings_for_sheikh_year_month(sheikh: str, year: int, month_label: str, limit: int, offset: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT storage_message_id, media_type, storage_chat_id
        FROM recordings
        WHERE sheikh = ? AND period_year = ? AND period_month = ?
        ORDER BY id ASC
        LIMIT ? OFFSET ?
    """, (sheikh, year, month_label, limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows


def list_latest_for_sheikh(sheikh: str, limit: int, offset: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT storage_message_id, media_type, storage_chat_id, period_month, period_year
        FROM recordings
        WHERE sheikh = ?
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (sheikh, limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows


def list_featured_for_sheikh(sheikh: str, limit: int, offset: int):
    conn = db()
    cur = conn.cursor()
    cur.execute("""
        SELECT storage_message_id, media_type, storage_chat_id, period_month, period_year
        FROM recordings
        WHERE sheikh = ? AND featured = 1
        ORDER BY id DESC
        LIMIT ? OFFSET ?
    """, (sheikh, limit, offset))
    rows = cur.fetchall()
    conn.close()
    return rows


# ---------------- Media processing (URL + trim silence) ----------------
def _ffmpeg_exists() -> bool:
    try:
        subprocess.run(["ffmpeg", "-version"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return True
    except Exception:
        return False


def download_url_to_file(url: str, out_path: str) -> int:
    """
    Downloads URL to out_path. Returns bytes downloaded.
    """
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=URL_TIMEOUT_SEC) as resp:
        total = 0
        with open(out_path, "wb") as f:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                f.write(chunk)
                total += len(chunk)
                if total > URL_MAX_BYTES:
                    raise RuntimeError("File too large")
        return total


def trim_silence_ffmpeg(in_path: str, out_path: str) -> None:
    """
    Produces out_path using ffmpeg silenceremove.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", in_path,
        "-vn",
        "-af", FFMPEG_SILENCE_FILTER,
        "-c:a", "libmp3lame",
        "-q:a", "4",
        out_path
    ]
    subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=True)


# ---------------- UI ----------------
def main_menu_kb(approved: bool, last_sheikh: str = "", fav_count: int = 0):
    buttons = [
        [InlineKeyboardButton("🔎 بحث عن شيخ", callback_data="menu:search")],
        [InlineKeyboardButton("📚 عرض المشايخ", callback_data="menu:browse:0")],
    ]

    # Quick access (optional but requested)
    if last_sheikh:
        k = upsert_sheikh_key(last_sheikh)
        buttons.append([InlineKeyboardButton("⚡ آخر شيخ", callback_data=f"sheikhk:{k}")])

    if fav_count > 0:
        buttons.append([InlineKeyboardButton("⭐ مفضلتي", callback_data="menu:favs:0")])

    if approved:
        buttons.append([InlineKeyboardButton("⬆️ رفع تسجيل لشيخ", callback_data="menu:upload")])
    else:
        buttons.append([InlineKeyboardButton("📝 طلب صلاحية رفع", callback_data="menu:request_upload")])

    return InlineKeyboardMarkup(buttons)


def back_to_menu_kb():
    return InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="menu:back")]])


def sheikh_actions_kb(k: str, sheikh_name: str, uid: int):
    """
    After picking a sheikh: show 3 main options:
    1) Featured
    2) Period buttons like "رمضان ٢٥"
    3) Latest
    + small favorite toggle row
    """
    total, years = get_sheikh_stats(sheikh_name)
    fav = is_favorite(uid, sheikh_name)

    # 1) Featured + Latest
    buttons = [
        [
            InlineKeyboardButton("⭐ تسجيلات مميزة", callback_data=f"feat:{k}:0"),
            InlineKeyboardButton("🆕 أحدث التسجيلات", callback_data=f"latest:{k}:0"),
        ]
    ]

    # 2) Periods as direct buttons "رمضان ٢٥"
    periods = list_periods_for_sheikh(sheikh_name, limit=12)
    period_buttons = []
    for r in periods:
        m = r["m"]
        y = int(r["y"])
        cnt = int(r["cnt"])
        label = f"{m} {short_year(y)} ({to_arabic_digits(str(cnt))})"
        period_buttons.append(InlineKeyboardButton(label, callback_data=f"per:{k}:{y}:{m}:0"))

    # Put periods in 1-column to avoid overflow
    for b in period_buttons:
        buttons.append([b])

    # Favorite toggle + back
    fav_label = "⭐ إزالة من المفضلة" if fav else "⭐ إضافة للمفضلة"
    buttons.append([InlineKeyboardButton(fav_label, callback_data=f"fav:{k}")])
    buttons.append([InlineKeyboardButton("⬅️ رجوع للمشايخ", callback_data="menu:browse:0")])

    header = (
        f"🎙 {sheikh_name}\n"
        f"📌 عدد التسجيلات: {to_arabic_digits(str(total))} | عدد السنين: {to_arabic_digits(str(years))}\n\n"
        "اختار:"
    )
    return header, InlineKeyboardMarkup(buttons)


# ---------------- Conversations ----------------
ASK_SHEIKH, ASK_PERIOD, WAIT_MEDIA, ASK_SEARCH = range(4)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    last = get_last_sheikh(uid)
    fav_count = count_favorites(uid)
    await update.message.reply_text("اختار من القائمة:", reply_markup=main_menu_kb(is_approved(uid), last, fav_count))


async def menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    last = get_last_sheikh(uid)
    fav_count = count_favorites(uid)
    await update.message.reply_text("اختار من القائمة:", reply_markup=main_menu_kb(is_approved(uid), last, fav_count))


async def whoami(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    username = f"@{u.username}" if u.username else "(no username)"
    await update.message.reply_text(f"user_id: {u.id}\nusername: {username}")


async def debug(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    await update.message.reply_text(f"chat_id = {chat.id}\nchat_type = {chat.type}")


# ---------------- Admin Commands ----------------
async def wipe_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    n = delete_last_recording()
    await update.message.reply_text("✅ تم حذف آخر تسجيل من قاعدة البيانات." if n else "مفيش تسجيلات اتحذف.")


async def wipe_sheikh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    name = normalize_ar(" ".join(context.args).strip())
    if not name:
        await update.message.reply_text("اكتب: /wipe_sheikh اسم_الشيخ")
        return
    n = delete_recordings_by_sheikh(name)
    await update.message.reply_text(f"✅ تم حذف {n} تسجيل(ات) للشيخ: {name}")


async def rename_sheikh_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    args = " ".join(context.args).strip()
    if "|" not in args:
        await update.message.reply_text("اكتب: /rename_sheikh القديم | الجديد")
        return
    old, new = [a.strip() for a in args.split("|", 1)]
    n = rename_sheikh(old, new)
    await update.message.reply_text(f"✅ تم تحديث {n} تسجيل(ات).")


async def feature_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    n = set_featured_last(1)
    await update.message.reply_text("✅ تم تمييز آخر تسجيل." if n else "مفيش تسجيلات.")


async def unfeature_last(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    n = set_featured_last(0)
    await update.message.reply_text("✅ تم إلغاء تمييز آخر تسجيل." if n else "مفيش تسجيلات.")


async def feature_storage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    if not context.args:
        await update.message.reply_text("اكتب: /feature_storage STORAGE_MESSAGE_ID")
        return
    try:
        mid = int(from_arabic_digits(context.args[0]))
    except Exception:
        await update.message.reply_text("اكتب رقم message_id صحيح.")
        return
    n = set_featured_by_storage_message_id(mid, 1)
    await update.message.reply_text("✅ تم تمييز التسجيل." if n else "مش لاقي التسجيل ده في DB.")


async def unfeature_storage(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    if not context.args:
        await update.message.reply_text("اكتب: /unfeature_storage STORAGE_MESSAGE_ID")
        return
    try:
        mid = int(from_arabic_digits(context.args[0]))
    except Exception:
        await update.message.reply_text("اكتب رقم message_id صحيح.")
        return
    n = set_featured_by_storage_message_id(mid, 0)
    await update.message.reply_text("✅ تم إلغاء تمييز التسجيل." if n else "مش لاقي التسجيل ده في DB.")


async def cleanup_orphans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in ADMIN_IDS:
        await update.message.reply_text("مش مسموح.")
        return
    if STORAGE_CHAT_ID == 0:
        await update.message.reply_text("⚠️ STORAGE_CHAT_ID مش متظبط.")
        return

    conn = db()
    cur = conn.cursor()
    cur.execute("SELECT storage_message_id, storage_chat_id FROM recordings ORDER BY id DESC")
    rows = cur.fetchall()
    conn.close()

    removed = 0
    checked = 0
    for r in rows:
        checked += 1
        mid = int(r["storage_message_id"])
        schat = int(r["storage_chat_id"] or 0) or STORAGE_CHAT_ID
        try:
            await context.bot.copy_message(chat_id=uid, from_chat_id=schat, message_id=mid)
        except Exception as e:
            msg = str(e).lower()
            if "not found" in msg or "message to copy not found" in msg or "message_id_invalid" in msg:
                removed += delete_by_storage_message_id(mid)

    await update.message.reply_text(f"✅ Cleanup done.\nChecked: {checked}\nRemoved: {removed}")


# ---------------- Upload Conversation ----------------
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("تم الإلغاء.")
    return ConversationHandler.END


async def start_upload_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    uid = q.from_user.id
    if not is_approved(uid):
        await q.edit_message_text("لازم موافقة الأدمن قبل الرفع. اضغط: 📝 طلب صلاحية رفع")
        return ConversationHandler.END

    await q.edit_message_text("تمام ✅ اكتب اسم الشيخ/القارئ:")
    return ASK_SHEIKH


async def ask_sheikh(update: Update, context: ContextTypes.DEFAULT_TYPE):
    name = normalize_ar(update.message.text or "")
    if len(name) < 2 or len(name) > MAX_SHEIKH_LEN:
        await update.message.reply_text(f"اكتب اسم الشيخ بشكل صحيح (من 2 إلى {MAX_SHEIKH_LEN} حرف).")
        return ASK_SHEIKH

    context.user_data["sheikh"] = name

    kb = InlineKeyboardMarkup(
        [[InlineKeyboardButton(txt, callback_data=cb)] for (txt, cb) in PERIOD_PRESETS] +
        [[InlineKeyboardButton("✍️ هكتبها بنفسي", callback_data="preset:period:manual")]]
    )
    await update.message.reply_text("اختار الشهر/الموسم (أو اكتبها بنفسك):", reply_markup=kb)
    return ASK_PERIOD


async def ask_period_text_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # If preset month chosen earlier and we asked for year only
    if context.user_data.get("__preset_month_only"):
        year_str = from_arabic_digits(update.message.text or "")
        year_str = "".join(ch for ch in year_str if ch.isdigit())
        if len(year_str) != 4:
            await update.message.reply_text("اكتب السنة فقط (٤ أرقام). مثال: ٢٠٢٦ أو 2026")
            return ASK_PERIOD
        try:
            year = int(year_str)
            if year < 1900 or year > 2100:
                raise ValueError("year range")
        except Exception:
            await update.message.reply_text("السنة غير صحيحة.")
            return ASK_PERIOD

        month_label = (context.user_data.get("period_month") or "").strip()
        if len(month_label) < 2:
            await update.message.reply_text("حصلت مشكلة في الشهر. ابدأ من /start.")
            return ConversationHandler.END

        context.user_data["period_year"] = year
        context.user_data.pop("__preset_month_only", None)

        await update.message.reply_text(
            "تمام ✅ ابعت التسجيل الآن (Voice أو Audio)\n"
            "أو ابعت لينك مباشر لملف صوت (mp3/m4a/wav) عشان نرفعه وننضفه من السكتات."
        )
        return WAIT_MEDIA

    # Normal parse: "<month> - <year>"
    parsed = parse_period(update.message.text or "")
    if not parsed:
        await update.message.reply_text("الصيغة غير صحيحة.\nمثال: رمضان - ٢٠٢٦ (أو رمضان - 2026)")
        return ASK_PERIOD

    month_label, year = parsed
    context.user_data["period_month"] = month_label
    context.user_data["period_year"] = year

    await update.message.reply_text(
        "تمام ✅ ابعت التسجيل الآن (Voice أو Audio)\n"
        "أو ابعت لينك مباشر لملف صوت (mp3/m4a/wav) عشان نرفعه وننضفه من السكتات."
    )
    return WAIT_MEDIA


async def receive_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    WAIT_MEDIA:
    - accept voice/audio
    - accept direct URL text -> download -> trim silence (if ffmpeg) -> upload as audio
    """
    if STORAGE_CHAT_ID == 0:
        await update.message.reply_text("⚠️ STORAGE_CHAT_ID مش متظبط.")
        return ConversationHandler.END

    uid = update.effective_user.id
    if not is_approved(uid):
        await update.message.reply_text("مش مسموح بالرفع بدون موافقة الأدمن.")
        return ConversationHandler.END

    # rate limit
    n_recent = uploads_in_window(uid, RATE_LIMIT_WINDOW_SEC)
    if n_recent >= RATE_LIMIT_MAX_UPLOADS and uid not in ADMIN_IDS:
        await update.message.reply_text(
            f"⚠️ علشان نمنع السبام: وصلت للحد الأقصى ({RATE_LIMIT_MAX_UPLOADS}) في آخر ساعة.\nجرب بعد شوية."
        )
        return ConversationHandler.END

    sheikh = (context.user_data.get("sheikh", "") or "").strip()
    month_label = (context.user_data.get("period_month", "") or "").strip()
    year = context.user_data.get("period_year", None)

    if not (sheikh and month_label and isinstance(year, int)):
        await update.message.reply_text("حصلت مشكلة في البيانات. ابدأ من /start.")
        return ConversationHandler.END

    msg = update.message
    media_type = None
    file_id = None

    # Case A: Voice/Audio
    if msg.voice:
        media_type = "voice"
        file_id = msg.voice.file_id
    elif msg.audio:
        media_type = "audio"
        file_id = msg.audio.file_id

    # Case B: URL
    if not media_type and msg.text and is_url(msg.text):
        url = msg.text.strip()
        upsert_sheikh_key(sheikh)

        period_display = f"{month_label} - {to_arabic_digits(str(year))}"
        await msg.reply_text("⏳ جاري تحميل الرابط وتنظيفه من السكتات...")

        # temp paths
        with tempfile.TemporaryDirectory() as td:
            in_path = os.path.join(td, "input_audio")
            out_path = os.path.join(td, "cleaned.mp3")

            try:
                download_url_to_file(url, in_path)
            except Exception as e:
                logging.warning("URL download failed: %s", e)
                await msg.reply_text("❌ فشل تحميل الرابط. تأكد إنه لينك مباشر لملف صوت وحجمه مناسب.")
                return ConversationHandler.END

            cleaned_path = in_path
            cleaned_note = ""
            if _ffmpeg_exists():
                try:
                    trim_silence_ffmpeg(in_path, out_path)
                    cleaned_path = out_path
                    cleaned_note = "🧹 تم تنظيف السكتات"
                except Exception as e:
                    logging.warning("ffmpeg trim failed: %s", e)
                    cleaned_note = "⚠️ لم يتم تنظيف السكتات (مشكلة في ffmpeg)"
            else:
                cleaned_note = "⚠️ ffmpeg غير موجود على السيرفر (تم الرفع بدون تنظيف)"

            caption = safe_caption(
                f"🎙 {sheikh}\n🗓 {period_display}\n👤 uploader: {uid}\n🔗 source: url\n{cleaned_note}"
            )

            try:
                stored = await context.bot.send_audio(
                    STORAGE_CHAT_ID,
                    audio=open(cleaned_path, "rb"),
                    caption=caption
                )
            except Exception as e:
                logging.exception("Failed to send cleaned audio to storage: %s", e)
                await msg.reply_text("❌ حصل خطأ أثناء رفع الملف للتخزين.")
                return ConversationHandler.END

        add_recording(sheikh, month_label, year, stored.message_id, "audio", uploader_id=uid, storage_chat_id=STORAGE_CHAT_ID)

        await msg.reply_text(
            "✅ تم حفظ التسجيل من الرابط.\n\n"
            f"🎙 الشيخ: {sheikh}\n"
            f"🗓 الفترة: {period_display}\n\n"
            "تقدر تلاقيه من: 📚 عرض المشايخ / 🔎 بحث عن شيخ"
        )
        return ConversationHandler.END

    # If not voice/audio/url
    if not media_type:
        await msg.reply_text("ابعت Voice أو Audio فقط، أو لينك مباشر لملف صوت.")
        return WAIT_MEDIA
    # send to storage using file_id (with silence trimming)
    upsert_sheikh_key(sheikh)
    period_display = f"{month_label} - {to_arabic_digits(str(year))}"
    caption_base = f"🎙 {sheikh}\n🗓 {period_display}\n👤 uploader: {uid}"

    await msg.reply_text("⏳ جاري تنظيف التسجيل من السكتات...")

    stored = None
    stored_media_type = media_type  # will become "audio" if cleaned successfully
    cleaned_note = ""

    try:
        # Download telegram file locally
        tg_file = await context.bot.get_file(file_id)

        with tempfile.TemporaryDirectory() as td:
            # keep original extension if possible
            in_path = os.path.join(td, "tg_input")
            out_path = os.path.join(td, "cleaned.mp3")

            # Download to disk
            await tg_file.download_to_drive(custom_path=in_path)

            # Try trimming using ffmpeg (if available)
            if _ffmpeg_exists():
                try:
                    trim_silence_ffmpeg(in_path, out_path)
                    cleaned_note = "🧹 تم تنظيف السكتات"
                    stored_media_type = "audio"  # cleaned output is mp3
                    caption = safe_caption(caption_base + "\n" + cleaned_note)

                    stored = await context.bot.send_audio(
                        STORAGE_CHAT_ID,
                        audio=open(out_path, "rb"),
                        caption=caption
                    )
                except Exception as e:
                    logging.warning("ffmpeg trim failed, fallback to original: %s", e)
                    cleaned_note = "⚠️ فشل تنظيف السكتات (تم حفظ الأصل)"
            else:
                cleaned_note = "⚠️ ffmpeg غير موجود (تم حفظ الأصل)"

    except Exception as e:
        logging.warning("Download/processing failed, fallback to original: %s", e)
        cleaned_note = "⚠️ تعذر التنظيف (تم حفظ الأصل)"

    # Fallback: send original file_id (voice/audio) if cleaning didn't produce stored message
    if stored is None:
        caption = safe_caption(caption_base + ("\n" + cleaned_note if cleaned_note else ""))
        try:
            if media_type == "voice":
                stored = await context.bot.send_voice(STORAGE_CHAT_ID, file_id, caption=caption)
            else:
                stored = await context.bot.send_audio(STORAGE_CHAT_ID, file_id, caption=caption)
        except Exception as e:
            logging.exception("Failed to send to storage: %s", e)
            await msg.reply_text("حصل خطأ أثناء الحفظ في التخزين. جرب تاني.")
            return ConversationHandler.END

    add_recording(
        sheikh, month_label, year,
        stored.message_id,
        stored_media_type,
        uploader_id=uid,
        storage_chat_id=STORAGE_CHAT_ID
    )

    await msg.reply_text(
        "✅ تم حفظ التسجيل.\n\n"
        f"🎙 الشيخ: {sheikh}\n"
        f"🗓 الفترة: {period_display}\n\n"
        "تقدر تلاقيه من: 📚 عرض المشايخ / 🔎 بحث عن شيخ"
    )
    return ConversationHandler.END
   

    add_recording(sheikh, month_label, year, stored.message_id, media_type, uploader_id=uid, storage_chat_id=STORAGE_CHAT_ID)

    await msg.reply_text(
        "✅ تم حفظ التسجيل.\n\n"
        f"🎙 الشيخ: {sheikh}\n"
        f"🗓 الفترة: {period_display}\n\n"
        "تقدر تلاقيه من: 📚 عرض المشايخ / 🔎 بحث عن شيخ"
    )
    return ConversationHandler.END


# ---------------- Search Conversation ----------------
async def start_search_from_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    await q.edit_message_text("اكتب جزء من اسم الشيخ للبحث:")
    return ASK_SEARCH


async def search_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    term = normalize_ar(update.message.text or "")
    if len(term) < 2:
        await update.message.reply_text("اكتب حرفين على الأقل.")
        return ASK_SEARCH

    context.user_data["search_q"] = term
    await update.message.reply_text("جارٍ البحث...")
    await show_sheikhs_page(update, context, page=0, q=term, from_message=True)
    return ConversationHandler.END


# ---------------- Browse UI helpers ----------------
async def show_sheikhs_page(update: Update, context: ContextTypes.DEFAULT_TYPE, page: int, q: str = "", from_message: bool = False, qobj=None):
    page = max(0, int(page))
    offset = page * SHEIKHS_PAGE_SIZE

    total = count_sheikhs(q=q)
    rows = list_sheikhs_with_counts(offset=offset, limit=SHEIKHS_PAGE_SIZE, q=q)

    if not rows:
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع", callback_data="menu:back")]])
        text = "مفيش نتائج." if q else "لسه مفيش تسجيلات مرفوعة."
        if from_message:
            await update.message.reply_text(text, reply_markup=kb)
        else:
            await qobj.edit_message_text(text, reply_markup=kb)
        return

    buttons = []
    for r in rows:
        name = r["sheikh"]
        cnt = r["cnt"]
        k = upsert_sheikh_key(name)
        buttons.append([InlineKeyboardButton(f"{name} ({cnt})", callback_data=f"sheikhk:{k}")])

    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"menu:browse:{page-1}" + (f":{q}" if q else "")))
    if offset + SHEIKHS_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("التالي ➡️", callback_data=f"menu:browse:{page+1}" + (f":{q}" if q else "")))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("⬅️ رجوع", callback_data="menu:back")])

    header = "نتائج البحث:" if q else "اختار الشيخ:"
    hint = f"\n({to_arabic_digits(str(offset+1))}-{to_arabic_digits(str(min(offset+SHEIKHS_PAGE_SIZE, total)))} من {to_arabic_digits(str(total))})"
    text = header + hint

    if from_message:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await qobj.edit_message_text(text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_favs_page(q, context, page: int, uid: int):
    page = max(0, int(page))
    offset = page * SHEIKHS_PAGE_SIZE
    total = count_favorites(uid)
    rows = list_favorites(uid, limit=SHEIKHS_PAGE_SIZE, offset=offset)

    if not rows:
        await q.edit_message_text("⭐ مفيش مفضلة لسه.", reply_markup=back_to_menu_kb())
        return

    buttons = []
    for r in rows:
        name = r["sheikh"]
        k = upsert_sheikh_key(name)
        buttons.append([InlineKeyboardButton(name, callback_data=f"sheikhk:{k}")])

    nav = []
    if offset > 0:
        nav.append(InlineKeyboardButton("⬅️ السابق", callback_data=f"menu:favs:{page-1}"))
    if offset + SHEIKHS_PAGE_SIZE < total:
        nav.append(InlineKeyboardButton("التالي ➡️", callback_data=f"menu:favs:{page+1}"))
    if nav:
        buttons.append(nav)

    buttons.append([InlineKeyboardButton("⬅️ رجوع", callback_data="menu:back")])

    hint = f"({to_arabic_digits(str(offset+1))}-{to_arabic_digits(str(min(offset+SHEIKHS_PAGE_SIZE, total)))} من {to_arabic_digits(str(total))})"
    await q.edit_message_text("⭐ مفضلتي\n" + hint, reply_markup=InlineKeyboardMarkup(buttons))


# ---------------- Sending recordings (unified batch = 10) ----------------
async def send_batch_by_rows(context, chat_id: int, name: str, rows, title_line: str):
    sent = 0
    skipped_deleted = 0

    for r in rows:
        storage_message_id = int(r["storage_message_id"])
        schat = int(r["storage_chat_id"] or 0) or STORAGE_CHAT_ID

        cap = safe_caption(title_line)

        # Try copy with caption; if fails, fallback copy without caption + text line
        try:
            await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=schat,
                message_id=storage_message_id,
                caption=cap
            )
            sent += 1
        except Exception as e:
            msg = str(e).lower()
            logging.warning("copy_message failed (msg_id=%s): %s", storage_message_id, e)

            if "not found" in msg or "message to copy not found" in msg or "message_id_invalid" in msg:
                delete_by_storage_message_id(storage_message_id)
                skipped_deleted += 1
                continue

            try:
                await context.bot.copy_message(
                    chat_id=chat_id,
                    from_chat_id=schat,
                    message_id=storage_message_id
                )
                await context.bot.send_message(chat_id, cap)
                sent += 1
            except Exception as e2:
                msg2 = str(e2).lower()
                logging.warning("fallback copy failed (msg_id=%s): %s", storage_message_id, e2)
                if "not found" in msg2 or "message to copy not found" in msg2 or "message_id_invalid" in msg2:
                    delete_by_storage_message_id(storage_message_id)
                    skipped_deleted += 1
                continue

    return sent, skipped_deleted


# ---------------- Click Handler ----------------
async def on_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()

    uid = q.from_user.id
    approved = is_approved(uid)

    # Back to main
    if q.data == "menu:back":
        last = get_last_sheikh(uid)
        fav_count = count_favorites(uid)
        await q.edit_message_text("اختار من القائمة:", reply_markup=main_menu_kb(approved, last, fav_count))
        return

    # Browse sheikhs (paged)
    if q.data.startswith("menu:browse"):
        parts = q.data.split(":", 3)
        page = 0
        qterm = ""
        if len(parts) >= 3 and parts[2].isdigit():
            page = int(parts[2])
        if len(parts) == 4:
            qterm = parts[3]
        await show_sheikhs_page(update, context, page=page, q=qterm, from_message=False, qobj=q)
        return

    # Favorites page
    if q.data.startswith("menu:favs:"):
        page = int(q.data.split("menu:favs:", 1)[1])
        await show_favs_page(q, context, page=page, uid=uid)
        return

    # Request upload permission
    if q.data == "menu:request_upload":
        if not ADMIN_IDS:
            await q.edit_message_text("⚠️ ADMIN_IDS مش متظبط عند الأدمن.")
            return

        user = q.from_user
        uname = f"@{user.username}" if user.username else user.full_name

        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Approve", callback_data=f"admin:approve:{user.id}"),
            InlineKeyboardButton("❌ Reject", callback_data=f"admin:reject:{user.id}"),
        ]])

        text = f"📝 طلب صلاحية رفع\nالمستخدم: {uname}\nuser_id: {user.id}"
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(admin_id, text, reply_markup=kb)
            except Exception as e:
                logging.warning("Failed to notify admin %s: %s", admin_id, e)

        await q.edit_message_text("تم إرسال طلبك للأدمن ✅\nهيوصلك رد بعد المراجعة.")
        return

    # Admin actions
    if q.data.startswith("admin:"):
        if uid not in ADMIN_IDS:
            await q.answer("مش مسموح.", show_alert=True)
            return

        _, action, target = q.data.split(":")
        target_id = int(target)

        if action == "approve":
            approve_user(target_id)
            await q.edit_message_text(f"✅ تم قبول المستخدم {target_id}")
            try:
                await context.bot.send_message(target_id, "✅ تم قبول طلبك. افتح /start (أو /menu) وهتلاقي زر الرفع.")
            except Exception:
                pass
        else:
            await q.edit_message_text(f"❌ تم رفض المستخدم {target_id}")
            try:
                await context.bot.send_message(target_id, "❌ تم رفض طلبك حاليًا.")
            except Exception:
                pass
        return

    # Period preset inside upload flow
    if q.data.startswith("preset:period:"):
        val = q.data.split("preset:period:", 1)[1]
        if val == "manual":
            await q.edit_message_text("اكتب شهر/موسم وسنة بالشكل:\nمثال: رمضان - ٢٠٢٦")
            return
        # store preset month and ask year
        context.user_data["period_month"] = normalize_ar(val)[:MAX_MONTH_LEN]
        context.user_data["__preset_month_only"] = True
        await q.edit_message_text(f"تمام ✅ اكتب السنة فقط (مثال: ٢٠٢٦ أو 2026)")
        return

    # Favorite toggle
    if q.data.startswith("fav:"):
        k = q.data.split("fav:", 1)[1]
        name = get_sheikh_name_by_key(k)
        if not name:
            await q.edit_message_text("حصلت مشكلة. ارجع وحاول تاني.")
            return
        now_fav = toggle_favorite(uid, name)
        header, kb = sheikh_actions_kb(k, name, uid)
        note = "✅ تم الإضافة للمفضلة" if now_fav else "✅ تم الإزالة من المفضلة"
        await q.edit_message_text(header + "\n\n" + note, reply_markup=kb)
        return

    # Sheikh selected -> Sheikh actions (3 options + periods)
    if q.data.startswith("sheikhk:"):
        k = q.data.split("sheikhk:", 1)[1]
        name = get_sheikh_name_by_key(k)
        if not name:
            await q.edit_message_text("حصلت مشكلة. ارجع وحاول تاني.")
            return
        set_last_sheikh(uid, name)
        header, kb = sheikh_actions_kb(k, name, uid)
        await q.edit_message_text(header, reply_markup=kb)
        return

    # Featured
    if q.data.startswith("feat:"):
        _, k, offset_str = q.data.split(":")
        offset = int(offset_str)
        name = get_sheikh_name_by_key(k)
        if not name:
            await q.edit_message_text("حصلت مشكلة. ارجع وحاول تاني.")
            return

        if offset == 0:
            await q.edit_message_text(f"⭐ تسجيلات مميزة\n🎙 {name}\nجارٍ الإرسال...")

        rows = list_featured_for_sheikh(name, limit=BATCH, offset=offset)
        if not rows:
            await context.bot.send_message(q.message.chat_id, "⭐ مفيش تسجيلات مميزة لسه.")
            return

        title_line = f"🎙 {name}\n⭐ تسجيلات مميزة"
        sent, skipped_deleted = await send_batch_by_rows(context, q.message.chat_id, name, rows, title_line)

        next_offset = offset + BATCH
        more = list_featured_for_sheikh(name, limit=1, offset=next_offset)

        status = f"✅ تم الإرسال: {sent}"
        if skipped_deleted:
            status += f"\n🧹 تم حذف محذوف من التخزين: {skipped_deleted}"

        if more:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➡️ إرسال المزيد", callback_data=f"feat:{k}:{next_offset}")],
                [InlineKeyboardButton("⬅️ رجوع للشيخ", callback_data=f"sheikhk:{k}")]
            ])
            await context.bot.send_message(q.message.chat_id, status + "\n\nلو عايز باقي المميز:", reply_markup=kb)
        else:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع للشيخ", callback_data=f"sheikhk:{k}")]])
            await context.bot.send_message(q.message.chat_id, status + "\n\n✅ خلصت كل المميز.", reply_markup=kb)
        return

    # Latest
    if q.data.startswith("latest:"):
        _, k, offset_str = q.data.split(":")
        offset = int(offset_str)
        name = get_sheikh_name_by_key(k)
        if not name:
            await q.edit_message_text("حصلت مشكلة. ارجع وحاول تاني.")
            return

        if offset == 0:
            await q.edit_message_text(f"🆕 أحدث التسجيلات\n🎙 {name}\nجارٍ الإرسال...")

        rows = list_latest_for_sheikh(name, limit=BATCH, offset=offset)
        if not rows:
            await context.bot.send_message(q.message.chat_id, "مفيش تسجيلات.")
            return

        title_line = f"🎙 {name}\n🆕 أحدث التسجيلات"
        sent, skipped_deleted = await send_batch_by_rows(context, q.message.chat_id, name, rows, title_line)

        next_offset = offset + BATCH
        more = list_latest_for_sheikh(name, limit=1, offset=next_offset)

        status = f"✅ تم الإرسال: {sent}"
        if skipped_deleted:
            status += f"\n🧹 تم حذف محذوف من التخزين: {skipped_deleted}"

        if more:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➡️ إرسال المزيد", callback_data=f"latest:{k}:{next_offset}")],
                [InlineKeyboardButton("⬅️ رجوع للشيخ", callback_data=f"sheikhk:{k}")]
            ])
            await context.bot.send_message(q.message.chat_id, status + "\n\nلو عايز باقي الأحدث:", reply_markup=kb)
        else:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع للشيخ", callback_data=f"sheikhk:{k}")]])
            await context.bot.send_message(q.message.chat_id, status + "\n\n✅ خلصت أحدث تسجيلات.", reply_markup=kb)
        return

    # Period selection (e.g., "رمضان ٢٥") -> batch send
    if q.data.startswith("per:"):
        parts = q.data.split(":")
        if len(parts) < 5:
            return
        _, k, y_str, month_label, offset_str = parts[0], parts[1], parts[2], parts[3], parts[4]
        year = int(y_str)
        offset = int(offset_str)

        name = get_sheikh_name_by_key(k)
        if not name:
            await q.edit_message_text("حصلت مشكلة. ارجع وحاول تاني.")
            return

        if offset == 0:
            await q.edit_message_text(f"📤 إرسال تسجيلات\n🎙 {name}\n🗓 {month_label} {short_year(year)} ...")

        rows = list_recordings_for_sheikh_year_month(name, year, month_label, limit=BATCH, offset=offset)
        if not rows:
            await context.bot.send_message(q.message.chat_id, "مفيش تسجيلات في الاختيار ده.")
            return

        title_line = f"🎙 {name}\n🗓 {month_label} {short_year(year)}"
        sent, skipped_deleted = await send_batch_by_rows(context, q.message.chat_id, name, rows, title_line)

        next_offset = offset + BATCH
        more = list_recordings_for_sheikh_year_month(name, year, month_label, limit=1, offset=next_offset)

        status = f"✅ تم الإرسال: {sent}"
        if skipped_deleted:
            status += f"\n🧹 تم حذف محذوف من التخزين: {skipped_deleted}"

        if more:
            kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("➡️ إرسال المزيد", callback_data=f"per:{k}:{year}:{month_label}:{next_offset}")],
                [InlineKeyboardButton("⬅️ رجوع للشيخ", callback_data=f"sheikhk:{k}")]
            ])
            await context.bot.send_message(q.message.chat_id, status + "\n\nلو عايز باقي التسجيلات:", reply_markup=kb)
        else:
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ رجوع للشيخ", callback_data=f"sheikhk:{k}")]])
            await context.bot.send_message(q.message.chat_id, status + "\n\n✅ خلصت كل تسجيلات الاختيار ده.", reply_markup=kb)
        return


# ---------------- Main ----------------
def main():
    if not TOKEN:
        raise RuntimeError("BOT_TOKEN مش موجود. اعمل export BOT_TOKEN='...'")
    if STORAGE_CHAT_ID == 0:
        logging.warning("STORAGE_CHAT_ID is 0. Set it to your storage group chat id (-100...)")

    init_db()

    app = Application.builder().token(TOKEN).build()

    # Upload conversation
    conv_upload = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_upload_from_button, pattern=r"^menu:upload$")],
        states={
            ASK_SHEIKH: [MessageHandler(filters.TEXT & ~filters.COMMAND, ask_sheikh)],
            ASK_PERIOD: [
                CallbackQueryHandler(on_click, pattern=r"^preset:period:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, ask_period_text_router),
            ],
            WAIT_MEDIA: [
                MessageHandler((filters.VOICE | filters.AUDIO) & ~filters.COMMAND, receive_media),
                MessageHandler(filters.TEXT & ~filters.COMMAND, receive_media),
            ],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    # Search conversation
    conv_search = ConversationHandler(
        entry_points=[CallbackQueryHandler(start_search_from_button, pattern=r"^menu:search$")],
        states={
            ASK_SEARCH: [MessageHandler(filters.TEXT & ~filters.COMMAND, search_text)],
        },
        fallbacks=[CommandHandler("cancel", cancel)],
        allow_reentry=True,
    )

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("menu", menu))
    app.add_handler(CommandHandler("whoami", whoami))
    app.add_handler(CommandHandler("debug", debug))

    # Admin
    app.add_handler(CommandHandler("wipe_last", wipe_last))
    app.add_handler(CommandHandler("wipe_sheikh", wipe_sheikh))
    app.add_handler(CommandHandler("rename_sheikh", rename_sheikh_cmd))
    app.add_handler(CommandHandler("cleanup_orphans", cleanup_orphans))
    app.add_handler(CommandHandler("feature_last", feature_last))
    app.add_handler(CommandHandler("unfeature_last", unfeature_last))
    app.add_handler(CommandHandler("feature_storage", feature_storage))
    app.add_handler(CommandHandler("unfeature_storage", unfeature_storage))

    app.add_handler(conv_upload)
    app.add_handler(conv_search)

    # Global callbacks
    app.add_handler(CallbackQueryHandler(on_click))

    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
