#!/usr/bin/env python3
import os
os.environ['TZ'] = 'UTC'

import telebot
import yfinance as yf
import feedparser
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import io
import threading
import time
import sqlite3
import sys
import logging
import html
import signal
import re
import requests
from datetime import datetime, timedelta, timezone
from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
from telebot.apihelper import ApiTelegramException
import concurrent.futures

# Fix yfinance cache issues
os.makedirs("/tmp/yfinance_cache", exist_ok=True)
yf.set_tz_cache_location("/tmp/yfinance_cache")

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    vader = SentimentIntensityAnalyzer()
    VADER_OK = True
except ImportError:
    vader = None
    VADER_OK = False

# ========== CONFIG ==========
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    print("❌ BOT_TOKEN not set")
    sys.exit(1)

NEWS_API_KEY = os.environ.get("NEWS_API_KEY", "").strip()
ADMIN_IDS = [int(x.strip()) for x in os.environ.get("ADMIN_IDS", "").split(",") if x.strip()]
MAX_ALERTS = 20
MAX_HISTORY = 5
PRICE_TTL = 60
HIST_TTL = 300
NEWS_TTL = 900

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger("CommodityOracle")

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", threaded=True)
os.makedirs("data", exist_ok=True)

# ========== COMMODITIES – ALL YFINANCE FUTURES (RELIABLE) ==========
COMMODITIES = {
    "WTI":      {"symbol": "CL=F",  "name": "WTI Crude Oil",   "unit": "USD/bbl",   "emoji": "🛢",  "group": "energy"},
    "BRENT":    {"symbol": "BZ=F",  "name": "Brent Crude Oil",  "unit": "USD/bbl",   "emoji": "🛢",  "group": "energy"},
    "NATGAS":   {"symbol": "NG=F",  "name": "Natural Gas",      "unit": "USD/MMBtu", "emoji": "🔥",  "group": "energy"},
    "GOLD":     {"symbol": "GC=F",  "name": "Gold",             "unit": "USD/oz",    "emoji": "🥇",  "group": "metals"},
    "SILVER":   {"symbol": "SI=F",  "name": "Silver",           "unit": "USD/oz",    "emoji": "🥈",  "group": "metals"},
    "COPPER":   {"symbol": "HG=F",  "name": "Copper",           "unit": "USD/lb",    "emoji": "🔶",  "group": "metals"},
    "PLATINUM": {"symbol": "PL=F",  "name": "Platinum",         "unit": "USD/oz",    "emoji": "⚪",  "group": "metals"},
    "WHEAT":    {"symbol": "ZW=F",  "name": "Wheat",            "unit": "USc/bu",    "emoji": "🌾",  "group": "agri"},
    "CORN":     {"symbol": "ZC=F",  "name": "Corn",             "unit": "USc/bu",    "emoji": "🌽",  "group": "agri"},
    "SOY":      {"symbol": "ZS=F",  "name": "Soybeans",         "unit": "USc/bu",    "emoji": "🫘",  "group": "agri"},
}

GROUPS = {
    "energy": ("⚡ Energy",      ["WTI", "BRENT", "NATGAS"]),
    "metals": ("🪙 Metals",      ["GOLD", "SILVER", "COPPER", "PLATINUM"]),
    "agri":   ("🌱 Agriculture", ["WHEAT", "CORN", "SOY"]),
}

KEYWORDS = {
    "WTI": ["crude oil", "wti", "opec", "petroleum", "barrel", "oil price", "energy market"],
    "BRENT": ["brent", "crude oil", "opec", "petroleum", "barrel"],
    "NATGAS": ["natural gas", "lng", "henry hub", "gas price"],
    "GOLD": ["gold", "bullion", "xau", "fed rate", "inflation", "precious metal"],
    "SILVER": ["silver", "xag", "precious metal"],
    "COPPER": ["copper", "base metal", "china demand"],
    "PLATINUM": ["platinum", "pgm"],
    "WHEAT": ["wheat", "grain", "ukraine", "food supply"],
    "CORN": ["corn", "maize", "ethanol"],
    "SOY": ["soybean", "soy", "oilseed"],
}

RSS_FEEDS = [
    "https://feeds.reuters.com/reuters/businessNews",
    "https://finance.yahoo.com/rss/topfinstories",
    "https://www.marketwatch.com/rss/marketpulse",
]

TIMEFRAMES = {
    "1D":  ("1d",  "30m"),
    "5D":  ("5d",  "1h"),
    "1M":  ("1mo", "1d"),
    "3M":  ("3mo", "1d"),
    "6M":  ("6mo", "1d"),
    "1Y":  ("1y",  "1wk"),
}

# ========== DATABASE ==========
db_path = "data/commodity.db"
db_lock = threading.RLock()
conn = sqlite3.connect(db_path, check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA foreign_keys=ON")

def db_query(sql, params=(), fetch_one=False, fetch_all=False):
    with db_lock:
        cur = conn.cursor()
        cur.execute(sql, params)
        if fetch_one:
            row = cur.fetchone(); conn.commit(); return row
        if fetch_all:
            rows = cur.fetchall(); conn.commit(); return rows
        conn.commit(); return cur.lastrowid

db_query("""CREATE TABLE IF NOT EXISTS profiles (
    user_id INTEGER PRIMARY KEY,
    join_date INTEGER, username TEXT, first_name TEXT,
    is_admin INTEGER DEFAULT 0
)""")
try:
    db_query("ALTER TABLE profiles ADD COLUMN is_admin INTEGER DEFAULT 0")
except sqlite3.OperationalError:
    pass

db_query("""CREATE TABLE IF NOT EXISTS alerts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER, chat_id INTEGER,
    commodity TEXT, target REAL, direction TEXT,
    active INTEGER DEFAULT 1, created_at INTEGER
)""")

def init_admins():
    for uid in ADMIN_IDS:
        db_query("UPDATE profiles SET is_admin=1 WHERE user_id=?", (uid,))
init_admins()

# ========== CACHE ==========
class TTLCache:
    def __init__(self):
        self._data = {}
        self._lock = threading.RLock()
    def set(self, key, val, ttl=60):
        with self._lock:
            self._data[key] = (val, time.time() + ttl)
    def get(self, key):
        with self._lock:
            if key not in self._data: return None
            val, exp = self._data[key]
            if time.time() > exp:
                del self._data[key]; return None
            return val

cache = TTLCache()

# ========== HELPERS ==========
def h(v):
    return html.escape("" if v is None else str(v), quote=False)

def fmt_price(p):
    if p is None: return "N/A"
    if p >= 1000: return f"${p:,.2f}"
    if p >= 1:    return f"${p:,.4f}"
    return f"${p:.6f}"

def is_admin(uid):
    if uid in ADMIN_IDS:
        return True
    row = db_query("SELECT is_admin FROM profiles WHERE user_id=?", (uid,), fetch_one=True)
    return row and row[0] == 1

def delete_msg(m):
    try:
        bot.delete_message(m.chat.id, m.message_id)
    except:
        pass

def safe_send(cid, text, markup=None):
    try:
        return bot.send_message(cid, text, reply_markup=markup, disable_web_page_preview=True, parse_mode="HTML")
    except ApiTelegramException as e:
        if "can't parse entities" in str(e):
            try:
                return bot.send_message(cid, text, reply_markup=markup, disable_web_page_preview=True, parse_mode=None)
            except Exception as e2:
                log.error(f"send error (plain) {cid}: {e2}")
                return None
        else:
            log.error(f"send error {cid}: {e}")
            return None
    except Exception as e:
        log.error(f"send error: {e}")
        return None

def safe_edit(cid, mid, text, markup=None):
    try:
        bot.edit_message_text(text, cid, mid, parse_mode="HTML", reply_markup=markup)
        return True
    except ApiTelegramException as e:
        if "message is not modified" in str(e):
            return True
        log.warning(f"edit error: {e}")
        return False
    except Exception as e:
        log.warning(f"edit error: {e}")
        return False

msg_queue = {}
q_lock = threading.RLock()

def send_and_track(cid, text, markup=None):
    sent = safe_send(cid, text, markup)
    if not sent:
        return None
    with q_lock:
        msg_queue.setdefault(cid, []).append(sent.message_id)
        while len(msg_queue[cid]) > MAX_HISTORY:
            old = msg_queue[cid].pop(0)
            try:
                bot.delete_message(cid, old)
            except:
                pass
    return sent

def back_button():
    kb = InlineKeyboardMarkup()
    kb.row(InlineKeyboardButton("⬅️ Back", callback_data="back_main"))
    return kb

# ========== PRICE FETCHING WITH TIMEOUT AND FIXED YFINANCE ==========
def _flatten(df):
    if df is None: return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    return df

def _download_yf(symbol, period, interval):
    """Download with thread timeout and disable threading to avoid SQLite locks."""
    def download():
        try:
            # Use yf.download with threads=False to prevent database lock
            df = yf.download(symbol, period=period, interval=interval,
                             progress=False, auto_adjust=True, threads=False)
            if df is not None and not df.empty:
                df = _flatten(df).dropna(subset=["Close"])
                if not df.empty:
                    return df
        except Exception as e:
            log.warning(f"yf.download failed {symbol}: {e}")
        # Fallback to Ticker.history
        try:
            df = yf.Ticker(symbol).history(period=period, interval=interval)
            if df is not None and not df.empty:
                df = _flatten(df).dropna(subset=["Close"])
                if not df.empty:
                    return df
        except Exception as e:
            log.warning(f"ticker.history failed {symbol}: {e}")
        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(download)
        try:
            return future.result(timeout=10)
        except concurrent.futures.TimeoutError:
            log.warning(f"yfinance timeout for {symbol}")
            return None

def get_price(key):
    ck = f"px_{key}"
    cached = cache.get(ck)
    if cached:
        return cached

    sym = COMMODITIES[key]["symbol"]
    df = _download_yf(sym, "5d", "1d")
    if df is None or df.empty:
        return None

    close = df["Close"].dropna()
    if len(close) < 1:
        return None

    price = float(close.iloc[-1])
    prev = float(close.iloc[-2]) if len(close) >= 2 else price
    change = ((price - prev) / prev * 100) if prev else 0.0

    result = {"price": price, "change": change, "prev": prev}
    cache.set(ck, result, ttl=PRICE_TTL)
    return result

def get_history(key, period="1mo", interval="1d"):
    ck = f"hist_{key}_{period}_{interval}"
    cached = cache.get(ck)
    if cached is not None: return cached
    sym = COMMODITIES[key]["symbol"]
    df = _download_yf(sym, period, interval)
    if df is not None and not df.empty:
        cache.set(ck, df, ttl=HIST_TTL)
    return df

# ========== NEWS FETCHING (unchanged) ==========
def fetch_news(key, max_items=6):
    ck = f"news_{key}"
    cached = cache.get(ck)
    if cached is not None:
        return cached

    query_map = {
        "WTI": "crude oil WTI",
        "BRENT": "Brent crude oil",
        "NATGAS": "natural gas",
        "GOLD": "gold commodity",
        "SILVER": "silver commodity",
        "COPPER": "copper commodity",
        "PLATINUM": "platinum commodity",
        "WHEAT": "wheat commodity",
        "CORN": "corn commodity",
        "SOY": "soybeans commodity",
    }
    query = query_map.get(key, COMMODITIES[key]["name"])
    articles = []

    if NEWS_API_KEY:
        try:
            url = "https://newsapi.org/v2/everything"
            params = {
                "q": query,
                "apiKey": NEWS_API_KEY,
                "language": "en",
                "sortBy": "publishedAt",
                "pageSize": max_items,
                "from": (datetime.now(timezone.utc) - timedelta(days=2)).strftime("%Y-%m-%d")
            }
            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "ok":
                    for article in data.get("articles", [])[:max_items]:
                        title = article.get("title")
                        link = article.get("url")
                        published = article.get("publishedAt", "")
                        if title and link:
                            articles.append({"title": title, "link": link, "published": published})
            else:
                log.warning(f"NewsAPI error: {response.status_code}")
        except Exception as e:
            log.warning(f"NewsAPI exception: {e}")

    if not articles:
        keywords = KEYWORDS.get(key, [])
        for url in RSS_FEEDS:
            if len(articles) >= max_items:
                break
            try:
                feed = feedparser.parse(url)
                for entry in feed.entries[:40]:
                    title = entry.get("title", "")
                    summary = entry.get("summary", "")
                    body = (title + " " + summary).lower()
                    if any(kw in body for kw in keywords):
                        link = entry.get("link", "")
                        if not any(a["link"] == link for a in articles):
                            articles.append({
                                "title": title,
                                "link": link,
                                "published": entry.get("published", ""),
                            })
                        if len(articles) >= max_items:
                            break
            except Exception as e:
                log.warning(f"RSS fallback error {url}: {e}")

    cache.set(ck, articles, ttl=NEWS_TTL)
    return articles

def sentiment_score(articles):
    if not VADER_OK or not articles: return 0.0, "⚪ Neutral"
    scores = [vader.polarity_scores(a["title"])["compound"] for a in articles]
    avg = sum(scores) / len(scores) if scores else 0.0
    if avg > 0.2: label = "🟢 Bullish"
    elif avg < -0.2: label = "🔴 Bearish"
    else: label = "⚪ Neutral"
    return avg, label

def headline_emoji(title):
    if not VADER_OK: return "⚪"
    s = vader.polarity_scores(title)["compound"]
    if s > 0.2: return "🟢"
    elif s < -0.2: return "🔴"
    return "⚪"

# ========== TECHNICAL ANALYSIS (unchanged) ==========
def compute_ta(df):
    if df is None or len(df) < 20: return None
    close = df["Close"].squeeze().dropna()
    if len(close) < 20: return None

    delta = close.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi = (100 - 100 / (1 + rs)).dropna()

    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    macd = ema12 - ema26
    sig = macd.ewm(span=9, adjust=False).mean()
    hist = macd - sig

    bb_mid = close.rolling(20).mean()
    bb_std = close.rolling(20).std()
    bb_up = bb_mid + 2 * bb_std
    bb_lo = bb_mid - 2 * bb_std
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean() if len(close) >= 50 else None

    recent = close.tail(20)
    support = float(recent.min())
    resistance = float(recent.max())

    return {
        "rsi": float(rsi.iloc[-1]) if len(rsi) else 50.0,
        "macd": float(macd.iloc[-1]),
        "macd_sig": float(sig.iloc[-1]),
        "macd_hist": float(hist.iloc[-1]),
        "bb_up": float(bb_up.iloc[-1]),
        "bb_lo": float(bb_lo.iloc[-1]),
        "bb_mid": float(bb_mid.iloc[-1]),
        "ema20": float(ema20.iloc[-1]),
        "ema50": float(ema50.iloc[-1]) if ema50 is not None else None,
        "close": float(close.iloc[-1]),
        "support": support,
        "resistance": resistance,
    }

def generate_signal(ta, sentiment=0.0):
    if not ta:
        return "⚪ HOLD", ["Insufficient data for analysis."], 0
    score = 0; reasons = []

    rsi = ta["rsi"]
    if rsi < 30:
        score += 2; reasons.append(f"✅ RSI {rsi:.1f} — oversold (bullish)")
    elif rsi > 70:
        score -= 2; reasons.append(f"🔴 RSI {rsi:.1f} — overbought (bearish)")
    else:
        reasons.append(f"⚪ RSI {rsi:.1f} — neutral zone")

    if ta["macd_hist"] > 0:
        score += 1; reasons.append("✅ MACD bullish crossover")
    elif ta["macd_hist"] < 0:
        score -= 1; reasons.append("🔴 MACD bearish crossover")
    else:
        reasons.append("⚪ MACD neutral")

    cl = ta["close"]
    if cl <= ta["bb_lo"]:
        score += 1; reasons.append("✅ Price at lower Bollinger Band — buy zone")
    elif cl >= ta["bb_up"]:
        score -= 1; reasons.append("🔴 Price at upper Bollinger Band — sell zone")
    else:
        reasons.append("⚪ Price inside Bollinger Bands — neutral")

    if ta["ema50"]:
        if ta["ema20"] > ta["ema50"]:
            score += 1; reasons.append("✅ EMA20 > EMA50 — uptrend")
        else:
            score -= 1; reasons.append("🔴 EMA20 < EMA50 — downtrend")

    sr = ta["resistance"] - ta["support"]
    if sr > 0:
        pos = (cl - ta["support"]) / sr
        if pos < 0.2:
            score += 1; reasons.append(f"✅ Near support ${ta['support']:.2f}")
        elif pos > 0.8:
            score -= 1; reasons.append(f"🔴 Near resistance ${ta['resistance']:.2f}")
        else:
            reasons.append(f"⚪ Mid-range S:${ta['support']:.2f} R:${ta['resistance']:.2f}")

    if sentiment > 0.2:
        score += 1; reasons.append(f"✅ News sentiment bullish ({sentiment:+.2f})")
    elif sentiment < -0.2:
        score -= 1; reasons.append(f"🔴 News sentiment bearish ({sentiment:+.2f})")
    else:
        reasons.append(f"⚪ News sentiment neutral ({sentiment:+.2f})")

    if score >= 3: sig = "🟢 STRONG BUY"
    elif score >= 1: sig = "🟡 BUY"
    elif score <= -3: sig = "🔴 STRONG SELL"
    elif score <= -1: sig = "🟠 SELL"
    else: sig = "⚪ HOLD"

    return sig, reasons, score

# ========== IMPROVED CHART GENERATION ==========
BG     = "#0a0e17"
GRID   = "#1e2433"
TEXT   = "#f0f3f8"
MUTED  = "#8b9bb0"
BLUE   = "#3b82f6"
GREEN  = "#22c55e"
RED    = "#ef4444"
ORANGE = "#f97316"
PURPLE = "#a855f7"
SPINE  = "#2a3441"

def _style_ax(ax):
    ax.set_facecolor(BG)
    ax.tick_params(colors=MUTED, labelsize=8)
    for sp in ax.spines.values():
        sp.set_color(SPINE)
    ax.grid(color=GRID, linewidth=0.5, alpha=0.6, linestyle='--')

def generate_chart(key, period="1mo", interval="1d"):
    df = get_history(key, period, interval)
    if df is None or df.empty or len(df) < 5:
        return None

    c = COMMODITIES[key]
    cl = df["Close"].values.astype(float)
    op = df["Open"].values.astype(float)  if "Open"   in df.columns else cl.copy()
    hi = df["High"].values.astype(float)  if "High"   in df.columns else cl.copy()
    lo = df["Low"].values.astype(float)   if "Low"    in df.columns else cl.copy()
    vol = df["Volume"].values.astype(float) if "Volume" in df.columns else np.zeros(len(df))

    n = len(df)
    x = np.arange(n)
    up = cl >= op

    cs = pd.Series(cl)
    delta = cs.diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    rs = gain / loss.replace(0, np.nan)
    rsi_s = 100 - 100 / (1 + rs)
    ema20 = cs.ewm(span=20, adjust=False).mean().values
    bb_mid = cs.rolling(20).mean()
    bb_std = cs.rolling(20).std()
    bb_up_v = (bb_mid + 2 * bb_std).values
    bb_lo_v = (bb_mid - 2 * bb_std).values

    last_price = cl[-1]

    fig = plt.figure(figsize=(14, 10), facecolor=BG, dpi=130)
    gs = fig.add_gridspec(3, 1, height_ratios=[4, 1, 2], hspace=0.08)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    for ax in [ax1, ax2, ax3]:
        _style_ax(ax)

    width = 0.55
    up_idx = np.where(up)[0]
    dn_idx = np.where(~up)[0]
    ax1.bar(up_idx, cl[up_idx] - op[up_idx], bottom=op[up_idx],
            color=GREEN, edgecolor=GREEN, width=width, linewidth=0, alpha=0.95)
    ax1.bar(dn_idx, op[dn_idx] - cl[dn_idx], bottom=cl[dn_idx],
            color=RED, edgecolor=RED, width=width, linewidth=0, alpha=0.95)
    ax1.vlines(up_idx, lo[up_idx], hi[up_idx], color=GREEN, linewidth=1, alpha=0.9)
    ax1.vlines(dn_idx, lo[dn_idx], hi[dn_idx], color=RED, linewidth=1, alpha=0.9)

    ax1.plot(x, ema20, color=ORANGE, linewidth=1.5, label="EMA20", alpha=0.9)
    ax1.fill_between(x, bb_up_v, bb_lo_v, alpha=0.12, color=BLUE)
    ax1.plot(x, bb_up_v, color=BLUE, linewidth=1, linestyle="--", alpha=0.7, label="BB ±2σ")
    ax1.plot(x, bb_lo_v, color=BLUE, linewidth=1, linestyle="--", alpha=0.7)

    ax1.axhline(y=last_price, color="cyan", linestyle="-.", linewidth=1.8, alpha=0.9, label=f"Current: {fmt_price(last_price)}")
    ax1.text(n-1, last_price, f"  {fmt_price(last_price)}", color="cyan", fontsize=9, va="bottom", ha="left", weight='bold')

    ax1.set_title(f"{c['emoji']}  {c['name']} — {period}  ({interval} candles)  |  Last: {fmt_price(last_price)}",
                  color=TEXT, fontsize=14, pad=12)
    ax1.set_ylabel(c["unit"], color=MUTED, fontsize=10)
    ax1.legend(facecolor="#111827", labelcolor=TEXT, fontsize=9, loc="upper left", framealpha=0.8)
    ax1.tick_params(labelbottom=False)

    vol_c = np.where(up, GREEN, RED)
    ax2.bar(x, vol, color=vol_c, width=width*0.8, alpha=0.6)
    ax2.set_ylabel("Volume", color=MUTED, fontsize=9)
    ax2.tick_params(labelbottom=False)
    if vol.max() > 0:
        ax2.set_ylim(0, vol.max() * 1.3)

    ax3.plot(x, rsi_s, color=PURPLE, linewidth=1.5)
    ax3.axhline(70, color=RED, linewidth=1, linestyle="--", alpha=0.7)
    ax3.axhline(30, color=GREEN, linewidth=1, linestyle="--", alpha=0.7)
    ax3.axhline(50, color=MUTED, linewidth=0.8, linestyle=":", alpha=0.6)
    ax3.fill_between(x, rsi_s, 70, where=(rsi_s >= 70), alpha=0.15, color=RED)
    ax3.fill_between(x, rsi_s, 30, where=(rsi_s <= 30), alpha=0.15, color=GREEN)
    ax3.set_ylabel("RSI(14)", color=MUTED, fontsize=9)
    ax3.set_ylim(0, 100)
    last_rsi = float(rsi_s.dropna().iloc[-1]) if not rsi_s.dropna().empty else 50
    ax3.text(n-1, last_rsi+3, f"{last_rsi:.0f}", color=PURPLE, fontsize=9, ha="right", weight='bold')

    step = max(1, n // 8)
    ticks = list(range(0, n, step))
    fmt = "%b %y" if period == "1y" else "%m/%d"
    labels = [df.index[i].strftime(fmt) for i in ticks]
    ax3.set_xticks(ticks)
    ax3.set_xticklabels(labels, rotation=25, ha="right", fontsize=8, color=MUTED)

    fig.text(0.99, 0.01, "Commodity Oracle  |  Data: Yahoo Finance",
             color=MUTED, fontsize=8, ha="right", va="bottom")

    plt.tight_layout(pad=1.2)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=130, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return buf

# ========== ALERTS, PROFILES, UI BUILDERS (unchanged from previous working version) ==========
# ... (keeping the rest identical to your last working script to avoid repetition)
# For brevity, I am including the remaining functions as they were in the previous script.

# NOTE: The full script is long. I am providing the complete final script as a single block below.
# Since the assistant message has a character limit, I will continue in the next response with the full file.
