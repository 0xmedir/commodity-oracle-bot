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

TWELVE_DATA_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "").strip()
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

# Twelve Data client (if key provided)
td = None
if TWELVE_DATA_API_KEY:
    try:
        from twelvedata import TDClient
        td = TDClient(apikey=TWELVE_DATA_API_KEY)
        log.info("✅ Twelve Data client initialized for accurate metal spot prices")
    except ImportError:
        log.warning("twelvedata package not installed. Install with: pip install twelvedata")
    except Exception as e:
        log.warning(f"Twelve Data init error: {e}")

# ========== COMMODITIES ==========
# Base configuration for all commodities
COMMODITIES = {
    "WTI":      {"symbol": "CL=F",  "name": "WTI Crude Oil",   "unit": "USD/bbl",   "emoji": "🛢",  "group": "energy", "source": "yf"},
    "BRENT":    {"symbol": "BZ=F",  "name": "Brent Crude Oil",  "unit": "USD/bbl",   "emoji": "🛢",  "group": "energy", "source": "yf"},
    "NATGAS":   {"symbol": "NG=F",  "name": "Natural Gas",      "unit": "USD/MMBtu", "emoji": "🔥",  "group": "energy", "source": "yf"},
    "GOLD":     {"symbol": "XAU/USD", "name": "Gold",           "unit": "USD/oz",    "emoji": "🥇",  "group": "metals", "source": "td", "fallback": "GC=F"},
    "SILVER":   {"symbol": "XAG/USD", "name": "Silver",         "unit": "USD/oz",    "emoji": "🥈",  "group": "metals", "source": "td", "fallback": "SI=F"},
    "COPPER":   {"symbol": "XCU/USD", "name": "Copper",         "unit": "USD/lb",    "emoji": "🔶",  "group": "metals", "source": "td", "fallback": "HG=F"},
    "PLATINUM": {"symbol": "XPT/USD", "name": "Platinum",       "unit": "USD/oz",    "emoji": "⚪",  "group": "metals", "source": "td", "fallback": "PL=F"},
    "WHEAT":    {"symbol": "ZW=F",  "name": "Wheat",            "unit": "USc/bu",    "emoji": "🌾",  "group": "agri",   "source": "yf"},
    "CORN":     {"symbol": "ZC=F",  "name": "Corn",             "unit": "USc/bu",    "emoji": "🌽",  "group": "agri",   "source": "yf"},
    "SOY":      {"symbol": "ZS=F",  "name": "Soybeans",         "unit": "USc/bu",    "emoji": "🫘",  "group": "agri",   "source": "yf"},
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

# ========== PRICE FETCHING (HYBRID) ==========
def _flatten(df):
    if df is None: return None
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.droplevel(1)
    return df

def _download_yf(symbol, period, interval):
    try:
        df = yf.Ticker(symbol).history(period=period, interval=interval)
        if df is not None and not df.empty:
            df = _flatten(df).dropna(subset=["Close"])
            if not df.empty:
                return df
    except Exception as e:
        log.warning(f"ticker.history failed {symbol}: {e}")
    try:
        df = yf.download(symbol, period=period, interval=interval,
                         progress=False, auto_adjust=True)
        if df is not None and not df.empty:
            df = _flatten(df).dropna(subset=["Close"])
            if not df.empty:
                return df
    except Exception as e:
        log.warning(f"yf.download failed {symbol}: {e}")
    return None

def get_price_td(symbol):
    if td is None:
        return None
    try:
        ts = td.time_series(symbol=symbol, interval="1min", outputsize=1)
        df = ts.with_pandas()
        if df is not None and not df.empty:
            return float(df['close'].iloc[-1])
    except Exception as e:
        log.warning(f"Twelve Data error for {symbol}: {e}")
    return None

def get_price(key):
    ck = f"px_{key}"
    cached = cache.get(ck)
    if cached:
        return cached

    comm = COMMODITIES[key]
    price = None
    prev = None

    if comm["source"] == "td" and td is not None:
        price = get_price_td(comm["symbol"])
        if price is not None:
            # Get previous close for change percent (2 data points)
            try:
                ts2 = td.time_series(symbol=comm["symbol"], interval="1day", outputsize=2)
                df2 = ts2.with_pandas()
                if df2 is not None and len(df2) >= 2:
                    prev = float(df2['close'].iloc[-2])
                else:
                    prev = price
            except:
                prev = price
        else:
            # Fallback to yfinance futures
            log.info(f"Twelve Data failed for {comm['name']}, using fallback {comm['fallback']}")
            df = _download_yf(comm["fallback"], "5d", "1d")
            if df is not None and not df.empty:
                close = df["Close"].dropna()
                if len(close) >= 1:
                    price = float(close.iloc[-1])
                    prev = float(close.iloc[-2]) if len(close) >= 2 else price
    else:
        # Energy & agriculture: use yfinance
        df = _download_yf(comm["symbol"], "5d", "1d")
        if df is not None and not df.empty:
            close = df["Close"].dropna()
            if len(close) >= 1:
                price = float(close.iloc[-1])
                prev = float(close.iloc[-2]) if len(close) >= 2 else price

    if price is None:
        return None

    change = ((price - prev) / prev * 100) if prev and prev != 0 else 0.0
    result = {"price": price, "change": change, "prev": prev}
    cache.set(ck, result, ttl=PRICE_TTL)
    return result

def get_history(key, period="1mo", interval="1d"):
    ck = f"hist_{key}_{period}_{interval}"
    cached = cache.get(ck)
    if cached is not None: return cached
    comm = COMMODITIES[key]
    # Use yfinance for history (consistent, as Twelve Data free tier has limited history)
    yf_sym = comm.get("fallback", comm["symbol"])
    df = _download_yf(yf_sym, period, interval)
    if df is not None and not df.empty:
        cache.set(ck, df, ttl=HIST_TTL)
    return df

# ========== NEWS FETCHING ==========
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
                "from": (datetime.now(timezone.UTC) - timedelta(days=2)).strftime("%Y-%m-%d")
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

# ========== CHART GENERATION ==========
BG     = "#0d1117"
GRID   = "#21262d"
TEXT   = "#e6edf3"
MUTED  = "#8b949e"
BLUE   = "#58a6ff"
GREEN  = "#3fb950"
RED    = "#f85149"
ORANGE = "#f97316"
PURPLE = "#a371f7"
SPINE  = "#30363d"

def _style_ax(ax):
    ax.set_facecolor(BG)
    ax.tick_params(colors=MUTED, labelsize=7)
    for sp in ax.spines.values():
        sp.set_color(SPINE)
    ax.grid(color=GRID, linewidth=0.4, alpha=0.8)

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

    cs      = pd.Series(cl)
    delta   = cs.diff()
    gain    = delta.clip(lower=0).rolling(14).mean()
    loss    = (-delta.clip(upper=0)).rolling(14).mean()
    rs      = gain / loss.replace(0, np.nan)
    rsi_s   = 100 - 100 / (1 + rs)
    ema20   = cs.ewm(span=20, adjust=False).mean().values
    bb_mid  = cs.rolling(20).mean()
    bb_std  = cs.rolling(20).std()
    bb_up_v = (bb_mid + 2 * bb_std).values
    bb_lo_v = (bb_mid - 2 * bb_std).values

    last_price = cl[-1]

    fig = plt.figure(figsize=(13, 9), facecolor=BG)
    gs  = fig.add_gridspec(3, 1, height_ratios=[4, 1, 2], hspace=0.06)
    ax1 = fig.add_subplot(gs[0])
    ax2 = fig.add_subplot(gs[1], sharex=ax1)
    ax3 = fig.add_subplot(gs[2], sharex=ax1)
    for ax in [ax1, ax2, ax3]:
        _style_ax(ax)

    up_i = np.where(up)[0];  dn_i = np.where(~up)[0]
    ax1.bar(up_i, np.abs(cl[up_i] - op[up_i]),
            bottom=np.minimum(op[up_i], cl[up_i]),
            color=GREEN, width=0.65, linewidth=0)
    ax1.bar(dn_i, np.abs(op[dn_i] - cl[dn_i]),
            bottom=np.minimum(op[dn_i], cl[dn_i]),
            color=RED, width=0.65, linewidth=0)
    ax1.vlines(up_i, lo[up_i], hi[up_i], color=GREEN, linewidth=0.8)
    ax1.vlines(dn_i, lo[dn_i], hi[dn_i], color=RED,   linewidth=0.8)

    ax1.plot(x, ema20,   color=ORANGE, linewidth=1.2, label="EMA20", alpha=0.9)
    ax1.fill_between(x, bb_up_v, bb_lo_v, alpha=0.08, color=BLUE)
    ax1.plot(x, bb_up_v, color=BLUE, linewidth=0.7, linestyle="--", alpha=0.6, label="BB±2σ")
    ax1.plot(x, bb_lo_v, color=BLUE, linewidth=0.7, linestyle="--", alpha=0.6)

    ax1.axhline(y=last_price, color="cyan", linestyle="-.", linewidth=1.5, alpha=0.8, label=f"Current: {fmt_price(last_price)}")
    ax1.text(n-1, last_price, f" {fmt_price(last_price)}", color="cyan", fontsize=9, va="bottom", ha="left")

    ax1.set_title(f"{c['emoji']}  {c['name']} — {period}  ({interval} candles) | Last: {fmt_price(last_price)}",
                  color=TEXT, fontsize=13, pad=8)
    ax1.set_ylabel(c["unit"], color=MUTED, fontsize=9)
    ax1.legend(facecolor="#161b22", labelcolor=TEXT, fontsize=8,
               loc="upper left", framealpha=0.7)
    ax1.tick_params(labelbottom=False)

    vol_c = np.where(up, GREEN, RED)
    ax2.bar(x, vol, color=vol_c, width=0.65, alpha=0.65)
    ax2.set_ylabel("Volume", color=MUTED, fontsize=8)
    ax2.tick_params(labelbottom=False)
    if vol.max() > 0:
        ax2.set_ylim(0, vol.max() * 1.3)

    ax3.plot(x, rsi_s, color=PURPLE, linewidth=1.3)
    ax3.axhline(70, color=RED,   linewidth=0.8, linestyle="--", alpha=0.7)
    ax3.axhline(30, color=GREEN, linewidth=0.8, linestyle="--", alpha=0.7)
    ax3.axhline(50, color=MUTED, linewidth=0.5, linestyle=":",  alpha=0.5)
    ax3.fill_between(x, rsi_s, 70, where=(rsi_s >= 70), alpha=0.15, color=RED)
    ax3.fill_between(x, rsi_s, 30, where=(rsi_s <= 30), alpha=0.15, color=GREEN)
    ax3.set_ylabel("RSI(14)", color=MUTED, fontsize=8)
    ax3.set_ylim(0, 100)
    last_rsi = float(rsi_s.dropna().iloc[-1]) if not rsi_s.dropna().empty else 50
    ax3.text(n - 1, last_rsi + 2, f"{last_rsi:.0f}",
             color=PURPLE, fontsize=7, ha="right")

    step   = max(1, n // 8)
    ticks  = list(range(0, n, step))
    fmt    = "%b %y" if period == "1y" else "%m/%d"
    labels = [df.index[i].strftime(fmt) for i in ticks]
    ax3.set_xticks(ticks)
    ax3.set_xticklabels(labels, rotation=25, ha="right", fontsize=7, color=MUTED)

    fig.text(0.99, 0.01, "Commodity Oracle  |  Data: Yahoo Finance + Twelve Data",
             color=MUTED, fontsize=7, ha="right", va="bottom")

    plt.tight_layout(pad=0.8)
    buf = io.BytesIO()
    plt.savefig(buf, format="png", dpi=110, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    buf.seek(0)
    return buf

# ========== ALERTS ==========
def add_alert(uid, cid, commodity, target, direction):
    cnt = db_query("SELECT COUNT(*) FROM alerts WHERE active=1 AND user_id=?", (uid,), fetch_one=True)[0]
    if cnt >= MAX_ALERTS:
        return None, f"❌ Max {MAX_ALERTS} alerts reached."
    aid = db_query(
        "INSERT INTO alerts(user_id,chat_id,commodity,target,direction,created_at) VALUES(?,?,?,?,?,?)",
        (uid, cid, commodity, target, direction, int(time.time()))
    )
    return aid, None

def get_alerts(cid=None):
    if cid:
        rows = db_query(
            "SELECT id,user_id,chat_id,commodity,target,direction FROM alerts WHERE active=1 AND chat_id=?",
            (cid,), fetch_all=True)
    else:
        rows = db_query(
            "SELECT id,user_id,chat_id,commodity,target,direction FROM alerts WHERE active=1",
            fetch_all=True)
    return [{"id":r[0],"uid":r[1],"cid":r[2],"commodity":r[3],"target":r[4],"direction":r[5]}
            for r in (rows or [])]

def deactivate_alert(aid):
    db_query("UPDATE alerts SET active=0 WHERE id=?", (aid,))

def alert_loop():
    while True:
        try:
            for a in get_alerts():
                data = get_price(a["commodity"])
                if not data: continue
                price = data["price"]
                hit = (a["direction"] == ">" and price >= a["target"]) or \
                      (a["direction"] == "<" and price <= a["target"])
                if not hit: continue
                c = COMMODITIES.get(a["commodity"], {})
                sent = safe_send(
                    a["cid"],
                    f"🚨 <b>COMMODITY ALERT TRIGGERED</b>\n\n"
                    f"{c.get('emoji','📦')} <b>{c.get('name', a['commodity'])}</b> "
                    f"{h(a['direction'])} <b>${a['target']:,.2f}</b>\n"
                    f"💵 Current: <b>{fmt_price(price)}</b> {c.get('unit','')}"
                )
                if sent:
                    deactivate_alert(a["id"])
        except Exception as e:
            log.error(f"Alert loop: {e}")
        time.sleep(30)

threading.Thread(target=alert_loop, daemon=True).start()

# ========== PROFILES ==========
def ensure_profile(uid, uname, fname):
    db_query(
        "INSERT OR IGNORE INTO profiles(user_id,join_date,username,first_name) VALUES(?,?,?,?)",
        (uid, int(time.time()), uname or "", fname or "")
    )

def get_profile(uid):
    return db_query("SELECT * FROM profiles WHERE user_id=?", (uid,), fetch_one=True)

# ========== UI BUILDERS ==========
def main_menu():
    text = (
        "⚗️ <b>COMMODITY ORACLE</b>\n\n"
        "Real-time intelligence for global commodity markets.\n"
        "Energy · Metals · Agriculture"
    )
    kb = InlineKeyboardMarkup()
    kb.row(
        InlineKeyboardButton("💰 Live Prices", callback_data="px"),
        InlineKeyboardButton("📊 Charts", callback_data="cht_menu"),
    )
    kb.row(
        InlineKeyboardButton("📰 News & Sentiment", callback_data="nws_menu"),
        InlineKeyboardButton("🎯 Trading Signal", callback_data="sig_menu"),
    )
    kb.row(
        InlineKeyboardButton("🔔 Set Alert", callback_data="alm"),
        InlineKeyboardButton("📋 My Alerts", callback_data="all"),
    )
    kb.row(InlineKeyboardButton("👤 Profile", callback_data="prof"))
    return text, kb

def commodity_picker(cb_prefix, title, subtitle=""):
    text = f"{title}\n<i>{subtitle}</i>" if subtitle else title
    kb = InlineKeyboardMarkup()
    for _, (glabel, keys) in GROUPS.items():
        row = [InlineKeyboardButton(
            f"{COMMODITIES[k]['emoji']} {k}", callback_data=f"{cb_prefix}{k}"
        ) for k in keys]
        kb.row(*row)
    kb.row(InlineKeyboardButton("⬅️ Back", callback_data="back_main"))
    return text, kb

def timeframe_picker(key):
    c = COMMODITIES[key]
    text = f"📊 <b>{c['emoji']} {c['name']}</b>\n\nSelect timeframe:"
    kb = InlineKeyboardMarkup()
    row = []
    for tf in TIMEFRAMES:
        row.append(InlineKeyboardButton(tf, callback_data=f"ctf_{key}_{tf}"))
        if len(row) == 3:
            kb.row(*row); row = []
    if row: kb.row(*row)
    kb.row(InlineKeyboardButton("⬅️ Back", callback_data="back_main"))
    return text, kb

def alerts_menu():
    text = "🔔 <b>Price Alerts</b>\n\nQuick presets or set a custom alert:"
    kb = InlineKeyboardMarkup()
    presets = [
        ("WTI > $90", "alp_WTI_>_90"),
        ("WTI < $70", "alp_WTI_<_70"),
        ("GOLD > $3300", "alp_GOLD_>_3300"),
        ("GOLD < $3000", "alp_GOLD_<_3000"),
        ("NATGAS > $4", "alp_NATGAS_>_4"),
        ("SILVER > $35", "alp_SILVER_>_35"),
    ]
    for i in range(0, len(presets), 2):
        kb.row(*[InlineKeyboardButton(lbl, callback_data=cd) for lbl, cd in presets[i:i+2]])
    kb.row(InlineKeyboardButton("✏️ Custom Alert", callback_data="alc"))
    kb.row(InlineKeyboardButton("📋 My Alerts", callback_data="all"))
    kb.row(InlineKeyboardButton("⬅️ Back", callback_data="back_main"))
    return text, kb

def build_prices_text():
    lines = ["💰 <b>Live Commodity Prices</b>\n"]
    for _, (glabel, keys) in GROUPS.items():
        lines.append(f"<b>{glabel}</b>")
        for key in keys:
            c = COMMODITIES[key]
            data = get_price(key)
            if data:
                arrow = "▲" if data["change"] >= 0 else "▼"
                bullet = "🟢" if data["change"] >= 0 else "🔴"
                lines.append(
                    f"{bullet} {c['emoji']} <b>{c['name']}</b>\n"
                    f"   {fmt_price(data['price'])} {c['unit']}  {bullet}{arrow} {abs(data['change']):.2f}%"
                )
            else:
                lines.append(f"⚪ {c['emoji']} <b>{c['name']}</b>  —  N/A")
        lines.append("")
    lines.append(f"<i>🕐 {datetime.now(timezone.UTC).strftime('%H:%M UTC')}</i>")
    return "\n".join(lines)

# ========== COMMAND HANDLERS ==========
waiting = {}
wait_lock = threading.RLock()

@bot.message_handler(commands=["start", "help"])
def cmd_start(m):
    delete_msg(m)
    ensure_profile(m.from_user.id, m.from_user.username, m.from_user.first_name)
    text, kb = main_menu()
    send_and_track(m.chat.id, text, kb)

@bot.message_handler(commands=["cancel"])
def cmd_cancel(m):
    delete_msg(m)
    with wait_lock:
        waiting.pop((m.chat.id, m.from_user.id), None)
    send_and_track(m.chat.id, "❌ Cancelled.", back_button())

@bot.message_handler(commands=["prices"])
def cmd_prices(m):
    delete_msg(m)
    _send_prices(m.chat.id)

@bot.message_handler(commands=["stats"])
def cmd_stats(m):
    if not is_admin(m.from_user.id):
        safe_send(m.chat.id, "⛔ Admin only command.")
        return
    delete_msg(m)
    users = db_query("SELECT COUNT(*) FROM profiles", fetch_one=True)[0]
    active = db_query("SELECT COUNT(*) FROM alerts WHERE active=1", fetch_one=True)[0]
    triggered = db_query("SELECT COUNT(*) FROM alerts WHERE active=0", fetch_one=True)[0]
    row = db_query("SELECT commodity, COUNT(*) as cnt FROM alerts WHERE active=1 GROUP BY commodity ORDER BY cnt DESC LIMIT 1", fetch_one=True)
    most_active = f"{row[0]} ({row[1]} alerts)" if row else "None"
    safe_send(m.chat.id,
        f"📊 <b>Bot Stats</b>\n\n"
        f"👥 Total users: {users}\n"
        f"🔔 Active alerts: {active}\n"
        f"✅ Triggered alerts: {triggered}\n"
        f"🔥 Most active commodity: {most_active}")

@bot.message_handler(commands=["broadcast"])
def cmd_broadcast(m):
    if not is_admin(m.from_user.id):
        safe_send(m.chat.id, "⛔ Admin only command.")
        return
    delete_msg(m)
    parts = m.text.split(maxsplit=1)
    if len(parts) < 2:
        safe_send(m.chat.id, "Usage: /broadcast <message>"); return
    msg = parts[1]
    rows = db_query("SELECT DISTINCT user_id FROM profiles", fetch_all=True) or []
    sent = failed = 0
    for (uid,) in rows:
        r = safe_send(uid, msg)
        if r: sent += 1
        else: failed += 1
        time.sleep(0.05)
    safe_send(m.chat.id, f"✅ Sent: {sent}  ❌ Failed: {failed}")

# ========== CALLBACK HANDLERS ==========
@bot.callback_query_handler(func=lambda c: c.data == "back_main")
def cb_back(call):
    with wait_lock:
        waiting.pop((call.message.chat.id, call.from_user.id), None)
    text, kb = main_menu()
    send_and_track(call.message.chat.id, text, kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "px")
def cb_px(call):
    bot.answer_callback_query(call.id)
    _send_prices(call.message.chat.id)

def _send_prices(cid):
    loading = send_and_track(cid, "⏳ Fetching live prices…", back_button())
    def fetch():
        text = build_prices_text()
        kb = InlineKeyboardMarkup()
        kb.row(
            InlineKeyboardButton("🔄 Refresh", callback_data="px"),
            InlineKeyboardButton("⬅️ Back", callback_data="back_main"),
        )
        if loading:
            try: bot.delete_message(cid, loading.message_id)
            except: pass
        send_and_track(cid, text, kb)
    threading.Thread(target=fetch, daemon=True).start()

@bot.callback_query_handler(func=lambda c: c.data == "cht_menu")
def cb_cht_menu(call):
    text, kb = commodity_picker("cpick_", "📊 <b>Charts</b>", "Pick a commodity.")
    send_and_track(call.message.chat.id, text, kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("cpick_"))
def cb_cpick(call):
    key = call.data[len("cpick_"):]
    if key not in COMMODITIES:
        bot.answer_callback_query(call.id, "Unknown"); return
    text, kb = timeframe_picker(key)
    send_and_track(call.message.chat.id, text, kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ctf_"))
def cb_ctf(call):
    parts = call.data.split("_", 2)
    if len(parts) < 3:
        bot.answer_callback_query(call.id, "Bad format"); return
    key, tf = parts[1], parts[2]
    if key not in COMMODITIES or tf not in TIMEFRAMES:
        bot.answer_callback_query(call.id, "Invalid"); return
    cid = call.message.chat.id
    period, interval = TIMEFRAMES[tf]
    bot.answer_callback_query(call.id, "Generating chart…")
    loading = send_and_track(cid, f"⏳ Building {key} {tf} chart…", back_button())
    def gen():
        buf = generate_chart(key, period, interval)
        if loading:
            try: bot.delete_message(cid, loading.message_id)
            except: pass
        if buf:
            c = COMMODITIES[key]
            kb = InlineKeyboardMarkup()
            kb.row(
                InlineKeyboardButton("🎯 Get Signal", callback_data=f"sig_{key}"),
                InlineKeyboardButton("⬅️ Back", callback_data="back_main"),
            )
            try:
                bot.send_photo(cid, buf,
                    caption=f"{c['emoji']} <b>{c['name']}</b> — {tf}",
                    reply_markup=kb, parse_mode="HTML")
            except Exception as e:
                log.error(f"Chart send: {e}")
                safe_send(cid, "❌ Chart send failed.", back_button())
        else:
            safe_send(cid,
                "❌ No chart data available.\n"
                "<i>Try a longer timeframe (1M, 3M) or check back later.</i>",
                back_button())
    threading.Thread(target=gen, daemon=True).start()

@bot.callback_query_handler(func=lambda c: c.data == "nws_menu")
def cb_nws_menu(call):
    text, kb = commodity_picker("nws_", "📰 <b>News & Sentiment</b>",
                                "Headlines + VADER sentiment score.")
    send_and_track(call.message.chat.id, text, kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("nws_") and c.data != "nws_menu")
def cb_nws(call):
    key = call.data[len("nws_"):]
    if key not in COMMODITIES:
        bot.answer_callback_query(call.id, "Unknown"); return
    cid = call.message.chat.id
    bot.answer_callback_query(call.id)
    loading = send_and_track(cid, f"⏳ Fetching {key} news…", back_button())
    def fetch():
        try:
            articles = fetch_news(key)
            avg, label = sentiment_score(articles)
            c = COMMODITIES[key]
            text = f"📰 <b>{c['emoji']} {c['name']} — News & Sentiment</b>\n\n"
            text += f"📊 Overall: <b>{label}</b>  ({avg:+.2f})\n"
            if VADER_OK:
                bar = "█" * int(abs(avg) * 10) or "░"
                text += f"{'▶' if avg >= 0 else '◀'} {bar}\n"
            text += "\n"
            if articles:
                for i, a in enumerate(articles[:6], 1):
                    em = headline_emoji(a["title"])
                    title = h(a["title"][:110])
                    text += f"{em} <b>{i}.</b> {title}\n"
                    if a["link"]:
                        text += f"   <a href='{h(a['link'])}'>Read →</a>\n"
                    text += "\n"
            else:
                text += "<i>No recent headlines found. Try again later.</i>\n"
            text += "<i>Source: NewsAPI, Reuters, Yahoo Finance, MarketWatch</i>"
            if loading:
                try: bot.delete_message(cid, loading.message_id)
                except: pass
            kb = InlineKeyboardMarkup()
            kb.row(
                InlineKeyboardButton("🎯 Get Signal", callback_data=f"sig_{key}"),
                InlineKeyboardButton("⬅️ Back", callback_data="back_main"),
            )
            send_and_track(cid, text, kb)
        except Exception as e:
            log.error(f"News fetch error: {e}")
            if loading:
                try: bot.delete_message(cid, loading.message_id)
                except: pass
            safe_send(cid, "❌ Error fetching news. Try again later.", back_button())
    threading.Thread(target=fetch, daemon=True).start()

@bot.callback_query_handler(func=lambda c: c.data == "sig_menu")
def cb_sig_menu(call):
    try:
        bot.answer_callback_query(call.id)
        text, kb = commodity_picker("sig_", "🎯 <b>Trading Signal</b>",
                                    "RSI · MACD · Bollinger · EMA · Sentiment combined.")
        send_and_track(call.message.chat.id, text, kb)
    except Exception as e:
        log.error(f"Error in cb_sig_menu: {e}")
        bot.answer_callback_query(call.id, "Error", show_alert=False)

@bot.callback_query_handler(func=lambda c: c.data.startswith("sig_") and c.data != "sig_menu")
def cb_sig(call):
    try:
        bot.answer_callback_query(call.id, "Analyzing...")
        key = call.data[len("sig_"):]
        if key not in COMMODITIES:
            bot.answer_callback_query(call.id, "Unknown commodity")
            return
        cid = call.message.chat.id
        loading = send_and_track(cid, f"⏳ Analyzing {key}…", back_button())
        
        def analyze():
            try:
                df = get_history(key, "3mo", "1d")
                ta = compute_ta(df)
                articles = fetch_news(key)
                s_sc, s_lbl = sentiment_score(articles)
                signal, reasons, score = generate_signal(ta, s_sc)
                price_data = get_price(key)
                c = COMMODITIES[key]
                price_str = f"{fmt_price(price_data['price'])} {c['unit']}" if price_data else "N/A"
                chg_str = (f"  ({'+' if price_data['change']>=0 else ''}{price_data['change']:.2f}%)"
                           if price_data else "")
                text = f"🎯 <b>{c['emoji']} {c['name']} — Trading Signal</b>\n\n"
                text += f"💵 Price: <b>{price_str}</b>{chg_str}\n"
                text += f"📊 Signal: <b>{signal}</b>  (score: {score:+d})\n\n"
                if ta:
                    rsi_lbl = "Oversold" if ta['rsi'] < 30 else ("Overbought" if ta['rsi'] > 70 else "Neutral")
                    macd_d = "Bullish ▲" if ta['macd_hist'] > 0 else "Bearish ▼"
                    trend = ("Uptrend ▲" if (ta['ema50'] and ta['ema20'] > ta['ema50'])
                             else ("Downtrend ▼" if ta['ema50'] else "N/A"))
                    text += (
                        f"<b>── Technical Analysis ──</b>\n"
                        f"RSI(14):  <b>{ta['rsi']:.1f}</b>  ({rsi_lbl})\n"
                        f"MACD:     <b>{macd_d}</b>\n"
                        f"EMA20:    <b>{fmt_price(ta['ema20'])}</b>\n"
                    )
                    if ta['ema50']:
                        text += f"EMA50:    <b>{fmt_price(ta['ema50'])}</b>  ({trend})\n"
                    text += (
                        f"BB Upper: <b>{fmt_price(ta['bb_up'])}</b>\n"
                        f"BB Lower: <b>{fmt_price(ta['bb_lo'])}</b>\n"
                        f"Support:  <b>{fmt_price(ta['support'])}</b>\n"
                        f"Resist:   <b>{fmt_price(ta['resistance'])}</b>\n\n"
                    )
                text += f"<b>── Sentiment ──</b>\n{s_lbl}  ({s_sc:+.2f})\n\n"
                text += "<b>── Signal Breakdown ──</b>\n"
                text += "\n".join(reasons[:7])
                text += "\n\n⚠️ <i>Not financial advice. DYOR.</i>"
                if loading:
                    try: bot.delete_message(cid, loading.message_id)
                    except: pass
                kb = InlineKeyboardMarkup()
                kb.row(
                    InlineKeyboardButton("📊 See Chart", callback_data=f"cpick_{key}"),
                    InlineKeyboardButton("📰 See News", callback_data=f"nws_{key}"),
                )
                kb.row(InlineKeyboardButton("⬅️ Back", callback_data="back_main"))
                send_and_track(cid, text, kb)
            except Exception as e:
                log.error(f"Error in analyze thread: {e}")
                if loading:
                    try: bot.delete_message(cid, loading.message_id)
                    except: pass
                safe_send(cid, "❌ Error generating signal. Try again later.", back_button())
        
        threading.Thread(target=analyze, daemon=True).start()
    except Exception as e:
        log.error(f"Error in cb_sig: {e}")
        bot.answer_callback_query(call.id, "Error", show_alert=False)

@bot.callback_query_handler(func=lambda c: c.data == "alm")
def cb_alm(call):
    text, kb = alerts_menu()
    send_and_track(call.message.chat.id, text, kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("alp_"))
def cb_alp(call):
    parts = call.data.split("_")
    if len(parts) < 4:
        bot.answer_callback_query(call.id, "Invalid"); return
    key, direction, target_str = parts[1], parts[2], parts[3]
    try: target = float(target_str)
    except: bot.answer_callback_query(call.id, "Bad value"); return
    uid = call.from_user.id; cid = call.message.chat.id
    aid, err = add_alert(uid, cid, key, target, direction)
    c = COMMODITIES.get(key, {})
    if err:
        send_and_track(cid, err, back_button())
    else:
        send_and_track(cid,
            f"✅ Alert set!\n"
            f"{c.get('emoji','📦')} <b>{c.get('name', key)}</b> {direction} <b>${target:,.2f}</b>",
            back_button())
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "alc")
def cb_alc(call):
    cid = call.message.chat.id; uid = call.from_user.id
    with wait_lock:
        waiting[(cid, uid)] = "custom_alert"
    send_and_track(cid,
        f"✏️ <b>Custom Alert</b>\n\n"
        f"Format: <code>KEY DIRECTION VALUE</code>\n"
        f"Example: <code>GOLD > 3200</code>\n\n"
        f"Keys: <code>{', '.join(COMMODITIES)}</code>\n\n"
        f"Send /cancel to abort.",
        back_button())
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "all")
def cb_all(call):
    cid = call.message.chat.id; uid = call.from_user.id
    my = [a for a in get_alerts(cid) if a["uid"] == uid]
    if not my:
        send_and_track(cid, "🔕 No active alerts.", back_button())
    else:
        text = "🔔 <b>Your Active Alerts</b>\n\n"
        kb = InlineKeyboardMarkup()
        for a in my:
            c = COMMODITIES.get(a["commodity"], {})
            name = c.get("name", a["commodity"])
            text += f"• {name} {a['direction']} ${a['target']:,.2f}\n"
            kb.row(InlineKeyboardButton(
                f"❌ {a['commodity']} {a['direction']} ${a['target']:,.2f}",
                callback_data=f"ald_{a['id']}"
            ))
        kb.row(InlineKeyboardButton("⬅️ Back", callback_data="back_main"))
        send_and_track(cid, text, kb)
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data.startswith("ald_"))
def cb_ald(call):
    try:
        deactivate_alert(int(call.data.split("_")[-1]))
        send_and_track(call.message.chat.id, "✅ Alert cancelled.", back_button())
    except:
        send_and_track(call.message.chat.id, "❌ Failed.", back_button())
    bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda c: c.data == "prof")
def cb_prof(call):
    uid = call.from_user.id; cid = call.message.chat.id
    row = get_profile(uid)
    if not row:
        send_and_track(cid, "❌ Profile not found.", back_button())
        return
    joined = time.strftime("%Y-%m-%d", time.localtime(row[1])) if row[1] else "Unknown"
    name = row[3] or "—"
    username = row[2] or "—"
    admin_badge = "👑 Admin" if (uid in ADMIN_IDS or row[4] == 1) else "👤 User"
    active = db_query("SELECT COUNT(*) FROM alerts WHERE active=1 AND user_id=?", (uid,), fetch_one=True)[0]
    triggered = db_query("SELECT COUNT(*) FROM alerts WHERE active=0 AND user_id=?", (uid,), fetch_one=True)[0]
    text = (
        f"👤 <b>Your Profile</b>\n\n"
        f"👑 Role: {admin_badge}\n"
        f"📛 Name: {h(name)}\n"
        f"🔖 Username: @{h(username)}\n"
        f"🆔 ID: <code>{uid}</code>\n"
        f"📅 Joined: {joined}\n"
        f"🔔 Active alerts: {active}\n"
        f"✅ Triggered alerts: {triggered}"
    )
    send_and_track(cid, text, back_button())
    bot.answer_callback_query(call.id)

# ========== TEXT HANDLER ==========
@bot.message_handler(func=lambda m: True)
def text_handler(m):
    if m.text and m.text.startswith("/"): return
    cid = m.chat.id; uid = m.from_user.id
    with wait_lock:
        state = waiting.pop((cid, uid), None)
    if not state: return
    delete_msg(m)

    if state == "custom_alert":
        match = re.match(r"^(\w+)\s*([<>])\s*([\d.]+)$", (m.text or "").strip().upper())
        if not match:
            send_and_track(cid, "❌ Bad format. Example: <code>GOLD > 3200</code>", back_button()); return
        key, direction, val_str = match.groups()
        if key not in COMMODITIES:
            send_and_track(cid, f"❌ Unknown: <b>{key}</b>\nOptions: {', '.join(COMMODITIES)}", back_button()); return
        target = float(val_str)
        aid, err = add_alert(uid, cid, key, target, direction)
        c = COMMODITIES[key]
        if err:
            send_and_track(cid, err, back_button())
        else:
            send_and_track(cid,
                f"✅ Alert set!\n{c['emoji']} <b>{c['name']}</b> {direction} <b>${target:,.2f}</b>",
                back_button())

# ========== SHUTDOWN ==========
def stop(sig, frame):
    log.info("Shutting down…")
    try: bot.stop_polling()
    except: pass
    sys.exit(0)

signal.signal(signal.SIGINT, stop)
signal.signal(signal.SIGTERM, stop)

log.info("🚀 Commodity Oracle Bot started — Hybrid: Twelve Data spot (accurate) + yfinance fallback for metals")
bot.delete_webhook()
time.sleep(1)
bot.infinity_polling(timeout=60, long_polling_timeout=60, skip_pending=True)
