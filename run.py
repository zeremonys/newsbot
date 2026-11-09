"""
NewsTrader (เวอร์ชัน GitHub Actions + Discord Webhook) — v2
รันครั้งเดียวแล้วจบ: cron-job.org / GitHub Actions ปลุกสคริปต์นี้ขึ้นมาทุกชั่วโมง
ดึงข่าว -> ให้ AI วิเคราะห์ -> โพสต์ LONG/SHORT ลง Discord -> บันทึกผลใน newsbot.db
-> สร้างหน้าเว็บสรุปผลใน docs/index.html (GitHub Pages)

v2: กันสัญญาณซ้ำ, เกณฑ์ความมั่นใจชัดเจน, เทียบผลกับดัชนีตลาด, จำกัดตลาด, หน้าเว็บสรุป, แท็กแจ้งเตือน
"""

import datetime as dt
import html
import json
import logging
import os
import re
import sqlite3
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime

import feedparser
import numpy as np
import yfinance as yf

from feeds import FEEDS

try:  # รันบนเครื่องตัวเองจะอ่าน .env ได้ด้วย
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

VERSION = 2

# ---------------------------------------------------------------- config
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
if not WEBHOOK_URL:
    raise SystemExit("ยังไม่ได้ตั้งค่า DISCORD_WEBHOOK_URL")

PROVIDER = (os.getenv("PROVIDER") or "gemini").lower()
PROVIDERS = {
    "gemini": {"base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
               "key_env": "GEMINI_API_KEY",
               "models": "gemini-3.5-flash,gemini-flash-latest,gemini-3.1-flash-lite",
               "max_headlines": 150},
    "groq": {"base_url": "https://api.groq.com/openai/v1",
             "key_env": "GROQ_API_KEY",
             "models": "openai/gpt-oss-120b,openai/gpt-oss-20b",
             "max_headlines": 50},
    "claude": {"base_url": None, "key_env": "ANTHROPIC_API_KEY",
               "models": "claude-sonnet-5-5", "max_headlines": 150},
}
if PROVIDER not in PROVIDERS:
    raise SystemExit(f"PROVIDER ต้องเป็น {', '.join(PROVIDERS)}")
_P = PROVIDERS[PROVIDER]


def _env(name, default):
    v = os.getenv(name)
    return v if v not in (None, "") else default


MODELS = [m.strip() for m in _env("MODEL", _P["models"]).split(",") if m.strip()]
MAX_EVENTS = int(_env("MAX_EVENTS", "3"))
MARKETS = [m.strip().upper() for m in _env("MARKETS", "US,TH").split(",") if m.strip()]
LANGUAGE = _env("LANGUAGE", "Thai")
MAX_HEADLINES = int(_env("MAX_HEADLINES", str(_P["max_headlines"])))
NEWS_MAX_AGE_HOURS = int(_env("NEWS_MAX_AGE_HOURS", "3"))
BOT_NAME = _env("BOT_NAME", "News Trader")
DB_PATH = _env("DB_PATH", "newsbot.db")
DASHBOARD_PATH = _env("DASHBOARD_PATH", "docs/index.html")
RUN_MODE = _env("RUN_MODE", "news")  # news / stats
AI_TIMEOUT = float(_env("AI_TIMEOUT", "150"))  # วินาที รอ AI ตอบได้นานสุดต่อครั้ง
ALERT_MIN_CONF = int(_env("ALERT_MIN_CONF", "5"))  # ความมั่นใจตั้งแต่เท่านี้จะแท็กแจ้งเตือน (6 = ปิด)
ALERT_MENTION = _env("ALERT_MENTION", "@everyone")  # หรือ <@&ROLE_ID> เพื่อแท็กเฉพาะ role
PENDING_MAX_DAYS = int(_env("PENDING_MAX_DAYS", "7"))  # รอราคาเกินกี่วันให้ยกเลิก

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("newsbot")
TH_TZ = dt.timezone(dt.timedelta(hours=7))

RELATIONS_TH = {"direct": "ได้รับผลโดยตรง", "competitor": "คู่แข่ง", "supplier": "ซัพพลายเออร์",
                "customer": "ลูกค้า", "sector": "ทั้งเซกเตอร์"}

# ตลาด -> นามสกุล ticker ของ Yahoo ("" = หุ้นอเมริกา ไม่มีนามสกุล)
MARKET_SUFFIXES = {
    "US": [""], "TH": ["BK"], "JP": ["T"], "HK": ["HK"], "UK": ["L"], "KR": ["KS", "KQ"],
    "SG": ["SI"], "IN": ["NS", "BO"], "TW": ["TW", "TWO"], "CN": ["SS", "SZ"], "AU": ["AX"],
    "EU": ["DE", "PA", "L", "MI", "AS", "SW", "MC", "ST", "CO", "HE", "OL", "BR", "LS", "VI"],
}
MARKET_NAMES = {"US": "US stocks (no suffix, e.g. NVDA)", "TH": "Thai SET stocks (suffix .BK, e.g. PTT.BK)",
                "JP": "Japan (.T)", "HK": "Hong Kong (.HK)", "UK": "UK (.L)", "KR": "Korea (.KS/.KQ)",
                "SG": "Singapore (.SI)", "IN": "India (.NS)", "TW": "Taiwan (.TW)", "CN": "China A (.SS/.SZ)",
                "AU": "Australia (.AX)", "EU": "Europe (.DE/.PA/.MI/.AS/.SW...)"}
# ดัชนีที่ใช้เทียบผล แยกตามนามสกุล ticker
BENCHMARKS = {"": "SPY", "BK": "^SET.BK", "T": "^N225", "HK": "^HSI", "L": "^FTSE", "KS": "^KS11",
              "KQ": "^KQ11", "SI": "^STI", "NS": "^NSEI", "BO": "^BSESN", "TW": "^TWII", "TWO": "^TWII",
              "SS": "000001.SS", "SZ": "399001.SZ", "AX": "^AXJO", "DE": "^GDAXI", "PA": "^FCHI",
              "MI": "FTSEMIB.MI", "AS": "^AEX", "SW": "^SSMI", "MC": "^IBEX"}
SUFFIX_MARKET = {s: m for m, ss in MARKET_SUFFIXES.items() for s in ss}


def suffix_of(ticker):
    if "." in ticker:
        s = ticker.rsplit(".", 1)[1]
        if s.isalpha() and len(s) <= 3:
            return s
    return ""


def market_of(ticker):
    return SUFFIX_MARKET.get(suffix_of(ticker), "OTHER")


def allowed_ticker(ticker):
    if ticker.startswith("^") or "=" in ticker:  # ดัชนี / ฟิวเจอร์ส / ค่าเงิน ซื้อขายตรงไม่ได้
        return False
    return market_of(ticker) in MARKETS


def benchmark_of(ticker):
    return BENCHMARKS.get(suffix_of(ticker), "ACWI")


# ---------------------------------------------------------------- database
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


NEW_COLUMNS = {"ver": "INTEGER", "market": "TEXT", "bench": "TEXT", "bench_entry": "REAL",
               "bench_exit": "REAL", "bench_ret": "REAL", "excess": "REAL"}


def init_db():
    with db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS seen (link TEXT PRIMARY KEY, ts TEXT)")
        c.execute("""CREATE TABLE IF NOT EXISTS calls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at TEXT, event TEXT, link TEXT,
            ticker TEXT, direction TEXT, relation TEXT,
            horizon INTEGER, confidence INTEGER, reason TEXT,
            status TEXT, entry_price REAL, entry_at TEXT,
            exit_price REAL, exit_at TEXT, ret REAL)""")
        have = {r[1] for r in c.execute("PRAGMA table_info(calls)")}
        for col, typ in NEW_COLUMNS.items():
            if col not in have:
                c.execute(f"ALTER TABLE calls ADD COLUMN {col} {typ}")
        c.execute("UPDATE calls SET ver=1 WHERE ver IS NULL")
        c.execute("UPDATE calls SET market=NULL WHERE market=''")
        # ลบประวัติข่าวที่เก่ากว่า 7 วัน ไม่ให้ไฟล์ใหญ่ขึ้นเรื่อย ๆ
        old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)).isoformat()
        c.execute("DELETE FROM seen WHERE ts < ?", (old,))
        # สัญญาณที่รอราคานานเกินไป (เช่น ticker ถูกระงับซื้อขาย) -> ยกเลิก
        old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=PENDING_MAX_DAYS)).isoformat()
        n = c.execute("UPDATE calls SET status='void' WHERE status='pending' AND created_at < ?", (old,)).rowcount
        if n:
            log.info("voided %d stale pending calls", n)


# ---------------------------------------------------------------- discord webhook
def send_webhook(embeds, content=None):
    payload = {"username": BOT_NAME, "embeds": embeds, "allowed_mentions": {"parse": []}}
    if content:
        payload["content"] = content
        parse = []
        if "@everyone" in content or "@here" in content:
            parse.append("everyone")
        if "<@&" in content:
            parse.append("roles")
        if re.search(r"<@!?\d", content):
            parse.append("users")
        payload["allowed_mentions"] = {"parse": parse}
    body = json.dumps(payload).encode()
    for _ in range(3):
        req = urllib.request.Request(WEBHOOK_URL, data=body, method="POST", headers={
            "Content-Type": "application/json", "User-Agent": "newsbot (github-actions, 2.0)"})
        try:
            urllib.request.urlopen(req, timeout=20).close()
            return
        except urllib.error.HTTPError as e:
            if e.code == 429:  # โดนจำกัดความถี่ -> รอแล้วลองใหม่
                time.sleep(float(e.headers.get("Retry-After", "2")))
                continue
            raise SystemExit(f"ส่ง Webhook ไม่ผ่าน ({e.code}): {e.read()[:300]!r}")


# ---------------------------------------------------------------- news
def _fetch_feed(feed):
    name, url = feed
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (newsbot)"})
        with urllib.request.urlopen(req, timeout=15) as r:
            parsed = feedparser.parse(r.read())
    except Exception as e:  # noqa: BLE001
        log.warning("feed failed %s: %s", name, e)
        return []
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=NEWS_MAX_AGE_HOURS)
    items = []
    for e in parsed.entries[:30]:
        link, title = e.get("link"), (e.get("title") or "").strip()
        if not link or not title:
            continue
        stamp = e.get("published") or e.get("updated")
        if stamp:
            try:
                pub = parsedate_to_datetime(stamp)
                if pub.tzinfo is None:
                    pub = pub.replace(tzinfo=dt.timezone.utc)
                if pub < cutoff:
                    continue
            except Exception:  # noqa: BLE001
                pass
        summary = re.sub(r"<[^>]+>", "", e.get("summary", ""))[:300].strip()
        items.append({"source": name, "title": title, "summary": summary, "link": link})
    return items


def collect_news():
    with ThreadPoolExecutor(max_workers=16) as ex:
        items = [i for r in ex.map(_fetch_feed, FEEDS) for i in r]
    out, titles = [], set()
    with db() as c:
        for i in items:
            k = i["title"].lower()[:80]
            if k in titles or c.execute("SELECT 1 FROM seen WHERE link=?", (i["link"],)).fetchone():
                continue
            titles.add(k)
            out.append(i)
    log.info("feeds: %d items, %d new", len(items), len(out))
    return out[:MAX_HEADLINES]


def mark_seen(items):
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    with db() as c:
        c.executemany("INSERT OR IGNORE INTO seen VALUES (?,?)", [(i["link"], now) for i in items])


def active_tickers():
    with db() as c:
        return {r[0] for r in c.execute(
            "SELECT DISTINCT ticker FROM calls WHERE status IN ('open','pending') AND ver >= ?", (VERSION,))}


def recent_events(hours=48):
    since = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=hours)).isoformat()
    with db() as c:
        return [r[0] for r in c.execute(
            "SELECT DISTINCT event FROM calls WHERE created_at > ? ORDER BY created_at DESC LIMIT 60", (since,))]


# ---------------------------------------------------------------- AI
SCHEMA = {
    "type": "object",
    "properties": {"events": {"type": "array", "items": {
        "type": "object",
        "properties": {
            "headline_index": {"type": "integer"},
            "title": {"type": "string", "description": f"short event title in {LANGUAGE}"},
            "summary": {"type": "string", "description": f"1-2 sentence summary in {LANGUAGE}"},
            "calls": {"type": "array", "items": {
                "type": "object",
                "properties": {
                    "ticker": {"type": "string", "description": "Yahoo Finance ticker e.g. NVDA, PTT.BK"},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT"]},
                    "relation": {"type": "string", "enum": list(RELATIONS_TH)},
                    "horizon": {"type": "integer", "description": "trading days, 1-20"},
                    "confidence": {"type": "integer", "description": "1-5, see rubric"},
                    "reason": {"type": "string", "description": f"one-line reason in {LANGUAGE}"},
                },
                "required": ["ticker", "direction", "relation", "horizon", "confidence", "reason"]}},
        },
        "required": ["headline_index", "title", "summary", "calls"]}}},
    "required": ["events"],
}
TOOL_NAME = "report_events"
SYSTEM_PROMPT = f"""You are an equity news analyst. From the numbered headlines, pick at most {MAX_EVENTS}
genuinely new, material world events likely to move specific listed stocks within 1-20 trading days.
Skip opinion pieces, sports, celebrity news, minor local stories and events already priced in.

ALLOWED MARKETS (strict — any other ticker will be discarded): {"; ".join(MARKET_NAMES.get(m, m) for m in MARKETS)}.
Only individual stocks or ETFs, never indices, futures or currencies. Use valid Yahoo Finance tickers.
For each event give 1-4 stock calls. Mark relation: direct, competitor, supplier, customer or sector.

CONFIDENCE RUBRIC — use the whole scale, most calls should NOT be 3 or 4:
1 = speculative; indirect or weak link, small expected move.
2 = plausible second-order effect (supplier/competitor/sector), direction uncertain.
3 = clear link and direction, moderate impact, meaningful chance it is already priced in.
4 = direct, material, surprising news; direction clear; likely not fully priced in yet.
5 = exceptional: very large surprise directly hitting this company, direction near-certain. At most 1 in 10 calls.
Rough target mix: 1-2 about 35%, 3 about 35%, 4 about 20%, 5 at most 10%.

The user message lists tickers that already have active signals and events already reported.
Do not issue calls on those tickers and do not report those events again.
If nothing is material, return an empty events list.
Write title, summary and reasons in {LANGUAGE}. Always answer by calling the {TOOL_NAME} function."""


def _parse_json_text(text):
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        return None
    try:
        return json.loads(m.group(0)).get("events", [])
    except (json.JSONDecodeError, AttributeError):
        return None


def _ask(model, user_msg):
    key = os.environ.get(_P["key_env"], "").strip()
    if not key:
        raise SystemExit(f"ยังไม่ได้ตั้งค่า {_P['key_env']}")
    if PROVIDER == "claude":
        from anthropic import Anthropic
        resp = Anthropic(api_key=key, timeout=AI_TIMEOUT, max_retries=1).messages.create(
            model=model, max_tokens=4000, system=SYSTEM_PROMPT,
            tools=[{"name": TOOL_NAME, "description": "Report market-moving events", "input_schema": SCHEMA}],
            tool_choice={"type": "tool", "name": TOOL_NAME},
            messages=[{"role": "user", "content": user_msg}])
        for b in resp.content:
            if b.type == "tool_use":
                return b.input.get("events", [])
        return []

    from openai import BadRequestError, OpenAI
    client = OpenAI(api_key=key, base_url=_P["base_url"], timeout=AI_TIMEOUT, max_retries=1)
    tool = {"type": "function", "function": {
        "name": TOOL_NAME, "description": "Report market-moving events", "parameters": SCHEMA}}
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_msg}]
    err = None
    # ลองแบบ "คิดน้อย" ก่อนให้ตอบเร็ว ถ้ารุ่นนั้นไม่รองรับค่อยถอยไปแบบปกติ
    attempts = [("required", {"reasoning_effort": "low"}), ("required", {}), ("auto", {})]
    if PROVIDER != "gemini":
        attempts = attempts[1:]
    for choice, extra in attempts:
        try:
            resp = client.chat.completions.create(
                model=model, messages=msgs, tools=[tool], tool_choice=choice, max_tokens=8000, **extra)
        except BadRequestError as e:
            err = e
            continue
        msg = resp.choices[0].message
        for tc in msg.tool_calls or []:
            try:
                return json.loads(tc.function.arguments).get("events", [])
            except json.JSONDecodeError:
                pass
        events = _parse_json_text(msg.content)
        return events if events is not None else []
    raise err


def discover_models():
    """ถามผู้ให้บริการว่าบัญชีนี้ใช้รุ่นอะไรได้บ้าง แล้วเรียงรุ่นที่น่าใช้ที่สุดขึ้นก่อน"""
    if PROVIDER == "claude":
        return []
    from openai import OpenAI
    client = OpenAI(api_key=os.environ.get(_P["key_env"], "").strip(), base_url=_P["base_url"], timeout=30)
    try:
        ids = [m.id.split("/", 1)[1] if m.id.startswith("models/") else m.id for m in client.models.list()]
    except Exception as e:  # noqa: BLE001
        log.warning("list models failed: %s", e)
        return []
    log.info("available models: %s", ", ".join(ids))
    if PROVIDER != "gemini":
        return []
    skip = ("image", "tts", "audio", "live", "embed", "vision", "native", "robotics", "computer")
    flash = [i for i in ids if "flash" in i and not any(s in i for s in skip)]

    def key(i):
        m = re.search(r"gemini-(\d+(?:\.\d+)?)", i)
        ver = float(m.group(1)) if m else 0.0
        return (-ver, "lite" in i, "preview" in i or "exp" in i, len(i))

    return sorted(flash, key=key)[:4]


def analyze(items, held, recent):
    user_msg = "Headlines:\n" + "\n".join(
        f"[{n}] ({i['source']}) {i['title']} — {i['summary']}" for n, i in enumerate(items))
    user_msg += "\n\nTickers with ACTIVE signals (do not use): " + (", ".join(sorted(held)) or "none")
    if recent:
        user_msg += "\n\nEvents ALREADY reported in the last 48h (do not report again):\n- " + "\n- ".join(recent)
    err, tried = None, []
    candidates = list(MODELS)
    discovered = False
    while candidates:
        model = candidates.pop(0)
        if model in tried:
            continue
        tried.append(model)
        try:
            events = _ask(model, user_msg)
            log.info("analyzed with %s/%s: %d events", PROVIDER, model, len(events))
            if model not in MODELS:
                log.info("tip: ตั้ง Variable MODEL=%s เพื่อให้เริ่มจากรุ่นนี้ทุกครั้ง", model)
            return events
        except SystemExit:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("model %s failed: %s", model, str(e)[:300])
            err = e
        if not candidates and not discovered:  # รุ่นที่ตั้งไว้ใช้ไม่ได้ทั้งหมด -> หาเองอัตโนมัติ
            discovered = True
            candidates = [m for m in discover_models() if m not in tried]
    raise err


# ---------------------------------------------------------------- prices
class PriceUnavailable(Exception):
    """ดึงราคาไม่ได้ชั่วคราว (เช่น Yahoo บล็อก) ต่างจาก ticker ที่ไม่มีจริง"""


_PRICE_CACHE = {}


def _to_utc(ts):
    ts = ts.to_pydatetime()
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=dt.timezone.utc)
    return ts.astimezone(dt.timezone.utc)


def prefetch(tickers):
    """ดึงราคาหลายตัวในคำขอเดียว ลดโอกาสโดน Yahoo จำกัดความถี่ (ถ้าพังจะถอยไปดึงทีละตัว)"""
    tickers = sorted({t for t in tickers if t and t not in _PRICE_CACHE})
    if not tickers:
        return
    try:
        df = yf.download(tickers, period="5d", interval="5m", group_by="ticker", progress=False,
                         threads=True, auto_adjust=False, timeout=20)
    except Exception as e:  # noqa: BLE001
        log.warning("batch price download failed: %s", e)
        return
    if df is None or df.empty:
        log.warning("batch price download returned nothing")
        return
    got = 0
    for t in tickers:
        try:
            sub = df[t] if len(tickers) > 1 or t in df.columns.get_level_values(0) else df
            s = sub["Close"].dropna()
        except Exception:  # noqa: BLE001
            continue
        if len(s):
            _PRICE_CACHE[t] = (float(s.iloc[-1]), _to_utc(s.index[-1]))
            got += 1
    log.info("prefetched prices: %d/%d", got, len(tickers))


def latest_bar(ticker):
    """(ราคา, เวลา UTC) ของแท่งล่าสุด / None = ไม่มี ticker นี้ / PriceUnavailable = ดึงไม่ได้"""
    if ticker in _PRICE_CACHE:
        return _PRICE_CACHE[ticker]
    try:
        t = yf.Ticker(ticker)
        h = t.history(period="5d", interval="5m", timeout=10)
        if h.empty:
            h = t.history(period="5d", timeout=10)
    except Exception as e:  # noqa: BLE001
        raise PriceUnavailable(str(e)) from e
    if h.empty:
        _PRICE_CACHE[ticker] = None
        return None
    bar = (float(h["Close"].iloc[-1]), _to_utc(h.index[-1]))
    _PRICE_CACHE[ticker] = bar
    return bar


def safe_price(ticker):
    try:
        bar = latest_bar(ticker)
    except PriceUnavailable:
        return None
    return bar[0] if bar else None


def open_call(call_id, price, ts, ticker):
    bench = benchmark_of(ticker)
    with db() as c:
        c.execute("UPDATE calls SET status='open', entry_price=?, entry_at=?, bench=?, bench_entry=? WHERE id=?",
                  (price, ts.isoformat(), bench, safe_price(bench), call_id))


def fill_pending():
    with db() as c:
        rows = c.execute("SELECT * FROM calls WHERE status='pending'").fetchall()
    for r in rows:
        try:
            bar = latest_bar(r["ticker"])
        except PriceUnavailable as e:
            log.warning("pending price unavailable %s: %s", r["ticker"], e)
            continue
        if bar is None:
            log.info("pending ticker has no data: %s", r["ticker"])
            continue
        if bar[1] > dt.datetime.fromisoformat(r["created_at"]):
            open_call(r["id"], bar[0], bar[1], r["ticker"])


def close_expired():
    today = dt.datetime.now(dt.timezone.utc).date()
    closed = []
    with db() as c:
        rows = c.execute("SELECT * FROM calls WHERE status='open'").fetchall()
    for r in rows:
        entry_day = dt.datetime.fromisoformat(r["entry_at"]).date()
        if np.busday_count(entry_day, today) < r["horizon"]:
            continue
        try:
            bar = latest_bar(r["ticker"])
        except PriceUnavailable:
            continue
        if not bar:
            continue
        sign = -1 if r["direction"] == "SHORT" else 1
        raw = bar[0] / r["entry_price"] - 1
        ret = sign * raw
        bench_exit = bench_ret = excess = None
        if r["bench"] and r["bench_entry"]:
            bench_exit = safe_price(r["bench"])
            if bench_exit:
                bench_ret = bench_exit / r["bench_entry"] - 1
                excess = sign * (raw - bench_ret)  # ผลที่ดีกว่า/แย่กว่าตลาด ตามทิศที่ทาย
        with db() as c:
            c.execute("""UPDATE calls SET status='closed', exit_price=?, exit_at=?, ret=?,
                         bench_exit=?, bench_ret=?, excess=? WHERE id=?""",
                      (bar[0], bar[1].isoformat(), ret, bench_exit, bench_ret, excess, r["id"]))
        closed.append({**dict(r), "ret": ret, "bench_ret": bench_ret, "excess": excess})
    return closed


# ---------------------------------------------------------------- stats
def compute_stats():
    with db() as c:
        rows = [dict(r) for r in c.execute("SELECT * FROM calls")]
    v2 = [r for r in rows if (r["ver"] or 1) >= VERSION]
    closed = [r for r in v2 if r["status"] == "closed"]
    s = {"legacy": sum((r["ver"] or 1) < VERSION for r in rows),
         "open": sum(r["status"] == "open" for r in v2),
         "pending": sum(r["status"] == "pending" for r in v2),
         "closed": len(closed), "rows": rows, "v2": v2, "closed_rows": closed}
    if closed:
        rets = [r["ret"] for r in closed]
        exc = [r["excess"] for r in closed if r["excess"] is not None]
        s.update(win=sum(x > 0 for x in rets) / len(rets), avg=float(np.mean(rets)),
                 avg_excess=float(np.mean(exc)) if exc else None,
                 beat=sum(x > 0 for x in exc) / len(exc) if exc else None)

    def group(key, label):
        out = []
        for k in sorted({key(r) for r in closed}):
            sub = [r for r in closed if key(r) == k]
            exc = [r["excess"] for r in sub if r["excess"] is not None]
            out.append({"label": label(k), "n": len(sub), "wins": sum(r["ret"] > 0 for r in sub),
                        "avg": float(np.mean([r["ret"] for r in sub])),
                        "excess": float(np.mean(exc)) if exc else None})
        return out

    s["by_conf"] = group(lambda r: r["confidence"], lambda k: "★" * k)
    s["by_market"] = group(lambda r: r["market"] or market_of(r["ticker"]), lambda k: k)
    s["by_dir"] = group(lambda r: r["direction"], lambda k: k)
    return s


def _pct(x):
    return "–" if x is None else f"{x:+.2%}"


def results_embed(closed):
    lines = []
    for r in closed[:20]:
        mark = "✅" if r["ret"] > 0 else "❌"
        mkt = f" (ตลาด {r['bench_ret']:+.2%})" if r.get("bench_ret") is not None else ""
        lines.append(f"{mark} **{r['direction']} {r['ticker']}** {r['ret']:+.2%}{mkt} · {r['event'][:50]}")
    return {"title": "🏁 สัญญาณที่ครบกำหนด", "description": "\n".join(lines), "color": 0x5865F2,
            "footer": {"text": "ตัวเลขในวงเล็บ = ดัชนีตลาดเดียวกันในช่วงเวลาเดียวกัน"}}


def stats_embed():
    s = compute_stats()
    fields = [{"name": "กำลังถือ", "value": str(s["open"]), "inline": True},
              {"name": "รอราคา", "value": str(s["pending"]), "inline": True},
              {"name": "ปิดแล้ว", "value": str(s["closed"]), "inline": True}]
    if s["closed"]:
        fields += [{"name": "ทายทิศถูก", "value": f"{s['win']:.0%}", "inline": True},
                   {"name": "ผลตอบแทนเฉลี่ย", "value": _pct(s["avg"]), "inline": True},
                   {"name": "ชนะตลาดเฉลี่ย", "value": _pct(s.get("avg_excess")), "inline": True}]
        if s.get("beat") is not None:
            fields.append({"name": "ชนะตลาดกี่ %", "value": f"{s['beat']:.0%} ของสัญญาณ", "inline": True})
        for key, name in (("by_conf", "แยกตามความมั่นใจ"), ("by_market", "แยกตามตลาด"), ("by_dir", "LONG / SHORT")):
            if s[key]:
                fields.append({"name": name, "inline": False, "value": "\n".join(
                    f"{g['label']}: ถูก {g['wins']}/{g['n']} · เฉลี่ย {_pct(g['avg'])} · ชนะตลาด {_pct(g['excess'])}"
                    for g in s[key])[:1024]})
    else:
        fields.append({"name": "ยังไม่มีสัญญาณครบกำหนด", "value": "รอสัญญาณชุดใหม่ครบระยะ 3-20 วันทำการ",
                       "inline": False})
    return {"title": "📊 สถิติสัญญาณประจำสัปดาห์", "color": 0x5865F2, "fields": fields,
            "footer": {"text": f"นับเฉพาะสัญญาณหลังปรับปรุงระบบ (ไม่รวมชุดแรก {s['legacy']} ตัว)"}}


# ---------------------------------------------------------------- dashboard
def _svg_curve(points):
    """points = [(label, value)] -> SVG เส้นผลสะสม"""
    if len(points) < 2:
        return '<p class="muted">ต้องมีสัญญาณครบกำหนดอย่างน้อย 2 ตัวถึงจะวาดกราฟได้</p>'
    w, h, pad = 640, 220, 30
    vals = [v for _, v in points]
    lo, hi = min(0, min(vals)), max(0, max(vals))
    span = (hi - lo) or 1
    xs = [pad + i * (w - 2 * pad) / (len(points) - 1) for i in range(len(points))]
    ys = [h - pad - (v - lo) / span * (h - 2 * pad) for v in vals]
    zero = h - pad - (0 - lo) / span * (h - 2 * pad)
    path = " ".join(f"{'M' if i == 0 else 'L'}{x:.1f},{y:.1f}" for i, (x, y) in enumerate(zip(xs, ys)))
    last = vals[-1]
    color = "var(--up)" if last >= 0 else "var(--down)"
    return (f'<svg viewBox="0 0 {w} {h}" role="img" aria-label="ผลชนะตลาดสะสม">'
            f'<line x1="{pad}" x2="{w - pad}" y1="{zero:.1f}" y2="{zero:.1f}" class="axis"/>'
            f'<text x="{pad}" y="{zero - 6:.1f}" class="lbl">0%</text>'
            f'<path d="{path}" fill="none" stroke="{color}" stroke-width="2.5" stroke-linejoin="round"/>'
            f'<circle cx="{xs[-1]:.1f}" cy="{ys[-1]:.1f}" r="4" fill="{color}"/>'
            f'<text x="{w - pad}" y="{ys[-1] - 10:.1f}" text-anchor="end" class="lbl strong">{last:+.1%}</text>'
            f'<text x="{pad}" y="{h - 6}" class="lbl">{html.escape(points[0][0])}</text>'
            f'<text x="{w - pad}" y="{h - 6}" text-anchor="end" class="lbl">{html.escape(points[-1][0])}</text>'
            '</svg>')


def _group_table(rows):
    if not rows:
        return '<p class="muted">ยังไม่มีข้อมูล</p>'
    body = "".join(
        f"<tr><td>{html.escape(g['label'])}</td><td>{g['wins']}/{g['n']}</td>"
        f"<td>{g['wins'] / g['n']:.0%}</td><td class='{_cls(g['avg'])}'>{_pct(g['avg'])}</td>"
        f"<td class='{_cls(g['excess'])}'>{_pct(g['excess'])}</td></tr>" for g in rows)
    return ("<table><thead><tr><th></th><th>ถูก/ทั้งหมด</th><th>อัตราถูก</th><th>ผลเฉลี่ย</th>"
            f"<th>ชนะตลาด</th></tr></thead><tbody>{body}</tbody></table>")


def _cls(x):
    return "" if x is None else ("up" if x > 0 else "down" if x < 0 else "")


def _th_date(iso, fmt="%d/%m %H:%M"):
    if not iso:
        return "–"
    return dt.datetime.fromisoformat(iso).astimezone(TH_TZ).strftime(fmt)


def write_dashboard():
    s = compute_stats()
    now = dt.datetime.now(TH_TZ)
    closed = sorted(s["closed_rows"], key=lambda r: r["exit_at"] or "")
    cum, pts = 0.0, []
    for r in closed:
        cum += r["excess"] if r["excess"] is not None else r["ret"]
        pts.append((_th_date(r["exit_at"], "%d/%m"), cum))

    def kpi(label, value, cls=""):
        return f'<div class="kpi"><div class="k-label">{label}</div><div class="k-val {cls}">{value}</div></div>'

    kpis = (kpi("ปิดแล้ว", s["closed"]) + kpi("กำลังถือ", s["open"]) + kpi("รอราคา", s["pending"])
            + kpi("ทายทิศถูก", f"{s['win']:.0%}" if s["closed"] else "–")
            + kpi("ผลเฉลี่ย/สัญญาณ", _pct(s.get("avg")), _cls(s.get("avg")))
            + kpi("ชนะตลาดเฉลี่ย", _pct(s.get("avg_excess")), _cls(s.get("avg_excess"))))

    active = sorted([r for r in s["v2"] if r["status"] in ("open", "pending")],
                    key=lambda r: r["created_at"], reverse=True)
    act_rows = []
    for r in active:
        cur = _PRICE_CACHE.get(r["ticker"])
        pl = None
        if r["status"] == "open" and cur and r["entry_price"]:
            pl = (cur[0] / r["entry_price"] - 1) * (-1 if r["direction"] == "SHORT" else 1)
        status = "รอราคา" if r["status"] == "pending" else f"{r['horizon']} วันทำการ"
        act_rows.append(
            f"<tr><td><b>{html.escape(r['ticker'])}</b></td>"
            f"<td><span class='tag {r['direction'].lower()}'>{r['direction']}</span></td>"
            f"<td class='{_cls(pl)}'>{_pct(pl)}</td>"
            f"<td>{'★' * r['confidence']}</td><td>{_th_date(r['created_at'])}</td><td>{status}</td>"
            f"<td class='ev'><a href='{html.escape(r['link'] or '#')}' target='_blank' rel='noopener'>"
            f"{html.escape((r['event'] or '')[:70])}</a></td></tr>")
    act_table = ("<table><thead><tr><th>หุ้น</th><th>ทิศ</th><th>ตอนนี้</th><th>มั่นใจ</th><th>ออกเมื่อ</th>"
                 "<th>ระยะ</th><th>ข่าว</th></tr></thead><tbody>" + "".join(act_rows) + "</tbody></table>"
                 ) if act_rows else '<p class="muted">ยังไม่มีสัญญาณที่กำลังถือ</p>'

    done_rows = "".join(
        f"<tr><td><b>{html.escape(r['ticker'])}</b></td>"
        f"<td><span class='tag {r['direction'].lower()}'>{r['direction']}</span></td>"
        f"<td class='{_cls(r['ret'])}'>{_pct(r['ret'])}</td>"
        f"<td class='{_cls(r['excess'])}'>{_pct(r['excess'])}</td>"
        f"<td>{'★' * r['confidence']}</td><td>{_th_date(r['exit_at'], '%d/%m')}</td><td>{_pct(r['bench_ret'])}</td>"
        f"<td class='ev'>{html.escape((r['event'] or '')[:70])}</td></tr>"
        for r in reversed(closed[-50:]))
    done_table = ("<table><thead><tr><th>หุ้น</th><th>ทิศ</th><th>ผล</th><th>ชนะตลาด</th><th>มั่นใจ</th>"
                  "<th>ปิดเมื่อ</th><th>ตลาด</th><th>ข่าว</th></tr></thead><tbody>" + done_rows + "</tbody></table>"
                  ) if done_rows else '<p class="muted">ยังไม่มีสัญญาณครบกำหนด</p>'

    page = f"""<!doctype html>
<html lang="th"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>News Trader Scorecard</title>
<style>
:root {{ --bg:#f6f7f9; --card:#fff; --ink:#15181d; --muted:#6b7280; --line:#e5e7eb;
  --up:#15803d; --down:#c62828; --accent:#4f46e5; }}
@media (prefers-color-scheme: dark) {{ :root {{ --bg:#0f1115; --card:#181b21; --ink:#e8eaed; --muted:#9aa0a6;
  --line:#2a2e36; --up:#4ade80; --down:#f87171; --accent:#a5b4fc; }} }}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--ink);
  font:15px/1.5 system-ui,-apple-system,"Segoe UI","Noto Sans Thai",Sarabun,sans-serif; }}
main {{ max-width:1000px; margin:0 auto; padding:24px 16px 48px; }}
h1 {{ font-size:24px; margin:0 0 4px; }} h2 {{ font-size:17px; margin:0 0 12px; }}
.muted {{ color:var(--muted); }} .up {{ color:var(--up); }} .down {{ color:var(--down); }}
.card {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:18px; margin-top:16px; }}
.kpis {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(140px,1fr)); gap:12px; margin-top:16px; }}
.kpi {{ background:var(--card); border:1px solid var(--line); border-radius:12px; padding:14px; }}
.k-label {{ color:var(--muted); font-size:13px; }} .k-val {{ font-size:24px; font-weight:700; margin-top:2px; }}
.grid2 {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
@media (max-width:720px) {{ .grid2 {{ grid-template-columns:1fr; }} }}
.scroll {{ overflow-x:auto; }}
table {{ width:100%; border-collapse:collapse; font-size:14px; }}
th,td {{ text-align:left; padding:7px 8px; font-variant-numeric:tabular-nums; border-bottom:1px solid var(--line); white-space:nowrap; }}
th {{ color:var(--muted); font-weight:600; font-size:13px; }}
td.ev {{ white-space:normal; min-width:220px; }} a {{ color:var(--accent); text-decoration:none; }}
.tag {{ font-size:12px; font-weight:700; padding:2px 8px; border-radius:99px; }}
.tag.long {{ background:color-mix(in srgb,var(--up) 15%,transparent); color:var(--up); }}
.tag.short {{ background:color-mix(in srgb,var(--down) 15%,transparent); color:var(--down); }}
svg {{ width:100%; height:auto; }} .axis {{ stroke:var(--line); stroke-dasharray:4 4; }}
.lbl {{ fill:var(--muted); font-size:12px; }} .lbl.strong {{ fill:var(--ink); font-weight:700; font-size:14px; }}
footer {{ margin-top:24px; font-size:13px; }}
@media (max-width:520px) {{ .lbl {{ font-size:20px; }} .lbl.strong {{ font-size:22px; }} th,td {{ padding:6px; }} }}
</style></head><body><main>
<h1>📰 News Trader Scorecard</h1>
<div class="muted">อัปเดต {now:%d/%m/%Y %H:%M} น. · ตลาด: {", ".join(MARKETS)} · นับเฉพาะสัญญาณหลังปรับปรุงระบบ</div>
<div class="kpis">{kpis}</div>
<div class="card"><h2>ผลชนะตลาดสะสม</h2>{_svg_curve(pts)}
<div class="muted" style="font-size:13px">รวมผลต่างเทียบดัชนีของทุกสัญญาณที่ครบกำหนด เส้นขึ้น = บอทเลือกได้ดีกว่าซื้อดัชนีเฉย ๆ</div></div>
<div class="grid2">
<div class="card"><h2>แยกตามความมั่นใจ</h2><div class="scroll">{_group_table(s["by_conf"])}</div></div>
<div class="card"><h2>แยกตามตลาด / ทิศ</h2><div class="scroll">{_group_table(s["by_market"] + s["by_dir"])}</div></div>
</div>
<div class="card"><h2>กำลังถือ ({len(act_rows)})</h2><div class="scroll">{act_table}</div></div>
<div class="card"><h2>ครบกำหนดล่าสุด</h2><div class="scroll">{done_table}</div></div>
<footer class="muted">สัญญาณสร้างโดย AI จากข่าว ไม่ใช่คำแนะนำการลงทุน · ราคาจาก Yahoo Finance อาจล่าช้า</footer>
</main></body></html>"""
    os.makedirs(os.path.dirname(DASHBOARD_PATH) or ".", exist_ok=True)
    with open(DASHBOARD_PATH, "w", encoding="utf-8") as f:
        f.write(page)
    nojekyll = os.path.join(os.path.dirname(DASHBOARD_PATH) or ".", ".nojekyll")
    if not os.path.exists(nojekyll):
        open(nojekyll, "w").close()


# ---------------------------------------------------------------- posts
def event_embed(ev, item, calls):
    longs = sum(c["direction"] == "LONG" for c in calls)
    color = 0x2ECC71 if longs * 2 > len(calls) else 0xE74C3C if longs * 2 < len(calls) else 0xF1C40F
    fields = []
    for c in calls:
        stars = "★" * c["confidence"] + "☆" * (5 - c["confidence"])
        wait = "" if c["_open"] else "\n⏳ ตลาดปิด รอราคาใหม่"
        fields.append({
            "name": f"{'🟢 LONG' if c['direction'] == 'LONG' else '🔴 SHORT'}  {c['ticker']}",
            "value": (f"{RELATIONS_TH.get(c['relation'], c['relation'])} · {c['horizon']} วันทำการ · {stars}\n"
                      f"{c['reason']}{wait}")[:1024],
            "inline": False})
    return {"title": f"📰 {ev['title']}"[:256], "url": item["link"], "description": ev["summary"][:1000],
            "color": color, "fields": fields,
            "footer": {"text": f"ที่มา: {item['source']} · ไม่ใช่คำแนะนำการลงทุน"},
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat()}


# ---------------------------------------------------------------- main
def run_news():
    held = active_tickers()
    prefetch(held | {benchmark_of(t) for t in held})
    fill_pending()
    closed = close_expired()
    if closed:
        send_webhook([results_embed(closed)])

    items = collect_news()
    if not items:
        return
    held = active_tickers()
    events = analyze(items, held, recent_events())
    mark_seen(items)

    now = dt.datetime.now(dt.timezone.utc)
    posted = 0
    used = set(held)
    for ev in events[:MAX_EVENTS]:
        idx = ev.get("headline_index", -1)
        if not isinstance(idx, int) or not 0 <= idx < len(items):
            continue
        item, calls = items[idx], []
        for call in ev.get("calls", []):
            t = str(call.get("ticker", "")).upper().strip()
            call["ticker"] = t
            if call.get("direction") not in ("LONG", "SHORT"):
                continue
            if not allowed_ticker(t):
                log.info("skip %s: market not in %s", t, MARKETS)
                continue
            if t in used:
                log.info("skip %s: already has an active signal", t)
                continue
            try:
                call["horizon"] = min(max(int(call.get("horizon", 5)), 1), 20)
                call["confidence"] = min(max(int(call.get("confidence", 3)), 1), 5)
            except (TypeError, ValueError):
                continue
            try:
                bar = latest_bar(t)
            except PriceUnavailable as e:
                log.warning("price unavailable %s: %s", t, e)
                bar = "unknown"
            if bar is None:
                log.info("drop unknown ticker %s", t)
                continue
            is_open = bar != "unknown" and (now - bar[1]) < dt.timedelta(minutes=30)
            call["_open"] = is_open
            calls.append(call)
            used.add(t)
            with db() as c:
                cur = c.execute(
                    """INSERT INTO calls (created_at, event, link, ticker, direction, relation, horizon,
                       confidence, reason, status, ver, market) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (now.isoformat(), ev["title"], item["link"], t, call["direction"], call["relation"],
                     call["horizon"], call["confidence"], call["reason"], "pending", VERSION, market_of(t)))
                call_id = cur.lastrowid
            if is_open:
                open_call(call_id, bar[0], bar[1], t)
        if calls:
            top = max(c["confidence"] for c in calls)
            content = None
            if top >= ALERT_MIN_CONF and ALERT_MENTION:
                content = f"{ALERT_MENTION} 🚨 สัญญาณความมั่นใจ {top} ดาว"
            send_webhook([event_embed(ev, item, calls)], content=content)
            posted += 1
    log.info("posted %d events", posted)


if __name__ == "__main__":
    started = time.time()
    init_db()
    if RUN_MODE == "stats":
        held = active_tickers()
        prefetch(held)
        send_webhook([stats_embed()])
    else:
        run_news()
    try:
        write_dashboard()
    except Exception:  # noqa: BLE001
        log.exception("dashboard failed")
    log.info("done in %.0fs", time.time() - started)
    logging.shutdown()
    os._exit(0)  # ปิดทันที ไม่รอ thread ค้างของไลบรารี
