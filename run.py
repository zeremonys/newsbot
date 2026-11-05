"""
NewsTrader (เวอร์ชัน GitHub Actions + Discord Webhook)
รันครั้งเดียวแล้วจบ: GitHub Actions จะปลุกสคริปต์นี้ขึ้นมาทุกชั่วโมง
ดึงข่าว -> ให้ AI วิเคราะห์ -> โพสต์ LONG/SHORT ลง Discord -> บันทึกผลใน newsbot.db
"""

import datetime as dt
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

# ---------------------------------------------------------------- config
WEBHOOK_URL = os.environ.get("DISCORD_WEBHOOK_URL", "").strip()
if not WEBHOOK_URL:
    raise SystemExit("ยังไม่ได้ตั้งค่า DISCORD_WEBHOOK_URL")

PROVIDER = (os.getenv("PROVIDER") or "gemini").lower()
PROVIDERS = {
    "gemini": {"base_url": "https://generativelanguage.googleapis.com/v1beta/openai/",
               "key_env": "GEMINI_API_KEY",
               "models": "gemini-3.8-flash,gemini-2.5-flash,gemini-2.5-flash-lite",
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
MARKETS = _env("MARKETS", "US,TH")
LANGUAGE = _env("LANGUAGE", "Thai")
MAX_HEADLINES = int(_env("MAX_HEADLINES", str(_P["max_headlines"])))
NEWS_MAX_AGE_HOURS = int(_env("NEWS_MAX_AGE_HOURS", "3"))
BOT_NAME = _env("BOT_NAME", "News Trader")
DB_PATH = _env("DB_PATH", "newsbot.db")
RUN_MODE = _env("RUN_MODE", "news")  # news / stats

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("newsbot")

RELATIONS_TH = {"direct": "ได้รับผลโดยตรง", "competitor": "คู่แข่ง", "supplier": "ซัพพลายเออร์",
                "customer": "ลูกค้า", "sector": "ทั้งเซกเตอร์"}


# ---------------------------------------------------------------- database
def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
        # ลบประวัติข่าวที่เก่ากว่า 7 วัน ไม่ให้ไฟล์ใหญ่ขึ้นเรื่อย ๆ
        old = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=7)).isoformat()
        c.execute("DELETE FROM seen WHERE ts < ?", (old,))


# ---------------------------------------------------------------- discord webhook
def send_webhook(embeds):
    body = json.dumps({"username": BOT_NAME, "embeds": embeds}).encode()
    for _ in range(3):
        req = urllib.request.Request(WEBHOOK_URL, data=body, method="POST", headers={
            "Content-Type": "application/json", "User-Agent": "newsbot (github-actions, 1.0)"})
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
                    "ticker": {"type": "string", "description": "Yahoo Finance ticker e.g. NVDA, PTT.BK, 7203.T"},
                    "direction": {"type": "string", "enum": ["LONG", "SHORT"]},
                    "relation": {"type": "string", "enum": list(RELATIONS_TH)},
                    "horizon": {"type": "integer", "description": "trading days, 1-20"},
                    "confidence": {"type": "integer", "description": "1-5"},
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
For each event give 1-4 stock calls. Prefer markets: {MARKETS}. Use valid Yahoo Finance tickers
(Thai stocks end in .BK). Mark relation: direct, competitor, supplier, customer or sector.
Be honest with confidence: 5 is rare. If nothing is material, return an empty events list.
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
        resp = Anthropic(api_key=key).messages.create(
            model=model, max_tokens=4000, system=SYSTEM_PROMPT,
            tools=[{"name": TOOL_NAME, "description": "Report market-moving events", "input_schema": SCHEMA}],
            tool_choice={"type": "tool", "name": TOOL_NAME},
            messages=[{"role": "user", "content": user_msg}])
        for b in resp.content:
            if b.type == "tool_use":
                return b.input.get("events", [])
        return []

    from openai import BadRequestError, OpenAI
    client = OpenAI(api_key=key, base_url=_P["base_url"])
    tool = {"type": "function", "function": {
        "name": TOOL_NAME, "description": "Report market-moving events", "parameters": SCHEMA}}
    msgs = [{"role": "system", "content": SYSTEM_PROMPT}, {"role": "user", "content": user_msg}]
    err = None
    for choice in ("required", "auto"):
        try:
            resp = client.chat.completions.create(
                model=model, messages=msgs, tools=[tool], tool_choice=choice, max_tokens=4000)
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
    client = OpenAI(api_key=os.environ.get(_P["key_env"], "").strip(), base_url=_P["base_url"])
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


def analyze(items):
    user_msg = "Headlines:\n" + "\n".join(
        f"[{n}] ({i['source']}) {i['title']} — {i['summary']}" for n, i in enumerate(items))
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


def latest_bar(ticker):
    """(ราคา, เวลา UTC) ของแท่งล่าสุด / None = ไม่มี ticker นี้ / PriceUnavailable = ดึงไม่ได้"""
    try:
        t = yf.Ticker(ticker)
        h = t.history(period="5d", interval="5m")
        if h.empty:
            h = t.history(period="5d")
    except Exception as e:  # noqa: BLE001
        raise PriceUnavailable(str(e)) from e
    if h.empty:
        return None
    ts = h.index[-1].to_pydatetime().astimezone(dt.timezone.utc)
    return float(h["Close"].iloc[-1]), ts


def fill_pending():
    with db() as c:
        rows = c.execute("SELECT * FROM calls WHERE status='pending'").fetchall()
    for r in rows:
        try:
            bar = latest_bar(r["ticker"])
        except PriceUnavailable:
            continue
        if bar and bar[1] > dt.datetime.fromisoformat(r["created_at"]):
            with db() as c:
                c.execute("UPDATE calls SET status='open', entry_price=?, entry_at=? WHERE id=?",
                          (bar[0], bar[1].isoformat(), r["id"]))


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
        ret = bar[0] / r["entry_price"] - 1
        if r["direction"] == "SHORT":
            ret = -ret
        with db() as c:
            c.execute("UPDATE calls SET status='closed', exit_price=?, exit_at=?, ret=? WHERE id=?",
                      (bar[0], bar[1].isoformat(), ret, r["id"]))
        closed.append({**dict(r), "ret": ret})
    return closed


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


def results_embed(closed):
    lines = []
    for r in closed[:20]:
        mark = "✅" if r["ret"] > 0 else "❌"
        lines.append(f"{mark} **{r['direction']} {r['ticker']}** {r['ret']:+.2%} · {r['event'][:60]}")
    return {"title": "🏁 สัญญาณที่ครบกำหนด", "description": "\n".join(lines), "color": 0x5865F2}


def stats_embed():
    with db() as c:
        closed = c.execute("SELECT confidence, ret FROM calls WHERE status='closed'").fetchall()
        n_open = c.execute("SELECT COUNT(*) FROM calls WHERE status='open'").fetchone()[0]
        n_pend = c.execute("SELECT COUNT(*) FROM calls WHERE status='pending'").fetchone()[0]
    fields = [{"name": "กำลังถือ", "value": str(n_open), "inline": True},
              {"name": "รอราคา", "value": str(n_pend), "inline": True},
              {"name": "ปิดแล้ว", "value": str(len(closed)), "inline": True}]
    if closed:
        rets = [r["ret"] for r in closed]
        fields += [{"name": "อัตราถูก", "value": f"{sum(x > 0 for x in rets) / len(rets):.0%}", "inline": True},
                   {"name": "ผลตอบแทนเฉลี่ย/สัญญาณ", "value": f"{np.mean(rets):+.2%}", "inline": True}]
        by_conf = []
        for k in range(1, 6):
            sub = [r["ret"] for r in closed if r["confidence"] == k]
            if sub:
                by_conf.append(f"{'★' * k}: ถูก {sum(x > 0 for x in sub)}/{len(sub)}, เฉลี่ย {np.mean(sub):+.2%}")
        if by_conf:
            fields.append({"name": "แยกตามความมั่นใจ", "value": "\n".join(by_conf), "inline": False})
    return {"title": "📊 สถิติสัญญาณประจำสัปดาห์", "color": 0x5865F2, "fields": fields}


# ---------------------------------------------------------------- main
def run_news():
    fill_pending()
    closed = close_expired()
    if closed:
        send_webhook([results_embed(closed)])

    items = collect_news()
    if not items:
        return
    events = analyze(items)
    mark_seen(items)

    now = dt.datetime.now(dt.timezone.utc)
    posted = 0
    for ev in events[:MAX_EVENTS]:
        idx = ev.get("headline_index", -1)
        if not isinstance(idx, int) or not 0 <= idx < len(items):
            continue
        item, calls = items[idx], []
        for call in ev.get("calls", []):
            call["ticker"] = str(call.get("ticker", "")).upper().strip()
            call["horizon"] = min(max(int(call.get("horizon", 5)), 1), 20)
            call["confidence"] = min(max(int(call.get("confidence", 3)), 1), 5)
            try:
                bar = latest_bar(call["ticker"])
            except PriceUnavailable as e:
                log.warning("price unavailable %s: %s", call["ticker"], e)
                bar = "unknown"
            if bar is None:
                log.info("drop unknown ticker %s", call["ticker"])
                continue
            is_open = bar != "unknown" and (now - bar[1]) < dt.timedelta(minutes=30)
            call["_open"] = is_open
            calls.append(call)
            with db() as c:
                c.execute("""INSERT INTO calls (created_at, event, link, ticker, direction, relation, horizon,
                             confidence, reason, status, entry_price, entry_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                          (now.isoformat(), ev["title"], item["link"], call["ticker"], call["direction"],
                           call["relation"], call["horizon"], call["confidence"], call["reason"],
                           "open" if is_open else "pending",
                           bar[0] if is_open else None, bar[1].isoformat() if is_open else None))
        if calls:
            send_webhook([event_embed(ev, item, calls)])
            posted += 1
    log.info("posted %d events", posted)


if __name__ == "__main__":
    init_db()
    if RUN_MODE == "stats":
        send_webhook([stats_embed()])
    else:
        run_news()
