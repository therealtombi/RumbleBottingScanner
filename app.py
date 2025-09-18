# app.py - Rumble Bot-Suspect Scanner
# - Continuous scanning loop with data updates
# - Per-category accordion tables (sortable)
# - Dark wide UI
# - Filters (ranges) applied DURING scan
# - No duplicate entries; updates existing rows and charts

import json
import re
import time
import sqlite3
import threading
import io
import csv
import queue
import traceback
from contextlib import closing
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, Response, render_template_string, request, abort, jsonify

# ---------------- Config ----------------
BASE_URL = "https://rumble.com"
BROWSE_URL = f"{BASE_URL}/browse"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TombiBot/1.0; +https://example.com)"}
REQUEST_TIMEOUT = 20
POLITE_DELAY_SEC = 0.3
VIEWS_SKIP_MAX = 20000
USE_BROWSER = True
DB_PATH = "suspects.sqlite"
SCAN_INTERVAL_SECONDS = 60  # Re-scan every 1 minute

# Default filter ranges
FILTER_DEFAULTS = {
    "views_min": 0, "views_max": 1_000_000,
    "viewers_min": 100, "viewers_max": 50_000,
    "ads_min": 0, "ads_max": 100,
    "chat_min": 0, "chat_max": 5_000,
    "age_min": 0, "age_max": 100,
    "cat_workers": 8,
    "page_workers": 10,
}

# Whitelist
WHITELIST_TITLES = [
    "News", "Politics", "24/7", "Republican Politics", "Entertainment",
    "Podcasts", "Music", "Conspiracies", "Cooking", "Gaming", "Vlogs",
    "Finance & Crypto",
]
WHITELIST_SLUG_MAP = {
    "News": "news", "Politics": "politics", "Republican Politics": "republican-politics",
    "Entertainment": "entertainment", "Podcasts": "podcasts", "Music": "music",
    "Conspiracies": "conspiracies", "Cooking": "cooking", "Gaming": "gaming",
    "Vlogs": "vlogs", "Finance & Crypto": "finance-and-crypto",
}

DB_LOCK = threading.RLock()


# ------------- App State Management ---------------
class AppState:
    def __init__(self):
        self.scan_thread = None
        self.stop_event = threading.Event()


APP_STATE = AppState()


# ------------- Shared HTTP session ---------------
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retries = Retry(total=3, backoff_factor=0.4, status_forcelist=(429, 500, 502, 503, 504))
    adapter = HTTPAdapter(max_retries=retries, pool_connections=100, pool_maxsize=100)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


HTTP = build_session()


# ------------- SSE Event Broker ---------------
class EventBroker:
    def __init__(self):
        self._subs: set[queue.Queue] = set()
        self._lock = threading.Lock()

    def subscribe(self):
        q = queue.Queue()
        with self._lock: self._subs.add(q)
        return q

    def unsubscribe(self, q):
        with self._lock: self._subs.discard(q)

    def publish(self, event: dict):
        with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(event)
            except Exception:
                pass


BROKER = EventBroker()


# ------------- Utilities ---------------
def txt(el) -> str:
    try:
        return el.get_text(strip=True) if el else ""
    except Exception:
        return ""


def get_soup(url: str) -> BeautifulSoup:
    r = HTTP.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return BeautifulSoup(r.text, "lxml")


def parse_viewers_text(txt_in: str) -> int:
    if not txt_in: return 0
    m = re.search(r"([\d.,]+)\s*([kKmM])?", txt_in)
    if not m: return 0
    num_str = m.group(1).replace(",", "")
    num = float(num_str) if "." in num_str else int(num_str)
    unit = (m.group(2) or "").lower()
    if unit == "k":
        num *= 1000
    elif unit == "m":
        num *= 1_000_000
    return int(num)


def parse_joined_to_date(joined_str: str):
    if not joined_str: return None
    cand, fmts = joined_str.strip(), ["%b %d, %Y", "%B %d, %Y", "%b %Y", "%B %Y"]
    for f in fmts:
        try:
            return datetime.strptime(cand, f).replace(tzinfo=timezone.utc)
        except Exception:
            continue
    return None


def compute_age_days(joined_str: str | None) -> int | None:
    dt = parse_joined_to_date(joined_str)
    if not dt: return None
    return max(0, (datetime.now(timezone.utc) - dt).days)


# ----------- Playwright helpers -----------
def browser_get_page_details(stream_url: str) -> dict:
    default_payload = {"current_viewers": 0, "chat_count": 0, "ads_count": 0, "upvotes": 0, "downvotes": 0,
                       "duration": "00:00:00", "followers": 0}
    if not USE_BROWSER: return default_payload
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return default_payload

    full_url = stream_url if stream_url.startswith("http") else urljoin(BASE_URL, stream_url)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = context.new_page()
            page.goto(full_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(7000)

            try:
                viewer_text = page.locator(".live-video-view-count-status-count").inner_text(timeout=1000)
                default_payload["current_viewers"] = parse_viewers_text(viewer_text)
            except Exception:
                pass

            try:
                duration_text = page.locator(".live-video-view-count-status-duration").inner_text(timeout=1000)
                default_payload["duration"] = duration_text.strip()
            except Exception:
                pass

            try:
                follower_text = page.locator(".media-heading-num-followers").inner_text(timeout=1000)
                default_payload["followers"] = parse_viewers_text(follower_text)
            except Exception:
                pass

            for sel in [".chat-history-list .chat-history--item", "[class*='chat-history'] li",
                        "#chat-history-list .chat-history--row"]:
                try:
                    count = page.eval_on_selector_all(sel, "els => els.length")
                    if count and int(count) > 0:
                        default_payload["chat_count"] = int(count)
                        break
                except Exception:
                    pass

            ads_found = 0
            ad_selectors = ["div.undecorate-links", ".media-sidebar-ads", "div[id^='ad-container']"]
            for selector in ad_selectors:
                try:
                    ads_found += page.eval_on_selector_all(selector, "els => els.length")
                except Exception:
                    pass
            default_payload["ads_count"] = ads_found

            try:
                up_text = page.locator('span[data-js="rumbles_up_votes"]').inner_text(timeout=1000)
                default_payload["upvotes"] = parse_viewers_text(up_text)
            except Exception:
                pass
            try:
                down_text = page.locator('span[data-js="rumbles_down_votes"]').inner_text(timeout=1000)
                default_payload["downvotes"] = parse_viewers_text(down_text)
            except Exception:
                pass

            context.close();
            browser.close()
    except Exception as e:
        print(f"Playwright error on {full_url}: {e}")
    return default_payload


def fetch_channel_details_fallback(username: str) -> dict:
    default_payload = {"joined_date": None, "total_videos": 0}
    if not username: return default_payload
    base_profile_url = None
    try:
        r = HTTP.head(urljoin(BASE_URL, f"/c/{username}"), timeout=10, allow_redirects=True)
        if r.status_code == 200: base_profile_url = r.url
    except Exception:
        pass
    if not base_profile_url:
        try:
            r = HTTP.head(urljoin(BASE_URL, f"/user/{username}"), timeout=10, allow_redirects=True)
            if r.status_code == 200: base_profile_url = r.url
        except Exception:
            pass

    if not base_profile_url:
        print(f"Could not find a valid profile for {username}")
        return default_payload

    joined_date, total_videos = None, 0
    try:
        about_soup = get_soup(f"{base_profile_url}/about")
        sidebar = about_soup.select_one(".channel-about-sidebar--inner")
        if sidebar:
            for p in sidebar.select("p"):
                p_text = txt(p)
                if "Joined" in p_text:
                    m = re.search(r"Joined\s+(.+)", p_text, re.IGNORECASE)
                    if m: joined_date = m.group(1).strip()
                elif "videos" in p_text:
                    total_videos = parse_viewers_text(p_text)
    except Exception as e:
        print(f"Error during fallback scrape for {username}: {e}")

    return {"joined_date": joined_date, "total_videos": total_videos}


# ------------- DB helpers --------------
def init_db():
    with DB_LOCK:
        with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            conn.execute("CREATE TABLE IF NOT EXISTS scans (id INTEGER PRIMARY KEY, ts TEXT NOT NULL);")
            conn.execute("""
            CREATE TABLE IF NOT EXISTS suspects (
                id INTEGER PRIMARY KEY, scan_id INTEGER, category TEXT, title TEXT, channel TEXT,
                views INTEGER, chat_count INTEGER, ads_count INTEGER, current_viewers INTEGER,
                channel_joined TEXT, username TEXT, url TEXT, video_id TEXT UNIQUE,
                account_age_days INTEGER, followers INTEGER, total_videos INTEGER,
                upvotes INTEGER, downvotes INTEGER, created_at TEXT,
                duration TEXT
            );""")
            conn.execute("""
            CREATE TABLE IF NOT EXISTS tracking_data (
                id INTEGER PRIMARY KEY, suspect_id INTEGER NOT NULL, ts TEXT NOT NULL,
                viewers INTEGER, upvotes INTEGER, downvotes INTEGER, ads_count INTEGER,
                FOREIGN KEY(suspect_id) REFERENCES suspects(id) ON DELETE CASCADE
            );""")

            cur = conn.cursor()
            cur.execute("PRAGMA table_info(suspects)")
            existing = {row[1] for row in cur.fetchall()}
            if 'user_followers' in existing: cur.execute("ALTER TABLE suspects DROP COLUMN user_followers")
            if 'channel_followers' in existing: cur.execute("ALTER TABLE suspects DROP COLUMN channel_followers")
            if 'duration' not in existing: cur.execute("ALTER TABLE suspects ADD COLUMN duration TEXT")
            if 'followers' not in existing: cur.execute("ALTER TABLE suspects ADD COLUMN followers INTEGER")

            cur.execute("PRAGMA table_info(tracking_data)")
            existing_tracking = {row[1] for row in cur.fetchall()}
            if 'ads_count' not in existing_tracking: cur.execute(
                "ALTER TABLE tracking_data ADD COLUMN ads_count INTEGER")

            conn.commit()


def get_recent_suspects():
    with DB_LOCK:
        with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn:
            cur = conn.cursor()
            cur.execute("""
            SELECT
                s.id, s.category, s.title, s.username,
                COALESCE(s.channel_joined,'') AS channel_joined,
                COALESCE(s.current_viewers,0) AS current_viewers,
                COALESCE(s.chat_count,0) AS chat_count,
                s.url,
                COALESCE(s.account_age_days, -1) AS account_age_days,
                s.created_at,
                COALESCE(s.followers, 0) AS followers,
                COALESCE(s.total_videos, 0) AS total_videos,
                COALESCE(s.upvotes, 0) AS upvotes,
                COALESCE(s.downvotes, 0) AS downvotes,
                COALESCE(s.duration, '00:00:00') AS duration,
                COALESCE(s.ads_count, 0) as ads_count
            FROM suspects s
            ORDER BY s.id DESC
            """)
            return cur.fetchall()


def clear_all_data():
    if APP_STATE.scan_thread and APP_STATE.scan_thread.is_alive():
        print("Stop signal sent to scanner before clearing data.")
        APP_STATE.stop_event.set()
        APP_STATE.scan_thread.join(timeout=10)
        APP_STATE.scan_thread = None
        APP_STATE.stop_event.clear()

    with DB_LOCK:
        with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn:
            conn.execute("PRAGMA foreign_keys = ON;")
            print("Clearing tracking data...")
            conn.execute("DELETE FROM tracking_data")
            print("Clearing suspects...")
            conn.execute("DELETE FROM suspects")
            print("Clearing scans...")
            conn.execute("DELETE FROM scans")
            conn.commit()
    try:
        with DB_LOCK:
            with closing(sqlite3.connect(DB_PATH, isolation_level=None, check_same_thread=False)) as conn:
                print("Compacting database (VACUUM)...")
                conn.execute("VACUUM")
    except Exception as e:
        print(f"VACUUM failed (skipped): {e}")


# ----------- Core Logic -----------
def handle_suspect_match(scan_id: int, metrics: dict):
    if not metrics.get("video_id"): return
    with DB_LOCK:
        with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn:
            cur = conn.cursor()
            cur.execute("SELECT id FROM suspects WHERE video_id = ?", (metrics["video_id"],))
            existing = cur.fetchone()
            ts = datetime.now(timezone.utc).isoformat()
            if existing:
                suspect_id = existing[0]
                cur.execute("""
                    UPDATE suspects SET
                    title = ?, channel = ?, views = ?, chat_count = ?, ads_count = ?, current_viewers = ?,
                    followers = ?, total_videos = ?, upvotes = ?, downvotes = ?, duration = ?
                    WHERE id = ?
                """, (
                    metrics.get("title"), metrics.get("channel"), int(metrics.get("views", 0)),
                    int(metrics.get("chat_count", 0)),
                    int(metrics.get("ads_count", 0)), int(metrics.get("current_viewers", 0)),
                    int(metrics.get("followers", 0)),
                    int(metrics.get("total_videos", 0)), int(metrics.get("upvotes", 0)),
                    int(metrics.get("downvotes", 0)), metrics.get("duration", "00:00:00"), suspect_id
                ))
                event_type = "suspect_update"
                print(f"Updated suspect {suspect_id}: {metrics.get('title')}")
            else:
                cur.execute("""
                    INSERT INTO suspects (video_id, scan_id, category, title, channel, views, chat_count, ads_count,
                    current_viewers, channel_joined, username, url, account_age_days, followers,
                    total_videos, upvotes, downvotes, created_at, duration)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    metrics["video_id"], scan_id, metrics.get("category"), metrics.get("title"), metrics.get("channel"),
                    int(metrics.get("views", 0)), int(metrics.get("chat_count", 0)), int(metrics.get("ads_count", 0)),
                    int(metrics.get("current_viewers", 0)), metrics.get("channel_joined", ""), metrics.get("username"),
                    metrics.get("url"), metrics.get("age_days"), int(metrics.get("followers", 0)),
                    int(metrics.get("total_videos", 0)), int(metrics.get("upvotes", 0)),
                    int(metrics.get("downvotes", 0)), ts, metrics.get("duration", "00:00:00")
                ))
                suspect_id = cur.lastrowid
                event_type = "suspect_add"
                print(f"Saved new suspect {suspect_id}: {metrics.get('title')}")

            cur.execute("""
                INSERT INTO tracking_data (suspect_id, ts, viewers, upvotes, downvotes, ads_count) VALUES (?, ?, ?, ?, ?, ?)
            """, (
            suspect_id, ts, metrics.get("current_viewers", 0), metrics.get("upvotes", 0), metrics.get("downvotes", 0),
            metrics.get("ads_count", 0)))
            conn.commit()

    metrics.update({"id": suspect_id, "created_at": ts})
    BROKER.publish({"type": event_type, "data": metrics})


def process_stream_item(scan_id: int, item: dict, filters: dict):
    if (item.get("views", 0) or 0) > VIEWS_SKIP_MAX: return
    time.sleep(POLITE_DELAY_SEC)

    page_details = browser_get_page_details(item.get("url"))
    ch_user = item.get("username", "")
    channel_details = fetch_channel_details_fallback(ch_user) if ch_user else {}

    metrics = {**item, **page_details, **channel_details}
    metrics["age_days"] = compute_age_days(channel_details.get("joined_date"))
    metrics["channel_joined"] = channel_details.get("joined_date")

    matches, reason = metrics_match_filters(metrics, filters)
    if matches:
        handle_suspect_match(scan_id, metrics)
    else:
        print(f"REJECTED: {metrics.get('title', '?')[:40]}... ({reason})")


def run_scan(scan_id: int, filters: dict):
    cat_workers = max(1, int(filters.get("cat_workers", FILTER_DEFAULTS["cat_workers"])))
    page_workers = max(1, int(filters.get("page_workers", FILTER_DEFAULTS["page_workers"])))
    all_categories = scrape_categories_all()
    categories = filter_or_build_whitelisted_categories(all_categories)
    live_items = []
    with ThreadPoolExecutor(max_workers=cat_workers) as ex:
        futures = {ex.submit(scrape_live_streams_for_category, c["title"], c["url"]): c for c in categories}
        for fut in as_completed(futures):
            try:
                live_items.extend(fut.result() or [])
            except Exception as e:
                print(f"Category scrape failed for {futures[fut].get('title', '?')}: {e}")

    print(f"Found {len(live_items)} total live streams to process. Using {page_workers} page workers.")

    with ThreadPoolExecutor(max_workers=page_workers) as ex:
        tasks = [ex.submit(process_stream_item, scan_id, item, filters) for item in live_items]
        for future in as_completed(tasks):
            try:
                future.result()
            except Exception as e:
                print(f"A stream processing task failed: {e}")


def main_scanner_loop(filters: dict):
    scan_count = 0
    try:
        while not APP_STATE.stop_event.is_set():
            scan_count += 1
            msg = f"Starting Scan #{scan_count}..."
            print(f"\n--- {msg} ---")
            BROKER.publish({"type": "toast", "data": {"message": msg, "level": "info"}})

            with DB_LOCK:
                with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn:
                    cur = conn.cursor()
                    cur.execute("INSERT INTO scans (ts) VALUES (?)", (datetime.now(timezone.utc).isoformat(),))
                    scan_id = cur.lastrowid
                    conn.commit()

            run_scan(scan_id, filters)

            print(f"--- Scan #{scan_count} finished. Waiting {SCAN_INTERVAL_SECONDS}s... ---")
            APP_STATE.stop_event.wait(SCAN_INTERVAL_SECONDS)
    except Exception as e:
        traceback.print_exc()
        BROKER.publish({"type": "toast", "data": {"message": f"Scan crashed: {e}", "level": "error"}})
    finally:
        print("Scan loop stopped.")
        BROKER.publish({"type": "toast", "data": {"message": "Scanning stopped.", "level": "info"}})
        APP_STATE.scan_thread = None
        APP_STATE.stop_event.clear()


def metrics_match_filters(metrics: dict, filters: dict) -> tuple[bool, str]:
    age_days, views, viewers, chat, ads = metrics.get("age_days"), metrics.get("views", 0), metrics.get(
        "current_viewers", 0), metrics.get("chat_count", 0), metrics.get("ads_count", 0)
    if not (filters["views_min"] <= views <= filters["views_max"]): return False, f"views {views} out of range"
    if not (filters["viewers_min"] <= viewers <= filters[
        "viewers_max"]): return False, f"viewers {viewers} out of range"
    if not (filters["chat_min"] <= chat <= filters["chat_max"]): return False, f"chat {chat} out of range"
    if not (filters["ads_min"] <= ads <= filters["ads_max"]): return False, f"ads {ads} out of range"
    if age_days is not None and not (
            filters["age_min"] <= age_days <= filters["age_max"]): return False, f"age {age_days}d out of range"
    return True, ""


# ----------- Scrapers ------------------
def scrape_categories_all():
    print(f"Fetching categories from {BROWSE_URL}")
    soup = get_soup(BROWSE_URL)
    cats = []
    for li in soup.select("li.category"):
        a = li.select_one("a.category__link")
        if not a: continue
        title = txt(li.select_one(".category__title"))
        href = a.get("href", "")
        viewers_txt = txt(li.select_one(".category__views"))
        viewers = parse_viewers_text(viewers_txt)
        if title and href:
            cats.append({"title": title, "url": urljoin(BASE_URL, href), "viewers": viewers})
    print(f"Found {len(cats)} total categories")
    return cats


def scrape_live_streams_for_category(cat_title: str, cat_url: str):
    BROKER.publish({"type": "toast", "data": {"message": f"Scanning: {cat_title}", "level": "info"}})
    print(f"Scanning LIVE streams in: {cat_title}")
    soup = get_soup(cat_url)
    results = []
    for item in soup.select(".videostream.thumbnail__grid-item"):
        if not item.select_one(".videostream__status.videostream__status--live"): continue
        url = urljoin(BASE_URL, item.select_one(".videostream__link").get("href", ""))
        title = txt(item.select_one(".thumbnail__title"))
        username = ""
        ch_link = item.select_one("a[href^='/c/'], a[href^='/user/']")
        if ch_link:
            href = (ch_link.get("href") or "").split("?")[0]
            if href.startswith("/c/"):
                username = href.split("/c/")[1].strip("/")
            elif href.startswith("/user/"):
                username = href.split("/user/")[1].strip("/")

        views_tag = item.select_one(".videostream__data--item.videostream__views")
        total_views = int(views_tag["data-views"]) if views_tag and views_tag.has_attr("data-views") else 0
        concurrent_viewers = parse_viewers_text(txt(item.select_one(".videostream__badge .videostream__number")))
        views = total_views if total_views > 0 else concurrent_viewers

        results.append({
            "category": cat_title, "title": title, "url": url, "channel": txt(item.select_one(".channel__name")),
            "username": username, "views": views, "current_viewers": concurrent_viewers,
            "video_id": item.get("data-video-id") or ""
        })
    print(f"{cat_title}: {len(results)} live streams found")
    return results


def filter_or_build_whitelisted_categories(all_categories: list[dict]) -> list[dict]:
    wanted = {t.casefold(): t for t in WHITELIST_TITLES}
    found = {c["title"]: c for c in all_categories if c["title"].casefold() in wanted}
    for title in WHITELIST_TITLES:
        if title not in found:
            if title == "24/7":
                url = urljoin(BASE_URL, "/live/24-7")
            else:
                slug = WHITELIST_SLUG_MAP.get(title)
                if not slug: continue
                url = urljoin(BASE_URL, f"/category/{slug}")
            found[title] = {"title": title, "url": url, "viewers": 0}
            print(f"Added fallback/special case for '{title}' -> {url}")
    return list(found.values())


# -------------- Filters --------------
def parse_int(val, default):
    try:
        if val is None or val == "": return default
        return int(float(val))
    except Exception:
        return default


def build_filters_from_form(form) -> dict:
    f = {}
    for k, default in FILTER_DEFAULTS.items():
        f[k] = parse_int(form.get(k), default)
    f["cat_workers"] = max(1, min(64, f["cat_workers"]))
    f["page_workers"] = max(1, min(64, f["page_workers"]))
    return f


# -------------- Flask App and Routes ------------------
app = Flask(__name__)
init_db()

INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Rumble Suspected Botting Scanner</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<style>
:root{ color-scheme: dark; }
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:#0f1115;color:#e6e6e6;}
.container{padding:1.2rem;margin:0 auto;}
h1{font-weight:700;letter-spacing:.2px;margin:.2rem 0 .8rem}
.button, button{padding:.5rem .9rem;border:1px solid #2a2f3a;border-radius:.6rem;background:#1a1f29;color:#e6e6e6;cursor:pointer;transition:.15s ease;font-size:1rem;text-decoration:none;}
.button:hover, button:hover{background:#222838}
button:disabled{cursor:not-allowed;background:#11141a;color:#555;}
button.small{font-size:.85rem;padding:.35rem .6rem}
a{color:#6ea8fe;text-decoration:none}
a:hover{text-decoration:underline}
.small{font-size:.95rem;color:#b9c2d0}
.pill{display:inline-block;padding:.15rem .5rem;border:1px solid #2a2f3a;border-radius:999px;background:#131722;color:#cbd5e1;white-space:nowrap}
.danger{background:#2a1111;border:1px solid #552222}
.filters{background:#0f1415;border:1px solid #1f2430;border-radius:.8rem;padding:1rem;margin:.6rem 0}
.filters .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:.8rem}
.filters label{display:block;font-size:.85rem;color:#9fb0c5;margin-bottom:.25rem}
.filters .row{display:grid;grid-template-columns:1fr 1fr;gap:.4rem}
.filters input[type=number]{width:100%;padding:.45rem .5rem;border:1px solid #2a2f3a;border-radius:.5rem;background:#0d1018;color:#e6e6e6}
.actions{display:flex;gap:.6rem;flex-wrap:wrap;align-items:center;margin:.8rem 0 1.2rem}
.accordion{border:1px solid #1f2430;border-radius:.8rem;overflow:hidden;margin-bottom:1rem;background:#0f1415}
.acc-head{display:flex;align-items:center;justify-content:space-between;gap:.8rem;padding:.8rem 1rem;background:#131722;border-bottom:1px solid #1f2430;cursor:pointer;user-select:none; transition: background-color 0.5s ease;}
.acc-head.flash { background-color: #552222 !important; }
.acc-head.has-suspects { border-bottom: 2px solid #992a2a; }
.acc-title{display:flex;align-items:center;gap:.6rem}
.acc-title h2{font-size:1rem;margin:0}
.chev{transition:transform .2s ease}
.acc.open .chev{transform:rotate(90deg)}
.acc-body{max-height:0;overflow:hidden;transition:max-height .25s ease}
.acc.open .acc-body{max-height:1200px}
.table-wrap{overflow:auto}
table{border-collapse:separate;border-spacing:0;width:100%;background:#0f1115}
th,td{border-bottom:1px solid #1f2430;padding:.55rem .6rem;text-align:left;vertical-align:top;white-space:nowrap;}
th:nth-child(2), td:nth-child(2){white-space:normal;min-width:250px;}
th{background:#151a26;position:sticky;top:0;z-index:1;cursor:pointer;user-select:none}
tr:hover td{background:#11151f}
.bad td{background:#161b27}
tr.flash td { background-color: #2c3a1e !important; transition: background-color 0.5s ease; }
.chart-row{display:none;}
.chart-row.visible{display:table-row;}
.chart-container{padding:1rem;background:#0c0e12; position: relative; height: 600px; width: 100%;}
#toast-container{position:fixed;top:20px;right:20px;z-index:2000;display:flex;flex-direction:column;gap:10px;align-items:flex-end;}
.toast{padding:12px 20px;border-radius:.5rem;color:#fff;background:#2a2f3a;box-shadow:0 4px 12px rgba(0,0,0,.3);opacity:0;transform:translateX(100%);transition:all .3s ease-in-out;font-size:.9rem;}
.toast.show{opacity:1;transform:translateX(0);}
.toast.info{background:#1d4ed8;}
.toast.success{background:#16a34a;}
.toast.error{background:#dc2626;}
#modal-backdrop{position:fixed;inset:0;background:rgba(9,11,15,.65);display:none;align-items:center;justify-content:center;z-index:1000}
#modal{background:#0f141d;border:1px solid #1f2430;border-radius:.8rem;max-width:820px;width:92%;max-height:80vh;overflow:auto;box-shadow:0 10px 40px rgba(0,0,0,.5)}
#modal header{display:flex;justify-content:space-between;align-items:center;padding:1rem;border-bottom:1px solid #1f2430}
#modal .content{padding:1rem}
#modal .close{background:#1a1f29;border:1px solid #2a2f3a;padding:.3rem .6rem;border-radius:.4rem;cursor:pointer}
</style>
<script>
const chartInstances = {};
function catId(cat){ return 'cat-' + (cat||'').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,''); }
function sortTableByCol(tableId, col, isNum=true){
    const table = document.getElementById(tableId); if(!table) return;
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.rows).filter(r => !r.classList.contains('chart-row'));
    const dir = table.dataset.dir === 'asc' ? 'desc' : 'asc';
    rows.sort((a,b)=>{
        const x = a.cells[col].dataset.v || a.cells[col].innerText;
        const y = b.cells[col].dataset.v || b.cells[col].innerText;
        const xv = isNum ? parseFloat(x.replace(/,/g, '')) : x.toLowerCase();
        const yv = isNum ? parseFloat(y.replace(/,/g, '')) : y.toLowerCase();
        if (dir === 'asc') return xv > yv ? 1 : -1;
        return xv < yv ? 1 : -1;
    });
    rows.forEach(r => {
        tbody.appendChild(r);
        const chartRow = document.getElementById(`chart-row-${r.dataset.id}`);
        if(chartRow) tbody.appendChild(chartRow);
    });
    table.dataset.dir = dir;
}
function toggleAcc(id){ document.getElementById(id)?.classList.toggle('open'); }
function flashHeader(category) {
    const header = document.querySelector(`#${catId(category)} .acc-head`);
    if (header) {
        header.classList.add('has-suspects');
        header.classList.add('flash');
        setTimeout(() => header.classList.remove('flash'), 1500);
    }
}
function addRowToCategoryTable(d){
    const id = catId(d.category);
    const tbody = document.querySelector(`#${id} .table-wrap tbody`);
    if (!tbody) return;
    tbody.querySelector('.placeholder')?.remove();
    const tr = document.createElement('tr'); tr.className = 'bad';
    tr.dataset.id = d.id;

    const chartRow = document.createElement('tr');
    chartRow.className = 'chart-row';
    chartRow.id = `chart-row-${d.id}`;
    const chartCell = document.createElement('td');
    chartCell.colSpan = 15;
    chartCell.innerHTML = `<div class="chart-container" id="chart-container-${d.id}"><canvas id="chart-${d.id}"></canvas></div>`;
    chartRow.appendChild(chartCell);

    tbody.insertBefore(chartRow, tbody.firstChild);
    tbody.insertBefore(tr, chartRow);

    updateRowContents(tr, d);
    tr.classList.add('flash');
    setTimeout(() => tr.classList.remove('flash'), 1500);
    flashHeader(d.category);
}
function updateRowInCategoryTable(d) {
    const row = document.querySelector(`tr[data-id="${d.id}"]`);
    if (row) {
        updateRowContents(row, d);
        row.classList.add('flash');
        setTimeout(() => row.classList.remove('flash'), 1500);
        flashHeader(d.category);

        const chartRow = document.getElementById(`chart-row-${d.id}`);
        if(chartRow && chartRow.classList.contains('visible')) {
            toggleChart(null, d.id, true);
        }
    } else {
        addRowToCategoryTable(d);
    }
}
function updateRowContents(tr, d) {
    const viewers=parseInt(d.current_viewers||0), chat=parseInt(d.chat_count||0), ads=parseInt(d.ads_count||0);
    const followers=parseInt(d.followers||0), videos=parseInt(d.total_videos||0);
    const upvotes=parseInt(d.upvotes||0), downvotes=parseInt(d.downvotes||0);
    const age_days = (d.age_days != null && d.age_days >=0) ? d.age_days : null;

    tr.innerHTML = `
        <td data-v="${d.created_at}">${d.when_pretty}</td>
        <td data-v="${(d.title||'').toLowerCase()}">${d.title||''}</td>
        <td data-v="${(d.username||'').toLowerCase()}">${d.username||''}</td>
        <td data-v="${String(age_days || 99999).padStart(5,'0')}">${age_days != null ? age_days : '—'}</td>
        <td data-v="${d.duration || ''}">${d.duration || '00:00:00'}</td>
        <td data-v="${followers}">${followers.toLocaleString()}</td>
        <td data-v="${videos}">${videos.toLocaleString()}</td>
        <td data-v="${viewers}">${viewers.toLocaleString()}</td>
        <td data-v="${chat}">${chat.toLocaleString()}</td>
        <td data-v="${ads}">${ads.toLocaleString()}</td>
        <td data-v="${upvotes}">${upvotes.toLocaleString()}</td>
        <td data-v="${downvotes}">${downvotes.toLocaleString()}</td>
        <td><a class="small" href="${d.url}" target="_blank">Open</a></td>
        <td><button type="button" class="small" onclick="showChatters('${d.url}')">View</button></td>
        <td><button type="button" class="small" onclick="toggleChart(this, ${d.id})">📈 View Chart</button></td>
    `;
}
function showToast(message, level = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast ${level}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => toast.classList.add('show'), 10);
    setTimeout(() => {
        toast.classList.remove('show');
        toast.addEventListener('transitionend', () => toast.remove());
    }, 5000);
}
async function showChatters(url){
    const modal = document.getElementById('modal-backdrop');
    const box = document.getElementById('modal-content');
    box.innerHTML='<p>Loading chatters…</p>';
    modal.style.display='flex';
    try{
        const res = await fetch(`/chatters?url=${encodeURIComponent(url)}`);
        if(!res.ok) throw new Error('HTTP '+res.status);
        box.innerHTML = await res.text();
    } catch(e) {
        box.innerHTML = '<p style="color:#ff6b6b">Failed to load chatters: '+e.message+'</p>';
    }
}
function closeModal(){ document.getElementById('modal-backdrop').style.display='none'; }
async function toggleChart(btn, suspectId, force_refresh = false) {
    const chartRow = document.getElementById(`chart-row-${suspectId}`);
    if (!chartRow) return;

    const isVisible = chartRow.classList.contains('visible');
    const show = !isVisible || force_refresh;

    if (show) {
        if (!isVisible) chartRow.classList.add('visible');
        setTimeout(async () => {
            if(chartInstances[suspectId]) chartInstances[suspectId].destroy();
            document.getElementById(`chart-container-${suspectId}`).innerHTML = '<p class="small">Loading chart data...</p><canvas id="chart-' + suspectId + '"></canvas>';
            const res = await fetch(`/tracking_data/${suspectId}`);
            const data = await res.json();
            if (data.length === 0) {
                document.getElementById(`chart-container-${suspectId}`).innerHTML = '<p class="small">No tracking data available yet. Please wait for the next interval.</p>';
                return;
            }
            const labels = data.map(d => new Date(d.ts).toLocaleTimeString());
            const viewersData = data.map(d => d.viewers);
            const upvotesData = data.map(d => d.upvotes);
            const downvotesData = data.map(d => d.downvotes);
            const adsData = data.map(d => d.ads_count);
            const ctx = document.getElementById(`chart-${suspectId}`).getContext('2d');
            chartInstances[suspectId] = new Chart(ctx, {
                type: 'line',
                data: { labels: labels, datasets: [
                    { label: 'Viewers', data: viewersData, borderColor: '#3b82f6', tension: 0.1, yAxisID: 'y' },
                    { label: 'Upvotes', data: upvotesData, borderColor: '#22c55e', tension: 0.1, yAxisID: 'y1' },
                    { label: 'Downvotes', data: downvotesData, borderColor: '#ef4444', tension: 0.1, yAxisID: 'y1' },
                    { label: 'Ads', data: adsData, borderColor: '#f97316', tension: 0.1, yAxisID: 'y1' }
                ]},
                options: { 
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: { 
                        y: { type: 'linear', display: true, position: 'left', beginAtZero: true, ticks: { maxTicksLimit: 11 } },
                        y1: { type: 'linear', display: true, position: 'right', beginAtZero: true, ticks: { stepSize: 1 }, grid: { drawOnChartArea: false } }
                    } 
                }
            });
        }, 10);
    } else {
        chartRow.classList.remove('visible');
    }
}
window.addEventListener('DOMContentLoaded', ()=>{
    const scanBtn = document.getElementById('scanBtn');
    if (scanBtn) {
        scanBtn.addEventListener('click', async () => {
            const isScanning = scanBtn.dataset.scanning === 'true';
            const action = isScanning ? '/stop_scan' : '/scan';
            scanBtn.disabled = true;
            try {
                const options = { method: 'POST' };
                if (!isScanning) {
                    options.body = new FormData(document.getElementById('scanForm'));
                }
                const res = await fetch(action, options);

                const raw = await res.text();
                let data = null;
                try { data = JSON.parse(raw); } catch(e) {
                    console.error('Non-JSON response from', action, raw);
                    showToast('Server error: '+ (raw.slice(0,120) || res.status), 'error');
                    scanBtn.disabled = false; return;
                }

                if (res.ok) {
                    if (data.status === 'stopping') {
                        scanBtn.textContent = 'Stopping...';
                    } else if (data.status === 'started') {
                        scanBtn.dataset.scanning = 'true';
                        scanBtn.textContent = '🛑 Stop Scan';
                        showToast('Scan started.', 'success');
                    } else {
                         scanBtn.dataset.scanning = 'false';
                         scanBtn.textContent = '🔎 Start Scan';
                         showToast('Scan is not running.', 'info');
                    }
                } else { showToast('Failed: '+(data.error||res.status), 'error'); }
            } catch (e) {
                console.error("Scan toggle failed:", e);
                showToast('Failed to toggle scan (network/JS).', 'error');
            } finally {
                scanBtn.disabled = false;
            }
        });
    }

    const clearBtn = document.getElementById('clearBtn');
    if (clearBtn) {
      clearBtn.addEventListener('click', async () => {
        if (!confirm('This will permanently delete all scanned data. Continue?')) return;
        clearBtn.disabled = true;
        try {
          const res = await fetch('/clear', { method: 'POST' });
          const raw = await res.text();
          let data = null; try { data = JSON.parse(raw); } catch(e) {}
          if (res.ok && data && data.ok) {
            showToast('Database cleared.', 'success');
            setTimeout(()=>location.reload(), 500);
          } else {
            showToast(`Clear failed: ${(data && data.error) || res.status}`, 'error');
          }
        } catch(e) {
          showToast('Clear failed (network).', 'error');
        } finally {
          clearBtn.disabled = false;
        }
      });
    }

    const es = new EventSource('/events');
    es.onmessage = (ev) => {
        try {
            const msg = JSON.parse(ev.data);
            if (msg.type === 'suspect_add'){
                addRowToCategoryTable(msg.data);
                showToast(`New suspect found: ${msg.data.title.substring(0, 30)}...`, 'success');
            } else if (msg.type === 'suspect_update'){
                updateRowInCategoryTable(msg.data);
                showToast(`Updated suspect: ${msg.data.title.substring(0, 30)}...`, 'info');
            } else if (msg.type === 'toast') {
                showToast(msg.data.message, msg.data.level);
            }
        } catch(e){}
    };
});
</script>
</head>
<body>
<div class="container">
    <div id="toast-container"></div>
    <h1>Rumble Suspected Botting Scanner</h1>
    <form id="scanForm" class="filters">
        <div class="grid">
            <div><label>Total views</label><div class="row"><input type="number" name="views_min" value="{{ filters.views_min }}"><input type="number" name="views_max" value="{{ filters.views_max }}"></div></div>
            <div><label>Live viewers</label><div class="row"><input type="number" name="viewers_min" value="{{ filters.viewers_min }}"><input type="number" name="viewers_max" value="{{ filters.viewers_max }}"></div></div>
            <div><label>Total ads</label><div class="row"><input type="number" name="ads_min" value="{{ filters.ads_min }}"><input type="number" name="ads_max" value="{{ filters.ads_max }}"></div></div>
            <div><label>Chat messages</label><div class="row"><input type="number" name="chat_min" value="{{ filters.chat_min }}"><input type="number" name="chat_max" value="{{ filters.chat_max }}"></div></div>
            <div><label>Account age (days)</label><div class="row"><input type="number" name="age_min" value="{{ filters.age_min }}"><input type="number" name="age_max" value="{{ filters.age_max }}"></div></div>
            <div><label>Category workers</label><div class="row"><input type="number" name="cat_workers" value="{{ filters.cat_workers }}"><input type="number" value="64" disabled class="small" title="max"/></div></div>
            <div><label>Page workers (Playwright)</label><div class="row"><input type="number" name="page_workers" value="{{ filters.page_workers }}"><input type="number" value="64" disabled class="small" title="max"/></div></div>
        </div>
    </form>
    <div class="actions">
        <span class="pill">Skip: Views > {{ "{:,}".format(views_skip) }}</span>
        <span class="pill">Browser: <strong>{{ 'ON' if browser_on else 'OFF' }}</strong></span>
        <button id="scanBtn" type="button" data-scanning="{{ 'true' if scan_is_running else 'false' }}">
            {% if scan_is_running %}🛑 Stop Scan{% else %}🔎 Start Scan{% endif %}
        </button>
        <a href="{{ url_for('export_csv') }}" class="button">⬇️ Drop to CSV</a>
        <button id="clearBtn" type="button" class="danger">🗑️ Clear Data</button>
    </div>
    {% macro table_head(table_id) -%}
    <div class="table-wrap">
    <table id="{{ table_id }}" data-dir="asc">
        <thead>
            <tr>
                <th onclick="sortTableByCol('{{ table_id }}', 0, false)">When</th>
                <th onclick="sortTableByCol('{{ table_id }}', 1, false)">Title</th>
                <th onclick="sortTableByCol('{{ table_id }}', 2, false)">Username</th>
                <th onclick="sortTableByCol('{{ table_id }}', 3)">Age (d)</th>
                <th onclick="sortTableByCol('{{ table_id }}', 4, false)">Duration</th>
                <th onclick="sortTableByCol('{{ table_id }}', 5)">Followers</th>
                <th onclick="sortTableByCol('{{ table_id }}', 6)">Videos</th>
                <th onclick="sortTableByCol('{{ table_id }}', 7)">Viewers</th>
                <th onclick="sortTableByCol('{{ table_id }}', 8)">Chat</th>
                <th onclick="sortTableByCol('{{ table_id }}', 9)">Ads</th>
                <th onclick="sortTableByCol('{{ table_id }}', 10)">Upvotes</th>
                <th onclick="sortTableByCol('{{ table_id }}', 11)">Downvotes</th>
                <th>Link</th><th>Chatters</th><th>Tracking</th>
            </tr>
        </thead>
        <tbody>
    {%- endmacro %}
    {% macro table_rows(rows) -%}
        {% if rows and rows|length > 0 %}{% for r in rows %}
            <tr class="bad" data-id="{{ r.id }}">
                <td data-v="{{ r.created_at }}">{{ r.when_pretty }}</td>
                <td data-v="{{ r.title|lower }}">{{ r.title }}</td>
                <td data-v="{{ r.username|lower }}">{{ r.username }}</td>
                <td data-v="{{ '%05d' % (r.age_days if (r.age_days is not none and r.age_days >= 0) else 99999) }}">{{ r.age_days if (r.age_days is not none and r.age_days >= 0) else '—' }}</td>
                <td data-v="{{ r.duration }}">{{ r.duration }}</td>
                <td data-v="{{ r.followers }}">{{ "{:,}".format(r.followers) }}</td>
                <td data-v="{{ r.total_videos }}">{{ "{:,}".format(r.total_videos) }}</td>
                <td data-v="{{ r.current_viewers }}">{{ "{:,}".format(r.current_viewers) }}</td>
                <td data-v="{{ r.chat_count }}">{{ "{:,}".format(r.chat_count) }}</td>
                <td data-v="{{ r.ads_count }}">{{ "{:,}".format(r.ads_count) }}</td>
                <td data-v="{{ r.upvotes }}">{{ "{:,}".format(r.upvotes) }}</td>
                <td data-v="{{ r.downvotes }}">{{ "{:,}".format(r.downvotes) }}</td>
                <td><a class="small" href="{{ r.url }}" target="_blank">Open</a></td>
                <td><button type="button" class="small" onclick="showChatters('{{ r.url }}')">View</button></td>
                <td><button type="button" class="small" onclick="toggleChart(this, {{ r.id }})">📈 View Chart</button></td>
            </tr>
            <tr class="chart-row" id="chart-row-{{ r.id }}"><td colspan="15"><div class="chart-container" id="chart-container-{{ r.id }}"><canvas id="chart-{{ r.id }}"></canvas></div></td></tr>
        {% endfor %}{% else %}<tr class="placeholder"><td colspan="15" class="small">No suspects in this category yet.</td></tr>{% endif %}
    {%- endmacro %}
    {% macro table_tail() -%}</tbody></table></div>{%- endmacro %}
    {% for cat in categories %}
    {% set _id = 'cat-' + cat.name|lower|replace(' ','-')|replace('&','and')|replace('/','-') %}
    <section id="{{ _id }}" class="accordion acc open">
        <div class="acc-head {% if cat.rows|length > 0 %}has-suspects{% endif %}" onclick="toggleAcc('{{ _id }}')">
            <div class="acc-title"><svg class="chev" width="16" height="16" viewBox="0 0 24 24"><path fill="currentColor" d="M9 6l6 6-6 6"/></svg><h2>{{ cat.name }}</h2><span class="badge">{{ cat.rows|length }}</span></div>
            <div class="small">Sortable columns • Live Updates & Charting on Rescan</div>
        </div>
        <div class="acc-body">
            {{ table_head(_id ~ '-table') }}
            {{ table_rows(cat.rows) }}
            {{ table_tail() }}
        </div>
    </section>
    {% endfor %}
    <div id="modal-backdrop" onclick="if(event.target===this)closeModal()"><div id="modal" role="dialog"><header><strong>Chatters</strong><button class="close" onclick="closeModal()">Close</button></header><div class="content" id="modal-content"></div></div></div>
</div>
</body>
</html>
"""

CHATTERS_FRAGMENT_HTML = """
{% if users %}
    <p class="small"><span class="badge">{{ users|length }}</span> unique chatters found.</p>
    <div class="table-wrap">
    <table style="width:100%">
        <thead><tr><th>#</th><th>Username</th><th>Joined</th><th>Profile</th></tr></thead>
        <tbody>
        {% for u in users %}
            <tr>
                <td>{{ loop.index }}</td>
                <td>{{ u.username }}</td>
                <td>{{ u.joined or '—' }}</td>
                <td><a href="{{ u.profile_url }}/about" target="_blank">Open</a></td>
            </tr>
        {% endfor %}
        </tbody>
    </table>
    </div>
{% else %}
    <p class="small">No chatters detected.</p>
{% endif %}
"""


# -------- Helpers to prep template data --------
def _row_to_dict(r):
    created_at_dt = datetime.fromisoformat(r[9]) if r[9] else None
    when_pretty = created_at_dt.strftime('%Y-%m-%d %H:%M:%S') if created_at_dt else 'N/A'

    return {
        "id": r[0], "category": r[1], "title": r[2], "username": r[3],
        "joined": r[4], "current_viewers": r[5], "chat_count": r[6], "url": r[7],
        "age_days": (None if r[8] == -1 else r[8]), "created_at": r[9], "when_pretty": when_pretty,
        "followers": r[10], "total_videos": r[11], "upvotes": r[12],
        "downvotes": r[13], "duration": r[14], "ads_count": r[15]
    }


def _group_by_category(rows):
    mapping = {name: [] for name in WHITELIST_TITLES}
    for rr in rows:
        d = _row_to_dict(rr)
        cat = d["category"]
        if cat in mapping:
            mapping[cat].append(d)
    cats = [{"name": name, "rows": mapping[name]} for name in WHITELIST_TITLES]
    return cats


# -------------- Flask Routes ------------------
@app.route("/", methods=["GET"])
def index():
    suspects = get_recent_suspects()
    categories = _group_by_category(suspects)
    scan_is_running = APP_STATE.scan_thread is not None and APP_STATE.scan_thread.is_alive()
    return render_template_string(
        INDEX_HTML, categories=categories, filters=FILTER_DEFAULTS,
        views_skip=VIEWS_SKIP_MAX, browser_on=USE_BROWSER, scan_is_running=scan_is_running
    )


@app.route("/scan", methods=["POST"])
def scan():
    try:
        if APP_STATE.scan_thread is None or not APP_STATE.scan_thread.is_alive():
            filters = build_filters_from_form(request.form)
            print(f"Starting continuous scan with filters: {filters}")
            APP_STATE.stop_event.clear()
            APP_STATE.scan_thread = threading.Thread(target=main_scanner_loop, args=(filters,), daemon=True)
            APP_STATE.scan_thread.start()
            return jsonify({"status": "started"}), 202
        return jsonify({"status": "already running"}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/stop_scan", methods=["POST"])
def stop_scan():
    try:
        if APP_STATE.scan_thread and APP_STATE.scan_thread.is_alive():
            print("Received stop signal. Scan will halt after this cycle.")
            APP_STATE.stop_event.set()
            return jsonify({"status": "stopping"}), 202
        return jsonify({"status": "not running"}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"status": "error", "error": str(e)}), 500


@app.route("/events")
def events():
    q = BROKER.subscribe()

    def gen():
        try:
            yield f"data: {json.dumps({'type': 'hello'})}\n\n"
            while True:
                msg = q.get()
                yield f"data: {json.dumps(msg)}\n\n"
        finally:
            BROKER.unsubscribe(q)

    return Response(
        gen(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        },
    )


@app.route("/chatters")
def chatters():
    stream_url = request.args.get("url", "").strip()
    if not stream_url:
        abort(400, "Missing url parameter")
    if stream_url.startswith("/"):
        stream_url = urljoin(BASE_URL, stream_url)
    users = []  # implement if needed
    return render_template_string(CHATTERS_FRAGMENT_HTML, users=users)


@app.route("/tracking_data/<int:suspect_id>")
def get_tracking_data(suspect_id):
    with DB_LOCK:
        with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT ts, viewers, upvotes, downvotes, ads_count FROM tracking_data WHERE suspect_id = ? ORDER BY ts ASC",
                (suspect_id,),
            )
            rows = cur.fetchall()
            data = [{"ts": r[0], "viewers": r[1], "upvotes": r[2], "downvotes": r[3], "ads_count": r[4]} for r in rows]
            return jsonify(data)


@app.route("/export_csv")
def export_csv():
    with DB_LOCK:
        output = io.StringIO()
        writer = csv.writer(output)

        with closing(sqlite3.connect(DB_PATH, check_same_thread=False)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM suspects ORDER BY id DESC")
            writer.writerow([description[0] for description in cursor.description])
            for row in cursor.fetchall():
                writer.writerow(row)

        csv_text = output.getvalue()
    return Response(
        csv_text,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment;filename=suspects.csv"},
    )


@app.route("/clear", methods=["POST"])
def clear_data():
    try:
        clear_all_data()
        print("All data cleared")
        return jsonify({"ok": True}), 200
    except Exception as e:
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


if __name__ == "__main__":
    init_db()
    app.run(debug=True, threaded=True, use_reloader=False)