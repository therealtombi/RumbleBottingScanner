# app.py - Rumble Bot-Suspect Scanner
# - Multithreaded + SSE live updates
# - Per-category accordion tables (sortable)
# - Dark wide UI
# - Filters (ranges) applied DURING scan:
#     * total views, viewers, ads, chat, account age (days)
# - Counts ads, chat; joined date -> account age (days)
# - Skips streams with total views > 20k (hard cap; adjust if desired)
# - Clear-data VACUUM fix

import json
import re
import time
import sqlite3
import threading
from contextlib import closing
from datetime import datetime, timezone
from urllib.parse import urljoin

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

from flask import Flask, Response, render_template_string, request, redirect, url_for, abort

# ---------------- Config ----------------
BASE_URL = "https://rumble.com"
BROWSE_URL = f"{BASE_URL}/browse"
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; TombiBot/1.0; +https://example.com)"}
REQUEST_TIMEOUT = 20
POLITE_DELAY_SEC = 0.4
VIEWS_SKIP_MAX = 20000  # hard skip
USE_BROWSER = True
DB_PATH = "suspects.sqlite"

# Default filter ranges
FILTER_DEFAULTS = {
    "views_min": 0, "views_max": 1_000_000,
    "viewers_min": 0, "viewers_max": 50_000,
    "ads_min": 0, "ads_max": 100,
    "chat_min": 0, "chat_max": 5_000,
    "age_min": 0, "age_max": 100,  # days
}

# Concurrency
MAX_FETCH_WORKERS = 8  # requests-based work
MAX_STREAM_WORKERS = 4  # playwright-heavy per-stream

# Whitelist (titles as shown on site; case-insensitive match)
WHITELIST_TITLES = [
    "News", "Politics", "24/7", "Republican Politics", "Entertainment",
    "Podcasts", "Music", "Conspiracies", "Cooking", "Gaming", "Vlogs",
    "Finance & Crypto",
]
WHITELIST_SLUG_MAP = {
    "News": "news",
    "Politics": "politics",
    "Republican Politics": "republican-politics",
    "Entertainment": "entertainment",
    "Podcasts": "podcasts",
    "Music": "music",
    "Conspiracies": "conspiracies",
    "Cooking": "cooking",
    "Gaming": "gaming",
    "Vlogs": "vlogs",
    "Finance & Crypto": "finance-and-crypto",
}


# ------------- Shared HTTP session (faster + retries) ---------------
def build_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    retries = Retry(
        total=3, backoff_factor=0.4,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"])
    )
    adapter = HTTPAdapter(max_retries=retries, pool_connections=50, pool_maxsize=50)
    s.mount("http://", adapter)
    s.mount("https://", adapter)
    return s


HTTP = build_session()


# ------------- SSE Event Broker ---------------
class EventBroker:
    def __init__(self):
        import queue
        self._subs: set[queue.Queue] = set()
        self._lock = threading.Lock()

    def subscribe(self):
        import queue
        q = queue.Queue()
        with self._lock:
            self._subs.add(q)
        return q

    def unsubscribe(self, q):
        with self._lock:
            self._subs.discard(q)

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
    num = float(num_str) if '.' in num_str else int(num_str)
    unit = (m.group(2) or "").lower()
    if unit == "k":
        num *= 1000
    elif unit == "m":
        num *= 1_000_000
    return int(num)


# Joined date parsing → age (days)
def parse_joined_to_date(joined_str: str):
    """Try multiple formats like 'Sep 12, 2025', 'September 12, 2025', 'Sep 2025'."""
    if not joined_str: return None
    cand = joined_str.strip()
    fmts = ["%b %d, %Y", "%B %d, %Y", "%b %Y", "%B %Y"]
    for f in fmts:
        try:
            dt = datetime.strptime(cand, f).replace(tzinfo=timezone.utc)
            return dt
        except Exception:
            continue
    return None


def compute_age_days(joined_str: str | None) -> int | None:
    if not joined_str: return None
    dt = parse_joined_to_date(joined_str)
    if not dt: return None
    today = datetime.now(timezone.utc)
    days = (today - dt).days
    return max(0, days)


# ----------- Playwright helpers -----------
def browser_count_chat_messages(stream_url: str) -> int:
    if not USE_BROWSER:
        return count_chat_messages_static(stream_url)
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return count_chat_messages_static(stream_url)

    full_url = stream_url if stream_url.startswith("http") else urljoin(BASE_URL, stream_url)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = context.new_page()
            page.goto(full_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(6000)
            selectors = [
                ".chat-history-list .chat-history--item",
                ".chat-history-list .chat-message",
                "#chat-history-list .chat-history--row",
                "[class*='chat-history'] li",
                "[class*='chat-history'] .chat-item",
            ]
            for sel in selectors:
                try:
                    count = page.eval_on_selector_all(sel, "els => els.length")
                    if count and int(count) > 0:
                        context.close();
                        browser.close()
                        return int(count)
                except Exception:
                    pass
            context.close();
            browser.close()
            return 0
    except Exception:
        return 0


def count_chat_messages_static(stream_url: str) -> int:
    try:
        soup = get_soup(stream_url if stream_url.startswith("http") else urljoin(BASE_URL, stream_url))
    except Exception:
        return 0
    candidates = [
        ".chat-history-list .chat-history--item",
        ".chat-history-list .chat-message",
        "#chat-history-list .chat-history--row",
        "[class*='chat-history'] li",
        "[class*='chat-history'] .chat-item",
    ]
    for sel in candidates:
        items = soup.select(sel)
        if items: return len(items)
    return 0


def browser_count_ads(stream_url: str) -> int:
    if not USE_BROWSER:
        return count_ads_static(stream_url)
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return count_ads_static(stream_url)

    full_url = stream_url if stream_url.startswith("http") else urljoin(BASE_URL, stream_url)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = context.new_page()
            page.goto(full_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(4500)

            total = 0
            try:
                c = page.eval_on_selector_all("div.undecorate-links", "els => els.length")
                total += int(c or 0)
            except Exception:
                pass
            for fr in page.frames:
                try:
                    if fr == page.main_frame: continue
                    c = fr.eval_on_selector_all("div.undecorate-links", "els => els.length")
                    total += int(c or 0)
                except Exception:
                    continue
            context.close();
            browser.close()
            return total
    except Exception:
        return 0


def count_ads_static(stream_url: str) -> int:
    try:
        soup = get_soup(stream_url if stream_url.startswith("http") else urljoin(BASE_URL, stream_url))
    except Exception:
        return 0
    return len(soup.select("div.undecorate-links"))


def browser_collect_chat_usernames(stream_url: str, max_users: int = 300) -> list[dict]:
    users = {}
    if not USE_BROWSER: return []
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return []
    full_url = stream_url if stream_url.startswith("http") else urljoin(BASE_URL, stream_url)
    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(headless=True)
            context = browser.new_context(user_agent=HEADERS["User-Agent"])
            page = context.new_page()
            page.goto(full_url, wait_until="domcontentloaded", timeout=30000)
            page.wait_for_timeout(7000)
            rows = page.query_selector_all("#chat-history-list .chat-history--row, #chat-history-list li")
            for r in rows:
                try:
                    uname = r.get_attribute("data-username")
                    if not uname:
                        btn = r.query_selector(".chat-history--username")
                        if btn:
                            uname = (btn.inner_text() or "").strip()
                    if not uname:
                        a = r.query_selector("a[href^='/user/']")
                        if a:
                            href = a.get_attribute("href") or ""
                            if href.startswith("/user/"):
                                uname = href.split("/user/")[1].split("/")[0].split("?")[0]
                    if uname and uname not in users:
                        profile_url = urljoin(BASE_URL, f"/user/{uname}")
                        users[uname] = {"username": uname, "profile_url": profile_url}
                        if len(users) >= max_users:
                            break
                except Exception:
                    continue
            context.close();
            browser.close()
    except Exception:
        return []
    return list(users.values())


# ------------- About scraping -------------
def fetch_user_joined_date(username: str) -> str | None:
    if not username: return None
    url = urljoin(BASE_URL, f"/user/{username}/about")
    try:
        soup = get_soup(url)
    except Exception:
        return None
    for p in soup.select(".channel-about-sidebar--inner p"):
        t = txt(p)
        if "joined" in t.lower():
            m = re.search(r"Joined\s+(.+)", t, re.IGNORECASE)
            return (m.group(1).strip() if m else t.strip())
    return None


def fetch_channel_details(username: str) -> dict:
    """
    Fetches followers, joined date, total views, and video count from a user's profile.
    Intelligently handles both /c/ (channel) and /user/ style profiles.
    """
    if not username:
        return {"followers": 0, "joined_date": None, "total_views": 0, "total_videos": 0, "description": ""}

    base_profile_url = None
    profile_type = None

    try:
        c_url = urljoin(BASE_URL, f"/c/{username}")
        r = HTTP.head(c_url, timeout=10, allow_redirects=True)
        if r.status_code == 200:
            base_profile_url = r.url
            profile_type = 'c'
    except Exception:
        pass

    if not base_profile_url:
        try:
            user_url = urljoin(BASE_URL, f"/user/{username}")
            r = HTTP.head(user_url, timeout=10, allow_redirects=True)
            if r.status_code == 200:
                base_profile_url = r.url
                profile_type = 'user'
        except Exception:
            pass

    if not base_profile_url:
        print(f"⚠️ Could not find a valid profile for {username} at /c/ or /user/")
        return {"followers": 0, "joined_date": None, "total_views": 0, "total_videos": 0, "description": ""}

    total_followers, joined_date, total_views, total_videos, description = 0, None, 0, 0, ""
    try:
        soup = get_soup(base_profile_url)

        if profile_type == 'c':
            follower_el = soup.select_one(".channel-header--followers")
            if follower_el: total_followers = parse_viewers_text(txt(follower_el))
        else:
            follower_el = soup.select_one("span.text-fjord span")
            if follower_el: total_followers = parse_viewers_text(txt(follower_el))

            try:
                channels_soup = get_soup(f"{base_profile_url}/channels")
                for follower_div in channels_soup.select("div.creator-card__followers"):
                    total_followers += parse_viewers_text(txt(follower_div))
            except Exception:
                pass

        about_soup = get_soup(f"{base_profile_url}/about")
        sidebar = about_soup.select_one(".channel-about-sidebar--inner")
        if sidebar:
            for p in sidebar.select("p"):
                p_text = txt(p)
                if "Joined" in p_text:
                    m = re.search(r"Joined\s+(.+)", p_text, re.IGNORECASE)
                    if m: joined_date = m.group(1).strip()
                elif "views" in p_text:
                    total_views = parse_viewers_text(p_text)
                elif "videos" in p_text:
                    total_videos = parse_viewers_text(p_text)

        desc_el = about_soup.select_one(".channel-about-main--description")
        description = txt(desc_el)

    except Exception as e:
        print(f"⚠️ Error during detailed scrape for {username}: {e}")

    return {
        "followers": total_followers, "joined_date": joined_date,
        "total_views": total_views, "total_videos": total_videos,
        "description": description.strip()
    }


# ------------- DB helpers --------------
def ensure_columns(conn, table: str, required: dict[str, str]):
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    existing = {row[1] for row in cur.fetchall()}
    for col, coltype in required.items():
        if col not in existing:
            print(f"⚠️ Adding missing column {col} to {table}")
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {coltype}")
    conn.commit()


def backfill_nulls_to_zero():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("UPDATE suspects SET views=0 WHERE views IS NULL")
        conn.execute("UPDATE suspects SET chat_count=0 WHERE chat_count IS NULL")
        conn.execute("UPDATE suspects SET ads_count=0 WHERE ads_count IS NULL")
        conn.execute("UPDATE suspects SET current_viewers=0 WHERE current_viewers IS NULL")
        conn.commit()


def init_db():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS scans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL
        );
        """)
        conn.execute("""
        CREATE TABLE IF NOT EXISTS suspects (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scan_id INTEGER,
            category TEXT,
            title TEXT,
            channel TEXT,
            views INTEGER,
            chat_count INTEGER,
            ads_count INTEGER,
            current_viewers INTEGER,
            channel_joined TEXT,
            username TEXT,
            url TEXT,
            video_id TEXT,
            account_age_days INTEGER,
            followers INTEGER,
            total_videos INTEGER,
            created_at TEXT,
            FOREIGN KEY(scan_id) REFERENCES scans(id)
        );
        """)
        required_cols = {
            "username": "TEXT", "ads_count": "INTEGER", "current_viewers": "INTEGER",
            "channel_joined": "TEXT", "account_age_days": "INTEGER", "followers": "INTEGER",
            "total_videos": "INTEGER"
        }
        ensure_columns(conn, "suspects", required_cols)
        conn.commit()
    backfill_nulls_to_zero()


def start_scan_row() -> int:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute("INSERT INTO scans (ts) VALUES (?)", (datetime.now(timezone.utc).isoformat(),))
        conn.commit()
        scan_id = cur.lastrowid
        print(f"🟢 Started scan id={scan_id}")
        return scan_id


def save_suspect(data: dict):
    created_at = datetime.now(timezone.utc).isoformat()
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("""
            INSERT INTO suspects (
                scan_id, category, title, channel, username, views, chat_count, 
                ads_count, current_viewers, channel_joined, url, video_id, 
                account_age_days, followers, total_videos, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data.get("scan_id"), data.get("category"), data.get("title"), data.get("channel"),
            data.get("username"), int(data.get("views", 0)), int(data.get("chat_count", 0)),
            int(data.get("ads_count", 0)), int(data.get("current_viewers", 0)), data.get("channel_joined", ""),
            data.get("url"), data.get("video_id"), data.get("age_days"),
            int(data.get("followers", 0)), int(data.get("total_videos", 0)), created_at
        ))
        conn.commit()

    data["when"] = created_at
    BROKER.publish({"type": "suspect", "data": data})


def get_recent_suspects(limit=400):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute("""
        SELECT
            category, title, channel, username,
            COALESCE(channel_joined,'') AS channel_joined,
            COALESCE(views,0) AS views,
            COALESCE(current_viewers,0) AS current_viewers,
            COALESCE(chat_count,0) AS chat_count,
            COALESCE(ads_count,0) AS ads_count,
            url, video_id,
            COALESCE(account_age_days, -1) AS account_age_days,
            created_at,
            COALESCE(followers, 0) AS followers,
            COALESCE(total_videos, 0) AS total_videos
        FROM suspects
        ORDER BY created_at DESC
        LIMIT ?
        """, (limit,))
        return cur.fetchall()


def clear_all_data():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.execute("DELETE FROM suspects")
        conn.execute("DELETE FROM scans")
        conn.commit()
    with closing(sqlite3.connect(DB_PATH, isolation_level=None)) as conn:
        conn.execute("VACUUM")


# ----------- Scrapers ------------------
def scrape_categories_all():
    print(f"🔎 Fetching categories from {BROWSE_URL}")
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
    print(f"✅ Found {len(cats)} total categories")
    return cats


def filter_or_build_whitelisted_categories(all_categories: list[dict]) -> list[dict]:
    wanted = {t.casefold(): t for t in WHITELIST_TITLES}
    found = {}
    for c in all_categories:
        title_cf = c["title"].casefold()
        if title_cf in wanted: found[wanted[title_cf]] = c

    for title in WHITELIST_TITLES:
        if title not in found:
            if title == "24/7":
                url = urljoin(BASE_URL, "/live/24-7")
                found[title] = {"title": title, "url": url, "viewers": 0}
                print(f"↩️ Added special case for '{title}' → {url}")
                continue

            slug = WHITELIST_SLUG_MAP.get(title)
            if slug:
                url = urljoin(BASE_URL, f"/category/{slug}")
                found[title] = {"title": title, "url": url, "viewers": 0}
                print(f"↩️ Added fallback for '{title}' → {url}")

    print("🎯 Whitelist scan set:")
    for t in WHITELIST_TITLES:
        c = found.get(t)
        print(f"  • {t}  ({c['url']})" if c else f"  • {t}  (SKIPPED)")
    return list(found.values())


def scrape_live_streams_for_category(cat_title: str, cat_url: str):
    BROKER.publish({'type': 'toast', 'data': {'message': f"Scanning category: {cat_title}", 'level': 'info'}})
    print(f"➡️  Scanning LIVE streams in: {cat_title} ({cat_url})")
    soup = get_soup(cat_url)
    results = []
    for item in soup.select(".videostream.thumbnail__grid-item"):
        live_badge = item.select_one(".videostream__status.videostream__status--live")
        if not live_badge: continue

        link = item.select_one(".videostream__link")
        url = urljoin(BASE_URL, link.get("href")) if link and link.get("href") else None

        title_tag = item.select_one(".thumbnail__title")
        title = (title_tag.get("title") or txt(title_tag)) if title_tag else ""

        channel = txt(item.select_one(".channel__name"))
        username = ""
        ch_link = item.select_one("a[href^='/c/'], a[href^='/user/']")
        if ch_link:
            href = (ch_link.get("href") or "").split("?")[0]
            if href.startswith("/c/"):
                username = href.split("/c/")[1].strip("/")
            elif href.startswith("/user/"):
                username = href.split("/user/")[1].strip("/")

        total_views = 0
        views_tag = item.select_one(".videostream__data--item.videostream__views")
        if views_tag and views_tag.has_attr("data-views"):
            try:
                total_views = int(views_tag["data-views"])
            except Exception:
                total_views = parse_viewers_text(txt(views_tag))

        concurrent_viewers = parse_viewers_text(txt(item.select_one(".videostream__badge .videostream__number")))
        views = total_views if total_views > 0 else concurrent_viewers

        video_id = item.get("data-video-id") or ""

        results.append({
            "category": cat_title, "title": title, "url": url, "channel": channel,
            "username": username, "views": views, "current_viewers": concurrent_viewers, "video_id": video_id
        })
    print(f"📺 {cat_title}: {len(results)} live streams found")
    return results


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
    return f


def metrics_match_filters(metrics: dict, filters: dict) -> tuple[bool, str]:
    """
    Checks if a stream's metrics match the filters.
    Returns (True, "") if it matches, or (False, "reason") if it doesn't.
    """
    age_days = metrics.get("age_days")
    views = metrics.get("views", 0)
    viewers = metrics.get("current_viewers", 0)
    ads = metrics.get("ads_count", 0)
    chat = metrics.get("chat_count", 0)

    if not (filters["views_min"] <= views <= filters["views_max"]):
        return False, f"views {views} out of range"
    if not (filters["viewers_min"] <= viewers <= filters["viewers_max"]):
        return False, f"viewers {viewers} out of range"
    if not (filters["ads_min"] <= ads <= filters["ads_max"]):
        return False, f"ads {ads} out of range"
    if not (filters["chat_min"] <= chat <= filters["chat_max"]):
        return False, f"chat {chat} out of range"
    if age_days is not None:
        if not (filters["age_min"] <= age_days <= filters["age_max"]):
            return False, f"age {age_days}d out of range"

    return True, ""


# -------------- Threaded processing --------------
joined_cache: dict[str, dict] = {}
joined_lock = threading.Lock()


def process_stream_item(scan_id: int, item: dict, filters: dict):
    try:
        url = item.get("url")
        if not url: return False

        if (item.get("views", 0) or 0) > VIEWS_SKIP_MAX:
            print(f"⏭️ Skipping (views>{VIEWS_SKIP_MAX}): {item.get('title', '?')}")
            return False

        time.sleep(POLITE_DELAY_SEC)

        ch_user = item.get("username", "")
        details = {}
        if ch_user:
            with joined_lock:
                cached = joined_cache.get(ch_user)
            if cached is None:
                details = fetch_channel_details(ch_user)
                with joined_lock:
                    joined_cache[ch_user] = details
            else:
                details = cached

        metrics = {**item, **details}
        metrics["age_days"] = compute_age_days(details.get("joined_date"))
        metrics["chat_count"] = max(0, browser_count_chat_messages(url))
        metrics["ads_count"] = max(0, browser_count_ads(url))

        matches, reason = metrics_match_filters(metrics, filters)

        if matches:
            print(
                f"✅ MATCH: {metrics.get('title', '?')}: V={metrics.get('views')}, C={metrics.get('current_viewers')}, "
                f"F={metrics.get('followers')}, Vid={metrics.get('total_videos')}, "
                f"Chat={metrics.get('chat_count')}, Age={metrics.get('age_days') or '—'}d"
            )
            metrics["scan_id"] = scan_id
            save_suspect(metrics)
            print(f"🚨 Saved & published suspect: {metrics.get('title', '?')}")
            return True
        else:
            title = metrics.get('title', '?')[:40]
            print(f"🚫 REJECTED: {title}... ({reason})")
            return False

    except Exception as e:
        print(f"❌ process_stream_item error for {item.get('title', '?')}: {e}")
        return False


def run_scan(scan_id: int, filters: dict):
    try:
        all_categories = scrape_categories_all()
        categories = filter_or_build_whitelisted_categories(all_categories)
        live_items = []
        with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
            futures = {ex.submit(scrape_live_streams_for_category, c["title"], c["url"]): c for c in categories}
            for fut in as_completed(futures):
                try:
                    live_items.extend(fut.result() or [])
                except Exception as e:
                    c = futures[fut]
                    print(f"❌ Category scrape failed for {c.get('title', '?')}: {e}")

        print(f"🧮 Total live items gathered: {len(live_items)}")

        global joined_cache
        joined_cache = {}
        saved = 0
        with ThreadPoolExecutor(max_workers=MAX_STREAM_WORKERS) as ex:
            futures = [ex.submit(process_stream_item, scan_id, item, filters) for item in live_items]
            for fut in as_completed(futures):
                if fut.result(): saved += 1

        print(f"🏁 Scan finished. Suspects saved this run: {saved}")
        BROKER.publish(
            {'type': 'toast', 'data': {'message': f"Scan finished. Found {saved} new suspects.", 'level': 'success'}})
    except Exception as e:
        print(f"❌ run_scan fatal error: {e}")
        BROKER.publish({'type': 'toast', 'data': {'message': 'An error occurred during the scan.', 'level': 'error'}})


# -------------- Flask ------------------
app = Flask(__name__)
init_db()

# ---------- Template ----------
INDEX_HTML = """
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Rumble Suspected Botting Scanner</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
:root{ color-scheme: dark; }
*{box-sizing:border-box}
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif;margin:0;background:#0f1115;color:#e6e6e6;}
.container{max-width:1600px;margin:0 auto;padding:1.2rem 1.2rem 2rem}
h1{font-weight:700;letter-spacing:.2px;margin:.2rem 0 .8rem}
button{padding:.5rem .9rem;border:1px solid #2a2f3a;border-radius:.6rem;background:#1a1f29;color:#e6e6e6;cursor:pointer;transition:.15s ease;}
button:hover{background:#222838}
button:disabled{cursor:not-allowed;background:#11141a;color:#555;}
button.small{font-size:.85rem;padding:.35rem .6rem}
a{color:#6ea8fe;text-decoration:none}
a:hover{text-decoration:underline}
.small{font-size:.95rem;color:#b9c2d0}
.pill{display:inline-block;padding:.15rem .5rem;border:1px solid #2a2f3a;border-radius:999px;background:#131722;color:#cbd5e1;white-space:nowrap}
.danger{background:#2a1111;border:1px solid #552222}
.filters{background:#0f1415;border:1px solid #1f2430;border-radius:.8rem;padding:1rem;margin:.6rem 0}
.filters .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:.8rem}
.filters label{display:block;font-size:.85rem;color:#9fb0c5;margin-bottom:.25rem}
.filters .row{display:grid;grid-template-columns:1fr 1fr;gap:.4rem}
.filters input[type=number]{width:100%;padding:.45rem .5rem;border:1px solid #2a2f3a;border-radius:.5rem;background:#0d1018;color:#e6e6e6}
.actions{display:flex;gap:.6rem;flex-wrap:wrap;align-items:center;margin:.8rem 0 1.2rem}
.accordion{border:1px solid #1f2430;border-radius:.8rem;overflow:hidden;margin-bottom:1rem;background:#0f1415}
.acc-head{display:flex;align-items:center;justify-content:space-between;gap:.8rem;padding:.8rem 1rem;background:#131722;border-bottom:1px solid #1f2430;cursor:pointer;user-select:none}
.acc-title{display:flex;align-items:center;gap:.6rem}
.acc-title h2{font-size:1rem;margin:0}
.chev{transition:transform .2s ease}
.acc.open .chev{transform:rotate(90deg)}
.acc-body{max-height:0;overflow:hidden;transition:max-height .25s ease}
.acc.open .acc-body{max-height:1200px}
.table-wrap{overflow:auto}
table{border-collapse:separate;border-spacing:0;min-width:1200px;width:100%;background:#0f1115}
th,td{border-bottom:1px solid #1f2430;padding:.55rem .6rem;text-align:left;vertical-align:top}
th{background:#151a26;position:sticky;top:0;z-index:1;cursor:pointer;user-select:none}
tr:hover td{background:#0f141d}
.bad td{background:#161b27}
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
function pad(n, width){ const s=String(n); return s.length>=width?s:"0".repeat(width-s.length)+s; }
function catId(cat){ return 'cat-' + (cat||'').toLowerCase().replace(/[^a-z0-9]+/g,'-').replace(/(^-|-$)/g,''); }
function sortTableByCol(tableId, col, isNum=true){
    const table = document.getElementById(tableId); if(!table) return;
    const tbody = table.tBodies[0];
    const rows = Array.from(tbody.rows);
    const dir = table.dataset.dir === 'asc' ? 'desc' : 'asc';

    rows.sort((a,b)=>{
        const x = a.cells[col].dataset.v || a.cells[col].innerText;
        const y = b.cells[col].dataset.v || b.cells[col].innerText;
        const xv = isNum ? parseFloat(x) : x.toLowerCase();
        const yv = isNum ? parseFloat(y) : y.toLowerCase();
        if (dir === 'asc') return xv > yv ? 1 : -1;
        return xv < yv ? 1 : -1;
    });
    rows.forEach(r => tbody.appendChild(r));
    table.dataset.dir = dir;
}
function toggleAcc(id){ document.getElementById(id)?.classList.toggle('open'); }
function addRowToCategoryTable(d){
    const id = catId(d.category);
    const tbody = document.querySelector(`#${id} .table-wrap tbody`);
    if (!tbody) return;
    tbody.querySelector('.placeholder')?.remove();
    const tr = document.createElement('tr'); tr.className = 'bad';
    const td = (text, dv) => {
        const cell = document.createElement('td');
        if (dv !== undefined) cell.dataset.v = dv;
        if (text instanceof HTMLElement) cell.appendChild(text); else cell.textContent = text;
        return cell;
    };
    const when = d.when || new Date().toISOString();
    const viewers=parseInt(d.current_viewers||0), chat=parseInt(d.chat_count||0);
    const followers=parseInt(d.followers||0), videos=parseInt(d.total_videos||0);

    tr.appendChild(td(when, when));
    tr.appendChild(td(d.title||'', (d.title||'').toLowerCase()));

    const ucell = document.createElement('span');
    ucell.textContent = d.username||"";
    const extra = [];
    if(d.channel_joined) extra.push(d.channel_joined);
    if(d.age_days != null && d.age_days >= 0) extra.push(d.age_days + 'd');
    if(extra.length) {
        const j=document.createElement('span');
        j.className='small';
        j.textContent=' ('+extra.join(' • ')+')';
        ucell.appendChild(j);
    }
    tr.appendChild(td(ucell, (d.username||"").toLowerCase()));

    tr.appendChild(td(String(followers), followers));
    tr.appendChild(td(String(videos), videos));
    tr.appendChild(td(String(viewers), viewers));
    tr.appendChild(td(String(chat), chat));

    const actions = document.createElement('td');
    const a = document.createElement('a'); a.className='small'; a.href=d.url; a.target='_blank'; a.textContent='Open';
    const b = document.createElement('button'); b.className='small'; b.textContent='Chatters'; b.onclick=()=>showChatters(d.url);
    actions.appendChild(a);
    actions.appendChild(document.createTextNode(' '));
    actions.appendChild(b);
    tr.appendChild(actions);

    tr.appendChild(td(d.video_id||'', d.video_id||''));
    tbody.insertBefore(tr, tbody.firstChild);
}
function showToast(message, level = 'info') { // Levels: info, success, error
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
function confirmClear(e){ if(!confirm('This will permanently delete all scanned data. Continue?')){ e.preventDefault(); } }

window.addEventListener('DOMContentLoaded', ()=>{
    const scanBtn = document.getElementById('scanBtn');
    const scanForm = document.getElementById('scanForm');
    if (scanBtn && scanForm) {
        scanBtn.addEventListener('click', async () => {
            scanBtn.disabled = true;
            scanBtn.textContent = 'Scanning…';
            try {
                await fetch(scanForm.action, { method: 'POST', body: new FormData(scanForm) });
            } catch (e) {
                console.error("Scan trigger failed:", e);
                showToast('Failed to start scan.', 'error');
            } finally {
                scanBtn.disabled = false;
                scanBtn.textContent = '🔎 Scan Now';
            }
        });
    }
    const es = new EventSource('/events');
    es.onmessage = (ev) => {
        try {
            const msg = JSON.parse(ev.data);
            if (msg.type === 'suspect'){
                addRowToCategoryTable(msg.data);
                showToast(`Suspect found: ${msg.data.title.substring(0, 30)}...`, 'success');
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

    <form id="scanForm" method="post" action="{{ url_for('scan') }}" class="filters">
        <div class="grid">
            <div>
                <label>Total views</label>
                <div class="row">
                    <input type="number" name="views_min" value="{{ filters.views_min }}">
                    <input type="number" name="views_max" value="{{ filters.views_max }}">
                </div>
            </div>
            <div>
                <label>Live viewers</label>
                <div class="row">
                    <input type="number" name="viewers_min" value="{{ filters.viewers_min }}">
                    <input type="number" name="viewers_max" value="{{ filters.viewers_max }}">
                </div>
            </div>
            <div>
                <label>Total ads</label>
                <div class="row">
                    <input type="number" name="ads_min" value="{{ filters.ads_min }}">
                    <input type="number" name="ads_max" value="{{ filters.ads_max }}">
                </div>
            </div>
            <div>
                <label>Chat messages</label>
                <div class="row">
                    <input type="number" name="chat_min" value="{{ filters.chat_min }}">
                    <input type="number" name="chat_max" value="{{ filters.chat_max }}">
                </div>
            </div>
            <div>
                <label>Account age (days)</label>
                <div class="row">
                    <input type="number" name="age_min" value="{{ filters.age_min }}">
                    <input type="number" name="age_max" value="{{ filters.age_max }}">
                </div>
            </div>
        </div>
    </form>

    <div class="actions">
        <span class="pill">Skip: Views > {{ "{:,}".format(views_skip) }}</span>
        <span class="pill">Browser: <strong>{{ 'ON' if browser_on else 'OFF' }}</strong></span>
        <button id="scanBtn" type="button" form="scanForm">🔎 Scan Now</button>
        <form method="post" action="{{ url_for('clear_data') }}" style="display:inline" onsubmit="confirmClear(event)">
            <button type="submit" class="danger">🗑️ Clear Data</button>
        </form>
    </div>

    {% macro table_head(table_id) -%}
    <div class="table-wrap">
    <table id="{{ table_id }}" data-dir="asc">
        <thead>
            <tr>
                <th onclick="sortTableByCol('{{ table_id }}', 0, false)">When</th>
                <th onclick="sortTableByCol('{{ table_id }}', 1, false)">Title</th>
                <th onclick="sortTableByCol('{{ table_id }}', 2, false)">Username (Joined • Age)</th>
                <th onclick="sortTableByCol('{{ table_id }}', 3)">Followers</th>
                <th onclick="sortTableByCol('{{ table_id }}', 4)">Videos</th>
                <th onclick="sortTableByCol('{{ table_id }}', 5)">Viewers</th>
                <th onclick="sortTableByCol('{{ table_id }}', 6)">Chat</th>
                <th>Actions</th>
                <th onclick="sortTableByCol('{{ table_id }}', 8, false)">Video ID</th>
            </tr>
        </thead>
        <tbody>
    {%- endmacro %}

    {% macro table_rows(rows) -%}
        {% if rows and rows|length > 0 %}
            {% for r in rows %}
            <tr class="bad">
                <td data-v="{{ r.created_at }}">{{ r.created_at }}</td>
                <td data-v="{{ r.title|lower }}">{{ r.title }}</td>
                <td data-v="{{ r.username|lower }}">
                    {{ r.username }}
                    {% if r.joined or (r.age_days is not none and r.age_days >= 0) %}
                        <span class="small">(
                        {% if r.joined %}{{ r.joined }}{% endif %}
                        {% if r.joined and (r.age_days is not none and r.age_days >= 0) %} • {% endif %}
                        {% if r.age_days is not none and r.age_days >= 0 %}{{ r.age_days }}d{% endif %}
                        )</span>
                    {% endif %}
                </td>
                <td data-v="{{ r.followers }}">{{ "{:,}".format(r.followers) }}</td>
                <td data-v="{{ r.total_videos }}">{{ "{:,}".format(r.total_videos) }}</td>
                <td data-v="{{ r.current_viewers }}">{{ "{:,}".format(r.current_viewers) }}</td>
                <td data-v="{{ r.chat_count }}">{{ "{:,}".format(r.chat_count) }}</td>
                <td>
                    <a class="small" href="{{ r.url }}" target="_blank">Open</a>
                    <button type="button" class="small" onclick="showChatters('{{ r.url }}')">Chatters</button>
                </td>
                <td data-v="{{ r.video_id }}">{{ r.video_id }}</td>
            </tr>
            {% endfor %}
        {% else %}
            <tr class="placeholder"><td colspan="9" class="small">No suspects in this category yet.</td></tr>
        {% endif %}
    {%- endmacro %}

    {% macro table_tail() -%}
        </tbody>
    </table>
    </div>
    {%- endmacro %}

    {% for cat in categories %}
    {% set _id = 'cat-' + cat.name|lower|replace(' ','-')|replace('&','and')|replace('/','-') %}
    <section id="{{ _id }}" class="accordion acc open">
        <div class="acc-head" onclick="toggleAcc('{{ _id }}')">
            <div class="acc-title">
                <svg class="chev" width="16" height="16" viewBox="0 0 24 24"><path fill="currentColor" d="M9 6l6 6-6 6"/></svg>
                <h2>{{ cat.name }}</h2>
                <span class="badge">{{ cat.rows|length }}</span>
            </div>
            <div class="small">Sortable columns • New suspects stream in live</div>
        </div>
        <div class="acc-body">
            {{ table_head(_id ~ '-table') }}
            {{ table_rows(cat.rows) }}
            {{ table_tail() }}
        </div>
    </section>
    {% endfor %}

    <div id="modal-backdrop" onclick="if(event.target===this)closeModal()">
        <div id="modal" role="dialog">
            <header>
                <strong>Chatters</strong>
                <button class="close" onclick="closeModal()">Close</button>
            </header>
            <div class="content" id="modal-content"></div>
        </div>
    </div>
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
    <p class="small">No chatters detected (chat might not be hydrated, or chat is off).</p>
{% endif %}
"""


# -------- Helpers to prep template data --------
def _row_to_dict(r):
    return {
        "category": r[0], "title": r[1], "channel": r[2], "username": r[3],
        "joined": r[4], "views": r[5], "current_viewers": r[6], "chat_count": r[7],
        "ads_count": r[8], "url": r[9], "video_id": r[10],
        "age_days": (None if r[11] == -1 else r[11]),
        "created_at": r[12], "followers": r[13], "total_videos": r[14]
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
    return render_template_string(
        INDEX_HTML,
        categories=categories,
        filters=FILTER_DEFAULTS,
        views_skip=VIEWS_SKIP_MAX,
        browser_on=USE_BROWSER
    )


@app.route("/scan", methods=["POST"])
def scan():
    filters = build_filters_from_form(request.form)
    print(f"🧪 Using filters: {filters}")
    scan_id = start_scan_row()
    BROKER.publish({'type': 'toast', 'data': {'message': 'Scan started...', 'level': 'info'}})
    t = threading.Thread(target=run_scan, args=(scan_id, filters), daemon=True)
    t.start()
    return ("OK", 202)


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

    return Response(gen(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive",
    })


@app.route("/chatters")
def chatters():
    stream_url = request.args.get("url", "").strip()
    if not stream_url: abort(400, "Missing url parameter")
    if stream_url.startswith("/"): stream_url = urljoin(BASE_URL, stream_url)

    print(f"🧑‍🤝‍🧑 Collecting chatters for: {stream_url}")
    users = browser_collect_chat_usernames(stream_url, max_users=300)
    print(f"    → Found {len(users)} unique usernames; fetching Joined dates")

    def _fetch_one_chatter(u):
        uname = u["username"]
        j = fetch_user_joined_date(uname)
        return {"username": uname, "profile_url": u["profile_url"], "joined": j}

    with ThreadPoolExecutor(max_workers=MAX_FETCH_WORKERS) as ex:
        enriched = list(ex.map(_fetch_one_chatter, users))

    return render_template_string(CHATTERS_FRAGMENT_HTML, users=enriched)


@app.route("/clear", methods=["POST"])
def clear_data():
    clear_all_data()
    print("🧹 All data cleared")
    return redirect(url_for("index"))


if __name__ == "__main__":
    # If using Playwright for the first time:
    # 1. pip install playwright
    # 2. playwright install chromium
    app.run(debug=True, threaded=True, use_reloader=False)