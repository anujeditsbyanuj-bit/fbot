"""
xvideos.com auto-scraper backend for auto_scraper.py's xvideos_uploader_worker
— same role as pornhub_scraper.py/xhamster_scraper.py, just for XVideos.

URL patterns (yt-dlp confirmed):
  - performer page: https://www.xvideos.com/profiles/<slug>
  - channel page:   https://www.xvideos.com/channels/<slug>

FIX v3:
  + requests+BeautifulSoup HTML scraping fallback for get_latest_videos()
    (yt-dlp playlist fetch fails on Render/cloud IPs due to IP-level block)
  + xvideos JSON API endpoint used for latest listing (more reliable than HTML)
  + impersonate=chrome-124 kept for individual video pages (still works)
"""

import asyncio
import json
import logging
import re
import urllib.request
from urllib.parse import urlparse

import yt_dlp

logger = logging.getLogger(__name__)

PER_PAGE = 30
# XVideos serves performer pages under any of these prefixes depending on
# how the performer registered — or with no prefix at all for a bare
# username (handled separately in get_model_page_videos, since that one
# has no fixed template to list here).
_MODEL_URL_PREFIXES = ("profiles", "models", "pornstars", "amateurs")
_STUDIO_URL = "https://www.xvideos.com/channels/{slug}"

_DOMAINS = [
    "https://www.xvideos.com",

]

# XVideos JSON API endpoints — these return structured JSON, not HTML,
# so yt-dlp playlist-parse is not needed (avoids the Unsupported URL error).
# XVideos blocks ALL cloud/server IPs — both HTML and API endpoints return 404.
# These are kept for reference but the monitor is disabled below.
_API_LATEST_URLS: list = []
_HTML_LATEST_URLS: list = []

_UNKNOWN_TOTAL_PAGES = 10_000
_CHROME_TARGET = "chrome-124"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    return slug.strip("-")


def _fetch_url(url: str, timeout: int = 15) -> str:
    """Simple HTTP GET with browser headers, no yt-dlp needed."""
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _items_from_xvideos_json(raw: str) -> list:
    """Parse xvideos JSON API response into item dicts."""
    try:
        data = json.loads(raw)
    except Exception:
        return []
    videos = data.get("videos") or data.get("results") or []
    items = []
    for v in videos:
        vid_url = v.get("url") or v.get("link") or v.get("u")
        if not vid_url:
            continue
        if not vid_url.startswith("http"):
            vid_url = "https://www.xvideos.com" + vid_url
        vid_id = str(v.get("id") or vid_url)
        items.append({
            "slug": f"xvideos-{vid_id}",
            "url": vid_url,
            "title": v.get("title") or v.get("tf") or vid_id,
        })
    return items


def _items_from_xvideos_html(html: str) -> list:
    """Scrape video URLs from xvideos listing HTML page."""
    # xvideos embeds video data as JSON in a JS variable
    # e.g. xvideos_list_videos_69_full = [{...}]
    pattern = re.compile(r'xv\.conf\s*=\s*(\{.*?\});', re.DOTALL)
    match = pattern.search(html)
    if match:
        try:
            conf = json.loads(match.group(1))
            videos = conf.get("videos") or []
            items = []
            for v in videos:
                vid_url = v.get("url") or v.get("u")
                if not vid_url or not vid_url.startswith("http"):
                    continue
                vid_id = str(v.get("id") or vid_url)
                items.append({
                    "slug": f"xvideos-{vid_id}",
                    "url": vid_url,
                    "title": v.get("tf") or v.get("title") or vid_id,
                })
            if items:
                return items
        except Exception:
            pass

    # Fallback: regex scrape href="/video<id>/"
    urls = re.findall(r'href="(https?://(?:www\.xvideos\.com|www\.xvideos\.red)/video\d+/[^"]+)"', html)
    items = []
    seen = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            vid_id = re.search(r'/video(\d+)/', u)
            slug_id = vid_id.group(1) if vid_id else u
            items.append({"slug": f"xvideos-{slug_id}", "url": u, "title": slug_id})
    return items


def _flat_entries(url: str, playlist_start: int, playlist_end: int) -> list:
    opts = {
        "quiet":          True,
        "no_warnings":    True,
        "extract_flat":   "in_playlist",
        "playliststart":  playlist_start,
        "playlistend":    playlist_end,
        "skip_download":  True,
        "socket_timeout": 20,
        "nocheckcertificate": True,
    }
    try:
        with yt_dlp.YoutubeDL({**opts, "impersonate": _CHROME_TARGET}) as ydl:
            info = ydl.extract_info(url, download=False)
        entries = (info or {}).get("entries") or []
        if entries:
            return entries
    except Exception:
        pass
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return (info or {}).get("entries") or []
    except Exception as e:
        logger.debug(f"[xvideos] _flat_entries failed for {url}: {e}")
        return []


def _try_urls_with_fallback(urls: list, playlist_start: int, playlist_end: int) -> list:
    # Same fix as xhamster_scraper.py's identical function — try every
    # pattern and keep whichever returns the most entries, instead of
    # stopping at the first pattern that returns anything at all (which
    # could be a single-video false-positive shadowing a pattern that
    # would've returned the full listing). See that file's comment for
    # the full reasoning; this file's own docstrings note yt-dlp's
    # XVideos extractor currently always returns empty for performer
    # pages regardless of pattern (falling through to a separate HTML
    # scrape instead), so this specific bug may not be live here today —
    # kept in sync anyway since both scrapers are meant to behave
    # identically.
    best_entries: list = []
    best_url = None
    for url in urls:
        try:
            entries = _flat_entries(url, playlist_start, playlist_end)
        except Exception as e:
            logger.debug(f"[xvideos] failed {url}: {e}")
            continue
        if entries:
            logger.debug(f"[xvideos] got {len(entries)} entries from {url}")
        if len(entries) > len(best_entries):
            best_entries, best_url = entries, url
        if len(best_entries) >= (playlist_end - playlist_start + 1):
            break
    if best_entries:
        logger.debug(f"[xvideos] using {len(best_entries)} entries from {best_url}")
        return best_entries
    logger.warning(f"[xvideos] all URLs failed. Tried: {urls}")
    return []


def _entry_to_item(entry: dict) -> dict | None:
    video_url = entry.get("webpage_url") or entry.get("url")
    if not video_url or not video_url.startswith("http"):
        return None
    video_id = entry.get("id") or video_url
    slug = f"xvideos-{video_id}"
    return {"slug": slug, "url": video_url, "title": entry.get("title") or slug}


def _term_to_slug(term: str, url_path_prefixes: tuple) -> str:
    if term.startswith("http"):
        path = urlparse(term).path.strip("/")
        segments = path.split("/")
        if len(segments) >= 2 and segments[0] in url_path_prefixes:
            return segments[1]
        if len(segments) == 1 and segments[0]:
            # A bare "xvideos.com/<username>" URL — no /profiles/,
            # /models/, /pornstars/, /amateurs/, or /channels/ prefix at
            # all (e.g. xvideos.com/freeuse96, xvideos.com/your_priya99).
            # XVideos serves plain usernames straight off the root, so
            # the single path segment itself IS the slug — not something
            # to fall through to _slugify() for (that would slugify the
            # whole URL string into garbage like
            # "https-www-xvideos-com-freeuse96" instead of "freeuse96").
            return segments[0]
        if len(segments) >= 1 and segments[0] in url_path_prefixes:
            return term.rstrip("/").rsplit("/", 1)[-1]
    return _slugify(term)


def _fetch_latest_items_sync() -> list:
    """
    Tries multiple strategies to get latest xvideos listings:
    1. JSON API endpoints (most reliable, no yt-dlp needed)
    2. HTML scraping fallback
    3. yt-dlp playlist (last resort, often blocked on server IPs)
    """
    # Strategy 1: JSON API
    for url in _API_LATEST_URLS:
        try:
            raw = _fetch_url(url)
            items = _items_from_xvideos_json(raw)
            if items:
                logger.info(f"[xvideos] ✅ latest via JSON API: {len(items)} items from {url}")
                return items
            else:
                logger.warning(f"[xvideos] JSON API returned empty from {url}")
        except Exception as e:
            logger.warning(f"[xvideos] JSON API failed {url}: {e}")

    # Strategy 2 & 3 removed: /new-videos/* always 404 on Render/cloud IPs.
    logger.warning("[xvideos] all strategies failed — returning empty list")
    return []


async def get_model_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    """Performer/model pages — XVideos serves these under several
    different path prefixes depending on how the performer registered
    (profiles/models/pornstars/amateurs), or with no prefix at all for a
    plain username (e.g. xvideos.com/freeuse96). Tries every known shape
    in order via _try_urls_with_fallback, which already stops at the
    first one that actually returns entries — so a bare-username link
    doesn't need to be told which category it is, and an ambiguous typed
    name (not a URL) still works the same way it always did.

    yt-dlp's XVideos extractor doesn't treat a performer page as a
    playlist at all (confirmed via a live "Unsupported URL" error), so
    _try_urls_with_fallback's yt-dlp-based _flat_entries() always came
    back empty here regardless of which prefix or page number was tried
    — this fell back to a plain HTML fetch + the same xv.conf/regex
    scrape get_latest_videos() already relies on, which works on any
    xvideos listing page's markup, not just the homepage's."""
    slug = _term_to_slug(term, _MODEL_URL_PREFIXES)
    start = (page - 1) * PER_PAGE + 1
    end = page * PER_PAGE
    urls = (
        [f"https://www.xvideos.com/{prefix}/{slug}" for prefix in _MODEL_URL_PREFIXES]
        + [f"https://www.xvideos.com/{slug}"]  # bare-username fallback
    )
    try:
        entries = await asyncio.to_thread(_try_urls_with_fallback, urls, start, end)
        items = [item for e in entries if (item := _entry_to_item(e)) is not None]
        if items:
            return items, _UNKNOWN_TOTAL_PAGES
    except Exception as e:
        logger.warning(f"[xvideos] get_model_page_videos yt-dlp path failed for {term!r}: {e}")

    for url in urls:
        try:
            html = await asyncio.to_thread(_fetch_url, url)
            items = await asyncio.to_thread(_items_from_xvideos_html, html)
            if items:
                return items[start - 1:end], _UNKNOWN_TOTAL_PAGES
        except Exception as e:
            logger.debug(f"[xvideos] model HTML scrape failed {url}: {e}")
    return [], 0


async def get_studio_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    slug = _term_to_slug(term, ("channels",))
    start = (page - 1) * PER_PAGE + 1
    end = page * PER_PAGE
    urls = [f"https://www.xvideos.com/channels/{slug}"]
    try:
        entries = await asyncio.to_thread(_try_urls_with_fallback, urls, start, end)
        items = [item for e in entries if (item := _entry_to_item(e)) is not None]
        if items:
            return items, _UNKNOWN_TOTAL_PAGES
    except Exception as e:
        logger.warning(f"[xvideos] get_studio_page_videos yt-dlp path failed for {term!r}: {e}")

    for url in urls:
        try:
            html = await asyncio.to_thread(_fetch_url, url)
            items = await asyncio.to_thread(_items_from_xvideos_html, html)
            if items:
                return items[start - 1:end], _UNKNOWN_TOTAL_PAGES
        except Exception as e:
            logger.debug(f"[xvideos] studio HTML scrape failed {url}: {e}")
    return [], 0


async def get_latest_videos() -> list:
    """
    Multi-strategy latest video fetch:
    1. XVideos JSON API (no yt-dlp, works even when playlist URLs are blocked)
    2. HTML scraping fallback
    3. yt-dlp as last resort
    """
    try:
        items = await asyncio.to_thread(_fetch_latest_items_sync)
        return items
    except Exception as e:
        logger.warning(f"[xvideos] get_latest_videos failed: {e}")
        return []


async def get_random_page_videos() -> list:
    return await get_latest_videos()
