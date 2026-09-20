"""
xhamster auto-scraper backend for auto_scraper.py's xhamster_uploader_worker
— same role as pornhub_scraper.py/xvideos_scraper.py, just for xHamster.

FIX v3:
  + Multi-strategy get_latest_videos():
    1. xHamster JSON API (no yt-dlp, works even when Render IP is blocked)
    2. HTML scraping fallback (urllib + regex)
    3. yt-dlp as absolute last resort
  + /videos/recent + /newest 404 on Render IPs — API used instead
  + xhamster2.com added as fallback domain
"""

import asyncio
import json
import logging
import re
import urllib.request
from urllib.parse import urlparse, quote

import yt_dlp

logger = logging.getLogger(__name__)

PER_PAGE = 30

_DOMAINS = [
    "https://xhamster.com",
    "https://xhamster18.com",
    "https://xhamster2.com",
    "https://xhamster.desi",
]

_MODEL_URL_PATTERNS = [
    "{domain}/users/{slug}/videos",
    "{domain}/creators/{slug}",
]

_STUDIO_URL_PATTERN = "{domain}/channels/{slug}"

_UNKNOWN_TOTAL_PAGES = 10_000
_CHROME_TARGET = "chrome-124"

_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "X-Requested-With": "XMLHttpRequest",
}

# xHamster JSON API endpoints for latest videos
_API_LATEST_URLS = [
    "https://xhamster.com/api/front/v2/videos?page=1&sort=newest",
    "https://xhamster.com/api/front/v2/videos?sort=newest&page=1",
    "https://xhamster18.com/api/front/v2/videos?page=1&sort=newest",
]

# HTML listing pages as fallback
_HTML_LATEST_URLS = [
    "https://xhamster.com/videos",
    "https://xhamster18.com/videos",
    "https://xhamster2.com/videos",
]


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    return slug.strip("-")


def _extract_domain_from_url(url: str) -> str | None:
    try:
        parsed = urlparse(url)
        domain = f"{parsed.scheme}://{parsed.netloc}"
        if "xhamster" in parsed.netloc:
            return domain
    except Exception:
        pass
    return None


def _fetch_url(url: str, timeout: int = 15) -> str:
    req = urllib.request.Request(url, headers=_HEADERS)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8", errors="replace")


def _items_from_xhamster_json(raw: str) -> list:
    """Parse xHamster API JSON response."""
    try:
        data = json.loads(raw)
    except Exception:
        return []
    # Try different response shapes
    videos = (data.get("videos") or {}).get("items") or \
             data.get("items") or \
             data.get("data") or []
    items = []
    for v in videos:
        vid_url = v.get("pageURL") or v.get("url") or v.get("link")
        if not vid_url or not vid_url.startswith("http"):
            continue
        vid_id = str(v.get("id") or vid_url)
        items.append({
            "slug": f"xhamster-{vid_id}",
            "url": vid_url,
            "title": v.get("title") or vid_id,
        })
    return items


def _items_from_xhamster_html(html: str) -> list:
    """Scrape video URLs from xHamster listing HTML."""
    # xhamster embeds video data as JSON in script tags
    pattern = re.compile(r'videos\s*:\s*(\[.*?\])', re.DOTALL)
    match = pattern.search(html)
    if match:
        try:
            videos = json.loads(match.group(1))
            items = []
            for v in videos:
                vid_url = v.get("pageURL") or v.get("url")
                if not vid_url or not vid_url.startswith("http"):
                    continue
                vid_id = str(v.get("id") or vid_url)
                items.append({
                    "slug": f"xhamster-{vid_id}",
                    "url": vid_url,
                    "title": v.get("title") or vid_id,
                })
            if items:
                return items
        except Exception:
            pass

    # Fallback: href regex
    urls = re.findall(
        r'href="(https?://(?:xhamster\d?\.com|xhamster\.desi)/videos/[^"]+)"',
        html
    )
    items = []
    seen = set()
    for u in urls:
        if u not in seen and "/videos/" in u:
            seen.add(u)
            vid_id = u.rstrip("/").rsplit("/", 1)[-1]
            items.append({"slug": f"xhamster-{vid_id}", "url": u, "title": vid_id})
    return items


def _flat_entries(url: str, playlist_start: int, playlist_end: int) -> list:
    opts = {
        "quiet":              True,
        "no_warnings":        True,
        "extract_flat":       "in_playlist",
        "playliststart":      playlist_start,
        "playlistend":        playlist_end,
        "skip_download":      True,
        "socket_timeout":     20,
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
        logger.debug(f"[xhamster] _flat_entries failed for {url}: {e}")
        return []


def _try_urls_with_fallback(urls: list, playlist_start: int, playlist_end: int) -> list:
    # FIX: this used to `return entries` on the FIRST url pattern that
    # returned ANYTHING non-empty — including just 1 entry. If yt-dlp's
    # extractor happens to treat one of the earlier url patterns (e.g.
    # "{domain}/users/{slug}/videos") as a single-video page rather than
    # a proper playlist listing for a given performer, that single entry
    # "succeeded" and the loop stopped right there, never trying the
    # other patterns (e.g. "{domain}/creators/{slug}") that might
    # correctly return the performer's full video list. This is exactly
    # what "auto-upload by actor name only finds 1 video" looks like.
    # Now every pattern gets tried and the one with the most entries
    # wins, so a single-video false-positive from one pattern can't
    # shadow a working multi-video result from another.
    best_entries: list = []
    best_url = None
    for url in urls:
        try:
            entries = _flat_entries(url, playlist_start, playlist_end)
        except Exception as e:
            logger.debug(f"[xhamster] failed {url}: {e}")
            continue
        if entries:
            logger.debug(f"[xhamster] got {len(entries)} entries from {url}")
        if len(entries) > len(best_entries):
            best_entries, best_url = entries, url
        # A pattern that already returns a full page is as good as it
        # gets — no need to keep hitting every remaining domain/pattern
        # combination once we've clearly found the real listing.
        if len(best_entries) >= (playlist_end - playlist_start + 1):
            break
    if best_entries:
        logger.debug(f"[xhamster] using {len(best_entries)} entries from {best_url}")
        return best_entries
    logger.warning(f"[xhamster] all URLs failed. Tried: {urls}")
    return []


def _entry_to_item(entry: dict) -> dict | None:
    video_url = entry.get("webpage_url") or entry.get("url")
    if not video_url or not video_url.startswith("http"):
        return None
    video_id = entry.get("id") or video_url
    slug = f"xhamster-{video_id}"
    return {"slug": slug, "url": video_url, "title": entry.get("title") or slug}


def _term_to_slug(term: str, url_path_prefixes: tuple) -> str:
    if term.startswith("http"):
        path = urlparse(term).path.strip("/")
        segments = path.split("/")
        if len(segments) >= 2 and segments[0] in url_path_prefixes:
            return segments[1]
        if len(segments) >= 1 and segments[0] in url_path_prefixes:
            return term.rstrip("/").rsplit("/", 1)[-1]
    return _slugify(term)


def _fetch_latest_items_sync() -> list:
    """
    Multi-strategy latest video fetch:
    1. xHamster JSON API (most reliable — no yt-dlp needed)
    2. HTML scraping fallback
    3. yt-dlp as last resort
    """
    # Strategy 1: JSON API
    for url in _API_LATEST_URLS:
        try:
            raw = _fetch_url(url)
            items = _items_from_xhamster_json(raw)
            if items:
                logger.info(f"[xhamster] ✅ latest via JSON API: {len(items)} items from {url}")
                return items
            else:
                logger.warning(f"[xhamster] JSON API returned empty from {url}")
        except Exception as e:
            logger.warning(f"[xhamster] JSON API failed {url}: {e}")

    # Strategy 2: HTML scrape
    for url in _HTML_LATEST_URLS:
        try:
            html = _fetch_url(url)
            items = _items_from_xhamster_html(html)
            if items:
                logger.info(f"[xhamster] ✅ latest via HTML scrape: {len(items)} items from {url}")
                return items
            else:
                logger.warning(f"[xhamster] HTML scrape returned empty from {url}")
        except Exception as e:
            logger.warning(f"[xhamster] HTML scrape failed {url}: {e}")

    # Strategy 3 removed: yt-dlp does not support /newest URL format
    logger.warning("[xhamster] all strategies failed — returning empty list")
    return []


async def get_model_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    slug  = _term_to_slug(term, ("users", "creators"))
    start = (page - 1) * PER_PAGE + 1
    end   = page * PER_PAGE
    custom_domain  = _extract_domain_from_url(term)
    domains_to_try = [custom_domain] if custom_domain else _DOMAINS
    urls_to_try    = [
        pattern.format(domain=domain, slug=slug)
        for domain in domains_to_try
        for pattern in _MODEL_URL_PATTERNS
    ]
    try:
        entries = await asyncio.to_thread(_try_urls_with_fallback, urls_to_try, start, end)
    except Exception as e:
        logger.warning(f"[xhamster] get_model_page_videos failed for {term!r}: {e}")
        entries = []

    if entries:
        items = [item for e in entries if (item := _entry_to_item(e)) is not None]
        return items, _UNKNOWN_TOTAL_PAGES

    # ── Search fallback (only on page 1) ─────────────────────────────
    # When the slug-based URL returns 404 (performer uses a different
    # username on xHamster than their stage name), try the xHamster search
    # URL to find their actual page.
    # e.g. "Mandy Flores" → slug "mandy-flores" → 404
    #      → search "mandy flores" → finds /users/mandyflores/videos
    if page == 1:
        logger.info(f"[xhamster] slug-based URL failed for {term!r}, trying search fallback...")
        for domain in domains_to_try:
            search_url = f"{domain}/search/videos?q={quote(term, safe='')}"
            try:
                entries = await asyncio.to_thread(_flat_entries, search_url, 1, PER_PAGE)
                if entries:
                    logger.info(f"[xhamster] search fallback found {len(entries)} entries for {term!r}")
                    items = [item for e in entries if (item := _entry_to_item(e)) is not None]
                    if items:
                        return items, _UNKNOWN_TOTAL_PAGES
            except Exception as e:
                logger.debug(f"[xhamster] search fallback failed for {domain}: {e}")

    return [], 0


async def get_studio_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    slug  = _term_to_slug(term, ("channels",))
    start = (page - 1) * PER_PAGE + 1
    end   = page * PER_PAGE
    custom_domain  = _extract_domain_from_url(term)
    domains_to_try = [custom_domain] if custom_domain else _DOMAINS
    urls_to_try    = [
        _STUDIO_URL_PATTERN.format(domain=domain, slug=slug)
        for domain in domains_to_try
    ]
    try:
        entries = await asyncio.to_thread(_try_urls_with_fallback, urls_to_try, start, end)
    except Exception as e:
        logger.warning(f"[xhamster] get_studio_page_videos failed for {term!r}: {e}")
        return [], 0
    items = [item for e in entries if (item := _entry_to_item(e)) is not None]
    return items, _UNKNOWN_TOTAL_PAGES


async def get_latest_videos() -> list:
    """
    Multi-strategy latest video fetch — JSON API first, HTML scrape second,
    yt-dlp last resort. Handles Render/cloud IP blocks gracefully.
    """
    try:
        items = await asyncio.to_thread(_fetch_latest_items_sync)
        return items
    except Exception as e:
        logger.warning(f"[xhamster] get_latest_videos failed: {e}")
        return []


async def get_random_page_videos() -> list:
    return await get_latest_videos()
