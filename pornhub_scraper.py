"""
pornhub.com auto-scraper backend for auto_scraper.py's pornhub_uploader_worker.

FIX v2:
  + impersonate=chrome-124 in _flat_entries (PornHub blocks Render/cloud IPs
    without a browser TLS fingerprint — same fix as xhamster/xvideos)
  + socket_timeout=20 added (was missing — could hang forever)
  + webpage_url preferred over url in _entry_to_item
  + fallback: try /model/<slug>/videos if /pornstar/<slug>/videos fails
"""

import asyncio
import logging
import random
import re
from urllib.parse import urlparse

import yt_dlp

logger = logging.getLogger(__name__)

PER_PAGE = 30

_MODEL_URLS = [
    "https://www.pornhub.com/pornstar/{slug}/videos",
    "https://www.pornhub.com/model/{slug}/videos",
]
_STUDIO_URL = "https://www.pornhub.com/channels/{slug}/videos"

_RANDOM_SOURCE_URLS = [
    "https://www.pornhub.com/video?o=mv",   # most viewed
    "https://www.pornhub.com/video?o=tr",   # top rated
    "https://www.pornhub.com/video?o=cm",   # most recent
]

_UNKNOWN_TOTAL_PAGES = 10_000
_CHROME_TARGET = "chrome-124"


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    return slug.strip("-")


def _flat_entries(url: str, playlist_start: int, playlist_end: int) -> list:
    """
    FIX: impersonate=chrome-124 + socket_timeout=20 added.
    PornHub actively blocks Render/cloud datacenter IPs without a browser
    TLS fingerprint. Falls back to plain requests if curl_cffi unavailable.
    """
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
    # With impersonation (preferred — bypasses bot detection)
    try:
        with yt_dlp.YoutubeDL({**opts, "impersonate": _CHROME_TARGET}) as ydl:
            info = ydl.extract_info(url, download=False)
        entries = (info or {}).get("entries") or []
        if entries:
            return entries
    except Exception:
        pass

    # Fallback: plain request
    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info = ydl.extract_info(url, download=False)
        return (info or {}).get("entries") or []
    except Exception as e:
        logger.debug(f"[pornhub] _flat_entries failed for {url}: {e}")
        return []


def _try_urls_with_fallback(urls: list, playlist_start: int, playlist_end: int) -> list:
    # Same fix as xhamster_scraper.py/xvideos_scraper.py's identical
    # function — try every pattern and keep whichever returns the most
    # entries, instead of stopping at the first pattern that returns
    # anything at all (which could be a single-video false-positive
    # shadowing a pattern that would've returned the full listing).
    best_entries: list = []
    best_url = None
    for url in urls:
        try:
            entries = _flat_entries(url, playlist_start, playlist_end)
        except Exception as e:
            logger.debug(f"[pornhub] failed {url}: {e}")
            continue
        if entries:
            logger.debug(f"[pornhub] got {len(entries)} entries from {url}")
        if len(entries) > len(best_entries):
            best_entries, best_url = entries, url
        if len(best_entries) >= (playlist_end - playlist_start + 1):
            break
    if best_entries:
        logger.debug(f"[pornhub] using {len(best_entries)} entries from {best_url}")
    return best_entries


def _entry_to_item(entry: dict) -> dict | None:
    # FIX: prefer webpage_url over url
    video_url = entry.get("webpage_url") or entry.get("url")
    if not video_url or not video_url.startswith("http"):
        return None
    video_id = entry.get("id") or video_url
    slug = f"pornhub-{video_id}"
    return {"slug": slug, "url": video_url, "title": entry.get("title") or slug}


def _term_to_slug(term: str, url_path_prefixes: tuple) -> str:
    if term.startswith("http"):
        path = urlparse(term).path.strip("/")
        segments = path.split("/")
        if len(segments) >= 2 and segments[0] in url_path_prefixes:
            return segments[1]
    return _slugify(term)


async def get_model_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    """term: a pornstar/model name or their PornHub page link.
    Tries /pornstar/<slug>/videos then /model/<slug>/videos as fallback."""
    slug  = _term_to_slug(term, ("pornstar", "model"))
    start = (page - 1) * PER_PAGE + 1
    end   = page * PER_PAGE

    urls = [u.format(slug=slug) for u in _MODEL_URLS]
    try:
        entries = await asyncio.to_thread(_try_urls_with_fallback, urls, start, end)
    except Exception as e:
        logger.warning(f"[pornhub] get_model_page_videos failed for {term!r}: {e}")
        return [], 0

    items = [item for e in entries if (item := _entry_to_item(e)) is not None]
    return items, _UNKNOWN_TOTAL_PAGES


async def get_studio_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    """term: a studio/channel name or channel page link."""
    slug  = _term_to_slug(term, ("channels",))
    url   = _STUDIO_URL.format(slug=slug)
    start = (page - 1) * PER_PAGE + 1
    end   = page * PER_PAGE

    try:
        entries = await asyncio.to_thread(_flat_entries, url, start, end)
    except Exception as e:
        logger.warning(f"[pornhub] get_studio_page_videos failed for {term!r}: {e}")
        return [], 0

    items = [item for e in entries if (item := _entry_to_item(e)) is not None]
    return items, _UNKNOWN_TOTAL_PAGES


async def search_videos(query: str, page: int = 1) -> tuple:
    """
    Search PornHub for `query`.
    Works for both a plain keyword ("comatozze") and a full
    /video/search?search=… URL (the query-string value is extracted).
    Returns (items, total_pages) same as get_model_page_videos().
    """
    from urllib.parse import urlparse as _up, parse_qs as _qs, quote as _q
    # Accept either a keyword or a full search URL
    if query.startswith("http"):
        parsed = _up(query)
        qs = _qs(parsed.query)
        query = (qs.get("search") or qs.get("q") or [""])[0].strip() or query
    search_url = f"https://www.pornhub.com/video/search?search={_q(query)}"
    start = (page - 1) * PER_PAGE + 1
    end   = page * PER_PAGE
    try:
        entries = await asyncio.to_thread(_flat_entries, search_url, start, end)
    except Exception as e:
        logger.warning(f"[pornhub] search_videos failed for {query!r}: {e}")
        return [], 0
    items = [item for e in entries if (item := _entry_to_item(e)) is not None]
    return items, _UNKNOWN_TOTAL_PAGES


async def get_random_page_videos() -> list:
    url   = random.choice(_RANDOM_SOURCE_URLS)
    page  = random.randint(1, 20)
    start = (page - 1) * PER_PAGE + 1
    end   = page * PER_PAGE
    try:
        entries = await asyncio.to_thread(_flat_entries, url, start, end)
    except Exception as e:
        logger.warning(f"[pornhub] get_random_page_videos failed: {e}")
        return []
    return [item for e in entries if (item := _entry_to_item(e)) is not None]
