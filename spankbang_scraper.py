"""SpankBang scraper for fbot.

Tries two approaches in order:
1. Direct HTML scrape via requests/cloudscraper (fast, no yt-dlp needed)
   — based on spankbang-dl library logic
2. Falls back to yt-dlp (same path as all other HOST_PATTERNS sites)

Typical SpankBang page: https://spankbang.com/<id>/video/<slug>
"""
import logging
import re

import requests

logger = logging.getLogger(__name__)

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)
_HEADERS = {"User-Agent": _UA, "Referer": "https://spankbang.com/"}

SPANKBANG_PATTERN = re.compile(
    r"https?://(?:www\.)?spankbang\.(?:com|party)/([A-Za-z0-9_-]+)/(?:video|embed)/",
    re.IGNORECASE,
)


def is_spankbang_link(url: str) -> bool:
    return bool(SPANKBANG_PATTERN.search(url))


def extract_spankbang_links(text: str) -> list[str]:
    return SPANKBANG_PATTERN.findall(text)


def get_video_info(url: str) -> dict | None:
    """Scrape SpankBang page and return {title, url, thumbnail} or None."""
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=20, allow_redirects=True)
        resp.raise_for_status()
        html = resp.text
    except Exception as e:
        logger.warning(f"SpankBang fetch failed: {e}")
        return None

    # Try to extract stream URL from page HTML
    # SpankBang embeds stream_data JSON or direct mp4 URLs in script tags
    title = None
    video_url = None
    thumbnail = None

    # Title extraction
    t_match = re.search(r'<title[^>]*>Watch\s+(.+?)\s*(?:-|—)\s*SpankBang', html, re.IGNORECASE)
    if t_match:
        title = t_match.group(1).strip()
    else:
        t_match = re.search(r'<h1[^>]*class="[^"]*video[_-]?title[^"]*"[^>]*>([^<]+)', html, re.IGNORECASE)
        if t_match:
            title = t_match.group(1).strip()

    # Stream URL extraction — try multiple patterns
    # Pattern 1: stream_data json blob
    sd_match = re.search(r'stream_data\s*=\s*(\{[^}]+\})', html)
    if sd_match:
        import json
        try:
            sd = json.loads(sd_match.group(1))
            for q in ("1080p", "720p", "480p", "320p", "240p"):
                if sd.get(q):
                    video_url = sd[q]
                    break
        except Exception:
            pass

    # Pattern 2: mp4 URL in script
    if not video_url:
        mp4 = re.search(r'["\']?(https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*)["\']?', html)
        if mp4:
            video_url = mp4.group(1)

    # Pattern 3: <video><source src=...>
    if not video_url:
        src = re.search(r'<source[^>]+src=["\']([^"\']+\.(?:mp4|m3u8)[^"\']*)["\']', html, re.IGNORECASE)
        if src:
            video_url = src.group(1)

    # Thumbnail
    th = re.search(r'(?:og:image|twitter:image)[^>]+content=["\']([^"\']+)["\']', html, re.IGNORECASE)
    if th:
        thumbnail = th.group(1)

    if not video_url:
        logger.info(f"SpankBang HTML scrape found no stream URL for {url} — yt-dlp will handle it")
        return None

    return {"title": title, "url": video_url, "thumbnail": thumbnail}
