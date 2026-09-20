"""
mat6tube.com auto-scraper backend for auto_scraper.py.

mat6tube.com is a VK-sourced video aggregator — watch URLs use VK-style
IDs like /watch/-<ownerId>_<videoId>. The site has no public API, so
this module scrapes HTML pages directly.

Browse/search URL patterns confirmed live on the site:
    Homepage (latest): https://mat6tube.com/
    Recent videos:     https://mat6tube.com/recent
    Search:             https://mat6tube.com/video/<query, spaces as %20>
    Watch page:         https://mat6tube.com/watch/-<ownerId>_<videoId>

Video links on listing pages follow the pattern:
    href="/watch/-<id>"

All meta-data needed for the caption (title, actor, duration, thumbnail)
comes from the watch page's own <meta> tags — see mat6tube_downloader.py.

Function contract auto_scraper.py expects (same as eporner_scraper):
    get_model_page_videos(term, page=1) -> (list[{slug,url,title}], total_pages)
    get_random_page_videos()            -> list[{slug,url,title}]
    get_latest_videos()                 -> list[{slug,url,title}]
"""

import logging
import random
import re
from urllib.parse import quote

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://mat6tube.com"
# noodlemagazine.com is the same site/player under a different domain
# (confirmed via mat6tube_downloader.py's NoodleMat-DL-ported fix) — used
# here as a fallback if mat6tube.com itself is down/blocked for this
# server, since a single hardcoded domain was a single point of failure
# for the whole browse/search/auto-upload flow.
_FALLBACK_BASE_URL = "https://noodlemagazine.com"
PER_PAGE = 24  # mat6tube shows ~24 videos per listing page

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)
_HEADERS = {
    "User-Agent": _UA,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": BASE_URL + "/",
}

# Broad search terms for random mode
_BROAD_TERMS = [
    "amateur", "teen", "milf", "asian", "latina", "ebony", "blonde",
    "brunette", "anal", "lesbian", "hardcore", "big tits", "creampie",
    "pov", "public", "homemade", "solo", "threesome", "mature", "indian",
]

# Regex to find watch links on listing pages
_WATCH_LINK_RE = re.compile(r'href=["\'](/watch/(-?\d+_\d+))["\']')
_TITLE_RE = re.compile(r'title=["\'](.*?)["\']', re.IGNORECASE)

# Simple pagination — mat6tube uses ?page=N or /page/<N>/ patterns
_TOTAL_RE = re.compile(r'(\d[\d,]*)\s*(?:videos?|results?)', re.IGNORECASE)


def _fetch(url: str) -> str | None:
    try:
        r = requests.get(url, headers=_HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        logger.warning(f"mat6tube fetch failed for {url}: {e}")
        # Retry on the mirror domain — same site, different host, so this
        # only helps when the failure was actually host-specific (down,
        # blocked, DNS) rather than a genuine 404/empty-result page.
        if BASE_URL in url:
            mirror_url = url.replace(BASE_URL, _FALLBACK_BASE_URL)
            try:
                r = requests.get(mirror_url, headers=_HEADERS, timeout=20)
                r.raise_for_status()
                logger.info(f"mat6tube: {BASE_URL} failed, mirror {_FALLBACK_BASE_URL} worked.")
                return r.text
            except Exception as e2:
                logger.warning(f"mat6tube mirror fetch also failed for {mirror_url}: {e2}")
        return None


def _parse_videos(html: str) -> list[dict]:
    """Extract video slugs/URLs/titles from a listing page."""
    items = []
    seen = set()

    # Find all watch links with their surrounding context for title extraction
    # mat6tube listing HTML pattern: <a href="/watch/ID" title="TITLE">
    for m in re.finditer(
        r'<a\s[^>]*href=["\'](/watch/(-?\d+_\d+))["\'][^>]*(?:title=["\'](.*?)["\'])?[^>]*>',
        html, re.IGNORECASE | re.DOTALL
    ):
        path = m.group(1)
        vid_id = m.group(2)
        title = m.group(3) or vid_id

        if vid_id in seen:
            continue
        seen.add(vid_id)

        url = BASE_URL + path
        slug = f"mat6tube-{vid_id}"
        items.append({"slug": slug, "url": url, "title": title})

    # Fallback: just find href="/watch/ID" without title
    if not items:
        for m in _WATCH_LINK_RE.finditer(html):
            vid_id = m.group(2)
            if vid_id in seen:
                continue
            seen.add(vid_id)
            url = BASE_URL + m.group(1)
            slug = f"mat6tube-{vid_id}"
            items.append({"slug": slug, "url": url, "title": vid_id})

    return items


def _estimate_total_pages(html: str, page: int) -> int:
    """Try to extract total video count from page, estimate total pages."""
    m = _TOTAL_RE.search(html)
    if m:
        try:
            total = int(m.group(1).replace(",", ""))
            return max(1, -(-total // PER_PAGE))  # ceiling division
        except (ValueError, TypeError):
            pass
    # Check if there's a "next page" link
    if re.search(r'[?&/]page[=/](\d+)', html):
        return page + 1
    return page


def _search_url(query: str, page: int) -> str:
    # Confirmed live pattern: /video/<query> with spaces as a literal
    # %20 — NOT /search/<query>/ (that 404s; same restructuring that
    # broke /new/, see get_latest_videos()'s docstring). Also doubles as
    # the model/performer lookup — no confirmed separate /models/<slug>/
    # page exists on the current site, this is the only confirmed way to
    # look someone up.
    q = quote(query.strip(), safe="")
    base = f"{BASE_URL}/video/{q}"
    return base if page <= 1 else f"{base}?page={page}"


async def get_model_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    """
    term: performer name, tag, or any keyword.
    Same contract as eporner_scraper.get_model_page_videos().

    Used to try a dedicated /models/<slug>/ page first, falling back to
    /search/<query>/ — both turned out to be wrong/404 (see _model_url's
    and _search_url's comments), and now both point at the same
    confirmed /video/<query> endpoint, so there's nothing left to
    meaningfully fall back to; this is just the one real fetch.
    """
    import asyncio

    def _fetch_model():
        html = _fetch(_search_url(term, page))
        if not html:
            return [], 1
        items = _parse_videos(html)
        total_pages = _estimate_total_pages(html, page)
        return items, total_pages

    return await asyncio.to_thread(_fetch_model)


async def get_random_page_videos() -> list:
    """
    Approximates random by picking a random search term + random page.
    Same contract as eporner_scraper.get_random_page_videos().
    """
    import asyncio

    def _fetch_random():
        term = random.choice(_BROAD_TERMS)
        page = random.randint(1, 10)
        html = _fetch(_search_url(term, page))
        if not html:
            return []
        return _parse_videos(html)

    return await asyncio.to_thread(_fetch_random)


async def get_latest_videos() -> list:
    """
    Polls for recently uploaded videos. Same contract as
    eporner_scraper.get_latest_videos().

    Tries several candidate "recent videos" paths on both the primary
    domain and the noodlemagazine.com mirror — a single hardcoded path
    was a single point of failure if the site ever restructured its
    URLs (confirmed live: the old /new/ path 404s; /recent is the
    confirmed-working one). Stops at the first candidate that actually
    returns videos.
    """
    import asyncio

    candidates = [
        f"{BASE_URL}/recent",
        f"{BASE_URL}/",
        f"{_FALLBACK_BASE_URL}/recent",
        f"{_FALLBACK_BASE_URL}/",
    ]

    def _fetch_latest():
        last_status = None
        last_snippet = None
        for url in candidates:
            try:
                r = requests.get(url, headers=_HEADERS, timeout=20)
                last_status = r.status_code
                if r.status_code >= 400:
                    continue
                items = _parse_videos(r.text)
                if items:
                    return items, None
                last_snippet = r.text[:200].replace("\n", " ")
            except Exception as e:
                last_status = f"error: {e}"
        # Every candidate came back empty/failed — surface exactly what
        # the last attempt actually returned (status + a snippet) instead
        # of just guessing at a cause, so the real reason (block page,
        # different markup, genuinely nothing new) is visible in one log
        # line rather than needing a live re-check to find out.
        return [], f"last status={last_status!r} snippet={last_snippet!r}"

    items, diag = await asyncio.to_thread(_fetch_latest)
    if not items and diag:
        logger.warning(f"mat6tube get_latest_videos: all candidate paths empty — {diag}")
    return items
