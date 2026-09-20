"""
eporner.com auto-scraper backend for auto_scraper.py's eporner_uploader_worker.

Uses eporner's own official, public, keyless "Webmasters API"
(https://www.eporner.com/api/v2/video/search/) instead of scraping HTML —
confirmed live and documented at https://github.com/eporner/API (and
still working per a recent GitHub issue showing a real example query
against it). No account/API key needed: it's the same endpoint eporner
publishes for other sites to list/embed their videos with. The
official API only has three methods (search / id / removed) — no
separate tag or studio/model navigation endpoint — so "model_name" in
auto_scraper.py's eporner_uploader_worker really just means "whatever
keyword string was typed after /autoupload eporner <term>"; a performer
name, a tag, anything — it's all the same `query` parameter here.

Actual video download/upload goes through ytdlp_downloader.py (eporner
already has a dedicated yt-dlp extractor — see that module's docstring);
this module only ever needs to produce a list of {"slug", "url", "title"}
for auto_scraper.py to hand off, exactly like faphouse_downloader's
get_page_videos()/get_page_videos_all_sites() do for the faphouse side.

Function contract auto_scraper.py's eporner_uploader_worker expects:
    get_model_page_videos(term, page=1) -> (list[{"slug","url","title"}], total_pages)
    get_random_page_videos() -> list[{"slug","url","title"}]
"""

import logging
import random

import aiohttp

logger = logging.getLogger(__name__)

SEARCH_URL = "https://www.eporner.com/api/v2/video/search/"
PER_PAGE = 30
_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

# Empty/whitespace query isn't a documented way to mean "everything" on a
# search-only API — safer to rotate through broad, generic terms for
# "random" mode instead of relying on undocumented empty-query behavior.
_BROAD_TERMS = [
    "amateur", "teen", "milf", "asian", "latina", "ebony", "blonde",
    "brunette", "anal", "lesbian", "hardcore", "big-tits", "creampie",
    "pov", "public", "college", "homemade", "solo", "threesome", "mature",
]
_ORDERS = ["latest", "top-weekly", "top-monthly", "most-popular", "longest"]

_logged_unexpected_shape = False  # log the raw response keys only once


def _slug_for(entry: dict) -> str | None:
    vid = entry.get("id")
    return f"eporner-{vid}" if vid else None


def _item_from_api(entry: dict) -> dict | None:
    slug = _slug_for(entry)
    url = entry.get("url")
    if not slug or not url:
        return None
    return {"slug": slug, "url": url, "title": entry.get("title") or slug}


async def _search(query: str, page: int = 1, order: str = "latest") -> tuple[list, int]:
    global _logged_unexpected_shape
    params = {
        "query": query,
        "per_page": str(PER_PAGE),
        "page": str(page),
        "thumbsize": "medium",
        "order": order,
        "gay": "0",
        "lq": "0",
        "format": "json",
    }
    headers = {"User-Agent": _UA}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.get(SEARCH_URL, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                raise RuntimeError(f"eporner search API returned HTTP {resp.status}")
            # format=json is requested explicitly, but don't fail parsing
            # over a mismatched/missing response Content-Type header.
            data = await resp.json(content_type=None)

    videos = data.get("videos")
    if videos is None and not _logged_unexpected_shape:
        # The API responding but not with a "videos" list at all (a
        # renamed field, a different error shape, etc.) is exactly the
        # kind of thing that's cheap to log once and expensive to debug
        # blind later — same reasoning as faphouse_downloader's saved
        # debug-HTML-on-failure approach elsewhere in this project.
        _logged_unexpected_shape = True
        logger.warning(f"eporner API response has no 'videos' key — top-level keys were: {list(data.keys())}")
    videos = videos or []

    items = [it for it in (_item_from_api(v) for v in videos) if it]

    total_count = data.get("total_count") or data.get("count") or 0
    try:
        total_count = int(total_count)
    except (TypeError, ValueError):
        total_count = 0
    if total_count:
        total_pages = max(1, -(-total_count // PER_PAGE))  # ceil division
    else:
        # No usable count field — conservatively assume there's a next
        # page only if this one came back full; caller stops as soon as
        # an empty page shows up regardless, so this only affects how
        # many pages get offered before that natural stop.
        total_pages = page + 1 if len(videos) >= PER_PAGE else page

    return items, total_pages


async def get_model_page_videos(term: str, page: int = 1) -> tuple[list, int]:
    """term: any keyword — performer name, tag, whatever. See module
    docstring for why there's no separate tag/model-specific endpoint."""
    return await _search(term.strip(), page=page)


async def get_random_page_videos() -> list:
    """No dedicated 'random' endpoint exists on the official API — this
    approximates one by combining a random broad search term, a random
    sort order, and a random page number each call, so repeated calls
    surface different videos instead of the same handful every time."""
    term = random.choice(_BROAD_TERMS)
    order = random.choice(_ORDERS)
    page = random.randint(1, 20)
    items, _ = await _search(term, page=page, order=order)
    return items


async def get_latest_videos(num_terms: int = 3) -> list:
    """Used by eporner_live_monitor for a "what's new" style check. There's
    no true site-wide "latest across everything" call on the official
    API (search always needs a query) — this approximates it by checking
    page 1, order=latest, for a handful of broad terms each cycle
    (num_terms of _BROAD_TERMS, rotated so a full pass through the list
    happens over several cycles rather than hammering all of them every
    time), merged and de-duplicated by slug. This is deliberately a
    partial slice of the site, not a true firehose — see
    EPORNER_MONITOR_MAX_PER_CYCLE in config.py for the other half of
    keeping this from flooding a channel with the sheer volume eporner
    actually publishes."""
    terms = random.sample(_BROAD_TERMS, min(num_terms, len(_BROAD_TERMS)))
    merged = {}
    for term in terms:
        try:
            items, _ = await _search(term, page=1, order="latest")
        except Exception as e:
            logger.warning(f"eporner get_latest_videos: term {term!r} failed: {e}")
            continue
        for it in items:
            merged.setdefault(it["slug"], it)
    return list(merged.values())
