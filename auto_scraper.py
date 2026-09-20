"""
Auto-scraper / auto-uploader for faphouse.com's public /videos listing.

Ported and adapted from the FAPHOUSE2-main reference project (helpers/
scraper.py + helpers/worker.py), rewired to use fbot's own faphouse_downloader
engine (session/login + ffmpeg) instead of that project's separate Vercel
API + aiohttp-based extractor, and fbot's own database.py connection
instead of a second MongoDB client.

Two independent things live here:
  - A per-chat worker (/autoupload): pages through /videos, uploading every
    not-yet-seen video to that chat, resumable across restarts via DB state.
  - A 24/7 live monitor (optional, DEFAULT_CHANNEL): watches page 1 only,
    for brand-new releases, and pushes them straight to one channel.

Both rely on `is_video_uploaded`/`save_uploaded_video` (database.py) as the
single source of truth for "already posted", so the two never re-upload
the same video, and a chat worker restarting from a stale page just skips
straight through anything the monitor already grabbed.
"""

import asyncio
import contextlib
import hashlib
import html
import logging
import os
import re
import shutil
import subprocess
import time
import urllib.parse

import aiohttp
from bs4 import BeautifulSoup
from pyrogram import Client, enums
from pyrogram.types import Message
from pyrogram.errors import PeerIdInvalid, ChannelInvalid, FloodWait

ParseMode = enums.ParseMode

from config import SITE_URL, USER_AGENT, DOWNLOAD_DIR, MAX_FILE_SIZE, SPLIT_PART_TARGET_BYTES, MONITOR_INTERVAL, AUTO_UPLOAD_COOLDOWN, OWNER_ID
from database import (
    is_video_uploaded, save_uploaded_video,
    get_chat_scraper_state, set_chat_scraper_state, get_all_active_scraper_states,
    get_skipped_size_limit_videos, get_failed_videos, delete_uploaded_video, get_uploaded_video_status,
    is_title_duplicate,
)
import faphouse_downloader as faphouse
import ytdlp_downloader as ytdlp
import fpo_downloader as fpo
import eporner_scraper
import mat6tube_scraper
import pornhub_scraper
import xhamster_scraper
import xvideos_scraper
import jav_scraper
import split_upload

logger = logging.getLogger(__name__)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Scrape both faphouse.com and faphouse2.com — they're mirror sites with
# overlapping catalogs, and a video posted to one often isn't (yet, or
# ever) on the other. SITE_URL is kept as the default single-site value
# (used elsewhere as a fallback) but listing scans check both.
SCRAPE_SITES = sorted(set(faphouse.BASE_URLS.values())) or [SITE_URL]

# Separate upload locks per source — FapHouse and Eporner each get their
# own lock so both monitors can upload simultaneously without blocking each
# other. A single shared lock meant starting a FapHouse upload froze the
# Eporner monitor (and vice-versa) for the entire duration of that upload.
# Within each source, one upload at a time still applies (same bandwidth
# argument as before — two concurrent uploads from the same source would
# just thrash each other).
_upload_lock = asyncio.Lock()          # FapHouse / manual downloads
_eporner_upload_lock = asyncio.Lock()  # Eporner monitor
# Keyed by (chat_id, site) instead of just chat_id — one worker per
# (chat, site) pair runs independently, so starting e.g. /autouploadxhamster
# in a chat that already has /autouploadeporner running there no longer
# stops the eporner one. "site" here means whichever tag each
# start_*_worker_task below registers under; see each one's own tag for
# exactly what does/doesn't share a slot with what.
_active_workers: dict[tuple[int, str], asyncio.Task] = {}
_active_retry_task: asyncio.Task | None = None


async def stop_worker_task(chat_id: int, site: str = None, timeout: float = 20.0) -> None:
    """Actually stops the running worker(s) for this chat, instead of only
    flipping the DB "is_running" flag and hoping the worker notices soon.

    site=None (the /stopupload default) stops EVERY site currently
    running in this chat — every (chat_id, *) slot. Pass a specific site
    (e.g. site="xhamster") to only stop that one, leaving any other
    site's worker in the same chat untouched — this is what each
    /autoupload<site>_cmd handler does before starting its own worker,
    so starting one site no longer stops a different site already
    running in the same chat.

    Without this, /stopupload just set is_running=False and returned right
    away — but chat_uploader_worker/actor/category workers only check that
    flag *between* videos (or every ~2s while polling download/upload
    progress). If the worker happened to be mid-download/upload of the
    current video when /stopupload ran, it wouldn't actually exit — and
    _active_workers wouldn't be cleared — until that video finished, which
    can take minutes for a large file. Any /autoupload sent in that window
    then wrongly reports "Already running in this chat", even though the
    person was just told it had stopped.

    Cancelling the task(s) and awaiting them here means /stopupload's reply
    only goes out once the worker (and its _active_workers entry) is
    actually gone, so the very next /autoupload is guaranteed a clean
    start."""
    if site is not None:
        keys = [(chat_id, site)] if (chat_id, site) in _active_workers else []
    else:
        keys = [k for k in _active_workers if k[0] == chat_id]

    tasks = [(k, _active_workers[k]) for k in keys if not _active_workers[k].done()]
    if not tasks:
        for k in keys:
            _active_workers.pop(k, None)
        return

    for _, task in tasks:
        task.cancel()
    for key, task in tasks:
        try:
            await asyncio.wait_for(task, timeout=timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError, Exception):
            # CancelledError is expected (that's what we asked for); a
            # TimeoutError or anything else just means it's taking unusually
            # long (e.g. ffmpeg won't respond to cancellation mid-segment) —
            # either way, force the slot free below so a restart isn't blocked
            # on it forever.
            pass
        _active_workers.pop(key, None)


def _any_manual_job_running(chat_id: int) -> asyncio.Task | None:
    """Used by each live_monitor (faphouse/eporner/xhamster/xvideos) to
    check "is ANY manual /autoupload<site> job currently running in this
    channel, regardless of which site" — so the automatic monitor still
    pauses for a manual job on a different site too, not just one on its
    own site. Returns the first still-running task found, or None."""
    for (cid, _site), task in _active_workers.items():
        if cid == chat_id and not task.done():
            return task
    return None


# Set once at startup via configure_caption_builder() (main.py calls it
# right before idle()) — lets process_and_upload_video build the exact
# same detailed caption, and schedule the exact same real auto-delete, as
# the manual /download flow uses. A plain module-level import would create
# a circular import (main.py already imports this module), so main.py
# hands the functions over instead.
_build_caption = None
_schedule_delete = None
_build_stream_markup = None


def configure_caption_builder(build_caption_fn, schedule_delete_fn, build_stream_markup_fn=None) -> None:
    global _build_caption, _schedule_delete, _build_stream_markup
    _build_caption = build_caption_fn
    _schedule_delete = schedule_delete_fn
    _build_stream_markup = build_stream_markup_fn


def _human_size(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB"]:
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} TB"


def _human_duration(seconds: int) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _human_speed(n: float) -> str:
    for unit in ["B/s", "KB/s", "MB/s", "GB/s"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB/s"


def _human_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def _progress_bar(pct: float, width: int = 10) -> str:
    filled = min(width, int(width * pct / 100))
    return "⬢" * filled + "⬡" * (width - filled)


def _format_duration_mmss(seconds) -> str:
    try:
        seconds = int(seconds)
    except (TypeError, ValueError):
        return None
    if seconds <= 0:
        return None
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m}:{s:02d}"


class _AutoProgressTracker:
    """Throttled Telegram status-message updater for the auto-uploader's
    download/upload progress — same look as the manual /download flow's
    ProgressTracker in main.py, kept as its own copy here to avoid a
    circular import (main.py already imports this module)."""

    def __init__(self, status_msg: Message, label: str, name: str, interval: float = 5.0,
                 quality: str = None, duration: str = None):
        """
        FIX: interval default 2.0 → 5.0 seconds.
        Telegram rate-limits messages.EditMessage to roughly 1 edit per
        message per 3-5 seconds. At 2.0s we hit FloodWait errors (logs
        showed "Waiting for 3-17 seconds before continuing") which stall
        the entire bot event loop. 5.0s stays safely under the limit.
        """
        self.status_msg = status_msg
        self.label = label
        self.name = name
        self.interval = interval
        self.quality = quality
        self.duration = duration
        self.start_time = time.time()
        self.last_edit_time = 0.0

    async def update(self, current: int, total: int):
        if not self.status_msg:
            return
        now = time.time()
        is_done = total and current >= total
        if not is_done and (now - self.last_edit_time) < self.interval:
            return
        self.last_edit_time = now

        elapsed = now - self.start_time
        speed = current / elapsed if elapsed > 0 else 0
        pct = (current / total * 100) if total else 0
        eta = (total - current) / speed if speed > 0 and total else 0

        is_download = "download" in self.label.lower()
        emoji = "📥" if is_download else "📤"
        title_verb = "Downloading" if is_download else "Upload"
        connections_word = "download" if is_download else "upload"
        duration_line = f"┣⪼ ⏱ Duration: {self.duration}\n" if self.duration else ""
        quality_line = f"┣⪼ 🎞 Quality: {self.quality}\n" if self.quality else ""

        try:
            await self.status_msg.edit_text(
                f"{emoji} <b>Fast {title_verb} via Main Engine</b>\n\n"
                "╭━━━━❰Progress❱━➣\n"
                f"┣⪼ 🎬 File: <code>{self.name}</code>\n"
                f"{duration_line}"
                f"{quality_line}"
                f"┣⪼ [{_progress_bar(pct)}]\n"
                f"┣⪼ ✅ {pct:.1f}%\n"
                f"┣⪼ 💾 {_human_size(current)} / {_human_size(total)}\n"
                f"┣⪼ ⚡ {_human_speed(speed)}\n"
                f"┣⪼ 🕐 Elapsed: {_human_time(elapsed)}\n"
                f"┣⪼ ⏳ ETA: {_human_time(eta)}\n"
                "╰━━━━━━━━━━━━━━━➣\n\n"
                f"⚡ Hyper {connections_word} connections active",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


def extract_slug(url: str) -> str:
    match = re.search(r'/videos/([^/?#]+)', url)  # faphouse.com / faphouse2.com
    if match:
        return match.group(1).strip()
    match = re.search(r'/video-([A-Za-z0-9]+)/', url)  # eporner.com
    if match:
        return f"eporner-{match.group(1)}"
    match = re.search(r'/video/(\d+)/', url)  # fpo.xxx  /video/123456/title/
    if match:
        return f"fpo-{match.group(1)}"
    match = re.search(r'/watch/(-?\d+_\d+)', url)  # mat6tube.com /watch/-<id>_<id>
    if match:
        return f"mat6tube-{match.group(1)}"
    # BUG FIX: falling through to the raw URL here (any site not matched
    # above) meant process_and_upload_video()'s work_dir/out_path — built
    # from this slug — contained "://" and "/" from the URL itself,
    # which the filesystem reads as actual path separators instead of
    # part of a folder name. Confirmed live: every mat6tube download
    # failed with "No such file or directory" for exactly this reason,
    # since mat6tube had no pattern here and always hit this fallback.
    # A short, deterministic hash keeps this safe for any future/unlisted
    # site the same way, instead of only being fixed for mat6tube.
    return f"video-{hashlib.sha256(url.strip().encode('utf-8')).hexdigest()[:16]}"


def _normalize_title(title: str) -> str:
    """Strips punctuation/site-suffix noise and collapses whitespace/case
    so the same clip re-listed under a different slug (a common mirror-
    site and re-upload pattern) still produces the same hash even if one
    copy's title has slightly different capitalization, dashes, or a
    trailing "- Faphouse"/quality tag the other doesn't."""
    if not title:
        return ""
    t = title.lower()
    t = re.sub(r'\b(faphouse2?|hd|full hd|4k|1080p|720p)\b', ' ', t)
    t = re.sub(r'[^a-z0-9]+', ' ', t)
    return re.sub(r'\s+', ' ', t).strip()


def _compute_title_hash(title: str) -> str | None:
    """Content-fingerprint for pre-download duplicate detection: the video
    itself isn't downloaded yet at the point this is needed (that's the
    whole point — skip the multi-GB download for a video we already have
    under a different slug), so the normalized page title is the only
    cheap signal available this early. Returns None for an empty/missing
    title rather than hashing an empty string, which would otherwise
    false-positive-match every other video with no title."""
    normalized = _normalize_title(title)
    if not normalized:
        return None
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _sanitize_filename(name: str) -> str:
    if not name:
        return "faphouse_video"
    clean = re.sub(r'[\\/*?:"<>|]', "", name)
    clean = re.sub(r'\s+', " ", clean).strip()
    return clean[:120] or "faphouse_video"


# Section headings/text that mark the end of a page's "own" video listing
# and the start of unrelated recommendations — an actor page's own videos
# come first, then one of these; the main /videos listing doesn't have
# this problem, but truncating there too is harmless (these phrases never
# appear as real video titles/content).
_LISTING_BOUNDARY_MARKERS = [
    "Trending Porn Videos", "Trending Videos", "Trending Now",
    "Related creators", "Related Creators", "Related pornstars", "Related Pornstars",
    "Related Videos", "Related Categories",
    "You may also like", "You've seen all videos of",
    "Suggested for you", "Suggested Videos", "Suggested Creators",
    "More from", "Popular Pornstars", "Popular Creators", "Popular Categories",
]


def _truncate_at_listing_boundary(html_text: str) -> str:
    earliest = len(html_text)
    for marker in _LISTING_BOUNDARY_MARKERS:
        idx = html_text.find(marker)
        if idx != -1 and idx < earliest:
            earliest = idx
    return html_text[:earliest]


async def get_page_videos(page: int = 1, base_url: str = None, path: str = "/videos") -> list:
    """Scrapes one listing page (on a single site) for video links. `path`
    defaults to the main /videos listing but can point at any other listing
    page on the same site (e.g. an actor/pornstar page) — the video-link
    extraction below only looks for /videos/ hrefs on the page, so it works
    the same regardless of which listing page it's scraping.

    BUG FIX: this used to fetch with a bare aiohttp GET (no session, no
    Cloudflare-challenge handling) even though every base_url here is a
    faphouse.com/faphouse2.com domain — the exact site
    faphouse_downloader.AkClient's authenticated session and
    _looks_like_challenge_page() exist specifically to handle (see that
    module's docstring, and _fetch_page_title() below which already used
    that session for actor-page lookups, just never for THIS, the plain
    /videos listing every /autoupload run actually depends on). A bare
    GET can come back 200 with a Cloudflare interstitial page instead of
    the real listing — no error, just zero /videos/ hrefs found — which
    looks exactly like "/autoupload finds nothing" with no visible cause.
    """
    base_url = base_url or SITE_URL
    page_url = f"{base_url}{path}?page={page}" if page > 1 else f"{base_url}{path}"

    def _sync_fetch() -> str | None:
        try:
            sess = faphouse.client.ensure_session(base_url)
            r = sess.get(page_url, timeout=15, headers={"User-Agent": faphouse._UA, "Referer": base_url})
            if r.status_code != 200:
                logger.warning(f"[scraper] {page_url} returned HTTP {r.status_code}")
                return None
            html_text = faphouse.client._decode_response(r)
            if faphouse._looks_like_challenge_page(html_text):
                logger.warning(f"[scraper] {page_url} looks like a Cloudflare challenge page, not the real listing")
                return None
            return html_text
        except Exception as e:
            logger.warning(f"[scraper] {page_url} fetch failed: {e}")
            return None

    html_text = await asyncio.to_thread(_sync_fetch)
    if not html_text:
        return []

    # An actor/pornstar (or similar profile-style) page lists that
    # performer's own videos FIRST, then follows with "Trending Porn
    # Videos" / "Related creators" / "Related pornstars" sections full of
    # totally unrelated content. Scraping every <a> tag on the whole page
    # with no boundary meant those sections' videos leaked into what was
    # supposed to be a single-performer scrape — confirmed root cause of
    # unrelated/"unknown" videos showing up during actor auto-upload.
    # Truncating the raw HTML at the first such marker (before it's even
    # parsed) keeps extraction strictly to the performer's own section,
    # regardless of the page's DOM structure.
    html_text = _truncate_at_listing_boundary(html_text)

    soup = BeautifulSoup(html_text, "html.parser")
    items = []
    seen = set()
    skip_slugs = {"vr", "latest", "top", "popular", "trending"}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/videos/" not in href:
            continue
        slug = extract_slug(href)
        if not slug or slug in skip_slugs or href.rstrip("/").endswith("/videos"):
            continue
        if slug in seen:
            continue
        seen.add(slug)
        items.append({"slug": slug, "url": urllib.parse.urljoin(base_url, href)})
    return items


async def get_page_videos_all_sites(page: int = 1, path: str = "/videos") -> list:
    """Same as get_page_videos, but checks faphouse.com AND faphouse2.com
    for this page number and merges the results, de-duplicated by slug —
    the two are mirror sites with overlapping but not identical catalogs,
    so a video missing from one page might still be caught on the other."""
    merged = {}
    for site in SCRAPE_SITES:
        for item in await get_page_videos(page=page, base_url=site, path=path):
            merged.setdefault(item["slug"], item)
    return list(merged.values())


# ---------------------------------------------------------------------
# Actor/pornstar page auto-upload — "/autoupload <name>" instead of the
# plain site-wide "/autoupload". Faphouse doesn't publish a documented API
# for these performer pages, so instead of hardcoding one guessed URL
# shape (which could just be wrong and silently upload nothing), this
# probes a handful of URL layouts common to tube sites and keeps whichever
# one actually returns video links on its first page.
# ---------------------------------------------------------------------

ACTOR_PATH_PATTERNS = [
    "/pornstars/{slug}",
    "/pornstar/{slug}",
    "/models/{slug}",
    "/model/{slug}",
    "/actors/{slug}",
    "/actor/{slug}",
    "/performers/{slug}",
    "/performer/{slug}",
    "/stars/{slug}",
    "/star/{slug}",
    "/girls/{slug}",
    "/girl/{slug}",
    "/profile/{slug}",
]


def slugify_name(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower())
    return slug.strip("-")


async def _fetch_page_title(base_url: str, path: str) -> str:
    """Fetch the <title> of an actor/listing page for name-matching validation.
    Uses faphouse's authenticated session for faphouse.com/faphouse2.com URLs
    so Cloudflare challenge pages don't return a wrong title like 'Just a moment...'
    and cause valid actor pages to be incorrectly skipped."""
    url = f"{base_url}{path}"
    # For FapHouse domains use the authenticated requests session (sync → thread)
    if faphouse.is_faphouse_link(url) or any(d in base_url for d in ("faphouse.com", "faphouse2.com")):
        def _sync_fetch():
            try:
                sess = faphouse.client.ensure_session(base_url)
                r = sess.get(url, timeout=10, headers={"User-Agent": faphouse._UA, "Referer": base_url})
                if r.status_code != 200:
                    return ""
                html = faphouse.client._decode_response(r)
                if faphouse._looks_like_challenge_page(html):
                    return ""
                m = re.search(r"<title[^>]*>([^<]+)</title>", html, re.I)
                return m.group(1).strip() if m else ""
            except Exception:
                return ""
        return await asyncio.to_thread(_sync_fetch)
    # Non-faphouse: plain aiohttp (no auth needed for public listing pages)
    headers = {"User-Agent": USER_AGENT}
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return ""
                html_text = await resp.text()
        m = re.search(r"<title[^>]*>([^<]+)</title>", html_text, re.I)
        return m.group(1).strip() if m else ""
    except Exception:
        return ""


def _name_tokens_match(name: str, title: str) -> bool:
    """True if ANY significant token of name shows up in title — loosened
    from ALL-tokens to ANY-token so single-word stage names like 'Comatozze'
    don't get rejected when the page title has minor capitalization/accent
    differences, OR if the performer's name is only partially in the title
    (e.g. first name only). Still rejects completely unrelated pages."""
    if not title:
        return False
    title_lower = title.lower()
    tokens = [t for t in re.split(r"[^a-z0-9]+", name.lower()) if len(t) > 1]
    if not tokens:
        return False
    # Single-word name: must match (no choice but to require it)
    if len(tokens) == 1:
        return tokens[0] in title_lower
    # Multi-word name: any token match is enough
    return any(t in title_lower for t in tokens)


async def _search_actor_path(base_url: str, name: str) -> str | None:
    """Last-resort: try faphouse's own search page to find the performer's
    actual slug — probes /search?q=<name> and looks for a /pornstars/ or
    /models/ link in the results that matches the name."""
    headers = {"User-Agent": USER_AGENT}
    search_url = f"{base_url}/search?q={urllib.parse.quote(name)}"
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(search_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status != 200:
                    return None
                html_text = await resp.text()
    except Exception:
        return None

    soup = BeautifulSoup(html_text, "html.parser")
    slug_lower = slugify_name(name)
    performer_patterns = re.compile(
        r"/(pornstars?|models?|actors?|performers?|stars?|girls?|profile)/([^/?#\"']+)",
        re.I
    )
    for a in soup.find_all("a", href=True):
        m = performer_patterns.search(a["href"])
        if not m:
            continue
        found_slug = m.group(2).strip("/").lower()
        # slug match ya partial name match
        if slug_lower in found_slug or found_slug in slug_lower:
            path = f"/{m.group(1)}/{m.group(2).strip('/')}"
            # Verify this path has videos
            items = await get_page_videos(page=1, base_url=base_url, path=path)
            if items:
                logger.info(f"[scraper] search fallback found: {base_url}{path}")
                return path
    return None


async def discover_actor_paths(name: str) -> dict:
    """Returns {base_url: path} for every mirror site where a matching
    performer-page URL was found. Tries ACTOR_PATH_PATTERNS first, then
    falls back to faphouse's own search page."""
    slug = slugify_name(name)
    if not slug:
        return {}

    async def _probe(base_url: str, pattern: str):
        path = pattern.format(slug=slug)
        try:
            items = await get_page_videos(page=1, base_url=base_url, path=path)
        except Exception as e:
            logger.warning(f"[scraper] actor-page probe {base_url}{path} failed: {e}")
            items = []
        return path, items

    async def _discover_for_site(base_url: str):
        # ── Pattern probing — CONCURRENT, not sequential ──
        # Was: one `await get_page_videos(...)` per pattern in a for loop,
        # so a slow/blocking site meant all 13 ACTOR_PATH_PATTERNS each
        # waited out their own timeout ONE AT A TIME — worst case ~13*15s
        # ≈ 3.25 minutes for this site alone, ~6.5 minutes across both
        # faphouse.com/faphouse2.com. From the user's side this just
        # looked like "/autoupload <name> doesn't work" — the status
        # message just sat at "Looking up..." the entire time this loop
        # ran, with no way to tell a genuinely slow lookup apart from a
        # hung one. gather() runs every pattern for this site at once, so
        # the wait is roughly one timeout period total, not thirteen —
        # and since gather() preserves input order in its results, the
        # first-matching-pattern-wins logic below still behaves exactly
        # like the sequential version did, just much faster.
        results = await asyncio.gather(*[_probe(base_url, p) for p in ACTOR_PATH_PATTERNS])
        for path, items in results:
            if not items:
                continue
            title = await _fetch_page_title(base_url, path)
            if title and not _name_tokens_match(name, title):
                logger.warning(
                    f"[scraper] {base_url}{path} title {title!r} doesn't match "
                    f"\"{name}\" — skipping as likely generic/fallback page."
                )
                continue
            logger.info(f"[scraper] actor page found via pattern: {base_url}{path}")
            return base_url, path

        # ── Search fallback (if pattern probing found nothing for this site) ──
        search_path = await _search_actor_path(base_url, name)
        if search_path:
            return base_url, search_path
        return base_url, None

    # ── Sites probed CONCURRENTLY too, not just patterns within a site ──
    # Was: `for base_url in SCRAPE_SITES: await _discover_for_site(...)`
    # sequential — so with multiple mirror sites configured, one site's
    # full probe+title+search-fallback time (tens of seconds worst case)
    # stacked on top of every other site's, one at a time. That's exactly
    # the "Looking up..." status message sitting there for a long time
    # with nothing telling the user it was still working, same failure
    # mode the per-pattern gather() above already fixed within one site —
    # this closes the same gap one level up.
    site_results = await asyncio.gather(*[_discover_for_site(b) for b in SCRAPE_SITES])
    return {base_url: path for base_url, path in site_results if path}


# ---------------------------------------------------------------------
# Category/tag page auto-upload — "/autouploadtag <name>" instead of the
# plain site-wide "/autoupload". Same probing strategy as the actor-page
# discovery above, since faphouse doesn't publish a documented API for
# these either: try a handful of common tube-site URL shapes and keep
# whichever one actually returns video links on its first page.
# ---------------------------------------------------------------------

CATEGORY_PATH_PATTERNS = [
    "/categories/{slug}",
    "/category/{slug}",
    "/tags/{slug}",
    "/tag/{slug}",
    "/genres/{slug}",
    "/genre/{slug}",
    "/niches/{slug}",
    "/niche/{slug}",
]


async def discover_category_paths(tag_name: str) -> dict:
    """Same idea as discover_actor_paths, but probes CATEGORY_PATH_PATTERNS
    (categories/tags/genres/niches) instead of performer-page patterns —
    returns {base_url: path} for every mirror site where a matching
    category/tag page was found, e.g. {"https://faphouse.com":
    "/categories/milf"}."""
    slug = slugify_name(tag_name)
    if not slug:
        return {}
    found = {}

    async def _probe(base_url: str, pattern: str):
        path = pattern.format(slug=slug)
        try:
            items = await get_page_videos(page=1, base_url=base_url, path=path)
        except Exception as e:
            logger.warning(f"[scraper] category-page probe {base_url}{path} failed: {e}")
            items = []
        return path, items

    for base_url in SCRAPE_SITES:
        # Same fix as discover_actor_paths above — probe all patterns for
        # this site concurrently instead of one at a time.
        results = await asyncio.gather(*[_probe(base_url, p) for p in CATEGORY_PATH_PATTERNS])
        for path, items in results:
            if items:
                found[base_url] = path
                break
    return found


# ---------------------------------------------------------------------
# Studio/production-company page auto-upload — "/autouploadstudio <name>",
# same probing strategy as actor/category discovery above.
# ---------------------------------------------------------------------

STUDIO_PATH_PATTERNS = [
    "/studios/{slug}",
    "/studio/{slug}",
    "/channels/{slug}",
    "/channel/{slug}",
    "/production/{slug}",
    "/producers/{slug}",
    "/producer/{slug}",
    "/networks/{slug}",
    "/network/{slug}",
]


async def discover_studio_paths(name: str) -> dict:
    """Same idea as discover_category_paths, but probes STUDIO_PATH_PATTERNS
    (studios/channels/production/networks) — returns {base_url: path} for
    every mirror site where a matching studio page was found, e.g.
    {"https://faphouse.com": "/studios/puretaboo"}.

    Tries both the normally-hyphenated slug and a hyphen-free "squashed"
    variant — studio/brand names are often stylized as one compound word
    (e.g. "PureTaboo" -> puretaboo, not pure-taboo), unlike performer
    names, which almost always have natural spaces that map correctly to
    hyphens."""
    slug = slugify_name(name)
    squashed = slug.replace("-", "")
    slug_candidates = [slug] if slug == squashed else [slug, squashed]
    if not slug:
        return {}
    found = {}

    async def _probe(base_url: str, candidate_slug: str, pattern: str):
        path = pattern.format(slug=candidate_slug)
        try:
            items = await get_page_videos(page=1, base_url=base_url, path=path)
        except Exception as e:
            logger.warning(f"[scraper] studio-page probe {base_url}{path} failed: {e}")
            items = []
        return path, items

    for base_url in SCRAPE_SITES:
        # Same fix as discover_actor_paths/discover_category_paths above,
        # just flattened across both loop variables (slug_candidates ×
        # STUDIO_PATH_PATTERNS) into one gather() per site — this one was
        # the worst offender, up to 2 candidates × 9 patterns = 18
        # sequential probes per site before even trying the second site.
        probes = [(cs, p) for cs in slug_candidates for p in STUDIO_PATH_PATTERNS]
        results = await asyncio.gather(*[_probe(base_url, cs, p) for cs, p in probes])
        for path, items in results:
            if items:
                found[base_url] = path
                break
    return found


async def get_listing_page_videos(paths: dict, page: int = 1) -> list:
    """Generic paginated-listing scraper for a {base_url: path} map — used
    for both actor pages (discover_actor_paths) and category/tag pages
    (discover_category_paths); the scraping itself doesn't care which kind
    of listing page it is, only get_page_videos' own /videos/ href-scan."""
    merged = {}
    for base_url, path in paths.items():
        for item in await get_page_videos(page=page, base_url=base_url, path=path):
            merged.setdefault(item["slug"], item)
    return list(merged.values())


async def get_latest_fresh_videos() -> list:
    return await get_page_videos_all_sites(page=1)


async def get_pending_videos_summary(sample_pages: int = 5, paths: dict = None) -> dict:
    """Scans the first few pages of a listing and reports how many videos
    on them haven't been uploaded yet — used by /pending. Scans the main
    site-wide /videos listing by default; pass `paths` (from
    discover_actor_paths/discover_category_paths) to scope the scan to one
    performer's or category's page instead."""
    all_videos = []
    for p in range(1, sample_pages + 1):
        page_items = await get_listing_page_videos(paths, page=p) if paths else await get_page_videos_all_sites(page=p)
        if not page_items:
            break
        for v in page_items:
            if not any(x["slug"] == v["slug"] for x in all_videos):
                all_videos.append(v)

    pending = [v for v in all_videos if not await is_video_uploaded(v["slug"])]
    return {
        "total_scanned": len(all_videos),
        "pending_count": len(pending),
        "scanned_pages": sample_pages,
        "sample_pending": pending[:5],
    }


async def _get_page_meta(video_url: str) -> dict:
    """Best-effort og:title / og:image from the video's own page — nicer
    filenames and a real poster thumbnail instead of a generic ffmpeg
    frame grab, when the page provides them.

    For FapHouse links, delegates to faphouse.get_page_meta() which uses
    the same authenticated requests.Session as M3U8 resolution — this
    avoids getting a Cloudflare challenge page back (status 200 but not
    the real video page) and storing "Just a moment..." as the title.

    For other sites, falls back to a plain aiohttp fetch but still checks
    for challenge-page markers before trusting any og:title it finds."""

    # ── FapHouse: use authenticated session via faphouse_downloader ──────────
    if faphouse.is_faphouse_link(video_url):
        try:
            return await asyncio.to_thread(faphouse.get_page_meta, video_url)
        except Exception as e:
            logger.warning(f"[scraper] faphouse.get_page_meta failed for {video_url}: {e}")
            return {"title": None, "poster_url": None}

    # ── FPO.XXX: use fpo_downloader.get_page_meta (embed page + flashvars) ───
    # Plain aiohttp fetch won't work here — fpo.xxx's video page embeds the
    # title inside the KVS player's flashvars JS block, not in og:title meta
    # tags that a plain GET would see. fpo.get_page_meta fetches the /embed/
    # page (same path get_available_qualities uses) and extracts the real title.
    if fpo.is_fpo_link(video_url):
        try:
            return await asyncio.to_thread(fpo.get_page_meta, video_url)
        except Exception as e:
            logger.warning(f"[scraper] fpo.get_page_meta failed for {video_url}: {e}")
            return {"title": None, "poster_url": None}

    # ── Other sites: plain aiohttp fetch with challenge-page guard ───────────
    headers = {"User-Agent": USER_AGENT}
    title, poster_url = None, None
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(video_url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                if resp.status == 200:
                    page_html = await resp.text()
                    # Don't trust a Cloudflare/anti-bot challenge page's title
                    if faphouse._looks_like_challenge_page(page_html):
                        logger.warning(
                            f"[scraper] challenge page in _get_page_meta for {video_url} — skipping title"
                        )
                        return {"title": None, "poster_url": None}
                    m = re.search(r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']', page_html, re.I)
                    if m:
                        title = m.group(1).strip()
                    m = re.search(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', page_html, re.I)
                    if m:
                        poster_url = m.group(1).strip()
    except Exception as e:
        logger.warning(f"[scraper] page meta fetch failed for {video_url}: {e}")
    return {"title": title, "poster_url": poster_url}


def _ffmpeg_thumbnail(video_path: str, thumb_path: str, seek_seconds: float = 3.0) -> bool:
    """A fixed 1s mark almost always lands on faphouse's own black intro/
    logo bumper — try further in first (still early, in case of short
    clips), falling back to 1s if that frame fails to extract."""
    for ts in (seek_seconds, 1.0):
        try:
            result = subprocess.run(
                ["ffmpeg", "-y", "-ss", str(ts), "-i", video_path, "-vframes", "1",
                 "-vf", "scale=320:-1", thumb_path],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=30,
            )
            if result.returncode == 0 and os.path.exists(thumb_path):
                return True
        except Exception:
            pass
    return False


AUTOUPLOAD_TARGET_HEIGHT = 720


def _pick_autoupload_stream_url(variants: list) -> str | None:
    """Picks the stream url autoupload should download, forcing every video
    to 720p instead of whatever "Auto (Best)" would resolve to (which is
    often 1080p/4K on faphouse).

    variants is get_available_qualities()'s output: explicit-resolution
    entries plus a trailing {"label": "Auto (Best)", "height": None, "url": None}.
    Picks the exact AUTOUPLOAD_TARGET_HEIGHT match if present; otherwise the
    closest resolution *below* it (e.g. 480p if only 480p/1080p exist, never
    upscaling to something above 720p); if the source has nothing at or
    below 720p (rare — an ultra-HD-only master playlist), falls back to the
    lowest available resolution rather than "Auto (Best)"'s full quality.
    Returns None (meaning "let download_video() auto-resolve") only if no
    explicit resolution variants were found at all.

    For FPO.xxx specifically: if only a single resolution is available
    (flashvars typically expose just one stream key), always take it
    regardless of whether it matches AUTOUPLOAD_TARGET_HEIGHT — skipping
    a video because it's only available at 1080p when we prefer 720p is
    worse than just uploading it at whatever quality exists."""
    resolutions = [v for v in variants if v.get("height")]
    if not resolutions:
        return None  # nothing but "Auto (Best)" — no choice to make

    # Single resolution only — take it unconditionally (common for FPO)
    if len(resolutions) == 1:
        return resolutions[0]["url"]

    exact = next((v for v in resolutions if v["height"] == AUTOUPLOAD_TARGET_HEIGHT), None)
    if exact:
        return exact["url"]

    at_or_below = [v for v in resolutions if v["height"] <= AUTOUPLOAD_TARGET_HEIGHT]
    if at_or_below:
        return max(at_or_below, key=lambda v: v["height"])["url"]

    # every available rendition is above 720p — take the smallest one on offer
    return min(resolutions, key=lambda v: v["height"])["url"]


def _download_poster(url: str, out_path: str) -> bool:
    try:
        import requests
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        with open(out_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception:
        return False


def _finalize_thumbnail(thumb_path: str) -> bool:
    """Normalize a site poster or ffmpeg frame grab to Telegram's actual
    thumb limits (JPEG, <=320x320, <200KB). Telegram silently rejects/
    mangles anything bigger, which is what made thumbnails inconsistent —
    fine for a small poster, blurry-fallback or missing for a large one.
    Downscale-only. Returns False if the file isn't a readable image, so
    the caller can fall back to the next source."""
    tmp_path = thumb_path + ".fix.jpg"
    try:
        for q in (2, 4, 8, 12, 16, 20, 24, 28, 31):  # ffmpeg mjpeg scale: lower = better quality
            result = subprocess.run(
                [
                    "ffmpeg", "-y", "-i", thumb_path,
                    "-vf", "scale='min(320,iw)':'min(320,ih)':force_original_aspect_ratio=decrease",
                    "-vframes", "1",
                    "-q:v", str(q),
                    tmp_path,
                ],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=20,
            )
            if result.returncode != 0 or not os.path.exists(tmp_path):
                return False
            if os.path.getsize(tmp_path) <= 200 * 1024:
                os.replace(tmp_path, thumb_path)
                return True
        os.replace(tmp_path, thumb_path)
        return os.path.getsize(thumb_path) > 0
    except Exception as e:
        logger.warning(f"[scraper] thumbnail finalize failed: {e}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        return False


def _ffprobe_metadata(video_path: str):
    try:
        import json
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height:format=duration",
             "-of", "json", video_path],
            capture_output=True, text=True, timeout=30,
        )
        data = json.loads(result.stdout)
        stream = data["streams"][0]
        width = int(stream.get("width") or 0) or None
        height = int(stream.get("height") or 0) or None
        duration = int(float(data["format"]["duration"]))
        return duration, width, height
    except Exception:
        return 0, None, None


async def process_and_upload_video(client: Client, video_url: str, chat_id: int,
                                    is_priority: bool = False, progress_msg: Message = None,
                                    user_id: int = 0, backend=None) -> bool:
    """Downloads one video and uploads it to chat_id, recording it in the
    uploaded-videos DB so it's never re-sent. Returns True for "done, move
    on" (uploaded OR permanently skipped e.g. too large) and False for
    "retry later" (a transient failure).

    backend is whichever downloader module actually resolves/fetches this
    URL — defaults to faphouse_downloader (the original, only backend
    this function supported before eporner auto-upload existed), so every
    pre-existing caller keeps working unchanged. Any backend passed in
    must implement get_available_qualities(url)/download_video(url,
    out_path, on_progress=, stream_url=), the same contract
    faphouse_downloader/ytdlp_downloader/fpo_downloader/porn_fetch_downloader
    all already share (see main.py's _downloader_for()). FanclubLockedError
    is faphouse-specific — getattr'd with a safe empty-tuple fallback so
    "except backend_error_cls" below still works (catches nothing) for a
    backend that doesn't define it."""
    backend = backend or faphouse
    backend_error_cls = getattr(backend, "FanclubLockedError", ())
    slug = extract_slug(video_url)
    if await is_video_uploaded(slug):
        return True

    ts = int(time.time())
    work_dir = os.path.join(DOWNLOAD_DIR, f"auto_{slug}_{ts}")
    os.makedirs(work_dir, exist_ok=True)
    out_path = os.path.join(work_dir, f"{slug}.mp4")

    # Phase tracker — updated by _tick_status so the ticker always shows
    # the current phase without needing a separate edit_text call.
    _phase = {"label": "🔍 Fetching page info...", "title_html": f"<code>{html.escape(slug)}</code>"}

    async def _tick_status():
        """Show a live animated ticker from the very first moment so the
        user never sees the message sitting frozen on 'Starting download...'
        — phase label updates as the pipeline moves through its stages."""
        elapsed = 0
        while True:
            await asyncio.sleep(2)
            elapsed += 2
            if progress_msg:
                try:
                    await progress_msg.edit_text(
                        f"{_phase['label']} ({elapsed}s)\n{_phase['title_html']}",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

    tick_task = asyncio.create_task(_tick_status())
    try:
        meta = await _get_page_meta(video_url)
        duration_str = None
        if hasattr(backend, "get_page_meta"):
            try:
                backend_meta = await asyncio.to_thread(backend.get_page_meta, video_url) or {}
                duration_str = _format_duration_mmss(backend_meta.get("duration"))
            except Exception:
                pass
        title = meta.get("title") or slug.replace("-", " ").title()
        # The raw scraped title can contain &, <, > etc. — safe to use for
        # DB storage/hashing/filenames, but embedding it as-is into an
        # HTML-parsed Telegram message breaks entity parsing on ANY of
        # those characters. That failure was being silently swallowed by
        # the try/except around every progress edit_text call below,
        # which made the progress message freeze forever at its very
        # first (title-free) text — looking exactly like "stuck on
        # Starting download..." even though the download itself was
        # running fine underneath.
        title_html = html.escape(title)
        poster_url = meta.get("poster_url")
        # Update ticker with real title now that we have it
        _phase["title_html"] = f"<code>{title_html}</code>"

        content_hash = _compute_title_hash(title)
        if content_hash and await is_title_duplicate(content_hash):
            await save_uploaded_video({
                "slug": slug, "title": title,
                "status": "duplicate_title", "duplicate_of_hash": content_hash,
            })
            if progress_msg:
                try:
                    await progress_msg.edit_text(
                        f"⏭️ <b>Skipped — already uploaded under a different link:</b>\n<code>{title_html}</code>"
                    )
                except Exception:
                    pass
            return True

        # Force every autoupload video to 720p (manual /download lets the
        # user pick a quality via show_quality_menu — autoupload has no one
        # to ask, so it always targets AUTOUPLOAD_TARGET_HEIGHT instead of
        # falling through to "Auto (Best)", which is often 1080p/4K).
        #
        # get_available_qualities() does up to 3 sequential HTTP round-trips
        # (authenticated page fetch, guest page fetch fallback, then the
        # m3u8 playlist itself) with no progress callback of its own — left
        # alone that's a silent gap of anywhere from a couple seconds to
        # 30-40s+ (worse if the site is slow/rate-limiting) where the
        # message just sits on the same "Starting download..." text with no
        # visible movement, which is exactly what looked "stuck"/very slow
        # in practice. A ticking elapsed-time status during this phase, and
        # a hard timeout so one slow/hanging link can't stall the whole
        # auto-upload queue, both fix that.
        _phase["label"] = "🔎 Resolving stream..."
        try:
            variants = await asyncio.wait_for(
                asyncio.to_thread(backend.get_available_qualities, video_url), timeout=60,
            )
        except asyncio.TimeoutError:
            if backend is fpo:
                # FPO uses yt-dlp under the hood — quality resolution timing out
                # (flashvars fetch slow/blocked) is not fatal.  Fall straight through
                # to yt-dlp's own auto-best selection instead of skipping the video.
                logger.warning(f"[worker] FPO quality resolution timed out for {slug} — falling back to yt-dlp auto-best")
                variants = [{"label": "Auto (Best)", "height": None, "url": None}]
            else:
                logger.warning(f"[worker] quality resolution timed out for {slug}")
                return False
        except backend_error_cls as e:
            logger.info(f"[worker] {slug} needs a separate Fanclub subscription — permanently skipping: {e}")
            await save_uploaded_video({
                "slug": slug, "title": title, "url": video_url,
                "status": "skipped_fanclub_locked",
            })
            if progress_msg:
                try:
                    await progress_msg.edit_text(
                        f"⚠️ <b>Skipped (needs a separate Fanclub sub):</b>\n<code>{title_html}</code>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
            return True
        except Exception as e:
            if backend is fpo:
                # FPO quality fetch failed (flashvars parse error, page layout change,
                # etc.) — don't skip the video.  yt-dlp can often still download it
                # on its own with auto-quality selection even when our flashvars
                # extraction fails, so fall back to "Auto (Best)" and let it try.
                logger.warning(f"[worker] FPO get_available_qualities failed for {slug}: {e} — falling back to yt-dlp auto-best")
                variants = [{"label": "Auto (Best)", "height": None, "url": None}]
            else:
                logger.warning(f"[worker] quality fetch failed for {slug}: {e}")
                return False

        target_stream_url = _pick_autoupload_stream_url(variants)
        quality_label = next((v["label"] for v in variants if v.get("url") == target_stream_url), "Auto (Best)")

        _phase["label"] = "⬇️ Downloading..."

        # Live download progress: faphouse.download_video() reports back via
        # on_progress (called from the worker thread), polled here into a
        # throttled Telegram edit exactly like the manual /download flow.
        _progress = {"pct": None, "downloaded": 0, "done": False, "error": None, "connecting": True}

        def _on_progress(info):
            _progress["pct"] = info.get("pct")
            _progress["downloaded"] = info.get("downloaded_bytes", 0)
            _progress["connecting"] = info.get("connecting", False)

        def _blocking_download():
            try:
                backend.download_video(video_url, out_path, on_progress=_on_progress, stream_url=target_stream_url)
            except Exception as exc:
                _progress["error"] = exc
            finally:
                _progress["done"] = True

        dl_tracker = _AutoProgressTracker(progress_msg, "Downloading", title_html, quality=quality_label, duration=duration_str) if progress_msg else None
        dl_start = time.time()
        dl_task = asyncio.create_task(asyncio.to_thread(_blocking_download))
        smoothed_total = 0

        # Hard timeout: if the entire download takes longer than this, kill it.
        # Prevents a stuck/slow ffmpeg from blocking the auto-upload queue forever.
        # Default 45 min — enough for the largest videos on the slowest CDN.
        _DL_HARD_TIMEOUT = int(os.environ.get("DOWNLOAD_HARD_TIMEOUT", str(45 * 60)))

        while not _progress["done"]:
            pct = _progress["pct"]
            elapsed_dl = time.time() - dl_start

            # Hard timeout guard — cancel the task and break out
            if elapsed_dl > _DL_HARD_TIMEOUT:
                logger.warning(
                    f"[worker] download hard timeout ({_DL_HARD_TIMEOUT}s) reached for {slug} — cancelling"
                )
                dl_task.cancel()
                _progress["done"] = True
                _progress["error"] = RuntimeError(
                    f"Download timed out after {_DL_HARD_TIMEOUT // 60} minutes"
                )
                break

            # As soon as ffmpeg fires its first callback, stop the phase ticker
            # and switch to real progress display
            if not _progress.get("connecting", True):
                _phase["label"] = "⬇️ Downloading..."
                try:
                    tick_task.cancel()  # stop the phase ticker — progress tracker takes over
                except Exception:
                    pass
            if pct and pct > 2:
                raw_total = int(_progress["downloaded"] / (pct / 100))
                smoothed_total = raw_total if not smoothed_total else int(smoothed_total * 0.8 + raw_total * 0.2)
            if dl_tracker:
                await dl_tracker.update(_progress["downloaded"], smoothed_total)
            await asyncio.sleep(1)  # 1s loop — was 2s, faster UI response
        try:
            await dl_task
        except asyncio.CancelledError:
            pass
        dl_seconds = time.time() - dl_start

        # Fully stop the phase ticker before upload begins.
        # tick_task.cancel() only *requests* cancellation — the coroutine
        # is still alive until it hits its next await point (asyncio.sleep).
        # If we don't await it here, _tick_status keeps editing progress_msg
        # during the upload, racing with part_tracker.update() and causing
        # Telegram's edit rate limit to fire mid-upload for Part 2+.
        tick_task.cancel()
        try:
            await tick_task
        except asyncio.CancelledError:
            pass

        if _progress["error"] is not None:
            if backend_error_cls and isinstance(_progress["error"], backend_error_cls):
                logger.info(f"[worker] {slug} needs a separate Fanclub subscription (found at download time) — permanently skipping")
                await save_uploaded_video({
                    "slug": slug, "title": title, "url": video_url,
                    "status": "skipped_fanclub_locked",
                })
                return True
            logger.warning(f"[worker] download failed for {slug}: {_progress['error']}")
            return False

        if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
            # ffmpeg exited without raising, but produced nothing usable —
            # e.g. the stream got cut off mid-way (expired signed URL,
            # a mid-download 403/404 from the CDN) so ffmpeg still exits
            # cleanly but writes an empty/partial file. This used to
            # return False with zero logging, making it indistinguishable
            # from every other failure reason in the logs.
            logger.warning(f"[worker] output file missing/empty after download for {slug} (out_path={out_path})")
            return False

        file_size = os.path.getsize(out_path)
        duration, width, height = await asyncio.to_thread(_ffprobe_metadata, out_path)

        thumb_path = os.path.join(work_dir, "thumb.jpg")
        got_thumb = False
        if poster_url:
            got_thumb = await asyncio.to_thread(_download_poster, poster_url, thumb_path)
        if not got_thumb:
            thumb_at = min(max(duration * 0.15, 3), 20) if duration else 3
            got_thumb = await asyncio.to_thread(_ffmpeg_thumbnail, out_path, thumb_path, thumb_at)

        if got_thumb:
            # Force whatever we ended up with (site poster or ffmpeg frame)
            # to Telegram's actual thumb limits (<=320x320, <200KB JPEG)
            # instead of letting an oversized one get silently rejected.
            got_thumb = await asyncio.to_thread(_finalize_thumbnail, thumb_path)

        parts = [out_path]
        if file_size > MAX_FILE_SIZE:
            if progress_msg:
                try:
                    await progress_msg.edit_text(
                        f"✂️ <b>Splitting into parts (over 2GB)...</b>\n<code>{title_html}</code>\n"
                        f"📦 {_human_size(file_size)}"
                    )
                except Exception:
                    pass
            # split_video_file() removes out_path itself once split
            # successfully — the thumbnail above was already generated
            # from it, so nothing downstream needs it to still exist.
            parts = await asyncio.to_thread(
                split_upload.split_video_file, out_path, work_dir, slug, SPLIT_PART_TARGET_BYTES,
            )
            if len(parts) <= 1:
                # ffmpeg couldn't split it (e.g. unreadable duration/corrupt
                # source) — nothing better to do than the old skip behavior.
                await save_uploaded_video({
                    "slug": slug, "title": title, "size": file_size,
                    "status": "skipped_size_limit", "url": video_url,
                })
                if progress_msg:
                    try:
                        await progress_msg.edit_text(
                            f"⚠️ <b>Skipped (over 2GB, couldn't split):</b> <code>{title_html}</code>\n"
                            f"📦 {_human_size(file_size)}"
                        )
                    except Exception:
                        pass
                return True

        name = os.path.basename(out_path)

        if progress_msg:
            try:
                await progress_msg.edit_text(f"📤 <b>Starting upload...</b>\n<code>{title_html}</code>")
            except Exception:
                pass
        ul_tracker = _AutoProgressTracker(progress_msg, "Uploading", title_html, quality=quality_label, duration=duration_str) if progress_msg else None
        ul_start = time.time()

        # Built with ul_seconds=0 for now and swapped for the real timing
        # further down via edit_caption — same two-step pattern the manual
        # /download flow uses, since the true upload duration isn't known
        # until send_video (or every part, for a split upload) finishes.
        if _build_caption:
            base_caption = await _build_caption(
                name=name, size_bytes=file_size, dl_seconds=dl_seconds, ul_seconds=0,
                user_id=user_id, source_link=video_url, quality_label=quality_label,
                duration_seconds=duration, suppress_auto_delete_note=True,
            )
        else:
            # configure_caption_builder() was never called (e.g. this
            # module used standalone) — fall back to the old minimal caption
            # instead of crashing.
            badge = "🔥 <b>FAPHOUSE FRESH RELEASE</b>" if is_priority else "🚀 <b>FAPHOUSE EXCLUSIVE</b>"
            base_caption = (
                f"{badge}\n\n"
                f"📂 <b>Title:</b> {title_html}\n"
                f"⏱ <b>Duration:</b> {_human_duration(duration)}\n"
                f"📦 <b>Size:</b> {_human_size(file_size)}"
            )

        # Stream button under the caption — built once and reused for every
        # part of a split upload, same as the thumbnail. None if main.py
        # never wired configure_caption_builder() with a builder (e.g. this
        # module run standalone), so the upload still goes through with no
        # button rather than crashing.
        #
        # FIX: _build_stream_markup() is a plain sync function (main.py's
        # build_stream_button_markup) that, for a TeraBox link, calls
        # probe_terabox_share() — a real network call (now up to two, since
        # it falls back from Baidu-PCS to hnn.workers.dev when the first
        # fails), each with a 15-20s timeout. Calling it directly here
        # blocked THIS event loop — meaning every user's bot interaction
        # bot-wide, not just this upload — for that entire duration on
        # every single auto-uploaded video. asyncio.to_thread() runs it on
        # a worker thread instead, so the wait no longer freezes anything
        # else the bot is doing concurrently.
        stream_markup = await asyncio.to_thread(_build_stream_markup, video_url) if _build_stream_markup else None

        async def _ensure_stream_markup(msg):
            # BUG FIX: reported as "the Stream button shows on some
            # autoupload videos but not others" with no pattern tying it
            # to site/backend (both are faphouse.com links here, same
            # code path, same stream_markup) — Telegram/Pyrogram
            # occasionally drop reply_markup passed alongside a video
            # send without raising (silent, not a caught exception, so
            # there was nothing here to log before). Checking and
            # re-attaching right after send is the same
            # verify-then-retry-once pattern already used for the "Full
            # Description" button below.
            if not stream_markup or msg.reply_markup:
                return
            try:
                await msg.edit_reply_markup(stream_markup)
                logger.warning(f"[scraper] stream button missing after send for {video_url}, re-attached on retry")
            except Exception as e:
                logger.warning(f"[scraper] stream button missing after send for {video_url}, re-attach also failed: {e}")

        if len(parts) == 1:
            sent = await client.send_video(
                chat_id=chat_id,
                video=parts[0],
                caption=base_caption,
                duration=int(duration),
                width=width or 1280,
                height=height or 720,
                thumb=thumb_path if got_thumb else None,
                supports_streaming=True,
                reply_markup=stream_markup,
                progress=ul_tracker.update if ul_tracker else None,
            )
            await _ensure_stream_markup(sent)
            message_ids = [sent.id]
            sent_msgs = [sent]
            total_parts = 1
        else:
            total_parts = len(parts)

            sent_msgs = []
            for i, part_path in enumerate(parts, start=1):
                # ── Inter-part cooldown (Part 2+) ───────────────────────────
                # Without a gap between parts, Telegram imposes FloodWait on
                # the second send_video() call — large video sends come right
                # after a successful Part 1, and the account hasn't had time
                # to "recover" from the first send.  FloodWait re-raised here
                # propagates to _upload_with_lock which retries the ENTIRE
                # video from scratch (re-download + re-split + Part 1 again),
                # creating an infinite loop of "only Part 1 uploads, chat
                # fills with duplicates".  A 3-second gap between parts is
                # enough to avoid the FloodWait in practice.
                if i > 1:
                    await asyncio.sleep(3)

                # ── Part status edit ─────────────────────────────────────────
                if progress_msg:
                    try:
                        await progress_msg.edit_text(
                            f"📤 <b>Starting upload — Part {i}/{total_parts}...</b>\n<code>{title_html}</code>"
                        )
                    except Exception:
                        pass

                # ── Fresh tracker for EVERY part ─────────────────────────────
                part_tracker = _AutoProgressTracker(
                    progress_msg, "Uploading", title_html,
                    quality=quality_label, duration=duration_str,
                ) if progress_msg else None

                async def _part_progress(current, total_bytes, _t=part_tracker):
                    if _t:
                        await _t.update(current, total_bytes)

                part_duration, part_width, part_height = await asyncio.to_thread(_ffprobe_metadata, part_path)

                # ── Send with inline FloodWait handling ──────────────────────
                # Catch FloodWait HERE (inside the loop) instead of letting it
                # propagate to the outer except FloodWait: raise, which would
                # exit process_and_upload_video(), trigger finally:
                # shutil.rmtree(work_dir) and DELETE the remaining parts, then
                # retry the whole video from scratch — causing duplicate Part 1
                # uploads and never getting to Part 2+.
                _fw_retries = 3
                part_msg = None
                for _attempt in range(_fw_retries):
                    try:
                        part_msg = await client.send_video(
                            chat_id=chat_id,
                            video=part_path,
                            caption=f"{base_caption}\n\n✂️ <b>Part {i}/{total_parts}</b>",
                            duration=int(part_duration),
                            width=part_width or width or 1280,
                            height=part_height or height or 720,
                            thumb=thumb_path if got_thumb else None,
                            supports_streaming=True,
                            reply_markup=stream_markup,
                            progress=_part_progress,
                        )
                        break  # success
                    except FloodWait as fw:
                        wait_s = fw.value
                        logger.warning(
                            f"[split-upload] FloodWait {wait_s}s before Part {i}/{total_parts} "
                            f"(attempt {_attempt + 1}/{_fw_retries}) — waiting in-loop, parts preserved"
                        )
                        if progress_msg:
                            try:
                                await progress_msg.edit_text(
                                    f"⏳ <b>Telegram rate limit — waiting {wait_s}s "
                                    f"(Part {i}/{total_parts})...</b>\n<code>{title_html}</code>"
                                )
                            except Exception:
                                pass
                        await asyncio.sleep(wait_s)
                        # Reset tracker for clean progress on retry
                        part_tracker = _AutoProgressTracker(
                            progress_msg, "Uploading", title_html,
                            quality=quality_label, duration=duration_str,
                        ) if progress_msg else None

                if part_msg is None:
                    # All FloodWait retries exhausted — propagate so
                    # _upload_with_lock can handle it at the video level
                    logger.error(f"[split-upload] Part {i}/{total_parts} failed after {_fw_retries} retries")
                    raise RuntimeError(f"Part {i}/{total_parts} upload failed after {_fw_retries} FloodWait retries")

                sent_msgs.append(part_msg)
                await _ensure_stream_markup(part_msg)
                try:
                    os.remove(part_path)
                except OSError:
                    pass
            sent = sent_msgs[0]
            message_ids = [m.id for m in sent_msgs]

        # Now that the real upload duration is known, swap in the final
        # caption with the true "Uploaded in" time, and — matching the
        # manual /download flow exactly — actually schedule the file for
        # deletion instead of printing an auto-delete warning that never
        # happens (autoupload never called schedule_delete before this).
        ul_seconds = time.time() - ul_start
        if _build_caption:
            final_caption = await _build_caption(
                name=name, size_bytes=file_size, dl_seconds=dl_seconds, ul_seconds=ul_seconds,
                user_id=user_id, source_link=video_url, quality_label=quality_label,
                duration_seconds=duration, suppress_auto_delete_note=True,
            )
            for i, part_msg in enumerate(sent_msgs, start=1):
                part_final = f"{final_caption}\n\n✂️ <b>Part {i}/{total_parts}</b>" if total_parts > 1 else final_caption
                try:
                    await part_msg.edit_caption(caption=part_final)
                except Exception:
                    pass
        # Auto-uploaded videos are NOT scheduled for deletion — unlike the
        # manual /download flow (a personal download, auto-deleted to save
        # space), auto-upload content is posted to a chat/channel as a
        # permanent archive/feed, so it should just stay there.

        await save_uploaded_video({
            "slug": slug, "title": title, "duration": duration,
            "size": file_size, "chat_id": chat_id, "message_id": sent.id,
            "parts": len(parts), "message_ids": message_ids,
            "content_hash": content_hash,
        })
        return True

    except FloodWait:
        # Do NOT sleep here — this function runs inside the global
        # _upload_lock (see _upload_with_lock below), and sleeping for
        # e.value seconds (often minutes, sometimes much longer for large
        # video uploads) while still holding that lock blocked EVERY other
        # video, in every chat/worker, for the full FloodWait duration —
        # with no visible message, which looked exactly like "the first
        # video downloads, then nothing ever downloads again." Re-raising
        # lets _upload_with_lock release the lock first and THEN wait,
        # so only this one video's turn is delayed, not the whole queue.
        raise
    except Exception as e:
        logger.error(f"[worker] unexpected error for {slug}: {e}")
        return False
    finally:
        # Cancel tick_task if it somehow survived (e.g. download completed
        # without ever setting connecting=False, so cancel was never called
        # in the polling loop).  Safe to call even if already cancelled.
        tick_task.cancel()
        try:
            await tick_task
        except (asyncio.CancelledError, Exception):
            pass
        shutil.rmtree(work_dir, ignore_errors=True)


async def _upload_with_lock(client: Client, video_url: str, chat_id: int,
                             is_priority: bool = False, progress_msg: Message = None,
                             user_id: int = 0, backend=None) -> bool:
    """Runs process_and_upload_video() under _upload_lock like before, but
    if it raises FloodWait, the lock is released BEFORE waiting out
    e.value seconds instead of sleeping while still holding it — so a
    flood-wait on one video only delays that one video, not every other
    video queued across every chat/worker. Loops back and retries (with
    the lock re-acquired fresh) once the wait is over."""
    while True:
        try:
            async with _upload_lock:
                return await process_and_upload_video(
                    client, video_url, chat_id, is_priority=is_priority,
                    progress_msg=progress_msg, user_id=user_id, backend=backend,
                )
        except FloodWait as e:
            wait_for = e.value
        logger.warning(f"[worker] FloodWait {wait_for}s — lock released, waiting before retry")
        if progress_msg:
            try:
                await progress_msg.edit_text(
                    f"⏳ <b>Telegram rate limit — waiting {wait_for}s before continuing...</b>"
                )
            except Exception:
                pass
        await asyncio.sleep(wait_for)


async def _eporner_upload_with_lock(client: Client, video_url: str, chat_id: int,
                                    is_priority: bool = False, progress_msg: Message = None,
                                    user_id: int = 0, backend=None) -> bool:
    """Same as _upload_with_lock() but uses _eporner_upload_lock instead of
    _upload_lock — so Eporner uploads run independently of FapHouse uploads
    and neither source blocks the other."""
    while True:
        try:
            async with _eporner_upload_lock:
                return await process_and_upload_video(
                    client, video_url, chat_id, is_priority=is_priority,
                    progress_msg=progress_msg, user_id=user_id, backend=backend,
                )
        except FloodWait as e:
            wait_for = e.value
        logger.warning(f"[eporner-worker] FloodWait {wait_for}s — lock released, waiting before retry")
        if progress_msg:
            try:
                await progress_msg.edit_text(
                    f"⏳ <b>Telegram rate limit — waiting {wait_for}s before continuing...</b>"
                )
            except Exception:
                pass
        await asyncio.sleep(wait_for)


async def _process_with_retries(client: Client, chat_id: int, video_url: str, label: str,
                                 progress_msg: Message, user_id: int, is_priority: bool = False,
                                 max_attempts: int = 3, backend=None) -> bool:
    """Wraps process_and_upload_video with retries + a visible reason on
    failure, instead of a single silent attempt that gives up immediately.

    Without this, ANY transient failure (a network hiccup, a flaky quality-
    variant pick, a momentary site error) made process_and_upload_video
    return False, and every worker loop just moved straight on to the next
    video in its list with nothing but a logger.warning() — invisible
    unless someone was tailing server logs. From inside Telegram that
    looked exactly like "the progress bar for one video gets replaced by
    the next video's progress bar, and nothing ever finishes downloading" —
    because nothing WAS finishing, it was failing and skipping every time.
    Retrying the SAME video a couple of times, with the reason shown in
    progress_msg, means a one-off hiccup actually recovers instead of
    permanently skipping, and a real, persistent problem is now visible
    instead of silently invisible.
    """
    # Use the Eporner-specific lock when the caller is the eporner monitor
    # (identified by backend=ytdlp + is_priority=True from eporner_live_monitor).
    # This lets FapHouse and Eporner upload simultaneously instead of
    # blocking each other through the single shared _upload_lock.
    _lock_fn = _eporner_upload_with_lock if (backend is ytdlp and is_priority) else _upload_with_lock

    ok = False
    for attempt in range(1, max_attempts + 1):
        try:
            ok = await _lock_fn(
                client, video_url, chat_id, is_priority=is_priority,
                progress_msg=progress_msg, user_id=user_id, backend=backend,
            )
        except Exception as e:
            # process_and_upload_video is supposed to catch its own
            # errors and return False — but if something still escapes
            # (e.g. a bug in a code path added later), treat it the
            # same as a normal "failed, retry" result instead of
            # letting it kill the whole worker loop. FloodWait itself is
            # already handled inside _upload_with_lock and never reaches
            # here.
            logger.error(f"[worker] process_and_upload_video raised for {label}: {e}")
            ok = False
        if ok:
            return True
        if attempt < max_attempts and progress_msg:
            try:
                await progress_msg.edit_text(
                    f"⚠️ <b>Download failed, retrying ({attempt}/{max_attempts - 1})...</b>\n<code>{label}</code>"
                )
            except Exception:
                pass
            await asyncio.sleep(5)

    # Every attempt failed. Without recording this, is_video_uploaded()
    # stays False forever for a video that can NEVER succeed (e.g. a
    # permanently broken/private page whose HTML never has a stream URL
    # to find) — live_site_monitor polls page 1 every MONITOR_INTERVAL
    # (3 min by default) and re-treats an unmarked video as "new" every
    # single cycle, so one bad video meant 3 fresh download attempts
    # every 3 minutes, forever, with no way for it to ever stop on its
    # own. Saving a "failed" record here makes is_video_uploaded() start
    # returning True for it immediately, the same dedup gate every
    # worker already checks before attempting anything — so this stops
    # ALL of them (monitor included) from retrying it again, without
    # having to touch each worker's own loop individually. Not silent,
    # either: get_failed_videos()/the /retryfailed flow (see database.py
    # and the command in main.py) surfaces exactly what's sitting here
    # and lets someone force a fresh attempt once whatever broke it is
    # actually fixed — this isn't a dead end, just no longer an
    # infinite, invisible retry loop.
    try:
        await save_uploaded_video({"slug": label, "status": "failed", "url": video_url})
    except Exception as e:
        logger.warning(f"[worker] couldn't record permanent-failure status for {label}: {e}")
    return False


async def chat_uploader_worker(client: Client, chat_id: int, user_id: int, is_admin: bool = False):
    """Pages through /videos for one chat, uploading anything new, saving
    its page position after every video so a restart resumes rather than
    re-scanning from page 1."""
    logger.info(f"[worker] started for chat {chat_id}")
    session_total = 0  # count for THIS session only — resets on every fresh start
    try:
        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            page = state.get("current_page", 1)
            videos = await get_page_videos_all_sites(page=page)
            if not videos:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete.</b>\n📊 Total uploaded: {session_total}",
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return

                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id, is_priority=False,
                )

                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                await set_chat_scraper_state(chat_id, {"current_page": page + 1})
    except Exception as e:
        logger.error(f"[worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "faphouse"), None)


def start_chat_worker_task(client: Client, chat_id: int, user_id: int, is_admin: bool = False):
    key = (chat_id, "faphouse")  # same slot as start_actor_worker_task — both are modes of plain /autoupload
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(chat_uploader_worker(client, chat_id, user_id, is_admin))
    _active_workers[key] = task
    return True


async def eporner_uploader_worker(client: Client, chat_id: int, user_id: int,
                                   model_name: str = None, is_admin: bool = False):
    """Same shape as chat_uploader_worker, but sourced from
    eporner_scraper.py (eporner.com's official search API) instead of
    faphouse's page-scrape, and downloaded/uploaded via ytdlp_downloader
    (process_and_upload_video's backend= param) instead of
    faphouse_downloader.

    Two distinct modes, matching /autouploadeporner's two call forms:
      - model_name is None: repeatedly pulls a random page (random sort
        order each time too — see eporner_scraper.get_random_page_videos)
        and uploads anything new from it. No natural "end" the way a
        specific listing has, so this runs until /stopupload — each cycle
        is independently random by design, so there's no meaningful page
        position to persist/resume across a restart the way model mode
        has.
      - model_name given: pages through that search's results in order,
        same resumable current_page persistence chat_uploader_worker
        uses for faphouse (kept under separate eporner_current_page/
        eporner_model fields in the same per-chat state document, so
        running both an eporner and a faphouse autoupload for the same
        chat — one after the other — don't clobber each other's saved
        page position)."""
    logger.info(f"[eporner-worker] started for chat {chat_id} (model={model_name or 'random'})")
    session_total = 0  # count for THIS session only
    try:
        if not model_name:
            while True:
                state = await get_chat_scraper_state(chat_id)
                if not state.get("is_running"):
                    break

                try:
                    videos = await eporner_scraper.get_random_page_videos()
                except Exception as e:
                    logger.warning(f"[eporner-worker] random page fetch failed: {e}")
                    await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                    continue

                any_new = False
                for item in videos:
                    latest = await get_chat_scraper_state(chat_id)
                    if not latest.get("is_running"):
                        return
                    if await is_video_uploaded(item["slug"]):
                        continue
                    any_new = True

                    progress_msg = None
                    try:
                        progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                    except Exception:
                        pass

                    ok = await _process_with_retries(
                        client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                        is_priority=False, backend=ytdlp,
                        max_attempts=1,
                    )
                    if progress_msg:
                        try:
                            await progress_msg.delete()
                        except Exception:
                            pass

                    if ok:
                        session_total += 1
                        await set_chat_scraper_state(chat_id, {"eporner_total_scraped": session_total})
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                    else:
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

                if not any_new:
                    await asyncio.sleep(5)
            return

        # Model/keyword mode — sequential paging, same resumable pattern
        # as chat_uploader_worker.
        state = await get_chat_scraper_state(chat_id)
        page = state.get("eporner_current_page", 1) if state.get("eporner_model") == model_name else 1
        await set_chat_scraper_state(chat_id, {"eporner_model": model_name, "eporner_current_page": page})

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            try:
                videos, total_pages = await eporner_scraper.get_model_page_videos(model_name, page=page)
            except Exception as e:
                logger.warning(f"[eporner-worker] model page fetch failed for {model_name!r}: {e}")
                await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                continue

            if not videos or page > total_pages:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{model_name}\".</b>\n"
                        f"📊 Total uploaded: {session_total}",
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return
                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                    is_priority=False, backend=ytdlp,
                    max_attempts=1,  # Eporner hash broken — skip immediately, mark failed
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"eporner_total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                page += 1
                await set_chat_scraper_state(chat_id, {"eporner_current_page": page})
    except Exception as e:
        logger.error(f"[eporner-worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "eporner"), None)


def start_eporner_worker_task(client: Client, chat_id: int, user_id: int,
                               model_name: str = None, is_admin: bool = False):
    key = (chat_id, "eporner")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(eporner_uploader_worker(client, chat_id, user_id, model_name, is_admin))
    _active_workers[key] = task
    return True


async def mat6tube_uploader_worker(client: Client, chat_id: int, user_id: int,
                                    model_name: str = None, is_admin: bool = False):
    """Same shape as eporner_uploader_worker, sourced from mat6tube_scraper.py.

    Two modes:
      - model_name is None: random mode — picks random videos endlessly.
      - model_name given: search/model mode — pages through results in order.

    Download backend: mat6tube_downloader (direct MP4, no yt-dlp needed).
    """
    import mat6tube_downloader as mat6tube_dl
    logger.info(f"[mat6tube-worker] started for chat {chat_id} (model={model_name or 'random'})")
    session_total = 0
    try:
        if not model_name:
            while True:
                state = await get_chat_scraper_state(chat_id)
                if not state.get("is_running"):
                    break

                try:
                    videos = await mat6tube_scraper.get_random_page_videos()
                except Exception as e:
                    logger.warning(f"[mat6tube-worker] random page fetch failed: {e}")
                    await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                    continue

                any_new = False
                for item in videos:
                    latest = await get_chat_scraper_state(chat_id)
                    if not latest.get("is_running"):
                        return
                    if await is_video_uploaded(item["slug"]):
                        continue
                    any_new = True

                    progress_msg = None
                    try:
                        progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                    except Exception:
                        pass

                    ok = await _process_with_retries(
                        client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                        is_priority=False, backend=mat6tube_dl,
                        max_attempts=2,
                    )
                    if progress_msg:
                        try:
                            await progress_msg.delete()
                        except Exception:
                            pass

                    if ok:
                        session_total += 1
                        await set_chat_scraper_state(chat_id, {"mat6tube_total_scraped": session_total})
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                    else:
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

                if not any_new:
                    await asyncio.sleep(5)
            return

        # Model/keyword mode
        state = await get_chat_scraper_state(chat_id)
        page = state.get("mat6tube_current_page", 1) if state.get("mat6tube_model") == model_name else 1
        await set_chat_scraper_state(chat_id, {"mat6tube_model": model_name, "mat6tube_current_page": page})

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            try:
                videos, total_pages = await mat6tube_scraper.get_model_page_videos(model_name, page=page)
            except Exception as e:
                logger.warning(f"[mat6tube-worker] model page fetch failed for {model_name!r}: {e}")
                await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                continue

            if not videos or page > total_pages:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{model_name}\".</b>\n"
                        f"📊 Total uploaded: {session_total}",
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return
                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                    is_priority=False, backend=mat6tube_dl,
                    max_attempts=2,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"mat6tube_total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                page += 1
                await set_chat_scraper_state(chat_id, {"mat6tube_current_page": page})
    except Exception as e:
        logger.error(f"[mat6tube-worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "mat6tube"), None)


def start_mat6tube_worker_task(client: Client, chat_id: int, user_id: int,
                                model_name: str = None, is_admin: bool = False):
    key = (chat_id, "mat6tube")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(mat6tube_uploader_worker(client, chat_id, user_id, model_name, is_admin))
    _active_workers[key] = task
    return True


async def pornhub_uploader_worker(client: Client, chat_id: int, user_id: int,
                                    model_name: str = None, is_admin: bool = False,
                                    mode: str = "pornstar"):
    """Same shape as eporner_uploader_worker, sourced from
    pornhub_scraper.py (yt-dlp's own PornHub playlist listing) instead of
    eporner's official search API — see pornhub_scraper.py's docstring
    for why listing works differently there. Download/upload still goes
    through ytdlp_downloader (process_and_upload_video's backend= param),
    same as eporner.

    mode="pornstar" (default, via /autouploadpornhub) lists a performer's
    own video page; mode="studio" (via /autouploadpornhubstudio) lists a
    studio/channel's page instead — genuinely different PornHub URLs,
    unlike eporner where "studio" is just the same keyword search (see
    autouploadepornerstudio_cmd's docstring). State fields are prefixed
    per mode (pornhub_pornstar_* / pornhub_studio_*) so running one of
    each for the same chat, one after another, doesn't clobber the
    other's saved page position — same reasoning as pornhub's fields
    being separate from eporner's own."""
    fetch_page = pornhub_scraper.get_studio_page_videos if mode == "studio" else pornhub_scraper.get_model_page_videos
    state_prefix = f"pornhub_{mode}"
    logger.info(f"[pornhub-worker] started for chat {chat_id} (mode={mode}, target={model_name or 'random'})")
    session_total = 0  # count for THIS session only
    try:
        if not model_name:
            while True:
                state = await get_chat_scraper_state(chat_id)
                if not state.get("is_running"):
                    break

                try:
                    videos = await pornhub_scraper.get_random_page_videos()
                except Exception as e:
                    logger.warning(f"[pornhub-worker] random page fetch failed: {e}")
                    await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                    continue

                any_new = False
                for item in videos:
                    latest = await get_chat_scraper_state(chat_id)
                    if not latest.get("is_running"):
                        return
                    if await is_video_uploaded(item["slug"]):
                        continue
                    any_new = True

                    progress_msg = None
                    try:
                        progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                    except Exception:
                        pass

                    ok = await _process_with_retries(
                        client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                        is_priority=False, backend=ytdlp,
                    )
                    if progress_msg:
                        try:
                            await progress_msg.delete()
                        except Exception:
                            pass

                    if ok:
                        session_total += 1
                        await set_chat_scraper_state(chat_id, {"pornhub_total_scraped": session_total})
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                    else:
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

                if not any_new:
                    await asyncio.sleep(5)
            return

        # Model/studio mode — sequential paging, same resumable pattern
        # as eporner_uploader_worker's own model mode.
        state = await get_chat_scraper_state(chat_id)
        page = state.get(f"{state_prefix}_current_page", 1) if state.get(f"{state_prefix}_target") == model_name else 1
        await set_chat_scraper_state(chat_id, {f"{state_prefix}_target": model_name, f"{state_prefix}_current_page": page})

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            try:
                videos, total_pages = await fetch_page(model_name, page=page)
            except Exception as e:
                logger.warning(f"[pornhub-worker] {mode} page fetch failed for {model_name!r}: {e}")
                await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                continue

            if not videos or page > total_pages:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{model_name}\".</b>\n"
                        f"📊 Total uploaded: {session_total}",
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return
                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                    is_priority=False, backend=ytdlp,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"pornhub_total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                page += 1
                await set_chat_scraper_state(chat_id, {f"{state_prefix}_current_page": page})
    except Exception as e:
        logger.error(f"[pornhub-worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "pornhub"), None)


def start_pornhub_worker_task(client: Client, chat_id: int, user_id: int,
                                model_name: str = None, is_admin: bool = False,
                                mode: str = "pornstar"):
    key = (chat_id, "pornhub")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(pornhub_uploader_worker(client, chat_id, user_id, model_name, is_admin, mode))
    _active_workers[key] = task
    return True


# ══════════════════════════════════════════════════════════════════════
#  JAV WORKER  (javct.net — code lookup + StreamWish auto-download)
# ══════════════════════════════════════════════════════════════════════

async def jav_uploader_worker(client: Client, chat_id: int, user_id: int,
                               query: str = None, is_admin: bool = False):
    """Auto-upload worker for javct.net.

    query is either:
      - A JAV code like "IPX-421" / "hz-3490"   → fetch that specific video
      - A search term like "Yui Hatano"           → search + upload all results
      - None                                       → upload latest videos (random mode)

    Download strategy (see jav_scraper.py module docstring):
      1. If a StreamWish link is found → download via ytdlp (works without premium)
      2. Otherwise → send the video's info card + link list to chat (no silent skip)

    JAV videos on javct.net mostly have file-host links (Keep2Share, RapidGator etc.)
    that need a paid account — only StreamWish links are auto-downloadable.
    Everything else is posted as a Telegram message with the info + links so the
    user can open them manually, which is better than silently skipping.
    """
    logger.info(f"[jav-worker] started for chat {chat_id} (query={query!r})")
    session_total = 0

    async def _post_info_card(info: dict):
        """Send video info + download links as a Telegram message."""
        code      = info.get("video_code") or "???"
        title     = info.get("title") or code
        duration  = info.get("duration") or "?"
        date      = info.get("release_date") or "?"
        studio    = info.get("studio") or "?"
        label     = info.get("label") or ""
        director  = info.get("director") or ""
        rating    = info.get("rating") or ""
        actresses = ", ".join(info.get("actresses") or []) or "?"
        actors    = ", ".join(info.get("actors") or [])
        genres    = ", ".join((info.get("genres") or [])[:5]) or "?"
        url       = info.get("url") or ""

        # Optional extra fields
        extra = ""
        if label:
            extra += f"\n🏷 <b>Label:</b> {html.escape(label)}"
        if director:
            extra += f"\n🎬 <b>Director:</b> {html.escape(director)}"
        if actors:
            extra += f"\n👨 <b>Actor(s):</b> {html.escape(actors)}"
        if rating:
            extra += f"\n⭐ <b>Rating:</b> {html.escape(rating)}"

        # Build download links section
        dl_lines = []
        for lnk in info.get("download_links") or []:
            provider = lnk.get("provider", "?")
            href     = lnk.get("url", "")
            ltype    = lnk.get("type", "")
            icon     = "🎬" if ltype == "stream" else ("🧲" if ltype == "magnet" else "📥")
            note     = " ✅" if provider in jav_scraper.YTDLP_COMPATIBLE_PROVIDERS else ""
            dl_lines.append(f'  {icon} <a href="{href}">{html.escape(provider)}</a>{note}')

        dl_section = ("\n<b>🔗 Download Links:</b>\n" + "\n".join(dl_lines)) if dl_lines else ""

        text = (
            f"🎌 <b>{html.escape(code)}</b> — {html.escape(title)}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"⏱ <b>Duration:</b> {html.escape(duration)}\n"
            f"📅 <b>Released:</b> {html.escape(date)}\n"
            f"🏢 <b>Studio:</b> {html.escape(studio)}\n"
            f"👩 <b>Actress(es):</b> {html.escape(actresses)}\n"
            f"🏷 <b>Genres:</b> {html.escape(genres)}"
            f"{extra}\n"
            f"🔗 <b>Page:</b> <a href=\"{url}\">{html.escape(code)}</a>"
            f"{dl_section}"
        )

        thumb = info.get("thumbnail") or info.get("cover_image")
        try:
            if thumb:
                await client.send_photo(chat_id, thumb, caption=text, parse_mode=ParseMode.HTML)
            else:
                await client.send_message(chat_id, text, parse_mode=ParseMode.HTML,
                                          link_preview=False)
        except Exception as e:
            logger.warning(f"[jav-worker] info card send failed: {e}")

    async def _try_download_and_upload(info: dict) -> bool:
        """Try to auto-download via StreamWish. Returns True if uploaded,
        False if only info card was sent (no StreamWish link available)."""
        code = info.get("video_code") or info.get("url", "").split("/")[-1]
        slug = f"jav_{code.lower()}"

        if await is_video_uploaded(slug):
            return True

        # Find StreamWish link (only auto-downloadable provider)
        streamwish_link = jav_scraper.pick_downloadable_link(info.get("download_links") or [])

        if not streamwish_link:
            # No auto-downloadable link → post info card + links instead
            logger.info(f"[jav-worker] {code}: no StreamWish link — posting info card")
            await _post_info_card(info)
            # Mark as uploaded so we don't repost
            await save_uploaded_video({"slug": slug, "title": info.get("title") or code,
                                       "status": "info_card_posted"})
            return True

        # StreamWish found → download via yt-dlp
        dl_url  = streamwish_link["url"]
        ts      = int(time.time())
        work_dir = os.path.join(DOWNLOAD_DIR, f"jav_{code}_{ts}")
        os.makedirs(work_dir, exist_ok=True)
        out_path = os.path.join(work_dir, f"{slug}.mp4")

        progress_msg = None
        try:
            progress_msg = await client.send_message(
                chat_id,
                f"🔍 <b>Fetching JAV stream...</b>\n🎌 <code>{html.escape(code)}</code>",
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass

        # ── Phase ticker — always visible from the first moment ───────────
        title_html = html.escape(info.get("title") or code)
        _phase = {"label": "🔍 Fetching stream...", "title_html": title_html}

        async def _tick_jav():
            elapsed = 0
            while True:
                await asyncio.sleep(2)
                elapsed += 2
                if progress_msg:
                    try:
                        await progress_msg.edit_text(
                            f"{_phase['label']} ({elapsed}s)\n🎌 {_phase['title_html']}",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

        tick_task = asyncio.create_task(_tick_jav())

        try:
            _progress = {
                "pct": None, "downloaded": 0, "speed": 0,
                "done": False, "error": None, "connecting": True,
            }

            def _on_progress(d):
                _progress["pct"]        = d.get("pct")
                _progress["downloaded"] = d.get("downloaded_bytes", 0)
                _progress["speed"]      = d.get("speed_bytes_s", 0)
                _progress["connecting"] = d.get("connecting", False)

            def _blocking_dl():
                try:
                    _phase["label"] = "🔎 Resolving StreamWish..."
                    qualities  = ytdlp.get_available_qualities(dl_url)
                    stream_url = qualities[0]["url"] if qualities else dl_url
                    _phase["label"] = "📥 Connecting to CDN..."
                    ytdlp.download_video(
                        dl_url, out_path,
                        on_progress=_on_progress,
                        stream_url=stream_url,
                    )
                    _progress["done"] = True
                except Exception as exc:
                    _progress["done"]  = True
                    _progress["error"] = exc

            # ── Download task ─────────────────────────────────────────────
            HARD_TIMEOUT  = 20 * 60   # 20 min ceiling
            STALL_TIMEOUT = 60        # 60s no new bytes → cancel

            dl_task        = asyncio.create_task(asyncio.to_thread(_blocking_dl))
            dl_start       = time.time()
            _last_bytes    = 0
            _last_bytes_ts = time.time()

            dl_tracker   = _AutoProgressTracker(progress_msg, "Downloading", title_html) if progress_msg else None
            ul_tracker   = None
            smoothed_total = 0

            while not _progress["done"]:
                now_t     = time.time()
                cur_bytes = _progress["downloaded"]

                # Hard timeout
                if now_t - dl_start > HARD_TIMEOUT:
                    dl_task.cancel()
                    _progress["done"]  = True
                    _progress["error"] = RuntimeError(f"Download timed out after {int(now_t - dl_start)}s.")
                    break

                # Stall watchdog
                if cur_bytes > _last_bytes:
                    _last_bytes    = cur_bytes
                    _last_bytes_ts = now_t
                elif not _progress["connecting"] and (now_t - _last_bytes_ts) > STALL_TIMEOUT:
                    logger.warning(f"[jav-worker] {code}: stalled {STALL_TIMEOUT}s — cancelling.")
                    dl_task.cancel()
                    _progress["done"]  = True
                    _progress["error"] = RuntimeError(f"Download stalled ({STALL_TIMEOUT}s no data).")
                    break

                # Switch from phase ticker to real progress tracker
                if not _progress["connecting"] and cur_bytes > 0:
                    _phase["label"] = "⬇️ Downloading..."
                    tick_task.cancel()   # stop phase ticker — tracker takes over

                pct = _progress["pct"]
                if pct and pct > 2 and cur_bytes > 0:
                    raw_total = int(cur_bytes / (pct / 100))
                    smoothed_total = raw_total if not smoothed_total else int(smoothed_total * 0.8 + raw_total * 0.2)
                if dl_tracker:
                    await dl_tracker.update(cur_bytes, smoothed_total)

                await asyncio.sleep(1)

            try:
                await dl_task
            except asyncio.CancelledError:
                pass

            tick_task.cancel()

            if _progress["error"]:
                raise _progress["error"]

            if not os.path.exists(out_path) or os.path.getsize(out_path) < 1024:
                raise RuntimeError("Downloaded file is empty or missing.")

            # ── Upload with live progress ─────────────────────────────────
            file_size  = os.path.getsize(out_path)
            thumb_path = info.get("cover_image") or info.get("thumbnail")

            ul_tracker = _AutoProgressTracker(progress_msg, "Uploading", title_html) if progress_msg else None

            caption = (
                f"🎌 <b>{html.escape(code)}</b>\n"
                f"🎬 {html.escape(info.get('title') or code)}\n"
                f"👩 {html.escape(', '.join(info.get('actresses') or []) or '?')}\n"
                f"⏱ {html.escape(info.get('duration') or '?')}"
            )

            _ul_progress = {"uploaded": 0, "done": False}

            async def _ul_progress_cb(current, total):
                _ul_progress["uploaded"] = current
                if ul_tracker:
                    await ul_tracker.update(current, total)

            if file_size > MAX_FILE_SIZE:
                await split_upload.upload_split(
                    client, chat_id, out_path,
                    caption=caption, thumb=thumb_path, parse_mode=ParseMode.HTML,
                )
            else:
                send_kwargs = dict(
                    chat_id=chat_id, video=out_path, caption=caption,
                    parse_mode=ParseMode.HTML, supports_streaming=True,
                    progress=_ul_progress_cb,
                )
                if thumb_path:
                    send_kwargs["thumb"] = thumb_path
                await client.send_video(**send_kwargs)

            await save_uploaded_video({
                "slug": slug, "title": info.get("title") or code, "status": "uploaded",
            })
            return True

        except Exception as e:
            logger.warning(f"[jav-worker] {code} download/upload failed: {e}")
            tick_task.cancel()
            await _post_info_card(info)
            await save_uploaded_video({
                "slug": slug, "title": info.get("title") or code, "status": "info_card_posted",
            })
            return True
        finally:
            tick_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await tick_task
            if progress_msg:
                try:
                    await progress_msg.delete()
                except Exception:
                    pass
            try:
                shutil.rmtree(work_dir, ignore_errors=True)
            except Exception:
                pass

    # ── Main worker loop ───────────────────────────────────────────────────────
    try:
        # Single code lookup: "IPX-421"
        if query and re.match(r"^[A-Za-z]{2,6}-?\d{2,5}$", query.strip()):
            code = query.strip()
            try:
                info = await asyncio.to_thread(jav_scraper.get_video_info, code)
            except Exception as e:
                logger.error(f"[jav-worker] get_video_info({code}) failed: {e}")
                try:
                    await client.send_message(
                        chat_id,
                        f"❌ <b>JAV code not found:</b> <code>{html.escape(code)}</code>\n"
                        f"Error: {html.escape(str(e))}",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
                return
            await _try_download_and_upload(info)
            await set_chat_scraper_state(chat_id, {"is_running": False})
            return

        # Search mode: "Yui Hatano"
        if query:
            page  = 1
            total = 0
            state = await get_chat_scraper_state(chat_id)
            page  = state.get("jav_search_page", 1) if state.get("jav_search_query") == query else 1
            await set_chat_scraper_state(chat_id, {"jav_search_query": query, "jav_search_page": page})

            # Try actress page first (more focused results)
            actress_slug = jav_scraper.slugify_actress(query)
            actress_results = await asyncio.to_thread(
                jav_scraper.get_actress_videos, actress_slug, 1, 20
            )
            use_actress_page = bool(actress_results)
            if use_actress_page:
                logger.info(f"[jav-worker] actress page found for {query!r} → /actress/{actress_slug}")

            while True:
                state = await get_chat_scraper_state(chat_id)
                if not state.get("is_running"):
                    break

                try:
                    if use_actress_page:
                        results = await asyncio.to_thread(
                            jav_scraper.get_actress_videos, actress_slug, page, 20
                        )
                    else:
                        results = await asyncio.to_thread(
                            jav_scraper.search_videos, query, page=page
                        )
                except Exception as e:
                    logger.warning(f"[jav-worker] fetch page {page} failed: {e}")
                    await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                    continue

                if not results:
                    await set_chat_scraper_state(chat_id, {"is_running": False})
                    try:
                        await client.send_message(
                            chat_id,
                            f"✅ <b>JAV complete for \"{html.escape(query)}\".</b>\n"
                            f"📊 Total processed: {total}",
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        pass
                    break

                for item in results:
                    s = await get_chat_scraper_state(chat_id)
                    if not s.get("is_running"):
                        return
                    slug = f"jav_{(item.get('code') or '').lower()}"
                    if await is_video_uploaded(slug):
                        continue
                    try:
                        info = await asyncio.to_thread(
                            jav_scraper.get_video_info, item["url"]
                        )
                    except Exception as e:
                        logger.warning(f"[jav-worker] get_video_info({item['url']}) failed: {e}")
                        continue
                    await _try_download_and_upload(info)
                    total += 1
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)

                page += 1
                await set_chat_scraper_state(chat_id, {"jav_search_page": page})
            return

        # Random/latest mode (no query)
        page = 1
        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            try:
                results = await asyncio.to_thread(jav_scraper.get_latest_videos, page=page)
            except Exception as e:
                logger.warning(f"[jav-worker] latest page {page} failed: {e}")
                await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                continue

            if not results:
                page = 1   # wrap around to page 1
                await asyncio.sleep(5)
                continue

            for item in results:
                s = await get_chat_scraper_state(chat_id)
                if not s.get("is_running"):
                    return
                slug = f"jav_{(item.get('code') or '').lower()}"
                if await is_video_uploaded(slug):
                    continue
                try:
                    info = await asyncio.to_thread(jav_scraper.get_video_info, item["url"])
                except Exception as e:
                    logger.warning(f"[jav-worker] get_video_info failed: {e}")
                    continue
                await _try_download_and_upload(info)
                await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)

            page += 1

    except Exception as e:
        logger.error(f"[jav-worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "jav"), None)


def start_jav_worker_task(client: Client, chat_id: int, user_id: int,
                           query: str = None, is_admin: bool = False):
    key = (chat_id, "jav")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(jav_uploader_worker(client, chat_id, user_id, query, is_admin))
    _active_workers[key] = task
    return True


def _backend_for_slug(slug: str):
    """Picks which downloader backend a saved DB entry belongs to, purely
    from its slug prefix (extract_slug()'s "eporner-<id>"/"pornhub-<id>"/
    "xhamster-<id>"/"xvideos-<id>"/"fpo-<id>" conventions vs everything
    else, which is faphouse's own /videos/<slug> — the only other source
    that's ever written into this same uploaded_videos collection). Used
    by the retry workers below, which only have a saved slug/url to go
    on — not which command originally created the entry — so a retry
    actually re-downloads through the SAME engine that made sense for
    that video instead of defaulting to faphouse_downloader
    unconditionally and failing outright on a non-faphouse URL."""
    if slug.startswith(("eporner-", "pornhub-", "xhamster-", "xvideos-")):
        return ytdlp
    if slug.startswith("fpo-"):
        return fpo
    return faphouse


async def xhamster_uploader_worker(client: Client, chat_id: int, user_id: int,
                                     model_name: str = None, is_admin: bool = False,
                                     mode: str = "pornstar"):
    """xhamster.com equivalent of pornhub_uploader_worker — identical
    shape, sourced from xhamster_scraper.py (yt-dlp's own XHamster
    playlist listing) instead of PornHub's. See xhamster_scraper.py's
    docstring for the URL patterns and the one xHamster-specific paging
    quirk it works around.

    mode="pornstar" (default, via /autouploadxhamster) lists a performer's
    own video page; mode="studio" (via /autouploadxhamsterstudio) lists a
    studio/channel's page instead. State fields are prefixed per mode
    (xhamster_pornstar_* / xhamster_studio_*) so running one of each for
    the same chat, one after another, doesn't clobber the other's saved
    page position — same reasoning as pornhub's fields being separate
    from eporner's own."""
    fetch_page = xhamster_scraper.get_studio_page_videos if mode == "studio" else xhamster_scraper.get_model_page_videos
    state_prefix = f"xhamster_{mode}"
    logger.info(f"[xhamster-worker] started for chat {chat_id} (mode={mode}, target={model_name or 'random'})")
    session_total = 0  # FIX: was missing → UnboundLocalError on first ok upload
    try:
        if not model_name:
            while True:
                state = await get_chat_scraper_state(chat_id)
                if not state.get("is_running"):
                    break

                try:
                    videos = await xhamster_scraper.get_random_page_videos()
                except Exception as e:
                    logger.warning(f"[xhamster-worker] random page fetch failed: {e}")
                    await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                    continue

                any_new = False
                for item in videos:
                    latest = await get_chat_scraper_state(chat_id)
                    if not latest.get("is_running"):
                        return
                    if await is_video_uploaded(item["slug"]):
                        continue
                    any_new = True

                    progress_msg = None
                    try:
                        progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                    except Exception:
                        pass

                    ok = await _process_with_retries(
                        client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                        is_priority=False, backend=ytdlp,
                    )
                    if progress_msg:
                        try:
                            await progress_msg.delete()
                        except Exception:
                            pass

                    if ok:
                        session_total += 1
                        await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                    else:
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

                if not any_new:
                    await asyncio.sleep(5)
            return

        # Model/studio mode — sequential paging, same resumable pattern
        # as pornhub_uploader_worker's own model mode.
        state = await get_chat_scraper_state(chat_id)
        page = state.get(f"{state_prefix}_current_page", 1) if state.get(f"{state_prefix}_target") == model_name else 1
        await set_chat_scraper_state(chat_id, {f"{state_prefix}_target": model_name, f"{state_prefix}_current_page": page})

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            try:
                videos, total_pages = await fetch_page(model_name, page=page)
            except Exception as e:
                logger.warning(f"[xhamster-worker] {mode} page fetch failed for {model_name!r}: {e}")
                await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                continue

            if not videos or page > total_pages:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{model_name}\".</b>\n"
                        f"📊 Total uploaded: {session_total}",
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return
                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                    is_priority=False, backend=ytdlp,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                page += 1
                await set_chat_scraper_state(chat_id, {f"{state_prefix}_current_page": page})
    except Exception as e:
        logger.error(f"[xhamster-worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "xhamster"), None)


def start_xhamster_worker_task(client: Client, chat_id: int, user_id: int,
                                 model_name: str = None, is_admin: bool = False,
                                 mode: str = "pornstar"):
    key = (chat_id, "xhamster")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(xhamster_uploader_worker(client, chat_id, user_id, model_name, is_admin, mode))
    _active_workers[key] = task
    return True


async def xvideos_uploader_worker(client: Client, chat_id: int, user_id: int,
                                    model_name: str = None, is_admin: bool = False,
                                    mode: str = "pornstar"):
    """xvideos.com equivalent of xhamster_uploader_worker — identical
    shape, sourced from xvideos_scraper.py (yt-dlp's own XVideos playlist
    listing) instead of xHamster's. See xvideos_scraper.py's docstring
    for the URL patterns (XVideos calls performer pages "profiles" and
    studio pages "channels").

    mode="pornstar" (default, via /autouploadxvideos) lists a performer's
    own /profiles/<slug> page; mode="studio" (via
    /autouploadxvideosstudio) lists a /channels/<slug> page instead.
    State fields are prefixed per mode (xvideos_pornstar_* /
    xvideos_studio_*) so running one of each for the same chat, one after
    another, doesn't clobber the other's saved page position — same
    reasoning as pornhub/xhamster's fields being separate from each
    other's."""
    fetch_page = xvideos_scraper.get_studio_page_videos if mode == "studio" else xvideos_scraper.get_model_page_videos
    state_prefix = f"xvideos_{mode}"
    logger.info(f"[xvideos-worker] started for chat {chat_id} (mode={mode}, target={model_name or 'random'})")
    session_total = 0
    try:
        if not model_name:
            while True:
                state = await get_chat_scraper_state(chat_id)
                if not state.get("is_running"):
                    break

                try:
                    videos = await xvideos_scraper.get_random_page_videos()
                except Exception as e:
                    logger.warning(f"[xvideos-worker] random page fetch failed: {e}")
                    await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                    continue

                any_new = False
                for item in videos:
                    latest = await get_chat_scraper_state(chat_id)
                    if not latest.get("is_running"):
                        return
                    if await is_video_uploaded(item["slug"]):
                        continue
                    any_new = True

                    progress_msg = None
                    try:
                        progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                    except Exception:
                        pass

                    ok = await _process_with_retries(
                        client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                        is_priority=False, backend=ytdlp,
                    )
                    if progress_msg:
                        try:
                            await progress_msg.delete()
                        except Exception:
                            pass

                    if ok:
                        session_total += 1
                        await set_chat_scraper_state(chat_id, {"xvideos_total_scraped": session_total})
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                    else:
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

                if not any_new:
                    await asyncio.sleep(5)
            return

        # Model/studio mode — sequential paging, same resumable pattern
        # as xhamster_uploader_worker's own model mode.
        state = await get_chat_scraper_state(chat_id)
        page = state.get(f"{state_prefix}_current_page", 1) if state.get(f"{state_prefix}_target") == model_name else 1
        await set_chat_scraper_state(chat_id, {f"{state_prefix}_target": model_name, f"{state_prefix}_current_page": page})

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            try:
                videos, total_pages = await fetch_page(model_name, page=page)
            except Exception as e:
                logger.warning(f"[xvideos-worker] {mode} page fetch failed for {model_name!r}: {e}")
                await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                continue

            if not videos or page > total_pages:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{model_name}\".</b>\n"
                        f"📊 Total uploaded: {session_total}",
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return
                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id,
                    is_priority=False, backend=ytdlp,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"xvideos_total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                page += 1
                await set_chat_scraper_state(chat_id, {f"{state_prefix}_current_page": page})
    except Exception as e:
        logger.error(f"[xvideos-worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "xvideos"), None)


def start_xvideos_worker_task(client: Client, chat_id: int, user_id: int,
                                model_name: str = None, is_admin: bool = False,
                                mode: str = "pornstar"):
    key = (chat_id, "xvideos")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(xvideos_uploader_worker(client, chat_id, user_id, model_name, is_admin, mode))
    _active_workers[key] = task
    return True


# ─────────────────────────────────────────────────────────────────────────────
#  FPO.XXX worker + monitor
#  Mirrors the eporner/xvideos pattern exactly:
#    • random mode  — continuously fetches random fpo.xxx pages, no end
#    • model mode   — pages through a specific performer's listing in order,
#                     finishes when all pages are done, sends completion notice
#  Download backend: fpo_downloader (yt-dlp + cookie injection) — NOT ytdlp
#  directly, because fpo.xxx's KVS/flashvars descramble logic lives in
#  fpo_downloader and isn't handled by yt-dlp's generic extractor alone.
# ─────────────────────────────────────────────────────────────────────────────

async def fpo_uploader_worker(client: Client, chat_id: int, user_id: int,
                               model_name: str = None, is_admin: bool = False):
    """fpo.xxx equivalent of eporner_uploader_worker.

    model_name=None  → random mode: fetches random category pages forever
                       (same as eporner/xvideos random modes — no end,
                       each batch is independently random, use /stopupload).
    model_name=<str> → performer mode: pages through /models/<slug>/ in
                       order until all pages are exhausted, then sends a
                       completion message and stops.

    State keys (per-chat, in DB):
      fpo_model         — current performer target (for resume detection)
      fpo_current_page  — last page successfully completed
      fpo_total_scraped — running count of uploaded videos
    """
    logger.info(f"[fpo-worker] started for chat {chat_id} (target={model_name or 'random'})")
    session_total = 0
    try:
        if not model_name:
            # Random mode — identical shape to xvideos/eporner random modes
            while True:
                state = await get_chat_scraper_state(chat_id)
                if not state.get("is_running"):
                    break

                try:
                    videos = await asyncio.to_thread(fpo.get_random_page_videos)
                except Exception as e:
                    logger.warning(f"[fpo-worker] random page fetch failed: {e}")
                    await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                    continue

                any_new = False
                for item in videos:
                    latest = await get_chat_scraper_state(chat_id)
                    if not latest.get("is_running"):
                        return
                    if await is_video_uploaded(item["slug"]):
                        continue
                    any_new = True

                    progress_msg = None
                    try:
                        progress_msg = await client.send_message(
                            chat_id, "📥 <b>Starting download...</b>",
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass

                    ok = await _process_with_retries(
                        client, chat_id, item["url"], item["slug"],
                        progress_msg, user_id, is_priority=False, backend=fpo,
                    )
                    if progress_msg:
                        try:
                            await progress_msg.delete()
                        except Exception:
                            pass

                    if ok:
                        session_total += 1
                        await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                    else:
                        await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

                if not any_new:
                    await asyncio.sleep(5)
            return

        # Performer mode — sequential paging with resume support
        state = await get_chat_scraper_state(chat_id)
        page = (
            state.get("fpo_current_page", 1)
            if state.get("fpo_model") == model_name
            else 1
        )
        await set_chat_scraper_state(chat_id, {
            "fpo_model": model_name,
            "fpo_current_page": page,
        })

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            try:
                videos, total_pages = await asyncio.to_thread(
                    fpo.get_model_page_videos, model_name, page
                )
            except Exception as e:
                logger.warning(f"[fpo-worker] model page fetch failed for {model_name!r} page {page}: {e}")
                await asyncio.sleep(AUTO_UPLOAD_COOLDOWN)
                continue

            if not videos or page > total_pages:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                total_done = state.get("fpo_total_scraped", 0)
                if page == 1 and not videos:
                    # Page 1 empty = search returned nothing for this name.
                    search_url = fpo._search_url(model_name)
                    msg = (
                        f"❌ <b>FPO model not found:</b> <code>{model_name}</code>\n"
                        f"Tried: <code>{search_url}</code>\n\n"
                        f"💡 <b>Options:</b>\n"
                        f"• Search manually: <a href=\"{search_url}\">fpo.xxx search</a>\n"
                        f"• Paste her exact video/search URL:\n"
                        f"  <code>/autouploadfpo https://www.fpo.xxx/search/Name-/</code>\n\n"
                        f"<i>She may not be on FPO.xxx, or spelled differently.</i>"
                    )
                else:
                    msg = (
                        f"✅ <b>Auto-upload complete for \"{model_name}\".</b>\n"
                        f"📊 Total uploaded: {total_done}"
                    )
                try:
                    await client.send_message(chat_id, msg, parse_mode=ParseMode.HTML)
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return
                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(
                        chat_id, "📥 <b>Starting download...</b>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"],
                    progress_msg, user_id, is_priority=False, backend=fpo,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                page += 1
                await set_chat_scraper_state(chat_id, {"fpo_current_page": page})

    except Exception as e:
        logger.error(f"[fpo-worker] exception in chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "fpo"), None)


def start_fpo_worker_task(client: Client, chat_id: int, user_id: int,
                           model_name: str = None, is_admin: bool = False):
    """Start the fpo_uploader_worker background task for chat_id.
    Returns False if a worker is already running for this chat (same guard
    as every other start_*_worker_task in this file)."""
    key = (chat_id, "fpo")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(
        fpo_uploader_worker(client, chat_id, user_id, model_name, is_admin)
    )
    _active_workers[key] = task
    return True


async def fpo_live_monitor(client: Client, default_channel: int):
    """24/7 live monitor for fpo.xxx — same shape as eporner_live_monitor /
    xvideos_live_monitor. Polls fpo.get_latest_videos() every MONITOR_INTERVAL
    seconds, uploading anything not yet in the dedup DB to default_channel.

    Pauses while a manual worker (/autouploadfpo or /autoupload) already owns
    the channel — same guard as all other monitors — to avoid interleaving
    monitor uploads with a human-initiated scrape run."""
    logger.info(f"[fpo-monitor] live monitor running -> channel {default_channel}")
    while True:
        try:
            manual_job = _any_manual_job_running(default_channel)
            if manual_job and not manual_job.done():
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            try:
                candidates = await asyncio.to_thread(fpo.get_latest_videos)
            except Exception as e:
                logger.warning(f"[fpo-monitor] get_latest_videos failed: {e}")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            if not candidates:
                logger.warning("[fpo-monitor] get_latest_videos returned empty — site may be blocking this IP")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            logger.info(f"[fpo-monitor] got {len(candidates)} candidates")
            for item in candidates:
                if await is_video_uploaded(item["slug"]):
                    continue

                manual_job = _any_manual_job_running(default_channel)
                if manual_job and not manual_job.done():
                    break

                progress_msg = None
                try:
                    progress_msg = await client.send_message(
                        default_channel, "📥 <b>Starting download...</b>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

                await _process_with_retries(
                    client, default_channel, item["url"], item["slug"],
                    progress_msg, OWNER_ID, is_priority=True, backend=fpo,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass
                await asyncio.sleep(3)

        except (PeerIdInvalid, ChannelInvalid):
            logger.warning("[fpo-monitor] DEFAULT_CHANNEL is not reachable — check the bot is an admin there.")
        except Exception as e:
            logger.error(f"[fpo-monitor] error: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)


async def retry_skipped_videos_worker(client: Client, chat_id: int, progress_msg: Message = None,
                                       user_id: int = 0) -> dict:
    """Re-attempts every video still marked status="skipped_size_limit" —
    almost all of these predate split_upload.py, back when anything over
    2GB was permanently skipped with no way to retry it; is_video_uploaded()
    treats ANY existing DB entry (skip or success) as "done", so they'd
    otherwise sit there forever even now that splitting exists.

    Deletes each entry right before retrying so process_and_upload_video's
    own is_video_uploaded() gate doesn't block it — the same
    delete-then-let-the-normal-path-re-save contract used everywhere else
    in this file. If a retry fails transiently (not re-skipped, just a
    download/upload error), the entry is simply gone from the dedup cache
    afterward rather than stuck — the regular scraper will pick it back up
    the next time it happens to page past it."""
    skipped = await get_skipped_size_limit_videos()
    total = len(skipped)
    uploaded = 0
    still_skipped = 0
    no_url = 0
    failed = 0

    for i, item in enumerate(skipped, start=1):
        slug = item["slug"]
        url = item.get("url")
        title = item.get("title", slug)
        title_html = html.escape(title)

        if not url:
            # Saved before the "url" field existed (see the skip-save call
            # in process_and_upload_video) — nothing to retry it with, so
            # just leave it alone rather than deleting a record we can't
            # recreate.
            no_url += 1
            continue

        if progress_msg:
            try:
                await progress_msg.edit_text(
                    f"🔄 <b>Retrying skipped videos ({i}/{total})...</b>\n<code>{title_html}</code>\n\n"
                    f"✅ Uploaded: {uploaded} | ⚠️ Still too big: {still_skipped} | "
                    f"🚫 No URL saved: {no_url} | ❌ Failed: {failed}"
                )
            except Exception:
                pass

        await delete_uploaded_video(slug)

        ok = await _upload_with_lock(
            client, url, chat_id, is_priority=False, progress_msg=None, user_id=user_id,
            backend=_backend_for_slug(slug),
        )

        if not ok:
            failed += 1
            continue

        status = await get_uploaded_video_status(slug)
        if status == "skipped_size_limit":
            still_skipped += 1
        else:
            uploaded += 1

        await asyncio.sleep(2)

    result = {"total": total, "uploaded": uploaded, "still_skipped": still_skipped, "no_url": no_url, "failed": failed}
    if progress_msg:
        try:
            await progress_msg.edit_text(
                f"✅ <b>Retry complete.</b>\n\n"
                f"📊 Checked: {total}\n"
                f"✅ Uploaded: {uploaded}\n"
                f"⚠️ Still too big to split: {still_skipped}\n"
                f"🚫 No URL saved (pre-fix entry): {no_url}\n"
                f"❌ Failed (transient — will resurface on next scrape): {failed}"
            )
        except Exception:
            pass
    return result


def start_retry_skipped_task(client: Client, chat_id: int, progress_msg: Message = None, user_id: int = 0):
    global _active_retry_task
    if _active_retry_task is not None and not _active_retry_task.done():
        return False
    _active_retry_task = asyncio.create_task(retry_skipped_videos_worker(client, chat_id, progress_msg, user_id))
    return True


async def retry_failed_videos_worker(client: Client, chat_id: int, progress_msg: Message = None,
                                      user_id: int = 0) -> dict:
    """Same idea as retry_skipped_videos_worker, for status="failed"
    entries instead — see _process_with_retries() in this file for how
    those get created (every retry attempt exhausted, most commonly a
    page whose HTML never had a stream URL to extract, e.g. the site's
    player markup changed for that one video, or it's since gone
    private/removed). Deletes each entry right before retrying for the
    same reason retry_skipped_videos_worker does: is_video_uploaded()
    treats ANY existing entry as "done", so the normal upload path would
    just re-skip it without this."""
    failed = await get_failed_videos()
    total = len(failed)
    uploaded = 0
    still_failed = 0
    no_url = 0

    for i, item in enumerate(failed, start=1):
        slug = item["slug"]
        url = item.get("url")
        title = item.get("title", slug)
        title_html = html.escape(title)

        if not url:
            no_url += 1
            continue

        if progress_msg:
            try:
                await progress_msg.edit_text(
                    f"🔄 <b>Retrying failed videos ({i}/{total})...</b>\n<code>{title_html}</code>\n\n"
                    f"✅ Uploaded: {uploaded} | ❌ Still failing: {still_failed} | 🚫 No URL saved: {no_url}"
                )
            except Exception:
                pass

        await delete_uploaded_video(slug)

        ok = await _process_with_retries(
            client, chat_id, url, slug, None, user_id, is_priority=False,
            backend=_backend_for_slug(slug),
        )
        if ok:
            uploaded += 1
        else:
            still_failed += 1
        await asyncio.sleep(2)

    result = {"total": total, "uploaded": uploaded, "still_failed": still_failed, "no_url": no_url}
    if progress_msg:
        try:
            await progress_msg.edit_text(
                f"✅ <b>Retry complete.</b>\n\n"
                f"📊 Checked: {total}\n"
                f"✅ Uploaded: {uploaded}\n"
                f"❌ Still failing: {still_failed}\n"
                f"🚫 No URL saved (pre-fix entry): {no_url}"
            )
        except Exception:
            pass
    return result


def start_retry_failed_task(client: Client, chat_id: int, progress_msg: Message = None, user_id: int = 0):
    global _active_retry_task
    if _active_retry_task is not None and not _active_retry_task.done():
        return False
    _active_retry_task = asyncio.create_task(retry_failed_videos_worker(client, chat_id, progress_msg, user_id))
    return True


async def actor_uploader_worker(client: Client, chat_id: int, user_id: int,
                                actor_name: str, is_admin: bool = False,
                                prefound_paths: dict = None):
    """Same idea as chat_uploader_worker, but paginates one performer's
    page instead of the full /videos listing.

    prefound_paths: already-discovered {base_url: path} from main.py's
    discover_actor_paths() call — skip re-discovery if provided."""
    logger.info(f"[worker] actor worker started for chat {chat_id} -> {actor_name!r}")
    session_total = 0  # count for THIS session only — resets on every fresh start
    try:
        state = await get_chat_scraper_state(chat_id)
        resuming_same_actor = state.get("mode") == "actor" and state.get("actor_display") == actor_name
        actor_paths = state.get("actor_sites") if resuming_same_actor else prefound_paths

        if not actor_paths:
            actor_paths = await discover_actor_paths(actor_name)
            if not actor_paths:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"⚠️ <b>Couldn't find a page for \"{actor_name}\".</b>\n"
                        "Double-check the spelling — or the site may have no matching performer page.",
                    )
                except Exception:
                    pass
                return
            await set_chat_scraper_state(chat_id, {
                "mode": "actor",
                "actor_display": actor_name,
                "actor_sites": actor_paths,
                "current_page": 1,
                "total_scraped": 0,
            })

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            page = state.get("current_page", 1)
            videos = await get_listing_page_videos(actor_paths, page=page)
            if not videos:
                # An empty result here currently looks identical whether
                # it's the genuine end of this performer's catalog OR a
                # one-off network hiccup/timeout on that fetch (see
                # get_page_videos — both cases return []). Retry once
                # after a short wait before concluding the whole run is
                # "complete", so a transient blip doesn't silently
                # truncate however many pages of this actor's videos
                # were still left.
                await asyncio.sleep(5)
                videos = await get_listing_page_videos(actor_paths, page=page)
            if not videos:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{actor_name}\".</b>\n"
                        f"📊 Total uploaded: {state.get('total_scraped', 0)}",
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return

                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id, is_priority=False,
                )

                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                await set_chat_scraper_state(chat_id, {"current_page": page + 1})
    except Exception as e:
        logger.error(f"[worker] exception in actor worker for chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "actor"), None)


def start_actor_worker_task(client: Client, chat_id: int, user_id: int, actor_name: str,
                             is_admin: bool = False, prefound_paths: dict = None):
    key = (chat_id, "actor")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(
        actor_uploader_worker(client, chat_id, user_id, actor_name, is_admin, prefound_paths)
    )
    _active_workers[key] = task
    return True


async def category_uploader_worker(client: Client, chat_id: int, user_id: int, tag_name: str, is_admin: bool = False):
    """Same idea as actor_uploader_worker, but paginates one category/tag's
    page (found via discover_category_paths) instead of a performer's —
    so "/autouploadtag <tag name>" uploads every video on every page of
    that category/tag's catalog, not just page 1."""
    logger.info(f"[worker] category worker started for chat {chat_id} -> {tag_name!r}")
    session_total = 0  # count for THIS session only — resets on every fresh start
    try:
        state = await get_chat_scraper_state(chat_id)
        resuming_same_tag = state.get("mode") == "category" and state.get("category_display") == tag_name
        category_paths = state.get("category_sites") if resuming_same_tag else None

        if not category_paths:
            category_paths = await discover_category_paths(tag_name)
            if not category_paths:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"⚠️ <b>Couldn't find a page for \"{tag_name}\".</b>\n"
                        "Double-check the spelling — or the site may have no matching category/tag page."
                    )
                except Exception:
                    pass
                return
            await set_chat_scraper_state(chat_id, {
                "mode": "category",
                "category_display": tag_name,
                "category_sites": category_paths,
                "current_page": 1,
                "total_scraped": 0,
            })

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            page = state.get("current_page", 1)
            videos = await get_listing_page_videos(category_paths, page=page)
            if not videos:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{tag_name}\".</b>\n"
                        f"📊 Total uploaded: {state.get('total_scraped', 0)}"
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return

                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id, is_priority=False,
                )

                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                await set_chat_scraper_state(chat_id, {"current_page": page + 1})
    except Exception as e:
        logger.error(f"[worker] exception in category worker for chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "tag"), None)


def start_category_worker_task(client: Client, chat_id: int, user_id: int, tag_name: str, is_admin: bool = False):
    key = (chat_id, "tag")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(category_uploader_worker(client, chat_id, user_id, tag_name, is_admin))
    _active_workers[key] = task
    return True


async def studio_uploader_worker(client: Client, chat_id: int, user_id: int, studio_name: str,
                                  is_admin: bool = False, prefound_paths: dict = None):
    """Same idea as category_uploader_worker, but for a studio/production-
    company page (found via discover_studio_paths) — so
    "/autouploadstudio <studio name>" uploads every video on every page
    of that studio's catalog."""
    logger.info(f"[worker] studio worker started for chat {chat_id} -> {studio_name!r}")
    session_total = 0  # count for THIS session only — resets on every fresh start
    try:
        state = await get_chat_scraper_state(chat_id)
        resuming_same_studio = state.get("mode") == "studio" and state.get("studio_display") == studio_name
        studio_paths = state.get("studio_sites") if resuming_same_studio else prefound_paths

        if not studio_paths:
            studio_paths = await discover_studio_paths(studio_name)
            if not studio_paths:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"⚠️ <b>Couldn't find a page for \"{studio_name}\".</b>\n"
                        "Double-check the spelling — or the site may have no matching studio page."
                    )
                except Exception:
                    pass
                return
            await set_chat_scraper_state(chat_id, {
                "mode": "studio",
                "studio_display": studio_name,
                "studio_sites": studio_paths,
                "current_page": 1,
                "total_scraped": 0,
            })

        while True:
            state = await get_chat_scraper_state(chat_id)
            if not state.get("is_running"):
                break

            page = state.get("current_page", 1)
            videos = await get_listing_page_videos(studio_paths, page=page)
            if not videos:
                await set_chat_scraper_state(chat_id, {"is_running": False})
                try:
                    await client.send_message(
                        chat_id,
                        f"✅ <b>Auto-upload complete for \"{studio_name}\".</b>\n"
                        f"📊 Total uploaded: {state.get('total_scraped', 0)}"
                    )
                except Exception:
                    pass
                break

            for item in videos:
                latest = await get_chat_scraper_state(chat_id)
                if not latest.get("is_running"):
                    return

                if await is_video_uploaded(item["slug"]):
                    continue

                progress_msg = None
                try:
                    progress_msg = await client.send_message(chat_id, "📥 <b>Starting download...</b>")
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, chat_id, item["url"], item["slug"], progress_msg, user_id, is_priority=False,
                )

                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass

                if ok:
                    session_total += 1
                    await set_chat_scraper_state(chat_id, {"total_scraped": session_total})
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN)
                else:
                    await asyncio.sleep(2 if is_admin else AUTO_UPLOAD_COOLDOWN * 3)

            current = await get_chat_scraper_state(chat_id)
            if current.get("is_running"):
                await set_chat_scraper_state(chat_id, {"current_page": page + 1})
    except Exception as e:
        logger.error(f"[worker] exception in studio worker for chat {chat_id}: {e}")
    finally:
        _active_workers.pop((chat_id, "studio"), None)


def start_studio_worker_task(client: Client, chat_id: int, user_id: int, studio_name: str,
                              is_admin: bool = False, prefound_paths: dict = None):
    key = (chat_id, "studio")
    if key in _active_workers and not _active_workers[key].done():
        return False
    task = asyncio.create_task(
        studio_uploader_worker(client, chat_id, user_id, studio_name, is_admin, prefound_paths)
    )
    _active_workers[key] = task
    return True


async def notify_server_restart(client: Client):
    """On boot: pause every chat that was mid-run when the process last
    stopped, and let each one know how to resume — a bare asyncio task
    for a paginated multi-hour job doesn't survive a restart on its own,
    so this is the recovery path rather than silently losing the run."""
    try:
        active = await get_all_active_scraper_states()
    except Exception as e:
        logger.warning(f"[worker] couldn't load active scraper states: {e}")
        return
    for state in active:
        chat_id = state.get("chat_id")
        if not chat_id:
            continue
        await set_chat_scraper_state(chat_id, {"is_running": False})
        page = state.get("current_page", 1)
        if state.get("mode") == "actor" and state.get("actor_display"):
            resume_hint = f"Send /autoupload {state['actor_display']} again to resume from here."
        elif state.get("mode") == "category" and state.get("category_display"):
            resume_hint = f"Send /autouploadtag {state['category_display']} again to resume from here."
        elif state.get("mode") == "studio" and state.get("studio_display"):
            resume_hint = f"Send /autouploadstudio {state['studio_display']} again to resume from here."
        else:
            resume_hint = "Send /autoupload again to resume from here."
        try:
            await client.send_message(
                chat_id,
                f"⚠️ <b>Bot restarted — auto-upload paused.</b>\n"
                f"📄 Saved position: page {page}\n\n"
                f"{resume_hint}",
            )
        except (PeerIdInvalid, ChannelInvalid):
            pass
        except Exception as e:
            logger.warning(f"[worker] restart notice failed for {chat_id}: {e}")
        await asyncio.sleep(3)


async def live_site_monitor(client: Client, default_channel: int):
    """Optional 24/7 watcher: polls page 1 only, for brand-new releases,
    and pushes them straight to default_channel as soon as they appear.

    Runs independently of /autoupload, /autouploadtag and /stopupload —
    it isn't tracked in _active_workers and nothing about those commands
    used to touch it. That meant starting a manual "/autoupload <actor>"
    or "/autouploadtag <tag>" job didn't pause this: it kept polling and
    pushing whatever fresh videos the whole site published, straight into
    the same default_channel, at the same time as the manual job's own
    videos — so "run it for one actor" actually got that actor's videos
    AND unrelated site-wide fresh videos mixed together in the channel.

    Now it checks _active_workers for default_channel before each poll
    and skips the pass entirely while a manual job owns that channel,
    picking back up on its own as soon as that job finishes/is stopped."""
    logger.info(f"[monitor] live site monitor running -> channel {default_channel}")
    while True:
        try:
            manual_job = _any_manual_job_running(default_channel)
            if manual_job and not manual_job.done():
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            for item in await get_latest_fresh_videos():
                if await is_video_uploaded(item["slug"]):
                    continue
                # Re-check on every item too, not just once per poll cycle —
                # a manual job could start midway through this loop (it can
                # run long: many items per cycle), and without this a job
                # started partway through would still get its videos
                # interleaved with whatever's left of this pass.
                manual_job = _any_manual_job_running(default_channel)
                if manual_job and not manual_job.done():
                    break
                progress_msg = None
                try:
                    progress_msg = await client.send_message(
                        default_channel, "📥 <b>Starting download...</b>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
                await _process_with_retries(
                    client, default_channel, item["url"], item["slug"],
                    progress_msg, OWNER_ID, is_priority=True,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass
                await asyncio.sleep(3)
        except (PeerIdInvalid, ChannelInvalid):
            logger.warning("[monitor] DEFAULT_CHANNEL is not reachable — check the bot is an admin there.")
        except Exception as e:
            logger.error(f"[monitor] error: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)


async def eporner_live_monitor(client: Client, default_channel: int):
    """eporner.com's equivalent of live_site_monitor — same idea (watch
    for brand-new videos, push straight to default_channel), same
    pause-while-a-manual-job-owns-the-channel guard, same
    _process_with_retries plumbing, just sourced from
    eporner_scraper.get_latest_videos() instead of faphouse's page-1
    scrape, and downloaded via ytdlp_downloader (backend=ytdlp) instead
    of faphouse_downloader.

    Uncapped, same as live_site_monitor — every new video
    get_latest_videos() returns each cycle gets pushed. Deliberately
    chosen this way (an earlier version capped this via
    EPORNER_MONITOR_MAX_PER_CYCLE, specifically because eporner is a
    high-volume general tube site rather than a small curated-release
    site like faphouse — removing the cap trades that flood-protection
    for parity with how live_site_monitor behaves)."""
    logger.info(f"[eporner-monitor] live monitor running -> channel {default_channel}")
    while True:
        try:
            manual_job = _any_manual_job_running(default_channel)
            if manual_job and not manual_job.done():
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            try:
                candidates = await eporner_scraper.get_latest_videos()
            except Exception as e:
                logger.warning(f"[eporner-monitor] get_latest_videos failed: {e}")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            pushed = 0
            for item in candidates:
                if await is_video_uploaded(item["slug"]):
                    continue

                manual_job = _any_manual_job_running(default_channel)
                if manual_job and not manual_job.done():
                    break

                progress_msg = None
                try:
                    progress_msg = await client.send_message(
                        default_channel, "📥 <b>Starting download...</b>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
                ok = await _process_with_retries(
                    client, default_channel, item["url"], item["slug"],
                    progress_msg, OWNER_ID, is_priority=True, backend=ytdlp,
                    max_attempts=1,  # Eporner hash broken — skip immediately, mark failed
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass
                if ok:
                    pushed += 1
                await asyncio.sleep(3)
        except (PeerIdInvalid, ChannelInvalid):
            logger.warning("[eporner-monitor] DEFAULT_CHANNEL is not reachable — check the bot is an admin there.")
        except Exception as e:
            logger.error(f"[eporner-monitor] error: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)


async def mat6tube_live_monitor(client: Client, default_channel: int):
    """24/7 live monitor for mat6tube.com — same shape as eporner_live_monitor.
    Polls mat6tube_scraper.get_latest_videos() every MONITOR_INTERVAL seconds
    and pushes any new videos straight to default_channel.
    Download backend: mat6tube_downloader (direct MP4).
    """
    import mat6tube_downloader as mat6tube_dl
    logger.info(f"[mat6tube-monitor] live monitor running -> channel {default_channel}")
    while True:
        try:
            manual_job = _any_manual_job_running(default_channel)
            if manual_job and not manual_job.done():
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            try:
                candidates = await mat6tube_scraper.get_latest_videos()
            except Exception as e:
                logger.warning(f"[mat6tube-monitor] get_latest_videos failed: {e}")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            if not candidates:
                logger.warning("[mat6tube-monitor] get_latest_videos returned empty — site may be blocking this IP")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            pushed = 0
            for item in candidates:
                if await is_video_uploaded(item["slug"]):
                    continue

                manual_job = _any_manual_job_running(default_channel)
                if manual_job and not manual_job.done():
                    break

                progress_msg = None
                try:
                    progress_msg = await client.send_message(
                        default_channel, "📥 <b>Starting download...</b>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

                ok = await _process_with_retries(
                    client, default_channel, item["url"], item["slug"],
                    progress_msg, OWNER_ID, is_priority=False, backend=mat6tube_dl,
                    max_attempts=2,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass
                if ok:
                    pushed += 1
                await asyncio.sleep(3)

        except (PeerIdInvalid, ChannelInvalid):
            logger.warning("[mat6tube-monitor] DEFAULT_CHANNEL is not reachable — check the bot is an admin there.")
        except Exception as e:
            logger.error(f"[mat6tube-monitor] error: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)


async def xhamster_live_monitor(client: Client, default_channel: int):
    """XHamster blocks cloud/server IPs — JSON API returns empty, HTML pages
    return 404 on Render. Monitor is disabled until a working endpoint is found."""
    logger.warning("[xhamster-monitor] DISABLED — XHamster blocks Render cloud IPs (API empty + HTML 404). Monitor will not run.")
    return  # no-op

    # Original monitor code below (kept for reference):
    logger.info(f"[xhamster-monitor] live monitor running -> channel {default_channel}")
    while True:
        try:
            manual_job = _any_manual_job_running(default_channel)
            if manual_job and not manual_job.done():
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            try:
                candidates = await xhamster_scraper.get_latest_videos()
            except Exception as e:
                logger.warning(f"[xhamster-monitor] get_latest_videos failed: {e}")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            if not candidates:
                logger.warning("[xhamster-monitor] get_latest_videos returned empty — API may be blocked on this IP")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            logger.info(f"[xhamster-monitor] got {len(candidates)} candidates")
            pushed = 0
            for item in candidates:
                if await is_video_uploaded(item["slug"]):
                    continue

                manual_job = _any_manual_job_running(default_channel)
                if manual_job and not manual_job.done():
                    break

                progress_msg = None
                try:
                    progress_msg = await client.send_message(
                        default_channel, "📥 <b>Starting download...</b>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
                ok = await _process_with_retries(
                    client, default_channel, item["url"], item["slug"],
                    progress_msg, OWNER_ID, is_priority=True, backend=ytdlp,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass
                if ok:
                    pushed += 1
                await asyncio.sleep(3)
        except (PeerIdInvalid, ChannelInvalid):
            logger.warning("[xhamster-monitor] DEFAULT_CHANNEL is not reachable — check the bot is an admin there.")
        except Exception as e:
            logger.error(f"[xhamster-monitor] error: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)


async def xvideos_live_monitor(client: Client, default_channel: int):
    """XVideos blocks all cloud/server IPs — HTML + API both return 404
    on Render. Monitor is disabled until a working proxy/endpoint is found."""
    logger.warning("[xvideos-monitor] DISABLED — XVideos blocks Render cloud IPs (all endpoints 404). Monitor will not run.")
    return  # no-op

    # Original monitor code below (kept for reference):
    logger.info(f"[xvideos-monitor] live monitor running -> channel {default_channel}")
    while True:
        try:
            manual_job = _any_manual_job_running(default_channel)
            if manual_job and not manual_job.done():
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            try:
                candidates = await xvideos_scraper.get_latest_videos()
            except Exception as e:
                logger.warning(f"[xvideos-monitor] get_latest_videos failed: {e}")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            if not candidates:
                logger.warning("[xvideos-monitor] get_latest_videos returned empty — API may be blocked on this IP")
                await asyncio.sleep(MONITOR_INTERVAL)
                continue

            logger.info(f"[xvideos-monitor] got {len(candidates)} candidates")
            for item in candidates:
                if await is_video_uploaded(item["slug"]):
                    continue

                manual_job = _any_manual_job_running(default_channel)
                if manual_job and not manual_job.done():
                    break

                progress_msg = None
                try:
                    progress_msg = await client.send_message(
                        default_channel, "📥 <b>Starting download...</b>",
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
                await _process_with_retries(
                    client, default_channel, item["url"], item["slug"],
                    progress_msg, OWNER_ID, is_priority=True, backend=ytdlp,
                )
                if progress_msg:
                    try:
                        await progress_msg.delete()
                    except Exception:
                        pass
                await asyncio.sleep(3)
        except (PeerIdInvalid, ChannelInvalid):
            logger.warning("[xvideos-monitor] DEFAULT_CHANNEL is not reachable — check the bot is an admin there.")
        except Exception as e:
            logger.error(f"[xvideos-monitor] error: {e}")
        await asyncio.sleep(MONITOR_INTERVAL)
