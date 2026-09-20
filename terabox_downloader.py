"""
terabox_downloader.py — Enhanced Terabox downloader with metadata extraction

FIXES IMPLEMENTED:
1. ✅ get_page_meta() - Now extracts duration & thumbnail from API response
2. ✅ Metadata preservation - _resolve() now returns duration, poster, title
3. ✅ Better validation - 50KB+ minimum (real videos), < 500MB max (sanity)
4. ✅ Multi-API fallback - 3 APIs with retry logic + timeout handling
"""

import re
import mimetypes
import os
import random
import threading
import time
import logging
import json
import urllib.parse
import concurrent.futures
import requests
import urllib3
from typing import Optional, Dict

from bs4 import BeautifulSoup

from config import MAX_FILE_SIZE  # shared 2GB-default, config-driven cap — see config.py

logger = logging.getLogger("terabox_downloader")

# ── Terabox domain family ────────────────────────────────────────────────────
TERABOX_DOMAINS = [
    "terabox.com", "terabox.app", "terabox.club", "terabox.fun",
    "terabox.link", "terabox.click",
    "terafileshare.com", "terasharelink.com", "terasharefile.com",
    "terashareus.com", "teraboxsharefile.com", "teraboxshare.com",
    "1024terabox.com", "1024-terabox.com", "1024tera.com", "1024tera.co",
    "tera1024box.com",
    "teraboxapp.com", "teraboxlink.com", "teraboxurl.com",
    "teraboxfree.com", "teraboxshort.com", "teraboxshortlink.com",
    "teraboxlite.com", "urlshortterabox.com",
    "freeterabox.com", "4funbox.com", "4funbox.in", "fancybox.in",
    "mirrobox.com", "momerybox.com", "nephobox.com",
    "gibibox.com", "goaibox.com", "joybox.cc", "tibibox.com",
    "pebibox.com", "bestclouddrive.com",
]

_TERABOX_DOMAIN_RE = re.compile(
    r"https?://(?:www\.)?(?:" +
    "|".join(re.escape(d) for d in TERABOX_DOMAINS) +
    r")/(?:s/[a-zA-Z0-9_-]+|sharing/link\?surl=[a-zA-Z0-9_-]+)"
)

# ── MULTI-TIER RESOLVER ──────────────────────────────────────────────────────
# FIX (this pass): the 3 APIs this file used to call (terabox.beer/api/
# terabox-new, terabox-free-dl.com/api/get, terabox-downloader.bot/api/
# resolve) are all dead/unreachable now, so every Terabox link failed with
# "All Terabox APIs failed". Replaced with the 5-tier resolver chain
# ported from the working Akbots/terabox.py (src--tera_api), reimplemented
# here with `requests` (sync) instead of aiohttp so the existing
# is_terabox_link/get_page_meta/get_available_qualities/get_stream_url/
# download_video call signatures below don't have to change — main.py
# already calls all of these via asyncio.to_thread, so a sync engine
# underneath is exactly what it expects.
#
# Order (each only tried if the previous one raises):
#   1. flowvideoplayer.com - CSRF-session-managed API (replaced xAPIverse —
#                        see _resolve_flowvideoplayer()'s docstring for why)
#   2. terabox.beer    - free, watch-page + API + redirect-chain scrape
#   3. anshapi.workers.dev - free Cloudflare Worker
#   4. azhawasadda.in  - free extractor, also the only tier with real
#                        per-quality (360p/480p/720p/1080p) stream URLs
#   5. guest page-scrape - last resort, regex-scans the raw share page
#
# FIX (this pass): added a 6th tier — Baidu PCS guest resolve, ported
# from src's Akbots/terabox.py — between azhawasadda and the guest
# scrape. src's version optionally loads an admin cookie via
# Akbots-only cookies_manager/cookie_utils for a higher speed tier, but
# its own comment notes that's optional: /api/shorturlinfo hands out a
# working guest session on its own regardless. So it's ported here
# without the cookie step (and without the Akbots dependency) — talks
# to TeraBox's own Baidu-PCS-compatible backend directly (not a
# third-party proxy), so it doesn't share flowvideoplayer/beer/ansh/
# azhawasadda's rate limits.
#
# Still not ported: src's "direct-API" tier (Akbots/terabox_lib/),
# since it needs an owned account's TERABOX_NDUS cookie and the
# Akbots-only terabox_lib module — 6 independent tiers is already a
# large reliability improvement over the previous 3 dead ones.
#
# REMOVED (this pass): xAPIverse (both its "PRO" and normal endpoints)
# — the PRO key this project had was never actually provisioned for PRO
# access ("Forbidden: Invalid token" on every call, confirmed live), so
# every "PRO" attempt silently wasted ~8s before falling through to the
# normal endpoint anyway, which xAPIverse deliberately throttles to push
# people onto a real PRO plan. Replaced tier 1 with flowvideoplayer.com
# instead — see _resolve_flowvideoplayer() below, ported from a working
# reference bot (main.py, a separate python-telegram-bot project) that
# uses flowvideoplayer's CSRF-token-gated /search/video API directly.

_BEER_BASE_URL = "https://terabox.beer"
_BEER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
    "Accept-Language": "en-MM,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
}

_ANSH_API_BASE = "https://terabox.anshapi.workers.dev/api/terabox-down"
_AZHAWASADDA_API_BASE = "https://azhawasadda.in/api/extract"

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

CACHE_DURATION = 300  # seconds
MIN_FILE_SIZE = 50 * 1024  # 50 KB - real videos are bigger

_session = requests.Session()
_session.headers.update({
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
})

_cache: Dict = {}  # url → {"ts": float, "result": dict}


# ── PUBLIC INTERFACE ─────────────────────────────────────────────────────────

def is_terabox_link(url: str) -> bool:
    """Check if URL is a valid Terabox link."""
    return bool(_TERABOX_DOMAIN_RE.search(url))


def extract_terabox_links(text: str) -> list:
    """Extract all Terabox links from text."""
    return _TERABOX_DOMAIN_RE.findall(text)


def get_page_meta(url: str) -> dict:
    """
    Extract metadata from Terabox link.
    Returns: {
        "poster_url", "duration", "title",
        "file_size", "extension", "category"
    }

    Strategy:
      1. _resolve() → first successful tier (fast, may or may not have filename)
      2. If no real filename, try Baidu PCS explicitly — it reliably returns
         server_filename (the actual uploaded name, not a hash or slug).
    """
    result = _resolve(url)
    meta = {}

    if result.get("poster_url"):
        meta["poster_url"] = result["poster_url"]
    if result.get("duration"):
        meta["duration"] = result["duration"]

    # Try to get a real title from the resolver result first
    raw_name = result.get("title") or result.get("file_name") or ""
    if raw_name and raw_name not in ("terabox_video", "download"):
        meta["title"] = raw_name
    if result.get("file_size"):
        meta["file_size"] = result["file_size"]

    # If no real filename yet, hit Baidu PCS which always returns server_filename
    if not meta.get("title"):
        try:
            pcs = _resolve_baidu_pcs(url)
            pcs_name = pcs.get("file_name") or pcs.get("title") or ""
            if pcs_name and pcs_name not in ("terabox_video", "download"):
                meta["title"] = pcs_name
            if not meta.get("file_size") and pcs.get("file_size"):
                meta["file_size"] = pcs["file_size"]
            if not meta.get("poster_url") and pcs.get("poster_url"):
                meta["poster_url"] = pcs["poster_url"]
        except Exception as e:
            logger.debug(f"[terabox] Baidu PCS metadata fallback failed: {e}")

    # Derive extension and category from filename
    title = meta.get("title", "")
    ext = os.path.splitext(title)[1].lower() if title else ""
    if not ext:
        # fallback: guess from URL or use .mp4
        slug = url.rstrip("/").split("/")[-1].split("?")[0]
        ext = os.path.splitext(slug)[1].lower() or ".mp4"
    meta["extension"] = ext

    video_exts = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
                  ".m4v", ".mpg", ".mpeg", ".3gp", ".ts", ".m2ts"}
    meta["category"] = "Video" if ext in video_exts else "File"

    return meta


def get_available_qualities(url: str) -> list:
    """Return the single resolved download link for a Terabox URL via
    the same multi-tier _resolve() chain get_page_meta/get_stream_url/
    download_video already use (flowvideoplayer -> terabox.beer ->
    anshapi -> azhawasadda -> Baidu PCS -> guest scrape).

    FIX: this used to call flowvideoplayer.com's /search/video API
    directly and ONLY that — a single tier with no fallback, so any
    flowvideoplayer hiccup (or a link it just can't handle, like a
    folder share) failed the whole step outright ("flowvideoplayer: no
    download_url in response"). But that response was never a real
    per-quality breakdown to begin with — every real response seen
    while building this had exactly one download_url, no resolution
    choice — so there was never an actual "quality" to fetch here, just
    one link to resolve. Routing through _resolve() keeps that same
    single-link behavior but with 5 more fallback tiers if
    flowvideoplayer itself is down or rejects this particular link —
    matching how every other terabox function already resolves.

    Returns a single-item [{"label": "Best", "url": ...}] list — the
    caller already treats a length-1 list as "skip the quality menu,
    download straight away" (unchanged)."""
    result = _resolve(url)
    download_link = result.get("proxy_url")
    if not download_link:
        raise RuntimeError(result.get("error") or "Terabox: couldn't resolve a download link for this URL.")
    return [{"label": "Best", "url": download_link}]


def get_stream_url(url: str) -> Optional[str]:
    """Get stream URL using the full _resolve() chain (flowvideoplayer
    first, then terabox.beer fallback, etc.) — previously used ONLY
    terabox.beer which meant flowvideoplayer was never tried for streams
    even though it's the faster/more reliable tier."""
    try:
        result = _resolve(url)
        return result.get("proxy_url")
    except Exception as e:
        logger.warning(f"terabox stream failed: {e}")
        return None


class _BadCandidate(Exception):
    """Raised internally to mean "this tier's link is bad/too slow, try
    the next one" — never escapes download_video() itself, which catches
    it and moves on to the next candidate."""
    pass


# If a candidate link hasn't managed at least this much throughput after
# _SLOW_GRACE_SECONDS, it's treated as a bad/throttled link rather than
# waited out — this is the actual fix for the reported symptom (a link
# that "succeeds" at resolve time but then crawls at ~35 KB/s and never
# meaningfully progresses): previously the code committed to whichever
# link the first non-erroring tier returned with no way to notice or
# recover from exactly this, even though a later tier's link often works
# fine and fast.
_SLOW_SPEED_BYTES_S = 150 * 1024
_SLOW_GRACE_SECONDS = 12

# ── Parallel multi-connection download settings ────────────────────────────
# Terabox CDN per-connection speed cap ~2–4 MB/s, but allows 4–8 simultaneous
# Range requests → real throughput 10–25 MB/s with parallel connections.
# Each "worker" downloads its own byte-range slice, then slices are joined.
_PARALLEL_CONNECTIONS  = int(os.environ.get("TERABOX_PARALLEL_CONN", "4"))
_PARALLEL_CHUNK_BYTES  = 8 * 1024 * 1024   # 8 MB per sub-chunk per worker
_MIN_SIZE_FOR_PARALLEL = 20 * 1024 * 1024  # only bother splitting files ≥ 20 MB


def _prepare_candidate_url(proxy_url: str) -> str:
    """Best-effort redirect-follow to the real CDN URL (xAPIverse's
    normal_dlink is often a redirect chain — following it gives the
    actual CDN URL with proper Content-Length and full speed). NEVER
    rejects a candidate here, even on a HEAD error status — several
    working tiers' CDNs don't implement HEAD at all (405) or briefly
    502 on it while still serving GET fine (confirmed in production:
    terabox.beer and azhawasadda were both getting killed here on HEAD
    405/502 and never even getting a real GET attempt, even though nothing
    confirms the GET itself would've failed). The actual GET in
    _attempt_download already validates status/content-type/size on its
    own — that's the real check; this is purely a best-effort speed-up
    when HEAD happens to work, never a gate."""
    if proxy_url.endswith(".m3u8"):
        return proxy_url  # handled by the HLS path in _attempt_download, no HEAD check
    try:
        head = _session.head(proxy_url, allow_redirects=True, timeout=15, verify=False)
        if head.status_code < 400 and head.url and str(head.url) != proxy_url:
            proxy_url = str(head.url)
            logger.info(f"Terabox: followed redirect to {proxy_url[:80]}…")
    except Exception:
        pass  # HEAD not supported by this CDN or similar — fine, the GET below still validates
    return proxy_url


def _check_range_support(url: str) -> tuple:
    """
    HEAD request to see if the CDN supports byte-range requests.
    Returns (supports_range: bool, total_bytes: int).
    Falls back gracefully — if HEAD fails or Accept-Ranges is missing,
    returns (False, 0) and _attempt_download will use the single-connection path.
    """
    try:
        head = _session.head(url, timeout=10, verify=True,
                             allow_redirects=True)
        accepts = head.headers.get("Accept-Ranges", "").lower()
        total   = int(head.headers.get("Content-Length", 0))
        supports = accepts == "bytes" and total > 0
        return supports, total
    except Exception:
        return False, 0


def _download_range_worker(url: str, start: int, end: int,
                           part_path: str) -> int:
    """
    Download bytes [start, end] of `url` into `part_path`.
    Returns number of bytes written, raises on failure.
    Called concurrently from _parallel_download().
    """
    headers = {"Range": f"bytes={start}-{end}"}
    resp = _session.get(url, headers=headers, stream=True,
                        timeout=60, verify=True)
    if resp.status_code not in (200, 206):
        resp.close()
        raise RuntimeError(f"Range request got HTTP {resp.status_code}")

    written = 0
    with open(part_path, "wb") as fh:
        for chunk in resp.iter_content(chunk_size=512 * 1024):
            if chunk:
                fh.write(chunk)
                written += len(chunk)
    resp.close()
    return written


def _parallel_download(url: str, out_path: str,
                       total: int, on_progress=None) -> str:
    """
    Split `total` bytes across `_PARALLEL_CONNECTIONS` workers, each
    downloading its own byte range concurrently, then stitch parts in order.

    Progress is reported in aggregate — the combined bytes across all workers
    are summed so the progress bar still advances smoothly.

    Falls back to a sequential single-connection download on any error
    (broken Range responses, mid-flight failures etc.) so the user always
    gets their file, just potentially slower.
    """
    n_conn  = _PARALLEL_CONNECTIONS
    part_size = (total + n_conn - 1) // n_conn  # ceiling division

    ranges = []
    start  = 0
    for _ in range(n_conn):
        end = min(start + part_size - 1, total - 1)
        ranges.append((start, end))
        start = end + 1
        if start >= total:
            break

    part_paths = [f"{out_path}.part{i}" for i in range(len(ranges))]

    # Shared counters for aggregate progress reporting
    written_per_part = [0] * len(ranges)
    lock = threading.Lock()
    start_time = time.time()
    last_report = [time.time()]
    # BUG FIX: Future.cancel() below only works on a future that hasn't
    # started running yet — with max_workers == len(ranges), every part
    # starts running almost immediately, so by the time one part fails
    # and this tries to "cancel" the rest, cancel() just silently no-ops
    # on all of them. The other connections kept running their full
    # download in the background regardless, and the `with
    # ThreadPoolExecutor(...)` block waits for all of them before this
    # function can even report the failure and fall back — a failed
    # parallel download wasn't actually failing fast, just eventually,
    # after every other part finished anyway. stop_event lets each
    # worker's own chunk loop notice a sibling failed and bail out on
    # its next iteration instead of running to completion regardless.
    stop_event = threading.Event()

    def _worker_with_progress(idx: int, rng: tuple, ppath: str) -> int:
        url_h    = {"Range": f"bytes={rng[0]}-{rng[1]}"}
        resp = _session.get(url, headers=url_h, stream=True,
                            timeout=60, verify=True)
        if resp.status_code not in (200, 206):
            resp.close()
            raise RuntimeError(f"Part {idx}: HTTP {resp.status_code}")
        written = 0
        with open(ppath, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=512 * 1024):
                if stop_event.is_set():
                    resp.close()
                    raise RuntimeError(f"Part {idx}: stopped — a sibling part failed")
                if not chunk:
                    continue
                fh.write(chunk)
                written += len(chunk)

                if on_progress:
                    with lock:
                        written_per_part[idx] = written
                        now = time.time()
                        if now - last_report[0] >= 1.0:
                            last_report[0] = now
                            total_done = sum(written_per_part)
                            pct = (total_done / total * 100) if total else 0
                            try:
                                on_progress({
                                    "pct": pct,
                                    "downloaded_bytes": total_done,
                                })
                            except Exception:
                                pass
        resp.close()
        return written

    logger.info(
        f"[terabox] Parallel download: {len(ranges)} connections, "
        f"{part_size // 1024 // 1024:.0f} MB each, total {total // 1024 // 1024:.0f} MB"
    )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(ranges)) as pool:
            futures = {
                pool.submit(_worker_with_progress, i, rng, ppath): i
                for i, (rng, ppath) in enumerate(zip(ranges, part_paths))
            }
            for fut in concurrent.futures.as_completed(futures):
                idx = futures[fut]
                try:
                    fut.result()
                except Exception as e:
                    # Signal every other in-flight worker to stop on its
                    # next chunk (see stop_event's comment above — this
                    # is what actually makes them exit early now, not
                    # the .cancel() calls below, which only help for any
                    # part that genuinely hadn't started yet).
                    stop_event.set()
                    for f in futures:
                        f.cancel()
                    raise RuntimeError(f"Part {idx} failed: {e}") from e

        # Stitch parts together in order
        with open(out_path, "wb") as out:
            for ppath in part_paths:
                with open(ppath, "rb") as p:
                    while True:
                        buf = p.read(4 * 1024 * 1024)  # 4 MB read buffer
                        if not buf:
                            break
                        out.write(buf)

        elapsed = time.time() - start_time
        speed_mb = (total / elapsed / 1024 / 1024) if elapsed > 0 else 0
        logger.info(
            f"[terabox] Parallel download complete: {total // 1024 // 1024:.0f} MB "
            f"in {elapsed:.1f}s → {speed_mb:.1f} MB/s"
        )
        return out_path

    except Exception as e:
        logger.warning(f"[terabox] Parallel download failed ({e}), falling back to single-connection")
        return None   # caller falls back to _attempt_download sequential path

    finally:
        for ppath in part_paths:
            try:
                os.remove(ppath)
            except OSError:
                pass


def _attempt_download(proxy_url: str, out_path: str, on_progress=None) -> str:
    """One candidate link's full download attempt: HEAD/redirect-resolve,
    HLS-vs-direct-file branch, streamed GET with content-type/size
    validation, and a throughput watchdog. Raises _BadCandidate for
    anything the caller should try the next tier for (bad content-type,
    too slow, empty/undersized result); RuntimeError for a hard failure
    even the caller shouldn't bother retrying (disk full, etc — though in
    practice download_video treats these the same way: try the next
    candidate anyway, since a different tier is still worth a shot)."""
    proxy_url = _prepare_candidate_url(proxy_url)

    # Some resolver tiers (terabox.beer, azhawasadda's per-quality
    # fast_stream_url) hand back an HLS (.m3u8) playlist instead of a
    # direct file — that's just a text manifest listing segment URLs, not
    # a video. Downloading it with a plain GET (like the direct-link path
    # below does) would save a few-KB text file with a ".mp4" name: it
    # passes the Content-Type check (m3u8's type isn't text/html/json),
    # slips through the extension-fix logic since m3u8 has no magic-byte
    # signature to sniff, and produces a broken "video" Telegram can't
    # play. ffmpeg -c copy remuxes the real segments into one playable
    # mp4 instead — same approach Akbots/terabox.py's
    # _download_hls_via_ffmpeg uses for the same reason.
    if ".m3u8" in proxy_url.lower():
        return _download_hls_via_ffmpeg(proxy_url, out_path, on_progress)

    logger.info(f"Downloading Terabox file: {proxy_url[:80]}…")

    try:
        resp = _session.get(proxy_url, stream=True, timeout=60, verify=True)
        resp.raise_for_status()
    except requests.exceptions.Timeout:
        raise _BadCandidate("request timed out after 60 seconds")
    except requests.exceptions.ConnectionError:
        raise _BadCandidate("connection failed")
    except Exception as e:
        raise _BadCandidate(f"request failed: {e}") from e

    # ── Validate Content-Type ──
    content_type = (resp.headers.get("Content-Type") or "").lower()
    if any(bad in content_type for bad in ("text/html", "application/json", "text/plain")):
        resp.close()
        raise _BadCandidate(f"returned error page instead of video (Content-Type: {content_type})")
    # Stripped of any "; charset=..." suffix, kept for the extension-
    # correction step once the download finishes below.
    _detected_content_type = content_type.split(";")[0].strip()

    # ── Validate Content-Length ──
    total = int(resp.headers.get("Content-Length", 0))
    if total > 0 and total < MIN_FILE_SIZE:
        resp.close()
        raise _BadCandidate(f"file too small ({total} bytes, need ≥{MIN_FILE_SIZE}) — looks like a thumbnail")

    if total > MAX_FILE_SIZE:
        resp.close()
        raise _BadCandidate(f"file too large ({total} bytes, max {MAX_FILE_SIZE}) — possible corrupted response")

    # ── Try parallel multi-connection download (much faster on capped CDNs) ──
    # Terabox CDN limits each TCP connection to ~2–4 MB/s but allows many
    # parallel Range requests. With 4 connections the real throughput is
    # typically 10–25 MB/s instead of 2–4 MB/s.
    # Conditions: file must be large enough to bother splitting, CDN must
    # have told us Accept-Ranges: bytes (checked via HEAD in _check_range_support).
    if total >= _MIN_SIZE_FOR_PARALLEL:
        resp.close()  # release the streaming GET we opened just for validation
        supports_range, _ = _check_range_support(proxy_url)
        if supports_range:
            result = _parallel_download(proxy_url, out_path, total, on_progress)
            if result is not None:
                # Parallel succeeded — validate and return
                file_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
                if file_size >= MIN_FILE_SIZE:
                    logger.info(f"[terabox] Parallel download validated: {file_size} bytes")
                    return _fix_extension(out_path, _detected_content_type)
                else:
                    logger.warning(f"[terabox] Parallel download incomplete ({file_size} bytes), retrying sequentially")
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
            # Parallel failed or incomplete — fall through to sequential path
            logger.info("[terabox] Falling back to single-connection sequential download")
        # Re-open the streaming GET for the sequential path
        try:
            resp = _session.get(proxy_url, stream=True, timeout=60, verify=True)
            resp.raise_for_status()
        except Exception as e:
            raise _BadCandidate(f"sequential fallback request failed: {e}") from e

    downloaded = 0
    last_report = time.time()
    start_time = time.time()
    _first_chunk = True
    _speed_checked = False

    try:
        with open(out_path, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=1024 * 512):  # 512 KB chunks
                if not chunk:
                    continue

                if _first_chunk:
                    _first_chunk = False
                    # Sniff first bytes for HTML/JSON error pages
                    head = chunk[:32].lstrip()
                    if (head.startswith(b"<!DOCTYPE") or
                        head.startswith(b"<html") or
                        head.startswith(b"{\"") or
                        head.startswith(b"{'") or
                        (head.startswith(b"<") and b"error" in chunk[:200].lower())):
                        resp.close()
                        try:
                            os.remove(out_path)
                        except OSError:
                            pass
                        raise _BadCandidate("API returned error page instead of video")

                fh.write(chunk)
                downloaded += len(chunk)

                now = time.time()
                elapsed = now - start_time

                # Throughput watchdog — one-shot check once past the grace
                # period, not on every chunk (a link that's merely a bit
                # slow after the check shouldn't get re-punished forever;
                # this is specifically for "never really got going at
                # all").
                if not _speed_checked and elapsed >= _SLOW_GRACE_SECONDS:
                    _speed_checked = True
                    speed = downloaded / elapsed
                    if speed < _SLOW_SPEED_BYTES_S:
                        resp.close()
                        try:
                            os.remove(out_path)
                        except OSError:
                            pass
                        raise _BadCandidate(
                            f"too slow ({speed / 1024:.1f} KB/s after {elapsed:.0f}s, "
                            f"need ≥{_SLOW_SPEED_BYTES_S / 1024:.0f} KB/s)"
                        )

                if on_progress and (now - last_report) >= 1.0:
                    last_report = now
                    pct = (downloaded / total * 100) if total else 0
                    try:
                        on_progress({"pct": pct, "downloaded_bytes": downloaded})
                    except Exception:
                        pass

    except IOError as e:
        raise RuntimeError(f"Disk write error: {e}") from e
    finally:
        resp.close()

    # ── Final file validation ──
    file_size = os.path.getsize(out_path) if os.path.exists(out_path) else 0

    if file_size == 0:
        try:
            os.remove(out_path)
        except OSError:
            pass
        raise _BadCandidate("download produced an empty file")

    if file_size < MIN_FILE_SIZE:
        try:
            os.remove(out_path)
        except OSError:
            pass
        raise _BadCandidate(f"downloaded file too small ({file_size} bytes, need ≥{MIN_FILE_SIZE})")

    if file_size > MAX_FILE_SIZE:
        try:
            os.remove(out_path)
        except OSError:
            pass
        raise _BadCandidate(f"downloaded file too large ({file_size} bytes)")

    logger.info(f"Terabox download complete: {out_path} ({file_size} bytes)")
    return _fix_extension(out_path, _detected_content_type)


def download_video(url: str, out_path: str,
                   on_progress=None, stream_url: str = None, strict_stream_url: bool = False) -> str:
    """
    Download a Terabox file, trying candidate links from every resolver
    tier IN ORDER until one actually downloads successfully at a
    reasonable pace — not just until one resolves without raising (see
    _iter_resolve_candidates' docstring for why that distinction matters;
    it's the fix for a link that resolves fine but then crawls at a few
    KB/s or hangs, which used to mean the whole download just sat there
    instead of moving on to a tier that works).

    Terabox is a general cloud-storage share, not a video-only site — the
    caller's out_path assumes ".mp4" (every other backend in this project
    IS video-only, so that assumption is safe there), but a Terabox share
    can be a zip, pdf, image, or any other file type. _attempt_download
    detects the real type from the response's Content-Type and renames
    the file to the correct extension before returning, instead of
    silently handing back e.g. a PDF saved with a ".mp4" name (which
    uploads fine to Telegram, but the recipient's player/viewer for the
    WRONG type then fails to open it).

    strict_stream_url: when True, a failing/slow stream_url raises
    instead of falling through to the full tier chain below. Needed for
    per-file folder downloads (_send_terabox_folder in main.py): `url`
    there is the FOLDER's share link, the SAME one for every file in it —
    falling through to _iter_resolve_candidates(url) would resolve that
    share link fresh through the single-file tiers, which return the
    share's FIRST/default file, not the specific file whose stream_url
    just failed. Silently swapping in the wrong file's bytes under the
    right filename is worse than just failing that one file outright and
    letting the folder loop's own try/except skip it — confirmed this
    was possible: every folder-file call passed the same share `url`
    with a per-file stream_url, with nothing stopping exactly this mix-up
    on any file whose direct CDN link happened to be flaky.
    """
    if stream_url:
        # Respect the user's explicit quality pick first, but don't dead-
        # end on it — if THIS specific link turns out to be the slow/bad
        # one (the exact symptom reported: a quality menu's chosen link
        # crawls at a few KB/s), fall through to the full tier chain
        # afterward rather than failing outright. That can mean the
        # actual downloaded quality differs slightly from what was
        # picked, but a slightly-different-quality file beats a download
        # that never finishes. (Skipped entirely when strict_stream_url —
        # see the docstring above for why that matters for folder files.)
        if strict_stream_url:
            candidates = [("explicit", {"proxy_url": stream_url})]
        else:
            candidates = [("explicit", {"proxy_url": stream_url})] + list(_iter_resolve_candidates(url, bypass_cache=True))
    else:
        candidates = list(_iter_resolve_candidates(url, bypass_cache=True))

    if not candidates:
        raise RuntimeError("Could not resolve Terabox download URL (all tiers exhausted).")

    errors = []
    for tier_name, result in candidates:
        if result.get("error"):
            errors.append(f"{tier_name}: {result['error']}")
            continue
        proxy_url = result.get("proxy_url")
        if not proxy_url:
            continue
        try:
            return _attempt_download(proxy_url, out_path, on_progress)
        except _BadCandidate as e:
            logger.warning(f"Terabox: {tier_name}'s link rejected ({e}) — trying next tier")
            errors.append(f"{tier_name}: {e}")
            continue
        except RuntimeError as e:
            # A hard failure (e.g. disk error) — still worth trying the
            # next tier rather than giving up outright, since it's a
            # different link/server entirely.
            logger.warning(f"Terabox: {tier_name} failed ({e}) — trying next tier")
            errors.append(f"{tier_name}: {e}")
            continue

    raise RuntimeError("All Terabox candidates failed or were too slow (" + " | ".join(errors) + ")")


# A few common overrides where mimetypes' own guess is unhelpful/generic
# (e.g. it doesn't know video/x-matroska -> .mkv on every platform, and
# "application/octet-stream" — a very common generic fallback Content-Type
# for direct-download links — isn't resolvable via mimetypes at all).
_EXTENSION_OVERRIDES = {
    "video/mp4": ".mp4", "video/x-matroska": ".mkv", "video/webm": ".webm",
    "video/quicktime": ".mov", "video/x-msvideo": ".avi", "video/mpeg": ".mpeg",
    "video/x-flv": ".flv", "video/3gpp": ".3gp",
    "application/pdf": ".pdf", "application/zip": ".zip",
    "application/x-rar-compressed": ".rar", "application/x-7z-compressed": ".7z",
    "application/vnd.rar": ".rar",
    "application/msword": ".doc",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
    "image/jpeg": ".jpg", "image/png": ".png", "image/gif": ".gif", "image/webp": ".webp",
    "audio/mpeg": ".mp3", "audio/mp4": ".m4a", "audio/x-wav": ".wav",
}

# Magic-byte signatures for the sniff-fallback, used only when
# Content-Type is missing or a generic value like
# "application/octet-stream" that doesn't tell us anything — checked
# against the first bytes of the file actually written to disk.
_MAGIC_SIGNATURES = [
    (b"\x00\x00\x00\x18ftyp", ".mp4"), (b"\x00\x00\x00\x1cftyp", ".mp4"),
    (b"\x1aE\xdf\xa3", ".mkv"),  # also matches .webm; .mkv is the safer generic default
    (b"PK\x03\x04", ".zip"), (b"%PDF-", ".pdf"),
    (b"\xff\xd8\xff", ".jpg"), (b"\x89PNG\r\n\x1a\n", ".png"),
    (b"Rar!\x1a\x07", ".rar"), (b"7z\xbc\xaf\x27\x1c", ".7z"),
]


def _sniff_extension(path: str) -> Optional[str]:
    try:
        with open(path, "rb") as fh:
            head = fh.read(32)
    except OSError:
        return None
    for signature, ext in _MAGIC_SIGNATURES:
        if head.startswith(signature):
            return ext
    return None


_GENERIC_CONTENT_TYPES = {"application/octet-stream", "binary/octet-stream", ""}


def _fix_extension(out_path: str, content_type: str) -> str:
    """Renames out_path to match its real file type if content_type (or,
    failing that, the file's own magic bytes) disagrees with the
    extension the caller originally chose. Returns out_path unchanged if
    everything already matches, or if no better extension could be
    determined (keeps the original rather than guessing further)."""
    if content_type in _GENERIC_CONTENT_TYPES:
        real_ext = _sniff_extension(out_path)
    else:
        real_ext = _EXTENSION_OVERRIDES.get(content_type) or mimetypes.guess_extension(content_type)
        if not real_ext or real_ext == ".jpe":  # mimetypes' .jpe is the same thing as .jpg, just spelled oddly
            real_ext = _sniff_extension(out_path) or real_ext

    if not real_ext:
        return out_path  # nothing more reliable than the caller's own guess

    current_ext = os.path.splitext(out_path)[1].lower()
    if current_ext == real_ext.lower():
        return out_path

    corrected_path = os.path.splitext(out_path)[0] + real_ext
    try:
        os.replace(out_path, corrected_path)
        logger.info(f"Terabox: corrected extension {current_ext or '(none)'} -> {real_ext} "
                    f"(Content-Type: {content_type or 'unknown'})")
        return corrected_path
    except OSError as e:
        logger.warning(f"Terabox: extension rename failed, keeping {out_path}: {e}")
        return out_path


def _download_hls_via_ffmpeg(m3u8_url: str, out_path: str, on_progress=None) -> str:
    """Remux an HLS (.m3u8) stream straight into a real mp4 via
    `ffmpeg -c copy` (no re-encoding — just repackages the existing
    segments, so it's fast and lossless). Reports coarse progress from
    ffmpeg's own stderr `time=` field since HLS has no Content-Length to
    track bytes against. Ported from Akbots/terabox.py's
    _download_hls_via_ffmpeg, trimmed to this project's plain
    on_progress({"pct":...,"downloaded_bytes":...}) callback shape."""
    import subprocess

    out_path = os.path.splitext(out_path)[0] + ".mp4"  # HLS remux is always mp4
    cmd = [
        "ffmpeg", "-y", "-hide_banner", "-loglevel", "error", "-progress", "pipe:1",
        # Same fix as faphouse_downloader.py's ffmpeg call: without an
        # explicit timeout, a slow/unresponsive CDN leaves ffmpeg hanging
        # forever on its initial connection with zero output — nothing
        # to catch, retry, or even see happening. -rw_timeout fails fast
        # instead (20s); -reconnect* has ffmpeg itself ride out a brief
        # mid-download stall/drop rather than losing all progress over
        # a hiccup.
        "-rw_timeout", "20000000",  # microseconds = 20s
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "5",
        "-i", m3u8_url, "-c", "copy", "-bsf:a", "aac_adtstoasc", out_path,
    ]

    duration_s = None
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-show_entries", "format=duration",
             "-of", "default=noprint_wrappers=1:nokey=1", m3u8_url],
            capture_output=True, text=True, timeout=20,
        )
        duration_s = float(probe.stdout.strip())
    except Exception:
        duration_s = None  # progress just won't report a % — remux still proceeds fine

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                 text=True, bufsize=1)
    except FileNotFoundError as e:
        raise RuntimeError("ffmpeg is not installed — required to download HLS (.m3u8) Terabox streams.") from e

    for line in proc.stdout:
        line = line.strip()
        if line.startswith("out_time_ms=") and on_progress:
            try:
                current_s = int(line.split("=", 1)[1]) / 1_000_000
                if duration_s:
                    try:
                        on_progress({"pct": min(current_s / duration_s * 100, 100), "downloaded_bytes": 0})
                    except Exception:
                        pass
            except (ValueError, ZeroDivisionError):
                pass

    stderr = proc.stderr.read()
    proc.wait()

    if proc.returncode != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) < MIN_FILE_SIZE:
        try:
            os.remove(out_path)
        except OSError:
            pass
        raise RuntimeError(f"ffmpeg failed to remux HLS stream: {stderr[-500:] if stderr else 'no output produced'}")

    logger.info(f"Terabox HLS remux complete: {out_path} ({os.path.getsize(out_path)} bytes)")
    return out_path


# ── INTERNAL HELPERS ─────────────────────────────────────────────────────────

def _generic_extract_stream_info(raw_text: str) -> Optional[dict]:
    """Schema-agnostic last-resort extractor, ported from Akbots/terabox.py
    — tries every response shape seen across Terabox third-party APIs
    before giving up: JSON dict -> common url-ish keys -> same keys
    nested under 'data'/'result' -> any URL-looking string value ->
    raw-text regex scan for an .m3u8/.mp4 URL. Used as the tail of
    several tiers below when a response doesn't match that tier's usual
    schema (e.g. after a free API silently changes its field names)."""
    url_keys = ("url", "stream_url", "play_url", "video_url", "normal_dlink", "dlink")
    name_keys = ("filename", "title", "name", "file_name")

    try:
        data = json.loads(raw_text)
    except Exception:
        data = None

    if isinstance(data, dict):
        stream_url, filename = None, None
        for key in url_keys:
            if data.get(key):
                stream_url = data[key]
                break
        nested = data.get("data") if isinstance(data.get("data"), dict) else (
            data.get("result") if isinstance(data.get("result"), dict) else None)
        if not stream_url and nested:
            for key in url_keys:
                if nested.get(key):
                    stream_url = nested[key]
                    break
        if not stream_url:
            for value in data.values():
                if isinstance(value, str) and value.startswith(("http://", "https://")):
                    stream_url = value
                    break
        for key in name_keys:
            if data.get(key):
                filename = data[key]
                break
        if stream_url:
            return {"download_link": stream_url, "name": filename or "download"}

    text = (raw_text or "").replace("\\/", "/")
    for pattern in (
        r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)',
        r'(https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*)',
    ):
        m = re.search(pattern, text)
        if m:
            return {"download_link": m.group(1), "name": "download"}

    return None


def _normalize_tier_result(download_link: str, name: str = None, size: str = None,
                            thumb: str = None, qualities: dict = None) -> dict:
    """Every tier below returns its own raw shape — this reshapes any of
    them into the one schema get_page_meta/get_available_qualities/
    get_stream_url/download_video already expect (unchanged from before
    this fix)."""
    fallback_urls = list(qualities.values()) if qualities else []
    return {
        "proxy_url": download_link,
        "file_name": name or "terabox_video",
        "file_size": size or "",
        "poster_url": thumb or None,
        "duration": None,
        "title": name or None,
        "bitrate": None,
        "fallback_urls": fallback_urls,
    }


# ── flowvideoplayer.com — CSRF-session-managed API (tier 1) ─────────────────
# Ported from a separate, working reference bot (a python-telegram-bot +
# telethon project, unrelated to this one's pyrogram/kurigram stack — only
# the resolving logic below was ported, not its Telegram-side code) that
# talks to flowvideoplayer.com's own frontend API directly. Every piece of
# this — the CSRF-token page-scrape, the /device/init fingerprint call, the
# specific retry-on-419/401/403/"Direct access blocked" handling — was
# copied from that bot's own working implementation, not guessed: this site
# actively rejects requests that skip any of these steps, the same way
# xAPIverse's PRO tier rejected a plain API-key call — but here the actual
# correct sequence was available to copy instead of reverse-engineered.
_FLOWVIDEOPLAYER_SITE_URL = "https://flowvideoplayer.com"
_FLOWVIDEOPLAYER_API_URL = "https://flowvideoplayer.com/search/video"
_FLOWVIDEOPLAYER_UA = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/152.0.0.0 Mobile Safari/537.36"
)
_FLOWVIDEOPLAYER_TOKEN_TTL = 300  # refresh a token if older than 5 min
_FLOWVIDEOPLAYER_MAX_REFRESH_ATTEMPTS = 4
_FLOWVIDEOPLAYER_BACKOFF_BASE = 0.7


def _flowvideoplayer_fresh_session_and_csrf():
    """Creates a NEW session, GETs the homepage to grab cookies + CSRF
    token (from either a <meta name="csrf-token"> tag or the XSRF-TOKEN
    cookie), then fires the /device/init fingerprint call the site
    requires before /search/video will respond at all (skip this and
    every search call comes back {"code":201,"message":"...blocked..."}
    regardless of how valid the CSRF token itself is). Returns (session,
    csrf_token) or None on failure — both from the SAME session so its
    cookies match the token."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": _FLOWVIDEOPLAYER_UA,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Accept-Encoding": "gzip, deflate, br",
    })

    try:
        resp = session.get(_FLOWVIDEOPLAYER_SITE_URL, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"flowvideoplayer: homepage fetch failed: {e}")
        return None

    soup = BeautifulSoup(resp.text, "html.parser")
    csrf = None
    meta = soup.find("meta", {"name": "csrf-token"})
    if meta and meta.get("content"):
        csrf = meta["content"]
    if not csrf:
        xsrf = session.cookies.get("XSRF-TOKEN")
        if xsrf:
            csrf = requests.utils.unquote(xsrf)
    if not csrf:
        logger.error("flowvideoplayer: CSRF token not found in page")
        return None

    try:
        init_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRF-TOKEN": csrf,
            "User-Agent": _FLOWVIDEOPLAYER_UA,
            "Referer": _FLOWVIDEOPLAYER_SITE_URL + "/",
            "Origin": _FLOWVIDEOPLAYER_SITE_URL,
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }
        session.post(
            _FLOWVIDEOPLAYER_SITE_URL + "/device/init",
            json={
                "cpu": 8, "memory": 8, "touch": 0, "platform": "Linux x86_64",
                "lang": "en-US", "vendor": "Google Inc.", "webgl_vendor": None,
                "webgl_renderer": None, "ua": _FLOWVIDEOPLAYER_UA,
                "backup_token": None, "os": "linux", "browser": "chrome",
                "pwa_installed": False,
            },
            headers=init_headers,
            timeout=30,
        )
    except Exception as e:
        logger.warning(f"flowvideoplayer: device/init failed (continuing): {e}")

    return session, csrf


class _FlowVideoPlayerCsrfManager:
    """Thread-safe CSRF token cache with automatic refresh on invalidation
    — same class as the reference bot's CsrfManager, ported as-is. Tokens
    are one-shot/short-lived on flowvideoplayer.com (expire, get
    invalidated after use, or mismatch with HTTP 419 when session cookies
    drift), so every call goes through post_json() below rather than
    reading self._csrf directly, to get the transparent refresh-and-retry
    behavior."""

    def __init__(self, ttl_seconds: int = _FLOWVIDEOPLAYER_TOKEN_TTL):
        self._ttl = ttl_seconds
        self._lock = threading.Lock()
        self._session = None
        self._csrf = None
        self._created = 0.0

    def _api_headers(self, csrf: str) -> dict:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Requested-With": "XMLHttpRequest",
            "X-CSRF-TOKEN": csrf,
            "User-Agent": _FLOWVIDEOPLAYER_UA,
            "Referer": _FLOWVIDEOPLAYER_SITE_URL + "/",
            "Origin": _FLOWVIDEOPLAYER_SITE_URL,
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Dest": "empty",
        }

    def _fetch_fresh(self) -> bool:
        ctx = _flowvideoplayer_fresh_session_and_csrf()
        if not ctx:
            return False
        self._session, self._csrf = ctx
        self._created = time.time()
        return True

    def invalidate(self):
        with self._lock:
            self._session = None
            self._csrf = None
            self._created = 0.0

    def post_json(self, url: str, payload: dict):
        """POST JSON using the managed token, automatically refreshing and
        retrying on HTTP 419/401/403 or a {"code":201,"...blocked..."}
        body (flowvideoplayer's way of saying the device fingerprint was
        rejected). Returns (response, error_message) — response is None
        only if every attempt failed outright (network error)."""
        last_resp = None
        for attempt in range(_FLOWVIDEOPLAYER_MAX_REFRESH_ATTEMPTS):
            with self._lock:
                if not self._session or not self._csrf or (time.time() - self._created) >= self._ttl:
                    self._fetch_fresh()
                if not self._session or not self._csrf:
                    return None, "Could not obtain a CSRF token"
                session, csrf = self._session, self._csrf

            headers = self._api_headers(csrf)
            try:
                resp = session.post(url, json=payload, headers=headers, timeout=30)
            except Exception as e:
                self.invalidate()
                if attempt == _FLOWVIDEOPLAYER_MAX_REFRESH_ATTEMPTS - 1:
                    return None, f"network error: {e}"
                time.sleep(_FLOWVIDEOPLAYER_BACKOFF_BASE * (attempt + 1) + random.uniform(0, 0.3))
                continue

            last_resp = resp

            if resp.status_code in (419, 401, 403):
                self.invalidate()
                if attempt == _FLOWVIDEOPLAYER_MAX_REFRESH_ATTEMPTS - 1:
                    break
                time.sleep(_FLOWVIDEOPLAYER_BACKOFF_BASE * (attempt + 1) + random.uniform(0, 0.3))
                continue

            if resp.status_code == 200 and resp.content:
                try:
                    body = resp.json()
                except Exception:
                    body = None
                if body is not None and body.get("code") == 201 and "blocked" in str(body.get("message", "")).lower():
                    self.invalidate()
                    if attempt == _FLOWVIDEOPLAYER_MAX_REFRESH_ATTEMPTS - 1:
                        break
                    time.sleep(_FLOWVIDEOPLAYER_BACKOFF_BASE * (attempt + 1) + random.uniform(0, 0.3))
                    continue

            return resp, None

        return last_resp, "CSRF/device token still failing after multiple attempts"


# Module-level instance — one shared token cache/session, same as the
# reference bot's module-level `_csrf`.
_flowvideoplayer_csrf = _FlowVideoPlayerCsrfManager()


def _resolve_flowvideoplayer(url: str) -> dict:
    """Tier 1 — flowvideoplayer.com. Replaced xAPIverse entirely (see the
    big comment block above _BEER_BASE_URL for why). Only ever returns
    ONE download link per video — flowvideoplayer's /search/video
    response has no per-quality breakdown the way xAPIverse's PRO tier
    claimed to (and azhawasadda genuinely does, tier 4) — so there's no
    per-quality "fallback_urls" to populate here, just the single best
    link the site hands back."""
    resp, err = _flowvideoplayer_csrf.post_json(_FLOWVIDEOPLAYER_API_URL, {"url": url})
    if resp is None:
        raise ValueError(f"flowvideoplayer: {err}")
    if resp.status_code != 200:
        raise ValueError(f"flowvideoplayer: HTTP {resp.status_code}" + (f" ({err})" if err else ""))

    try:
        data = resp.json()
    except Exception as e:
        raise ValueError(f"flowvideoplayer: invalid JSON response: {e}")

    if not (data.get("code") == 200 and data.get("status") and data.get("response")):
        raise ValueError(f"flowvideoplayer: {data.get('message') or 'no response data'}")

    info = data["response"][0]
    download_link = info.get("download_url", "")
    if not download_link:
        raise ValueError("flowvideoplayer: no download_url in response")

    return _normalize_tier_result(
        download_link, info.get("file_name"), info.get("file_size"),
    )


def _beer_extract_video_id(url: str) -> Optional[str]:
    for pattern in (r"/s/([a-zA-Z0-9_-]+)", r"share\.com/s/([a-zA-Z0-9_-]+)", r"file\.com/s/([a-zA-Z0-9_-]+)"):
        m = re.search(pattern, url)
        if m:
            return m.group(1)
    return None


def _beer_extract_m3u8_url(text: str) -> Optional[str]:
    m = re.search(r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)', text)
    return m.group(1) if m else None


def _beer_follow_redirects(session, url: str, max_redirects: int = 5) -> dict:
    current_url = url
    for _ in range(max_redirects):
        try:
            response = session.get(
                current_url, headers=_BEER_HEADERS | {"Referer": _BEER_BASE_URL + "/"},
                allow_redirects=False, timeout=30, verify=False,
            )
        except Exception:
            return {"final_url": current_url, "m3u8_url": None}
        if response.status_code in (301, 302, 303, 307, 308):
            location = response.headers.get("Location")
            if location:
                if location.startswith("/"):
                    parsed = urllib.parse.urlparse(current_url)
                    location = f"{parsed.scheme}://{parsed.netloc}{location}"
                current_url = location
                continue
        m3u8_url = _beer_extract_m3u8_url(response.text) if response.text else None
        return {"final_url": current_url, "m3u8_url": m3u8_url}
    return {"final_url": current_url, "m3u8_url": None}


def _resolve_beer(url: str) -> dict:
    """Tier 2 — terabox.beer, free, no API key. Warms a session against
    the site's home + watch page (same 2-hop sequence the site itself
    does) before calling its API, then follows redirects looking for a
    playable .m3u8. Ported from Akbots/terabox.py's _beer_resolve_sync."""
    video_id = _beer_extract_video_id(url)
    if not video_id:
        raise ValueError("terabox.beer: could not extract video ID from the link")

    session = requests.Session()
    session.verify = False
    session.get(_BEER_BASE_URL, headers=_BEER_HEADERS | {"Referer": "https://www.google.com/"}, timeout=30, verify=False)
    watch_url = f"{_BEER_BASE_URL}/watch/{video_id}"
    session.get(watch_url, headers=_BEER_HEADERS | {"Referer": _BEER_BASE_URL + "/"}, timeout=30, verify=False)

    encoded_url = urllib.parse.quote(url, safe="")
    api_url = f"{_BEER_BASE_URL}/api/terabox-new?link={encoded_url}"
    resp = session.get(api_url, headers=_BEER_HEADERS | {"Referer": watch_url}, timeout=30, verify=False)

    try:
        api_result = resp.json()
    except Exception:
        generic = _generic_extract_stream_info(resp.text or "")
        if generic:
            return _normalize_tier_result(generic["download_link"], generic["name"])
        raise ValueError("terabox.beer: failed to parse API response")

    # BUG FIX: the old check was `api_result.get("error") is not False` which
    # incorrectly rejects ANY response that doesn't have an explicit "error": false
    # field — including valid success responses that simply omit the key entirely.
    # Correct check: only fail if the error field is truthy (an actual error string/code).
    if not isinstance(api_result, dict):
        raise ValueError("terabox.beer: API returned non-dict response")
    if api_result.get("error"):
        error_msg = api_result.get("error") or api_result.get("message") or "Unknown error"
        raise ValueError(f"terabox.beer: API request failed: {error_msg}")

    video_url = None
    for field in ("stream_download_url", "download_link", "fallback_url", "proxy_url", "url", "video_url"):
        if api_result.get(field):
            video_url = api_result[field]
            break
    if not video_url:
        for value in api_result.values():
            if isinstance(value, str) and value.startswith(("http://", "https://")):
                video_url = value
                break
    if not video_url:
        generic = _generic_extract_stream_info(resp.text or "")
        video_url = generic["download_link"] if generic else None
    if not video_url:
        raise ValueError("terabox.beer: no video URL found in API response")

    redirect_result = _beer_follow_redirects(session, video_url)
    final_url = redirect_result["m3u8_url"] or video_url
    return _normalize_tier_result(
        final_url, api_result.get("file_name"),
        str(api_result.get("file_size", "")),
    )


def _resolve_ansh(url: str) -> dict:
    """Tier 3 — anshapi.workers.dev, free Cloudflare Worker, no API key."""
    api_url = f"{_ANSH_API_BASE}?url={urllib.parse.quote(url, safe='')}"
    resp = _session.get(api_url, timeout=30)
    # BUG FIX: missing HTTP error check — a 4xx/5xx response would silently
    # fall through to JSON parsing and produce a confusing "no usable link" error
    # instead of the real reason (API down, rate-limited, etc.)
    if resp.status_code >= 400:
        raise ValueError(f"anshapi: HTTP {resp.status_code}")
    raw_text = resp.text

    try:
        data = json.loads(raw_text)
    except Exception:
        data = None

    def _pick(d: dict) -> Optional[dict]:
        link = None
        for key in ("download_link", "download_url", "direct_link", "url", "dlink"):
            if d.get(key):
                link = d[key]
                break
        if not link:
            return None
        name = None
        for key in ("file_name", "filename", "name", "title"):
            if d.get(key):
                name = d[key]
                break
        size = None
        for key in ("file_size", "size"):
            if d.get(key):
                size = str(d[key])
                break
        return _normalize_tier_result(link, name, size, d.get("thumbnail") or d.get("thumb"))

    if isinstance(data, dict):
        result = _pick(data)
        if not result and isinstance(data.get("files"), list) and data["files"]:
            result = _pick(data["files"][0])
        if not result and isinstance(data.get("data"), dict):
            result = _pick(data["data"])
        if result:
            return result

    generic = _generic_extract_stream_info(raw_text or "")
    if generic:
        return _normalize_tier_result(generic["download_link"], generic["name"])

    raise ValueError("anshapi: no usable download link in response")


def _resolve_azhawasadda(url: str) -> dict:
    """Tier 4 — azhawasadda.in, free, no API key. The only tier here with
    real per-quality (360p/480p/720p/1080p) stream URLs, so its qualities
    dict feeds get_available_qualities()'s fallback_urls list."""
    api_url = f"{_AZHAWASADDA_API_BASE}?url={urllib.parse.quote(url, safe='')}"
    headers = {
        "User-Agent": ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36"),
        "Accept": "*/*",
    }
    resp = _session.get(api_url, headers=headers, timeout=30)
    # BUG FIX: missing HTTP error check
    if resp.status_code >= 400:
        raise ValueError(f"azhawasadda: HTTP {resp.status_code}")

    try:
        data = json.loads(resp.text)
    except Exception:
        raise ValueError("azhawasadda: couldn't parse API response")

    if data.get("errno"):
        raise ValueError(data.get("errmsg") or f"azhawasadda: errno {data.get('errno')}")

    file_info = ((data.get("data") or {}).get("file")) or {}
    download_link = file_info.get("direct_link") or file_info.get("download_url")
    if not download_link:
        raise ValueError("azhawasadda: no usable download link in response")

    qualities = file_info.get("fast_stream_url") or {}
    return _normalize_tier_result(
        download_link, file_info.get("file_name"),
        file_info.get("size_readable") or data.get("total_size"),
        file_info.get("thumbnail"), qualities,
    )


_BAIDU_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
_BAIDU_SURL_RE = re.compile(r"/s/([a-zA-Z0-9_-]+)")

_TERADL_1024_API = "https://1024teradl.com/api/proxy"
_HNN_WORKERS_API = "https://terabox.hnn.workers.dev/api/get-info?shorturl="
_NEPCODER_WORKERS_API = "https://teraboxvideodownloader.nepcoderdevs.workers.dev/?url="


def _resolve_1024teradl(url: str) -> dict:
    """Tier — 1024teradl.com's proxy endpoint, POST {"url": ...}. Free,
    no API key. Response shape varies (dlink/direct_link/url, or a
    response[] list with a per-quality resolutions dict) — same
    multi-shape handling TeraboxBot-main's api_handler.py used."""
    domain = _TERADL_1024_API.split("/api")[0]
    headers = {
        "Origin": domain,
        "Referer": f"{domain}/",
        "Accept": "application/json",
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"),
    }
    resp = _session.post(_TERADL_1024_API, json={"url": url}, headers=headers, timeout=15)
    if resp.status_code >= 400:
        raise ValueError(f"1024teradl: HTTP {resp.status_code}")

    try:
        data = resp.json()
    except Exception:
        raise ValueError("1024teradl: couldn't parse API response")

    link = data.get("dlink") or data.get("direct_link") or data.get("url")
    name = None
    if not link and isinstance(data.get("response"), list) and data["response"]:
        entry = data["response"][0]
        resolutions = entry.get("resolutions") or {}
        link = resolutions.get("Fast Download") or entry.get("link")
        name = entry.get("title") or entry.get("filename")
    if not link:
        raise ValueError("1024teradl: no usable download link in response")
    return _normalize_tier_result(link, name)


def _hnn_workers_list_files(url: str) -> list:
    """Folder-aware counterpart to _resolve_hnn_workers() — that function
    only ever takes file_list[0], discarding every other file when the
    share is actually a folder. This returns every entry in the same
    "list" field, normalized to get_all_folder_files()'s own
    {"name","url","size"} shape, so it can serve as a fallback for that
    function when the direct Baidu-PCS calls fail (e.g. blocked egress
    on some hosts — Render in particular has been seen 403ing direct
    1024terabox.com calls while still reaching this Cloudflare Worker
    fine, since it's a different domain entirely)."""
    match = _BAIDU_SURL_RE.search(url)
    if not match:
        raise ValueError("hnn.workers.dev: couldn't extract a shorturl (/s/<id>) from this link")
    api_url = f"{_HNN_WORKERS_API}{match.group(1)}"
    resp = _session.get(api_url, timeout=15)
    if resp.status_code >= 400:
        raise ValueError(f"hnn.workers.dev: HTTP {resp.status_code}")

    try:
        data = resp.json()
    except Exception:
        raise ValueError("hnn.workers.dev: couldn't parse API response")

    file_list = data.get("list") or []
    if not file_list:
        raise ValueError("hnn.workers.dev: no usable download link in response")

    results = []
    for entry in file_list:
        link = entry.get("dlink")
        if not link:
            continue
        results.append({
            "name": entry.get("filename") or f"file_{entry.get('fs_id') or len(results)}",
            "url": link,
            "size": int(entry.get("size") or 0),
        })
    if not results:
        raise ValueError("hnn.workers.dev: no usable download link in response")
    return results


def _resolve_hnn_workers(url: str) -> dict:
    """Tier — terabox.hnn.workers.dev, a free Cloudflare Worker. GET-only,
    keyed by the share URL's shorturl (the /s/<id> segment), not the
    full URL."""
    match = _BAIDU_SURL_RE.search(url)
    if not match:
        raise ValueError("hnn.workers.dev: couldn't extract a shorturl (/s/<id>) from this link")
    api_url = f"{_HNN_WORKERS_API}{match.group(1)}"
    resp = _session.get(api_url, timeout=15)
    if resp.status_code >= 400:
        raise ValueError(f"hnn.workers.dev: HTTP {resp.status_code}")

    try:
        data = resp.json()
    except Exception:
        raise ValueError("hnn.workers.dev: couldn't parse API response")

    file_list = data.get("list") or []
    if not file_list:
        raise ValueError("hnn.workers.dev: no usable download link in response")
    video = file_list[0]
    link = video.get("dlink")
    if not link:
        raise ValueError("hnn.workers.dev: no usable download link in response")
    return _normalize_tier_result(link, video.get("filename"), str(video.get("size") or "") or None)


def _resolve_nepcoder_workers(url: str) -> dict:
    """Tier — teraboxvideodownloader.nepcoderdevs.workers.dev, a free
    Cloudflare Worker. GET, plain ?url= query param."""
    api_url = f"{_NEPCODER_WORKERS_API}{urllib.parse.quote(url, safe='')}"
    resp = _session.get(api_url, timeout=15)
    if resp.status_code >= 400:
        raise ValueError(f"nepcoder-workers: HTTP {resp.status_code}")

    try:
        data = resp.json()
    except Exception:
        raise ValueError("nepcoder-workers: couldn't parse API response")

    file_list = data.get("list") or []
    if file_list:
        video = file_list[0]
        link = video.get("dlink")
        if link:
            return _normalize_tier_result(link, video.get("filename"), str(video.get("size") or "") or None)

    link = data.get("dlink") or data.get("direct_link") or data.get("url")
    if not link:
        raise ValueError("nepcoder-workers: no usable download link in response")
    return _normalize_tier_result(link)


def _human_size(n: int) -> str:
    """Minimal byte-count formatter — Baidu PCS is the only tier here
    that hands back a raw integer size instead of an API's own
    pre-formatted string, so none of the other tiers needed this."""
    size = float(n or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024 or unit == "TB":
            return f"{size:.2f} {unit}" if unit != "B" else f"{int(size)} {unit}"
        size /= 1024
    return f"{size:.2f} TB"


def probe_terabox_share(url: str) -> dict:
    """Cheap folder-vs-file check for a Baidu-PCS-backed TeraBox share:
    shorturlinfo + share/list only (no per-file api/download calls), so
    it can run ahead of the normal Download/Stream quality menu and
    decide whether a link needs the folder path (_send_terabox_folder in
    main.py) instead. That quality menu only makes sense for a single
    video — handed a real folder it fails with flowvideoplayer's
    generic "no download_url in response", which is the bug this
    function exists to avoid: callers should check this FIRST and skip
    the quality menu entirely (no Stream button either) whenever
    is_folder is True, rather than only finding out it was a folder
    after the quality fetch already failed.

    A single-file share returns is_folder=False, same as a plain video
    link. Only 2+ files, or any subfolder entries, at the share root
    count as a folder.

    Returns {"is_folder": bool, "file_count": int}. Raises the same
    ValueErrors get_all_folder_files()/_resolve_baidu_pcs() do for
    shares this Baidu PCS tier can't read at all (password-protected,
    expired, wrong/unsupported domain, etc.) — callers should treat
    that as "can't tell, fall back to the normal single-file flow",
    not as "not a folder".

    FIX: falls back to the hnn.workers.dev proxy (a different domain
    from 1024terabox.com/terabox.com itself) when the direct Baidu-PCS
    calls fail — some hosts (Render in particular) have their egress to
    the TeraBox domains themselves blocked/403'd while still reaching
    this Cloudflare Worker fine, which used to mean probe_terabox_share
    raised, callers caught it as probe=None ("can't tell"), and a real
    folder link silently fell through to the single-file quality menu
    instead of the folder path.
    """
    try:
        return _probe_via_baidu_pcs(url)
    except Exception as baidu_err:
        try:
            files = _hnn_workers_list_files(url)
        except Exception as hnn_err:
            # FIX: this used to re-raise only baidu_err, silently dropping
            # hnn_err entirely — so a failure log only ever showed "Baidu
            # PCS: ..." even when it was really "both tiers failed, for
            # two different reasons" (e.g. Baidu wants a CAPTCHA/verify_v2
            # challenge AND hnn.workers.dev separately errored) — no way
            # to tell hnn even ran, let alone why it failed too.
            raise ValueError(f"Baidu PCS: {baidu_err} | hnn.workers.dev: {hnn_err}") from baidu_err
        return {"is_folder": len(files) > 1, "file_count": len(files)}


def _probe_via_baidu_pcs(url: str) -> dict:
    """The original direct-Baidu-PCS implementation of probe_terabox_share()
    — split out so probe_terabox_share() can try this first and fall back
    to the hnn.workers.dev proxy (see its docstring) without duplicating
    the shorturlinfo/share/list logic."""
    parsed = urllib.parse.urlparse(url)
    base_origin = f"{parsed.scheme}://{parsed.netloc}"

    m = _BAIDU_SURL_RE.search(url)
    surl = m.group(1) if m else _beer_extract_video_id(url)
    if not surl:
        raise ValueError("Baidu PCS: couldn't find a share code in that link.")

    session = requests.Session()
    session.headers.update({"User-Agent": _BAIDU_UA, "Accept": "application/json"})
    referer = {"Referer": f"{base_origin}/s/{surl}"}

    meta_resp = session.get(
        f"{base_origin}/api/shorturlinfo", params={"shorturl": surl, "root": "1"},
        headers=referer, timeout=20, verify=False,
    )
    meta = meta_resp.json()
    errno = meta.get("errno")
    if errno:
        if errno == -12:
            raise ValueError("Baidu PCS: share is password protected (not supported by this tier).")
        if errno == 1:
            raise ValueError("Baidu PCS: share expired or not found.")
        raise ValueError(meta.get("errmsg") or f"Baidu PCS: shorturlinfo error {errno}")

    shareid, uk, bdstoken = meta.get("shareid"), meta.get("uk"), meta.get("bdstoken")
    if not (shareid and uk and bdstoken):
        raise ValueError("Baidu PCS: shorturlinfo response is missing shareid/uk/bdstoken.")

    all_file_entries = []
    has_subfolders = False
    page = 1
    while True:
        list_resp = session.get(
            f"{base_origin}/share/list",
            params={"shareid": shareid, "uk": uk, "bdstoken": bdstoken, "dir": "/", "num": 100, "page": page},
            headers=referer, timeout=20, verify=False,
        )
        list_data = list_resp.json()
        if list_data.get("errno"):
            raise ValueError(list_data.get("errmsg") or f"Baidu PCS: share/list error {list_data.get('errno')}")

        entries = list_data.get("list") or []
        if not entries:
            break
        # BUG FIX: this used to be a single num=100 call with no paging —
        # any share with 100+ files at the root always reported
        # file_count=100 (silently capped), even when the real count was
        # much higher. Now pages the same way get_all_folder_files()'s
        # _list_dir() already does, stopping once a page comes back with
        # fewer than requested (the real "no more pages" signal — this
        # API doesn't expose a has_more field to check directly).
        all_file_entries.extend(f for f in entries if f.get("isdir") != 1)
        has_subfolders = has_subfolders or any(f.get("isdir") == 1 for f in entries)
        if len(entries) < 100:
            break
        page += 1

    is_folder = has_subfolders or len(all_file_entries) > 1

    return {"is_folder": is_folder, "file_count": len(all_file_entries)}


def get_all_folder_files(url: str) -> list:
    """Resolves every file in a TeraBox folder share. Tries the direct
    Baidu-PCS pipeline first (_get_all_folder_files_via_baidu_pcs — see
    its docstring for the pagination/subfolder/sign fixes already in
    there); if that raises at all, falls back to hnn.workers.dev (a
    different domain from 1024terabox.com/terabox.com) via
    _hnn_workers_list_files().

    FIX: some hosts (Render in particular) have their egress to the
    TeraBox domains themselves blocked/403'd while still reaching that
    Cloudflare Worker fine — previously that meant this whole function
    raised, the folder path gave up entirely, and the caller fell
    through to the single-file quality menu (which then failed too,
    since that menu can't handle a folder). The Worker fallback has no
    subfolder recursion of its own (it just returns whatever "list" the
    proxy gives back), so a share with nested subfolders may come back
    flatter than the Baidu-PCS path would have — still strictly better
    than the previous "raise, and the whole folder is unreachable"."""
    try:
        return _get_all_folder_files_via_baidu_pcs(url)
    except Exception as baidu_err:
        try:
            return _hnn_workers_list_files(url)
        except Exception as hnn_err:
            raise ValueError(f"Baidu PCS: {baidu_err} | hnn.workers.dev: {hnn_err}") from baidu_err


def _get_all_folder_files_via_baidu_pcs(url: str) -> list:
    """The original direct-Baidu-PCS implementation of
    get_all_folder_files() — split out so that function can try this
    first and fall back to hnn.workers.dev (see its docstring) without
    duplicating this pipeline.

    FIX 1: Pagination — fetches all pages (num=100, page=1,2,...) until
    the API returns an empty list. Old code only fetched page 1, silently
    dropping everything past file #100.

    FIX 2: Recursive subfolders — if an entry has isdir==1, recursively
    lists that subfolder too. Old code filtered out all isdir==1 entries
    with a single list comprehension, so any folder-within-a-folder was
    silently ignored.

    Returns [{"name","url","size"}, ...], one entry per file.
    Raises ValueError for share-level errors (password, expired, etc);
    individual file failures are logged and skipped."""
    parsed = urllib.parse.urlparse(url)
    base_origin = f"{parsed.scheme}://{parsed.netloc}"

    m = _BAIDU_SURL_RE.search(url)
    surl = m.group(1) if m else _beer_extract_video_id(url)
    if not surl:
        raise ValueError("Baidu PCS: couldn't find a share code in that link.")

    session = requests.Session()
    session.headers.update({"User-Agent": _BAIDU_UA, "Accept": "application/json"})
    referer = {"Referer": f"{base_origin}/s/{surl}"}

    meta_resp = session.get(
        f"{base_origin}/api/shorturlinfo", params={"shorturl": surl, "root": "1"},
        headers=referer, timeout=20, verify=False,
    )
    meta = meta_resp.json()
    errno = meta.get("errno")
    if errno:
        if errno == -12:
            raise ValueError("Baidu PCS: share is password protected (not supported by this tier).")
        if errno == 1:
            raise ValueError("Baidu PCS: share expired or not found.")
        raise ValueError(meta.get("errmsg") or f"Baidu PCS: shorturlinfo error {errno}")

    shareid, uk, bdstoken = meta.get("shareid"), meta.get("uk"), meta.get("bdstoken")
    if not (shareid and uk and bdstoken):
        raise ValueError("Baidu PCS: shorturlinfo response is missing shareid/uk/bdstoken.")

    def _list_dir(dir_path: str, depth: int = 0) -> list:
        """Paginated listing of one directory — returns all file entries
        (isdir==0) found across all pages, recursing into subfolders.
        depth caps recursion at a sane max (real TeraBox shares are
        never anywhere near this deep) purely as a safety net against a
        malformed/cyclical path from the API recursing forever."""
        if depth > 12:
            logger.warning(f"Baidu PCS: subfolder recursion capped at depth {depth} for {dir_path!r} — stopping here.")
            return []
        all_files = []
        page = 1
        while True:
            resp = session.get(
                f"{base_origin}/share/list",
                params={
                    "shareid": shareid, "uk": uk, "bdstoken": bdstoken,
                    "dir": dir_path, "num": 100, "page": page,
                },
                headers=referer, timeout=20, verify=False,
            )
            data = resp.json()
            if data.get("errno"):
                logger.warning(f"Baidu PCS: share/list error {data.get('errno')} for dir {dir_path!r}")
                break
            entries = data.get("list") or []
            if not entries:
                break  # no more pages

            for entry in entries:
                if entry.get("isdir") == 1:
                    # Recurse into subfolder
                    sub_path = entry.get("path") or f"{dir_path}/{entry.get('server_filename', '')}"
                    logger.info(f"Baidu PCS: recursing into subfolder {sub_path!r}")
                    all_files.extend(_list_dir(sub_path, depth + 1))
                else:
                    all_files.append(entry)

            if len(entries) < 100:
                break  # last page — fewer than requested means no more
            page += 1

        return all_files

    all_entries = _list_dir("/")
    if not all_entries:
        raise ValueError("Baidu PCS: no downloadable files in this share.")

    results = []
    for entry in all_entries:
        try:
            dl_resp = session.post(
                f"{base_origin}/api/download",
                data={
                    "shareid": shareid, "uk": uk, "fs_id": entry.get("fs_id"),
                    # BUG FIX: sign/timestamp were being read from entry
                    # (the per-file share/list record) — that's not where
                    # they live. sign+timestamp are SHARE-level tokens
                    # that come back on the shorturlinfo call (meta,
                    # above), same shareid/uk/bdstoken triple — every file
                    # in the share uses the same pair. entry.get("sign")
                    # was always None (share/list's file records don't
                    # carry a sign field at all), so every single
                    # api/download call in this loop was silently
                    # failing/being skipped — the whole reason folder
                    # downloads produced zero files.
                    "sign": meta.get("sign") or entry.get("sign"),
                    "timestamp": meta.get("timestamp") or int(time.time()),
                    "bdstoken": bdstoken, "primaryid": uk, "type": "nolimit",
                },
                headers=referer, timeout=20, verify=False,
            )
            dl_data = dl_resp.json()
            if dl_data.get("errno"):
                logger.warning(f"Baidu PCS: skipping {entry.get('server_filename')!r} — api/download error {dl_data.get('errno')}")
                continue
            dlink = dl_data.get("dlink") or (dl_data.get("list") or [{}])[0].get("dlink")
            if not dlink:
                logger.warning(f"Baidu PCS: skipping {entry.get('server_filename')!r} — no dlink in response")
                continue
            results.append({
                "name": entry.get("server_filename") or f"file_{entry.get('fs_id')}",
                "url": dlink,
                "size": int(entry.get("size") or 0),
            })
        except Exception as e:
            logger.warning(f"Baidu PCS: skipping {entry.get('server_filename')!r} — {e}")
            continue

    if not results:
        raise ValueError("Baidu PCS: none of the files in this share could be resolved.")
    return results


def _resolve_baidu_pcs(url: str) -> dict:
    """Tier 5 — Baidu PCS guest resolve. Ported from src's
    Akbots/terabox.py (_get_file_info_baidu_pcs_sync): talks to
    TeraBox's own Baidu-PCS-compatible share backend directly — the
    same one DiskWala/TheDiskWala reskins — instead of a third-party
    proxy, so it isn't subject to xAPIverse/beer/ansh/azhawasadda's
    rate limits. No API key or login needed: /api/shorturlinfo hands
    out a working guest session on its own. (src's version can also
    load an admin-uploaded cookie here for a higher speed tier, via
    Akbots-only cookies_manager/cookie_utils — skipped here since it's
    optional and this project doesn't have that module.)"""
    parsed = urllib.parse.urlparse(url)
    domain = parsed.netloc.lower().split(":")[0]
    if domain.startswith("www."):
        domain = domain[4:]
    base_origin = f"{parsed.scheme}://{parsed.netloc}"

    m = _BAIDU_SURL_RE.search(url)
    surl = m.group(1) if m else _beer_extract_video_id(url)
    if not surl:
        raise ValueError("Baidu PCS: couldn't find a share code in that link.")

    session = requests.Session()
    session.headers.update({"User-Agent": _BAIDU_UA, "Accept": "application/json"})

    try:
        meta_resp = session.get(
            f"{base_origin}/api/shorturlinfo",
            params={"shorturl": surl, "root": "1"},
            headers={"Referer": f"{base_origin}/s/{surl}"},
            timeout=20, verify=False,
        )
        meta = meta_resp.json()
    except Exception as e:
        raise ValueError(f"Baidu PCS: shorturlinfo request failed: {e}") from e

    errno = meta.get("errno")
    if errno:
        if errno == -12:
            raise ValueError("Baidu PCS: share is password protected (not supported by this tier).")
        if errno == 1:
            raise ValueError("Baidu PCS: share expired or not found.")
        raise ValueError(meta.get("errmsg") or f"Baidu PCS: shorturlinfo error {errno}")

    shareid, uk, bdstoken = meta.get("shareid"), meta.get("uk"), meta.get("bdstoken")
    if not (shareid and uk and bdstoken):
        raise ValueError("Baidu PCS: shorturlinfo response is missing shareid/uk/bdstoken.")

    try:
        list_resp = session.get(
            f"{base_origin}/share/list",
            params={"shareid": shareid, "uk": uk, "bdstoken": bdstoken, "dir": "/", "num": 100},
            headers={"Referer": f"{base_origin}/s/{surl}"},
            timeout=20, verify=False,
        )
        list_data = list_resp.json()
    except Exception as e:
        raise ValueError(f"Baidu PCS: share/list request failed: {e}") from e

    if list_data.get("errno"):
        raise ValueError(list_data.get("errmsg") or f"Baidu PCS: share/list error {list_data.get('errno')}")

    files = [f for f in (list_data.get("list") or []) if f.get("isdir") != 1]
    if not files:
        raise ValueError("Baidu PCS: no downloadable files in this share.")
    file = files[0]

    try:
        dl_resp = session.post(
            f"{base_origin}/api/download",
            data={
                "shareid": shareid, "uk": uk, "fs_id": file.get("fs_id"),
                # Same fix as get_all_folder_files() — sign/timestamp are
                # share-level tokens from shorturlinfo (meta), not fields
                # on the individual share/list file record.
                "sign": meta.get("sign") or file.get("sign"),
                "timestamp": meta.get("timestamp") or int(time.time()),
                "bdstoken": bdstoken, "primaryid": uk, "type": "nolimit",
            },
            headers={"Referer": f"{base_origin}/s/{surl}"},
            timeout=20, verify=False,
        )
        dl_data = dl_resp.json()
    except Exception as e:
        raise ValueError(f"Baidu PCS: api/download request failed: {e}") from e

    if dl_data.get("errno"):
        raise ValueError(dl_data.get("errmsg") or f"Baidu PCS: api/download error {dl_data.get('errno')}")

    dlink = dl_data.get("dlink") or (dl_data.get("list") or [{}])[0].get("dlink")
    if not dlink:
        raise ValueError("Baidu PCS: api/download response had no dlink.")

    size = int(file.get("size") or 0)
    return _normalize_tier_result(
        dlink,
        file.get("server_filename"),
        _human_size(size),
        (file.get("thumbs") or {}).get("url3"),
    )


def _resolve_guest(url: str) -> dict:
    """Tier 6 — true last resort. Fetches the share page itself (no API
    at all) and regex-scans it for an embedded download link. Only
    catches links the site renders server-side; one that needs
    client-side JS after page load can't be caught this way either."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    }
    resp = _session.get(url, headers=headers, timeout=20, verify=False)
    if resp.status_code != 200:
        raise ValueError(f"guest scrape: page fetch got HTTP {resp.status_code}")

    found = _generic_extract_stream_info(resp.text or "")
    if not found:
        raise ValueError(
            "guest scrape: couldn't find a download link on the share page "
            "(it may render the link via client-side JS, which this tier can't run)."
        )
    return _normalize_tier_result(found["download_link"], found.get("name"))


# Tried in order — each is only attempted if every tier before it raised.
_RESOLVER_TIERS = [
    ("flowvideoplayer", _resolve_flowvideoplayer),
    ("terabox.beer", _resolve_beer),
    ("anshapi", _resolve_ansh),
    ("azhawasadda", _resolve_azhawasadda),
    ("Baidu PCS", _resolve_baidu_pcs),
    ("1024teradl", _resolve_1024teradl),
    ("hnn.workers.dev", _resolve_hnn_workers),
    ("nepcoder-workers", _resolve_nepcoder_workers),
    ("guest scrape", _resolve_guest),
]


def _iter_resolve_candidates(url: str, bypass_cache: bool = False):
    """Like _resolve(), but yields EVERY tier's result in order instead of
    stopping at the first one that resolves without raising. That
    distinction matters: a tier "succeeding" here only means it returned
    *a* link, not that the link is actually fast/working — xAPIverse in
    particular has returned links that resolve fine but then crawl at a
    few KB/s or hang entirely on the actual download. The caller
    (download_video) is what actually validates and can move on to the
    next tier if one turns out bad, which _resolve()'s old
    stop-at-first-success behavior made impossible (it committed to
    tier 1 the moment it didn't throw, so a bad-but-non-erroring tier 1
    link meant tiers 2-6 never even got tried).

    bypass_cache: FIX — download_video()'s retry loop calls this to get
    the FULL tier chain to fall through when its first candidate turns
    out bad/rate-limited/slow. But this same URL is almost always
    already cached by then (get_available_qualities()/the quality menu
    resolved it moments earlier, and download_video() is passed that
    exact result as its "explicit" first candidate) — so without this
    flag, the cache hit below would yield ONLY that one already-tried
    tier and return, silently skipping every other tier in
    _RESOLVER_TIERS entirely. Confirmed from a real failure log: both
    "explicit" and the next "terabox.beer" candidate hit HTTP 429 on the
    same underlying CDN worker (the cached result, tried twice under two
    names) and download_video() gave up there, never having reached
    anshapi/azhawasadda/Baidu PCS/1024teradl/hnn.workers.dev/
    nepcoder-workers/guest scrape at all. get_page_meta/
    get_available_qualities/get_stream_url (via _resolve()) still want
    the cache — they only need one metadata result and re-resolving on
    every call would be wasteful — so this only bypasses it for the
    retry-loop caller that actually needs every tier tried."""
    if not bypass_cache:
        cached = _cache.get(url)
        if cached and (time.time() - cached["ts"]) < CACHE_DURATION:
            logger.debug(f"Using cached result for {url[:60]}…")
            yield cached["tier"], cached["result"]
            return

    if len(_cache) > 200:
        now = time.time()
        stale = [k for k, v in _cache.items() if now - v["ts"] >= CACHE_DURATION]
        for k in stale:
            _cache.pop(k, None)

    for name, resolver in _RESOLVER_TIERS:
        logger.debug(f"Trying resolver: {name}")
        try:
            result = resolver(url)
        except Exception as e:
            logger.warning(f"✗ {name} failed: {e}")
            continue
        logger.info(f"✓ {name} resolved a candidate link")
        yield name, result


def _resolve(url: str) -> dict:
    """Back-compat single-result wrapper (used by get_page_meta/
    get_available_qualities/get_stream_url, which only need metadata —
    not a validated downloadable link — so the old "first tier that
    doesn't raise" behavior is fine for them)."""
    for name, result in _iter_resolve_candidates(url):
        _cache[url] = {"ts": time.time(), "tier": name, "result": result}
        return result
    return _err("All Terabox resolvers failed.")


def _format_duration(seconds: int) -> str:
    """Format duration in seconds as MM:SS or HH:MM:SS."""
    if not seconds or seconds < 0:
        return "Unknown"
    
    hours = seconds // 3600
    minutes = (seconds % 3600) // 60
    secs = seconds % 60
    
    if hours > 0:
        return f"{hours}:{minutes:02d}:{secs:02d}"
    else:
        return f"{minutes}:{secs:02d}"


def _err(msg: str) -> dict:
    """Create error response dict."""
    return {"error": msg}
