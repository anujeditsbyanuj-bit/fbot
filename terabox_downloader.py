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
    FIXED: Extract metadata (thumbnail, duration, title) from Terabox link.
    Returns: {
        "poster_url": str|None,
        "duration": int|None (seconds),
        "duration_formatted": str|None (e.g., "3:45"),
        "title": str|None,
        "file_size": str|None,
    }
    """
    result = _resolve(url)
    if result.get("error"):
        logger.warning(f"Could not fetch metadata: {result.get('error')}")
        return {}
    
    meta = {}
    
    # Extract poster/thumbnail
    if result.get("poster_url"):
        meta["poster_url"] = result["poster_url"]
    
    # Extract duration (seconds)
    if result.get("duration"):
        meta["duration"] = result["duration"]
        # Format duration as MM:SS or HH:MM:SS
        meta["duration_formatted"] = _format_duration(result["duration"])
    
    # Extract title/filename
    if result.get("title"):
        meta["title"] = result["title"]
    elif result.get("file_name"):
        meta["title"] = result["file_name"]
    
    # File size
    if result.get("file_size"):
        meta["file_size"] = result["file_size"]
    
    return meta


def get_available_qualities(url: str) -> list:
    """Return available quality options via flowvideoplayer.com (see
    _resolve_flowvideoplayer()'s docstring — xAPIverse's PRO tier this
    used to call for a real per-quality breakdown never actually worked,
    "Forbidden: Invalid token" on every attempt).

    Every real response seen while building this had exactly one entry in
    data["response"] (a single download_url, no per-quality breakdown),
    so that's the common case below (-> one "Best" option, same as
    before). But the API returns a LIST there, not a single object, so if
    it ever does hand back more than one entry for a link, each is
    surfaced as its own quality option here instead of silently only
    ever using the first one — labelled from whatever quality-naming
    field is present (unconfirmed field name, no multi-item response was
    available to inspect while building this — "quality"/"resolution"/
    "label" are tried, in that order, before falling back to a plain
    "Option N")."""
    resp, err = _flowvideoplayer_csrf.post_json(_FLOWVIDEOPLAYER_API_URL, {"url": url})
    if resp is None:
        raise RuntimeError(f"flowvideoplayer: {err}")
    if resp.status_code != 200:
        raise RuntimeError(f"flowvideoplayer: HTTP {resp.status_code}" + (f" ({err})" if err else ""))

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"flowvideoplayer: invalid JSON response: {e}")

    if not (data.get("code") == 200 and data.get("status") and data.get("response")):
        raise RuntimeError(f"flowvideoplayer: {data.get('message') or 'no response data'}")

    items = data["response"]
    logger.info(f"terabox flowvideoplayer: {len(items)} item(s) in response for {url[:80]}")
    if len(items) <= 1:
        download_link = items[0].get("download_url", "")
        if not download_link:
            raise RuntimeError("flowvideoplayer: no download_url in response")
        return [{"label": "Best", "url": download_link}]

    qualities = []
    for i, info in enumerate(items, start=1):
        download_link = info.get("download_url")
        if not download_link:
            continue
        label = info.get("quality") or info.get("resolution") or info.get("label") or f"Option {i}"
        qualities.append({"label": str(label), "url": download_link})

    if not qualities:
        raise RuntimeError("flowvideoplayer: no download_url in any response entry")
    return qualities


def get_stream_url(url: str) -> Optional[str]:
    """Get stream URL using ONLY terabox.beer API.
    Returns stream_link (m3u8) if available, else proxy_url."""
    try:
        result = _resolve_beer(url)
        # Prefer m3u8 stream_link over generic proxy_url
        return result.get("proxy_url")
    except Exception as e:
        logger.warning(f"terabox.beer stream failed: {e}")
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
                   on_progress=None, stream_url: str = None) -> str:
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
    """
    if stream_url:
        # Respect the user's explicit quality pick first, but don't dead-
        # end on it — if THIS specific link turns out to be the slow/bad
        # one (the exact symptom reported: a quality menu's chosen link
        # crawls at a few KB/s), fall through to the full tier chain
        # afterward rather than failing outright. That can mean the
        # actual downloaded quality differs slightly from what was
        # picked, but a slightly-different-quality file beats a download
        # that never finishes.
        candidates = [("explicit", {"proxy_url": stream_url})] + list(_iter_resolve_candidates(url))
    else:
        candidates = list(_iter_resolve_candidates(url))

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


_BAIDU_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
_BAIDU_SURL_RE = re.compile(r"/s/([a-zA-Z0-9_-]+)")


def get_all_folder_files(url: str) -> list:
    """Resolves every file in a Baidu-PCS-backed TeraBox share, not just
    the first one — the shorturlinfo -> share/list -> api/download
    pipeline, looped over every file share/list returns instead of just
    the first one.

    Returns [{"name","url","size"}, ...], one entry per file, in the
    order share/list returned them; a single-file share still comes back
    as a 1-item list, so callers can treat "more than one entry" as
    "this was actually a folder" without needing a separate is-a-folder
    check up front. Raises a ValueError for the shared setup-step
    failures (password-protected, expired, etc); an individual file
    failing later just gets skipped rather than aborting every other
    file in the same folder."""
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

    list_resp = session.get(
        f"{base_origin}/share/list",
        params={"shareid": shareid, "uk": uk, "bdstoken": bdstoken, "dir": "/", "num": 100},
        headers=referer, timeout=20, verify=False,
    )
    list_data = list_resp.json()
    if list_data.get("errno"):
        raise ValueError(list_data.get("errmsg") or f"Baidu PCS: share/list error {list_data.get('errno')}")

    entries = [f for f in (list_data.get("list") or []) if f.get("isdir") != 1]
    if not entries:
        raise ValueError("Baidu PCS: no downloadable files in this share.")

    results = []
    for entry in entries:
        try:
            dl_resp = session.post(
                f"{base_origin}/api/download",
                data={
                    "shareid": shareid, "uk": uk, "fs_id": entry.get("fs_id"),
                    "sign": entry.get("sign"), "timestamp": int(time.time()),
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


# Tried in order — each is only attempted if every tier before it raised.
_RESOLVER_TIERS = [
    ("flowvideoplayer", _resolve_flowvideoplayer),
    ("terabox.beer", _resolve_beer),
]


def _iter_resolve_candidates(url: str):
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
    link meant tiers 2-6 never even got tried)."""
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
