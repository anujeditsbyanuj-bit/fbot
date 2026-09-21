"""
terabox_downloader.py — Terabox downloader via diskwala.net's terabox API

SIMPLIFIED (this pass, on request): every other resolver this file used
to have — flowvideoplayer.com, terabox.beer, anshapi.workers.dev,
azhawasadda.in, 1024teradl.com, hnn.workers.dev, nepcoder.workers.dev,
and a direct Baidu-PCS pipeline — has been removed entirely. Replaced
with exactly one API: diskwala.net's own terabox-downloader page's
backend, confirmed working from a live HAR capture of that page in
actual use (POST https://diskwala.net/web/api/terabox/download). One
thing to maintain/debug instead of eight, and per the capture this one
API already covers everything the removed tiers did — single files,
folders with subfolders, thumbnails, file sizes, and both a direct
downloadLink and a separate streamUrl per file.

API contract (from the HAR capture):
    POST https://diskwala.net/web/api/terabox/download
    Body: {"link": "<terabox share URL>", "dir_path": "<folder path,
           or \"\" for the share's root>", "page": 1}
    Response: {"ok": true, "success": true, "data": [
        {"fsId", "fileName", "fileSize", "fileSizeMB", "thumbnail",
         "downloadLink", "streamUrl", "duration", "type"
         ("folder" or a file type), "isDir", "path", "dirPath"}, ...
    ]}
A share can be a single file (root "data" has one non-folder item) or a
real folder (root "data" has folder entries — call again with dir_path
set to that folder's own "path" to list its contents; recurses for
nested subfolders).

Cloudflare: diskwala.net's own site sits behind Cloudflare — the HAR
capture's request carried a cf_clearance cookie, meaning a fresh
request without one likely gets a challenge page instead of JSON. This
module goes through cf_bypass.py (the same cloudscraper/FlareSolverr
module ytdlp_downloader.py already uses for other Cloudflare-protected
sites) to solve that once and reuse the resulting cookie, retrying
automatically the first time a call comes back non-JSON/403.
"""

import logging
import mimetypes
import os
import re
import time
import threading
import concurrent.futures
import requests
from typing import Optional
from urllib.parse import quote

from config import MAX_FILE_SIZE  # shared 2GB-default, config-driven cap — see config.py
import cf_bypass

logger = logging.getLogger("terabox_downloader")

# ── Terabox domain family — unchanged. Still needed to recognize a
# terabox share link (of any of its many mirror domains) before handing
# it to diskwala.net's API, which itself accepts any of these. ──
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

urllib3_disabled = False
try:
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    urllib3_disabled = True
except Exception:
    pass

MIN_FILE_SIZE = 50 * 1024  # 50 KB - real videos are bigger; smaller = a thumbnail/error page

_API_URL = "https://diskwala.net/web/api/terabox/download"
_REFERER_URL = "https://diskwala.net/terabox-downloader"
# BUG FIX: confirmed against two real HAR captures (dl-worker.teraboxdl.site
# and *.teramera*.workers.dev/*.workers.dev traffic) — every cross-origin
# CDN/worker request the diskwala.net PAGE itself makes (dl-worker/api.
# teraboxdl.site, *.teraboxpage.com, the tera-stream-*.teramera*.workers.dev/
# tera-proxy.dailyweb577.workers.dev pool) consistently carries a BARE-ORIGIN
# Referer (just "https://diskwala.net/", no path) — standard browser
# strict-origin-when-cross-origin behavior, distinct from the FULL-PATH
# Referer (_REFERER_URL above) diskwala.net's own API call correctly gets.
# _session's global default is the full-path one (right for diskwala.net
# itself); the CDN/worker download functions below override it per-request
# to this instead, rather than sending the wrong one or none at all.
_CDN_REFERER = "https://diskwala.net/"
# Matches the real browser UA seen in the working HAR capture — diskwala.
# net's Cloudflare rule may key off a plausible mobile-Chrome UA
# specifically, so kept identical to what was actually observed working
# rather than a generic desktop string.
_DEFAULT_UA = (
    "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/139.0.0.0 Mobile Safari/537.36"
)

_session = None  # created lazily below, once we know curl_cffi's availability
try:
    from curl_cffi import requests as _curl_requests
    _CURL_CFFI_AVAILABLE = True
except ImportError:
    _CURL_CFFI_AVAILABLE = False
    logger.warning(
        "[terabox] curl_cffi not installed — falling back to plain requests for the "
        "diskwala.net API call. A valid cf_clearance cookie can still get rejected "
        "without a matching browser-like TLS fingerprint; `pip install curl_cffi` if "
        "the web-API tier keeps 403ing even after cf_bypass reports success."
    )

if _CURL_CFFI_AVAILABLE:
    _session = _curl_requests.Session()
else:
    _session = requests.Session()
_session.headers.update({
    "User-Agent": _DEFAULT_UA,
    "Accept": "*/*",
    "Content-Type": "application/json",
    "Origin": "https://diskwala.net",
    "Referer": _REFERER_URL,
    # ── Exact headers from a working HAR capture of this endpoint ──
    # Cloudflare's bot detection checks these for presence/consistency,
    # not just the TLS/JA3 fingerprint — curl_cffi's generic "chrome"
    # impersonation profile simulates a DESKTOP Chrome by default and
    # doesn't generate mobile-Android-specific Client Hints matching
    # _DEFAULT_UA above, so a mismatch here (claiming a mobile UA while
    # sending desktop-shaped headers) is a plausible reason a request
    # can still get rejected even with a valid cf_clearance cookie and
    # curl_cffi impersonation both present. These are static per-UA
    # values (not secrets/session-specific), so hardcoding them to match
    # the real captured browser exactly is safe and stable.
    "sec-ch-ua": '"Chromium";v="139", "Not;A=Brand";v="99"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-ch-ua-platform-version": '"13.0.0"',
    "sec-ch-ua-arch": '""',
    "sec-ch-ua-bitness": '""',
    "sec-ch-ua-model": '"CPH2371"',
    "sec-ch-ua-full-version": '"139.0.7339.0"',
    "sec-ch-ua-full-version-list": '"Chromium";v="139.0.7339.0", "Not;A=Brand";v="99.0.0.0"',
    "sec-fetch-site": "same-origin",
    "sec-fetch-mode": "cors",
    "sec-fetch-dest": "empty",
    "Accept-Language": "en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
})

_api_cache: dict = {}  # (link, dir_path) -> {"ts": float, "data": list}
_API_CACHE_TTL = 300
_api_cache_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────
#  Optional: Telethon token-API tier — reuses diskwala.py's EXISTING
#  Telethon session (api2.diskwala.net) rather than opening a second one.
#
#  diskwala.py already talks to api2.diskwala.net/api/{diskwala,flezen,
#  vidbunker}/{download,status} — a bearer-token-gated job-submit+poll
#  API, not a browser page, so it isn't behind the same Cloudflare wall
#  the diskwala.net WEBSITE (used by the web-API tier above) sits behind.
#  Since that website has its own /terabox-downloader page backed by
#  (presumably) the same infrastructure, "terabox" may well be a
#  supported service value on api2.diskwala.net the same way diskwala/
#  flezen/vidbunker are.
#
#  THIS IS UNVERIFIED — never tested against a live response, since
#  confirming it needs an actual account/session to check with. Built to
#  fail safe: every call is wrapped so ANY failure (wrong service name,
#  404, timeout, whatever) just logs once and falls through to the
#  web-API + cf_bypass tier above, which was already working before this
#  existed. Only handles the single-file case (api2.diskwala.net's
#  job-poll pattern has no known "list a folder" contract the way the
#  web API's dir_path does) — get_all_folder_files() below only tries
#  this at the top level (the share's root), not for subfolder
#  recursion. Check logs for "[terabox-token-api]" to see whether this
#  is actually being used.
# ─────────────────────────────────────────────────────────────────────────
try:
    from diskwala import get_auth_token_sync, DiskwalaAuthError, decrypt_file, _invalidate_auth_token as _invalidate_diskwala_token
    _TOKEN_API_AVAILABLE = True
except Exception as e:
    logger.warning(f"[terabox-token-api] diskwala.py's Telethon session not importable ({e}) — token-API tier disabled, using web+cf_bypass only.")
    _TOKEN_API_AVAILABLE = False

TOKEN_API_DOWNLOAD = "https://diskwala.net/web/api/terabox/download"
TOKEN_API_STATUS = "https://diskwala.net/web/api/terabox/status"


def _fetch_terabox_via_token_api(link: str, auth: str) -> dict:
    """Same request shape as diskwala.py's _fetch_diskwala_video_via_api
    (headers, job-submit + poll pattern), pointed at a "terabox" service
    key instead of "diskwala"/"flezen"/"vidbunker". See the module
    comment above — this is a guess following that established pattern,
    not a confirmed-working endpoint."""
    headers = {
        "Authorization": f"Bearer {auth}",
        "X-Bot-Id": "diskwala",
        "Content-Type": "application/json",
        "Origin": "https://miniapp.diskwala.net",
        "Referer": "https://miniapp.diskwala.net/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
    }
    r = requests.post(TOKEN_API_DOWNLOAD, headers=headers, json={"link": link}, timeout=60)
    if r.status_code in (401, 403):
        raise DiskwalaAuthError(f"terabox token-API auth rejected (HTTP {r.status_code})")
    if r.status_code == 404:
        raise RuntimeError("terabox token-API: 404 — 'terabox' likely isn't a supported service on api2.diskwala.net")
    try:
        data = r.json()
    except Exception:
        raise RuntimeError(f"terabox token-API: non-JSON response (HTTP {r.status_code})")
    if not data.get("ok"):
        raise RuntimeError(data.get("error") or f"terabox token-API error: {data}")

    status_url = f"{TOKEN_API_STATUS}?link=" + quote(link, safe="")
    poll_interval = 0.5
    for _ in range(90):
        r = requests.get(status_url, headers=headers, timeout=60)
        if r.status_code in (401, 403):
            raise DiskwalaAuthError(f"terabox token-API auth rejected while polling (HTTP {r.status_code})")
        try:
            data = r.json()
        except Exception:
            raise RuntimeError(f"terabox token-API: non-JSON status response (HTTP {r.status_code})")
        if not data.get("ok"):
            raise RuntimeError(data.get("error") or f"terabox token-API error: {data}")
        status = (data.get("status") or "").lower()
        if status == "pending":
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 2.0)
            continue
        if status == "done":
            file = data.get("file") or {}
            if file.get("_x"):
                logger.info("[terabox-token-api] file is encrypted, decrypting...")
                file = decrypt_file(file)
            return {
                "fileName": file.get("name") or file.get("fileName") or "file",
                "fileSize": file.get("size") or file.get("fileSize") or 0,
                "downloadLink": file.get("downloadUrl") or file.get("url"),
                "streamUrl": file.get("streamUrl") or file.get("downloadUrl") or file.get("url"),
                "thumbnail": file.get("thumb") or file.get("thumbnail"),
                "duration": file.get("duration"),
                "isDir": False,
                "type": "video",
            }
        raise RuntimeError(f"terabox token-API: unexpected status {status!r}")
    raise RuntimeError("terabox token-API: timed out waiting for result")


def _resolve_single_file_via_token_api(link: str) -> dict | None:
    """Returns a normalized file dict (same shape diskwala.net's web-API
    items use) on success, or None if this tier isn't usable right now
    for ANY reason — never raises, callers just fall through to the
    web-API + cf_bypass tier on None."""
    if not _TOKEN_API_AVAILABLE:
        return None
    try:
        auth = get_auth_token_sync()
        try:
            return _fetch_terabox_via_token_api(link, auth)
        except DiskwalaAuthError:
            _invalidate_diskwala_token()
            auth = get_auth_token_sync()
            return _fetch_terabox_via_token_api(link, auth)
    except Exception as e:
        logger.info(f"[terabox-token-api] not usable for this link ({e}) — falling back to web API.")
        return None


# ── PUBLIC INTERFACE ─────────────────────────────────────────────────────────

def is_terabox_link(url: str) -> bool:
    """Check if URL is a valid Terabox link."""
    return bool(_TERABOX_DOMAIN_RE.search(url))


def extract_terabox_links(text: str) -> list:
    """Extract all Terabox links from text."""
    return _TERABOX_DOMAIN_RE.findall(text)


def _api_call(link: str, dir_path: str = "", page: int = 1, _retry: bool = True) -> list:
    """POST to diskwala.net's terabox API, returning its "data" list
    (files and/or subfolders at that dir_path). Raises RuntimeError with
    a clear message on any failure. Cached briefly per (link, dir_path,
    page) — a single share/folder view is commonly fetched more than
    once in the same request (get_page_meta + get_available_qualities
    + download_video, or a folder listing feeding a quality-menu-less
    per-file send loop in main.py)."""
    cache_key = (link, dir_path, page)
    now = time.time()
    with _api_cache_lock:
        cached = _api_cache.get(cache_key)
    if cached and (now - cached["ts"]) < _API_CACHE_TTL:
        return cached["data"]

    bypass = cf_bypass.get_bypass_for_requests(_API_URL)
    cookies = bypass["cookies"] if bypass else {}
    # Deliberately NOT using bypass["headers"]["User-Agent"] here — that's
    # whatever UA FlareSolverr/cloudscraper's OWN solving browser reported
    # (commonly a desktop Chrome), which would clash with the mobile-
    # Android sec-ch-ua-* Client Hints already set on _session (see
    # above) if used instead of _DEFAULT_UA. A self-consistent header set
    # (UA + Client Hints all agreeing with each other, matching a real
    # captured mobile session) matters more here than matching whichever
    # UA happened to solve the underlying Cloudflare challenge.

    try:
        post_kwargs = dict(
            json={"link": link, "dir_path": dir_path, "page": page},
            cookies=cookies, timeout=30,
        )
        if _CURL_CFFI_AVAILABLE:
            post_kwargs["impersonate"] = "chrome"
        resp = _session.post(_API_URL, **post_kwargs)
    except Exception as e:
        raise RuntimeError(f"diskwala.net terabox API request failed: {e}") from e

    # A Cloudflare challenge page comes back as HTML — either way,
    # .json() fails on it. That (or an explicit 403) is the "need a
    # fresh cf_clearance" signal: solve once via cf_bypass and retry,
    # same one-retry pattern ytdlp_downloader.py uses for the same site
    # family of problem.
    data = None
    try:
        data = resp.json()
    except Exception:
        pass

    if data is None or resp.status_code == 403 or not data.get("ok"):
        if _retry and cf_bypass.try_solve(_REFERER_URL):
            return _api_call(link, dir_path, page, _retry=False)
        if data is None:
            raise RuntimeError(
                f"diskwala.net terabox API: non-JSON response (status {resp.status_code}) — "
                "likely a Cloudflare challenge cf_bypass couldn't solve."
            )
        raise RuntimeError(data.get("message") or data.get("error") or f"diskwala.net terabox API error (status {resp.status_code})")

    items = data.get("data") or []
    with _api_cache_lock:
        _api_cache[cache_key] = {"ts": now, "data": items}
    return items


def _is_folder_item(it: dict) -> bool:
    return bool(it.get("isDir")) or it.get("type") == "folder"


def probe_terabox_share(url: str) -> dict:
    """FIX: this function was missing entirely from this rewrite, even
    though main.py's process_link() calls it unconditionally on EVERY
    TeraBox link (folder or not) before deciding whether to route into
    the folder-download path — without it, every single TeraBox link
    (not just folders) would hit AttributeError: module
    'terabox_downloader' has no attribute 'probe_terabox_share' the
    moment this rewrite was deployed. Re-added here to match the exact
    contract main.py expects.

    Cheap folder-vs-file check: a single root-level API call (page 1
    only — this only needs to know is_folder/file_count, not the full
    listing get_all_folder_files() builds), so it can run ahead of the
    normal Download/Stream quality menu the same way it always has.

    Returns {"is_folder": bool, "file_count": int}. Raises whatever
    _api_call() raises for shares that can't be read at all (expired,
    wrong link, the diskwala.net API itself down, etc.) — callers
    should treat that as "can't tell, fall back to the normal
    single-file flow", not as "not a folder", same as before."""
    items = _api_call(url, "", 1)
    is_folder = any(_is_folder_item(it) for it in items) or len(items) > 1
    return {"is_folder": is_folder, "file_count": len(items)}


def get_all_folder_files(url: str, _dir_path: str = "", _depth: int = 0) -> list:
    """Recursively lists every file under a TeraBox share — the root, or
    a given subfolder — flattening nested subfolders into one list.
    Returns [{"name","url","size"}, ...], one entry per file; url is the
    direct downloadLink (falls back to streamUrl if a file only has
    that). A genuine single-file share just comes back as a 1-item list
    — same contract main.py already relies on (every TeraBox link goes
    through this function unconditionally, single file or real folder
    alike; see main.py's own comment on that near its terabox handling).

    Tries the token-API tier first, but ONLY at the top level (root, on
    the first call) — it has no known way to list a folder's contents,
    so a share that's actually a multi-file folder still needs the
    web-API tier's dir_path-based recursion below regardless."""
    if _depth > 8:  # guard against a pathological/cyclic folder structure
        return []
    if _dir_path == "" and _depth == 0:
        tok = _resolve_single_file_via_token_api(url)
        if tok:
            # BUG FIX: was `or tok.get("streamUrl")` — confirmed via a
            # real HAR capture that downloadLink and streamUrl point at
            # completely different content: downloadLink serves the real
            # video/mp4 file, streamUrl serves an HLS .m3u8 PLAYLIST
            # (content-type application/vnd.apple.mpegurl, a few KB of
            # text, not the video). Falling back to streamUrl here for
            # an actual file download would silently save that playlist
            # text as if it were the video — a tiny, corrupt ".mp4" that
            # nothing can play. get_stream_url() below is the one place
            # streamUrl is actually the right thing to use.
            link = tok.get("downloadLink") or tok.get("streamUrl")
            if link:
                return [{"name": tok.get("fileName") or "file", "url": link, "size": tok.get("fileSize") or 0}]
    # BUG FIX: this only ever fetched page 1 of this dir_path — any
    # folder whose contents span more than one page (the API's response
    # has no has_more/total field to check, per a real captured
    # response, so the only reliable stop condition is "this page came
    # back empty") silently lost every file past page 1. Now keeps
    # requesting page 2, 3, ... until a page returns nothing.
    files = []
    page = 1
    while True:
        items = _api_call(url, _dir_path, page)
        if not items:
            break
        for it in items:
            if _is_folder_item(it):
                sub_path = it.get("path") or it.get("dirPath") or ""
                files.extend(get_all_folder_files(url, sub_path, _depth + 1))
            else:
                # downloadLink preferred; streamUrl as fallback (safe: _attempt_download
                # handles HLS via ffmpeg when Content-Type is application/vnd.apple.mpegurl).
                # Without this fallback, files that only have streamUrl are silently skipped.
                link = it.get("downloadLink") or it.get("streamUrl")
                if not link:
                    continue
                files.append({
                    "name": it.get("fileName") or "file",
                    "url": link,
                    "size": it.get("fileSize") or 0,
                })
        page += 1
    return files


def _first_file(url: str, _dir_path: str = "", _depth: int = 0) -> dict:
    """The single-file case get_page_meta/get_available_qualities/
    get_stream_url all need — just the first actual file found anywhere
    under the share, however deep. Raises RuntimeError if nothing
    resolvable is found.

    BUG FIX: this used to hand-unroll only 2 levels (root, then one level
    into the root's first folder) while get_all_folder_files() (the
    actual-download path) recurses up to 8 levels — confirmed against a
    real share's HAR capture where the root's only folder ("From：vivo
    1901") itself contains further subfolders ("DCIM", "Watch Full
    V!deos") alongside its own files. Any share where a folder's direct
    contents are ALL further subfolders (files only appearing one level
    deeper still) would fail here — "couldn't find a file in this share"
    — while get_all_folder_files() found them fine, since it actually
    recurses. Now uses the same 8-level guard, and tries every sibling
    folder in turn (not just the first) before giving up, so a folder
    that happens to come first but is itself empty doesn't block finding
    a file in one of its siblings.

    Tries the token-API tier first (see the module comment above it),
    once, at depth 0 only — falls through to the web-API + cf_bypass
    tier on any failure."""
    if _depth == 0:
        tok = _resolve_single_file_via_token_api(url)
        if tok:
            return tok
    if _depth > 8:  # same guard as get_all_folder_files() — pathological/cyclic folder structure
        raise RuntimeError("Terabox: couldn't find a file in this share (nesting too deep).")

    # Same pagination fix as get_all_folder_files() — a file sitting on
    # page 2+ of this dir_path was invisible before (only page 1 was
    # ever fetched), same root cause. Collect every page's items first
    # so a file on a later page is still found before falling through to
    # recursing into subfolders.
    items = []
    page = 1
    while True:
        page_items = _api_call(url, _dir_path, page)
        if not page_items:
            break
        items.extend(page_items)
        page += 1

    for it in items:
        if not _is_folder_item(it):
            return it
    for it in items:
        if _is_folder_item(it):
            sub_path = it.get("path") or it.get("dirPath") or ""
            try:
                return _first_file(url, sub_path, _depth + 1)
            except RuntimeError:
                continue  # this folder (or everything under it) had nothing — try the next sibling
    raise RuntimeError("Terabox: couldn't find a file in this share.")


def get_page_meta(url: str) -> dict:
    """Extract metadata from a Terabox link.
    Returns: {"poster_url", "duration", "title", "file_size", "extension", "category"}"""
    it = _first_file(url)
    meta = {}
    title = it.get("fileName") or ""
    if title:
        meta["title"] = title
    if it.get("fileSize"):
        meta["file_size"] = it["fileSize"]
    if it.get("thumbnail"):
        meta["poster_url"] = it["thumbnail"]
    if it.get("duration"):
        meta["duration"] = it["duration"]

    ext = os.path.splitext(title)[1].lower() if title else ""
    if not ext:
        slug = url.rstrip("/").split("/")[-1].split("?")[0]
        ext = os.path.splitext(slug)[1].lower() or ".mp4"
    meta["extension"] = ext

    video_exts = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
                  ".m4v", ".mpg", ".mpeg", ".3gp", ".ts", ".m2ts"}
    meta["category"] = "Video" if ext in video_exts else "File"
    return meta


def get_available_qualities(url: str) -> list:
    """diskwala.net's API only ever returns one link per file (no
    per-quality breakdown) — same single-"Best"-option contract this
    function has always had here. The caller already treats a length-1
    list as "skip the quality menu, download straight away"."""
    it = _first_file(url)
    # BUG FIX: same downloadLink-only fix as get_all_folder_files() —
    # this is the link the user's quality-menu choice ultimately
    # downloads, not a streaming preview, so it must never fall back to
    # streamUrl's .m3u8 playlist.
    link = it.get("downloadLink")
    if not link:
        raise RuntimeError("Terabox: couldn't resolve a download link for this URL.")
    return [{"label": "Best", "url": link}]


def get_stream_url(url: str) -> Optional[str]:
    try:
        it = _first_file(url)
        return it.get("streamUrl") or it.get("downloadLink")
    except Exception as e:
        logger.warning(f"terabox stream failed: {e}")
        return None


class _BadCandidate(Exception):
    """Raised internally to mean "this link is bad/too slow, don't
    retry it the same way" — caught in download_video() below."""
    pass


# If a link hasn't managed at least this much throughput after
# _SLOW_GRACE_SECONDS, it's treated as bad/throttled rather than waited
# out — a link that "resolves" fine but then crawls at ~35 KB/s and
# never meaningfully progresses is exactly what this catches.
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
        head = _session.head(proxy_url, allow_redirects=True, timeout=15, verify=False, headers={"Referer": _CDN_REFERER})
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
                             allow_redirects=True, headers={"Referer": _CDN_REFERER})
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
    headers = {"Range": f"bytes={start}-{end}", "Referer": _CDN_REFERER}
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
        url_h    = {"Range": f"bytes={rng[0]}-{rng[1]}", "Referer": _CDN_REFERER}
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
        resp = _session.get(proxy_url, stream=True, timeout=60, verify=True, headers={"Referer": _CDN_REFERER})
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

    # BUG FIX: the ".m3u8" in proxy_url check above only catches HLS
    # links that literally have ".m3u8" in the URL string — confirmed
    # against a real HAR capture that a genuine Terabox stream tier
    # (teramera4.workers.dev's own CDN proxy) hands back a URL shaped
    # like ".../stream?token=<opaque>" with NO ".m3u8" anywhere in it,
    # yet its actual response IS an HLS playlist (Content-Type:
    # application/vnd.apple.mpegurl, and a suspiciously small body —
    # 16-330 KB in that capture, a text manifest, not a video). That
    # slipped straight past both checks above (its content-type isn't
    # one of the "bad" ones) and would have been saved byte-for-byte as
    # if it were the actual video — exactly the broken-file failure mode
    # the URL-string check's own comment already described, just from a
    # source that check didn't cover. Catching it here, from the
    # response's real Content-Type instead of guessing off the URL
    # shape, covers both this tier and the original ".m3u8"-in-URL one.
    if any(hls in content_type for hls in ("mpegurl", "x-mpegurl")):
        resp.close()
        logger.info(
            f"[terabox] URL had no \".m3u8\" in it but Content-Type says HLS "
            f"({content_type}) — remuxing via ffmpeg instead of a raw download."
        )
        return _download_hls_via_ffmpeg(proxy_url, out_path, on_progress)

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
            resp = _session.get(proxy_url, stream=True, timeout=60, verify=True, headers={"Referer": _CDN_REFERER})
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
    Download a Terabox file. Terabox is a general cloud-storage share,
    not a video-only site — the caller's out_path assumes ".mp4" (every
    other backend in this project IS video-only, so that assumption is
    safe there), but a Terabox share can be a zip, pdf, image, or any
    other file type. _attempt_download detects the real type from the
    response's Content-Type and renames the file to the correct
    extension before returning, instead of silently handing back e.g. a
    PDF saved with a ".mp4" name.

    stream_url: an explicit link already resolved by the caller (the
    quality menu's pick, or a specific file's link from a folder
    listing — see _send_terabox_folder in main.py). Used directly
    instead of re-resolving `url` through the API — important for
    folder downloads in particular, where `url` is the FOLDER's share
    link (the same one for every file in it): re-resolving it here
    would hand back the share's first/default file, not the specific
    file `stream_url` actually points to.

    strict_stream_url: kept for call-signature compatibility with
    main.py's existing calls (folder-file downloads pass True). With
    only one resolver tier left there's no "fall through to other
    tiers" behavior to gate here anymore — an explicit stream_url is
    always used as-is either way — but the parameter stays so callers
    don't need to change.
    """
    if not stream_url:
        it = _first_file(url)
        # downloadLink preferred (direct mp4 via requests).
        # streamUrl fallback is safe: _attempt_download() detects .m3u8
        # Content-Type and routes to _download_hls_via_ffmpeg automatically,
        # so a streamUrl-only file still downloads correctly as a real mp4.
        # (Confirmed via HAR: downloadLink → dl-worker.teraboxdl.site video/mp4,
        #  streamUrl → api.teraboxdl.site/get_m3u8_stream_fast → HLS playlist.)
        stream_url = it.get("downloadLink") or it.get("streamUrl")
    if not stream_url:
        raise RuntimeError("Terabox: couldn't resolve a download link for this URL.")

    try:
        return _attempt_download(stream_url, out_path, on_progress)
    except _BadCandidate as e:
        # Only one tier left — no "try the next one" to fall through to
        # anymore, so a bad candidate is just a hard failure now. Still
        # worth one fresh-resolve retry when the caller didn't pin an
        # explicit stream_url (a cached/stale link from an earlier
        # _api_call is the one case a re-resolve can actually help).
        if strict_stream_url:
            raise RuntimeError(str(e)) from e
        logger.warning(f"Terabox: link rejected ({e}) — re-resolving and retrying once.")
        it = _first_file(url)
        fresh_link = it.get("downloadLink")
        if not fresh_link or fresh_link == stream_url:
            raise RuntimeError(str(e)) from e
        return _attempt_download(fresh_link, out_path, on_progress)


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
    # BUG FIX: neither this command nor the ffprobe duration-check below
    # sent any Referer at all before — ffmpeg/ffprobe don't share Python's
    # `requests` session (and its headers) the way the rest of this file
    # does, so each has to be told explicitly. Same bare-origin Referer as
    # _CDN_REFERER's own comment: confirmed via HAR capture that these
    # *.workers.dev/*.teraboxdl.site/*.teraboxpage.com CDN hosts expect
    # "https://diskwala.net/" (no path), not the full-path one diskwala.net
    # itself gets.
    _referer_header = f"Referer: {_CDN_REFERER}\r\n"
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
        "-headers", _referer_header,
        "-i", m3u8_url, "-c", "copy", "-bsf:a", "aac_adtstoasc", out_path,
    ]

    duration_s = None
    try:
        probe = subprocess.run(
            ["ffprobe", "-v", "error", "-headers", _referer_header,
             "-show_entries", "format=duration",
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
