import re
import time
import json
import html
import logging
import os
import requests
from urllib.parse import quote, urljoin
from bs4 import BeautifulSoup

logger = logging.getLogger("faphouse_bot")

HTML_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"
)

# Optional: a logged-in Flezen account cookie. Without it, the Flezen
# fallback can still confirm the file exists and report its name/size, but
# cannot produce an actual download URL (Flezen only serves direct links to
# saved/logged-in accounts). Set this in .env if you have one.
FLEZEN_COOKIE = os.getenv("FLEZEN_COOKIE") or os.getenv("FLEZEN_ACCOUNT_COOKIE")

# ─────────────────────────────────────────────────────────────────────────
#  Telethon auth-token tier (ported from ultra-main's main.py)
#
#  ultra-main's get_auth_token()/_get_persistent_telethon_client() lived in
#  main.py and ran on the bot's own asyncio event loop — every caller was
#  already an `async def` on that loop. fbot-main's adapter functions
#  below are plain sync `def`s (this module's whole contract with
#  fbot-main's main.py — see the INTEGRATION ADAPTER section further
#  down), called via asyncio.to_thread(), so there's no running event loop
#  in the thread they execute on for Telethon's async client calls to use.
#
#  Fix: run Telethon on ONE persistent background thread with its own
#  event loop, started lazily on first use, and give every sync caller a
#  blocking get_auth_token_sync() that hands work to that loop via
#  asyncio.run_coroutine_threadsafe() and waits for the result. The
#  Telethon client itself is created once on that loop and reused (same
#  as ultra-main's _telethon_client global) rather than reconnecting on
#  every call.
# ─────────────────────────────────────────────────────────────────────────
import threading
import asyncio

_auth_cache = {"token": None, "expires": 0}
_telethon_client = None
_telethon_async_lock = None  # created lazily, ON the Telethon loop (see below)

_telethon_loop = None
_telethon_loop_thread = None
_telethon_loop_start_lock = threading.Lock()


def _ensure_telethon_loop():
    """Start the dedicated Telethon event-loop thread on first use, or
    return the already-running one. Safe to call from any thread."""
    global _telethon_loop, _telethon_loop_thread
    with _telethon_loop_start_lock:
        if _telethon_loop is None:
            _telethon_loop = asyncio.new_event_loop()

            def _run_forever():
                asyncio.set_event_loop(_telethon_loop)
                _telethon_loop.run_forever()

            _telethon_loop_thread = threading.Thread(
                target=_run_forever, name="telethon-auth-loop", daemon=True
            )
            _telethon_loop_thread.start()
    return _telethon_loop


async def _get_persistent_telethon_client():
    """Runs ON the Telethon loop. Returns a connected Telethon client,
    creating/reconnecting if needed — same logic as ultra-main's."""
    global _telethon_client, _telethon_async_lock
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    from config import API_ID, API_HASH, SESSION

    if _telethon_async_lock is None:
        _telethon_async_lock = asyncio.Lock()
    if not SESSION:
        raise RuntimeError(
            "SESSION env var not set — the Telethon token-API tier needs a "
            "user-session string (see config.py's comment on SESSION)."
        )
    if _telethon_client is None:
        _telethon_client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
    if not _telethon_client.is_connected():
        await _telethon_client.connect()
    return _telethon_client


async def _async_get_auth_token() -> str:
    """Runs ON the Telethon loop — identical flow to ultra-main's
    get_auth_token(): logs into the "sky577bot" Mini App as the Telethon
    user session and pulls the bearer token out of the WebApp URL."""
    global _telethon_async_lock
    if _auth_cache["token"] and time.time() < _auth_cache["expires"]:
        return _auth_cache["token"]

    # BUG FIX: _telethon_async_lock used to only get created inside
    # _get_persistent_telethon_client() below — but the `async with` right
    # here runs BEFORE that function is ever called, so on the very first
    # request (or any time this races back to None) it was still None,
    # and `async with None:` blew up with "'NoneType' object does not
    # support the asynchronous context manager protocol". Every token
    # fetch hit this, which is why the token-API tier never actually
    # worked even with SESSION correctly configured. Create it here,
    # before it's needed, instead.
    if _telethon_async_lock is None:
        _telethon_async_lock = asyncio.Lock()

    async with _telethon_async_lock:
        if _auth_cache["token"] and time.time() < _auth_cache["expires"]:
            return _auth_cache["token"]

        from telethon.tl.functions.messages import RequestAppWebViewRequest
        from telethon.tl.types import InputBotAppShortName, InputPeerSelf, DataJSON
        from urllib.parse import urlparse, unquote

        client = await _get_persistent_telethon_client()
        bot = await client.get_input_entity("sky577bot")
        r = await client(RequestAppWebViewRequest(
            peer=InputPeerSelf(),
            app=InputBotAppShortName(bot_id=bot, short_name="open"),
            platform="android",
            write_allowed=True,
            start_param="",
            theme_params=DataJSON("{}"),
        ))
        token = unquote(urlparse(r.url).fragment.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion=", 1)[0])
        _auth_cache["token"] = token
        _auth_cache["expires"] = time.time() + 3600  # 1 hour — same as ultra-main
        return token



def get_auth_token_sync(timeout: float = 30) -> str:
    """Blocking sync entry point for the adapter functions below — the
    Telethon equivalent of just calling get_auth_token() directly, minus
    needing to be inside an async def on the Telethon loop. Raises
    whatever _async_get_auth_token() raised (including the SESSION-not-set
    RuntimeError above) if the token-API tier isn't usable."""
    loop = _ensure_telethon_loop()
    fut = asyncio.run_coroutine_threadsafe(_async_get_auth_token(), loop)
    return fut.result(timeout=timeout)


def _invalidate_auth_token():
    _auth_cache["token"] = None
    _auth_cache["expires"] = 0


def resolve_diskwala_with_auth_retry(link: str) -> dict:
    """Sync equivalent of ultra-main's resolve_diskwala_with_retry(): try
    the token-API tier (auto-refreshing the token once on a 401/403
    instead of failing until the 1-hour cache naturally expires), and let
    the caller (_resolve_no_auth below) fall back to HTML-scrape if this
    raises at all — including when SESSION isn't configured, so nothing
    breaks for anyone who hasn't set it up."""
    auth = get_auth_token_sync()
    try:
        return fetch_diskwala_video(link, auth)
    except DiskwalaAuthError as e:
        logger.warning(f"Diskwala auth token rejected ({e}), refreshing and retrying once...")
        _invalidate_auth_token()
        auth = get_auth_token_sync()
        return fetch_diskwala_video(link, auth)


def fetch_playlist_info_with_auth_retry(playlist_url: str) -> dict:
    """Same job as resolve_diskwala_with_auth_retry() above, but for
    playlists: fetch_playlist_info() needs an auth token same as
    fetch_diskwala_video() does, and playlists have no no-auth HTML-scrape
    fallback at all (the playlist page itself is a client-rendered SPA
    with no server-side file list to scrape) — so unlike single-video
    links, there's nothing to fall back to if this raises. Bug fix: this
    wrapper didn't exist at all — main.py called
    diskwala.fetch_playlist_info_with_auth_retry(...) expecting the same
    auth-token-plus-one-retry contract every other entry point in this
    file has, and got AttributeError instead, on every single playlist
    link (SESSION configured or not — fetch_playlist_info() was simply
    unreachable from main.py)."""
    auth = get_auth_token_sync()
    try:
        return fetch_playlist_info(playlist_url, auth)
    except DiskwalaAuthError as e:
        logger.warning(f"Diskwala auth token rejected ({e}), refreshing and retrying once...")
        _invalidate_auth_token()
        auth = get_auth_token_sync()
        return fetch_playlist_info(playlist_url, auth)


class DiskwalaAuthError(Exception):
    """Raised when the Diskwala miniapp API rejects the bearer token itself
    (HTTP 401/403) — distinct from a normal 'not found' / processing error,
    so callers know a fresh token (not a retry) is what's needed."""
    pass

API_DOWNLOAD = "https://api2.diskwala.net/api/diskwala/download"
API_STATUS = "https://api2.diskwala.net/api/diskwala/status"
# Bug fix / replacement: api2.diskwala.net's VidBunker routes needed a
# bearer token from the Diskwala miniapp AND a two-payload-shape workaround
# (see the old _fetch_diskwala_video_via_api docstring below) just to get a
# flat "not found" for most links. VIDBUNKER_WORKER_API (see
# _resolve_vidbunker_new below) is a dedicated, VidBunker-specific
# Cloudflare Worker — no auth token needed — that replaces those two
# endpoints entirely; VidBunker links no longer touch api2.diskwala.net at
# all (see _resolve_no_auth's VidBunker branch). Kept here (unused by
# VidBunker now) only because _get_endpoints() below still returns them for
# any caller that imports these names directly.
VIDBUNKER_API_DOWNLOAD = "https://api2.diskwala.net/api/vidbunker/download"
VIDBUNKER_API_STATUS = "https://api2.diskwala.net/api/vidbunker/status"
VIDBUNKER_WORKER_API = "https://vidbunker-backend.dailyweb577.workers.dev/api/download"
ENCRYPTION_KEY = "e7109544dab612bd5b80b8a427ac474ba5541b9efff7a4ca1c8ef85df2489c23"


def _get_endpoints(link: str) -> tuple[str, str]:
    """Return (download_api, status_api) based on which service the link belongs to."""
    if "flezen.com" in link.lower():
        return (
            "https://api2.diskwala.net/api/flezen/download",
            "https://api2.diskwala.net/api/flezen/status?link=",
        )
    if "vidbunker.in" in link.lower():
        return (
            VIDBUNKER_API_DOWNLOAD,
            VIDBUNKER_API_STATUS + "?link=",
        )
    return (
        API_DOWNLOAD,
        API_STATUS + "?link=",
    )


def decrypt_file(file_data: dict) -> dict:
    """Decrypt AES-GCM encrypted file response from Diskwala API.

    The API has been observed using two different byte orderings:
      - Old:  ciphertext = p_hex + h_hex  (p=ciphertext, h=auth-tag)
      - New:  ciphertext = p_hex, tag appended separately as h_hex
    We try the new (ct + tag) order first; if AESGCM rejects it we fall
    back to the old (p + h) order so both API versions work.
    """
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM

    key = bytes.fromhex(ENCRYPTION_KEY)
    iv  = bytes.fromhex(file_data["s"])
    p   = bytes.fromhex(file_data["p"])
    h   = bytes.fromhex(file_data["h"])
    aesgcm = AESGCM(key)

    # Try new order: ct + tag
    try:
        plaintext = aesgcm.decrypt(iv, p + h, None)
        return json.loads(plaintext.decode("utf-8"))
    except Exception:
        pass

    # Fallback: old order p_hex + h_hex treated as one blob
    try:
        plaintext = aesgcm.decrypt(iv, bytes.fromhex(file_data["p"] + file_data["h"]), None)
        return json.loads(plaintext.decode("utf-8"))
    except Exception as e:
        raise ValueError(f"AES-GCM decryption failed (tried both byte orderings): {e}") from e


def extract_diskwala_links(text: str) -> list[str]:
    """Extract Diskwala/Flezen/VidBunker URLs from text.

    Supports all known URL patterns:
      diskwala.com/app|playlist|d|v|f|s|share|view/<id>
      filecrush.com/...  filesadda.com/...  miniapp.diskwala.net/...
      t.me/<botname>?startapp=<id>  (Telegram Mini App deep links)
      flezen.com/s|share|f|v|d/<id>
      vidbunker.in/watch/<id>  (or a bare id if that's the whole message)
    """
    patterns = [
        # diskwala.com — broad match (any path), not restricted to a
        # specific path-segment list, so a new URL shape Diskwala adds in
        # the future is still caught automatically.
        r"https?://\S*diskwala\.com/\S+",
        # thediskwala.com / filecrush.com / filesadda.com — still the
        # specific known path styles (app/playlist/d/v/f/s/share/view).
        # Bug fix: "thediskwala.com" used to be invisible to this regex —
        # the optional subdomain group (?:[\w.-]+\.)? requires a literal
        # dot before "diskwala", but "thediskwala" glues "the" straight
        # onto "diskwala" with no dot, so it matched neither the prefix
        # group nor the literal "diskwala.*" alternative. main.py's own
        # /help text advertises thediskwala.com/app/... as a supported
        # domain, so listing it explicitly here fixes real (not just
        # theoretical) missed links.
        r"https?://(?:[\w.-]+\.)?(?:thediskwala\.[a-z]{2,}|filecrush\.[a-z]{2,}|filesadda\.[a-z]{2,})"
        r"/(?:app|playlist|sharing/link|share|d|view|v|f|s)/[A-Za-z0-9_-]+\S*",
        # Diskwala Mini App (browser)
        r"https?://miniapp\.diskwala\.net/\S+",
        # Telegram Mini App deep links (t.me/<bot>?startapp=<id>)
        r"https?://t\.me/(?:sky577bot|diskwalabot|[\w]+bot)(?:/[a-zA-Z0-9_-]+)?\?(?:startapp|start)=\S+",
        # Flezen share links
        r"https?://(?:www\.)?flezen\.com/(?:s|share|f|v|d)/[A-Za-z0-9_-]+",
        r"https?://(?:www\.)?flezen\.com/[A-Za-z0-9_-]+",
        # VidBunker share links
        r"(?:https?://)?(?:www\.)?vidbunker\.in/watch/[A-Za-z0-9_-]+",
    ]
    links = []
    for pattern in patterns:
        links.extend(re.findall(pattern, text))
    links = list(dict.fromkeys(links))
    # Drop prefix-only matches (e.g. "flezen.com/s" subset of "flezen.com/s/abc123")
    links = [l for l in links if not any(other != l and other.startswith(l) for other in links)]

    # Bare VidBunker id: sometimes the whole message is just the id with no
    # domain at all (e.g. "aB3xY9zK1m"). Only treat this as a link when
    # nothing else matched AND the whole stripped message is exactly this
    # shape — matching it inside arbitrary text would misfire on ordinary
    # chat.
    if not links:
        stripped = text.strip()
        if re.fullmatch(r"[A-Za-z0-9_-]{6,15}", stripped):
            links.append(f"https://vidbunker.in/watch/{stripped}")

    return links


# General-purpose Diskwala share-ID shape (24 hex chars) — Diskwala's own
# IDs are always this shape regardless of which URL path style wraps them.
# Used as a companion to the URL patterns above: extract_diskwala_id()
# below pulls one out of ANY diskwala.com URL (or bare text) that contains
# one, even a path shape none of the patterns above were written for yet.
DISKWALA_LINK_ID_RE = re.compile(r"[a-fA-F0-9]{24}")


def extract_diskwala_id(link_or_text: str) -> str | None:
    m = DISKWALA_LINK_ID_RE.search(link_or_text or "")
    return m.group(0) if m else None


FLEZEN_ID_RE = re.compile(
    r"flezen\.[a-z]{2,}/(?:s|share|f|v|d)/([a-zA-Z0-9_-]+)|flezen\.[a-z]{2,}/([a-zA-Z0-9_-]+)",
    re.IGNORECASE,
)


def _extract_flezen_id(link: str) -> str | None:
    m = FLEZEN_ID_RE.search(link)
    if m:
        return m.group(1) or m.group(2)
    return None


VIDBUNKER_ID_RE = re.compile(
    r"vidbunker\.[a-z]{2,}/watch/([A-Za-z0-9_-]+)",
    re.IGNORECASE,
)


def _extract_vidbunker_id(link: str) -> str | None:
    m = VIDBUNKER_ID_RE.search(link)
    if m:
        return m.group(1)
    return None


# Free public third-party resolver for diskwala.com/net/app links — no
# auth, no cookies, single GET request. Ported from terabox-diskwala-bot's
# src/providers/diskwala.js. Not affiliated with Diskwala; can go down or
# rate-limit independently of Diskwala's own site, so it's tried as a fast
# FIRST attempt in _resolve_no_auth below, falling through to the existing
# token-API/HTML-scrape chain unchanged if it fails — never a replacement
# for that chain, just a quicker path when it happens to work.
_DISKWALA_VERCEL_RESOLVER = "https://diskwala-dl-six.vercel.app/api/scrap"


def _resolve_diskwala_vercel(link: str) -> dict:
    r = requests.get(
        _DISKWALA_VERCEL_RESOLVER,
        params={"q": link},
        headers={"User-Agent": HTML_USER_AGENT, "Accept": "application/json"},
        timeout=20,
    )
    if not r.ok:
        raise Exception(f"Diskwala Vercel resolver failed (HTTP {r.status_code})")
    try:
        data = r.json()
    except ValueError:
        raise Exception("Diskwala Vercel resolver returned an invalid response")

    file = (data.get("data") or {}).get("file") or {}
    if not data.get("success") or not file.get("downloadUrl"):
        raise Exception("Diskwala Vercel resolver: link not resolvable (private, deleted, or a playlist)")

    ext = re.sub(r"[^a-z0-9]", "", str(file.get("extension") or "mp4").lower()) or "mp4"
    name = str(file.get("name") or "diskwala").strip() or "diskwala"
    if not name.lower().endswith(f".{ext}"):
        name = f"{name}.{ext}"

    dl = file["downloadUrl"]
    return {
        "name": name, "extension": ext, "size": file.get("size") or 0,
        "downloadUrl": dl, "streamUrl": dl,
        "thumb": file.get("thumb") or None, "creator": None, "duration_seconds": None,
        "views": None, "likes": None, "description": None,
        "upload_date": None, "category": "Video",
    }


def _vb_guess_name(watch_url: str) -> str:
    """Guess a filename from the VidBunker watch URL slug.
    Used when the direct API doesn't return a filename field.
    e.g. https://vidbunker.in/watch/AbCd1234 → AbCd1234.mp4
    """
    from urllib.parse import urlparse as _up
    slug = _up(watch_url).path.rstrip("/").split("/")[-1] or "video"
    return f"{slug}.mp4"


def _resolve_vidbunker_new(link: str) -> dict:
    """VidBunker resolve via the dedicated Cloudflare Worker API, ported
    from Vid-bunker-downloader-main's vidbot/extractor.py — this REPLACES
    the old api2.diskwala.net/api/vidbunker/{download,status} pair
    entirely for VidBunker links (see VIDBUNKER_WORKER_API's comment
    above for why: that pair needed a Diskwala miniapp bearer token and a
    payload-shape workaround just to mostly return "not found"). This
    worker is VidBunker-specific, needs no auth token, and answers
    directly in one call — POST first (retried with backoff on transient
    5xx/429s), then a GET-with-content-type-check fallback if the POST
    never comes back with a usable link, same order/logic as the
    reference implementation.

    Returns the same dict shape every other resolve function in this file
    does, so get_available_qualities/get_stream_url/get_page_meta below
    don't need to know VidBunker is handled differently now. Only
    name/size/downloadUrl/streamUrl are populated — this worker doesn't
    hand back thumbnail/duration/creator/etc. the way the old token-API
    tier did, so those stay None; get_page_meta already falls back to the
    HTML __NEXT_DATA__ scrape for that metadata when a resolve result is
    missing it (see get_page_meta's own docstring), so display info isn't
    lost, just sourced from a second request when needed."""
    last_error = None

    for attempt in range(4):
        try:
            r = requests.post(
                VIDBUNKER_WORKER_API, json={"url": link},
                headers={"Content-Type": "application/json"}, timeout=60,
            )
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    data = {}
                dl = data.get("link")
                if dl:
                    name = data.get("filename") or _vb_guess_name(link)
                    ext = os.path.splitext(name)[1].lstrip(".").lower() or "mp4"
                    return {
                        "name": name, "extension": ext,
                        "size": data.get("size") or 0,
                        "downloadUrl": dl, "streamUrl": dl,
                        "thumb": None, "creator": None, "duration_seconds": None,
                        "views": None, "likes": None, "description": None,
                        "upload_date": None, "category": "Video",
                    }
                last_error = f"API 200 but no link: {data}"
            elif r.status_code in (429, 500, 502, 503, 504):
                last_error = f"transient status {r.status_code}"
            else:
                last_error = f"API status {r.status_code}: {r.text[:200]}"
                break
        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)
        time.sleep(min(2 ** attempt, 10))

    # Fallback: same worker, GET/query-string form — a failure here comes
    # back as JSON/HTML, so a real video is distinguished by content-type
    # rather than trying to parse a body that might not even be JSON.
    fallback_url = f"{VIDBUNKER_WORKER_API}?url={quote(link, safe='')}"
    try:
        with requests.get(fallback_url, stream=True, timeout=60, allow_redirects=True) as r:
            ctype = r.headers.get("content-type", "")
            if r.status_code == 200 and ("video" in ctype or "octet-stream" in ctype):
                name = _vb_guess_name(link)
                return {
                    "name": name, "extension": os.path.splitext(name)[1].lstrip(".") or "mp4",
                    "size": int(r.headers.get("content-length") or 0),
                    "downloadUrl": fallback_url, "streamUrl": fallback_url,
                    "thumb": None, "creator": None, "duration_seconds": None,
                    "views": None, "likes": None, "description": None,
                    "upload_date": None, "category": "Video",
                }
            last_error = f"fallback status {r.status_code}, content-type {ctype!r}"
    except requests.RequestException as exc:
        last_error = str(exc)

    raise Exception(f"VidBunker: could not resolve {link}: {last_error}")


def _flezen_save_and_resolve(share_id: str, session: requests.Session) -> tuple[str | None, str | None]:
    """If a logged-in FLEZEN_COOKIE is configured, save the file to that
    account and pull the resulting direct download and stream links.

    Returns (download_url, stream_url) — matched SEPARATELY, each against
    its own keyword, rather than one combined "download|stream|file"
    regex that just took whichever of the three happened to appear first
    in the page. That's what caused the Stream button to trigger a
    forced browser download instead of playing inline: if the files
    page's "download" link came before its "stream" link in the HTML
    (the more common layout — download is usually the primary/first
    action), the old regex grabbed the download link for BOTH purposes,
    every time, regardless of a genuine stream link existing right below
    it. Falls back to whichever one link was found for both fields if
    the page only exposes one kind."""
    try:
        session.get(f"https://flezen.com/user/save?id={share_id}", allow_redirects=True, timeout=15)
        files_page = session.get("https://flezen.com/user/files", timeout=15)
        if files_page.status_code != 200:
            return None, None
        text = files_page.text

        stream_match = re.search(r"href=['\"](https?://[^'\"]*stream[^'\"]*)['\"]", text)
        download_match = re.search(r"href=['\"](https?://[^'\"]*download[^'\"]*)['\"]", text)
        if not (stream_match or download_match):
            # Neither specific keyword matched — last resort, the old
            # generic "file" pattern, same link for both.
            generic_match = re.search(r"href=['\"](https?://[^'\"]*file[^'\"]*)['\"]", text)
            if generic_match:
                return generic_match.group(1), generic_match.group(1)
            return None, None

        download_url = download_match.group(1) if download_match else None
        stream_url = stream_match.group(1) if stream_match else None
        return (download_url or stream_url), (stream_url or download_url)
    except Exception as e:
        logger.info(f"Flezen account save/resolve failed: {e}")
        return None, None


def _extract_flezen_creator(html_text: str) -> str | None:
    """Flezen's own "uploaded by" field lives in a plain HTML element on
    the share page, not a JSON blob — unlike Diskwala, which exposes
    creator/uploader through __NEXT_DATA__/JSON-LD that
    _scrape_preview_from_html's other patterns already cover. This
    mirrors the same site convention resolve_flezen_html already relies
    on for upload_date (a data-datetime attribute) and views (an icon
    immediately followed by a <p class="text-gray-600"> value): tries a
    handful of the most common Remix Icon glyph names used for a
    "user/uploader" icon on this kind of Tailwind-styled page, then a
    couple of plain data-attribute and visible "Uploaded by ..." text
    fallbacks. No single shape is 100% confirmed without a live page
    sample, so this casts a reasonably wide net rather than betting on
    one exact pattern the way the title/views regexes above can (those
    were confirmed against a real page)."""
    if not html_text:
        return None

    for attr in ("data-uploader", "data-username", "data-creator", "data-author", "data-user"):
        m = re.search(rf'{attr}=["\']([^"\']+)["\']', html_text)
        if m and m.group(1).strip():
            return m.group(1).strip()

    for icon in ("ri-user-line", "ri-user-3-line", "ri-user-follow-line",
                 "ri-account-circle-line", "ri-upload-cloud-2-line", "ri-user-star-line"):
        m = re.search(
            rf'<i class=["\']{icon}["\'][^>]*>.*?<p class=["\']text-gray-600["\']>([^<]+)</p>',
            html_text, re.DOTALL,
        )
        if m and m.group(1).strip():
            return m.group(1).strip()

    m = re.search(r'(?:Uploaded\s*by|Uploader|Posted\s*by)\s*[:\-]?\s*<[^>]*>\s*([^<\n]{2,60})<', html_text, re.I)
    if m and m.group(1).strip():
        return m.group(1).strip()

    m = re.search(r'(?:Uploaded\s*by|Uploader|Posted\s*by)\s*[:\-]?\s*([A-Za-z0-9_.\-]{2,40})', html_text, re.I)
    if m:
        candidate = m.group(1).strip()
        if candidate.lower() not in ("admin", "system", "bot", "diskwala", "flezen", "null", "undefined"):
            return candidate

    return None


def _parse_duration_string(raw: str) -> int | None:
    """Turns '7:17' / '1:07:17' / a bare '437' into total seconds. Returns
    None (rather than 0) for anything unparseable, so callers can tell
    "found nothing" apart from a genuine 0-second value."""
    if not raw:
        return None
    raw = raw.strip()
    try:
        if ":" in raw:
            parts = [int(p) for p in raw.split(":")]
            return sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
        return int(float(raw))
    except (ValueError, TypeError):
        return None


def _extract_flezen_duration(html_text: str) -> int | None:
    """Flezen's video duration — same caveat as _extract_flezen_creator
    above: no confirmed single markup shape without a live page sample,
    so this tries a data-attribute first, then the same icon+paragraph
    convention the page already uses for views/size elsewhere in
    resolve_flezen_html, with a handful of the most common Remix Icon
    names for a clock/duration glyph, then a last-resort JSON-style
    "duration": ... scan of the raw page text."""
    if not html_text:
        return None

    for attr in ("data-duration", "data-seconds", "data-length", "data-runtime"):
        m = re.search(rf'{attr}=["\']([^"\']+)["\']', html_text)
        if m:
            secs = _parse_duration_string(m.group(1))
            if secs:
                return secs

    for icon in ("ri-time-line", "ri-timer-line", "ri-play-circle-line",
                 "ri-film-line", "ri-video-line", "ri-movie-line"):
        m = re.search(
            rf'<i class=["\']{icon}["\'][^>]*>.*?<p class=["\']text-gray-600["\']>([^<]+)</p>',
            html_text, re.DOTALL,
        )
        if m:
            secs = _parse_duration_string(m.group(1).strip())
            if secs:
                return secs

    m = re.search(r'"duration"\s*:\s*"?(\d+(?::\d+){0,2})"?', html_text)
    if m:
        secs = _parse_duration_string(m.group(1))
        if secs:
            return secs

    return None


def resolve_flezen_html(link: str) -> dict:
    """Flezen-specific fallback: scrape the flezen.com share page directly.
    Gives a clear 'link deleted/expired' error when the page itself says so
    (instead of a generic API 'not found'), and — if FLEZEN_COOKIE is
    configured — resolves real download AND stream URLs via the logged-in
    account.
    """
    share_id = _extract_flezen_id(link)
    if not share_id:
        raise Exception(f"Could not extract Flezen share ID from: {link}")

    page_url = f"https://flezen.com/s/{share_id}"
    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://flezen.com/",
    }

    session = requests.Session()
    session.headers.update(headers)
    if FLEZEN_COOKIE:
        session.headers["Cookie"] = FLEZEN_COOKIE.strip()

    r = session.get(page_url, timeout=15)
    if r.status_code == 404:
        raise Exception("This Flezen link does not exist or has been deleted by the uploader.")
    if r.status_code != 200:
        # Original share URL as given might use a different path style — retry with it directly
        r = session.get(link, timeout=15)
        if r.status_code != 200:
            raise Exception(f"Flezen returned HTTP {r.status_code}")

    page_html = r.text

    title_match = re.search(r"<h1[^>]*>(.*?)</h1>", page_html, re.DOTALL)
    if not title_match:
        title_match = re.search(
            r'<p[^>]*class=["\'][^"\']*text-gray-600 break-all[^"\']*["\'][^>]*>(.*?)</p>',
            page_html, re.DOTALL,
        )

    bytes_match = re.search(r'data-bytes=["\'](\d+)["\']', page_html)
    size = int(bytes_match.group(1)) if bytes_match else 0

    if (not title_match and not bytes_match):
        raise Exception("This Flezen link does not exist or has been deleted by the uploader.")

    if title_match:
        raw_title = re.sub(r"<[^>]+>", "", title_match.group(1)).strip()
        filename = html.unescape(raw_title).strip()
    else:
        filename = f"flezen_{share_id}.mp4"

    if "can't find this file" in filename.lower() or "file not found" in filename.lower():
        raise Exception("This Flezen link does not exist or has been deleted by the uploader.")

    _ALL_KNOWN_EXTS = (
        ".mp4", ".mkv", ".webm", ".mov", ".avi", ".flv", ".wmv", ".m4v", ".ts", ".3gp",
        ".mp3", ".flac", ".aac", ".ogg", ".m4a", ".wav", ".opus", ".wma",
        ".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".heic", ".avif",
        ".pdf", ".doc", ".docx", ".ppt", ".pptx", ".xls", ".xlsx", ".txt",
        ".zip", ".rar", ".7z", ".tar", ".gz",
    )
    if not any(filename.lower().endswith(ext) for ext in _ALL_KNOWN_EXTS):
        filename += ".mp4"

    # Extract upload_date and views (from TeraBox-Video-Downloader project)
    upload_date = None
    dt_match = re.search(r'data-datetime=["\']([^"\']+)["\']', page_html)
    if dt_match:
        upload_date = dt_match.group(1).strip()

    views = None
    views_match = re.search(
        r'<i class=["\']ri-eye-line["\'][^>]*>.*?<p class=["\']text-gray-600["\']>(\d+)</p>',
        page_html, re.DOTALL
    )
    if views_match:
        views = int(views_match.group(1))

    # Bug fix: this field was entirely missing before — main.py's
    # video_info.get("creator") always came back None for Flezen links,
    # even when the page itself does show an uploader name.
    creator = _extract_flezen_creator(page_html)

    # Bug fix: same as creator above — this field was entirely missing
    # before, so video_info.get("duration_seconds") always came back None
    # for Flezen links even when the page itself does show a duration.
    duration_seconds = _extract_flezen_duration(page_html)

    # FIX: Extract thumbnail from og:image on the Flezen share page.
    # Previously this always returned thumb=None because the FLEZEN_COOKIE-
    # gated _flezen_save_and_resolve() path was the only place a URL was
    # found, and it never parsed the share page for a thumbnail. The og:image
    # meta tag on Flezen's share page IS the video thumbnail — no cookie needed.
    thumb = None
    og_img_m = re.search(
        r"""<meta[^>]+property=["']og:image["'][^>]+content=["']([^"']+)["']"""
        r"""|<meta[^>]+content=["']([^"']+)["'][^>]+property=["']og:image["']""",
        page_html,
    )
    if og_img_m:
        thumb = og_img_m.group(1) or og_img_m.group(2)
        if thumb and not thumb.startswith("http"):
            thumb = urljoin("https://flezen.com", thumb)

    download_url = None
    stream_url = None
    if FLEZEN_COOKIE:
        download_url, stream_url = _flezen_save_and_resolve(share_id, session)

    if not download_url:
        raise Exception(
            f"Flezen file '{filename}' ({size} bytes) exists, but a direct download URL "
            f"could not be obtained — Flezen only serves direct links to a logged-in account. "
            f"Set FLEZEN_COOKIE in .env to enable this."
        )

    logger.info(f"Flezen HTML fallback resolved: {filename} -> download={download_url[:120]} stream={(stream_url or download_url)[:120]}")

    # Extension from filename
    _flezen_ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else None

    # Category from extension
    _V = {"mp4","mkv","avi","mov","webm","flv","wmv","m4v","ts","3gp","m2ts","vob"}
    _A = {"mp3","flac","aac","ogg","m4a","wav","opus","wma","alac","aiff"}
    _I = {"jpg","jpeg","png","gif","webp","bmp","tiff","tif","svg","heic","heif","avif"}
    _D = {"pdf","doc","docx","ppt","pptx","xls","xlsx","txt","zip","rar","7z","tar","gz"}
    _flezen_cat = ("Video"    if _flezen_ext in _V else
                   "Audio"    if _flezen_ext in _A else
                   "Photo"    if _flezen_ext in _I else
                   "Document" if _flezen_ext in _D else None)

    return {
        "name":             filename,
        "extension":        _flezen_ext,
        "category":         _flezen_cat,
        "size":             size,
        "downloadUrl":      download_url,
        "streamUrl":        stream_url or download_url,
        "thumb":            thumb,   # FIX: was always None, now from og:image
        "creator":          creator,
        "upload_date":      upload_date,
        "duration_seconds": duration_seconds,
        "views":            views,
    }


def resolve_diskwala_html(link: str) -> dict:
    """Fallback resolver: scrape the Diskwala/Flezen share page directly for
    a video URL, bypassing the bearer-token miniapp API entirely. Used when
    api2.diskwala.net returns an error (e.g. 404 "not found") for a link
    that otherwise loads fine in a browser.
    """
    if "vidbunker" in link.lower():
        headers = {
            "User-Agent": HTML_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://vidbunker.in/",
            "Origin": "https://vidbunker.in",
        }
    elif "flezen" in link.lower():
        # Bug fix: this function is also used as fetch_diskwala_video's
        # generic last-resort fallback for Flezen links (after
        # resolve_flezen_html and the token-API both fail) — without this
        # branch it fell into the diskwala.com else-case below and sent a
        # diskwala.com Referer/Origin to flezen.com, which can make the
        # scrape less likely to succeed.
        headers = {
            "User-Agent": HTML_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://flezen.com/",
            "Origin": "https://flezen.com",
        }
    else:
        headers = {
            "User-Agent": HTML_USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.diskwala.com/",
            "Origin": "https://www.diskwala.com",
        }

    r = requests.get(link, headers=headers, timeout=20, allow_redirects=True)
    r.raise_for_status()
    # Named html_text, not html — this function's local scope used to
    # shadow the module-level `import html` (stdlib, used for
    # html.unescape elsewhere in this file). Harmless today since nothing
    # in this function calls html.unescape, but a landmine for any future
    # edit that adds one here.
    html_text = r.text
    soup = BeautifulSoup(html_text, "html.parser")

    name = None
    thumb = None
    download_url = None

    og_title = soup.find("meta", property="og:title")
    if og_title and og_title.get("content"):
        name = og_title["content"].strip()

    og_image = soup.find("meta", property="og:image")
    if og_image and og_image.get("content"):
        thumb = urljoin(link, og_image["content"].strip())

    # <video>/<source> tags
    for tag in soup.find_all(["video", "source"]):
        src = tag.get("src")
        if src:
            download_url = urljoin(link, src.strip().strip("\"'"))
            break

    # Embedded JSON / JS blobs
    if not download_url:
        json_url_patterns = [
            r'"downloadUrl"\s*:\s*"([^"]+)"',
            r'"download_url"\s*:\s*"([^"]+)"',
            r'"directUrl"\s*:\s*"([^"]+)"',
            r'"direct_url"\s*:\s*"([^"]+)"',
            r'"contentUrl"\s*:\s*"([^"]+)"',
            r'"content_url"\s*:\s*"([^"]+)"',
        ]
        for script in soup.find_all("script"):
            content = script.string
            if not content:
                continue
            matched = False
            for pattern in json_url_patterns:
                m = re.search(pattern, content)
                if m:
                    download_url = urljoin(link, m.group(1))
                    matched = True
                    break
            if not matched:
                m2 = re.search(r'(https?://[^\s"\'<>]+\.(?:mp4|mkv|webm|m3u8)[^\s"\'<>]*)', content)
                if m2:
                    download_url = m2.group(1)
                    matched = True
            if matched:
                break

    # Last resort: raw media-URL scan of the full page text
    if not download_url:
        m3 = re.search(r'(https?://[^\s"\'<>]+\.(?:mp4|mkv|webm|m3u8)[^\s"\'<>]*)', html_text)
        if m3:
            download_url = m3.group(1)

    # __NEXT_DATA__ — Diskwala, VidBunker and Flezen are Next.js apps.
    # The video URL is embedded in window.__NEXT_DATA__ JSON, not in
    # <video> tags or plain script variables. This is the most reliable
    # extraction path when the API token is expired or unavailable.
    if not download_url:
        nd_match = re.search(r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(\{.*?\})</script>', html_text, re.DOTALL)
        if nd_match:
            try:
                nd = json.loads(nd_match.group(1))
                nd_text = json.dumps(nd)
                # Search entire JSON blob for any video URL
                for pat in [
                    r'"(?:downloadUrl|download_url|directUrl|direct_url|contentUrl|content_url|streamUrl|stream_url|url|videoUrl|video_url)"\s*:\s*"(https?://[^"]+\.(?:mp4|mkv|webm|m3u8)[^"]*)"',
                    r'"(?:src|source|file|link)"\s*:\s*"(https?://[^"]+\.(?:mp4|mkv|webm|m3u8)[^"]*)"',
                ]:
                    m4 = re.search(pat, nd_text, re.IGNORECASE)
                    if m4:
                        download_url = m4.group(1).replace('\\/', '/')
                        break
                # Also grab title and thumbnail from __NEXT_DATA__ if not found yet
                if not name:
                    for pat in [r'"(?:title|name|filename)"\s*:\s*"([^"]{3,200})"']:
                        tm = re.search(pat, nd_text)
                        if tm:
                            name = tm.group(1)
                            break
                if not thumb:
                    for pat in [r'"(?:thumbnail|thumb|poster|image|cover)"\s*:\s*"(https?://[^"]+)"']:
                        im = re.search(pat, nd_text)
                        if im:
                            thumb = im.group(1).replace('\\/', '/')
                            break
            except Exception as e:
                logger.debug(f"__NEXT_DATA__ parse failed: {e}")

    # Last-ditch: internal metadata endpoint — extended to cover VidBunker
    # and Flezen IDs in addition to Diskwala app IDs.
    if not download_url:
        id_match = (
            re.search(r"diskwala\.com/app/([A-Za-z0-9]+)", link)
            or re.search(r"vidbunker\.in/watch/([A-Za-z0-9_-]+)", link)
            or re.search(r"flezen\.com/(?:s|share|f|v|d)/([A-Za-z0-9_-]+)", link)
        )
        if id_match:
            try:
                # Try all known metadata endpoints
                endpoints_to_try = [
                    ("POST", "https://api2.diskwala.net/api/v1/file/temp_info", {"id": id_match.group(1)}),
                    ("POST", "https://api2.diskwala.net/api/vidbunker/temp_info", {"id": id_match.group(1)}),
                    ("POST", "https://api2.diskwala.net/api/flezen/temp_info",   {"id": id_match.group(1)}),
                ]
                for method, url_ep, payload in endpoints_to_try:
                    try:
                        api_resp = requests.post(url_ep, json=payload, headers=headers, timeout=15)
                        if api_resp.status_code == 200:
                            data = api_resp.json()
                            payload_data = data.get("data") if isinstance(data.get("data"), dict) else data
                            for key in ("downloadUrl", "download_url", "url", "video_url", "streamUrl"):
                                if payload_data.get(key):
                                    download_url = payload_data[key]
                                    break
                            if download_url:
                                break
                    except Exception:
                        continue
            except Exception as e:
                logger.info(f"HTML-fallback metadata endpoint also failed: {e}")

    if not download_url:
        # Diagnostic breadcrumb for whoever reads the server logs next —
        # the generic "not found" message alone doesn't say WHY the
        # scrape came up empty (dead file vs. a changed page structure
        # vs. a bot-detection challenge page), and there's no way to
        # re-fetch this exact response later to check. A short snippet
        # plus a couple of common tells is usually enough to tell those
        # apart at a glance without needing to reproduce the request.
        page_title_tag = soup.find("title")
        page_title_text = page_title_tag.get_text(strip=True) if page_title_tag else None
        lower_html = html_text.lower()
        looks_challenged = any(
            marker in lower_html
            for marker in ("just a moment", "checking your browser", "cf-challenge", "captcha", "cloudflare")
        )
        logger.warning(
            f"resolve_diskwala_html: no media URL found for {link} — "
            f"page <title>: {page_title_text!r}, "
            f"looks_like_bot_challenge={looks_challenged}, "
            f"html_len={len(html_text)}, snippet={html_text[:300]!r}"
        )
        raise Exception("not found (HTML fallback also found no media URL)")

    if not name:
        fn_match = re.search(r"/([^/?#]+?)(?:\?|#|$)", download_url)
        name = fn_match.group(1) if fn_match else "video.mp4"
    if "." not in name:
        ext_match = re.search(r"\.([a-zA-Z0-9]{2,5})(?:\?|#|$)", download_url)
        name += "." + ext_match.group(1) if ext_match else ".mp4"

    # Extra metadata: this fallback previously returned creator/upload_date/
    # views/likes/description/category as entirely absent, no matter what
    # the page or Diskwala's own API actually had — a token-API failure
    # (the only reason this HTML fallback ever runs) meant those fields
    # silently disappeared even though two other sources for them already
    # exist elsewhere in this file. Reuses both, same combination
    # fetch_diskwala_preview already relies on: the page's own
    # __NEXT_DATA__/JSON-LD (_scrape_preview_from_html) plus Diskwala's
    # internal temp_info endpoint, which — unlike for Flezen links — DOES
    # recognize genuine diskwala.com/app/<id> file IDs.
    scraped_meta = {}
    try:
        scraped_meta = _scrape_preview_from_html(link, html_text)
    except Exception as e:
        logger.info(f"resolve_diskwala_html: metadata scrape failed: {e}")

    temp_meta = {}
    try:
        temp_meta = fetch_diskwala_temp_info(link)
    except Exception as e:
        logger.info(f"resolve_diskwala_html: temp_info fetch failed: {e}")

    if not thumb:
        thumb = scraped_meta.get("thumb") or temp_meta.get("thumb")

    logger.info(f"HTML fallback resolved: {name} -> {download_url[:120]}")

    return {
        "name": name,
        "size": 0,
        "downloadUrl": download_url,
        "streamUrl": download_url,
        "thumb": thumb,
        "creator": temp_meta.get("creator") or scraped_meta.get("author"),
        "duration_seconds": temp_meta.get("duration_seconds") or scraped_meta.get("duration_seconds"),
        "views": temp_meta.get("views"),
        "likes": temp_meta.get("likes"),
        "description": temp_meta.get("description"),
        "upload_date": temp_meta.get("upload_date") or scraped_meta.get("upload_date"),
        "category": temp_meta.get("category"),
    }


def fetch_diskwala_video(link: str, auth: str) -> dict:
    """Fetch video info, routing Flezen and Diskwala links differently:

    - Flezen links go straight to resolve_flezen_html() FIRST — scraping
      flezen.com directly, not through Diskwala's infrastructure at all.
      The token-API path only reaches Flezen via _get_endpoints()'s
      api2.diskwala.net/api/flezen/* proxy of the same site, which is a
      second-hand route through someone else's infra rather than the
      site itself — trying that first meant Flezen links almost always
      fell through to the (correct, direct) HTML path anyway, just after
      a wasted round-trip and a delay every time. Only falls back to the
      token-API/generic HTML path if the direct scrape itself fails, so
      there's still a fallback — it's just no longer tried first.

    - Diskwala links are unaffected — still token-API first, with the
      existing HTML-scrape fallback if that fails, same as before."""
    if "flezen." in link.lower():
        try:
            return resolve_flezen_html(link)
        except Exception as flezen_error:
            logger.warning(f"Flezen direct HTML fetch failed ({flezen_error}), trying token-API fallback...")
            try:
                return _fetch_diskwala_video_via_api(link, auth)
            except Exception as api_error:
                logger.warning(f"Token-API fallback also failed ({api_error}), trying generic HTML fallback...")
                try:
                    return resolve_diskwala_html(link)
                except Exception:
                    # All three attempts failed — the direct Flezen-
                    # specific error (dead link / needs FLEZEN_COOKIE) is
                    # more useful to the user than the generic API error.
                    raise flezen_error

    try:
        return _fetch_diskwala_video_via_api(link, auth)
    except Exception as api_error:
        logger.warning(f"Token-API fetch failed ({api_error}), trying HTML fallback...")
        try:
            return resolve_diskwala_html(link)
        except Exception as html_error:
            logger.warning(f"HTML fallback also failed: {html_error}")
            raise api_error


def _fetch_diskwala_video_via_api(link: str, auth: str) -> dict:
    """Original bearer-token miniapp API path.

    BUG FIX — VidBunker: the download endpoint was always called with
    {"link": link}, the same payload shape Diskwala/Flezen's endpoints
    take. But this file's OWN last-ditch temp_info fallback further up
    (resolve_diskwala_html's metadata lookup) calls ITS vidbunker
    endpoint with {"id": <share id>} instead — suggesting
    api2.diskwala.net's vidbunker routes key off the bare share id, not
    the full watch URL. Every VidBunker link was coming back a flat
    "not found" from this endpoint, including freshly-shared links that
    open fine in a browser — that pattern (100% failure regardless of
    which file) matches an endpoint being sent a payload shape it
    doesn't recognize, not every link actually being missing.

    So for VidBunker specifically, {"id": id} is now tried FIRST —
    falling back to the original {"link": link} if that also comes back
    not-ok, so this can't make a previously-working case worse. The
    status-poll identifier below follows whichever payload the download
    call actually succeeded with, on the assumption a REST API keys its
    status lookup the same way it keyed the request that created it."""
    headers = {
        "Authorization": f"Bearer {auth}",
        "X-Bot-Id": "diskwala",
        "Content-Type": "application/json",
        "Origin": "https://miniapp.diskwala.net",
        "Referer": "https://miniapp.diskwala.net/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
    }

    download_api, status_api_prefix = _get_endpoints(link)

    vb_id = _extract_vidbunker_id(link) if "vidbunker.in" in link.lower() else None
    payloads_to_try = [{"id": vb_id}, {"link": link}] if vb_id else [{"link": link}]

    data = {}
    status_identifier = link
    for payload in payloads_to_try:
        logger.info(f"Calling API: {download_api} payload_keys={list(payload.keys())}")
        r = requests.post(download_api, headers=headers, json=payload, timeout=60)
        logger.info(f"Download response: {r.status_code} - {r.text[:200]}")
        if r.status_code in (401, 403):
            raise DiskwalaAuthError(f"Diskwala auth token rejected (HTTP {r.status_code})")
        try:
            data = r.json()
        except Exception:
            data = {"ok": False, "error": f"non-JSON response (HTTP {r.status_code})"}
        if data.get("ok"):
            status_identifier = payload.get("id") or payload.get("link")
            break

    if not data.get("ok"):
        raise Exception(data.get("error", f"API Error: {data}"))

    status_url = status_api_prefix + quote(status_identifier, safe="")

    poll_interval = 0.5   # adaptive backoff: 0.5s → 1s → 2s (capped)
    for _ in range(90):
        r = requests.get(status_url, headers=headers, timeout=60)
        if r.status_code in (401, 403):
            raise DiskwalaAuthError(f"Diskwala auth token rejected while polling (HTTP {r.status_code})")
        data = r.json()

        if not data.get("ok"):
            raise Exception(data.get("error", f"API Error: {data}"))

        status = data.get("status", "").lower()

        if status == "pending":
            time.sleep(poll_interval)
            poll_interval = min(poll_interval * 1.5, 2.0)
            continue

        # BUG FIX: "error" is a real, documented status this API returns
        # (confirmed from a real response: {"ok": True, "status": "error"})
        # — not a malformed/unrecognized value, so it shouldn't fall into
        # the generic "Unexpected status" catch-all below, which produced
        # a confusing raw-dict error message for what's actually a clean,
        # known failure signal (Diskwala itself couldn't process this
        # link — expired, removed, or conversion failed on their end).
        if status == "error":
            reason = data.get("error") or data.get("message") or data.get("reason")
            raise Exception(
                f"Diskwala couldn't process this link{f': {reason}' if reason else ''} "
                "(link may be invalid, expired, or removed)."
            )

        if status == "done":
            file = data.get("file")
            if not file:
                raise Exception(f"No file returned: {data}")

            # Decrypt if encrypted
            if file.get("_x"):
                logger.info("File is encrypted, decrypting...")
                file = decrypt_file(file)
                logger.info(f"Decrypted file: {json.dumps(file)[:300]}")

            def _pick(d, *keys):
                for k in keys:
                    if k in d and d[k] not in (None, ""):
                        return d[k]
                return None

            def _parse_duration(raw):
                """Parse duration from int (seconds) or 'HH:MM:SS' string."""
                if raw is None:
                    return None
                try:
                    if isinstance(raw, str) and ":" in raw:
                        parts = [int(p) for p in raw.split(":")]
                        return sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
                    return int(float(raw))
                except (ValueError, TypeError):
                    return None

            # Also check top-level data dict for metadata (some API versions
            # put creator/duration at root level, not inside 'file')
            meta_src = {**file, **{k: v for k, v in data.items()
                                   if k not in ("file", "ok", "status")}}

            # Extension — present in decrypted API response as "extension": "mp4"
            raw_name_api = _pick(file, "name", "fileName", "filename", "title") or "video.mp4"
            api_ext = _pick(file, "extension", "ext", "fileExtension", "file_extension")
            if not api_ext and "." in raw_name_api:
                api_ext = raw_name_api.rsplit(".", 1)[-1].lower()
            api_ext = api_ext.lower().strip(".") if api_ext else None

            # Category — infer from extension if API doesn't provide it
            _VIDEO_EXTS = {"mp4", "mkv", "avi", "mov", "webm", "flv", "wmv", "m4v", "ts", "3gp", "m2ts", "vob"}
            _AUDIO_EXTS = {"mp3", "flac", "aac", "ogg", "m4a", "wav", "opus", "wma", "alac", "aiff"}
            _IMAGE_EXTS = {"jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "tif", "svg", "heic", "heif", "avif"}
            _DOC_EXTS   = {"pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx", "txt", "zip", "rar", "7z", "tar", "gz"}
            api_category = _pick(meta_src, "category", "genre", "tag", "type")
            if not api_category and api_ext:
                if api_ext in _VIDEO_EXTS:
                    api_category = "Video"
                elif api_ext in _AUDIO_EXTS:
                    api_category = "Audio"
                elif api_ext in _IMAGE_EXTS:
                    api_category = "Photo"
                elif api_ext in _DOC_EXTS:
                    api_category = "Document"

            return {
                "name":             raw_name_api,
                "extension":        api_ext,
                "size":             _pick(file, "size", "fileSize", "length") or 0,
                "downloadUrl":      _pick(file, "downloadUrl", "download_url", "url", "link"),
                "streamUrl":        _pick(file, "streamUrl", "stream_url", "hls")
                                    or _pick(file, "downloadUrl", "download_url", "url", "link"),
                "thumb":            _pick(meta_src, "thumb", "thumbnail", "thumbnailUrl", "poster", "image"),
                # ── Extra metadata from API response ───────────────────────
                "creator":          _pick(meta_src, "creator", "uploader", "author", "uploaderName",
                                          "uploader_name", "channel", "creatorName", "username"),
                "duration_seconds": _parse_duration(
                                        _pick(meta_src, "duration", "duration_seconds",
                                              "durationSeconds", "video_duration", "length_seconds")
                                    ),
                "views":            _pick(meta_src, "views", "view_count", "viewCount",
                                          "watchCount", "watch_count"),
                "likes":            _pick(meta_src, "likes", "like_count", "likeCount"),
                "description":      _pick(meta_src, "description", "desc", "caption", "details"),
                "upload_date":      _pick(meta_src, "upload_date", "uploadDate", "created_at",
                                          "createdAt", "date", "uploadedAt"),
                "category":         api_category,
            }

        raise Exception(f"Unexpected status: {status} - {data}")

    raise Exception("Timeout waiting for Diskwala API response")

# ─────────────────────────────────────────────────────────────────────────────
#  PLAYLIST SUPPORT
# ─────────────────────────────────────────────────────────────────────────────

API_PLAYLIST_INFO = "https://api2.diskwala.net/api/diskwala/playlist"

PLAYLIST_ID_RE = re.compile(
    r"diskwala\.com/playlist/([A-Za-z0-9]{24})",
    re.IGNORECASE,
)

# Full-URL version of PLAYLIST_ID_RE (scheme + optional subdomain + optional
# trailing path/query) — used by extract_playlist_links() below to pull
# whole matching URLs out of a message's text, not just the ID out of a
# single already-known URL (that's PLAYLIST_ID_RE's own job, via
# extract_playlist_id() right below it).
_PLAYLIST_URL_RE = re.compile(
    r"https?://(?:[\w.-]+\.)?diskwala\.[a-z]{2,}/playlist/[A-Za-z0-9]{24}\S*",
    re.IGNORECASE,
)


def extract_playlist_id(url: str) -> str | None:
    m = PLAYLIST_ID_RE.search(url)
    return m.group(1) if m else None


def extract_playlist_links(text: str) -> list[str]:
    """Bug fix: main.py's link_handler calls this expecting the same
    contract as extract_diskwala_links(text) — scan arbitrary message
    text and return every matching URL, in order, de-duplicated — but it
    never actually existed here (only the single-URL extract_playlist_id()
    and is_playlist_link() did), so every message main.py processed raised
    AttributeError before it could even look for a playlist link, let
    alone anything else in the same message."""
    if not text:
        return []
    seen = set()
    links = []
    for match in _PLAYLIST_URL_RE.findall(text):
        url = match.rstrip(").,!?>'\"")
        if url not in seen:
            seen.add(url)
            links.append(url)
    return links



def is_playlist_link(url: str) -> bool:
    return bool(PLAYLIST_ID_RE.search(url))


def fetch_playlist_info(playlist_url: str, auth: str) -> dict:
    """Fetch playlist metadata + list of file links from Diskwala API.
    Returns:
      {
        "title": str,
        "thumb": str | None,
        "files": [{"name": str, "link": str}, ...]
      }
    """
    playlist_id = extract_playlist_id(playlist_url)
    if not playlist_id:
        raise Exception(f"Could not extract playlist ID from: {playlist_url}")

    headers = {
        "Authorization": f"Bearer {auth}",
        "X-Bot-Id": "diskwala",
        "Content-Type": "application/json",
        "Origin": "https://miniapp.diskwala.net",
        "Referer": "https://miniapp.diskwala.net/",
        "User-Agent": "Mozilla/5.0 (Linux; Android 13) AppleWebKit/537.36",
    }

    # Try POST with link first (same pattern as single file)
    try:
        r = requests.post(
            API_PLAYLIST_INFO,
            headers=headers,
            json={"link": playlist_url},
            timeout=30,
        )
        if r.status_code in (401, 403):
            raise DiskwalaAuthError(f"Auth token rejected (HTTP {r.status_code})")
        data = r.json()
        if data.get("ok"):
            return _parse_playlist_response(data, playlist_url)
    except DiskwalaAuthError:
        raise
    except Exception as e:
        logger.info(f"Playlist POST failed: {e}, trying GET...")

    # Try GET with playlist id
    r = requests.get(
        f"{API_PLAYLIST_INFO}/{playlist_id}",
        headers=headers,
        timeout=30,
    )
    if r.status_code in (401, 403):
        raise DiskwalaAuthError(f"Auth token rejected (HTTP {r.status_code})")
    data = r.json()
    if not data.get("ok"):
        # Last resort: scrape the playlist page HTML
        return _scrape_playlist_html(playlist_url)

    return _parse_playlist_response(data, playlist_url)


def _parse_playlist_response(data: dict, playlist_url: str) -> dict:
    """Parse API response into standard playlist dict."""
    payload = data.get("data") or data.get("playlist") or data
    if isinstance(payload, list):
        # Some APIs return a bare list of files
        files_raw = payload
        title = "Diskwala Playlist"
        thumb = None
    else:
        title = payload.get("title") or payload.get("name") or "Diskwala Playlist"
        thumb = payload.get("thumb") or payload.get("thumbnail")
        files_raw = (
            payload.get("files") or payload.get("items") or
            payload.get("videos") or payload.get("links") or []
        )

    files = []
    for f in files_raw:
        if isinstance(f, str):
            # bare URL
            files.append({"name": f.split("/")[-1] or "video.mp4", "link": f})
        elif isinstance(f, dict):
            link = (
                f.get("link") or f.get("url") or f.get("downloadUrl") or
                f.get("shareLink") or f.get("appLink") or ""
            )
            name = f.get("name") or f.get("title") or f.get("fileName") or link.split("/")[-1] or "video"
            if link:
                files.append({"name": name, "link": link})

    if not files:
        raise Exception("Playlist API returned no files")

    return {"title": title, "thumb": thumb, "files": files}


def _scrape_playlist_html(playlist_url: str) -> dict:
    """HTML fallback: scrape playlist page for app links."""
    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.diskwala.com/",
    }
    r = requests.get(playlist_url, headers=headers, timeout=30, allow_redirects=True)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    title_tag = soup.find("h1") or soup.find("meta", property="og:title")
    title = (
        (title_tag.get("content") if title_tag and title_tag.name == "meta" else
         title_tag.get_text(strip=True) if title_tag else None)
        or "Diskwala Playlist"
    )

    thumb_tag = soup.find("meta", property="og:image")
    thumb = thumb_tag.get("content") if thumb_tag else None

    # Find all /app/ links in the page
    file_links = []
    seen = set()
    for a in soup.find_all("a", href=re.compile(r"/app/[A-Za-z0-9]{24}")):
        href = a.get("href", "")
        if not href.startswith("http"):
            href = urljoin("https://www.diskwala.com", href)
        if href not in seen:
            seen.add(href)
            name = a.get_text(strip=True) or href.split("/")[-1]
            file_links.append({"name": name, "link": href})

    # Also scan script tags for JSON with app links
    if not file_links:
        for script in soup.find_all("script"):
            text = script.string or ""
            for m in re.finditer(r'diskwala\.com/app/([A-Za-z0-9]{24})', text):
                link = f"https://www.diskwala.com/app/{m.group(1)}"
                if link not in seen:
                    seen.add(link)
                    file_links.append({"name": f"video_{m.group(1)[:8]}.mp4", "link": link})

    if not file_links:
        raise Exception("Could not find any video links in playlist page")

    return {"title": title, "thumb": thumb, "files": file_links}


# ─────────────────────────────────────────────────────────────────────────────
#  PREVIEW SCRAPER — title, author, duration, thumb from HTML (no auth needed)
# ─────────────────────────────────────────────────────────────────────────────

# Generic/site-level titles to ignore
_GENERIC_TITLES = {
    "diskwala", "flezen", "free unlimited cloud", "cloud storage",
    "creator platform", "upload files", "share with",
}

def _is_generic_title(t: str) -> bool:
    if not t:
        return True
    tl = t.lower()
    return any(g in tl for g in _GENERIC_TITLES)


def _scrape_preview_from_html(link: str, html_text: str) -> dict:
    """Extract title/author/duration/thumb.
    Priority: __NEXT_DATA__ > JSON-LD > og:meta > page text.
    """
    soup = BeautifulSoup(html_text, "html.parser")
    title = None
    author = None
    duration_seconds = 0
    thumb = None
    size = 0

    # 1. __NEXT_DATA__ (Next.js) — most reliable for Diskwala
    next_script = soup.find("script", id="__NEXT_DATA__")
    if next_script and next_script.string:
        try:
            nd = json.loads(next_script.string)
            page_props = nd.get("props", {}).get("pageProps", {})
            file_data = (
                page_props.get("file")
                or page_props.get("video")
                or page_props.get("data")
                or page_props.get("fileData")
                or {}
            )
            if not file_data:
                for v in page_props.values():
                    if isinstance(v, dict) and (v.get("name") or v.get("title")):
                        file_data = v
                        break

            nd_title = (file_data.get("title") or file_data.get("name") or file_data.get("fileName"))
            if nd_title and not _is_generic_title(nd_title):
                title = str(nd_title).strip()

            nd_author = (
                file_data.get("creator") or file_data.get("author")
                or file_data.get("uploader") or file_data.get("username")
                or file_data.get("creatorName")
                or (file_data.get("user") or {}).get("username")
                or (file_data.get("user") or {}).get("name")
            )
            if nd_author and isinstance(nd_author, str):
                author = nd_author.strip()

            nd_dur = file_data.get("duration") or file_data.get("videoDuration")
            if nd_dur:
                try:
                    duration_seconds = int(float(nd_dur))
                except Exception:
                    pass

            nd_thumb = (
                file_data.get("thumb") or file_data.get("thumbnail")
                or file_data.get("coverImage") or file_data.get("poster")
            )
            if nd_thumb:
                thumb = urljoin(link, str(nd_thumb))

            nd_size = file_data.get("size") or file_data.get("fileSize")
            if nd_size:
                try:
                    size = int(nd_size)
                except Exception:
                    pass
        except Exception as e:
            logger.debug(f"__NEXT_DATA__ parse failed: {e}")

    description = None

    # 2. JSON-LD: fill gaps
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if not title:
                ld_t = data.get("name") or data.get("headline")
                if ld_t and not _is_generic_title(ld_t):
                    title = str(ld_t).strip()
            if not author:
                ao = data.get("author") or data.get("creator") or data.get("uploadedBy")
                if ao:
                    author = (ao.get("name") if isinstance(ao, dict) else str(ao)).strip()
            if not duration_seconds:
                dur_str = data.get("duration")
                if dur_str:
                    m = re.match(r"PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?", str(dur_str))
                    if m:
                        h, mi, s = (int(x or 0) for x in m.groups())
                        duration_seconds = h * 3600 + mi * 60 + s
            if not thumb:
                img = data.get("thumbnailUrl") or data.get("image")
                if img:
                    thumb = urljoin(link, img if isinstance(img, str) else img[0])
            if not description:
                # schema.org's VideoObject (and the generic Article/
                # CreativeWork types some pages use instead) both define
                # "description" as a standard property — this isn't a
                # guess about Diskwala/Flezen's own markup, it's the same
                # standard field every other JSON-LD value above already
                # reads from this same script tag.
                ld_desc = data.get("description")
                if ld_desc and isinstance(ld_desc, str) and ld_desc.strip():
                    description = ld_desc.strip()
        except Exception:
            pass

    # 3. og:image for thumb (og:title skipped — usually generic on Diskwala)
    if not thumb:
        og_img = soup.find("meta", property="og:image")
        if og_img and og_img.get("content"):
            thumb = urljoin(link, og_img["content"].strip())

    # og:description — same universal convention as og:image above, and
    # the only other source this function has for description now that
    # fetch_diskwala_temp_info's API endpoints are unreliable (see
    # fetch_diskwala_preview's merge logic below, which prefers this
    # HTML-scraped value over temp_info's, same "HTML first" priority
    # every other field in this function already uses).
    if not description:
        og_desc = soup.find("meta", property="og:description")
        if og_desc and og_desc.get("content") and og_desc["content"].strip():
            description = og_desc["content"].strip()

    # 4. Flezen: data-bytes + h1 + uploader (plain HTML, not JSON — see
    # _extract_flezen_creator's own docstring for why this needs its own
    # pattern separate from the JSON-key regexes in step 5 below)
    if not size:
        bm = re.search(r"data-bytes=[\x27\"](\d+)[\x27\"]", html_text)
        if bm:
            size = int(bm.group(1))
    if not title:
        h1 = soup.find("h1")
        if h1:
            t = h1.get_text(strip=True)
            if not _is_generic_title(t):
                title = t
    if not author:
        author = _extract_flezen_creator(html_text)

    # Flezen-specific duration (icon+paragraph / data-attribute — see
    # _extract_flezen_duration's own docstring) tried before the generic
    # JSON-key scan in step 5 below, same priority order as the creator
    # fallback right above it.
    if not duration_seconds:
        flezen_dur = _extract_flezen_duration(html_text)
        if flezen_dur:
            duration_seconds = flezen_dur

    # Flezen's upload_date lives in a plain data-datetime attribute (same
    # one resolve_flezen_html already reads) rather than anywhere
    # fetch_diskwala_temp_info's Diskwala-only endpoints can see — without
    # this, upload_date for a Flezen link depended entirely on
    # resolve_flezen_html succeeding (it never runs at all if that link
    # doesn't need a fresh resolve, e.g. only the preview is being shown),
    # so it silently came back empty most of the time.
    upload_date = None
    dt_match = re.search(r'data-datetime=["\']([^"\']+)["\']', html_text)
    if dt_match:
        upload_date = dt_match.group(1).strip()

    # 5. Author / duration from raw JSON blobs in page scripts
    if not author:
        for pat in [
            r'"creatorName"\s*:\s*"([^"]+)"',
            r'"username"\s*:\s*"([^"]+)"',
            r'"creator"\s*:\s*"([^"]+)"',
            r'"author"\s*:\s*"([^"]+)"',
            r'"uploader"\s*:\s*"([^"]+)"',
        ]:
            am = re.search(pat, html_text)
            if am:
                c = am.group(1).strip()
                if c.lower() not in ("admin","system","bot","diskwala","flezen","null","undefined"):
                    author = c
                    break

    if not duration_seconds:
        dm = re.search(r'"duration"\s*:\s*(\d+)', html_text)
        if dm:
            duration_seconds = int(dm.group(1))

    return {"title": title, "author": author, "duration_seconds": duration_seconds, "thumb": thumb,
            "size": size, "upload_date": upload_date, "description": description}


def fetch_diskwala_temp_info(link: str) -> dict:
    """Calls the same api2.diskwala.net/api/v1/file/temp_info
    endpoint already used elsewhere in this file (as a last-resort
    download-URL fallback in resolve_diskwala_html) — same request shape,
    just reading more of the payload this time: creator, duration, view/
    like counts, description, upload date, category, size, and thumbnail,
    not just the media URL.

    This is an undocumented internal endpoint (found via reverse
    engineering, not an official API) with no confirmed field-naming —
    each attribute below tries several plausible key spellings the way
    the existing downloadUrl/download_url/url/... fallback chain already
    does, since that's the only naming convention evidence available for
    this endpoint from within this codebase. Fields the response doesn't
    have (or that don't match any of the tried spellings) just come back
    None — callers already treat any None field as "unknown", same as
    the HTML-scrape path.

    Returns a dict with keys: creator, duration_seconds, views, likes,
    description, upload_date, category, size, thumb. Returns all-None on
    any failure (link doesn't look like a diskwala.com/app/<id> link,
    network error, non-200, unexpected JSON shape, etc.) rather than
    raising — this is meant to be a best-effort enrichment layered on
    top of fetch_diskwala_preview's existing HTML scrape, not something
    that should ever block showing what the HTML scrape already found."""
    empty = {"creator": None, "duration_seconds": None, "views": None, "likes": None,
             "description": None, "upload_date": None, "category": None,
             "size": None, "thumb": None}

    file_id_match = re.search(r"diskwala\.com/app/([A-Za-z0-9]+)", link)
    if file_id_match:
        file_id = file_id_match.group(1)
    else:
        # Flezen shares the same api2.diskwala.net backend as Diskwala
        # (see _get_endpoints() above) — worth trying its share id here
        # too rather than assuming this endpoint is Diskwala-only just
        # because that's the only pattern this file's other temp_info
        # call site (in resolve_diskwala_html) happened to check for.
        # VidBunker also goes through api2.diskwala.net (see
        # _get_endpoints() above), so its share id is worth trying too —
        # without this, temp_info always returned `empty` for vidbunker
        # links and views/likes/creator/etc. never got filled in.
        file_id = _extract_flezen_id(link) or _extract_vidbunker_id(link)
    if not file_id:
        return empty

    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "application/json",
        "Referer": "https://www.diskwala.com/",
        "Origin": "https://www.diskwala.com",
    }

    # Try multiple endpoints — api2.diskwala.net has several info paths,
    # and which one works depends on the file type and API version.
    # temp_info returns 404 for some files; info/meta/file_info may work instead.
    _INFO_ENDPOINTS = [
        ("POST", "https://api2.diskwala.net/api/v1/file/temp_info", {"id": file_id}),
        ("POST", "https://api2.diskwala.net/api/v1/file/info",      {"id": file_id}),
        ("POST", "https://api2.diskwala.net/api/v1/file/meta",      {"id": file_id}),
        ("GET",  f"https://api2.diskwala.net/api/v1/file/{file_id}", None),
        ("POST", "https://api2.diskwala.net/api/diskwala/file_info", {"id": file_id}),
    ]

    data = None
    for method, endpoint, body in _INFO_ENDPOINTS:
        try:
            if method == "POST":
                resp = requests.post(endpoint, json=body, headers=headers, timeout=15)
            else:
                resp = requests.get(endpoint, headers=headers, timeout=15)

            if resp.status_code == 200:
                data = resp.json()
                break
            else:
                logger.info(f"temp_info returned status {resp.status_code} for {endpoint[:60]}")
        except Exception as e:
            logger.info(f"temp_info call failed for {endpoint[:40]}: {e}")

    if data is None:
        return empty

    payload = data.get("data") if isinstance(data.get("data"), dict) else data

    def _first(*keys):
        for key in keys:
            val = payload.get(key)
            if val not in (None, ""):
                return val
        return None

    duration_raw = _first("duration", "duration_seconds", "durationSeconds", "length", "video_duration")
    duration_seconds = None
    if duration_raw is not None:
        try:
            # Some of these endpoints hand back "HH:MM:SS" instead of a
            # raw number — this covers both without needing to know in
            # advance which shape this particular response used.
            if isinstance(duration_raw, str) and ":" in duration_raw:
                parts = [int(p) for p in duration_raw.split(":")]
                duration_seconds = sum(p * 60 ** i for i, p in enumerate(reversed(parts)))
            else:
                duration_seconds = int(float(duration_raw))
        except (ValueError, TypeError):
            duration_seconds = None

    return {
        "creator": _first("creator", "uploader", "author", "uploaderName", "uploader_name", "channel"),
        "duration_seconds": duration_seconds,
        "views": _first("views", "view_count", "viewCount", "watchCount", "watch_count"),
        "likes": _first("likes", "like_count", "likeCount"),
        "description": _first("description", "desc", "caption"),
        "upload_date": _first("upload_date", "uploadDate", "created_at", "createdAt", "date"),
        "category": _first("category", "genre", "tag", "type"),
        "size": _first("size", "fileSize", "file_size"),
        "thumb": _first("thumbnail", "thumb", "thumbnailUrl", "thumbnail_url", "poster", "image"),
    }


def fetch_diskwala_preview(link: str) -> dict:
    """Scrape Diskwala OR Flezen share page to get title, author, duration,
    and thumbnail WITHOUT needing a bearer token.  Returns a dict with keys:
        title, author, duration_seconds, thumb, size,
        views, likes, description, upload_date, category
    Any field that can't be found is None / 0.

    The last five keys come from fetch_diskwala_temp_info() (Option B —
    the api2.diskwala.net file-info endpoint) layered on top of this
    function's own HTML scrape below — that scrape has no way to surface
    view/like counts, a description, upload date, or category at all, so
    those five are always temp_info's alone; author/duration_seconds/
    thumb/size only get overwritten by temp_info's version if the HTML
    scrape came back empty for that particular field.
    """
    headers = {
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }

    # Route Flezen links to their own page URL
    if "flezen" in link.lower():
        share_id = _extract_flezen_id(link)
        if share_id:
            link = f"https://flezen.com/s/{share_id}"
        headers["Referer"] = "https://flezen.com/"
    elif "vidbunker" in link.lower():
        headers["Referer"] = "https://vidbunker.in/"
    else:
        headers["Referer"] = "https://www.diskwala.com/"

    try:
        r = requests.get(link, headers=headers, timeout=20, allow_redirects=True)
        r.raise_for_status()
        html_text = r.text
        result = _scrape_preview_from_html(link, html_text)
    except Exception as e:
        logger.warning(f"fetch_diskwala_preview GET failed: {e}")
        result = {"title": None, "author": None, "duration_seconds": 0, "thumb": None, "size": 0,
                  "upload_date": None, "description": None}

    temp_info = fetch_diskwala_temp_info(link)
    result["views"] = temp_info["views"]
    result["likes"] = temp_info["likes"]
    # FIX: this used to unconditionally overwrite with temp_info's
    # description, same bug the upload_date fix below already covers —
    # temp_info's api2.diskwala.net endpoints have been 404ing (confirmed
    # in production logs), which made description ALWAYS come back None
    # with no way to recover it even though the page's own JSON-LD/
    # og:description usually has it (see _scrape_preview_from_html's new
    # description extraction above). views/likes/category genuinely have
    # no HTML-scrape source in this function yet, so those three still
    # depend entirely on temp_info and will stay None while it's down —
    # that's a real, known gap, not something this fix silently papers
    # over.
    result["description"] = result.get("description") or temp_info["description"]
    # Bug fix: this used to unconditionally overwrite with temp_info's
    # upload_date — for Flezen links that's always None (temp_info's
    # api2.diskwala.net endpoints don't recognize Flezen file IDs at all,
    # it's a Diskwala-only internal API), which threw away the real value
    # _scrape_preview_from_html above already found from the page's own
    # data-datetime attribute. Same "HTML scrape first, temp_info fills
    # the gap" priority the other fields below already use.
    result["upload_date"] = result.get("upload_date") or temp_info["upload_date"]
    result["category"] = temp_info["category"]
    if not result.get("author"):
        result["author"] = temp_info["creator"]
    if not result.get("duration_seconds"):
        result["duration_seconds"] = temp_info["duration_seconds"] or 0
    if not result.get("thumb"):
        result["thumb"] = temp_info["thumb"]
    if not result.get("size"):
        result["size"] = temp_info["size"] or 0

    logger.info(f"Preview scraped ({link[:60]}): title={result['title']!r} author={result['author']!r} dur={result['duration_seconds']}s")
    return result


# ─────────────────────────────────────────────────────────────────────────────
#  AUTO FLEZEN COOKIE GENERATOR
#  (ported from TeraBox-Video-Downloader/scripts/auto_flezen_cookie.py)
#
#  Creates a disposable mail.tm inbox → registers on Flezen → verifies email
#  → completes onboarding → returns a ready-to-use cookie string.
#
#  Used by the /refresh_flezen_cookie admin command in main.py so that
#  FLEZEN_COOKIE never has to be refreshed manually.
# ─────────────────────────────────────────────────────────────────────────────

def generate_flezen_cookie() -> tuple[str, str] | None:
    """
    Fully automated Flezen account creation + cookie extraction.

    Returns (cookie_string, email) on success, None on failure.

    Flow:
      1. Create disposable mailbox via mail.tm
      2. Register on flezen.com with that email
      3. Poll for verification email, extract token
      4. Verify account, complete onboarding
      5. Extract + return session cookie string
    """
    import random, string

    session = requests.Session()
    session.headers.update({
        "User-Agent": HTML_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    })

    # ── Step 1: Disposable mailbox ──────────────────────────────────────────
    try:
        dom_res = requests.get("https://api.mail.tm/domains", timeout=10)
        domains = [d["domain"] for d in dom_res.json().get("hydra:member", []) if d.get("isActive")]
        if not domains:
            logger.error("[flezen-cookie] No active mail.tm domains available")
            return None
        domain = domains[0]
    except Exception as e:
        logger.error(f"[flezen-cookie] mail.tm domain fetch failed: {e}")
        return None

    rand_id  = "".join(random.choices(string.ascii_lowercase + string.digits, k=8))
    email    = f"fbot_{rand_id}@{domain}"
    password = "P@ss" + "".join(random.choices(string.digits, k=8))

    logger.info(f"[flezen-cookie] Creating mailbox: {email}")
    try:
        requests.post("https://api.mail.tm/accounts",
                      json={"address": email, "password": password}, timeout=10)
        tok_res    = requests.post("https://api.mail.tm/token",
                                   json={"address": email, "password": password}, timeout=10)
        mail_token = tok_res.json().get("token")
        if not mail_token:
            logger.error("[flezen-cookie] Could not get mail.tm token")
            return None
        mail_headers = {"Authorization": f"Bearer {mail_token}"}
    except Exception as e:
        logger.error(f"[flezen-cookie] Mailbox creation failed: {e}")
        return None

    # ── Step 2: Register on Flezen ──────────────────────────────────────────
    session.headers.update({"Referer": "https://flezen.com/auth/register",
                             "Origin": "https://flezen.com"})
    logger.info("[flezen-cookie] Registering on flezen.com...")
    try:
        reg = session.post("https://flezen.com/auth/register",
                           data={"email": email, "password": password,
                                 "confirm_password": password}, timeout=15)
        if reg.status_code not in (200, 201, 302):
            logger.error(f"[flezen-cookie] Registration failed: HTTP {reg.status_code}")
            return None
    except Exception as e:
        logger.error(f"[flezen-cookie] Registration request failed: {e}")
        return None

    # ── Step 3: Poll for verification email ────────────────────────────────
    logger.info("[flezen-cookie] Waiting for verification email...")
    verify_url = None
    for attempt in range(20):
        time.sleep(3)
        try:
            msgs = requests.get("https://api.mail.tm/messages",
                                headers=mail_headers, timeout=10).json()
            for msg in msgs.get("hydra:member", []):
                msg_body = requests.get(f"https://api.mail.tm/messages/{msg['id']}",
                                        headers=mail_headers, timeout=10).json()
                body = msg_body.get("text", "") or msg_body.get("html", "")
                m = re.search(r"https?://flezen\.com/auth/verify\?token=([a-zA-Z0-9_-]+)", body)
                if m:
                    verify_url = m.group(0)
                    break
        except Exception:
            pass
        if verify_url:
            break

    if not verify_url:
        logger.error("[flezen-cookie] Timed out waiting for verification email")
        return None

    # ── Step 4: Verify + onboard ────────────────────────────────────────────
    logger.info(f"[flezen-cookie] Verifying: {verify_url}")
    try:
        session.get(verify_url, allow_redirects=True, timeout=15)
    except Exception as e:
        logger.warning(f"[flezen-cookie] Verify request failed: {e}")

    logger.info("[flezen-cookie] Completing onboarding...")
    session.headers.update({"Referer": "https://flezen.com/user/onboard",
                             "Origin": "https://flezen.com"})
    try:
        session.post("https://flezen.com/user/onboard", data={
            "first_name": "Bot", "last_name": "User",
            "display_name": f"BotUser_{rand_id}",
            "ref_code": "", "traffic_sources": "https://t.me/",
        }, allow_redirects=False, timeout=15)
    except Exception as e:
        logger.warning(f"[flezen-cookie] Onboarding failed (non-fatal): {e}")

    # ── Step 5: Extract cookies ─────────────────────────────────────────────
    cookies = session.cookies.get_dict()
    if not cookies:
        logger.error("[flezen-cookie] No cookies after registration — likely blocked or captcha")
        return None

    cookie_str = "; ".join(f"{k}={v}" for k, v in cookies.items())
    logger.info(f"[flezen-cookie] ✅ Cookie generated for {email}")
    return cookie_str, email


# ═════════════════════════════════════════════════════════════════════════
#  fbot-main INTEGRATION ADAPTER

# ── Per-URL resolve cache ────────────────────────────────────────────────────
# Both get_page_meta() and get_available_qualities() call _resolve_no_auth().
# Without a cache they make TWO separate API calls for the same URL within
# a few seconds of each other (quality-menu render, then download confirm).
# Cache the result for 5 minutes — long enough to cover both calls for a
# single user interaction, short enough not to serve stale download URLs.
_resolve_cache: dict = {}
_RESOLVE_CACHE_TTL = 300   # seconds


def _resolve_no_auth_cached(link: str) -> dict:
    """Like _resolve_no_auth() but caches the result for _RESOLVE_CACHE_TTL
    seconds to avoid double-resolving for get_page_meta + get_available_qualities."""
    now = time.time()
    cached = _resolve_cache.get(link)
    if cached and (now - cached[1]) < _RESOLVE_CACHE_TTL:
        logger.debug(f"[diskwala] resolve cache hit: {link[:60]}")
        return cached[0]
    result = _resolve_no_auth(link)
    _resolve_cache[link] = (result, now)
    # Keep cache small — prune oldest 50 entries when over 100
    if len(_resolve_cache) > 100:
        for k in sorted(_resolve_cache, key=lambda k: _resolve_cache[k][1])[:50]:
            _resolve_cache.pop(k, None)
    return result

#
#  Everything above this point is ultra-main's diskwala.py, ported over
#  as-is. Everything below is new — a thin adapter exposing the exact
#  is_X_link / extract_X_links / get_page_meta / get_available_qualities /
#  download_video / get_stream_url interface every other backend module
#  in fbot-main's main.py (_downloader_for, show_quality_menu,
#  download_video) already duck-types against, so this module slots in
#  the same way terabox_downloader.py or faphouse_downloader.py do,
#  without touching that shared dispatch code at all.
#
#  TOKEN-API TIER — now wired up. ultra-main's get_auth_token() tier
#  needed a real Telegram *user* session (Telethon, phone-number login —
#  separate from this bot's own Pyrogram *bot* token) logged into
#  Telegram's "sky577bot" Mini App to pull a bearer token out of the
#  WebApp URL. That infrastructure is now ported too (see the Telethon
#  auth-token block near the top of this file: get_auth_token_sync(),
#  resolve_diskwala_with_auth_retry()) — it runs on its own dedicated
#  background event-loop thread so these sync adapter functions can call
#  it without needing an `async def`.
#
#  It only actually activates if the SESSION env var is set (see
#  config.py's comment on it) — a manual one-time Telethon login is still
#  required, this port just can't skip that step for you. Without
#  SESSION, or if the Telethon call fails for any reason (bad/expired
#  session, network issue, etc.), _resolve_no_auth() below catches it and
#  falls straight through to the same no-token HTML-scrape path
#  (resolve_flezen_html / resolve_diskwala_html) this adapter always used
#  — so nothing breaks for a deploy that hasn't configured SESSION.
# ═════════════════════════════════════════════════════════════════════════

_DISKWALA_DOMAINS_RE = re.compile(
    r"https?://(?:[\w.-]+\.)?(?:thediskwala\.[a-z]{2,}|diskwala\.[a-z]{2,}"
    r"|filecrush\.[a-z]{2,}|filesadda\.[a-z]{2,}|flezen\.[a-z]{2,}|vidbunker\.[a-z]{2,})",
    re.IGNORECASE,
)


def is_diskwala_link(url: str) -> bool:
    """True for a Diskwala/Flezen/VidBunker link — used by main.py's
    _downloader_for() dispatcher, same role as terabox.is_terabox_link()
    etc. extract_diskwala_links() already does the heavier regex work
    for pulling links out of free text; this is the simpler single-URL
    check the dispatcher needs."""
    if not url:
        return False
    return bool(_DISKWALA_DOMAINS_RE.search(url)) or bool(_extract_vidbunker_id(url))


def _looks_like_not_found(msg: str) -> bool:
    """True if an error message from either the token-API tier or an
    HTML scrape reads like the file genuinely doesn't exist server-side
    (deleted/expired/invalid share link) rather than a transient network
    or auth problem. Used only to phrase the combined error below more
    clearly for that — very common — case; doesn't change control flow."""
    m = (msg or "").lower()
    return "not found" in m or " 404" in m or m.strip() == "404"


def _resolve_no_auth(link: str) -> dict:
    """Resolve a Diskwala/Flezen/VidBunker link's playable video info.

    BUG FIX: this used to run its own independent "try token-API, then
    try resolve_diskwala_html/resolve_flezen_html again" chain — but
    resolve_diskwala_with_auth_retry() -> fetch_diskwala_video() (the
    ported ultra-main function above) ALREADY implements that exact
    fallback chain internally (Flezen-direct-scrape-first for Flezen
    links, token-API-first with its own HTML-scrape fallback for
    everything else — see fetch_diskwala_video()'s own docstring).
    Retrying here on top of that meant every failed resolve made the
    same HTML scrape (or Flezen fetch) TWICE — once inside
    fetch_diskwala_video(), once again here — doubling the latency (and
    the load on vidbunker.in/flezen.com) of every single failure for no
    benefit, since the second attempt could never come back with a
    different answer.

    Now this just defers straight to resolve_diskwala_with_auth_retry(),
    and only runs its own no-auth chain when the token tier was never
    actually reachable at all — SESSION/API_ID/API_HASH unset, or the
    Telethon call itself broken (get_auth_token_sync() raises a
    RuntimeError before fetch_diskwala_video() is ever called, so its
    internal HTML fallback never ran either). When a token WAS obtained
    and fetch_diskwala_video() still failed both of its own tiers, that
    failure (usually a genuine "not found" from both a live scrape AND
    Diskwala's own backend — strong evidence the link is simply dead) is
    the final answer; retrying the same HTTP calls again wouldn't change
    it, so it's re-raised as-is instead, reworded to be clearer when it
    looks like a dead/expired link rather than a bug.

    REPLACED: VidBunker links now go straight to _resolve_vidbunker_new()
    (the dedicated worker API) and never touch resolve_diskwala_with_auth_retry
    or resolve_diskwala_html at all — see VIDBUNKER_WORKER_API's comment
    above for why the old api2.diskwala.net/api/vidbunker/* pair this used
    to fall through to is gone from this path entirely, not just
    deprioritized. This also means a VidBunker link no longer needs
    SESSION/Telethon configured to resolve at all."""
    if "vidbunker" in link.lower() or _extract_vidbunker_id(link):
        return _resolve_vidbunker_new(link)

    if "flezen" not in link.lower():
        # Cheap, fast first attempt for diskwala.com/net/app links — see
        # _resolve_diskwala_vercel's docstring above. Not affiliated with
        # Diskwala, so any failure (down, rate-limited, doesn't recognize
        # this particular URL shape) just falls straight through to the
        # existing token-API/HTML-scrape chain below unchanged; nothing
        # here ever raises past this point.
        try:
            return _resolve_diskwala_vercel(link)
        except Exception as e:
            logger.info(f"Diskwala Vercel resolver unavailable ({e}), using the regular chain...")

    try:
        return resolve_diskwala_with_auth_retry(link)
    except RuntimeError as e:
        if "SESSION" not in str(e):
            raise
        logger.info(f"Token-API tier unavailable ({e}), using no-auth scrape chain instead...")
    except ImportError as e:
        # BUG FIX: telethon not being installed (ModuleNotFoundError, a
        # subclass of ImportError) used to fall into the `except Exception`
        # branch below and get RE-RAISED as a real failure — SESSION was
        # configured, so it never hit the RuntimeError("SESSION...") path
        # above either. The result: "Stream link failed / No module named
        # 'telethon'" shown straight to the end user instead of quietly
        # falling back to the no-auth scrape chain every other unavailable-
        # token-tier case already falls back to. Same fix as the SESSION-
        # unset case above — this tier just isn't usable right now, for a
        # different reason, not a real resolve error worth surfacing.
        logger.info(f"Token-API tier unavailable (telethon not installed: {e}), using no-auth scrape chain instead...")
    except Exception as e:
        # fetch_diskwala_video() already tried its own HTML fallback and
        # still failed — see this function's docstring for why retrying
        # that here would just repeat the same failing HTTP calls.
        if _looks_like_not_found(str(e)):
            raise Exception(
                "This file couldn't be found — both the token-API and a "
                "direct page scrape report it missing, so the link is "
                "most likely expired, deleted, or invalid. Double-check "
                "it still opens in a browser, or get a fresh share link."
            ) from e
        raise

    if "flezen" in link.lower():
        try:
            return resolve_flezen_html(link)
        except Exception as e:
            logger.warning(f"Flezen direct scrape failed ({e}), trying generic HTML fallback...")
            try:
                return resolve_diskwala_html(link)
            except Exception as e2:
                if _looks_like_not_found(str(e)) and _looks_like_not_found(str(e2)):
                    raise Exception(
                        "This file couldn't be found — the link is most "
                        "likely expired, deleted, or invalid. Double-check "
                        "it still opens in a browser, or get a fresh share "
                        "link."
                    ) from e2
                raise Exception(f"{e2} [flezen-direct: {e}]") from e2
    return resolve_diskwala_html(link)


def get_page_meta(url: str) -> dict:
    """Matches terabox_downloader.get_page_meta()'s contract: {poster_url,
    duration, title, file_size, extension, category}.

    FIX: Previously used fetch_diskwala_preview() (HTML scrape only) which
    returned thumb=None for Flezen and often missed title/author for Diskwala.
    Now calls _resolve_no_auth_cached() — the same full resolve that
    get_available_qualities() uses — so the real API filename, thumbnail, and
    creator are all available. The cached result means no extra API call is
    made when get_available_qualities() runs immediately after.
    """
    VIDEO_EXTS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm",
                  ".m4v", ".mpg", ".mpeg", ".3gp", ".ts", ".m2ts"}

    try:
        # Primary: use the full resolve result (has real name + thumbnail)
        info = _resolve_no_auth_cached(url)

        raw_name = info.get("name") or info.get("title") or ""
        # Strip file extension for the display title (🎬 Title line)
        if raw_name and "." in raw_name:
            title_display = os.path.splitext(raw_name)[0]
            ext = os.path.splitext(raw_name)[1].lower()
        else:
            title_display = raw_name or "Video"
            slug = url.rstrip("/").split("/")[-1].split("?")[0]
            ext = os.path.splitext(slug)[1].lower() or ".mp4"

        # Thumbnail: resolve result > fetch_diskwala_preview fallback
        thumb = info.get("thumb")
        if not thumb:
            try:
                preview = fetch_diskwala_preview(url)
                thumb = preview.get("thumb")
            except Exception:
                pass

        return {
            "title":      title_display or "Video",
            "author":     info.get("creator") or info.get("author") or "Unknown",
            "duration":   info.get("duration_seconds") or 0,
            "poster_url": thumb,
            "file_size":  info.get("size") or 0,
            "extension":  ext,
            "category":   "Video" if ext in VIDEO_EXTS else info.get("category") or "File",
        }

    except Exception as e:
        # Fallback to HTML scrape if resolve fails completely
        logger.warning(f"[diskwala] get_page_meta resolve failed ({e}), using HTML scrape")
        info = fetch_diskwala_preview(url)
        title = info.get("title") or "Video"
        ext = os.path.splitext(title)[1].lower()
        if not ext:
            slug = url.rstrip("/").split("/")[-1].split("?")[0]
            ext = os.path.splitext(slug)[1].lower() or ".mp4"
        return {
            "title":      title,
            "author":     info.get("author") or "Unknown",
            "duration":   info.get("duration_seconds") or 0,
            "poster_url": info.get("thumb"),
            "file_size":  info.get("size") or 0,
            "extension":  ext,
            "category":   "Video" if ext in VIDEO_EXTS else info.get("category") or "File",
        }


def get_available_qualities(url: str) -> list:
    """Matches terabox_downloader.get_available_qualities()'s contract —
    same "just one real link, no actual quality ladder" shape as Terabox
    (see that function's own docstring): Diskwala/Flezen/VidBunker hand
    back one downloadUrl each, never a resolution choice. A length-1 list
    is what makes main.py's show_quality_menu() skip straight to
    download_video() instead of showing a pointless one-button menu."""
    # Playlist links (e.g. diskwala.com/playlist/...) match the same
    # domain check as a single file (is_diskwala_link doesn't distinguish
    # them), but resolve_diskwala_html/resolve_flezen_html — and this
    # adapter's single-downloadUrl return shape below — are built for one
    # file at a time. fetch_playlist_info() (ported further up, in the
    # ultra-main section) CAN now resolve a playlist's contents using the
    # same Telethon auth token this adapter just gained, but wiring that
    # into a real multi-file download flow is a separate change to
    # main.py's dispatch/UI (it expects one file per link here), not
    # something this function's contract can express — so this stays a
    # clear, honest "not supported yet" rather than a silent wrong-file
    # download or a half-working single-file-of-a-playlist result.
    if is_playlist_link(url):
        raise RuntimeError(
            "This looks like a Diskwala playlist link — playlist support isn't "
            "wired up in this bot yet. Send a single file's share link instead."
        )
    # FIX: use cached resolve — if get_page_meta() was called first (it always
    # is, for the quality menu), the result is already cached and this is free.
    result = _resolve_no_auth_cached(url)
    download_link = result.get("downloadUrl")
    if not download_link:
        raise RuntimeError("Diskwala/Flezen/VidBunker: couldn't resolve a download link for this URL.")
    return [{"label": "Best", "url": download_link}]
def get_stream_url(url: str):
    """Matches terabox_downloader.get_stream_url()'s contract — used for
    main.py's Stream button."""
    try:
        result = _resolve_no_auth(url)
        return result.get("streamUrl") or result.get("downloadUrl")
    except Exception as e:
        logger.warning(f"diskwala get_stream_url failed: {e}")
        return None


def download_video(url: str, out_path: str, on_progress=None, stream_url: str = None) -> str:
    """Matches terabox_downloader.download_video()'s contract: downloads
    to out_path, reports progress the same {"pct", "downloaded_bytes"}
    shape every other backend's on_progress callback already expects, and
    returns the final path. stream_url (the already-resolved link from
    get_available_qualities(), passed back in once the user picks/
    confirms) is used directly when given, so this doesn't re-resolve
    from scratch on every download — same reasoning as Terabox's
    download_video accepting a pre-resolved stream_url."""
    download_link = stream_url
    if not download_link:
        result = _resolve_no_auth(url)
        download_link = result.get("downloadUrl")
    if not download_link:
        raise RuntimeError("Diskwala/Flezen/VidBunker: no download link available.")

    headers = {"User-Agent": HTML_USER_AGENT}
    if "vidbunker" in url.lower():
        headers["Referer"] = "https://vidbunker.in/"
    elif "flezen" in url.lower():
        headers["Referer"] = "https://flezen.com/"
    else:
        headers["Referer"] = "https://www.diskwala.com/"

    resp = requests.get(download_link, headers=headers, stream=True, timeout=30)
    resp.raise_for_status()
    total = int(resp.headers.get("Content-Length", 0))
    downloaded = 0
    last_report = 0.0
    try:
        with open(out_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                now = time.time()
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

    return out_path
