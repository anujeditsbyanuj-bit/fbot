"""
Faphouse video downloader engine — ported from bot.py.

Resolves a faphouse.com / faphouse2.com video page link to its m3u8
stream (AkClient) and downloads it to a local file via ffmpeg (segment
copy, no re-encode). Kept free of any Telegram/Mongo/Pyrogram imports
so it can be dropped into any bot as a plain module.

Login: faphouse.com and faphouse2.com are separate domains, so cookies
from logging into one never carry over to the other — that's a browser/
HTTP cookie-scoping fact, not something code can route around. To make
"one login" behave as one login from the user's point of view, AkClient
keeps a separate authenticated requests.Session per domain, but logs
both in with the *same* EMAIL/PASSWORD automatically and on demand —
whichever domain a link belongs to gets its own session logged in the
first time a link from that domain is seen.
"""

import gzip
import json
import logging
import os
import re
import shutil
import subprocess
import threading
import time
import zlib
from collections import deque
from urllib.parse import urlparse, urljoin

import requests

# ── Persistent M3U8 disk cache ──────────────────────────────────────────────
# Survives bot restarts. Same video = instant resolve, no login/page fetch.
# TTL: 6 hours (FapHouse signed URLs typically expire in ~12h).
_CACHE_FILE = os.path.join(os.environ.get("DOWNLOAD_DIR", "downloads"), "m3u8_cache.json")
_CACHE_TTL  = int(os.environ.get("M3U8_CACHE_TTL", str(6 * 3600)))  # 6 hours

def _load_disk_cache() -> dict:
    try:
        with open(_CACHE_FILE, "r") as f:
            data = json.load(f)
        # Prune expired entries on load
        now = time.time()
        pruned = {k: v for k, v in data.items() if now - v.get("ts", 0) < _CACHE_TTL}
        return pruned
    except Exception:
        return {}

def _save_disk_cache(cache: dict):
    try:
        os.makedirs(os.path.dirname(_CACHE_FILE), exist_ok=True)
        with open(_CACHE_FILE, "w") as f:
            json.dump(cache, f)
    except Exception as e:
        logger.warning(f"m3u8 disk cache save failed: {e}")

# Load cache at import time — populated as videos are resolved
_disk_cache: dict = _load_disk_cache()
_disk_cache_lock = threading.Lock()

logger = logging.getLogger(__name__)

BASE_URLS = {
    "faphouse.com": "https://faphouse.com",
    "www.faphouse.com": "https://faphouse.com",
    "faphouse2.com": "https://faphouse2.com",
    "www.faphouse2.com": "https://faphouse2.com",
}
DEFAULT_BASE_URL = os.environ.get("BASE_URL", "https://faphouse2.com")

EMAIL = os.environ.get('EMAIL', 'rockstarga69@gmail.com')
PASSWORD = os.environ.get('PASSWORD', 'SajagOG@1234')
# Force a fresh login after this long, even if the client still thinks it's
# logged in — the site can silently expire the session server-side with no
# client-visible signal, so time-based re-login is the only reliable guard.
SESSION_MAX_AGE = int(os.environ.get('SESSION_MAX_AGE', str(30 * 60)))  # 30 minutes

_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'

# Where a failing page's raw HTML gets saved when M3U8 extraction comes up
# empty on BOTH attempts — same env var as config.py's DOWNLOAD_DIR (kept
# as a plain os.environ read rather than importing config.py, per this
# module's own "no framework imports" contract). Lets an admin pull the
# actual current markup via /debughtml or /getdebughtml in main.py instead
# of guessing blind at what changed on the site.
_DEBUG_DIR = os.environ.get("DOWNLOAD_DIR", "downloads")
_DEBUG_HTML_PATH = os.path.join(_DEBUG_DIR, "debug_last_m3u8_fail.html")

_CHALLENGE_MARKERS = (
    # These only appear on actual Cloudflare challenge/interstitial pages,
    # NOT on normal pages that merely use Cloudflare CDN for their assets.
    # Crucially, "cloudflare" alone is removed — faphouse.com's own HTML
    # references Cloudflare CDN scripts on every normal video page, so
    # checking for "cloudflare" alone produced massive false positives,
    # treating real video pages as challenge pages and skipping the session.
    "cf-browser-verification",
    "cf_chl_",
    "/cdn-cgi/challenge-platform",
    "checking your browser",
    "enable javascript and cookies to continue",
    "ray id:",              # Cloudflare error page footer
    "captcha-bypass",
)

# A title-based challenge check — "just a moment" and "attention required"
# can appear in normal page copy, but as the *page title* they're
# unambiguous Cloudflare markers.  We check these separately against the
# <title> tag rather than the full body to avoid false positives.
_CHALLENGE_TITLE_MARKERS = (
    "just a moment",
    "attention required",
    "access denied",
    "checking your browser",
    "please wait",
)
_TITLE_TAG_RE = re.compile(r"<title[^>]*>([^<]{0,200})</title>", re.I)

def _looks_like_challenge_page(html: str) -> bool:
    """Cheap heuristic: does this page look like an anti-bot challenge/
    interstitial rather than the real video page?

    Key design change: "cloudflare" alone is NOT checked here — faphouse.com
    uses Cloudflare CDN for its own assets, so that string appears in every
    normal video page's HTML. Only markers that exclusively appear on actual
    Cloudflare challenge pages are used."""
    if not html:
        return False
    lowered = html[:20000].lower()
    # Body-text markers that only appear on challenge/interstitial pages
    if any(marker in lowered for marker in _CHALLENGE_MARKERS):
        return True
    # Title-based check — these phrases as the page <title> are unambiguous
    title_match = _TITLE_TAG_RE.search(html[:5000])
    if title_match:
        title_text = title_match.group(1).strip().lower()
        if any(marker in title_text for marker in _CHALLENGE_TITLE_MARKERS):
            return True
    return False


def _parse_view_state(html: str) -> dict | None:
    """Extracts and parses the <script id="view-state-data"> JSON block
    faphouse's current (xHamster/flixcdn-backed) pages embed — this is
    the site's own real tracking/analytics data for the page, present on
    every video page regardless of whether a stream is findable, so it's
    a far more reliable signal to check than scraped UI copy (which
    changes every redesign, unlike a stable internal data contract)."""
    if not html:
        return None
    m = re.search(r'<script id="view-state-data"[^>]*>(.*?)</script>', html, re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(1))
    except Exception:
        return None


def _view_state_logged_in(view_state: dict) -> bool | None:
    """True/False on whether the page's own tracking data thinks a real
    user is logged in (view-state-data's user.currentUserId), or None if
    that field isn't present. Used to catch a session that LOOKS logged
    in on our side (ensure_session's cache says logged_in=True, the GET
    returns 200) but the site itself is actually treating the request as
    a guest — e.g. an auth token that's expired/been invalidated
    server-side without our cache knowing. That mismatch is exactly what
    turns a normal premium video into a false "fanclub locked"/"player
    markup changed" diagnosis: the video isn't actually inaccessible,
    OUR session just silently stopped being authenticated."""
    try:
        return view_state.get("user", {}).get("currentUserId") is not None
    except Exception:
        return None


def _save_debug_html(html: str):
    if not html:
        return
    try:
        os.makedirs(_DEBUG_DIR, exist_ok=True)
        with open(_DEBUG_HTML_PATH, "w", encoding="utf-8", errors="replace") as f:
            f.write(html)
    except Exception as e:
        logger.warning(f"Couldn't save debug HTML: {e}")


def fetch_debug_html(video_url: str) -> str | None:
    """Just fetches the raw page HTML with the same session/header setup
    _resolve_m3u8_url uses — no extraction. Used by main.py's /debughtml
    admin command so the actual current markup can be pulled straight from
    the live site (which, unlike a dev sandbox, this bot can reach) for
    manual inspection when extraction is failing and the site may have
    changed something."""
    base_url = get_base_url(video_url) or DEFAULT_BASE_URL
    headers = {
        'User-Agent': _UA,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Referer': base_url,
    }
    try:
        session = client.ensure_session(base_url) if (EMAIL and PASSWORD) else requests
        response = session.get(video_url, timeout=15, headers=headers)
        return client._decode_response(response)
    except Exception as e:
        logger.warning(f"fetch_debug_html failed for {video_url}: {e}")
        return None


def get_base_url(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().split("@")[-1].split(":")[0]
        return BASE_URLS.get(host)
    except Exception:
        return None


def is_faphouse_link(url: str) -> bool:
    """True if url points at a supported faphouse.com/faphouse2.com host."""
    return get_base_url(url) is not None


class FanclubLockedError(RuntimeError):
    """Raised when a video's own view-state-data shows a REAL authenticated
    user (currentUserId/email actually set — i.e. the session genuinely is
    logged in server-side, not just locally cached as such) but this
    specific video's videoAccessType is 'fan' with videoViewAllowed False —
    Faphouse's per-creator Fan Club paywall, separate from and not covered
    by the account's site-wide Premium subscription.

    Same attribute name auto_scraper.py's process_and_upload_video already
    generically looks for via getattr(backend, "FanclubLockedError", ())
    (see that function's docstring, and fpo_downloader.py's own
    FanclubLockedError for the same pattern on a different backend) — so
    defining it here is enough on its own for auto-upload to permanently
    skip these instead of endlessly re-resolving a video this account can
    never actually get an m3u8 for without a separate Fan Club purchase.

    NOT raised when currentUserId is null even on the "authenticated"
    session attempt — that's the session/token having expired or never
    really authenticated server-side, a real bug to keep surfacing and
    retrying, not a permanent per-video lock."""
    pass


_URL_RE = re.compile(r"https?://\S+")


def extract_faphouse_links(text: str) -> list[str]:
    """Pull out all faphouse.com/faphouse2.com URLs found in text, in order,
    de-duplicated."""
    if not text:
        return []
    seen = set()
    links = []
    for match in _URL_RE.findall(text):
        url = match.rstrip(').,!?>\'"')
        if is_faphouse_link(url) and url not in seen:
            seen.add(url)
            links.append(url)
    return links


class AkClient:
    def __init__(self):
        # One entry per site: base_url -> {"session", "logged_in", "started_at", "failed_at"}.
        # Kept separate because faphouse.com and faphouse2.com are different
        # domains — a cookie set on one is never sent to the other, no matter
        # how the requests.Session is configured. Logging both in with the
        # same EMAIL/PASSWORD is what makes "one login" work for both sites.
        self._sites: dict[str, dict] = {}
        self._m3u8_cache = {}
        # How long to wait before retrying a failed login — avoids hammering
        # the auth endpoint on every request when credentials are wrong/
        # the site is temporarily rate-limiting us.
        self._LOGIN_RETRY_DELAY = 120  # seconds
        # base_url -> unix timestamp until which login won't even be
        # attempted, once the site's own maxAttemptsError lockout has
        # fired — see login()'s docstring note on why this exists and why
        # it needs to be much longer than _LOGIN_RETRY_DELAY.
        self._maxattempts_until: dict[str, float] = {}
        self._MAXATTEMPTS_BACKOFF = 3600  # seconds (1 hour)

    def ensure_session(self, base_url: str = None):
        base_url = base_url or DEFAULT_BASE_URL
        site = self._sites.get(base_url)
        now = time.time()
        session_expired = bool(site and site["logged_in"] and (now - site["started_at"] > SESSION_MAX_AGE))

        # Locked out by the site's own brute-force protection — don't even
        # attempt a login until the backoff expires (see login()'s note on
        # maxAttemptsError and _MAXATTEMPTS_BACKOFF above). Returns a
        # plain unauthenticated session so callers still get guest-fetch
        # behavior instead of erroring outright.
        lockout_until = self._maxattempts_until.get(base_url, 0)
        if now < lockout_until:
            logger.debug(
                f"Login for {base_url} is still locked out for "
                f"{int(lockout_until - now)}s more — using guest session, not retrying."
            )
            if site and site.get("session"):
                return site["session"]
            session = requests.Session()
            session.headers.update({'User-Agent': _UA})
            return session

        # Don't retry a recently-failed login on every single request — that
        # hammers the auth endpoint and adds 2-3s latency to every video.
        # Wait _LOGIN_RETRY_DELAY seconds before trying again.
        login_failed_recently = bool(
            site and not site["logged_in"]
            and (now - site.get("failed_at", 0)) < self._LOGIN_RETRY_DELAY
        )
        if login_failed_recently:
            logger.debug(f"Login for {base_url} failed recently — skipping retry, using existing session.")
            return site["session"]

        if not site or not site["logged_in"] or session_expired:
            if site and session_expired:
                logger.info(f"Session for {base_url} is old — forcing a fresh login...")
            else:
                logger.info(f"Creating new session for {base_url}...")
            session = requests.Session()
            session.headers.update({
                'User-Agent': _UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })
            site = {"session": session, "logged_in": False, "started_at": now, "failed_at": 0}
            self._sites[base_url] = site
            site["logged_in"] = self.login(session, base_url)
            site["started_at"] = time.time()
            if not site["logged_in"]:
                site["failed_at"] = time.time()
        return site["session"]

    def login(self, session: requests.Session, base_url: str) -> bool:
        if not EMAIL or not PASSWORD:
            logger.warning(f"EMAIL/PASSWORD not set — skipping login for {base_url}, guest fetch only.")
            return False

        logger.info(f"Attempting login to {base_url} with email: {EMAIL[:5]}...")

        session.headers.update({
            'User-Agent': _UA,
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Content-Type': 'application/json',
            'Origin': base_url,
            'Referer': f'{base_url}/',
            'DNT': '1',
            'Connection': 'keep-alive'
        })

        try:
            logger.info(f"Getting initial page for {base_url}...")
            init_res = session.get(base_url, timeout=10)
            logger.info(f"Initial page status: {init_res.status_code}")

            tracking_bag = "eyJwcm9tb19pZCI6IiIsInZpZGVvX2lkIjpudWxsLCJzdHVkaW9faWQiOm51bGwsInByb2R1Y2VyX2lkIjpudWxsLCJvcmllbnRhdGlvbiI6InN0cmFpZ2h0IiwibWxfcGFnZSI6Im1haW5fcGFnZSIsIm1sX3BhZ2VfdmFsdWVfaWQiOm51bGwsIm1sX3BhZ2VfdmFsdWUiOm51bGwsIm1sX3BhZ2VfbnVtYmVyIjpudWxsLCJtbF9yZWZfcGFnZV92YWx1ZV9pZCI6bnVsbCwibWxfcmVmX3BhZ2VfdmFsdWUiOiIiLCJtbF9yZWZfcGFnZV9udW1iZXIiOm51bGwsIm1sX3JlZl9wYWdlIjoiZGlyZWN0In0="

            # BUG FIX: this used to also try "email" as a fallback field
            # name whenever the first attempt got a 400, on the theory
            # that a field-name mismatch was the likely cause. Confirmed
            # wrong in production (2026-09-19): the site's own error body
            # for the "email"-field attempt is `{"errors":{"login":
            # ["Login cannot be blank."]}}` — that response is naming
            # "login" as the missing/invalid field regardless of what we
            # sent, which means "login" IS the correct field name and
            # always was; the second attempt could only ever repeat the
            # same failure while doubling every login's request volume.
            # That doubling, combined with ensure_session()'s own retry-
            # once-on-invalidated-session logic, was almost certainly
            # what tripped the account into the OTHER error also seen in
            # that same response body — `"maxAttemptsError": true` — the
            # site's own brute-force lockout, checked for below.
            payload = {
                "login": EMAIL,
                "password": PASSWORD,
                "rememberMe": "1",
                "recaptcha": "",
                "trackingParamsBag": tracking_bag,
            }
            logger.info(f"Sending login request to {base_url}...")
            login_res = session.post(f"{base_url}/api/auth/signin", json=payload, timeout=15)
            logger.info(f"Login response status: {login_res.status_code}")

            if login_res.status_code != 200:
                logger.warning(f"Login response body: {login_res.text[:1000]}")
                # The site's own account-lockout signal — once this is
                # true, retrying (even after the normal
                # _LOGIN_RETRY_DELAY) only prolongs the lockout instead of
                # recovering from it. Callers (ensure_session) check
                # self._maxattempts_until to back off far longer than a
                # normal failed-login retry.
                try:
                    body = login_res.json()
                except Exception:
                    body = {}
                if isinstance(body, dict) and body.get("maxAttemptsError"):
                    self._maxattempts_until[base_url] = time.time() + self._MAXATTEMPTS_BACKOFF
                    logger.error(
                        f"Login to {base_url} is locked out by the site's own brute-force "
                        f"protection (maxAttemptsError) — backing off for "
                        f"{self._MAXATTEMPTS_BACKOFF // 60} minutes instead of retrying every "
                        f"{self._LOGIN_RETRY_DELAY}s. Falling back to guest fetch until then."
                    )

            if login_res.status_code == 200:
                try:
                    data = login_res.json()
                    # Extract Bearer token — FapHouse is an SPA that uses JWT auth.
                    # The token comes back in the response body, NOT as a cookie.
                    # Without adding it as an Authorization header, all subsequent
                    # page fetches are effectively unauthenticated even though
                    # login "succeeded" (cookies alone don't carry the auth).
                    token = None
                    if isinstance(data, dict):
                        # Try common token field locations
                        token = (
                            data.get("token")
                            or (data.get("data") or {}).get("token")
                            or (data.get("data") or {}).get("access_token")
                            or data.get("access_token")
                            or data.get("jwt")
                        )
                    if token:
                        session.headers.update({"Authorization": f"Bearer {token}"})
                        logger.info(f"Login to {base_url} successful — Bearer token extracted and set!")
                        return True
                    if isinstance(data, dict) and (data.get("success") or data.get("data")):
                        # Login confirmed successful but no extractable token field —
                        # site may use HttpOnly cookies (fine, requests.Session carries them).
                        logger.info(f"Login to {base_url} successful (no token field — relying on cookies)!")
                        return True
                except Exception as ex:
                    logger.debug(f"Login JSON parse error: {ex}")

                # Last resort: check if the auth endpoint actually set session cookies
                # (NOT the initial homepage cookies, which are always present).
                # We check the login response's own Set-Cookie headers directly.
                if login_res.cookies:
                    session.cookies.update(login_res.cookies)
                    logger.info(f"Login to {base_url} successful (session cookies set)!")
                    return True

            logger.warning(f"Login to {base_url} failed — status {login_res.status_code}, will use guest fetch.")
            return False

        except Exception as e:
            logger.error(f"Login error for {base_url}: {str(e)}")
            return False

    def _decode_response(self, response):
        try:
            content_encoding = response.headers.get('Content-Encoding', '')

            if content_encoding:
                logger.info(f"Decoding {content_encoding} response...")

            if 'gzip' in content_encoding:
                try:
                    return gzip.decompress(response.content).decode('utf-8', errors='ignore')
                except Exception:
                    pass

            if 'deflate' in content_encoding:
                try:
                    return zlib.decompress(response.content).decode('utf-8', errors='ignore')
                except Exception:
                    try:
                        return zlib.decompress(response.content, -zlib.MAX_WBITS).decode('utf-8', errors='ignore')
                    except Exception:
                        pass

            if 'br' in content_encoding:
                try:
                    import brotli
                    return brotli.decompress(response.content).decode('utf-8', errors='ignore')
                except ImportError:
                    logger.warning("Brotli not installed, skipping...")
                except Exception:
                    pass

            try:
                return response.text
            except Exception:
                pass

            return response.text if response.text else str(response.content)

        except Exception as e:
            logger.error(f"Decoding error: {str(e)}")
            return response.text if response.text else str(response.content)

    def _find_m3u8_in_obj(self, obj, depth=0):
        """Recursively search a parsed JSON object for any string value
        that looks like an m3u8 URL — used for Next.js __NEXT_DATA__ blobs
        where the URL may be nested 5-10 levels deep."""
        if depth > 15:
            return None
        if isinstance(obj, str):
            if '.m3u8' in obj and obj.startswith('http'):
                return obj
        elif isinstance(obj, dict):
            for v in obj.values():
                found = self._find_m3u8_in_obj(v, depth + 1)
                if found:
                    return found
        elif isinstance(obj, list):
            for item in obj:
                found = self._find_m3u8_in_obj(item, depth + 1)
                if found:
                    return found
        return None

    def get_m3u8_url(self, video_url):
        cache_key = video_url.split('#')[0].strip()

        # 1. Check in-memory cache (fastest — same process, no disk I/O)
        cached = self._m3u8_cache.get(cache_key)
        if cached is not None:
            logger.info("✅ M3U8 from memory cache (instant).")
            return cached

        # 2. Check persistent disk cache (survives restarts)
        now = time.time()
        with _disk_cache_lock:
            entry = _disk_cache.get(cache_key)
        if entry and (now - entry.get("ts", 0)) < _CACHE_TTL:
            url = entry["url"]
            logger.info(f"✅ M3U8 from disk cache (age: {int(now - entry['ts'])}s, instant).")
            # Warm memory cache too
            self._m3u8_cache[cache_key] = url
            return url

        # 3. Full resolve (login + page fetch + m3u8 extraction)
        logger.info("🔍 Resolving M3U8 (no cache hit)...")
        result = self._resolve_m3u8_url(video_url)
        if result:
            # Save to memory cache
            if len(self._m3u8_cache) >= 200:
                self._m3u8_cache.pop(next(iter(self._m3u8_cache)))
            self._m3u8_cache[cache_key] = result
            # Save to disk cache
            with _disk_cache_lock:
                _disk_cache[cache_key] = {"url": result, "ts": now}
                # Prune old entries (keep max 500)
                if len(_disk_cache) > 500:
                    oldest = sorted(_disk_cache, key=lambda k: _disk_cache[k].get("ts", 0))
                    for old_k in oldest[:100]:
                        del _disk_cache[old_k]
            _save_disk_cache(_disk_cache)
            logger.info("💾 M3U8 cached to disk for future use.")
        return result

    def _resolve_m3u8_url(self, video_url):
        logger.info(f"Processing video URL: {video_url[:80]}...")

        if '#' in video_url:
            video_url = video_url.split('#')[0]

        base_url = get_base_url(video_url) or DEFAULT_BASE_URL

        session = self.ensure_session(base_url) if (EMAIL and PASSWORD) else None
        session_html = None
        if session:
            for login_attempt in (1, 2):
                try:
                    logger.info(f"Attempt 1: Using authenticated session for {base_url}...")

                    headers = {
                        'User-Agent': _UA,
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                        'Accept-Language': 'en-US,en;q=0.5',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'Referer': base_url,
                        'DNT': '1',
                        'Connection': 'keep-alive',
                        'Upgrade-Insecure-Requests': '1'
                    }

                    response = session.get(video_url, timeout=15, headers=headers)
                    logger.info(f"Session GET Status: {response.status_code}")

                    if response.status_code == 200:
                        session_html = self._decode_response(response)
                        if session_html:
                            m3u8 = self._extract_m3u8(session_html)
                            if m3u8:
                                logger.info("Found M3U8 URL with session!")
                                return m3u8

                            # Reached only if the page fetched fine but no
                            # stream was found — check whether our session
                            # is ACTUALLY still authenticated server-side,
                            # since ensure_session()'s cache saying
                            # logged_in=True just means login looked like
                            # it worked when it happened, not that the
                            # token/cookies are still valid on every
                            # request since (confirmed against a real
                            # failing page, 2026-09-10: session GET
                            # returned 200 with a full page, but
                            # view-state-data's user object was entirely
                            # null — a premium video silently served as a
                            # logged-out guest looks exactly like "video
                            # not found", not like an auth error). One
                            # retry with a forced-fresh login before
                            # falling through to the (also-unauthenticated)
                            # guest path and a wrong "locked"/"markup
                            # changed" diagnosis.
                            view_state = _parse_view_state(session_html)
                            if (view_state is not None and login_attempt == 1
                                    and _view_state_logged_in(view_state) is False):
                                logger.warning(
                                    f"Session for {base_url} claims logged_in but the site's own "
                                    f"tracking data shows no authenticated user — the session/token "
                                    f"has likely expired or been invalidated server-side. Forcing a "
                                    f"fresh login and retrying once before giving up."
                                )
                                self._sites.pop(base_url, None)
                                session = self.ensure_session(base_url)
                                if session:
                                    continue  # retry with the fresh session
                except Exception as e:
                    logger.warning(f"Session attempt failed: {str(e)}")
                break

        logger.info("Attempt 2: Trying guest fetch...")
        guest_html = None
        try:
            guest_session = requests.Session()
            guest_session.headers.update({
                'User-Agent': _UA,
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Accept-Encoding': 'gzip, deflate, br',
                'Referer': base_url,
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            })

            response = guest_session.get(video_url, timeout=15)
            logger.info(f"Guest Status: {response.status_code}")

            if response.status_code == 200:
                guest_html = self._decode_response(response)
                if guest_html:
                    m3u8 = self._extract_m3u8(guest_html)
                    if m3u8:
                        logger.info("Found M3U8 URL with guest!")
                        return m3u8
        except Exception as e:
            logger.warning(f"Guest attempt failed: {str(e)}")

        # ── Diagnose the failure ─────────────────────────────────────────
        # FIX: this used to only report guest_html's length regardless of
        # what happened on the session (authenticated) attempt, so a "0
        # chars" or misleading count could show up even when the session
        # attempt was the one that actually got real content (or vice
        # versa) — impossible to tell, from the log alone, which of the
        # two attempts actually ran/failed and why. Now both are reported
        # separately.
        #
        # Also dumps the ACTUAL VALUES (not just key names, per an earlier
        # version of this fix) of the specific view-state-data fields
        # confirmed relevant from a real failing page (2026-09-19):
        # user.currentUserId/email (null when not really authenticated,
        # even if ensure_session() thinks it's logged in) and
        # video.videoAccessType/videoViewAllowed (whether this specific
        # video needs premium/purchase access the account may not have).
        # Checked for BOTH session_html and guest_html separately — if
        # session_html shows a real currentUserId but videoViewAllowed is
        # still False, the account genuinely lacks access to THIS video
        # (not a bug); if session_html's currentUserId is ALSO null, the
        # "authenticated" session isn't actually authenticated server-side
        # at all, which is the real bug to chase (login()/ensure_session()
        # believes it succeeded but the site disagrees).
        def _html_status(html):
            if not html:
                return "none"
            if _looks_like_challenge_page(html):
                return f"{len(html)} chars, challenge page"
            return f"{len(html)} chars, not a challenge page"

        def _access_summary(html):
            vs = _parse_view_state(html or "")
            if vs is None:
                return "no view-state-data"
            user_obj = vs.get("user") or {}
            video_obj = vs.get("video") or {}
            return (
                f"currentUserId={user_obj.get('currentUserId')!r} "
                f"email={user_obj.get('email')!r} "
                f"hasPremium={user_obj.get('hasPremium')!r} "
                f"payingUser={user_obj.get('payingUser')!r} "
                f"videoAccessType={video_obj.get('videoAccessType')!r} "
                f"videoViewAllowed={video_obj.get('videoViewAllowed')!r}"
            )

        session_status = _html_status(session_html)
        guest_status = _html_status(guest_html)
        logger.error(
            f"Failed to find M3U8 URL with all attempts. "
            f"session_html: {session_status} | guest_html: {guest_status} | "
            f"saved to {_DEBUG_HTML_PATH} for inspection."
        )
        if session_html:
            logger.error(f"session_html access: {_access_summary(session_html)}")
        if guest_html:
            logger.error(f"guest_html access: {_access_summary(guest_html)}")
        _save_debug_html(guest_html or session_html)

        # FIX: a video whose videoAccessType is 'fan' (Faphouse's per-
        # creator Fan Club paywall — separate from and not covered by a
        # site-wide Premium subscription) can NEVER resolve an m3u8 no
        # matter how many times this is retried: hasPremium/payingUser
        # being True doesn't help, and the account is genuinely,
        # permanently locked out of this specific video. Previously this
        # just returned None like every other failure, so auto_scraper.py
        # treated it as a generic transient "download failed" and kept
        # retrying it forever on every scrape pass. Only raise this when
        # session_html's own currentUserId is a REAL value — if it's null
        # too, the session itself isn't actually authenticated server-side
        # (a genuinely different, retry-worthy bug — see the comment
        # above _access_summary), not a permanent per-video lock.
        session_vs = _parse_view_state(session_html or "")
        if session_vs is not None:
            user_obj = session_vs.get("user") or {}
            video_obj = session_vs.get("video") or {}
            if (user_obj.get("currentUserId") is not None
                    and video_obj.get("videoAccessType") == "fan"
                    and video_obj.get("videoViewAllowed") is False):
                raise FanclubLockedError(
                    f"{video_url} needs a separate Fan Club subscription to this "
                    "creator — not covered by the account's site-wide Premium plan."
                )

        return None

    def _extract_m3u8(self, html_content):
        if not html_content:
            return None

        html_content = re.sub(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]', '', html_content)
        html_content = html_content.replace('\\/', '/')
        html_content = re.sub(r'\\u002[Ff]', '/', html_content)

        # ── Strategy 1: Extract from Next.js __NEXT_DATA__ blob ────────────
        # FapHouse is a Next.js app. All video/player config is pre-loaded
        # into a <script id="__NEXT_DATA__"> JSON blob. Extracting and
        # parsing it is more reliable than regex-scanning the entire HTML,
        # especially when the URL is deeply nested or has extra escaping.
        next_data_match = re.search(
            r'<script\s+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
            html_content, re.DOTALL | re.IGNORECASE
        )
        if next_data_match:
            try:
                next_json_str = next_data_match.group(1)
                # Recursively search for any .m3u8 URL string in the JSON blob
                m3u8_in_json = re.findall(
                    r'https?://[^"\'\\<>\s]+\.m3u8[^"\'\\<>\s]*',
                    next_json_str, re.IGNORECASE
                )
                if m3u8_in_json:
                    logger.info(f"Found M3U8 in __NEXT_DATA__: {m3u8_in_json[0][:80]}")
                    return m3u8_in_json[0]
                # Also try proper JSON parse for deeply-nested/escaped URLs
                next_data = json.loads(next_json_str)
                found = self._find_m3u8_in_obj(next_data)
                if found:
                    logger.info(f"Found M3U8 via JSON parse of __NEXT_DATA__: {found[:80]}")
                    return found
            except Exception as e:
                logger.debug(f"__NEXT_DATA__ parse failed: {e}")

        # ── Strategy 2: Standard regex patterns on full HTML ────────────────
        patterns = [
            r'https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*',
            r'//[^\s"\'<>]+\.m3u8[^\s"\'<>]*',
            r'src\s*=\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'href\s*=\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'file\s*:\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'url\s*:\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            r'source\s*:\s*["\']([^"\']+\.m3u8[^"\']*)["\']',
            # Quoted JSON key style: {"file": "https://...m3u8"}
            r'"(?:file|url|source|src|hls|hls_url|m3u8|m3u8_url|video_url|stream_url|playlist_url)"\s*:\s*"([^"]+\.m3u8[^"]*)"',
        ]

        found_urls = []
        for pattern in patterns:
            matches = re.findall(pattern, html_content, re.IGNORECASE | re.DOTALL)
            if matches:
                for match in matches:
                    if isinstance(match, tuple):
                        match = match[0]
                    m3u8_url = match.strip()
                    if '"' in m3u8_url:
                        m3u8_url = m3u8_url.split('"')[0]
                    if "'" in m3u8_url:
                        m3u8_url = m3u8_url.split("'")[0]
                    if '&amp;' in m3u8_url:
                        m3u8_url = m3u8_url.replace('&amp;', '&')

                    if m3u8_url.startswith('//'):
                        m3u8_url = 'https:' + m3u8_url

                    if m3u8_url.startswith('http') and '.m3u8' in m3u8_url:
                        found_urls.append(m3u8_url)

        seen = set()
        unique_urls = []
        for url in found_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        if unique_urls:
            logger.info(f"Found {len(unique_urls)} M3U8 URLs")
            return unique_urls[0]

        return None


# Module-level client instance, shared across calls (keeps both sites'
# sessions/logins warm between requests) — mirrors bot.py's `client = AkClient()`.
client = AkClient()


_OG_TITLE_RE = re.compile(r'<meta\s+property=["\']og:title["\']\s+content=["\']([^"\']+)["\']', re.I)
_OG_IMAGE_RE = re.compile(r'<meta\s+property=["\']og:image["\']\s+content=["\']([^"\']+)["\']', re.I)


def get_page_meta(video_url: str) -> dict:
    """Best-effort og:title / og:image straight off the video's own page —
    og:image is the site's own poster/thumbnail, so using it means the
    Telegram thumbnail matches exactly what's shown on the faphouse.com/
    faphouse2.com page itself, rather than a generic ffmpeg frame grab.
    Returns {"title": str|None, "poster_url": str|None}; both None on
    any failure (caller should fall back to something else).

    Uses the same authenticated session as get_m3u8_url so the real video
    page is returned — a bare unauthenticated GET often gets a Cloudflare
    challenge page back (status 200, but not the real content), whose
    og:title would be "Just a moment..." or "Attention Required!" rather
    than the actual video title.  Challenge pages are detected and rejected
    so callers always get a real title or None, never a CF placeholder."""
    base_url = get_base_url(video_url) or DEFAULT_BASE_URL
    try:
        # Prefer authenticated session — same one used for M3U8 resolution
        session = client.ensure_session(base_url) if (EMAIL and PASSWORD) else requests
        headers = {
            "User-Agent": _UA,
            "Referer": base_url,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        }
        r = session.get(video_url, timeout=10, headers=headers)
        if r.status_code != 200:
            logger.warning(f"get_page_meta: HTTP {r.status_code} for {video_url}")
            return {"title": None, "poster_url": None}
        html_text = client._decode_response(r)
    except Exception as e:
        logger.warning(f"get_page_meta fetch failed for {video_url}: {e}")
        return {"title": None, "poster_url": None}

    # Reject challenge/anti-bot pages — their og:title would be "Just a
    # moment...", "Attention Required!", etc., not the real video title.
    if _looks_like_challenge_page(html_text):
        logger.warning(
            f"get_page_meta: got a challenge/anti-bot page for {video_url} "
            f"— returning title=None to avoid storing CF placeholder as title."
        )
        return {"title": None, "poster_url": None}

    title_match = _OG_TITLE_RE.search(html_text)
    image_match = _OG_IMAGE_RE.search(html_text)
    return {
        "title": title_match.group(1).strip() if title_match else None,
        "poster_url": image_match.group(1).strip() if image_match else None,
    }


_STREAM_INF_RE = re.compile(r'#EXT-X-STREAM-INF:([^\n]*)\n\s*(\S+)')
_RESOLUTION_RE = re.compile(r'RESOLUTION=(\d+)x(\d+)')
_BANDWIDTH_RE = re.compile(r'BANDWIDTH=(\d+)')


def get_available_qualities(video_url: str) -> list:
    """Resolves the page's m3u8 and, if it's a master playlist, returns its
    quality variants sorted best-first:
    [{"label": "1080p", "height": 1080, "url": <absolute sub-playlist url>}, ...]
    Falls back to a single "Auto (Best)" entry (url=None, meaning "use
    whatever get_m3u8_url() finds" — see download_video's stream_url arg)
    if the playlist isn't a master playlist or something goes wrong, so
    callers never have to special-case "no choices"."""
    fallback = [{"label": "Auto (Best)", "height": None, "url": None}]

    master_url = client.get_m3u8_url(video_url)
    if not master_url:
        return fallback

    try:
        base_url = get_base_url(video_url) or DEFAULT_BASE_URL
        r = requests.get(master_url, timeout=10, headers={"User-Agent": _UA, "Referer": base_url})
        if r.status_code != 200:
            return fallback
        text = r.text
    except Exception as e:
        logger.warning(f"get_available_qualities fetch failed: {e}")
        return fallback

    if "#EXT-X-STREAM-INF" not in text:
        return fallback  # already a single-quality media playlist

    by_height = {}
    for match in _STREAM_INF_RE.finditer(text):
        attrs, rel_url = match.group(1), match.group(2).strip()
        res_match = _RESOLUTION_RE.search(attrs)
        if not res_match:
            continue  # not a real video-quality rendition (e.g. an audio-only track)
        height = int(res_match.group(2))
        bw_match = _BANDWIDTH_RE.search(attrs)
        bandwidth = int(bw_match.group(1)) if bw_match else 0
        abs_url = rel_url if rel_url.startswith("http") else urljoin(master_url, rel_url)
        # The same resolution can appear more than once at different
        # bitrates — keep only the highest-bandwidth entry for each, but
        # every distinct resolution present in the playlist is kept.
        if height not in by_height or bandwidth > by_height[height]["bandwidth"]:
            by_height[height] = {"height": height, "bandwidth": bandwidth, "url": abs_url}

    if not by_height:
        return fallback

    variants = sorted(by_height.values(), key=lambda v: v["height"], reverse=True)
    result = [{"label": f"{v['height']}p", "height": v["height"], "url": v["url"]} for v in variants]
    # Auto (url=None) alongside the explicit resolutions — download_video()
    # falls back to auto-resolving when url is None, and ffmpeg's HLS
    # demuxer picks the best variant on its own when handed the master
    # playlist directly, so this is a real "let it pick" option, not a
    # duplicate of the top resolution.
    result.append({"label": "Auto (Best)", "height": None, "url": None})
    return result


def find_ffmpeg() -> str | None:
    return shutil.which('ffmpeg')


def get_video_duration(url: str) -> float:
    """ffprobe the m3u8 for its total duration, in seconds — needed to turn
    ffmpeg's time=HH:MM:SS progress into an actual percentage below.
    Returns 0.0 if ffprobe isn't available or the probe fails (progress
    then falls back to showing downloaded time instead of a percentage)."""
    ffprobe = shutil.which('ffprobe')
    if not ffprobe:
        return 0.0
    try:
        result = subprocess.run(
            [ffprobe, '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', url],
            capture_output=True, text=True, timeout=20,
        )
        return float(result.stdout.strip())
    except Exception:
        return 0.0


def download_video(video_url: str, out_path: str, on_progress=None, stream_url: str = None) -> tuple[str, float]:
    """Resolves video_url to its m3u8 stream, then downloads+remuxes it to
    out_path via ffmpeg (segment copy — no re-encoding, fast and lossless).
    Raises RuntimeError with a clear reason on failure.
    Returns (out_path, total_duration_seconds).

    stream_url, if given, is used directly instead of resolving video_url
    again — this is how a specific quality picked via
    get_available_qualities() actually gets downloaded instead of
    whatever "auto" would pick.

    on_progress, if given, is called (from this same thread — run this
    function via asyncio.to_thread from async callers) with a dict:
    {pct, downloaded_bytes, speed_bytes_s, eta_s, elapsed_s, duration_s}.
    pct is None if the stream's duration couldn't be probed."""
    if not find_ffmpeg():
        raise RuntimeError("ffmpeg isn't installed / not on PATH on this host.")

    m3u8_url = stream_url or client.get_m3u8_url(video_url)
    if not m3u8_url:
        raise RuntimeError("Couldn't resolve a stream URL for that link.")

    start_time = time.time()
    referer = get_base_url(video_url) or DEFAULT_BASE_URL
    cmd = [
        'ffmpeg', '-y',
        # Without an explicit timeout, ffmpeg's initial connection to the
        # CDN can hang indefinitely on a slow/unresponsive server — no
        # error, no progress output, nothing to catch or retry, just
        # stuck forever on "Connecting to CDN" with no way out short of
        # manually killing the process. -rw_timeout aborts (raising a
        # clear ffmpeg error this function's caller can see and retry)
        # if the CDN doesn't respond within 20s. -reconnect* handles the
        # more common, milder case — a brief mid-download stall/drop —
        # by having ffmpeg itself retry instead of dying over a hiccup
        # that would otherwise waste all progress made so far.
        '-rw_timeout', '20000000',  # microseconds = 20s
        '-reconnect', '1',
        '-reconnect_streamed', '1',
        '-reconnect_delay_max', '5',
        '-headers', f'Referer: {referer}\r\nUser-Agent: Mozilla/5.0\r\n',
        '-i', m3u8_url,
        '-c', 'copy', '-bsf:a', 'aac_adtstoasc',
        '-progress', 'pipe:1', '-nostats',
        out_path,
    ]
    logger.info(f"[downloader] Starting ffmpeg download -> {out_path}")
    # stderr is merged into stdout (not a separate PIPE) — reading two
    # separate pipes from only one of them can deadlock if the unread one
    # fills its OS buffer while ffmpeg blocks trying to write to it.
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                universal_newlines=True, bufsize=1)

    # get_video_duration() does its own ffprobe network round-trip to the
    # CDN — running it before starting ffmpeg meant the visible download
    # didn't begin until BOTH that probe AND ffmpeg's own connection setup
    # had finished, often 5-10s of nothing happening. It's only needed for
    # the progress percentage, not for the download itself, so it runs
    # concurrently in the background instead — ffmpeg starts immediately,
    # and the percentage just switches on once the probe finishes (falls
    # back to a downloaded-bytes-only display for those first moments).
    duration_holder = {"value": 0.0}

    def _probe_duration():
        duration_holder["value"] = get_video_duration(m3u8_url)

    threading.Thread(target=_probe_duration, daemon=True).start()

    downloaded_secs = 0.0
    downloaded_kb = 0
    tail_lines = deque(maxlen=40)  # for diagnostics if ffmpeg fails

    # Track last activity time for stall detection (no new data = stuck)
    _last_activity = {"t": time.time(), "kb": 0}
    # Hard stall timeout: if ffmpeg produces no new data for this long, kill it
    _STALL_TIMEOUT = int(os.environ.get("FFMPEG_STALL_TIMEOUT", "90"))  # seconds

    def _maybe_fire_progress(force=False):
        """Fire on_progress callback — called on every progress=continue AND
        on the first out_time_ms line so the UI updates before the first full
        segment completes (30-60s head-start on slow CDNs)."""
        if not on_progress:
            return
        elapsed = time.time() - start_time
        speed = (downloaded_kb * 1024) / elapsed if elapsed > 0 else 0
        total_duration = duration_holder["value"]
        pct = (downloaded_secs / total_duration * 100) if total_duration > 0 else None
        eta = ((total_duration - downloaded_secs) / (downloaded_secs / elapsed)
               if total_duration > 0 and downloaded_secs > 0 and elapsed > 0 else 0)
        on_progress({
            "pct": pct,
            "downloaded_bytes": downloaded_kb * 1024,
            "speed_bytes_s": speed,
            "eta_s": eta,
            "elapsed_s": elapsed,
            "duration_s": total_duration,
            "connecting": downloaded_kb == 0,
        })

    _fired_first = False
    for line in process.stdout:
        line = line.strip()
        tail_lines.append(line)

        if line.startswith("out_time_ms="):
            try:
                downloaded_secs = int(line.split("=")[1]) / 1_000_000
            except (ValueError, IndexError):
                pass
            # Fire on very first out_time_ms so UI switches out of "Connecting..."
            # immediately — without this the first callback only arrives after
            # the full first progress=continue block (30-60s on slow CDNs).
            if not _fired_first and downloaded_secs > 0:
                _fired_first = True
                _maybe_fire_progress()

        elif line.startswith("total_size="):
            try:
                new_kb = int(line.split("=")[1]) / 1024
                if new_kb > _last_activity["kb"]:
                    _last_activity["t"] = time.time()
                    _last_activity["kb"] = new_kb
                downloaded_kb = new_kb
            except (ValueError, IndexError):
                pass

        elif line == "progress=continue":
            # Stall detection: kill ffmpeg if no new data in _STALL_TIMEOUT seconds
            stall_secs = time.time() - _last_activity["t"]
            if stall_secs > _STALL_TIMEOUT:
                logger.warning(
                    f"[downloader] ffmpeg stalled for {stall_secs:.0f}s (no new data) — killing process"
                )
                try:
                    process.kill()
                except Exception:
                    pass
                break
            _maybe_fire_progress()

    process.wait()

    if process.returncode != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        if os.path.exists(out_path):
            os.remove(out_path)
        if tail_lines:
            logger.error(f"[downloader] ffmpeg output (last {len(tail_lines)} lines):\n" + "\n".join(tail_lines))
        raise RuntimeError(f"ffmpeg failed (exit code {process.returncode}) — the stream may be geo/session-locked.")

    logger.info(f"[downloader] Done: {out_path} ({os.path.getsize(out_path) / 1024 / 1024:.1f} MB)")
    return out_path, duration_holder["value"]
