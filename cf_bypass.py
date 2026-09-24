"""cf_bypass.py — shared Cloudflare JS-challenge bypass.

Referenced (via `import cf_bypass`, guarded by try/except ImportError so
nothing breaks if this file is ever missing) by every scraper backend that
hits a Cloudflare-protected site:

  - ytdlp_downloader.py — calls try_solve(url) on a suspected CF 403, then
    get_bypass_opts(url) to fetch the cached cookiefile/http_headers to
    feed straight into yt-dlp's own options dict.
  - diskwala.py — _get_diskwala_net_cf_cookies() reads get_bypass_opts()'s
    cookiefile directly (Netscape format, stdlib http.cookiejar) rather
    than calling get_requests_cookies() below, but the effect is the same.

Two-tier strategy, cheapest first (see requirements.txt's own comment
above the cloudscraper pin, which this implements):

  1. cloudscraper — an in-process JS-VM that solves the older/simpler
     Cloudflare "I'm Under Attack Mode" JS challenges without a real
     browser. Fast: no browser to spin up, just a plain HTTP round-trip.
     Can't solve newer Turnstile/managed-challenge pages.

  2. FlareSolverr — a real headless-Chrome solver (cloned to
     /opt/flaresolver by the Dockerfile, started by
     flaresolverr_bootstrap.py at boot — see main.py's
     flaresolver_bootstrap.start_background() call). Slower, since it
     drives an actual browser, but handles challenges cloudscraper can't.
     Talked to over its own local HTTP API (default port 8191); see
     FLARESOLVERR_URL below if that ever needs to point somewhere else.

Every public function here is deliberately fail-soft — solving Cloudflare
challenges is inherently a best-effort, "try to help if we can" feature
for every caller, never a hard requirement, so nothing in this module
ever raises. A caller that gets False/None back just proceeds exactly as
it would if this whole module didn't exist.
"""

import http.cookiejar
import logging
import os
import tempfile
import threading
import time
from urllib.parse import urlparse

import requests

logger = logging.getLogger("faphouse_bot")

try:
    import cloudscraper
    _CLOUDSCRAPER_AVAILABLE = True
except ImportError:
    _CLOUDSCRAPER_AVAILABLE = False
    logger.warning("cf_bypass: cloudscraper not installed — skipping straight to FlareSolverr for every challenge.")

FLARESOLVERR_URL = os.getenv("FLARESOLVERR_URL", "http://127.0.0.1:8191/v1")
FLARESOLVERR_TIMEOUT_MS = int(os.getenv("FLARESOLVERR_TIMEOUT_MS", "60000"))

# How long a solved cf_clearance is trusted for before try_solve() will
# solve fresh again instead of relying on the cache. Real Cloudflare
# clearance cookies are usually valid 30min-2h depending on the site's own
# security-level setting; 25 minutes is a conservative floor that stays
# safely under that for every setting without re-solving needlessly often.
_CACHE_TTL_SECONDS = 25 * 60

_cache: dict[str, dict] = {}          # domain -> {cookies, user_agent, cookiefile, expires}
_cache_lock = threading.Lock()
_domain_locks: dict[str, threading.Lock] = {}
_domain_locks_guard = threading.Lock()

_COOKIEFILE_DIR = os.path.join(tempfile.gettempdir(), "cf_bypass_cookies")


def _domain_of(url: str) -> str:
    return urlparse(url).netloc.lower().split("@")[-1].split(":")[0]


def _domain_lock(domain: str) -> threading.Lock:
    """One lock per domain, so two callers racing to solve the SAME
    domain's challenge at once don't both spin up cloudscraper/FlareSolverr
    in parallel — the second one just waits and then reuses whatever the
    first one cached, rather than doubling the (slow) solve work."""
    with _domain_locks_guard:
        lock = _domain_locks.get(domain)
        if lock is None:
            lock = threading.Lock()
            _domain_locks[domain] = lock
        return lock


def _write_cookiefile(domain: str, cookies: list[dict], user_agent: str) -> str:
    """Write a Netscape-format cookiefile yt-dlp (and diskwala.py's own
    cookiejar reader) can load directly. One file per domain, overwritten
    on every fresh solve."""
    os.makedirs(_COOKIEFILE_DIR, exist_ok=True)
    path = os.path.join(_COOKIEFILE_DIR, f"{domain}.txt")
    jar = http.cookiejar.MozillaCookieJar(path)
    for c in cookies:
        jar.set_cookie(http.cookiejar.Cookie(
            version=0,
            name=c["name"], value=c["value"],
            port=None, port_specified=False,
            domain=c.get("domain") or f".{domain}",
            domain_specified=True,
            domain_initial_dot=(c.get("domain") or "").startswith("."),
            path=c.get("path") or "/",
            path_specified=True,
            secure=bool(c.get("secure", True)),
            expires=int(time.time()) + _CACHE_TTL_SECONDS,
            discard=False, comment=None, comment_url=None, rest={},
        ))
    jar.save(ignore_discard=True, ignore_expires=True)
    return path


def _store(domain: str, cookies: list[dict], user_agent: str) -> None:
    cookiefile = _write_cookiefile(domain, cookies, user_agent)
    with _cache_lock:
        _cache[domain] = {
            "cookies": cookies,
            "user_agent": user_agent,
            "cookiefile": cookiefile,
            "expires": time.time() + _CACHE_TTL_SECONDS,
        }


def _cached(domain: str) -> dict | None:
    with _cache_lock:
        entry = _cache.get(domain)
    if entry and entry["expires"] > time.time():
        return entry
    return None


def try_solve_cloudscraper(url: str) -> bool:
    """Tier 1: cloudscraper's in-process JS-VM. Returns True and caches
    the result on success. Never raises — any exception (network error,
    cloudscraper's own CloudflareChallengeError when it can't crack a
    harder challenge, etc.) is treated the same as "couldn't solve it",
    leaving the door open for try_solve() to fall back to FlareSolverr."""
    if not _CLOUDSCRAPER_AVAILABLE:
        return False
    domain = _domain_of(url)
    try:
        scraper = cloudscraper.create_scraper(
            browser={"browser": "chrome", "platform": "windows", "mobile": False}
        )
        resp = scraper.get(url, timeout=30)
        if resp.status_code in (403, 503):
            logger.info(f"cf_bypass[cloudscraper]: {domain} still returned {resp.status_code} — couldn't crack this challenge")
            return False
        cookies = [
            {"name": c.name, "value": c.value, "domain": c.domain, "path": c.path, "secure": c.secure}
            for c in scraper.cookies
        ]
        if not cookies:
            logger.info(f"cf_bypass[cloudscraper]: {domain} gave a 200 but no cookies came back — nothing to cache")
            return False
        user_agent = scraper.headers.get("User-Agent", "Mozilla/5.0")
        _store(domain, cookies, user_agent)
        logger.info(f"cf_bypass[cloudscraper]: solved {domain} ({len(cookies)} cookie(s) cached)")
        return True
    except Exception as e:
        logger.info(f"cf_bypass[cloudscraper]: failed for {domain} ({e})")
        return False


def try_solve_flaresolverr(url: str) -> bool:
    """Tier 2: a real headless-Chrome solve via the local FlareSolverr
    instance (see this module's docstring for where that comes from).
    Same fail-soft contract as try_solve_cloudscraper() — a FlareSolverr
    that isn't running (connection refused), times out, or reports its
    own failure all just mean "couldn't solve it", never an exception."""
    domain = _domain_of(url)
    try:
        resp = requests.post(
            FLARESOLVERR_URL,
            json={"cmd": "request.get", "url": url, "maxTimeout": FLARESOLVERR_TIMEOUT_MS},
            timeout=(FLARESOLVERR_TIMEOUT_MS / 1000) + 10,
        )
        data = resp.json()
        if data.get("status") != "ok":
            logger.info(f"cf_bypass[flaresolverr]: {domain} solve reported status={data.get('status')!r}: {data.get('message')}")
            return False
        solution = data.get("solution") or {}
        raw_cookies = solution.get("cookies") or []
        cookies = [
            {"name": c["name"], "value": c["value"], "domain": c.get("domain") or f".{domain}",
             "path": c.get("path") or "/", "secure": c.get("secure", True)}
            for c in raw_cookies if "name" in c and "value" in c
        ]
        if not cookies:
            logger.info(f"cf_bypass[flaresolverr]: {domain} solve reported ok but returned no cookies")
            return False
        user_agent = solution.get("userAgent") or "Mozilla/5.0"
        _store(domain, cookies, user_agent)
        logger.info(f"cf_bypass[flaresolverr]: solved {domain} ({len(cookies)} cookie(s) cached)")
        return True
    except requests.exceptions.ConnectionError:
        logger.info(f"cf_bypass[flaresolverr]: couldn't reach {FLARESOLVERR_URL} — is it still starting up, or did flaresolver_bootstrap fail?")
        return False
    except Exception as e:
        logger.info(f"cf_bypass[flaresolverr]: failed for {domain} ({e})")
        return False


def try_solve(url: str) -> bool:
    """Public entry point every backend calls. Tries cloudscraper first,
    then FlareSolverr, caching whichever succeeds first (or returning
    False if both failed — callers already treat that as "no bypass
    available" and fall back to their normal, un-bypassed request chain).

    Serialized per-domain (see _domain_lock) so concurrent callers for the
    same site share one solve instead of racing duplicate ones; a second
    caller that arrives after the first already solved it just gets the
    fresh cache immediately without solving again."""
    domain = _domain_of(url)
    with _domain_lock(domain):
        if _cached(domain) is not None:
            return True
        if try_solve_cloudscraper(url):
            return True
        return try_solve_flaresolverr(url)


def get_bypass_opts(url: str) -> dict | None:
    """yt-dlp-shaped cached bypass for this URL's domain, or None if
    try_solve() hasn't succeeded for it (recently enough — see
    _CACHE_TTL_SECONDS). Shape matches what yt-dlp's own options dict
    expects: {"cookiefile": <path>, "http_headers": {"User-Agent": ...}}."""
    entry = _cached(_domain_of(url))
    if entry is None:
        return None
    return {"cookiefile": entry["cookiefile"], "http_headers": {"User-Agent": entry["user_agent"]}}


def get_requests_cookies(url: str) -> dict:
    """Same cache as get_bypass_opts(), as a plain {name: value} dict for
    a `requests.Session` to use directly. Never raises — returns {} for
    "nothing cached yet", same as every other lookup in this module."""
    entry = _cached(_domain_of(url))
    if entry is None:
        return {}
    return {c["name"]: c["value"] for c in entry["cookies"]}
