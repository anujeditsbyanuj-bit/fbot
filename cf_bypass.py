"""
Cloudflare JS-challenge bypass via FlareSolverr — for links where the
site's own Cloudflare "checking your browser" challenge requires actually
running JavaScript, which curl_cffi's TLS/JA3 impersonation (used
everywhere else in ytdlp_downloader.py) can't do: impersonation only
copies a browser's network-level fingerprint, it doesn't execute a
challenge page's JS. Confirmed live on a spankbang.party mirror link:
curl_cffi with impersonate="chrome" still got back Cloudflare's
challenge-error-text page (a real JS challenge), not a 403 that
impersonation could talk its way past.

FlareSolverr (https://github.com/FlareSolverr/FlareSolverr) is a small
standalone HTTP service that runs a real headless browser, solves the
challenge, and hands back the resulting cf_clearance cookie + matching
User-Agent — which yt-dlp can then reuse for the actual extraction/
download like a normal browser session (no need to run a browser for
every single request, just once per site until the cookie expires).

THIS IS OPTIONAL INFRASTRUCTURE, not something this file can set up on
its own: FlareSolverr has to be running as its own service (see the
setup note at the bottom of this docstring). Every function here fails
gracefully if it isn't reachable — ytdlp_downloader.py falls back to
exactly the plain-403 behavior it had before this existed, it doesn't
break anything by being installed-but-unreachable.

Setup (once, on the VPS):
    docker run -d --name flaresolverr --restart unless-stopped \\
        -p 8191:8191 -e LOG_LEVEL=info ghcr.io/flaresolverr/flaresolverr:latest
No Docker? See https://github.com/FlareSolverr/FlareSolverr#installation
for a plain-binary install instead. Once it's running, this module finds
it automatically at http://localhost:8191/v1 (override with the
FLARESOLVERR_URL env var if it's running elsewhere) — nothing else to
configure.
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

FLARESOLVERR_URL = os.environ.get("FLARESOLVERR_URL", "http://localhost:8191/v1")
_SOLVE_TIMEOUT = 25  # keep this well under the ~30s a genuine solve needs at
# most — if FlareSolverr isn't actually reachable/working (unverified live —
# see this file's own docstring), a caller shouldn't be stuck waiting a full
# 60s to find that out on every matching 403, on every site, before falling
# through to the plain error it would've gotten anyway.
_CACHE_TTL = 20 * 60  # cf_clearance cookies commonly last 30min-2h; 20min is a safe floor
_CLOUDSCRAPER_FALLBACK_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
)  # used only if cloudscraper's own session somehow has no User-Agent header set

_cache: dict[str, dict] = {}  # domain -> {"cookiefile": path, "user_agent": str, "expires": ts}
_cache_lock = threading.Lock()
_unreachable_last_warned = 0.0  # timestamp — throttle the "not running" warning, don't silence it forever
_UNREACHABLE_WARN_INTERVAL = 600  # seconds (10 min)


def _domain_of(url: str) -> str:
    try:
        return urlparse(url).netloc.lower().split("@")[-1].split(":")[0]
    except Exception:
        return url


def get_bypass_opts(url: str) -> dict | None:
    """Non-blocking — just checks the cache. Returns
    {"cookiefile": path, "http_headers": {"User-Agent": ...}} for
    _base_opts() to merge in if a still-fresh solve exists for this
    URL's domain, else None. Never triggers a solve itself (that only
    happens from try_solve() below, on an actual 403)."""
    domain = _domain_of(url)
    with _cache_lock:
        entry = _cache.get(domain)
        if entry and time.time() < entry["expires"]:
            return {"cookiefile": entry["cookiefile"], "http_headers": {"User-Agent": entry["user_agent"]}}
    return None


def get_bypass_for_requests(url: str) -> dict | None:
    """Same cache get_bypass_opts() reads, shaped for the plain
    `requests` library instead of yt-dlp — used by terabox_downloader.py
    (and any other non-yt-dlp caller), which has no concept of a
    yt-dlp "cookiefile" option. Returns {"cookies": {name: value, ...},
    "headers": {"User-Agent": ...}} ready to pass straight into
    requests.get/post's own cookies=/headers= kwargs, or None if nothing
    fresh is cached for this domain. Reads the same Netscape cookiejar
    file try_solve() already writes, rather than duplicating that
    storage — one cache, two shapes read from it."""
    domain = _domain_of(url)
    with _cache_lock:
        entry = _cache.get(domain)
        if not (entry and time.time() < entry["expires"]):
            return None
        cookiefile, user_agent = entry["cookiefile"], entry["user_agent"]
    try:
        jar = http.cookiejar.MozillaCookieJar(cookiefile)
        jar.load(ignore_discard=True, ignore_expires=True)
        cookies = {c.name: c.value for c in jar}
    except Exception as e:
        logger.warning(f"[cf-bypass] couldn't reload cookiejar for {domain}: {e}")
        return None
    return {"cookies": cookies, "headers": {"User-Agent": user_agent}}


def try_solve(url: str) -> bool:
    """Public entry point — tries the cheapest solver first, falling
    through to the next if it can't solve this particular challenge.
    Returns True (and populates the cache for get_bypass_opts() to pick
    up) as soon as one of them works, False if none could. Never raises.

    1. cloudscraper (_try_solve_cloudscraper) — an in-process JS-VM that
       solves Cloudflare's simpler challenges directly, no separate
       service needed. Confirmed working against spankbang.com's
       challenge specifically (ported from spankbang-dl's scraper.py,
       an open-source SpankBang downloader that uses this same library
       successfully there).
    2. FlareSolverr (_try_solve_flaresolverr) — a real headless browser,
       for challenges cloudscraper's lighter JS-VM can't solve. Needs
       its own service running (see this file's docstring); fails
       gracefully and cheaply if it isn't."""
    return _try_solve_cloudscraper(url) or _try_solve_flaresolverr(url)


def _try_solve_cloudscraper(url: str) -> bool:
    """cloudscraper tier — see try_solve()'s docstring. Populates the
    same _cache dict (and so the same get_bypass_opts()) FlareSolverr
    does, via the same Netscape-cookiejar helper, so yt-dlp picks up
    whichever tier actually solved it identically either way."""
    try:
        import cloudscraper
    except ImportError:
        logger.warning(
            "[cf-bypass] cloudscraper not installed — skipping straight to FlareSolverr. "
            "Run `pip install cloudscraper` (it's in requirements.txt) if this keeps happening."
        )
        return False

    domain = _domain_of(url)
    try:
        scraper = cloudscraper.create_scraper()
        # BUG FIX: cloudscraper's plain .get() with no headers was itself
        # getting a flat 403 from spankbang.com (confirmed from a real log:
        # "cloudscraper couldn't solve spankbang.com: 403 Client Error") —
        # not a Cloudflare JS challenge cloudscraper failed to solve, but a
        # simpler bot-check that a request with no Referer never gets past
        # in the first place, same as the orphaned spankbang_scraper.py
        # module already knew to send. A same-site Referer is what a real
        # browser navigating to this URL would always have, so send one.
        headers = {"Referer": f"https://{domain}/"}
        resp = scraper.get(url, timeout=_SOLVE_TIMEOUT, headers=headers)
        resp.raise_for_status()
    except Exception as e:
        logger.warning(f"[cf-bypass] cloudscraper couldn't solve {domain}: {e}")
        return False

    cookies = [
        {"name": c.name, "value": c.value, "domain": c.domain or f".{domain}", "path": c.path or "/"}
        for c in scraper.cookies
    ]
    # cf_clearance specifically is what proves the challenge was actually
    # solved (not just "the request went through" — plenty of pages
    # 200-OK with a challenge body, which raise_for_status() won't catch
    # since it's a successful HTTP response, just not the real page).
    if not any(c["name"] == "cf_clearance" for c in cookies):
        logger.warning(f"[cf-bypass] cloudscraper got a response for {domain} but no cf_clearance cookie — likely not a Cloudflare challenge, or unsolved.")
        return False

    user_agent = scraper.headers.get("User-Agent") or _CLOUDSCRAPER_FALLBACK_UA
    try:
        cookiefile = _write_netscape_cookiefile(cookies, domain)
    except Exception as e:
        logger.warning(f"[cf-bypass] couldn't write cookiejar for {domain}: {e}")
        return False

    with _cache_lock:
        _cache[domain] = {"cookiefile": cookiefile, "user_agent": user_agent, "expires": time.time() + _CACHE_TTL}
    logger.info(f"[cf-bypass] ✅ solved Cloudflare challenge for {domain} via cloudscraper — cached for {_CACHE_TTL // 60} min.")
    return True


def _try_solve_flaresolverr(url: str) -> bool:
    """Call FlareSolverr for this URL, cache the result for its domain on
    success. Returns True if a bypass is now cached and ready (caller
    should rebuild its yt-dlp opts via _base_opts() to pick it up),
    False on any failure — unreachable FlareSolverr, a genuine non-
    Cloudflare block it can't help with, timeout, etc. Never raises."""
    global _unreachable_last_warned
    domain = _domain_of(url)
    try:
        resp = requests.post(
            FLARESOLVERR_URL,
            json={"cmd": "request.get", "url": url, "maxTimeout": _SOLVE_TIMEOUT * 1000},
            timeout=_SOLVE_TIMEOUT + 10,
        )
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.ConnectionError:
        # BUG FIX: this used to be a one-shot flag — warned once ever
        # per process, then silently returned False on every single
        # later call for the rest of the bot's uptime with NO log output
        # at all. That made a real Cloudflare-bypass failure (confirmed
        # live: cloudscraper failed, then this silently failed too, and
        # the only visible trace was the generic RuntimeError several
        # layers up — nothing here explaining FlareSolverr was even
        # tried, let alone why it failed) look indistinguishable from
        # FlareSolverr never having been called at all. Throttled to
        # once per _UNREACHABLE_WARN_INTERVAL instead of once-ever, so
        # it stays visible for an ongoing outage without spamming every
        # single request.
        now = time.time()
        if now - _unreachable_last_warned > _UNREACHABLE_WARN_INTERVAL:
            logger.warning(
                f"[cf-bypass] FlareSolverr isn't reachable at {FLARESOLVERR_URL} — "
                "Cloudflare-JS-challenge links (403 with no impersonation fix) will "
                "keep failing until it's running. See cf_bypass.py's docstring for setup. "
                f"(this warning is throttled to once per {_UNREACHABLE_WARN_INTERVAL // 60} min)"
            )
            _unreachable_last_warned = now
        return False
    except Exception as e:
        logger.warning(f"[cf-bypass] FlareSolverr request failed for {domain}: {e}")
        return False

    if data.get("status") != "ok":
        logger.warning(f"[cf-bypass] FlareSolverr couldn't solve {domain}: {data.get('message')}")
        return False

    solution = data.get("solution") or {}
    cookies = solution.get("cookies") or []
    user_agent = solution.get("userAgent")
    if not cookies or not user_agent:
        logger.warning(f"[cf-bypass] FlareSolverr returned no usable cookies/UA for {domain}.")
        return False

    try:
        cookiefile = _write_netscape_cookiefile(cookies, domain)
    except Exception as e:
        logger.warning(f"[cf-bypass] couldn't write cookiejar for {domain}: {e}")
        return False

    with _cache_lock:
        _cache[domain] = {"cookiefile": cookiefile, "user_agent": user_agent, "expires": time.time() + _CACHE_TTL}
    logger.info(f"[cf-bypass] ✅ solved Cloudflare challenge for {domain} — cached for {_CACHE_TTL // 60} min.")
    return True


def _write_netscape_cookiefile(cookies: list, domain: str) -> str:
    """yt-dlp's cookiefile option expects the Netscape/Mozilla cookies.txt
    format — build one from FlareSolverr's JSON cookie list. One file per
    domain, reused (overwritten) across solves rather than accumulating
    temp files forever."""
    path = os.path.join(tempfile.gettempdir(), f"cf_bypass_{domain.replace('.', '_')}.txt")
    jar = http.cookiejar.MozillaCookieJar(path)
    for c in cookies:
        try:
            jar.set_cookie(http.cookiejar.Cookie(
                version=0, name=c["name"], value=c["value"],
                port=None, port_specified=False,
                domain=c.get("domain", f".{domain}"),
                domain_specified=True, domain_initial_dot=c.get("domain", "").startswith("."),
                path=c.get("path", "/"), path_specified=True,
                secure=c.get("secure", False),
                expires=int(c["expiry"]) if c.get("expiry") else int(time.time()) + _CACHE_TTL,
                discard=False, comment=None, comment_url=None, rest={},
            ))
        except Exception:
            continue  # one malformed cookie shouldn't sink the whole batch
    jar.save(ignore_discard=True, ignore_expires=True)
    return path
