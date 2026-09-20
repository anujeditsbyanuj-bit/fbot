"""
mat6tube.com video downloader engine.

REWRITTEN against NoodleMat-DL (github.com — a working, tested reference
implementation for this exact site/player), which revealed the old
approach here was wrong in two ways:

  1. Quality resolution: mat6tube (a "pvvstream.pro" hosted player) embeds
     its real quality ladder as `window.playlist = {"sources": [{"label":
     "720p", "file": <url>}, ...]}` JSON directly on the watch page (or,
     for some videos, only on a secondary "download page" the watch page
     links to via a `downloadUrl="..."` attribute) — NOT via a static
     `ya:ovs:content_url` meta tag pointing at a predictable
     /videofile/<id>.mp4 path. That meta-tag/guessed-pattern approach
     (this module's old docstring: "confirmed directly against a real
     page fetch") only worked for whichever video it happened to be
     checked against; it 404s for a meaningful fraction of videos (the
     old docstring's own note about group-owned/negative-ownerId ids),
     and even when it worked it only ever exposed ONE quality with no
     real ladder — the HEAD-check-then-raise dance around that guess was
     itself a chunk of the "quality bohot time mein fetch hota hai"
     slowness this was fixed for.
  2. Downloading: the resolved CDN file URL is on a *different* domain
     from mat6tube.com's own watch pages, and that CDN blocks plain
     `requests`' TLS fingerprint — NoodleMat-DL's own comment: "aria2c is
     required because requests is blocked by the CDN's TLS protection".
     Its own native-downloader fallback (used when aria2c isn't
     available) works around this with curl_cffi's impersonate="chrome",
     the same mechanism this codebase already uses elsewhere (see
     fpo_downloader.py's _ensure_session) — ported here instead of
     shelling out to aria2c, to avoid a second download codepath.

The watch page itself (mat6tube.com) still fetches fine with plain
requests — this only affects the actual video-file CDN request.
"""

import json as _json
import logging
import os
import re
import time
from urllib.parse import urlparse

import requests

try:
    from curl_cffi.requests import Session as CurlSession
    _CURL_CFFI_OK = True
except ImportError:
    _CURL_CFFI_OK = False

logger = logging.getLogger(__name__)

BASE_URL = "https://mat6tube.com"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

_WATCH_ID_RE = re.compile(r"/watch/(-?\d+_\d+)")
_PLAYLIST_RE = re.compile(r"window\.playlist\s*=\s*(\{.*?\});", re.DOTALL)
_DOWNLOAD_URL_ATTR_RE = re.compile(r'downloadUrl="([^"]+)"')
_DIRECT_MP4_RE = re.compile(r'(https://[^\s"\'<>]+?\.mp4[^\s"\'<>]*)')

# noodlemagazine.com and noodle.yemoja.xyz are the same site/player under
# different domains — NoodleMat-DL's own reference implementation just
# rewrites either one to mat6tube.com before doing anything else, so
# _normalize_url() below does the same (same /watch/<id> URL structure on
# all three, confirmed by that reference).
_ALT_DOMAINS = ("noodlemagazine.com", "www.noodlemagazine.com", "noodle.yemoja.xyz", "www.noodle.yemoja.xyz")


def _normalize_url(url: str) -> str:
    for domain in _ALT_DOMAINS:
        if domain in url.lower():
            return re.sub(re.escape(domain), BASE_URL.split("//")[1], url, flags=re.IGNORECASE)
    return url


def is_mat6tube_link(url: str) -> bool:
    try:
        host = urlparse(url).netloc.lower().split("@")[-1].split(":")[0]
        return host in ("mat6tube.com", "www.mat6tube.com") + _ALT_DOMAINS
    except Exception:
        return False


_URL_RE = re.compile(r"https?://\S+")


def extract_mat6tube_links(text: str) -> list[str]:
    """Same contract as faphouse_downloader.extract_faphouse_links()."""
    if not text:
        return []
    seen, out = set(), []
    for match in _URL_RE.findall(text):
        url = match.rstrip(").,!?>'\"")
        if is_mat6tube_link(url) and url not in seen:
            seen.add(url)
            out.append(url)
    return out


def _video_id_from_url(video_url: str) -> str | None:
    m = _WATCH_ID_RE.search(video_url)
    return m.group(1) if m else None


# ── Watch-page HTML cache (plain requests — the mat6tube.com domain
# itself isn't the CDN that blocks non-browser TLS fingerprints) ──────────
_HTML_CACHE: dict = {}
_HTML_CACHE_TTL = 300  # seconds — covers get_available_qualities() + get_page_meta()
                        # running back-to-back for the same link (main.py's
                        # show_quality_menu() always calls both); without this
                        # each one independently re-fetched the same watch page,
                        # doubling every request's latency for no reason.


def _fetch_html(url: str, force: bool = False, referer: str = None) -> str:
    now = time.time()
    if not force:
        cached = _HTML_CACHE.get(url)
        if cached and (now - cached[0]) < _HTML_CACHE_TTL:
            return cached[1]
    r = requests.get(url, timeout=15, headers={"User-Agent": _UA, "Referer": referer or BASE_URL})
    r.raise_for_status()
    html = r.text
    _HTML_CACHE[url] = (now, html)
    if len(_HTML_CACHE) > 300:
        for k in list(_HTML_CACHE.keys())[:50]:
            _HTML_CACHE.pop(k, None)
    return html


# ── CDN session (impersonated — see module docstring point 2) ─────────────
_session_state = {"session": None, "started_at": 0.0}
_SESSION_MAX_AGE = 20 * 60


def _cdn_session():
    """A curl_cffi Session with Chrome TLS impersonation for talking to
    the video-file CDN (a different domain from mat6tube.com's own watch
    pages), which otherwise blocks plain requests/urllib — see module
    docstring. Falls back to plain requests if curl_cffi isn't installed
    (shouldn't happen — it's a hard requirement in requirements.txt for
    ytdlp_downloader.py/fpo_downloader.py already — but degrades instead
    of hard-crashing this module specifically if it's ever missing)."""
    now = time.time()
    expired = _session_state["session"] is not None and (now - _session_state["started_at"] > _SESSION_MAX_AGE)
    if _session_state["session"] is None or expired:
        if _CURL_CFFI_OK:
            session = CurlSession(impersonate="chrome")
        else:
            session = requests.Session()
            session.headers.update({"User-Agent": _UA})
            logger.warning("mat6tube: curl_cffi not available — falling back to plain requests; "
                           "the CDN download may get blocked (see module docstring).")
        _session_state.update({"session": session, "started_at": now})
    return _session_state["session"]


def _meta_content(html: str, prop: str) -> str | None:
    """Pulls a <meta property="X" content="Y"> or <meta name="X" content="Y">
    value — mat6tube uses both property= and name= across different tags,
    so this checks either rather than assuming one."""
    m = re.search(
        rf'<meta[^>]+(?:property|name)=["\']' + re.escape(prop) + r'["\'][^>]+content=["\']([^"\']*)["\']',
        html, re.IGNORECASE,
    )
    return m.group(1) if m else None


def _sources_from_html(html: str) -> list[dict]:
    """[{"label": "720p", "file": <url>}, ...] from a page's embedded
    `window.playlist = {...}` JSON (the pvvstream.pro player this site
    uses) — see module docstring. Empty list if the page doesn't have
    one, or it's not valid JSON."""
    m = _PLAYLIST_RE.search(html)
    if not m:
        return []
    try:
        playlist = _json.loads(m.group(1))
    except _json.JSONDecodeError:
        return []
    return playlist.get("sources") or []


def _resolve_sources(video_url: str, html: str) -> list[dict]:
    """Real per-resolution sources for this video, in order of where
    NoodleMat-DL looks for them:
      1. `window.playlist = {...}` embedded directly on the watch page.
      2. The site's own "download page" (linked via a `downloadUrl="..."`
         attribute on the watch page) — some videos only expose their
         playlist there, not on the watch page itself.
      3. A bare https://....mp4 URL anywhere in the watch page's HTML, as
         a last-resort single-quality fallback (no real "sources" list to
         pick a label from, so this becomes one "Best available" entry).
    Empty list if none of the three find anything."""
    sources = _sources_from_html(html)
    if sources:
        return sources

    dl_match = _DOWNLOAD_URL_ATTR_RE.search(html)
    if dl_match:
        dl_url = dl_match.group(1)
        if not dl_url.startswith("http"):
            dl_url = f"{BASE_URL}{dl_url}"
        try:
            dl_html = _fetch_html(dl_url, referer=video_url)
            sources = _sources_from_html(dl_html)
            if sources:
                return sources
        except Exception as e:
            logger.debug(f"mat6tube: download-page fallback failed: {e}")

    mp4_match = _DIRECT_MP4_RE.search(html)
    if mp4_match:
        return [{"label": "Best available", "file": mp4_match.group(1)}]

    return []


def get_available_qualities(video_url: str) -> list:
    """Same contract as faphouse_downloader.get_available_qualities() —
    [{"label": "720p", "height": 720, "url": ...}, ...], best-first. See
    module docstring for how this now actually finds the real per-
    resolution ladder instead of guessing one file path."""
    video_url = _normalize_url(video_url)
    video_id = _video_id_from_url(video_url)
    if not video_id:
        raise RuntimeError(f"Couldn't find a watch id in {video_url!r}")

    html = _fetch_html(video_url)
    sources = _resolve_sources(video_url, html)

    if not sources:
        # Last-resort: the old meta-tag/guessed-pattern this module used
        # to rely on exclusively — kept as a final fallback (costs
        # nothing extra, `html` is already fetched) rather than removed
        # outright, in case some video genuinely only exposes this.
        content_url = _meta_content(html, "ya:ovs:content_url") or f"{BASE_URL}/videofile/{video_id}.mp4"
        sources = [{"label": "Best available", "file": content_url}]

    variants = []
    seen_urls = set()
    for s in sources:
        file_url = s.get("file") or s.get("url")
        if not file_url or file_url in seen_urls:
            continue
        seen_urls.add(file_url)
        label = str(s.get("label") or "Best available")
        height = None
        hm = re.match(r"(\d{3,4})", label)
        if hm:
            height = int(hm.group(1))
            label = f"{height}p"
        variants.append({"label": label, "height": height, "url": file_url})

    if not variants:
        raise RuntimeError(f"mat6tube: couldn't find any video source for {video_url}")

    variants.sort(key=lambda v: v["height"] or 0, reverse=True)
    return variants


def download_video(video_url: str, out_path: str, on_progress=None, stream_url: str = None) -> tuple[str, float]:
    """Same contract as faphouse_downloader.download_video() — a plain
    progressive-MP4 download, now through the impersonated CDN session
    (see module docstring) instead of plain requests, which the CDN was
    silently blocking."""
    video_url = _normalize_url(video_url)
    target_url = stream_url or get_available_qualities(video_url)[0]["url"]
    video_id = _video_id_from_url(video_url)
    session = _cdn_session()

    start_time = time.time()

    def _fresh_source() -> str | None:
        try:
            html = _fetch_html(video_url, force=True)
            fresh_sources = _resolve_sources(video_url, html)
            if fresh_sources:
                return fresh_sources[0].get("file") or fresh_sources[0].get("url")
        except Exception as e:
            logger.debug(f"mat6tube: fresh re-resolve failed: {e}")
        return None

    r = session.get(target_url, stream=True, timeout=30, headers={"Referer": video_url})
    try:
        if r.status_code == 404:
            fresh = video_id and _fresh_source()
            if fresh and fresh != target_url:
                r.close()
                r = session.get(fresh, stream=True, timeout=30, headers={"Referer": video_url})
                target_url = fresh
        r.raise_for_status()
        total = int(r.headers.get("Content-Length", 0))
        downloaded = 0
        with open(out_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)
                if on_progress:
                    elapsed = time.time() - start_time
                    pct = (downloaded / total * 100) if total else None
                    on_progress({
                        "pct": pct,
                        "downloaded_bytes": downloaded,
                        "speed_bytes_s": downloaded / elapsed if elapsed > 0 else 0,
                        "eta_s": ((total - downloaded) / (downloaded / elapsed)) if (total and downloaded and elapsed > 0) else None,
                        "elapsed_s": elapsed,
                        "duration_s": 0,
                    })
    finally:
        r.close()

    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        raise RuntimeError("Download finished but the output file is missing/empty.")
    return out_path, time.time() - start_time


def get_page_meta(video_url: str) -> dict:
    """Same contract as faphouse_downloader.get_page_meta(). Everything
    here comes straight off the watch page's own meta tags — no
    secondary API call needed, unlike most other backends in this
    codebase."""
    video_url = _normalize_url(video_url)
    try:
        html = _fetch_html(video_url)
    except Exception as e:
        logger.warning(f"get_page_meta failed for {video_url}: {e}")
        return {"title": None, "author": None, "duration": None, "poster_url": None,
                "view_count": None, "like_count": None, "comment_count": None, "upload_date": None}

    duration_raw = _meta_content(html, "video:duration")
    try:
        duration = int(duration_raw) if duration_raw else None
    except ValueError:
        duration = None

    views_raw = _meta_content(html, "ya:ovs:views_total")
    likes_raw = _meta_content(html, "ya:ovs:likes")

    return {
        "title": _meta_content(html, "og:title"),
        "author": _meta_content(html, "video:actor"),
        "duration": duration,
        "poster_url": _meta_content(html, "og:image"),
        "view_count": int(views_raw) if views_raw and views_raw.isdigit() else None,
        "like_count": int(likes_raw) if likes_raw and likes_raw.isdigit() else None,
        "comment_count": None,
        "upload_date": _meta_content(html, "ya:ovs:upload_date"),
    }
