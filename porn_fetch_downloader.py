"""
xfreehd.com downloader, built on EchterAlsFake's xfreehd_api package.

The other 6 sites this module used to also cover (xnxx, xvideos,
xhamster, spankbang, youporn, beeg) — plus eporner/pornhub before that —
have all moved to ytdlp_downloader.py, which uses real yt-dlp's own
dedicated, actively-maintained extractors instead. xfreehd is the one
site with no yt-dlp extractor at all (confirmed via
yt_dlp.extractor.gen_extractors() — not present), so it's the only one
still going through this EchterAlsFake-package route.

Calling convention (video.download(configuration=<DownloadConfigRAW>))
confirmed by directly installing and introspecting xfreehd_api's actual
signature — not guessed from docs, which is what caused several rounds
of wrong assumptions for the sites that used to be registered here too.

If xfreehd.com is unreliable, that's expected to be watched — this is
the last site still on a comparatively less battle-tested package
compared to the yt-dlp-backed sites.

Same function contract as faphouse_downloader.py / fpo_downloader.py, so
main.py's _downloader_for() dispatch doesn't need to know which backend
it's calling:
  is_supported_link(url) -> bool
  extract_supported_links(text) -> list[str]
  get_available_qualities(video_url) -> [{"label","height","url"}, ...] best-first
  download_video(video_url, out_path, on_progress=None, stream_url=None) -> (out_path, elapsed_s)
  get_page_meta(video_url) -> {"title","author","duration","poster_url"}
"""

import asyncio
import importlib
import inspect
import logging
import os
import re
import time
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

try:
    from base_api.modules.config import DownloadConfigHLS, DownloadConfigRAW
except Exception as e:
    DownloadConfigHLS = None
    DownloadConfigRAW = None
    logger.error(f"eaf_base_api isn't installed (or its layout changed again) — porn_fetch_downloader is disabled: {e}")

try:
    from config import PROXY_URL, PROXY_AUTH
except Exception:
    PROXY_URL, PROXY_AUTH = None, None


SITE_REGISTRY = {
    "xfreehd": {
        "host_re": re.compile(r"(?:^|\.)xfreehd\.[a-z.]{2,}$", re.IGNORECASE),
        "package": "xfreehd_api", "class_name": "Client",
        "download_style": "raw_config", "config_kwarg": "configuration",
    },
}

# Fixed quality menu offered for every link on every site — none of
# these packages expose a per-video quality list to enumerate (only a
# fixed set of accepted labels each resolves internally to whatever the
# real stream offers), so this is the same menu every time.
_QUALITY_MENU = [
    {"label": "Auto (Best)", "height": None, "url": "best"},
    {"label": "1080p", "height": 1080, "url": "1080p"},
    {"label": "720p", "height": 720, "url": "720p"},
    {"label": "480p", "height": 480, "url": "480p"},
    {"label": "360p", "height": 360, "url": "360p"},
]

_clients = {}          # site key -> live Client instance, built lazily
_broken_sites = {}      # site key -> import/init error, so it's only logged once


def _client_for_site(site_key: str):
    if site_key in _clients:
        return _clients[site_key]
    if site_key in _broken_sites:
        raise RuntimeError(_broken_sites[site_key])

    entry = SITE_REGISTRY[site_key]
    try:
        module = importlib.import_module(entry["package"])
        client_cls = getattr(module, entry["class_name"])
        client = client_cls()  # CONFIRMED: no core= argument on any of the 9
    except Exception as e:
        msg = (f"{entry['package']} isn't installed/importable for {site_key} "
               f"— add it to requirements.txt ({e})")
        _broken_sites[site_key] = msg
        raise RuntimeError(msg)

    # Optional proxy (PROXY_URL in config.py) — several of these sites'
    # Cloudflare/anti-bot protection blocks datacenter IPs outright
    # (HTTP 403 AccessDeniedError) no matter how browser-like the request
    # looks, which a proxy routes around. Set as a post-construction
    # attribute rather than a constructor kwarg (Client(core=...) isn't
    # accepted the same way on every site's own __init__ signature — see
    # module docstring) — client.core.configuration is a plain mutable
    # RuntimeConfig instance every site's Client builds one of internally
    # regardless of its own constructor's exact shape, confirmed present
    # on all 9 the same way, so setting .proxy on it after the fact works
    # uniformly without needing to match each site's constructor.
    if PROXY_URL:
        try:
            client.core.configuration.proxy = PROXY_URL
            if PROXY_AUTH:
                client.core.configuration.proxy_auth = PROXY_AUTH
        except Exception as e:
            logger.warning(f"Couldn't set proxy for {site_key}: {e}")

    _clients[site_key] = client
    return client


def _site_key_for(url: str) -> str | None:
    try:
        host = urlparse(url).netloc.lower().split("@")[-1].split(":")[0]
    except Exception:
        return None
    for site_key, entry in SITE_REGISTRY.items():
        if entry["host_re"].search(host):
            return site_key
    return None


def is_supported_link(url: str) -> bool:
    return _site_key_for(url) is not None


_URL_RE = re.compile(r"https?://\S+")


def extract_supported_links(text: str) -> list[str]:
    """Same contract as faphouse_downloader.extract_faphouse_links()."""
    if not text:
        return []
    seen, out = set(), []
    for match in _URL_RE.findall(text):
        url = match.rstrip(").,!?>'\"")
        if is_supported_link(url) and url not in seen:
            seen.add(url)
            out.append(url)
    return out


async def _maybe_await(value):
    """Awaits `value` only if it's actually awaitable. Some of these 9
    packages' own docs disagree about whether get_video()/download() are
    async — this makes the caller correct either way instead of having
    to resolve that per-site uncertainty up front (see module docstring)."""
    if inspect.isawaitable(value):
        return await value
    return value


def _normalize_xfreehd_url(video_url: str) -> str:
    """Rewrites xfreehd.com's "beta." subdomain to the canonical www.
    domain. NOTE: this alone turned out NOT to fix the underlying
    'NoneType' object has no attribute 'text' crash — the same error
    happens on www.xfreehd.com too, so xfreehd_api's scraper is broken
    against the site's current markup generally, not specifically on
    the beta redesign as first assumed. Kept anyway since collapsing
    both subdomains to one is still correct on its own merits (avoids
    the Origin/Referer mismatch _align_origin_headers warns about), it
    just isn't the whole fix."""
    parsed = urlparse(video_url)
    host = parsed.netloc.lower()
    if host.startswith("beta."):
        return parsed._replace(netloc="www." + host[len("beta."):]).geturl()
    return video_url


_VIDEO_CACHE = {}  # video_url -> (fetched_at, video_object)
_VIDEO_CACHE_TTL = 300  # seconds -- long enough to cover quality-select -> download, short enough that any resolved stream tokens don't go stale


def _fetch_video(video_url: str):
    """Runs (possibly-async) client.get_video() to completion from plain
    sync code via its own event loop, since callers already run this
    whole module from a worker thread via asyncio.to_thread(), which has
    no loop of its own.

    Cached by video_url for _VIDEO_CACHE_TTL -- get_available_qualities(),
    get_page_meta(), and download_video() all call this for the SAME
    video during one download flow (quality menu -> caption/thumbnail ->
    actual download), so without caching that's the same real network
    fetch happening up to 3 times per download."""
    cached = _VIDEO_CACHE.get(video_url)
    if cached and (time.time() - cached[0]) < _VIDEO_CACHE_TTL:
        return cached[1]

    site_key = _site_key_for(video_url)
    if not site_key:
        raise RuntimeError(f"No registered site for {video_url!r}")
    if site_key == "xfreehd":
        video_url = _normalize_xfreehd_url(video_url)
    client = _client_for_site(site_key)
    _align_origin_headers(client, video_url)

    async def _run():
        return await _maybe_await(client.get_video(video_url))

    try:
        video = asyncio.run(_run())
    except Exception as e:
        # Re-raised with the same "[SiteName] ..." shape yt-dlp's own
        # DownloadError uses, so main.py's _friendly_download_error()
        # recognizes this as a site-side scraper break too, instead of
        # only handling that for the yt-dlp-backed sites. Doesn't change
        # what actually went wrong -- just makes it presentable the same
        # way everywhere.
        raise RuntimeError(f"[{site_key}] {e}") from e

    _VIDEO_CACHE[video_url] = (time.time(), video)
    if len(_VIDEO_CACHE) > 500:  # bound growth on a long-running process
        for k in list(_VIDEO_CACHE.keys())[:100]:
            _VIDEO_CACHE.pop(k, None)
    return video



def _align_origin_headers(client, video_url: str):
    """Several of these packages (confirmed for spankbang_api by reading
    its own source: modules/consts.py + Client.__init__) hardcode their
    session's Origin/Referer headers to that site's main domain (e.g.
    "https://www.spankbang.com") regardless of what URL you actually
    request through it. That's silently wrong for any mirror/alt domain
    of the same site (spankbang.party, faphouse2.com-style mirrors,
    etc.) — the request's Host won't match its own claimed Origin/
    Referer, which is exactly the shape of a forged/spoofed request to
    Cloudflare-style anti-bot protection, and a very plausible reason a
    mirror-domain link gets an HTTP 403 AccessDeniedError a same-site
    canonical-domain link wouldn't.

    Best-effort, silent no-op if this client doesn't expose a plain
    requests/curl_cffi-style session.headers (a dict-like) at
    client.core.session — not every one of the 9 packages necessarily
    has the exact same internals, only spankbang_api's was actually
    read here."""
    try:
        origin = f"{urlparse(video_url).scheme}://{urlparse(video_url).netloc}"
        client.core.session.headers.update({"Origin": origin, "Referer": origin + "/"})
    except Exception as e:
        logger.debug(f"Couldn't align Origin/Referer headers for {video_url}: {e}")


def is_supported_link_or_raise(video_url: str) -> str:
    site_key = _site_key_for(video_url)
    if not site_key:
        raise RuntimeError(f"No registered site for {video_url!r}")
    return site_key


def get_available_qualities(video_url: str) -> list:
    """Same contract as faphouse_downloader.get_available_qualities():
    [{"label": "1080p", "height": 1080, "url": <quality value to pass
    back into download_video's stream_url>}, ...], best-first.

    No network call here — there's no per-video quality list to query
    on any of these 9 packages, every site takes the same fixed quality
    labels — which also means a dead/bad link isn't caught at this step,
    it surfaces at the actual download instead."""
    is_supported_link_or_raise(video_url)
    return list(_QUALITY_MENU)


def _call_download(video, entry: dict, quality: str, out_path: str, callback):
    """Every one of these 7 packages' Video.download() takes ONE config
    object (DownloadConfigHLS or DownloadConfigRAW from
    base_api.modules.config) — confirmed directly against each package's
    actual installed signature via inspect.signature(), not docs (several
    of which turned out to be stale/wrong, which is what broke this
    twice before). Only two things vary per site:
      - which config class (config_kwarg + download_style in
        SITE_REGISTRY route that)
      - spankbang's kwarg is "configuration_hls" instead of the
        "configuration" every other site uses
    """
    style = entry["download_style"]
    config_cls = DownloadConfigHLS if style == "hls_config" else DownloadConfigRAW
    if config_cls is None:
        raise RuntimeError(
            f"base_api.modules.config.{'DownloadConfigHLS' if style == 'hls_config' else 'DownloadConfigRAW'} "
            f"isn't available — eaf_base_api may have changed its layout again."
        )
    try:
        config = config_cls(quality=quality, path=out_path, callback=callback)
    except TypeError:
        # callback= not accepted by this version of the config class —
        # degrade to no live progress rather than fail the download.
        config = config_cls(quality=quality, path=out_path)

    return video.download(**{entry["config_kwarg"]: config})


def download_video(video_url: str, out_path: str, on_progress=None, stream_url=None) -> tuple[str, float]:
    """Same contract as faphouse_downloader.download_video(). stream_url
    here is one of _QUALITY_MENU's "url" values ("best"/"1080p"/etc.)
    from get_available_qualities(), not an actual URL — kept as the same
    parameter name as the other backends so main.py's dispatch doesn't
    need to know or care which backend it's calling."""
    site_key = is_supported_link_or_raise(video_url)
    entry = SITE_REGISTRY[site_key]

    video = _fetch_video(video_url)
    quality = stream_url if stream_url else "best"
    duration_s = getattr(video, "length", None) or 0

    start_time = time.time()

    def _callback(position, total):
        if not on_progress:
            return
        pct = (position / total * 100) if total else None
        elapsed = time.time() - start_time
        on_progress({
            "pct": pct,
            "downloaded_bytes": position,
            "speed_bytes_s": position / elapsed if elapsed > 0 else 0,
            "eta_s": ((total - position) / (position / elapsed)) if (total and position and elapsed > 0) else None,
            "elapsed_s": elapsed,
            "duration_s": duration_s,
        })

    async def _run_download():
        return await _maybe_await(_call_download(video, entry, quality, out_path, _callback))

    try:
        asyncio.run(_run_download())
    except Exception as e:
        # One retry for transient failures (a network blip, a connection
        # reset mid-stream) — not every failure here is a calling-
        # convention bug; some of these packages' own internals are just
        # flaky. A second identical attempt is cheap insurance against
        # exactly that class of one-off error.
        logger.warning(f"{site_key} download failed, retrying once: {e}")
        time.sleep(3)
        asyncio.run(_run_download())

    if not os.path.exists(out_path) or os.path.getsize(out_path) == 0:
        # Some of these packages treat `path` as a DIRECTORY to derive
        # their own filename inside, rather than the literal file path we
        # asked for — check out_path's own directory for anything newly
        # written before concluding the download genuinely failed.
        out_dir = os.path.dirname(out_path) or "."
        candidates = [
            os.path.join(out_dir, f) for f in os.listdir(out_dir)
            if f != os.path.basename(out_path)
            and os.path.getmtime(os.path.join(out_dir, f)) >= start_time
            and os.path.getsize(os.path.join(out_dir, f)) > 0
        ]
        if candidates:
            newest = max(candidates, key=os.path.getmtime)
            os.replace(newest, out_path)
        else:
            raise RuntimeError(
                f"Download finished but the output file is missing/empty ({site_key})."
            )
    return out_path, time.time() - start_time


def get_page_meta(video_url: str) -> dict:
    """Same contract as faphouse.get_page_meta() — used for the upload
    caption/thumbnail. title/author come straight off the Video object
    (author isn't present on every site's Video class — getattr covers
    that); no dedicated poster_url in this interface, so thumbnailing
    falls back to an ffmpeg frame grab same as it would for a site with
    no poster."""
    video = _fetch_video(video_url)
    return {
        "title": getattr(video, "title", None),
        "author": getattr(video, "author", None),
        "duration": getattr(video, "length", None),
        "poster_url": None,
    }
