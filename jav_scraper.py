"""
jav_scraper.py — javct.net scraper for fbot
════════════════════════════════════════════
Ported from JAV-Video-Scraper-API (VideoScrapingService class), stripped of
FastAPI/Pydantic wrapping — plain dicts instead, matching pornhub_scraper.py/
xhamster_scraper.py style so auto_scraper.py can call it the same way.

New vs old jav_scraper.py:
  + HTTPAdapter retry strategy (3 retries, exponential backoff)
  + preview_images list (gallery/sample images)
  + actors list (separate from actresses)
  + rating field
  + label field (JAV label/studio sub-brand)
  + description (4 fallback extraction patterns)
  + better thumbnail (data-original attr + parent container search)
  + quick=True mode (skip download page — faster info fetch)
  + VideoSummary includes thumbnail (from parent <div>/<article> img)

DOWNLOAD STRATEGY (see auto_scraper.jav_uploader_worker):
  Only StreamWish links are auto-downloadable via yt-dlp.
  All other hosts (Keep2Share, RapidGator, Nitroflare etc.) need a paid
  account — shown as clickable links in an info card instead of
  silently skipping.
"""

import re
import logging
from urllib.parse import urljoin, quote

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

BASE_URL        = "https://javct.net"
REQUEST_TIMEOUT = 20   # seconds
MAX_RETRIES     = 3
RETRY_BACKOFF   = 1.0  # seconds

_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0.0.0 Safari/537.36"
)

# Providers yt-dlp can resolve WITHOUT a premium account
YTDLP_COMPATIBLE_PROVIDERS = {"StreamWish"}

_PROVIDERS_MAP = {
    "keep2share":  "Keep2Share",
    "k2s.cc":      "Keep2Share",
    "nitroflare":  "Nitroflare",
    "rapidgator":  "RapidGator",
    "uploaded":    "Uploaded",
    "turbobit":    "Turbobit",
    "mexashare":   "MexaShare",
    "katfile":     "Katfile",
    "rosefile":    "Rosefile",
    "filefox":     "FileFox",
    "subyshare":   "SubyShare",
    "streamwish":  "StreamWish",
}


# ─────────────────────────────────────────────
#  HTTP SESSION  (retry + headers)
# ─────────────────────────────────────────────
def _make_session() -> requests.Session:
    """Configured session: 3 retries with exponential backoff on 429/5xx."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent":      _UA,
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer":         "https://javct.net/",
    })
    return session


# ─────────────────────────────────────────────
#  PROVIDER / LINK TYPE DETECTION
# ─────────────────────────────────────────────
def _detect_provider(href: str, label: str):
    href_l, label_l = href.lower(), label.lower()
    for key, pname in _PROVIDERS_MAP.items():
        if key in href_l:
            return pname
    if "watch online" in label_l or "stream" in label_l:
        return "Stream"
    if re.search(r"download\s+[a-z]{2}", label, re.I):
        return label
    return None


def _detect_link_type(href: str, label: str) -> str:
    if "watch online" in label.lower() or "stream" in label.lower():
        return "stream"
    return "file_host"


# ─────────────────────────────────────────────
#  DOWNLOAD LINKS PAGE
# ─────────────────────────────────────────────
def _scrape_download_links(session: requests.Session, dl_page_url: str) -> list:
    try:
        r = session.get(dl_page_url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
    except Exception as e:
        logger.warning(f"[jav] download page fetch failed {dl_page_url}: {e}")
        return []

    seen, out = set(), []
    for a in soup.find_all("a", href=True):
        href  = a.get("href", "")
        label = a.get_text(strip=True)
        if not href or href in seen or href.startswith("#") or href == "/":
            continue
        provider = _detect_provider(href, label)
        if provider:
            seen.add(href)
            out.append({
                "provider": provider,
                "url":      href,
                "label":    label,
                "type":     _detect_link_type(href, label),
            })
    return out


# ─────────────────────────────────────────────
#  FULL VIDEO INFO
# ─────────────────────────────────────────────
def get_video_info(url_or_code: str, quick: bool = False) -> dict:
    """
    Returns a dict with all video metadata + download links.

    url_or_code: full URL, or just a code like "IPX-421" / "hz-3490".
    quick=True: skip download page (no file-host links, faster).

    Dict keys:
      url, title, video_code, thumbnail, cover_image, preview_images,
      duration, release_date, studio, label, director, series,
      actresses, actors, genres, tags, description, rating,
      download_links (list of {provider, url, label, type})
    """
    if not url_or_code.startswith("http"):
        url = f"{BASE_URL}/v/{url_or_code.strip().lower()}"
    else:
        url = url_or_code

    session = _make_session()
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        # Site blocked this server IP (403) — return minimal info dict
        # with just the video code so the UI can still show something
        # useful (the /d/ page link) instead of a blank error.
        import re as _re
        m = _re.search(r"/v/([a-z0-9-]+)", url)
        code = m.group(1).upper() if m else None
        logger.warning(f"[jav] page fetch failed for {url}: {e} — returning minimal info")
        return {
            "url": url, "title": code, "video_code": code,
            "thumbnail": None, "cover_image": None, "preview_images": [],
            "duration": None, "release_date": None, "studio": None,
            "label": None, "director": None, "series": None,
            "actresses": [], "actors": [], "genres": [], "tags": [],
            "description": None, "rating": None, "download_links": [],
        }
    soup = BeautifulSoup(r.content, "html.parser")

    info = {
        "url":            url,
        "title":          None,
        "video_code":     None,
        "thumbnail":      None,
        "cover_image":    None,
        "preview_images": [],
        "duration":       None,
        "release_date":   None,
        "studio":         None,
        "label":          None,
        "director":       None,
        "series":         None,
        "actresses":      [],
        "actors":         [],
        "genres":         [],
        "tags":           [],
        "description":    None,
        "rating":         None,
        "download_links": [],
    }

    # ── Code ──────────────────────────────────────────────────────────────
    m = re.search(r"/v/([a-z0-9-]+)", url)
    if m:
        info["video_code"] = m.group(1).upper()

    # ── Title ─────────────────────────────────────────────────────────────
    title_tag = (soup.find("h1")
                 or soup.find("meta", property="og:title")
                 or soup.find("title"))
    if title_tag:
        raw = (title_tag.get("content", "") if title_tag.name == "meta"
               else title_tag.get_text(strip=True))
        info["title"] = re.sub(r"\s*[-|]\s*javct\.net.*$", "", raw or "",
                                flags=re.I).strip() or None

    # ── Thumbnail / Cover ─────────────────────────────────────────────────
    img_tag = (soup.find("img", class_=re.compile(r"(video-cover|poster|thumbnail)", re.I))
               or soup.find("meta", property="og:image"))
    if img_tag:
        thumb = (img_tag.get("content") if img_tag.name == "meta"
                 else (img_tag.get("src")
                       or img_tag.get("data-src")
                       or img_tag.get("data-original")))
        if thumb:
            if not thumb.startswith("http"):
                thumb = urljoin(BASE_URL, thumb)
            info["thumbnail"] = thumb

    # Cover image (og:image as fallback)
    og_img = soup.find("meta", property="og:image")
    if og_img and og_img.get("content"):
        info["cover_image"] = og_img["content"]

    # Preview images (gallery/sample)
    for img in soup.find_all("img", src=re.compile(r"(gallery|preview|sample)", re.I)):
        src = (img.get("src") or img.get("data-src") or img.get("data-original") or "")
        if src and src != info["thumbnail"]:
            if not src.startswith("http"):
                src = urljoin(BASE_URL, src)
            info["preview_images"].append(src)

    # ── Duration / Date / Rating ──────────────────────────────────────────
    all_text = soup.get_text()

    for pat in (r"Duration[:\s]+(\d+)\s*min",
                r"時長[:\s]+(\d+)",
                r"(\d{2,3})\s*min"):
        mm = re.search(pat, all_text, re.I)
        if mm:
            info["duration"] = mm.group(1) + " min"
            break

    for pat in (r"release\s+date\s+([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})",
                r"(\d{4}-\d{2}-\d{2})",
                r"([A-Z][a-z]+\.?\s+\d{1,2},?\s+\d{4})"):
        mm = re.search(pat, all_text, re.I)
        if mm:
            info["release_date"] = mm.group(1)
            break

    rating_tag = soup.find(class_=re.compile(r"(rating|score|stars?)", re.I))
    if rating_tag:
        mm = re.search(r"(\d+\.?\d*)", rating_tag.get_text(strip=True))
        if mm:
            info["rating"] = mm.group(1)

    # ── Studio / Label / Director / Series ───────────────────────────────
    def _link_text(pattern: str):
        t = soup.find("a", href=re.compile(pattern, re.I))
        return t.get_text(strip=True) if t else None

    info["studio"]   = _link_text(r"/studio/")
    info["label"]    = _link_text(r"/label/")
    info["director"] = _link_text(r"/director/")
    info["series"]   = _link_text(r"/series/")

    # ── Actresses / Actors / Genres / Tags ───────────────────────────────
    for a in soup.find_all("a", href=re.compile(r"/actress/", re.I)):
        n = a.get_text(strip=True)
        if n and n not in info["actresses"]:
            info["actresses"].append(n)

    # Fallback: extract actress from title if none found
    if not info["actresses"] and info["title"]:
        mm = re.search(r"-\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)\s*$", info["title"])
        if mm:
            info["actresses"].append(mm.group(1).strip())

    for a in soup.find_all("a", href=re.compile(r"/actor/", re.I)):
        n = a.get_text(strip=True)
        if n and n not in info["actors"]:
            info["actors"].append(n)

    for a in soup.find_all("a", href=re.compile(r"/genre/", re.I)):
        g = a.get_text(strip=True)
        if g and g not in info["genres"]:
            info["genres"].append(g)

    for a in soup.find_all("a", href=re.compile(r"/tag/", re.I)):
        t = a.get_text(strip=True)
        if t and t not in info["tags"]:
            info["tags"].append(t)

    # ── Description (4 fallback patterns) ────────────────────────────────
    for tag, attrs in [
        ("meta",  {"property": "og:description"}),
        ("meta",  {"name": "description"}),
        ("div",   {"class": re.compile(r"(description|content|summary)", re.I)}),
        ("p",     {"class": re.compile(r"(description|content|summary)", re.I)}),
    ]:
        dt = soup.find(tag, attrs)
        if dt:
            text = (dt.get("content", "").strip() if tag == "meta"
                    else dt.get_text(strip=True))
            if text and len(text) > 20:
                info["description"] = text
                break

    # Final fallback: regex on raw text
    if not info["description"] and info["video_code"]:
        mm = re.search(
            rf"{re.escape(info['video_code'])}\s*[-–]\s*(.+?)(?:\.|Rate and discuss|$)",
            all_text, re.DOTALL | re.I
        )
        if mm:
            candidate = " ".join(mm.group(1).split())
            if len(candidate) > 50:
                info["description"] = candidate

    # ── Download links ────────────────────────────────────────────────────
    if not quick:
        # Magnet links directly on the page
        for a in soup.find_all("a", href=re.compile(r"magnet:\?")):
            href = a.get("href")
            if href:
                info["download_links"].append({
                    "provider": "Magnet",
                    "url":      href,
                    "label":    a.get_text(strip=True) or "Magnet Link",
                    "type":     "magnet",
                })

        # /d/ download page
        dl_a = next((a for a in soup.find_all("a", href=re.compile(r"/d/"))
                     if a.get("href")), None)
        if dl_a:
            dl_href = dl_a["href"]
            if not dl_href.startswith("http"):
                dl_href = urljoin(BASE_URL, dl_href)
            info["download_links"].extend(_scrape_download_links(session, dl_href))

    return info


# ─────────────────────────────────────────────
#  SEARCH
# ─────────────────────────────────────────────
def search_videos(query: str, page: int = 1, limit: int = 20) -> list:
    """
    Search javct.net. Returns list of summary dicts:
      [{url, code, title, thumbnail}, ...]
    """
    session = _make_session()
    url = f"{BASE_URL}/search?q={quote(query)}&page={page}"
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "html.parser")
    return _extract_summaries(soup, limit)


# ─────────────────────────────────────────────
#  LATEST VIDEOS
# ─────────────────────────────────────────────
def get_latest_videos(page: int = 1, limit: int = 20) -> list:
    """
    Latest videos from javct.net homepage.
    Returns list of summary dicts: [{url, code, title, thumbnail}, ...]
    """
    session = _make_session()
    url = f"{BASE_URL}/?page={page}" if page > 1 else BASE_URL
    r = session.get(url, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    soup = BeautifulSoup(r.content, "html.parser")
    return _extract_summaries(soup, limit)


# ─────────────────────────────────────────────
#  ACTRESS PAGE
# ─────────────────────────────────────────────
def get_actress_videos(actress_slug: str, page: int = 1, limit: int = 20) -> list:
    """
    Videos from an actress page: javct.net/actress/<slug>
    actress_slug: e.g. "yui-hatano" (auto-slugified from name)
    """
    session = _make_session()
    url = f"{BASE_URL}/actress/{actress_slug}?page={page}" if page > 1 else f"{BASE_URL}/actress/{actress_slug}"
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        soup = BeautifulSoup(r.content, "html.parser")
        return _extract_summaries(soup, limit)
    except Exception as e:
        logger.warning(f"[jav] actress page fetch failed ({actress_slug} p{page}): {e}")
        return []


def slugify_actress(name: str) -> str:
    """'Yui Hatano' → 'yui-hatano'"""
    return re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")


# ─────────────────────────────────────────────
#  SUMMARY EXTRACTOR  (shared by search + latest + actress)
# ─────────────────────────────────────────────
def _extract_summaries(soup: BeautifulSoup, limit: int) -> list:
    out, seen = [], set()
    for a in soup.find_all("a", href=re.compile(r"/v/[a-z0-9-]+")):
        href = a.get("href")
        if not href or href in seen:
            continue
        seen.add(href)

        mm = re.search(r"/v/([a-z0-9-]+)", href)
        code = mm.group(1).upper() if mm else None
        title = (a.get("title", "").strip()
                 or a.get_text(strip=True)
                 or code or "")

        # Thumbnail: check parent container for <img>
        thumbnail = None
        parent = a.find_parent(["div", "article", "li"])
        if parent:
            img = parent.find("img")
            if img:
                thumbnail = (img.get("src")
                             or img.get("data-src")
                             or img.get("data-original") or "")
                if thumbnail and not thumbnail.startswith("http"):
                    thumbnail = urljoin(BASE_URL, thumbnail)
                thumbnail = thumbnail or None

        out.append({
            "url":       urljoin(BASE_URL, href),
            "code":      code,
            "title":     title,
            "thumbnail": thumbnail,
        })
        if len(out) >= limit:
            break
    return out


# ─────────────────────────────────────────────
#  PICK DOWNLOADABLE LINK
# ─────────────────────────────────────────────
def pick_downloadable_link(download_links: list) -> dict | None:
    """
    Returns the first link auto-downloadable via yt-dlp (StreamWish only),
    or None if every link requires a premium account / torrent client.
    """
    for lnk in download_links:
        if lnk.get("provider") in YTDLP_COMPATIBLE_PROVIDERS:
            return lnk
    return None


# ─────────────────────────────────────────────
#  STANDARD BACKEND INTERFACE
#  (matches ytdlp_downloader / faphouse_downloader contract)
# ─────────────────────────────────────────────

_JAVCT_HOST_RE = re.compile(r"(?:^|\.)javct\.net$", re.IGNORECASE)
_VIDEO_URL_RE  = re.compile(
    r"https?://(?:www\.)?javct\.net/v/[a-z0-9][a-z0-9-]*",
    re.IGNORECASE,
)


def is_javct_link(url: str) -> bool:
    """True for any javct.net/v/<code> video page URL."""
    try:
        from urllib.parse import urlparse as _up
        host = _up(url).netloc.lower().split("@")[-1].split(":")[0]
        path = _up(url).path
        return bool(_JAVCT_HOST_RE.search(host) and path.startswith("/v/"))
    except Exception:
        return False


def extract_javct_links(text: str) -> list:
    """Pull all javct.net/v/... URLs out of a block of text."""
    if not text:
        return []
    seen, out = set(), []
    for match in _VIDEO_URL_RE.findall(text):
        url = match.rstrip(").,!?>'\"")
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def get_page_meta(video_url: str) -> dict:
    """
    Return metadata for caption building.
    Maps jav_scraper fields → standard caption dict keys.
    """
    empty = {
        "title": None, "author": None, "author_url": None,
        "duration": None, "poster_url": None,
        "views": None, "upload_date": None, "likes": None,
        "comments": None, "category": None,
        "description": None, "site_name": "JAVCT",
    }
    try:
        info = get_video_info(video_url, quick=True)
        # Duration: "90 min" → 5400 (seconds)
        duration_s = None
        if info.get("duration"):
            m = re.search(r"(\d+)", str(info["duration"]))
            if m:
                duration_s = int(m.group(1)) * 60

        author = ", ".join(info.get("actresses") or []) or None

        return {
            "title":       info.get("title"),
            "author":      author,
            "author_url":  None,
            "duration":    duration_s,
            "poster_url":  info.get("cover_image") or info.get("thumbnail"),
            "views":       None,
            "upload_date": info.get("release_date"),
            "likes":       None,
            "comments":    None,
            "category":    ", ".join(info.get("genres") or []) or None,
            "description": info.get("description"),
            "site_name":   "JAVCT",
        }
    except Exception as e:
        logger.warning(f"[jav] get_page_meta failed for {video_url}: {e}")
        return empty


def get_available_qualities(video_url: str) -> list:
    """
    Fetch the download page and return a quality list.

    Strategy (in order):
    1. Try yt-dlp directly on the javct.net page — some JAV sites embed
       a StreamWish/Doodstream player that yt-dlp can extract without any
       scraping, and this works even when Render's IP is 403'd by the site.
    2. Scrape /d/ page for StreamWish links → hand to yt-dlp.
    3. File-host only (K2S, RapidGator etc.) → info card with /d/ page link.
    4. Everything failed → info card with just the /d/ page link.
    """
    import yt_dlp as _ytdl

    # ── javxxx.me: scrape stream/embed links directly ─────────────────────
    if is_javxxx_link(video_url):
        try:
            info = get_javxxx_video_info(video_url)
        except Exception as e:
            logger.warning(f"[javxxx] get_javxxx_video_info failed: {e}")
            info = {"stream_links": [], "download_links": []}

        variants = []
        # Prefer auto-downloadable embeds (StreamWish, DoodStream etc.)
        for lnk in (info.get("stream_links") or []) + (info.get("download_links") or []):
            provider = lnk.get("provider", "")
            if provider in YTDLP_COMPATIBLE_PROVIDERS or provider in {
                "DoodStream", "MixDrop", "UpStream", "FileLions"
            }:
                variants.append({
                    "label":     provider,
                    "url":       lnk["url"],
                    "_type":     "streamwish",
                    "_jav_info": info,
                })

        if variants:
            return variants

        # No auto-downloadable links → info card
        return [{
            "label":     "📋 Info Card (No Direct Download)",
            "url":       None,
            "_type":     "info_card",
            "_jav_info": info,
        }]

    # ── Step 1: Try yt-dlp directly on the javct.net video page ──────────
    try:
        ydl_opts = {
            "quiet": True, "no_warnings": True,
            "nocheckcertificate": True,
            "socket_timeout": 15,
            "http_headers": {"User-Agent": _UA, "Referer": BASE_URL + "/"},
        }
        with _ytdl.YoutubeDL(ydl_opts) as ydl:
            info_dict = ydl.extract_info(video_url, download=False)
        formats = info_dict.get("formats") or []
        # Filter to real video formats with height
        video_fmts = sorted(
            [f for f in formats if f.get("height") and f.get("vcodec") != "none"],
            key=lambda f: f.get("height", 0), reverse=True,
        )
        if video_fmts:
            logger.info(f"[jav] yt-dlp found {len(video_fmts)} formats directly on {video_url}")
            variants = []
            seen_h = set()
            for f in video_fmts:
                h = f["height"]
                if h in seen_h:
                    continue
                seen_h.add(h)
                variants.append({
                    "label":     f"{h}p",
                    "url":       video_url,   # download_video uses format_id via yt-dlp
                    "_format_id": f.get("format_id"),
                    "_type":     "ytdlp_direct",
                })
            return variants
    except Exception as e:
        logger.info(f"[jav] yt-dlp direct extract failed ({e}), trying scrape...")

    # ── Step 2: Scrape for StreamWish links ───────────────────────────────
    try:
        info = get_video_info(video_url, quick=False)
    except Exception as e:
        logger.warning(f"[jav] get_video_info failed: {e}")
        info = {"download_links": [], "video_code": None}

    dl_links = info.get("download_links") or []
    streamwish = [l for l in dl_links if l.get("provider") in YTDLP_COMPATIBLE_PROVIDERS]
    if streamwish:
        variants = []
        for lnk in streamwish:
            variants.append({
                "label":     lnk["provider"],
                "url":       lnk["url"],
                "_type":     "streamwish",
                "_jav_info": info,
            })
        return variants

    # ── Step 3/4: Info card (file-host links or nothing) ─────────────────
    return [{
        "label":     "📋 Info Card (No Direct Download)",
        "url":       None,
        "_type":     "info_card",
        "_jav_info": info,
    }]


def download_video(video_url: str, out_path: str,
                   on_progress=None, stream_url: str = None,
                   _format_id: str = None) -> str:
    """
    Download a javct.net video.

    stream_url: StreamWish URL or javct.net direct URL (from get_available_qualities).
    _format_id: yt-dlp format id (for ytdlp_direct type — picks exact quality).

    Hands everything to yt-dlp — same as ytdlp_downloader.download_video().
    Raises RuntimeError if no downloadable link is available.
    """
    import yt_dlp as ytdl
    import time as _time

    target_url = stream_url or video_url

    # FIX: javxxx.me is NOT a yt-dlp supported site — passing the page URL
    # directly produces "ERROR: Unsupported URL" with noisy ANSI color codes.
    # This happens when stream_url is None (info-card path) and target_url
    # falls back to the javxxx page URL. Raise a clean error instead.
    if is_javxxx_link(target_url) and not stream_url:
        raise RuntimeError(
            "No auto-downloadable stream found for this javxxx.me video.\n"
            "Use the info card to access the available file-host links."
        )

    if not target_url:
        raise RuntimeError(
            "No direct download link available for this JAV video.\n"
            "Use /jav <code> to see the info card with all available links."
        )

    start = _time.time()
    outtmpl = out_path if out_path.endswith(".%(ext)s") else out_path

    # Immediate connecting callback — yt-dlp can take 10-20s to resolve
    # StreamWish before the first progress hook fires.
    if on_progress:
        on_progress({
            "pct": 0, "downloaded_bytes": 0, "speed_bytes_s": 0,
            "eta_s": 0, "elapsed_s": 0, "duration_s": None, "connecting": True,
        })

    _last_hook_fire = 0.0

    def _hook(d):
        nonlocal _last_hook_fire
        if on_progress is None:
            return
        status = d.get("status", "")
        if status == "downloading":
            now        = _time.time()
            downloaded = d.get("downloaded_bytes") or 0
            total      = d.get("total_bytes") or d.get("total_bytes_estimate") or 0
            pct        = (downloaded / total * 100) if total else None
            elapsed    = now - start
            speed      = d.get("speed") or (downloaded / elapsed if elapsed > 0 else 0)
            eta        = d.get("eta") or 0
            # Throttle to max 1 call/sec — hooks can fire many times/sec
            if (now - _last_hook_fire) >= 1.0:
                _last_hook_fire = now
                on_progress({
                    "pct":              pct,
                    "downloaded_bytes": downloaded,
                    "speed_bytes_s":    speed,
                    "eta_s":            eta,
                    "elapsed_s":        elapsed,
                    "duration_s":       None,
                    "connecting":       False,
                })

    ydl_opts = {
        "outtmpl":                       outtmpl,
        "quiet":                         True,
        "no_warnings":                   True,
        "nocheckcertificate":            True,
        "concurrent_fragment_downloads": 4,
        "socket_timeout":                15,
        "progress_hooks":                [_hook],
        "http_headers": {
            "User-Agent": _UA,
            "Referer":    "https://javct.net/",
        },
    }
    # If a specific format_id was selected (ytdlp_direct quality picker),
    # tell yt-dlp to download exactly that quality + best audio
    if _format_id:
        ydl_opts["format"] = f"{_format_id}+bestaudio/best"

    with ytdl.YoutubeDL(ydl_opts) as ydl:
        ydl.download([target_url])

    # yt-dlp picks the actual extension — find what it wrote
    import glob as _glob
    base = re.sub(r"\.%(ext)s$", "", outtmpl)
    candidates = _glob.glob(base + ".*")
    if candidates:
        return candidates[0]
    if os.path.exists(out_path):
        return out_path
    raise RuntimeError(f"yt-dlp finished but no output file found at {out_path}")


import os  # already imported at module top in the original, but guarding here


# ═══════════════════════════════════════════════════════════════════════
#  JAVXXX.ME  SUPPORT
# ═══════════════════════════════════════════════════════════════════════

JAVXXX_BASE = "https://www.javxxx.me"

_JAVXXX_HOST_RE = re.compile(r"(?:^|\.)javxxx\.me$", re.IGNORECASE)
_JAVXXX_URL_RE  = re.compile(
    r"https?://(?:www\.)?javxxx\.me/[^\s\"'<>]+", re.IGNORECASE
)


def is_javxxx_link(url: str) -> bool:
    """True for any javxxx.me video page URL."""
    try:
        from urllib.parse import urlparse as _up
        host = _up(url).netloc.lower().split("@")[-1].split(":")[0]
        path = _up(url).path
        return bool(_JAVXXX_HOST_RE.search(host) and len(path) > 3)
    except Exception:
        return False


def extract_javxxx_links(text: str) -> list:
    """Pull all javxxx.me URLs out of a block of text."""
    if not text:
        return []
    seen, out = set(), []
    for match in _JAVXXX_URL_RE.findall(text):
        url = match.rstrip(").,!?>'\"")
        if url not in seen:
            seen.add(url)
            out.append(url)
    return out


def get_javxxx_video_info(url: str) -> dict:
    """
    Scrape a javxxx.me video page and return metadata + stream/download links.

    Returns dict with keys:
      url, title, video_code, thumbnail, duration, release_date,
      studio, actresses, genres, description,
      stream_links (list of {provider, url, label}),
      download_links (list of {provider, url, label})
    """
    session = _make_session()
    session.headers["Referer"] = JAVXXX_BASE + "/"

    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
    except Exception as e:
        logger.warning(f"[javxxx] page fetch failed for {url}: {e}")
        return {"url": url, "title": None, "error": str(e),
                "stream_links": [], "download_links": []}

    soup = BeautifulSoup(r.text, "html.parser")
    info: dict = {"url": url, "stream_links": [], "download_links": []}

    # ── Title ─────────────────────────────────────────────────────────
    h1 = soup.find("h1")
    title = h1.get_text(strip=True) if h1 else None
    if not title:
        og = soup.find("meta", property="og:title")
        title = og["content"].strip() if og and og.get("content") else None
    info["title"] = title

    # ── Video code ────────────────────────────────────────────────────
    code_m = re.search(r"\b([A-Z]{2,6}-\d{2,5})\b", title or url, re.IGNORECASE)
    info["video_code"] = code_m.group(1).upper() if code_m else None

    # ── Thumbnail ─────────────────────────────────────────────────────
    og_img = soup.find("meta", property="og:image")
    info["thumbnail"] = og_img["content"].strip() if og_img and og_img.get("content") else None
    if not info["thumbnail"]:
        img = soup.find("img", class_=re.compile(r"thumb|poster|cover", re.I))
        if img:
            info["thumbnail"] = urljoin(url, img.get("src") or img.get("data-src", ""))

    # ── Meta fields ───────────────────────────────────────────────────
    info["duration"]     = None
    info["release_date"] = None
    info["studio"]       = None
    info["actresses"]    = []
    info["genres"]       = []
    info["description"]  = None

    # Look for common meta containers
    for row in soup.find_all(["li", "div", "p", "span"]):
        text_raw = row.get_text(" ", strip=True)

        if not info["duration"] and re.search(r"duration|time|length", text_raw, re.I):
            dm = re.search(r"(\d+:\d{2}(?::\d{2})?)", text_raw)
            if dm:
                info["duration"] = dm.group(1)

        if not info["release_date"] and re.search(r"release|date|publish", text_raw, re.I):
            dm = re.search(r"(\d{4}[-/]\d{2}[-/]\d{2})", text_raw)
            if dm:
                info["release_date"] = dm.group(1)

        if not info["studio"] and re.search(r"studio|maker|label|producer", text_raw, re.I):
            a = row.find("a")
            if a:
                info["studio"] = a.get_text(strip=True)

        if re.search(r"actress|cast|model|star", text_raw, re.I):
            for a in row.find_all("a"):
                name = a.get_text(strip=True)
                if name and name not in info["actresses"]:
                    info["actresses"].append(name)

        if re.search(r"genre|categor|tag", text_raw, re.I):
            for a in row.find_all("a"):
                g = a.get_text(strip=True)
                if g and g not in info["genres"]:
                    info["genres"].append(g)

    # ── Description ───────────────────────────────────────────────────
    for sel in [".description", ".summary", "#description", "article p"]:
        el = soup.select_one(sel)
        if el:
            info["description"] = el.get_text(" ", strip=True)[:500]
            break

    # ── Stream / embed links (iframes) ────────────────────────────────
    for iframe in soup.find_all("iframe"):
        src = iframe.get("src") or iframe.get("data-src", "")
        if src and src.startswith("http"):
            provider = _guess_provider(src)
            info["stream_links"].append({
                "provider": provider,
                "url":      src,
                "label":    f"▶ {provider}",
                "type":     "stream",
            })

    # ── Download links ────────────────────────────────────────────────
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if not href.startswith("http"):
            href = urljoin(url, href)
        label = a.get_text(strip=True) or ""
        if re.search(r"download|mega|drive|rapidgator|keep2|nitroflare|katfile|rosefile|doodstream|streamwish", href + label, re.I):
            provider = _guess_provider(href)
            info["download_links"].append({
                "provider": provider,
                "url":      href,
                "label":    label or provider,
                "type":     "download",
            })

    # Deduplicate
    seen_urls = set()
    for key in ("stream_links", "download_links"):
        deduped = []
        for item in info[key]:
            if item["url"] not in seen_urls:
                seen_urls.add(item["url"])
                deduped.append(item)
        info[key] = deduped

    logger.info(f"[javxxx] scraped {url}: title={info['title']!r} streams={len(info['stream_links'])} dl={len(info['download_links'])}")
    return info


def _guess_provider(url: str) -> str:
    """Guess provider name from URL."""
    url_l = url.lower()
    for kw, name in [
        ("streamwish", "StreamWish"), ("doodstream", "DoodStream"),
        ("dood.", "DoodStream"), ("mega.nz", "Mega"),
        ("drive.google", "Google Drive"), ("rapidgator", "RapidGator"),
        ("keep2share", "Keep2Share"), ("k2s.cc", "Keep2Share"),
        ("nitroflare", "Nitroflare"), ("katfile", "Katfile"),
        ("rosefile", "RoseFile"), ("filelions", "FileLions"),
        ("mixdrop", "MixDrop"), ("upstream", "UpStream"),
    ]:
        if kw in url_l:
            return name
    return "Direct"
