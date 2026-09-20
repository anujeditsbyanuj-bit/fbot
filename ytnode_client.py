"""
Python client for ytnode/server.js — a local ytdl-core-based YouTube
downloader that needs no cookies or PO token at all, unlike
ytdlp_downloader.py's yt-dlp + bgutil-PO-token-server path.

This is NOT a wholesale replacement for that path — ytdl-core is a
separate library fighting the exact same cat-and-mouse game against
YouTube's own blocking measures, so it isn't guaranteed to work better,
just DIFFERENTLY. It's wired in as a FALLBACK inside
ytdlp_downloader.py's get_available_qualities(): only tried when yt-dlp's
own result looks degraded (fewer than a real quality ladder's worth of
distinct heights — see _YTNODE_FALLBACK_THRESHOLD there), and only
actually used if it turns up MORE options than yt-dlp did.

Same [{"label", "height", "url"}] contract as
ytdlp_downloader.get_available_qualities() — "url" here is an opaque
"ytnode:<itag>" string (or "ytnode:auto"), meaningful only to this
module's own download_video(), the same "opaque id, not a real URL"
convention ytdlp_downloader.py already uses for its own format_ids.
ytdlp_downloader.download_video() checks for the "ytnode:" prefix and
routes to this module's download_video() instead of its own when it
sees one — see that function's own top for the dispatch.
"""

import logging
import os
import time

import requests

logger = logging.getLogger("faphouse_bot")

YTNODE_PORT = int(os.getenv("YTNODE_PORT", "4417"))
YTNODE_BASE = f"http://127.0.0.1:{YTNODE_PORT}"

URL_PREFIX = "ytnode:"


def is_ready() -> bool:
    """True once the local ytnode HTTP server is up and accepting
    connections — same idea as pot_provider.is_ready(), just for this
    separate service."""
    try:
        r = requests.get(f"{YTNODE_BASE}/health", timeout=2)
        return r.status_code == 200
    except requests.exceptions.RequestException:
        return False


def get_available_qualities(video_url: str) -> list:
    """Raises on any failure — callers (ytdlp_downloader.py) are
    expected to catch and just keep whatever yt-dlp already gave them,
    since this is a best-effort fallback, not something that should ever
    make a request fail outright on its own."""
    r = requests.get(f"{YTNODE_BASE}/formats", params={"url": video_url}, timeout=25)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"ytnode: {data['error']}")

    by_height: dict[int, dict] = {}
    for f in data.get("formats", []):
        height = f.get("height")
        if not height:
            continue
        current = by_height.get(height)
        # Prefer a progressive (already-has-audio) format at this height
        # when one exists — it skips the server-side mux step entirely.
        if current is None or (f.get("hasAudio") and not current.get("hasAudio")):
            by_height[height] = f

    if not by_height:
        raise RuntimeError("ytnode: no video formats with a resolvable height")

    variants = [
        {"label": f"{h}p", "height": h, "url": f"{URL_PREFIX}{f['itag']}"}
        for h, f in by_height.items()
    ]
    variants.sort(key=lambda v: v["height"], reverse=True)
    return [{"label": "Auto (Best)", "height": None, "url": f"{URL_PREFIX}auto"}] + variants


def download_video(video_url: str, out_path: str, on_progress=None, stream_url: str = None) -> tuple[str, float]:
    """stream_url: a "ytnode:<itag>" or "ytnode:auto" string from
    get_available_qualities() above (the URL_PREFIX gets stripped by
    ytdlp_downloader.download_video() before calling this — see its
    dispatch check)."""
    start_time = time.time()
    params = {"url": video_url}
    if stream_url and stream_url != "auto":
        params["itag"] = stream_url

    with requests.get(f"{YTNODE_BASE}/download", params=params, stream=True, timeout=(15, 1800)) as r:
        if r.status_code != 200:
            try:
                err = r.json().get("error", r.text[:300])
            except Exception:
                err = r.text[:300]
            raise RuntimeError(f"ytnode download failed: {err}")

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
                    on_progress({
                        "pct": (downloaded / total * 100) if total else None,
                        "downloaded_bytes": downloaded,
                        "speed_bytes_s": downloaded / elapsed if elapsed > 0 else 0,
                        "eta_s": ((total - downloaded) / (downloaded / elapsed))
                                 if (total and downloaded and elapsed > 0) else None,
                        "elapsed_s": elapsed,
                        "duration_s": 0,
                    })

    if not os.path.exists(out_path) or os.path.getsize(out_path) < 1024:
        raise RuntimeError("ytnode: downloaded file missing/empty")
    return out_path, time.time() - start_time
