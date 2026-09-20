"""
YouTube search for fbot.

Commands:
  /search <query>  — search YouTube, paginated results with quality picker
  /yts <query>     — same (short alias)

Plain-text: any message that looks like "yt <q>", "youtube <q>",
"search <q>" or just plain text (after all link extractors fail)
gets routed here by main.py's link_handler via try_plain_text_search().

IMPORTANT: call register(app) from main.py AFTER creating the Client
instance — @Client.on_* class-level decorators never fire on the actual
app instance unless plugins are enabled, so handlers must be registered
on the app object directly.
"""

import asyncio
import logging
import re
import uuid

from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, Message

logger = logging.getLogger("faphouse_bot")

try:
    import yt_dlp as _yt_dlp
except ImportError:
    _yt_dlp = None

# ── Config ────────────────────────────────────────────────────────────────────
SEARCH_CHUNK_SIZE = 30   # results fetched per yt-dlp call
SEARCH_PAGE_SIZE  = 10   # results shown per Telegram page

# ── State: msg_id → {query, results, exhausted} ───────────────────────────────
_SEARCH_CACHE: dict = {}

# URL hint — if text looks like a link, skip search
_URL_HINT = re.compile(r"https?://|www\.|t\.me/|magnet:\?", re.IGNORECASE)

# Natural-language search triggers
_YT_TRIGGER = r"(?:yt|yts|youtube)"
_YT_SEARCH_PATTERNS = [
    re.compile(rf"(?i)^\s*{_YT_TRIGGER}\s+search\s+(.+)$"),
    re.compile(rf"(?i)^\s*search\s+(.+?)\s+on\s+{_YT_TRIGGER}\s*$"),
    re.compile(rf"(?i)^\s*{_YT_TRIGGER}\s+(.+)$"),
    re.compile(rf"(?i)^\s*(.+?)\s+on\s+{_YT_TRIGGER}\s*$"),
    re.compile(r"(?i)^\s*search\s+(.+)$"),
]


# ── Helpers ───────────────────────────────────────────────────────────────────
def _trim_cache(cache: dict, limit: int = 500):
    while len(cache) > limit:
        cache.pop(next(iter(cache)), None)


def _search_youtube_sync(query: str, chunk_size: int = SEARCH_CHUNK_SIZE) -> list:
    """Flat metadata-only search — fast, no per-video fetch."""
    if _yt_dlp is None:
        raise RuntimeError("yt-dlp not installed")
    with _yt_dlp.YoutubeDL({
        "quiet": True, "no_warnings": True,
        "extract_flat": "in_playlist",
        "skip_download": True,
        "default_search": "ytsearch",
    }) as ydl:
        info = ydl.extract_info(f"ytsearch{chunk_size}:{query}", download=False)
    entries = (info or {}).get("entries") or []
    results = []
    for entry in entries:
        if not entry or not entry.get("id"):
            continue
        dur = entry.get("duration")
        dur_str = ""
        if isinstance(dur, (int, float)) and dur > 0:
            m_, s = divmod(int(dur), 60)
            h,  m_ = divmod(m_, 60)
            dur_str = f"{h}:{m_:02d}:{s:02d}" if h else f"{m_:02d}:{s:02d}"
        results.append({
            "id":       entry["id"],
            "title":    (entry.get("title") or "Untitled")[:70],
            "uploader": entry.get("uploader") or entry.get("channel") or "",
            "duration": dur_str,
        })
    return results


def _results_text(query: str, results: list, page: int, exhausted: bool) -> str:
    start = page * SEARCH_PAGE_SIZE
    end   = min(start + SEARCH_PAGE_SIZE, len(results))
    lines = [f"🔍 <b>YouTube Search:</b> <i>{query}</i>\n"]
    for i, r in enumerate(results[start:end], start=start + 1):
        meta = " — ".join(x for x in (r["uploader"], r["duration"]) if x)
        lines.append(f"{i}. {r['title']}" + (f"\n    <i>{meta}</i>" if meta else ""))
    if not (exhausted and end >= len(results)):
        lines.append("\n<i>Tap a number to download.</i>")
    return "\n".join(lines)


def _results_kb(
    make_button, BTN_PRIMARY,
    results: list, page: int, exhausted: bool, cache_key: str
) -> InlineKeyboardMarkup:
    """
    Build the result keyboard.
    cache_key is a short UUID string stored in _SEARCH_CACHE — NOT the
    message ID. Using message IDs as cache keys was the original bug:
    after the search message is edited to show the quality menu, its ID
    doesn't change but the cache slot for that message is logically
    consumed. Using a separate UUID means the cache survives message edits
    and multiple users searching simultaneously never collide.
    """
    start = page * SEARCH_PAGE_SIZE
    end   = min(start + SEARCH_PAGE_SIZE, len(results))

    rows, row = [], []
    for abs_idx in range(start, end):
        row.append(make_button(str(abs_idx + 1), callback_data=f"ytsr:{abs_idx}:{cache_key}", style=BTN_PRIMARY))
        if len(row) == 5:
            rows.append(row); row = []
    if row:
        rows.append(row)

    nav = []
    if page > 0:
        nav.append(make_button("◀️ Prev", callback_data=f"ytsrpg:{page-1}:{cache_key}", style=BTN_PRIMARY))
    nav.append(make_button(f"Page {page+1}", callback_data="ytsr:noop:x", style=BTN_PRIMARY))
    if not exhausted or end < len(results):
        nav.append(make_button("Next ▶️", callback_data=f"ytsrpg:{page+1}:{cache_key}", style=BTN_PRIMARY))
    if nav:
        rows.append(nav)
    return InlineKeyboardMarkup(rows)


# ── Core search flow ──────────────────────────────────────────────────────────
async def _do_search(client: Client, message: Message, query: str,
                     status: Message = None, *, make_button, BTN_PRIMARY):
    if _yt_dlp is None:
        txt = "❌ <b>yt-dlp not installed.</b>"
        try:
            if status:
                return await status.edit_text(txt, parse_mode=ParseMode.HTML)
            return await message.reply_text(txt, parse_mode=ParseMode.HTML)
        except Exception:
            return

    if status is None:
        status = await message.reply_text("🔍 <b>Searching YouTube...</b>", parse_mode=ParseMode.HTML)
    else:
        try:
            await status.edit_text("🔍 <b>Searching YouTube...</b>", parse_mode=ParseMode.HTML)
        except Exception:
            pass  # a FloodWait/etc. here shouldn't stop the search itself from proceeding

    try:
        results = await asyncio.to_thread(_search_youtube_sync, query)
    except Exception as e:
        try:
            return await status.edit_text(
                f"❌ <b>Search failed:</b>\n<code>{e}</code>", parse_mode=ParseMode.HTML
            )
        except Exception:
            return

    if not results:
        try:
            return await status.edit_text(
                f"❌ <b>No results for:</b> <i>{query}</i>", parse_mode=ParseMode.HTML
            )
        except Exception:
            return

    # BUG FIX: use a UUID as cache key, not status.id (message ID).
    # Using message ID meant: once show_quality_menu edited the message,
    # the "back" flow had no stable key to recover search results from.
    # A UUID is stable across all edits of that message.
    cache_key = uuid.uuid4().hex[:12]
    exhausted = len(results) < SEARCH_CHUNK_SIZE
    _SEARCH_CACHE[cache_key] = {"query": query, "results": results, "exhausted": exhausted}
    _trim_cache(_SEARCH_CACHE)

    try:
        await status.edit_text(
            _results_text(query, results, page=0, exhausted=exhausted),
            parse_mode=ParseMode.HTML,
            reply_markup=_results_kb(make_button, BTN_PRIMARY, results, 0, exhausted, cache_key),
        )
    except Exception as e:
        # BUG FIX: this was the one unguarded edit_text() in this function —
        # a FloodWait (or any other edit failure) here used to propagate
        # straight up uncaught. Search results are still cached at this
        # point even if the message itself couldn't be edited, so this at
        # least degrades to "results ready but the message didn't refresh"
        # instead of crashing whatever called _do_search().
        logger.warning(f"[ytsearch] couldn't render results message: {e}")


def _extract_yt_query(text: str) -> str | None:
    stripped = text.strip()
    for pat in _YT_SEARCH_PATTERNS:
        m = pat.match(stripped)
        if m:
            q = m.group(1).strip()
            if q:
                return q
    return None


# ── Public fallback called by main.py's link_handler ─────────────────────────
async def try_plain_text_search(client: Client, message: Message,
                                *, make_button, BTN_PRIMARY) -> bool:
    """Called by main.py's link_handler after every dedicated extractor
    has already found nothing. Returns True if a search was run."""
    if _yt_dlp is None:
        return False
    text = (message.text or message.caption or "").strip()
    if not text or text.startswith("/"):
        return False
    if _URL_HINT.search(text):
        return False
    query = _extract_yt_query(text) or text
    if not (2 <= len(query) <= 120):
        return False
    await _do_search(client, message, query,
                     make_button=make_button, BTN_PRIMARY=BTN_PRIMARY)
    return True


# ── register(app) — called from main.py after app = Client(...) ───────────────
def register(app: Client, make_button, BTN_PRIMARY, LINK_CACHE: dict,
             show_quality_menu) -> None:
    """
    Register all ytsearch handlers on the actual app instance.

    BUG FIX: previously used @Client.on_callback_query class-level
    decorators. These register on the Client CLASS, not on the specific
    app INSTANCE — they only fire if Pyrogram's plugin system is active
    (plugins= kwarg in Client()). Since fbot doesn't use plugins,
    NONE of the ytsr:/ytsrpg: callbacks ever fired. Every button tap
    was silently dropped. Fix: use app.on_* decorators inside register()
    so handlers bind to the actual running instance.
    """

    # ── /search and /yts commands ─────────────────────────────────────
    @app.on_message(filters.command(["search", "yts"]) & filters.private)
    async def search_cmd(client: Client, message: Message):
        if len(message.command) < 2:
            return await message.reply_text(
                "🔍 <b>Usage:</b> <code>/search &lt;song or video name&gt;</code>\n"
                "<i>e.g.</i> <code>/search Believer Imagine Dragons</code>",
                parse_mode=ParseMode.HTML,
            )
        query = message.text.split(None, 1)[1].strip()
        await _do_search(client, message, query,
                         make_button=make_button, BTN_PRIMARY=BTN_PRIMARY)

    # ── noop (Page N button) ──────────────────────────────────────────
    @app.on_callback_query(filters.regex(r"^ytsr:noop:"))
    async def cb_noop(client: Client, cq: CallbackQuery):
        await cq.answer()

    # ── Number button tapped → show quality menu ──────────────────────
    @app.on_callback_query(filters.regex(r"^ytsr:(\d+):([a-f0-9]{12})$"))
    async def cb_select(client: Client, cq: CallbackQuery):
        match   = cq.matches[0]
        idx     = int(match.group(1))
        cache_key = match.group(2)

        cached = _SEARCH_CACHE.get(cache_key)
        if not cached:
            return await cq.answer(
                "⌛ Search expired — please search again.", show_alert=True
            )
        results = cached["results"]
        if idx < 0 or idx >= len(results):
            return await cq.answer("Invalid selection.", show_alert=True)

        # BUG FIX: answer the callback IMMEDIATELY (Telegram requires < 5s).
        # show_quality_menu can take 10-30s for quality resolution — if we
        # awaited it before answering, Telegram would show "bot not responding"
        # and the button would appear stuck. Answer first, then run the slow op.
        await cq.answer("⏳ Loading qualities...")

        video_url = f"https://www.youtube.com/watch?v={results[idx]['id']}"
        link_id   = uuid.uuid4().hex[:8]
        LINK_CACHE[link_id] = video_url
        await show_quality_menu(client, cq, link_id, video_url)

    # ── Pagination ────────────────────────────────────────────────────
    @app.on_callback_query(filters.regex(r"^ytsrpg:(\d+):([a-f0-9]{12})$"))
    async def cb_page(client: Client, cq: CallbackQuery):
        match     = cq.matches[0]
        page      = int(match.group(1))
        cache_key = match.group(2)

        cached = _SEARCH_CACHE.get(cache_key)
        if not cached:
            return await cq.answer(
                "⌛ Search expired — please search again.", show_alert=True
            )

        results   = cached["results"]
        query     = cached["query"]
        exhausted = cached.get("exhausted", False)
        page_start = page * SEARCH_PAGE_SIZE

        # Load more results if we've paged past what we have
        if not exhausted and page_start >= len(results):
            # BUG FIX: cq.answer() was called inside a while loop, but a
            # CallbackQuery can only be answered ONCE — second call raises
            # BAD_REQUEST. Answer once here, then do all the loading.
            await cq.answer("⏳ Loading more results...")
            try:
                more = await asyncio.to_thread(_search_youtube_sync, query)
                if not more or len(more) < SEARCH_CHUNK_SIZE:
                    exhausted = True
                    cached["exhausted"] = True
                existing_ids = {r["id"] for r in results}
                for r in more:
                    if r["id"] not in existing_ids:
                        results.append(r)
                        existing_ids.add(r["id"])
                cached["results"] = results
            except Exception as e:
                return await cq.answer(f"Error: {e}", show_alert=True)
        else:
            await cq.answer()

        try:
            await cq.message.edit_text(
                _results_text(query, results, page=page, exhausted=exhausted),
                parse_mode=ParseMode.HTML,
                reply_markup=_results_kb(
                    make_button, BTN_PRIMARY, results, page, exhausted, cache_key
                ),
            )
        except Exception as e:
            # BUG FIX: unguarded before — a FloodWait from rapid Next/Prev
            # tapping (the most likely way to trigger one on THIS specific
            # call) used to propagate straight up uncaught instead of just
            # failing this one page-turn.
            logger.warning(f"[ytsearch] pagination edit failed: {e}")
            try:
                await cq.answer("Couldn't refresh — try again in a moment.", show_alert=True)
            except Exception:
                pass
