"""
MongoDB-backed persistence for the Faphouse bot (using motor, the async
MongoDB driver, so calls don't block pyrogram's event loop).

Collections:
  users        - _id: user_id (int)
                 first_seen, is_banned, premium_lifetime, premium_until,
                 referred_by, referral_count, referral_rewards (claimed thresholds)
  daily_counts - _id: "{user_id}:{YYYY-MM-DD}"
                 user_id, day, count
  downloads    - one doc per completed download: user_id, ts
  file_cache   - _id: "{link}::{quality}"
                 link, quality, file_id, name, size, quality_label, cached_at,
                 duration, cache_chat_id, cache_message_id
                 cache_chat_id/cache_message_id point at the copy stored in
                 CACHE_CHANNEL_ID, used to (re)send via copy_message() for a
                 fresh file_reference instead of the raw file_id (which can
                 go stale for chats that never received the original
                 upload). Both are None on older docs written before
                 CACHE_CHANNEL_ID support / when it isn't configured.
  active_downloads - one doc per in-flight download: chat_id, message_id,
                 link, quality_url, quality_label, started_at.
                 Written the moment a download actually starts and removed
                 the moment it finishes (success, handled failure, or
                 /cancel). If the bot process dies mid-download, its doc is
                 the only one left behind — on the next startup those are
                 read back and the download is kicked off again
                 automatically instead of leaving the user's status message
                 stuck forever.
  descriptions - _id: desc_id (the short hex id in the "Full Description"
                 button's callback_data). site_name, title, description,
                 cached_at. Backs main.py's DESC_CACHE dict — that dict
                 alone doesn't survive a restart, so a "Full Description"
                 button on any video sent before the bot's last restart
                 would otherwise permanently show "Description no longer
                 available" the moment someone finally taps it days/weeks
                 later. This collection is what makes it durable; the dict
                 stays as a fast in-process cache in front of it.
"""

from datetime import datetime, timedelta

from motor.motor_asyncio import AsyncIOMotorClient
from pymongo import ReturnDocument

from config import MONGO_URI, MONGO_DB_NAME

_client = AsyncIOMotorClient(MONGO_URI)
_db = _client[MONGO_DB_NAME]

users_col = _db["users"]
daily_counts_col = _db["daily_counts"]
downloads_col = _db["downloads"]
file_cache_col = _db["file_cache"]
channels_col = _db["channels"]
pending_deletes_col = _db["pending_deletes"]
active_downloads_col = _db["active_downloads"]
descriptions_col = _db["descriptions"]


def _today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


# ---------------------------------------------------------------------
# Users / registration
# ---------------------------------------------------------------------

async def register_user_if_new(user_id: int) -> bool:
    result = await users_col.update_one(
        {"_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "first_seen": datetime.utcnow(),
            "is_banned": False,
            "premium_lifetime": False,
            "premium_until": None,
            "referral_count": 0,
            "referral_rewards": [],
        }},
        upsert=True,
    )
    return result.upserted_id is not None


# ---------------------------------------------------------------------
# Bans
# ---------------------------------------------------------------------

async def set_banned(user_id: int, banned: bool):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"is_banned": banned},
         "$setOnInsert": {"first_seen": datetime.utcnow(), "premium_lifetime": False, "premium_until": None}},
        upsert=True,
    )


async def is_banned(user_id: int) -> bool:
    doc = await users_col.find_one({"_id": user_id}, {"is_banned": 1})
    return bool(doc and doc.get("is_banned"))


# ---------------------------------------------------------------------
# Premium
# ---------------------------------------------------------------------

async def set_premium(user_id: int, days):
    """days=None means lifetime."""
    if days is None:
        update = {"premium_lifetime": True, "premium_until": None}
    else:
        update = {"premium_lifetime": False, "premium_until": datetime.utcnow() + timedelta(days=days)}

    await users_col.update_one(
        {"_id": user_id},
        {"$set": update,
         "$setOnInsert": {"first_seen": datetime.utcnow(), "is_banned": False}},
        upsert=True,
    )


async def remove_premium(user_id: int):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"premium_lifetime": False, "premium_until": None}},
    )


async def get_premium_status(user_id: int) -> dict:
    doc = await users_col.find_one({"_id": user_id}, {"premium_lifetime": 1, "premium_until": 1})

    if not doc:
        return {"is_premium": False, "lifetime": False, "expires_at": None}

    if doc.get("premium_lifetime"):
        return {"is_premium": True, "lifetime": True, "expires_at": None}

    until = doc.get("premium_until")
    if until and until > datetime.utcnow():
        return {"is_premium": True, "lifetime": False, "expires_at": until}

    return {"is_premium": False, "lifetime": False, "expires_at": None}


async def grant_referral_premium_days(user_id: int, days: int):
    """Adds `days` of premium on top of whatever the user already has
    (extends from their current expiry if it's still active, otherwise
    from now). No-op for lifetime-premium users — nothing to add."""
    doc = await users_col.find_one({"_id": user_id}, {"premium_lifetime": 1, "premium_until": 1})
    if doc and doc.get("premium_lifetime"):
        return
    now = datetime.utcnow()
    base = now
    if doc and doc.get("premium_until") and doc["premium_until"] > now:
        base = doc["premium_until"]
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"premium_until": base + timedelta(days=days), "premium_lifetime": False},
         "$setOnInsert": {"first_seen": now, "is_banned": False}},
        upsert=True,
    )


# ---------------------------------------------------------------------
# Referrals
# ---------------------------------------------------------------------

async def set_referrer(user_id: int, referrer_id: int) -> bool:
    """Records who referred this user — only ever the first time (won't
    overwrite an existing referrer) and never lets a user refer themself.
    Returns True if this call is the one that actually set it."""
    if user_id == referrer_id:
        return False
    result = await users_col.update_one(
        {"_id": user_id, "referred_by": {"$exists": False}},
        {"$set": {"referred_by": referrer_id}},
    )
    return result.modified_count > 0


async def increment_referral_count(referrer_id: int) -> int:
    doc = await users_col.find_one_and_update(
        {"_id": referrer_id},
        {"$inc": {"referral_count": 1},
         "$setOnInsert": {"first_seen": datetime.utcnow(), "is_banned": False,
                           "premium_lifetime": False, "premium_until": None}},
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    return doc.get("referral_count", 0)


async def get_referral_count(user_id: int) -> int:
    doc = await users_col.find_one({"_id": user_id}, {"referral_count": 1})
    return (doc or {}).get("referral_count", 0)


async def get_referral_rewards_claimed(user_id: int) -> list:
    doc = await users_col.find_one({"_id": user_id}, {"referral_rewards": 1})
    return (doc or {}).get("referral_rewards", [])


async def mark_referral_reward_claimed(user_id: int, threshold: int):
    await users_col.update_one(
        {"_id": user_id},
        {"$addToSet": {"referral_rewards": threshold}},
    )


# ---------------------------------------------------------------------
# Daily free-download limit
# ---------------------------------------------------------------------

async def get_daily_count(user_id: int) -> int:
    doc = await daily_counts_col.find_one({"_id": f"{user_id}:{_today()}"}, {"count": 1})
    return doc["count"] if doc else 0


async def bump_daily_count(user_id: int):
    day = _today()
    await daily_counts_col.update_one(
        {"_id": f"{user_id}:{day}"},
        {"$inc": {"count": 1}, "$setOnInsert": {"user_id": user_id, "day": day}},
        upsert=True,
    )


# ---------------------------------------------------------------------
# Download stats
# ---------------------------------------------------------------------

async def bump_total_downloads(user_id: int):
    await downloads_col.insert_one({"user_id": user_id, "ts": datetime.utcnow()})


async def get_user_total_downloads(user_id: int) -> int:
    return await downloads_col.count_documents({"user_id": user_id})


# ---------------------------------------------------------------------
# Custom caption (per-user override for the upload caption template)
# ---------------------------------------------------------------------

async def set_caption(user_id: int, caption: str):
    await users_col.update_one({"_id": user_id}, {"$set": {"caption": caption}}, upsert=True)


async def get_caption(user_id: int):
    doc = await users_col.find_one({"_id": user_id}, {"caption": 1})
    return (doc or {}).get("caption")


async def del_caption(user_id: int):
    await users_col.update_one({"_id": user_id}, {"$unset": {"caption": ""}})


# ---------------------------------------------------------------------
# Custom thumbnail (per-user override for the upload thumbnail)
# ---------------------------------------------------------------------

async def set_thumbnail(user_id: int, file_id: str):
    await users_col.update_one({"_id": user_id}, {"$set": {"thumbnail": file_id}}, upsert=True)


async def get_thumbnail(user_id: int):
    doc = await users_col.find_one({"_id": user_id}, {"thumbnail": 1})
    return (doc or {}).get("thumbnail")


async def del_thumbnail(user_id: int):
    await users_col.update_one({"_id": user_id}, {"$unset": {"thumbnail": ""}})


# ---------------------------------------------------------------------
# Per-user dump chat (personal backup destination for delivered files)
# ---------------------------------------------------------------------

async def set_dump_chat(user_id: int, chat_id):
    if chat_id is None:
        await users_col.update_one({"_id": user_id}, {"$unset": {"dump_chat": ""}})
    else:
        await users_col.update_one({"_id": user_id}, {"$set": {"dump_chat": chat_id}}, upsert=True)


async def get_dump_chat(user_id: int):
    doc = await users_col.find_one({"_id": user_id}, {"dump_chat": 1})
    return (doc or {}).get("dump_chat")


# ---------------------------------------------------------------------
# Index setup — safe to call every startup (create_index is idempotent).
# ---------------------------------------------------------------------

async def ensure_indexes():
    await daily_counts_col.create_index("user_id")
    await downloads_col.create_index("user_id")
    await users_col.create_index("is_banned")
    await pending_deletes_col.create_index("delete_at")
    await uploaded_videos_col.create_index("content_hash")


# ---------------------------------------------------------------------
# Auto-delete scheduling — persisted so a bot restart (redeploy, crash,
# Render free-tier spin-down) doesn't just silently lose the in-memory
# asyncio.sleep() timer and leave the video undeleted forever.
# ---------------------------------------------------------------------

async def add_pending_delete(chat_id: int, message_id: int, delete_at: datetime):
    await pending_deletes_col.update_one(
        {"chat_id": chat_id, "message_id": message_id},
        {"$set": {"chat_id": chat_id, "message_id": message_id, "delete_at": delete_at}},
        upsert=True,
    )


async def remove_pending_delete(chat_id: int, message_id: int):
    await pending_deletes_col.delete_one({"chat_id": chat_id, "message_id": message_id})


async def get_all_pending_deletes() -> list:
    return [doc async for doc in pending_deletes_col.find({})]


# ---------------------------------------------------------------------
# In-flight download tracking — lets a download that was cut off by a bot
# restart (crash, redeploy, host restart) automatically pick back up
# instead of leaving the user's "Downloading..." message stuck forever.
# ---------------------------------------------------------------------

async def add_active_download(chat_id: int, message_id: int, link: str,
                                quality_url: str, quality_label: str):
    await active_downloads_col.update_one(
        {"chat_id": chat_id, "link": link, "quality_label": quality_label},
        {"$set": {
            "chat_id": chat_id,
            "message_id": message_id,
            "link": link,
            "quality_url": quality_url,
            "quality_label": quality_label,
            "started_at": datetime.utcnow(),
        }},
        upsert=True,
    )


async def remove_active_download(chat_id: int, link: str, quality_label: str):
    await active_downloads_col.delete_one(
        {"chat_id": chat_id, "link": link, "quality_label": quality_label}
    )


async def get_all_active_downloads() -> list:
    return [doc async for doc in active_downloads_col.find({})]


async def get_stats_summary() -> dict:
    total_users = await users_col.count_documents({})
    banned_count = await users_col.count_documents({"is_banned": True})
    now = datetime.utcnow()
    premium_count = await users_col.count_documents({
        "$or": [
            {"premium_lifetime": True},
            {"premium_until": {"$ne": None, "$gt": now}},
        ]
    })
    total_downloads = await downloads_col.count_documents({})
    total_files_cached = await file_cache_col.count_documents({})

    return {
        "total_users": total_users,
        "premium_count": premium_count,
        "banned_count": banned_count,
        "total_downloads": total_downloads,
        "total_files_cached": total_files_cached,
    }


async def delete_user(user_id: int):
    await users_col.delete_one({"_id": user_id})


async def all_chat_ids() -> list:
    cursor = users_col.find({}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]


async def get_all_users_full() -> list:
    """Full user docs for admin export (id, ban/premium status, first_seen)."""
    cursor = users_col.find({})
    return [doc async for doc in cursor]


# ---------------------------------------------------------------------
# File cache (instant resend via cached file_id)
# ---------------------------------------------------------------------

def _cache_key(link: str, quality: str) -> str:
    return f"{link}::{quality}"


async def get_cached_file(link: str, quality: str):
    doc = await file_cache_col.find_one({"_id": _cache_key(link, quality)})
    if not doc:
        return None
    return {
        "file_id": doc["file_id"],
        "name": doc["name"],
        "size": doc["size"],
        "quality_label": doc["quality_label"],
        "duration": doc.get("duration", 0),
        "cache_chat_id": doc.get("cache_chat_id"),
        "cache_message_id": doc.get("cache_message_id"),
    }


async def set_cached_file(link, quality, file_id, name, size, quality_label, duration=0,
                           cache_chat_id=None, cache_message_id=None):
    """cache_chat_id/cache_message_id: location of this file's copy inside
    CACHE_CHANNEL_ID (None if CACHE_CHANNEL_ID isn't configured or the copy
    failed) — every cache entry still needs at least a file_id to be
    resendable even without a cache-channel copy on record."""
    await file_cache_col.update_one(
        {"_id": _cache_key(link, quality)},
        {"$set": {
            "link": link,
            "quality": quality,
            "file_id": file_id,
            "name": name,
            "size": size,
            "quality_label": quality_label,
            "duration": duration,
            "cache_chat_id": cache_chat_id,
            "cache_message_id": cache_message_id,
            "cached_at": datetime.utcnow(),
        }},
        upsert=True,
    )


async def delete_cached_file(link: str, quality: str):
    await file_cache_col.delete_one({"_id": _cache_key(link, quality)})


# ---------------------------------------------------------------------
# "Full Description" button backing store — see descriptions_col's
# docstring entry above for why this needs to be durable, not just
# main.py's in-memory DESC_CACHE dict.
# ---------------------------------------------------------------------

async def get_cached_description(desc_id: str):
    doc = await descriptions_col.find_one({"_id": desc_id})
    if not doc:
        return None
    return {
        "site_name": doc.get("site_name"),
        "title": doc.get("title"),
        "description": doc.get("description"),
    }


async def set_cached_description(desc_id: str, site_name, title, description):
    await descriptions_col.update_one(
        {"_id": desc_id},
        {"$set": {
            "site_name": site_name,
            "title": title,
            "description": description,
            "cached_at": datetime.utcnow(),
        }},
        upsert=True,
    )


# ---------------------------------------------------------------------
# Backup-channel linking (admin-managed, dynamic — no redeploy needed)
# channels: _id: channel_id (int), added_at
# ---------------------------------------------------------------------

async def add_channel(channel_id: int) -> bool:
    """Link a channel/group so it receives a copy of every delivered file.
    Returns True if this was a new link, False if it was already linked."""
    result = await channels_col.update_one(
        {"_id": channel_id},
        {"$setOnInsert": {"added_at": datetime.utcnow()}},
        upsert=True,
    )
    return result.upserted_id is not None


async def remove_channel(channel_id: int) -> bool:
    """Unlink a single channel/group. Returns True if it was actually removed."""
    result = await channels_col.delete_one({"_id": channel_id})
    return result.deleted_count > 0


async def remove_all_channels() -> int:
    """Unlink every dynamically-added channel/group. Returns how many were removed."""
    result = await channels_col.delete_many({})
    return result.deleted_count


async def get_channels() -> list:
    """Return every dynamically-linked channel/group id."""
    cursor = channels_col.find({}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]


# ---------------------------------------------------------------------
# Titanium Clone Mode — per-user connected @BotFather bot tokens.
# Stored on the user's own doc: titanium_bots: [
#   {token, username, bot_id, source, added_at, last_used}
# ]
# source: "manual" (/addbot with a pasted @BotFather token) or "managed"
# (Bot API 9.6 Managed Bots auto-create — see titanium.py).
# ---------------------------------------------------------------------

async def get_titanium_bots(user_id: int) -> list:
    doc = await users_col.find_one({"_id": user_id}, {"titanium_bots": 1})
    return (doc or {}).get("titanium_bots", [])


async def add_titanium_bot(user_id: int, token: str, username: str, bot_id: int = None, source: str = "manual"):
    entry = {
        "token": token,
        "username": username,
        "bot_id": bot_id,
        "source": source,
        "added_at": datetime.utcnow().isoformat(),
        "last_used": 0,
    }
    await users_col.update_one(
        {"_id": user_id},
        {
            "$push": {"titanium_bots": entry},
            "$setOnInsert": {
                "first_seen": datetime.utcnow(),
                "is_banned": False,
                "premium_lifetime": False,
                "premium_until": None,
            },
        },
        upsert=True,
    )


async def remove_titanium_bot(user_id: int, username: str) -> bool:
    result = await users_col.update_one(
        {"_id": user_id},
        {"$pull": {"titanium_bots": {"username": username}}},
    )
    return result.modified_count > 0


async def touch_titanium_bot(user_id: int, token: str):
    await users_col.update_one(
        {"_id": user_id, "titanium_bots.token": token},
        {"$set": {"titanium_bots.$.last_used": datetime.utcnow().timestamp()}},
    )


async def get_all_titanium_owners() -> list:
    """[(user_id, [bot_entry, ...]), ...] for every user with at least one
    connected Titanium bot — used to reconnect clones on process restart."""
    cursor = users_col.find(
        {"titanium_bots": {"$exists": True, "$ne": []}}, {"titanium_bots": 1}
    )
    return [(doc["_id"], doc.get("titanium_bots", [])) async for doc in cursor]


# ---------------------------------------------------------------------
# Auto-scraper / auto-uploader (see auto_scraper.py)
# ---------------------------------------------------------------------

uploaded_videos_col = _db["uploaded_videos"]
scraper_state_col = _db["scraper_state"]


async def is_video_uploaded(slug: str) -> bool:
    return await uploaded_videos_col.find_one({"_id": slug}) is not None


async def save_uploaded_video(data: dict):
    data = dict(data)
    slug = data.pop("slug")
    data["uploaded_at"] = datetime.utcnow()
    await uploaded_videos_col.update_one(
        {"_id": slug}, {"$set": data}, upsert=True,
    )


async def get_uploaded_video_count() -> int:
    return await uploaded_videos_col.count_documents({})


async def get_skipped_size_limit_videos() -> list:
    """Every video still marked status="skipped_size_limit" — includes
    pre-split_upload.py entries (no split-splitting existed yet, so
    everything over 2GB got permanently skipped) as well as genuine
    can't-split failures saved since. Callers should treat a missing
    "url" field as unretryable (saved before that field was added)."""
    cursor = uploaded_videos_col.find({"status": "skipped_size_limit"})
    docs = [doc async for doc in cursor]
    for doc in docs:
        doc["slug"] = doc.pop("_id")
    return docs


async def get_failed_videos() -> list:
    """Every video marked status="failed" — saved by
    auto_scraper._process_with_retries() once every retry attempt for it
    has been exhausted, so it stops getting endlessly re-attempted by
    live_site_monitor (which otherwise treats anything not yet in
    uploaded_videos_col as "new" every poll cycle, forever). Surfaced
    here for /retryfailed the same way get_skipped_size_limit_videos()
    already is for /retryskipped."""
    cursor = uploaded_videos_col.find({"status": "failed"})
    docs = [doc async for doc in cursor]
    for doc in docs:
        doc["slug"] = doc.pop("_id")
    return docs


async def delete_uploaded_video(slug: str):
    """Removes one entry from the dedup cache so process_and_upload_video's
    is_video_uploaded() check no longer blocks reprocessing it — used by
    /retryskipped right before re-attempting a previously-skipped video."""
    await uploaded_videos_col.delete_one({"_id": slug})


async def get_uploaded_video_status(slug: str) -> str | None:
    """Returns the "status" field of one uploaded-videos entry (e.g.
    "skipped_size_limit"), or None if there's no status field (a normal
    successful upload) or no entry at all. Used right after a retry
    attempt to tell "uploaded successfully this time" apart from
    "re-saved as skipped_size_limit again" — process_and_upload_video's
    own return value is True for both cases."""
    doc = await uploaded_videos_col.find_one({"_id": slug}, {"status": 1})
    return (doc or {}).get("status")


async def is_title_duplicate(content_hash: str) -> bool:
    """Whether some OTHER, already-successfully-uploaded video already
    carries this normalized-title hash — used as a pre-download duplicate
    check when the same clip gets re-listed under a different slug (a
    common mirror-site/re-upload pattern). Only successful uploads set
    content_hash (see process_and_upload_video), so this never matches a
    skipped/duplicate entry — those aren't "already uploaded" in the
    first place."""
    return await uploaded_videos_col.find_one({"content_hash": content_hash}) is not None


async def get_chat_scraper_state(chat_id: int) -> dict:
    doc = await scraper_state_col.find_one({"_id": chat_id})
    if doc:
        return doc
    default = {
        "_id": chat_id, "chat_id": chat_id,
        "is_running": False, "current_page": 1, "total_scraped": 0,
    }
    await scraper_state_col.update_one(
        {"_id": chat_id}, {"$setOnInsert": default}, upsert=True,
    )
    return default


async def set_chat_scraper_state(chat_id: int, updates: dict):
    updates = dict(updates)
    updates["last_updated"] = datetime.utcnow()
    await scraper_state_col.update_one(
        {"_id": chat_id}, {"$set": updates}, upsert=True,
    )


async def get_all_active_scraper_states() -> list:
    return [doc async for doc in scraper_state_col.find({"is_running": True})]


# ---------------------------------------------------------------------
# Generic bot settings (key/value) — used to persist things like session
# cookies (see fpo_downloader.set_cookies / main.py's /setcookies) across
# restarts, instead of only living in memory until the process dies.
# ---------------------------------------------------------------------

bot_settings_col = _db["bot_settings"]


async def set_bot_setting(key: str, value: str):
    await bot_settings_col.update_one(
        {"_id": key}, {"$set": {"value": value, "updated_at": datetime.utcnow()}}, upsert=True,
    )


async def get_bot_setting(key: str, default: str = "") -> str:
    doc = await bot_settings_col.find_one({"_id": key})
    return doc["value"] if doc else default
