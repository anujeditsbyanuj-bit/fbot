import os
from dotenv import load_dotenv

load_dotenv()

# Hardcoded as defaults per your request — still overridable via a real
# env var (e.g. on Render) if one is set, since os.getenv's second
# argument is only used when the env var is missing.
API_ID = int(os.getenv("API_ID", "33029767"))
API_HASH = os.getenv("API_HASH", "5d897bed11bc8b062a12f6c1c3c5360a")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8677080919:AAFNPM5-uSom0PmIn_8OIrdt7MH7oSH0HWo")
# Optional: a Telethon user-session string (phone-number login, separate
# from BOT_TOKEN above). Only needed for diskwala.py's token-API tier —
# logs into Telegram's "sky577bot" Mini App as a real user account to pull
# an auth token, the same way ultra-main's bot does (see get_auth_token()
# in diskwala.py). Without it, Diskwala/Flezen/VidBunker links still work
# via the no-auth HTML-scrape fallback, just without that tier's extra
# reliability. Generate one with Telethon's StringSession (same process as
# ultra-main's own README) and paste it here if you want it wired up.
SESSION = os.getenv("SESSION", "1BVtsOJ0Bu2ZBlMX0GJ1hlVlPPKgBfdyxh6tl5LOJ23TrAxHYMi_JMDyFLDSW45rlSbyeDJhBrdFtzwW5Ka2iLnLwrKHOBLt7gYpCwSmFa8yq5l2Kr0oUM5Vbo4svgx1wZ33eZJLqMqmoVB9FA1UPFi2rEp4_WZS5176eYQuyCa3wfPymmtKSQnsmNm84Vok6UTc3OzrJimxcXHEEDxecvEpnXw90pTH2Bg6FBPtMVDikvCHP_u9dcH3O2VFjsDTr8T6Ov3DhwG6Uh0qkIjfLRhAb232u27hUE44nvYrZZl1_CVAvZM84-mMa7KhHhb9xYRstG5w-YNbcf7GYi7ssiqVuZUgGUbo=")
OWNER_ID = int(os.getenv("OWNER_ID", "8931907813"))

TG_BOT_WORKERS = int(os.getenv("TG_BOT_WORKERS", "8"))   # 4→8: zyada parallel Telegram connections
DOWNLOAD_DIR = "downloads"
MAX_CONCURRENT_DOWNLOADS = 3   # 5→3: bandwidth ek file pe focus karega, sabka upload tez hoga

# MongoDB connection. MONGO_URI is required (e.g. a MongoDB Atlas
# connection string). MONGO_DB_NAME defaults to "faphouse_bot".
MONGO_URI = os.getenv("MONGO_URI", "mongodb+srv://Anujedit:Anujedit@cluster0.7cs2nhd.mongodb.net/?appName=Cluster0")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "faphouse_bot")

# ---------------------------------------------------------------------
# Extra settings for premium / admin / cache features.
# ADMINS: comma-separated user ids in the ADMINS env var. OWNER_ID is
# always treated as an admin even if not listed.
# ---------------------------------------------------------------------
ADMINS = list({OWNER_ID, *[int(x) for x in os.getenv("ADMINS", "8931907813").split(",") if x.strip()]})

# Photo shown on /start. Can be a URL or a local file path.
START_PHOTO_URL = os.getenv("START_PHOTO_URL", "https://iili.io/n2jHVj9.jpg")

# Free (non-premium) users can download this many files per day (UTC).
DAILY_FREE_LIMIT = int(os.getenv("DAILY_FREE_LIMIT", "5"))

# "📋 20 links ek saath" — how many links from one message get queued for
# processing. Free users are capped lower; premium gets the full 20.
MAX_LINKS_FREE = int(os.getenv("MAX_LINKS_FREE", "5"))
MAX_LINKS_PREMIUM = int(os.getenv("MAX_LINKS_PREMIUM", "20"))

# If > 0, delivered videos are auto-deleted from the chat after this many
# seconds (the caption warns the user to forward it first). 0 disables it.
# Default: 3600 seconds = 1 hour.
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "3600"))

# Optional: channel id (e.g. -100xxxxxxxxxx) where new-user/download logs
# are posted. Leave unset/empty to disable logging.
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1003925649805")) or None

# Optional: comma-separated channel ids to also receive a copy of every
# delivered video (a simple off-site backup). Leave empty to disable.
BACKUP_CHANNEL_IDS = [int(x) for x in os.getenv("BACKUP_CHANNEL_IDS", "-1003925649805").split(",") if x.strip()]

# Optional: a private channel (bot must be admin there) where one copy of
# every freshly-uploaded video is stored. Cache hits are then served with
# copy_message() from this channel instead of a bare file_id — this gives
# Telegram a fresh file_reference for the recipient, which fixes cache
# hits failing/redownloading when a *different* user requests a link that
# someone else already downloaded (the old file_id is only guaranteed
# valid for the chat it was originally sent to). Leave unset to fall back
# to the old file_id-only behaviour.
CACHE_CHANNEL_ID = int(os.getenv("CACHE_CHANNEL_ID", "-1003925649805")) or None

# ---------------------------------------------------------------------
# Auto-scraper / auto-uploader (/autoupload) — scrapes faphouse.com's
# public /videos listing and bulk-uploads new videos to a chat/channel.
# ---------------------------------------------------------------------
SITE_URL = os.getenv("SITE_URL", "https://faphouse2.com")
USER_AGENT = os.getenv(
    "USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
)
# Non-admin cooldown between auto-uploaded videos in the same chat (seconds).
AUTO_UPLOAD_COOLDOWN = int(os.getenv("AUTO_UPLOAD_COOLDOWN", "60"))
# Telegram's per-file limit for regular bots.
MAX_FILE_SIZE = int(os.getenv("MAX_FILE_SIZE", str(2000 * 1024 * 1024)))
# Target size per part when a video exceeds MAX_FILE_SIZE and gets split
# instead of skipped. Deliberately below MAX_FILE_SIZE — ffmpeg's
# time-based segmenting only *estimates* each part's size off the whole
# file's average bitrate, so a part can land a bit over that estimate on
# a higher-bitrate stretch. ~7% headroom absorbs that without needing a
# second corrective re-split pass in the common case.
SPLIT_PART_TARGET_BYTES = int(os.getenv("SPLIT_PART_TARGET_BYTES", str(int(MAX_FILE_SIZE * 0.93))))
# Optional: a channel id for the 24/7 live monitor (watches for brand-new
# releases and posts them here automatically). Leave unset to disable it.
DEFAULT_CHANNEL = int(os.getenv("DEFAULT_CHANNEL", "-1003925649805")) or None
MONITOR_INTERVAL = int(os.getenv("MONITOR_INTERVAL", "180"))

# eporner.com is a massive general tube site (non-stop firehose uploads),
# not a curated-release site like faphouse — "watch and push instantly"
# there would mean hundreds of essentially-random new videos per
# MONITOR_INTERVAL if left uncapped. This caps how many NEW (not-yet-
# uploaded) videos eporner_live_monitor pushes per cycle; anything past
# the cap just waits for the next cycle instead of getting dropped.
EPORNER_MONITOR_MAX_PER_CYCLE = int(os.getenv("EPORNER_MONITOR_MAX_PER_CYCLE", "5"))

# ---------------------------------------------------------------------
# YouTube (via ytdlp_downloader.py) — cookies.
#
# YT_COOKIES: path to a Netscape-format cookies.txt file. Fixes
# "Sign in to confirm you're not a bot" on some videos/IPs. Optional —
# yt-dlp still works cookie-less via the ios/mweb/tv_embedded/android
# player clients, just capped to lower-res formats on videos that need a
# login.
YT_COOKIES = os.getenv("YT_COOKIES", "")
INSTA_COOKIES = os.getenv("INSTA_COOKIES", "")
FB_COOKIES = os.getenv("FB_COOKIES", "")
VK_COOKIES = os.getenv("VK_COOKIES", "")
YOUPORN_COOKIES  = os.getenv("YOUPORN_COOKIES",  "")
SPANKBANG_COOKIES = os.getenv("SPANKBANG_COOKIES", "")
BEEG_COOKIES     = os.getenv("BEEG_COOKIES",     "")
BILI_COOKIES = os.getenv("BILI_COOKIES", "")

# YTDL_MAX_DURATION_SECONDS: reject a video before downloading it at all
# if yt-dlp's extracted info reports a longer duration — catches a
# multi-hour livestream VOD or similar before wasting bandwidth/time on
# it. 0/unset = no limit.
YTDL_MAX_DURATION_SECONDS = int(os.getenv("YTDL_MAX_DURATION_SECONDS", "0") or 0)
# YTDL_MAX_FILESIZE: passed to yt-dlp as its own max_filesize option
# (best-effort — yt-dlp can only enforce this precisely for formats that
# report a size upfront; fragmented HLS/DASH formats often don't) AND
# checked again against the actual file on disk once the download
# finishes, so an oversized fragmented download still gets caught even
# though yt-dlp's own in-flight check couldn't stop it early. Bytes;
# 0/unset = no limit.
YTDL_MAX_FILESIZE = int(os.getenv("YTDL_MAX_FILESIZE", "0") or 0)

# PO-token support is back (see pot_provider.py + ytdlp_downloader.py's
# player_client logic) — a real implementation this time, running a local
# bgutil-ytdlp-pot-provider HTTP server in the background so yt-dlp's
# "web" YouTube client becomes usable again for its full quality ladder.
# ios/tv/mweb/tv_embedded/android (PO-token-free) stay as the fallback
# client list whenever the provider isn't up — still true on a host
# missing what pot_provider.py's setup needs (see its own docstring).
