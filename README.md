# 🚀 Faphouse Bot

> ⚡ A powerful Telegram bot for downloading and streaming videos from `faphouse.com` / `faphouse2.com` links directly through Telegram.

---

## ✨ Features

| Feature | Description |
|---|---|
| 📥 **Direct Downloads** | Download faphouse.com/faphouse2.com videos directly to Telegram |
| ▶️ **Stream Links** | Get streamable links for in-app viewing |
| 🔗 **Multi-Link Support** | Send and process multiple links at once |
| 📊 **Progress Tracking** | Real-time download progress with speed display |
| 💎 **Premium System** | Premium plans with daily free-download limits |
| 🗑️ **Auto Delete** | Automatically delete sent files after a configurable time |
| 🛡️ **Admin Tools** | Premium management, ban/unban, statistics and broadcasting |
| 💾 **Backup Channels** | Automatically copy successfully downloaded videos to linked channels/groups |
| 📝 **Event Logging** | Monitor bot startup, new users and successful downloads |
| 🗄️ **MongoDB Storage** | Persistent file cache and user management |

---

## 🧰 Prerequisites

Before starting, make sure you have:

- 🐍 **Python 3.10+**
- 🤖 **Telegram Bot Token** — Get it from `@BotFather`
- 🔑 **Telegram API Credentials** — Get them from `my.telegram.org`
- 🗄️ **MongoDB Database**
- 🎬 **ffmpeg** — installed automatically via the Dockerfile, or install locally for development
- 🔑 **A faphouse.com/faphouse2.com account** *(optional — only needed for account-gated videos)*

---

# ⚙️ Setup

## 1️⃣ Get Telegram Bot Token

1. Open Telegram and search for `@BotFather`
2. Send `/newbot`
3. Follow the instructions provided by BotFather
4. Copy your generated Bot Token

---

## 2️⃣ Get Telegram API Credentials

1. Visit `my.telegram.org`
2. Log in using your Telegram phone number
3. Open **API development tools**
4. Create a new application
5. Copy your:

```text
api_id
api_hash
```

---

## 3️⃣ Install & Run

Clone/download the project and enter the directory:

```bash
cd faphouse_bot
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create your environment file:

```bash
cp .env.example .env
```

Edit `.env` and add your credentials.

Finally, start the bot:

```bash
python main.py
```

---

# 🔐 Environment Variables

Configure your `.env` file:

```env
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
OWNER_ID=your_telegram_user_id
MONGO_URI=your_mongodb_connection_string
```

### ⚡ Optional Configuration

```env
ADMINS=123456789,987654321
DAILY_FREE_LIMIT=10
AUTO_DELETE_SECONDS=3600
LOG_CHANNEL=-1001234567890
EMAIL=your_faphouse_account_email
PASSWORD=your_faphouse_account_password
BASE_URL=https://faphouse2.com
SESSION_MAX_AGE=1800
```

| Variable | Description |
|---|---|
| `API_ID` | Telegram API ID |
| `API_HASH` | Telegram API Hash |
| `BOT_TOKEN` | Telegram Bot Token |
| `OWNER_ID` | Main bot owner Telegram ID |
| `MONGO_URI` | MongoDB connection string |
| `ADMINS` | Additional admin Telegram IDs |
| `DAILY_FREE_LIMIT` | Daily downloads allowed for free users |
| `AUTO_DELETE_SECONDS` | Auto-delete delay in seconds |
| `LOG_CHANNEL` | Channel/group ID for event logs |
| `EMAIL` | faphouse.com/faphouse2.com login email *(optional)* |
| `PASSWORD` | faphouse.com/faphouse2.com login password *(optional)* |
| `BASE_URL` | Default faphouse site to use/login to first |
| `SESSION_MAX_AGE` | Seconds before forcing a fresh faphouse re-login |

> ⚠️ **Security:** Never commit your real `.env` file or credentials to GitHub.

---

# 🤖 Usage

## 1. 🚀 Start the Bot

Open your bot on Telegram and send:

```text
/start
```

## 2. 🔗 Send a faphouse Link

Example:

```text
https://faphouse2.com/videos/xxxxx
```

## 3. 🎯 Choose an Action

The bot will provide options such as:

- 📥 **Download File**
- ▶️ **Get Stream Link**

---

# 🧾 Commands

## 👤 User Commands

| Command | Description |
|---|---|
| `/start` | 🚀 Welcome message |
| `/help` | 📚 Show usage instructions |
| `/myplan` | 📊 Show your plan / account status |

`/myplan` is also available through the **📊 My Status** button.

---

## 👑 Admin Commands

> 🔒 The following commands require the user to be listed in `ADMINS` or be the configured `OWNER_ID`.

### 🚀 Auto-Scraper / Auto-Uploader

```text
/autoupload
```

Starts scraping faphouse.com's public `/videos` listing page by page and
uploading every not-yet-seen video into the chat the command was sent in
(works in channels/groups the bot is an admin of, not just DMs). Progress
is saved after every video, so a restart resumes from where it left off
instead of starting over.

```text
/stopupload
```

Stops auto-uploading in the current chat.

```text
/pending
```

Scans the first few listing pages and reports how many videos haven't
been uploaded yet.

Optional env vars: `SITE_URL` (default `https://faphouse2.com`),
`AUTO_UPLOAD_COOLDOWN` (seconds between uploads for non-admin runs,
default `60`), `MAX_FILE_SIZE` (bytes, default 2GB — larger videos are
skipped and logged rather than failing the run). Setting `DEFAULT_CHANNEL`
to a channel id additionally starts a 24/7 monitor on boot that watches
page 1 for brand-new releases and posts them there automatically
(`MONITOR_INTERVAL` controls how often it checks, default `180`s).

### 💎 Premium Management

```text
/addpremium <user_id> <days|lifetime>
```

Grant premium access.

```text
/removepremium <user_id>
```

Revoke premium access.

### 🛡️ User Management

```text
/ban <user_id>
```

Ban a user.

```text
/unban <user_id>
```

Unban a user.

### 📊 Statistics & Broadcasting

```text
/stats
```

View bot usage statistics.

```text
/broadcast <message>
```

Broadcast a message to all known users.

You can also reply to an existing Telegram message with:

```text
/broadcast
```

---

# 💾 Linked Backup Channels

Admins can connect one or more Telegram channels/groups using:

```text
/set_channel_id <channel_id>
```

Every successfully downloaded video will also be copied to the linked destination.

### ⭐ Why use Backup Channels?

- 🗄️ Maintain a persistent video archive
- 🔄 Keep files beyond the auto-delete period
- 📦 Store downloaded content separately
- 🛡️ Reduce the risk of losing sent files

### 🔎 Getting a Channel ID

Forward any message from the channel/group to:

```text
@MissRose_bot
```

The bot must have administrator permissions in the destination channel/group.

### 📋 Backup Channel Commands

```text
/channel_id
```

List all linked backup channels/groups.

```text
/del_channel_id [id]
```

Unlink a specific channel, or all channels if no ID is provided.

---

# 📝 Log Channel

Set `LOG_CHANNEL` to a single Telegram channel/group ID to receive bot event logs.

```env
LOG_CHANNEL=-1001234567890
```

Set it to `0` or leave it unset to disable logging.

### 📌 Logged Events

- 🚀 Bot startup notification
- 👤 First `/start` notification for each new user
- 📥 Successful download notifications
- 🔗 Downloaded link information

> ℹ️ **Important:** `LOG_CHANNEL` is separate from the `/set_channel_id` backup channels.
>
> 📝 `LOG_CHANNEL` → Text/event logs  
> 💾 Backup channels → Actual downloaded video files

---

# 💎 Premium Plans

Free users receive:

```env
DAILY_FREE_LIMIT=10
```

downloads per day by default.

🔄 The free-download counter resets at **UTC midnight**.

The **💎 Plans** menu displays available pricing tiers and allows users to select a plan.

> ⚠️ **Payment Gateway:** There is currently no payment gateway integrated into the bot.
>
> Users should contact an administrator for premium activation.

The admin can then run:

```text
/addpremium <user_id> <days|lifetime>
```

## 💎 Premium Options

### ⏳ 7 Days

```text
/addpremium 123456789 7
```

### 📅 30 Days

```text
/addpremium 123456789 30
```

### ♾️ Lifetime

```text
/addpremium 123456789 lifetime
```

---

# 🗑️ Auto-Delete System

The bot can automatically remove sent videos after a configurable period.

### Default

```env
AUTO_DELETE_SECONDS=3600
```

⏱️ `3600` seconds = **1 hour**

To disable auto-delete:

```env
AUTO_DELETE_SECONDS=0
```

### 📌 How It Works

1. 📥 Video is downloaded
2. 📤 Video is sent to the user
3. ⏳ Auto-delete timer starts
4. 🗑️ File is automatically deleted after the configured duration
5. 🔔 A notice tells the user to re-download or forward the file if they want to keep it

Every sent file also includes an upfront auto-delete notice in its caption.

---

# 🗂️ Project Structure

```text
faphouse_bot/
│
├── main.py
│   └── Telegram bot handlers, plans, admin commands & auto-delete
│
├── faphouse_downloader.py
│   └── faphouse.com/faphouse2.com session/login + m3u8 resolver + ffmpeg downloader
│
├── db.py
│   └── MongoDB file cache, premium, ban & daily-limit tracking
│
├── config.py
│   └── Environment/configuration management
│
├── requirements.txt
│   └── Python dependencies
│
├── render.yaml
│   └── Render.com deployment configuration
│
├── Dockerfile
│   └── Docker deployment configuration
│
├── .env.example
│   └── Environment variable template
│
└── README.md
    └── Project documentation
```

---

# 🧠 Technical Notes

- 🔐 An `EMAIL`/`PASSWORD` login is optional — without it the bot still works via guest fetching for non-gated videos.
- 🚫 Never publish your `.env` file or account credentials.
- 📥 Videos are temporarily downloaded before being sent.
- 🗑️ Temporary video files are deleted after sending.
- 💾 MongoDB is used for persistent cache and user-related data.
- ☁️ The project includes configuration files for containerized/Render deployment.

---

# 🚀 Deployment

The project includes:

```text
🐳 Dockerfile
☁️ render.yaml
```

This makes the bot suitable for deployment on supported container/cloud hosting platforms.

Before deploying, make sure all required environment variables are configured securely.

---

# 🛡️ Security Checklist

Before making the bot public:

- ✅ Keep `BOT_TOKEN` private
- ✅ Keep `API_HASH` private
- ✅ Keep `EMAIL` / `PASSWORD` private
- ✅ Keep `MONGO_URI` private
- ✅ Never upload `.env` to GitHub
- ✅ Add `.env` to `.gitignore`
- ✅ Restrict admin commands
- ✅ Give the bot only required Telegram permissions

---

# 📌 Important

> 🔐 **Never share your Telegram session string, bot token, API credentials, or MongoDB credentials with anyone.**

> 💡 **For production deployments, always use environment variables or a secure secrets manager instead of hardcoding credentials.**

---

# ⚡ Quick Start

```bash
git clone <your-repository>
cd faphouse_bot

pip install -r requirements.txt

cp .env.example .env
nano .env

python main.py
```

Then open your Telegram bot and send:

```text
/start
```

---

# 👨‍💻 Project

**Faphouse Bot** — A feature-rich Telegram automation project focused on faphouse.com/faphouse2.com video downloading, streaming, premium management, logging and automated file handling.

---

<div align="center">

### ⚡ Fast • Secure • Reliable • Developer Friendly

**❤️ Powered by Anuj Kumar**

</div>
