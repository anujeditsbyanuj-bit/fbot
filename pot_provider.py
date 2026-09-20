"""
Sets up and runs Brainicism's bgutil-ytdlp-pot-provider (a local
"proof-of-origin" token generator for YouTube — see
https://github.com/Brainicism/bgutil-ytdlp-pot-provider) as a background
HTTP server, so yt-dlp's "web" YouTube client becomes usable — that's the
ONLY client that exposes the full 1080p/720p/480p/360p (and, on eligible
videos, 2K/4K) format ladder; every other client (ios/tv/mweb/
tv_embedded/android) reliably caps out around 1080p, often lower.

On a Docker deploy, entrypoint.sh + the Dockerfile already clone+build
this at IMAGE-BUILD time and start the server before main.py ever runs —
this module is a no-op there (start_background() finds the server already
reachable on its first check and returns immediately). It exists for any
"just run python3 main.py" deploy target that skips entrypoint.sh
entirely (Replit, or any other host where the process manager calls this
file directly) — those need the same clone+build+start done here, in
Python, at bot startup instead.

BUG FIX (the version before this one): this used to build the server with
Deno + `--allow-scripts=npm:canvas`, based on a wrong assumption that the
provider needs the "canvas" npm package. It doesn't — confirmed against a
known-working reference deploy of the same tool, which only ever needs
Node.js + npm + a plain TypeScript compile (`npm ci && npx tsc`). Canvas
needs a full C/C++ toolchain plus cairo/pango system libraries to
compile, which is exactly the kind of native build that silently fails
on a lot of minimal hosts — so the old approach was solving a problem
the real tool doesn't have, while adding a genuinely fragile step. This
version drops Deno and canvas entirely; the only things it needs are
git, node, and npm.

THIS CAN STILL LEGITIMATELY FAIL on some hosts (no git/node/npm on
PATH, e.g. Render's native Python buildpack with no Docker/apt access),
and that's handled gracefully on purpose — every step here is wrapped so
a failure just leaves is_ready() False forever, and ytdlp_downloader.py
still requests "web" regardless (yt-dlp's own plugin just won't have a
token to attach, same graceful degradation as not asking for "web" at
all). Check get_status() (wired into /potstatus in main.py) to see
exactly which step failed, instead of grepping raw logs for
"[pot-provider]" lines.
"""

import logging
import os
import shutil
import socket
import subprocess
import threading
import time

logger = logging.getLogger("faphouse_bot")

POT_VERSION = "1.3.1"
POT_REPO_URL = "https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git"
POT_PORT = 4416
POT_PING_URL = f"http://127.0.0.1:{POT_PORT}/ping"

# Kept inside the project directory (not /opt, which entrypoint.sh's own
# Docker-image-build-time clone uses) so this fallback path works
# regardless of filesystem permissions on whatever host it runs on, and
# persists across restarts on platforms with persistent storage.
POT_HOME = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".bgutil-pot")
POT_SERVER_DIR = os.path.join(POT_HOME, "server")
POT_MAIN_JS = os.path.join(POT_SERVER_DIR, "build", "main.js")

_ready = threading.Event()
_server_process: subprocess.Popen | None = None
_status = "not started"   # human-readable, updated at each setup step — see get_status()
_status_lock = threading.Lock()


def _set_status(s: str):
    global _status
    with _status_lock:
        _status = s
    logger.info(f"[pot-provider] {s}")


def is_ready() -> bool:
    """True once the local PO-token HTTP server is actually up and
    accepting connections on 127.0.0.1:4416 — whether that's because
    entrypoint.sh started it at container boot (Docker) or this module's
    own start_background() did (non-Docker fallback)."""
    return _ready.is_set()


def get_status() -> str:
    """Human-readable status: what's currently happening, or exactly
    which step failed and why. Wired into /potstatus in main.py for a
    one-message answer to "why is YouTube still capped at 360p" instead
    of guessing blind or digging through raw logs."""
    with _status_lock:
        return _status


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _run(cmd: list, cwd: str = None, timeout: int = 300) -> bool:
    """Run a setup command to completion, logging output only on
    failure (success is just a debug line — this can be noisy)."""
    try:
        result = subprocess.run(
            cmd, cwd=cwd, timeout=timeout,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        )
        if result.returncode != 0:
            out = (result.stdout or b"").decode("utf-8", errors="replace")[-2000:]
            logger.warning(f"[pot-provider] command failed ({' '.join(cmd)}): {out}")
            return False
        logger.debug(f"[pot-provider] ok: {' '.join(cmd)}")
        return True
    except subprocess.TimeoutExpired:
        logger.warning(f"[pot-provider] command timed out ({timeout}s): {' '.join(cmd)}")
        return False
    except Exception as e:
        logger.warning(f"[pot-provider] command errored ({' '.join(cmd)}): {e}")
        return False


def _setup_and_start():
    """Runs once, in a background thread — never blocks bot startup.
    Every failure path just returns early with is_ready() left False,
    and _set_status() records exactly which step it was on (see
    get_status())."""
    global _server_process

    # Docker path: entrypoint.sh already cloned+built+started this at
    # image-build/container-boot time, well before main.py (and this
    # thread) even runs. Nothing to do — just confirm and exit.
    if _port_open("127.0.0.1", POT_PORT):
        _ready.set()
        _set_status("ready — server already up (started by entrypoint.sh, or a previous boot on this host).")
        return

    _set_status("checking for git/node/npm...")
    missing = [t for t in ("git", "node", "npm") if not shutil.which(t)]
    if missing:
        _set_status(
            f"failed: {', '.join(missing)} not found on PATH — skipping PO-token "
            f"provider setup, YouTube stays on the capped-quality client list."
        )
        return

    # ── 1. Clone the server code (skip if already present from a previous boot on the same disk) ──
    if not os.path.isdir(os.path.join(POT_SERVER_DIR, "src")):
        _set_status(f"cloning provider server ({POT_VERSION}) to {POT_HOME}...")
        if os.path.isdir(POT_HOME):
            shutil.rmtree(POT_HOME, ignore_errors=True)
        if not _run(
            ["git", "clone", "--single-branch", "--branch", POT_VERSION,
             "--depth", "1", POT_REPO_URL, POT_HOME],
            timeout=60,
        ):
            _set_status("failed: git clone of the provider repo didn't succeed (see warning above for the command output).")
            return

    # ── 2. Build it — plain npm + TypeScript compile, no native deps ──
    if not os.path.exists(POT_MAIN_JS):
        _set_status("building provider server (npm ci && npx tsc) — first boot only, can take a minute...")
        if not _run(["npm", "ci"], cwd=POT_SERVER_DIR, timeout=300):
            _set_status("failed: npm ci didn't succeed (see warning above for the command output).")
            return
        if not _run(["npx", "tsc"], cwd=POT_SERVER_DIR, timeout=180):
            _set_status("failed: npx tsc (build) didn't succeed (see warning above for the command output).")
            return

    # ── 3. Start the HTTP server in the background ──
    _set_status(f"starting provider HTTP server on 127.0.0.1:{POT_PORT}...")
    try:
        _server_process = subprocess.Popen(
            ["node", POT_MAIN_JS],
            cwd=POT_SERVER_DIR,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        _set_status(f"failed: couldn't start the server process: {e}")
        return

    # Give it a few seconds to come up, polling rather than a fixed sleep.
    for _ in range(15):
        if _port_open("127.0.0.1", POT_PORT):
            _ready.set()
            _set_status("ready — PO-token provider is up, YouTube's full quality ladder is available.")
            return
        if _server_process.poll() is not None:
            _set_status(f"failed: server process exited early (code {_server_process.returncode}).")
            return
        time.sleep(1)
    _set_status("failed: server didn't come up within 15s — giving up for this boot.")


def start_background():
    """Call once at bot startup. Non-blocking — runs setup in a daemon
    thread so a slow/failed clone+build never delays the bot itself from
    starting up and serving everything else. Safe to call unconditionally
    on every platform: it's a fast no-op on Docker (entrypoint.sh already
    has the server up by the time this checks)."""
    threading.Thread(target=_setup_and_start, name="pot-provider-setup", daemon=True).start()
