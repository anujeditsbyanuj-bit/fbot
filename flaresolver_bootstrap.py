"""
Starts FlareSolverr (https://github.com/FlareSolverr/FlareSolverr) as a
background process in this SAME container — no separate service, no
FLARESOLVERR_URL to set. cf_bypass.py already defaults to
http://localhost:8191/v1, which is exactly where this brings FlareSolverr
up, so once this succeeds cf_bypass.py's existing try_solve()/
get_bypass_opts() just start working with zero extra configuration.

WHY THIS EXISTS: cf_bypass.py talks to FlareSolverr over HTTP but never
started it — it was optional infrastructure the person running this bot
had to set up themselves (a separate FlareSolverr service). This module
is that setup, automated: it starts Xvfb (a virtual display — Chromium
needs SOME display to render into even when driven headlessly this way)
and then FlareSolverr itself, both as background subprocesses, using the
isolated venv + chromium the Dockerfile sets up specifically for this
(NOT pip-installed alongside this project's own requirements.txt, to
avoid a real dependency-version conflict — see the Dockerfile's comment
on this).

THIS CAN LEGITIMATELY FAIL on a host that didn't build the Dockerfile's
FlareSolverr step (e.g. a non-Docker deploy) — handled the same way
pot_provider.py handles its own equivalent risk: every step is wrapped,
a failure just leaves is_ready() False forever, and cf_bypass.py's own
existing "FlareSolverr isn't reachable" fallback (a plain 403, same as
before any of this existed) takes over with nothing else breaking. Check
this module's logger output ("[flaresolverr] ...") to see exactly which
step failed if Cloudflare-gated links never start working.
"""

import logging
import os
import shutil
import socket
import subprocess
import threading
import time

logger = logging.getLogger("faphouse_bot")

FLARESOLVERR_HOME = "/opt/flaresolverr"
FLARESOLVERR_VENV_PY = os.path.join(FLARESOLVERR_HOME, "venv", "bin", "python")
FLARESOLVERR_ENTRY = os.path.join(FLARESOLVERR_HOME, "src", "flaresolverr.py")
FLARESOLVERR_PORT = 8191
XVFB_DISPLAY = ":99"

_ready = threading.Event()
_xvfb_process: subprocess.Popen | None = None
_fs_process: subprocess.Popen | None = None
_status = "not started"   # human-readable, updated at each setup step — see get_status()
_status_lock = threading.Lock()


def _set_status(s: str):
    global _status
    with _status_lock:
        _status = s
    logger.info(f"[flaresolverr] {s}")


def is_ready() -> bool:
    """True once FlareSolverr is actually up and accepting connections
    on 127.0.0.1:8191. Informational only — cf_bypass.py doesn't need to
    check this itself (its own requests just fail gracefully if it's not
    up yet), this is mainly for startup logging/diagnostics."""
    return _ready.is_set()


def get_status() -> str:
    """Human-readable status: what's currently happening, or exactly
    which step failed and why — same idea as pot_provider.py's
    get_status(), added for the same reason: is_ready() alone only ever
    says False, with no way to tell "Xvfb missing" apart from "this
    build has no /opt/flaresolverr" apart from "Chromium itself never
    came up" apart from any of the other things _setup_and_start() can
    bail on, short of grepping raw logs for "[flaresolverr]" lines. Wire
    this into an admin command (e.g. /flarestatus) for a one-message
    answer to "why did diskwala.net's Cloudflare challenge never get
    solved" instead of guessing blind."""
    with _status_lock:
        return _status


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _setup_and_start():
    """Runs once, in a background thread — never blocks bot startup.
    Every failure path just returns early with is_ready() left False,
    same shape as pot_provider.py's equivalent."""
    global _xvfb_process, _fs_process

    if _port_open("127.0.0.1", FLARESOLVERR_PORT):
        _ready.set()
        _set_status("ready — something was already listening on :8191, assuming it's FlareSolverr from a previous boot.")
        return

    if not os.path.exists(FLARESOLVERR_VENV_PY) or not os.path.exists(FLARESOLVERR_ENTRY):
        _set_status(
            "failed: not found at /opt/flaresolverr — this build didn't "
            "run the Dockerfile's FlareSolverr setup step (e.g. a non-Docker "
            "deploy). Cloudflare-JS-challenge links will keep hitting a plain "
            "403, same as before this existed."
        )
        return

    # ── 1. Start a virtual display for Chromium to render into ──
    if shutil.which("Xvfb"):
        try:
            _xvfb_process = subprocess.Popen(
                ["Xvfb", XVFB_DISPLAY, "-screen", "0", "1920x1080x24", "-nolisten", "tcp"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            time.sleep(1)  # give it a moment to bind before Chromium tries to use it
        except Exception as e:
            _set_status(f"warning: Xvfb failed to start: {e} — FlareSolverr will likely fail to launch Chromium. Continuing anyway.")
    else:
        _set_status("warning: Xvfb not found on PATH — FlareSolverr will likely fail to launch Chromium. Continuing anyway.")

    # ── 2. Start FlareSolverr itself, pointed at the system Chromium the Dockerfile installed ──
    env = os.environ.copy()
    env["DISPLAY"] = XVFB_DISPLAY
    env.setdefault("BROWSER_EXECUTABLE_PATH", "/usr/bin/chromium")
    env.setdefault("HOST", "127.0.0.1")
    env.setdefault("PORT", str(FLARESOLVERR_PORT))
    env.setdefault("LOG_LEVEL", "warning")  # its own request-by-request logging is noisy at "info"

    _set_status(f"starting FlareSolverr process on 127.0.0.1:{FLARESOLVERR_PORT}...")
    try:
        _fs_process = subprocess.Popen(
            [FLARESOLVERR_VENV_PY, FLARESOLVERR_ENTRY],
            cwd=os.path.join(FLARESOLVERR_HOME, "src"),
            env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        _set_status(f"failed: couldn't start the process: {e}")
        return

    # Give it time to come up — a real headless-Chromium launch + FlareSolverr's
    # own startup self-test genuinely takes longer than pot-provider's plain
    # HTTP server, so this polls longer (60s) before giving up for this boot.
    for _ in range(60):
        if _port_open("127.0.0.1", FLARESOLVERR_PORT):
            _ready.set()
            _set_status("ready — Cloudflare-JS-challenge links (e.g. diskwala.net's terabox API) can now be bypassed automatically.")
            return
        if _fs_process.poll() is not None:
            _set_status(f"failed: process exited early (code {_fs_process.returncode}) — likely Chromium itself couldn't launch (missing libs, or Xvfb didn't actually bind — see the warning above if there was one).")
            return
        time.sleep(1)
    _set_status("failed: didn't come up within 60s — giving up for this boot.")


def start_background():
    """Call once at bot startup (see main.py, next to pot_provider's own
    start_background() call). Non-blocking — runs setup in a daemon
    thread so a slow/failed launch never delays the bot itself from
    starting up and serving everything else."""
    threading.Thread(target=_setup_and_start, name="flaresolverr-setup", daemon=True).start()
