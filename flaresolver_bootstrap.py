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


def is_ready() -> bool:
    """True once FlareSolverr is actually up and accepting connections
    on 127.0.0.1:8191. Informational only — cf_bypass.py doesn't need to
    check this itself (its own requests just fail gracefully if it's not
    up yet), this is mainly for startup logging/diagnostics."""
    return _ready.is_set()


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
        logger.info(f"[flaresolverr] something's already listening on :{FLARESOLVERR_PORT}, assuming it's ready.")
        _ready.set()
        return

    if not os.path.exists(FLARESOLVERR_VENV_PY) or not os.path.exists(FLARESOLVERR_ENTRY):
        logger.warning(
            "[flaresolverr] not found at /opt/flaresolverr — this build didn't "
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
            logger.warning(f"[flaresolverr] Xvfb failed to start: {e} — FlareSolverr will likely fail to launch Chromium.")
    else:
        logger.warning("[flaresolverr] Xvfb not found on PATH — FlareSolverr will likely fail to launch Chromium.")

    # ── 2. Start FlareSolverr itself, pointed at the system Chromium the Dockerfile installed ──
    env = os.environ.copy()
    env["DISPLAY"] = XVFB_DISPLAY
    env.setdefault("BROWSER_EXECUTABLE_PATH", "/usr/bin/chromium")
    env.setdefault("HOST", "127.0.0.1")
    env.setdefault("PORT", str(FLARESOLVERR_PORT))
    env.setdefault("LOG_LEVEL", "warning")  # its own request-by-request logging is noisy at "info"

    logger.info(f"[flaresolverr] starting on 127.0.0.1:{FLARESOLVERR_PORT}...")
    try:
        _fs_process = subprocess.Popen(
            [FLARESOLVERR_VENV_PY, FLARESOLVERR_ENTRY],
            cwd=os.path.join(FLARESOLVERR_HOME, "src"),
            env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        logger.warning(f"[flaresolverr] failed to start process: {e}")
        return

    # Give it time to come up — a real headless-Chromium launch + FlareSolverr's
    # own startup self-test genuinely takes longer than pot-provider's plain
    # HTTP server, so this polls longer (60s) before giving up for this boot.
    for _ in range(60):
        if _port_open("127.0.0.1", FLARESOLVERR_PORT):
            _ready.set()
            logger.info("[flaresolverr] ✅ up — Cloudflare-JS-challenge links can now be bypassed automatically.")
            return
        if _fs_process.poll() is not None:
            logger.warning(f"[flaresolverr] process exited early (code {_fs_process.returncode}).")
            return
        time.sleep(1)
    logger.warning("[flaresolverr] didn't come up within 60s — giving up for this boot.")


def start_background():
    """Call once at bot startup (see main.py, next to pot_provider's own
    start_background() call). Non-blocking — runs setup in a daemon
    thread so a slow/failed launch never delays the bot itself from
    starting up and serving everything else."""
    threading.Thread(target=_setup_and_start, name="flaresolverr-setup", daemon=True).start()
