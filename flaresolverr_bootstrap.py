"""
Starts FlareSolverr (a real headless-Chrome Cloudflare-challenge solver —
see cf_bypass.py, which is what actually talks to it once it's up) plus
the Xvfb virtual display it needs, as background processes.

On a Docker deploy, entrypoint.sh + the Dockerfile already clone+build
FlareSolverr at IMAGE-BUILD time and start both it and Xvfb before
main.py ever runs (see entrypoint.sh's own comment above its FlareSolverr
section) — this module is a no-op there (start_background() finds the
server already reachable on its first check and returns immediately).

It exists for the same reason pot_provider.py does: any "just run
python3 main.py" deploy target that skips entrypoint.sh entirely
(Replit, or any other host where the process manager calls this file
directly) still needs FlareSolverr cloned, built, and started somehow —
this does that in Python, at bot startup, instead.

THIS CAN STILL LEGITIMATELY FAIL on a lot of hosts (no git on PATH, no
Xvfb/chromium installed outside the Docker image, no venv permissions,
etc.), and that's handled gracefully on purpose — every step here is
wrapped so a failure just leaves is_ready() False forever. cf_bypass.py's
own try_solve_flaresolverr() already treats "can't reach FlareSolverr" as
just one more "couldn't solve it" case, same as it would for a Docker
deploy where FlareSolverr simply didn't come up in time. Check
get_status() to see exactly which step failed instead of grepping raw
logs for "[flaresolverr]" lines.
"""

import logging
import os
import shutil
import socket
import subprocess
import threading
import time

logger = logging.getLogger("faphouse_bot")

FLARESOLVERR_HOME = "/opt/flaresolver"
FLARESOLVERR_VENV_PY = os.path.join(FLARESOLVERR_HOME, "venv", "bin", "python")
FLARESOLVERR_MAIN = os.path.join(FLARESOLVERR_HOME, "src", "flaresolverr.py")
FLARESOLVERR_REPO_URL = "https://github.com/FlareSolverr/FlareSolverr.git"
FLARESOLVERR_VERSION = "v3.5.0"
FLARESOLVERR_PORT = int(os.getenv("FLARESOLVERR_PORT", "8191"))
XVFB_DISPLAY = os.getenv("FLARESOLVERR_DISPLAY", ":99")

_ready = threading.Event()
_status = "not started"
_status_lock = threading.Lock()
_xvfb_process = None
_server_process = None


def _set_status(s: str):
    global _status
    with _status_lock:
        _status = s
    logger.info(f"[flaresolverr] {s}")


def is_ready() -> bool:
    """True once FlareSolverr's /health endpoint has answered — either
    because entrypoint.sh's own Docker-path already had it up before this
    module's start_background() even ran, or this module's own non-Docker
    fallback did."""
    return _ready.is_set()


def get_status() -> str:
    with _status_lock:
        return _status


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _health_ok() -> bool:
    # A plain TCP connect is enough to know something's listening, but
    # FlareSolverr's own /health endpoint confirms the Bottle app inside
    # it actually finished booting, not just that the socket is bound.
    try:
        import urllib.request
        with urllib.request.urlopen(f"http://127.0.0.1:{FLARESOLVERR_PORT}/health", timeout=2) as r:
            return r.status == 200
    except Exception:
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
            logger.warning(f"[flaresolverr] command failed ({' '.join(cmd)}): {out}")
            return False
        logger.debug(f"[flaresolverr] ok: {' '.join(cmd)}")
        return True
    except subprocess.TimeoutExpired:
        logger.warning(f"[flaresolverr] command timed out ({timeout}s): {' '.join(cmd)}")
        return False
    except Exception as e:
        logger.warning(f"[flaresolverr] command errored ({' '.join(cmd)}): {e}")
        return False


def _setup_and_start():
    """Runs once, in a background thread — never blocks bot startup.
    Every failure path just returns early with is_ready() left False."""
    global _xvfb_process, _server_process

    # Docker path: entrypoint.sh already has this up by the time main.py
    # (and this thread) even runs. Nothing to do — just confirm and exit.
    if _port_open("127.0.0.1", FLARESOLVERR_PORT):
        _ready.set()
        _set_status("ready — server already up (started by entrypoint.sh, or a previous boot on this host).")
        return

    _set_status("checking for git/chromium/Xvfb...")
    missing = [t for t in ("git", "Xvfb") if not shutil.which(t)]
    if not (shutil.which("chromium") or shutil.which("chromium-browser") or shutil.which("google-chrome")):
        missing.append("chromium")
    if missing:
        _set_status(
            f"failed: {', '.join(missing)} not found on PATH — skipping FlareSolverr "
            f"setup, cf_bypass.py falls back to cloudscraper-only (no FlareSolverr retry on harder challenges)."
        )
        return

    # ── 1. Clone + venv + install (skip if already present from a previous boot on the same disk) ──
    if not os.path.exists(FLARESOLVERR_MAIN):
        _set_status(f"cloning FlareSolverr ({FLARESOLVERR_VERSION}) to {FLARESOLVERR_HOME}...")
        if os.path.isdir(FLARESOLVERR_HOME):
            shutil.rmtree(FLARESOLVERR_HOME, ignore_errors=True)
        if not _run(
            ["git", "clone", "--branch", FLARESOLVERR_VERSION, "--depth", "1",
             FLARESOLVERR_REPO_URL, FLARESOLVERR_HOME],
            timeout=60,
        ):
            _set_status("failed: git clone of FlareSolverr didn't succeed (see warning above for the command output).")
            return

    if not os.path.exists(FLARESOLVERR_VENV_PY):
        _set_status("building FlareSolverr's own venv — first boot only, can take a minute...")
        if not _run(["python3", "-m", "venv", os.path.join(FLARESOLVERR_HOME, "venv")], timeout=60):
            _set_status("failed: venv creation didn't succeed (see warning above for the command output).")
            return
        if not _run([FLARESOLVERR_VENV_PY, "-m", "pip", "install", "--no-cache-dir", "--upgrade", "pip"], timeout=120):
            _set_status("failed: pip upgrade didn't succeed (see warning above for the command output).")
            return
        if not _run(
            [FLARESOLVERR_VENV_PY, "-m", "pip", "install", "--no-cache-dir",
             "-r", os.path.join(FLARESOLVERR_HOME, "requirements.txt")],
            timeout=300,
        ):
            _set_status("failed: FlareSolverr's own pip install didn't succeed (see warning above for the command output).")
            return

    # ── 2. Xvfb — FlareSolverr drives a real, non-headless-flagged Chrome for better Cloudflare stealth, so it needs a virtual display ──
    _set_status(f"starting Xvfb on {XVFB_DISPLAY}...")
    try:
        _xvfb_process = subprocess.Popen(
            ["Xvfb", XVFB_DISPLAY, "-screen", "0", "1024x768x24", "-nolisten", "tcp"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        _set_status(f"failed: couldn't start Xvfb: {e}")
        return
    time.sleep(1)  # give Xvfb a moment to bind the display before Chrome tries to use it

    # ── 3. Start the FlareSolverr HTTP server in the background ──
    _set_status(f"starting FlareSolverr HTTP server on 127.0.0.1:{FLARESOLVERR_PORT}...")
    env = {**os.environ, "DISPLAY": XVFB_DISPLAY, "HOST": "127.0.0.1", "PORT": str(FLARESOLVERR_PORT), "LOG_LEVEL": "info"}
    try:
        _server_process = subprocess.Popen(
            [FLARESOLVERR_VENV_PY, FLARESOLVERR_MAIN],
            cwd=FLARESOLVERR_HOME, env=env,
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
    except Exception as e:
        _set_status(f"failed: couldn't start the server process: {e}")
        return

    # Give it a few seconds to come up (a real browser launch is slower
    # than the plain Node servers pot_provider.py/ytnode wait on), polling
    # rather than a fixed sleep.
    for _ in range(25):
        if _health_ok():
            _ready.set()
            _set_status("ready — FlareSolverr is up, cf_bypass.py can now retry harder Cloudflare challenges through it.")
            return
        if _server_process.poll() is not None:
            _set_status(f"failed: server process exited early (code {_server_process.returncode}).")
            return
        time.sleep(1)
    _set_status("failed: server didn't come up within 25s — giving up for this boot.")


def start_background():
    """Call once at bot startup. Non-blocking — runs setup in a daemon
    thread so a slow/failed clone+build never delays the bot itself from
    starting up and serving everything else. Safe to call unconditionally
    on every platform: it's a fast no-op on Docker (entrypoint.sh already
    has the server up by the time this checks)."""
    threading.Thread(target=_setup_and_start, name="flaresolverr-bootstrap", daemon=True).start()
