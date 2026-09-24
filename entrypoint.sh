#!/bin/sh
# entrypoint.sh — starts all background services then launches the bot.
#
# KEY FIX: bgutil-pot binds to [::]:4416 by default (all interfaces).
# Render detects any externally-visible port as the "primary port" and
# triggers a full redeploy restart — causing the double-start seen in logs.
# Fix: pass --host 127.0.0.1 so it only listens on loopback, which Render
# ignores. The bot itself binds Render's $PORT via keep_alive.py.
#
# ALSO: start the bot process FIRST (background) so it binds $PORT quickly,
# THEN poll the sidecar services. This guarantees Render sees the health
# server on $PORT before any 15s timeout and never triggers a port-change
# restart.

# ── 1. Start the bot immediately (background) so it binds $PORT ──────────
python3 main.py &
BOT_PID=$!
echo "[entrypoint] bot started (pid $BOT_PID) — binding Render \$PORT..."

# Brief wait so the health server (keep_alive.py) has time to come up
# before we go into the sidecar polls below.
sleep 3

# ── 2. bgutil PO-token server ─────────────────────────────────────────────
# --host 127.0.0.1 is CRITICAL: without it Node binds [::]:4416 (all
# interfaces), which Render misidentifies as the primary web-service port
# and triggers an unnecessary redeploy restart.
node /opt/bgutil-pot/server/build/main.js \
    --host 127.0.0.1 \
    > /tmp/bgutil-pot.log 2>&1 &
BGUTIL_PID=$!

BGUTIL_UP=0
for i in $(seq 1 45); do
    if wget -q -O- http://127.0.0.1:4416/ping >/dev/null 2>&1; then
        BGUTIL_UP=1
        break
    fi
    # Also abort early if the process already died
    kill -0 "$BGUTIL_PID" 2>/dev/null || break
    sleep 1
done

if [ "$BGUTIL_UP" = "1" ] && kill -0 "$BGUTIL_PID" 2>/dev/null; then
    echo "[bgutil-pot] OK — PO token server is up on 127.0.0.1:4416"
else
    echo "[bgutil-pot] WARNING — PO token server did not come up after 45s. Last log lines:"
    tail -n 20 /tmp/bgutil-pot.log 2>/dev/null
    echo "[bgutil-pot] Bot will still run — YouTube just falls back to tv_embedded/android's lower-res formats."
fi

# ── 3. ytnode cookie-free YouTube fallback ────────────────────────────────
node /app/ytnode/server.js > /tmp/ytnode.log 2>&1 &
YTNODE_PID=$!

YTNODE_UP=0
for i in $(seq 1 45); do
    if wget -q -O- http://127.0.0.1:4417/health >/dev/null 2>&1; then
        YTNODE_UP=1
        break
    fi
    kill -0 "$YTNODE_PID" 2>/dev/null || break
    sleep 1
done

if [ "$YTNODE_UP" = "1" ] && kill -0 "$YTNODE_PID" 2>/dev/null; then
    echo "[ytnode] OK — cookie-free YouTube fallback is up on :4417"
else
    echo "[ytnode] WARNING — did not come up after 45s. Last log lines:"
    tail -n 20 /tmp/ytnode.log 2>/dev/null
    echo "[ytnode] Bot will still run — YouTube quality just relies on the PO-token path alone."
fi

# ── 4. FlareSolverr headless-Chrome Cloudflare solver ────────────────────
Xvfb :99 -screen 0 1024x768x24 -nolisten tcp > /tmp/xvfb.log 2>&1 &
export DISPLAY=:99

HOST=127.0.0.1 PORT=8191 LOG_LEVEL=info \
    /opt/flaresolver/venv/bin/python /opt/flaresolver/src/flaresolverr.py \
    > /tmp/flaresolverr.log 2>&1 &
FLARESOLVERR_PID=$!

FLARESOLVERR_UP=0
for i in $(seq 1 45); do
    if wget -q -O- http://127.0.0.1:8191/health >/dev/null 2>&1; then
        FLARESOLVERR_UP=1
        break
    fi
    kill -0 "$FLARESOLVERR_PID" 2>/dev/null || break
    sleep 1
done

if [ "$FLARESOLVERR_UP" = "1" ] && kill -0 "$FLARESOLVERR_PID" 2>/dev/null; then
    echo "[flaresolverr] OK — Cloudflare-challenge solver is up on 127.0.0.1:8191"
else
    echo "[flaresolverr] WARNING — did not come up after 45s. Last log lines:"
    tail -n 20 /tmp/flaresolverr.log 2>/dev/null
    echo "[flaresolverr] Bot will still run — cf_bypass.py just won't get an automatic retry on Cloudflare 403s (cloudscraper's in-process solver still works on its own)."
fi

# ── 5. Wait for the bot process (it's already running) ───────────────────
wait $BOT_PID
