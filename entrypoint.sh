#!/bin/sh
# Starts the bgutil PO-token HTTP server (built at image-build time — see
# Dockerfile) in the background, waits a moment, then prints whether it's
# actually reachable before starting the bot — so "is it working" is
# answered by the container logs on every boot instead of staying a
# silent guess. Replaces the old approach of building+starting this from
# inside pot_provider.py at bot runtime with Deno — see the Dockerfile's
# own comment for why that was fragile (an unnecessary "canvas" native
# build) and pot_provider.py's docstring for what it does now instead
# (a Node.js-based fallback for non-Docker hosts where this script never
# runs, e.g. Render's native Python buildpack).
node /opt/bgutil-pot/server/build/main.js > /tmp/bgutil-pot.log 2>&1 &
BGUTIL_PID=$!

# Poll for up to ~15s instead of a single fixed sleep — on slower/cold
# instances the Node server can take longer than a couple seconds to
# bind its port.
BGUTIL_UP=0
for i in $(seq 1 15); do
    if wget -q -O- http://127.0.0.1:4416/ping >/dev/null 2>&1; then
        BGUTIL_UP=1
        break
    fi
    sleep 1
done

if [ "$BGUTIL_UP" = "1" ] && kill -0 "$BGUTIL_PID" 2>/dev/null; then
    echo "[bgutil-pot] OK — PO token server is up on :4416 (YouTube 'web' client should get the full quality ladder)"
else
    echo "[bgutil-pot] WARNING — PO token server did not come up after 15s. Last log lines:"
    tail -n 20 /tmp/bgutil-pot.log 2>/dev/null
    echo "[bgutil-pot] Bot will still run — YouTube just falls back to tv_embedded/android's lower-res formats."
fi

# ytnode — a cookie-free ytdl-core-based fallback YouTube downloader
# (ytnode_client.py) for when the PO-token path above still comes back
# degraded. Same start-in-background-then-poll pattern as bgutil-pot
# above; a failure here is non-fatal the same way — ytdlp_downloader.py's
# fallback is already written to just skip it (ytnode_client.is_ready()
# returning False) and use whatever yt-dlp's own result was.
node /app/ytnode/server.js > /tmp/ytnode.log 2>&1 &
YTNODE_PID=$!

YTNODE_UP=0
for i in $(seq 1 15); do
    if wget -q -O- http://127.0.0.1:4417/health >/dev/null 2>&1; then
        YTNODE_UP=1
        break
    fi
    sleep 1
done

if [ "$YTNODE_UP" = "1" ] && kill -0 "$YTNODE_PID" 2>/dev/null; then
    echo "[ytnode] OK — cookie-free YouTube fallback is up on :4417"
else
    echo "[ytnode] WARNING — did not come up after 15s. Last log lines:"
    tail -n 20 /tmp/ytnode.log 2>/dev/null
    echo "[ytnode] Bot will still run — YouTube quality just relies on the PO-token path alone."
fi

exec python3 main.py
