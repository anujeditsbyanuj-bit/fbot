FROM python:3.12-slim

WORKDIR /app

# BUG FIX: this used to install Deno + a pile of native "canvas" build
# libs (libcairo2-dev/libpango1.0-dev/libjpeg-dev/libgif-dev/
# librsvg2-dev) for pot_provider.py's `deno install --allow-scripts=
# npm:canvas` step. That was based on a wrong assumption: the actual
# bgutil-ytdlp-pot-provider server (github.com/Brainicism/
# bgutil-ytdlp-pot-provider) is a plain TypeScript project — it never
# depended on the "canvas" npm package at all, so that whole native-
# build chain was solving a problem the real tool doesn't have, while
# adding a genuinely fragile step (canvas needs a full C/C++ toolchain
# + cairo/pango headers, which is exactly the kind of thing that fails
# silently on a lot of hosts). Confirmed against a known-working
# reference deploy of the same tool: it only ever needs Node.js + npm +
# a TypeScript compile (`npm ci && npx tsc`) — no native compilation,
# no canvas, nothing cairo/pango-related. Building it here at image-
# build time (once, cached in the image) instead of at bot-runtime
# (pot_provider.py's old approach) also means it's ready the instant
# the container starts, not "maybe ready a minute after boot".
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential gcc libffi-dev python3-dev ffmpeg git aria2 curl unzip \
    chromium xvfb fonts-liberation \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# yt-dlp PO-token provider (ytdlp_downloader.py's YouTube "web" client is
# the only one with the full 1080p/720p/480p/360p quality ladder, but it
# needs a valid PO token or YouTube silently caps/drops most of those
# formats). requirements.txt only pip-installs the *plugin* side (the
# Python glue that talks to a provider) — the actual token generator is
# this separate Node.js HTTP server, built here and started by
# entrypoint.sh below. Pinned to a known-working release tag instead of
# the moving default branch.
ENV BGUTIL_POT_VERSION=1.3.1
RUN git clone --single-branch --branch ${BGUTIL_POT_VERSION} --depth 1 \
        https://github.com/Brainicism/bgutil-ytdlp-pot-provider.git /opt/bgutil-pot \
    && cd /opt/bgutil-pot/server \
    && npm ci \
    && npx tsc

# FlareSolverr (Cloudflare JS-challenge bypass — see cf_bypass.py) runs in
# this SAME container as its own background process, in its own isolated
# venv (NOT pip-installed alongside this project's own requirements.txt —
# FlareSolverr pins its own selenium/undetected-chromedriver versions,
# and mixing those into this project's environment risks a real version
# conflict with something else here needing a different pinned version
# of a shared dependency). chromium+xvfb above are what it actually
# drives; flaresolverr_bootstrap.py starts both it and Xvfb at bot
# startup — see that file's docstring. Pinned to the v3.5.0 tag rather
# than a branch so this build doesn't silently start pulling in
# FlareSolverr's own future breaking changes.
RUN git clone --branch v3.5.0 --depth 1 https://github.com/FlareSolverr/FlareSolverr.git /opt/flaresolver \
    && python3 -m venv /opt/flaresolver/venv \
    && /opt/flaresolver/venv/bin/pip install --no-cache-dir --upgrade pip \
    && /opt/flaresolver/venv/bin/pip install --no-cache-dir -r /opt/flaresolver/requirements.txt

# YouTube now requires solving a JS challenge before yt-dlp can get a
# playable URL — yt-dlp needs an external JS runtime to do that. This
# used to install Deno for it; now that Node.js is already here for the
# PO-token server above, ytdlp_downloader.py points yt-dlp at Node
# instead (opts["js_runtimes"] = {"node": {}}) — one JS runtime for both
# jobs instead of two.

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip setuptools wheel
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir -U "yt-dlp[default]"

COPY . .

# ytnode/ — a cookie-free ytdl-core-based fallback YouTube downloader
# (see ytnode_client.py's module docstring) for when yt-dlp's own PO-
# token-gated "web" client result comes back degraded. Installed after
# COPY . . since package.json only exists in the image from that point
# on; --omit=dev keeps this to just express + @distube/ytdl-core, no
# dev tooling needed for a plain `node server.js` run.
RUN cd ytnode && npm install --omit=dev

# diskwala_engine/ — real-headless-Chrome (CDP) resolver for diskwala.net
# links (see diskwala.py's _resolve_diskwala_browser_engine() docstring
# and diskwala_engine.js's own header comment for why: it drives the
# actual diskwala.net web player rather than replicating its API calls,
# sidestepping Cloudflare entirely since it's a real browser). Reuses the
# same chromium binary already apt-installed for FlareSolverr above —
# findBrowser() in diskwala_engine.js checks /usr/bin/chromium first.
# --omit=dev keeps this to just the `ws` WebSocket polyfill (Node 20,
# installed above for ytnode/bgutil-pot, doesn't have WebSocket built in
# as a global the way Node 22+ does).
RUN cd diskwala_engine && npm install --omit=dev

RUN chmod +x /app/entrypoint.sh

CMD ["/app/entrypoint.sh"]
