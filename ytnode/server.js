/**
 * ytnode/server.js — local YouTube-download HTTP microservice.
 *
 * A ytdl-core-based alternative to ytdlp_downloader.py's YouTube path
 * (which is yt-dlp + the bgutil PO-token server — see pot_provider.py).
 * ytdl-core does its own signature/format extraction and needs no
 * cookies or PO token at all — but it's a completely separate library
 * fighting the same cat-and-mouse game against YouTube's own blocking
 * measures, so it is NOT guaranteed to work better than the PO-token
 * path; it's wired in (see ytnode_client.py) as a FALLBACK for when
 * yt-dlp's own quality ladder comes back degraded, not a replacement.
 *
 * Kept as one persistent process (started by entrypoint.sh, same
 * pattern as pot_provider.py's bgutil server) rather than spawning a
 * fresh `node` process per request — Node's own startup cost plus
 * require()'ing ytdl-core adds a few hundred ms every single call,
 * which would undo exactly the "quality fetch is slow" fixes already
 * made in ytdlp_downloader.py.
 *
 * Endpoints:
 *   GET /health                        -> { ok: true }
 *   GET /formats?url=<youtube url>     -> { title, duration, author, thumbnail, formats: [...] }
 *   GET /download?url=<...>&itag=<...> -> streams the muxed MP4 (itag omitted = highest quality)
 *
 * A format with hasAudio=false (true for basically every resolution
 * above 360p on YouTube — that's just how YouTube's own DASH streams
 * are split) gets its video downloaded separately from the best
 * available audio-only track and muxed together with ffmpeg (already
 * installed in this project's Docker image for other purposes) before
 * being sent back — the same "video-only + audio-only -> one file"
 * step yt-dlp does automatically, which a naive ytdl-core integration
 * would otherwise skip, producing a silent video with no sound.
 */

const express = require("express");
const ytdl = require("@distube/ytdl-core");
const { execFile } = require("child_process");
const fs = require("fs");
const os = require("os");
const path = require("path");

const PORT = process.env.YTNODE_PORT ? parseInt(process.env.YTNODE_PORT, 10) : 4417;
const app = express();

app.get("/health", (req, res) => res.json({ ok: true }));

app.get("/formats", async (req, res) => {
  const url = req.query.url;
  if (!url || !ytdl.validateURL(url)) {
    return res.status(400).json({ error: "invalid or missing url" });
  }
  try {
    const info = await ytdl.getInfo(url);
    const formats = info.formats
      .filter((f) => f.hasVideo)
      .map((f) => ({
        itag: f.itag,
        height: f.height || null,
        qualityLabel: f.qualityLabel || null,
        hasAudio: !!f.hasAudio,
        container: f.container || null,
        contentLength: f.contentLength || null,
      }));
    res.json({
      title: info.videoDetails.title,
      duration: parseInt(info.videoDetails.lengthSeconds, 10) || null,
      author: (info.videoDetails.author && info.videoDetails.author.name) || null,
      thumbnail:
        (info.videoDetails.thumbnails &&
          info.videoDetails.thumbnails.length &&
          info.videoDetails.thumbnails[info.videoDetails.thumbnails.length - 1].url) ||
        null,
      formats,
    });
  } catch (e) {
    res.status(500).json({ error: String((e && e.message) || e) });
  }
});

function streamToFile(readable, filePath) {
  return new Promise((resolve, reject) => {
    const ws = fs.createWriteStream(filePath);
    let settled = false;
    const fail = (err) => {
      if (settled) return;
      settled = true;
      reject(err);
    };
    readable.on("error", fail);
    ws.on("error", fail);
    ws.on("finish", () => {
      if (settled) return;
      settled = true;
      resolve();
    });
    readable.pipe(ws);
  });
}

function pickBestAudio(formats) {
  const audioOnly = formats.filter((f) => f.hasAudio && !f.hasVideo);
  if (!audioOnly.length) return null;
  audioOnly.sort((a, b) => (b.audioBitrate || 0) - (a.audioBitrate || 0));
  return audioOnly[0];
}

app.get("/download", async (req, res) => {
  const { url, itag } = req.query;
  if (!url || !ytdl.validateURL(url)) {
    return res.status(400).json({ error: "invalid or missing url" });
  }

  let tmpDir;
  try {
    const info = await ytdl.getInfo(url);
    const target = itag
      ? info.formats.find((f) => String(f.itag) === String(itag))
      : ytdl.chooseFormat(info.formats, { quality: "highest" });
    if (!target) {
      return res.status(404).json({ error: `format itag=${itag} not found for this video` });
    }

    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "ytnode-"));
    const finalPath = path.join(tmpDir, "final.mp4");

    if (target.hasAudio) {
      // Progressive format — already has audio, no muxing needed.
      await streamToFile(ytdl.downloadFromInfo(info, { format: target }), finalPath);
    } else {
      const audioFormat = pickBestAudio(info.formats);
      if (!audioFormat) {
        throw new Error("no audio-only track found to mux with this video-only format");
      }
      const videoOnlyPath = path.join(tmpDir, "video_only.mp4");
      const audioPath = path.join(tmpDir, "audio.m4a");
      await Promise.all([
        streamToFile(ytdl.downloadFromInfo(info, { format: target }), videoOnlyPath),
        streamToFile(ytdl.downloadFromInfo(info, { format: audioFormat }), audioPath),
      ]);
      await new Promise((resolve, reject) => {
        execFile(
          "ffmpeg",
          ["-y", "-i", videoOnlyPath, "-i", audioPath, "-c", "copy", finalPath],
          { timeout: 20 * 60 * 1000 },
          (err) => (err ? reject(err) : resolve())
        );
      });
    }

    res.setHeader("Content-Type", "video/mp4");
    res.setHeader("Content-Length", fs.statSync(finalPath).size);
    const rs = fs.createReadStream(finalPath);
    rs.pipe(res);
    rs.on("close", () => fs.rmSync(tmpDir, { recursive: true, force: true }));
    rs.on("error", () => fs.rmSync(tmpDir, { recursive: true, force: true }));
  } catch (e) {
    if (tmpDir) fs.rmSync(tmpDir, { recursive: true, force: true });
    if (!res.headersSent) {
      res.status(500).json({ error: String((e && e.message) || e) });
    }
  }
});

app.listen(PORT, "127.0.0.1", () => {
  console.log(`[ytnode] listening on 127.0.0.1:${PORT}`);
});
