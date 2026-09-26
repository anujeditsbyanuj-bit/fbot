import { spawn, execSync } from 'node:child_process';
import http from 'node:http';
import fs from 'node:fs';
import path from 'node:path';
// BUG FIX: this file calls `new WebSocket(wsUrl)` further down relying on
// it as a Node global — only actually built into Node 22+. This
// project's own Dockerfile installs Node 20.x (see the nodesource
// setup_20.x line, shared with the ytnode/ and bgutil-pot Node servers
// already in this project), where WebSocket doesn't exist at all yet —
// every run would fail immediately with "ReferenceError: WebSocket is
// not defined". The `ws` package (declared in this folder's own
// package.json) is the standard, tiny polyfill for exactly this.
import WebSocket from 'ws';

function findBinaryRecursive(dir, maxDepth = 4) {
  if (!dir || !fs.existsSync(dir) || maxDepth <= 0) return null;
  try {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    for (const entry of entries) {
      const fullPath = path.join(dir, entry.name);
      if (entry.isFile() && (entry.name === 'chrome' || entry.name === 'chromium' || entry.name === 'chrome.exe')) {
        return fullPath;
      }
      if (entry.isDirectory()) {
        const found = findBinaryRecursive(fullPath, maxDepth - 1);
        if (found) return found;
      }
    }
  } catch (e) {}
  return null;
}

function findBrowser() {
  const directPaths = [
    '/usr/bin/chromium',
    '/usr/bin/chromium-browser',
    '/usr/bin/google-chrome',
    '/usr/bin/google-chrome-stable',
    'C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe',
    'C:\\Program Files (x86)\\Microsoft\\Edge\\Application\\msedge.exe',
    'C:\\Program Files\\Microsoft\\Edge\\Application\\msedge.exe'
  ];
  for (const p of directPaths) {
    if (fs.existsSync(p)) return p;
  }

  const home = process.env.HOME || '/root';
  const searchDirs = [
    path.join(home, '.cache', 'ms-playwright'),
    path.join(home, '.cache', 'puppeteer'),
    path.join(process.cwd(), 'browser'),
    '/opt/render/project/src/browser'
  ];
  for (const d of searchDirs) {
    const found = findBinaryRecursive(d);
    if (found) return found;
  }

  // Auto-download standalone chrome on Linux if missing
  if (process.platform === 'linux') {
    try {
      execSync('npx -y @puppeteer/browsers install chrome@stable --path ./browser', { stdio: 'inherit', timeout: 90000 });
      const downloaded = findBinaryRecursive(path.join(process.cwd(), 'browser'));
      if (downloaded) return downloaded;
    } catch (e) {}
  }

  return process.platform === 'win32' ? 'chrome.exe' : 'chromium';
}

function cleanFilename(rawName, extension) {
  const ext = extension || 'mp4';
  if (!rawName) return `DiskWala_Video.${ext}`;
  let cleaned = rawName.replace(/[*_]+/g, ' ').replace(/\s+/g, ' ').trim();
  cleaned = cleaned.replace(/\.(mp|m|mp4)$/i, '').trim();
  if (!cleaned || cleaned.length < 2) {
    return `DiskWala_Video.${ext}`;
  }
  return `${cleaned}.${ext}`;
}

export async function resolveDiskwalaLink(linkId) {
  const browserPath = findBrowser();
  const port = 9330 + Math.floor(Math.random() * 500);
  const targetUrl = 'https://diskwala.net/';
  const tempProfile = path.join(process.env.TEMP || '/tmp', `cdp_dw_${Date.now()}_${Math.random().toString(36).substring(2, 7)}`);

  const args = [
    '--no-sandbox',
    '--disable-gpu',
    '--disable-extensions',
    '--disable-blink-features=AutomationControlled',
    '--no-first-run',
    '--no-default-browser-check',
    '--mute-audio',
    `--remote-debugging-port=${port}`,
    `--user-data-dir=${tempProfile}`
  ];
  if (process.platform !== 'win32' || process.env.HEADLESS === 'true') {
    args.unshift('--headless=new');
  }
  args.push(targetUrl);

  let browser = null;
  try {
    browser = spawn(browserPath, args, { stdio: 'ignore' });
  } catch (err) {
    return {
      success: false,
      error: `Browser launch failed: ${err.message}`
    };
  }

  let wsUrl = null;

  for (let i = 0; i < 30; i++) {
    await new Promise(r => setTimeout(r, 200));
    try {
      const data = await new Promise((resolve, reject) => {
        const req = http.get(`http://127.0.0.1:${port}/json`, res => {
          let buf = '';
          res.on('data', d => buf += d);
          res.on('end', () => resolve(buf));
        });
        req.on('error', reject);
      });

      const targets = JSON.parse(data);
      const pageTarget = targets.find(t => t.type === 'page' && (t.url.includes('diskwala') || t.webSocketDebuggerUrl));
      if (pageTarget) {
        wsUrl = pageTarget.webSocketDebuggerUrl;
        break;
      }
    } catch (e) {}
  }

  if (!wsUrl) {
    if (browser) browser.kill();
    try { fs.rmSync(tempProfile, { recursive: true, force: true }); } catch (e) {}
    return {
      success: false,
      error: 'Failed to connect to browser CDP'
    };
  }

  return new Promise((resolve) => {
    const ws = new WebSocket(wsUrl);
    let id = 1;
    const pendingReqs = new Map();
    let fileInfo = null;

    const timeoutTimer = setTimeout(() => {
      cleanup();
      buildResponse();
    }, 28000);

    function cleanup() {
      clearTimeout(timeoutTimer);
      try { ws.close(); } catch (e) {}
      if (browser) try { browser.kill(); } catch (e) {}
      setTimeout(() => {
        try { fs.rmSync(tempProfile, { recursive: true, force: true }); } catch (e) {}
      }, 500);
    }

    function buildResponse() {
      if (!fileInfo || !fileInfo.downloadUrl) {
        resolve({
          success: false,
          error: "Stream URL could not be resolved from diskwala.net"
        });
        return;
      }

      const name = fileInfo?.name ? cleanFilename(fileInfo.name, fileInfo.extension) : `DiskWala Video (${linkId})`;
      const rawBytes = fileInfo?.size || 0;
      const sizeStr = rawBytes > 0 ? `${(rawBytes / (1024 * 1024)).toFixed(2)} MB` : "HD Video";
      const directStreamUrl = fileInfo?.downloadUrl || null;
      const thumb = fileInfo?.thumb || null;

      const downloadFormats = [];
      if (directStreamUrl) {
        downloadFormats.push({
          label: `Direct Fast Download (${sizeStr})`,
          quality: `High Speed MP4 (${sizeStr})`,
          resolution: "Full HD",
          ext: fileInfo?.extension || "mp4",
          size: sizeStr,
          url: directStreamUrl,
          download_url: directStreamUrl,
          is_direct: true,
          mode: "diskwala"
        });
      }
      downloadFormats.push({
        label: "Open in DiskWala App",
        quality: "Official DiskWala App",
        resolution: "Mobile App",
        ext: "app",
        size: sizeStr,
        url: `https://www.diskwala.com/app/${linkId}`,
        download_url: `https://www.diskwala.com/app/${linkId}`,
        is_direct: false,
        mode: "diskwala"
      });

      const result = {
        success: true,
        surl: linkId,
        full_surl: linkId,
        title: name,
        uploader: "DiskWala Creator",
        size: sizeStr,
        size_bytes: rawBytes,
        duration_str: "HD Video",
        thumbnail: thumb,
        stream_url: directStreamUrl,
        proxy_stream_url: directStreamUrl,
        download_url: directStreamUrl || `https://www.diskwala.com/app/${linkId}`,
        download_formats: downloadFormats,
        is_hls: false,
        mode: "diskwala",
        playlist: [{
          index: 0,
          title: name,
          size: sizeStr,
          thumbnail: thumb,
          stream_url: directStreamUrl,
          download_url: directStreamUrl || `https://www.diskwala.com/app/${linkId}`
        }]
      };

      resolve(result);
    }

    function send(method, params = {}) {
      const msgId = id++;
      return new Promise((res, rej) => {
        pendingReqs.set(msgId, { res, rej });
        ws.send(JSON.stringify({ id: msgId, method, params }));
      });
    }

    ws.onmessage = async (evt) => {
      const msg = JSON.parse(evt.data);

      if (msg.id && pendingReqs.has(msg.id)) {
        const { res } = pendingReqs.get(msg.id);
        pendingReqs.delete(msg.id);
        res(msg.result);
        return;
      }

      if (msg.method === 'Network.responseReceived') {
        const { requestId, response } = msg.params;
        const url = response.url;
        if (url.includes('/web/api/status')) {
          try {
            const bodyRes = await send('Network.getResponseBody', { requestId });
            if (bodyRes && bodyRes.body) {
              const parsed = JSON.parse(bodyRes.body);
              if (parsed?.file?.downloadUrl) {
                fileInfo = parsed.file;
                cleanup();
                buildResponse();
              }
            }
          } catch (e) {}
        }
      }
    };

    ws.onerror = () => {
      cleanup();
      buildResponse();
    };

    ws.onopen = async () => {
      await send('Network.enable');
      await send('Page.enable');
      await send('Runtime.enable');

      // Wait until input with placeholder 'Paste Diskwala link' is rendered
      for (let i = 0; i < 20; i++) {
        await new Promise(r => setTimeout(r, 1000));
        const check = await send('Runtime.evaluate', {
          expression: `Boolean(document.querySelector('input[placeholder*="Diskwala"]'))`,
          returnByValue: true
        });
        if (check?.result?.value === true) break;
      }

      const targetLink = `https://www.diskwala.com/app/${linkId}`;

      await send('Runtime.evaluate', {
        expression: `(() => {
          const input = document.querySelector('input[placeholder*="Diskwala"]');
          if (input) {
            const nativeInputValueSetter = Object.getOwnPropertyDescriptor(window.HTMLInputElement.prototype, "value").set;
            nativeInputValueSetter.call(input, "${targetLink}");
            input.dispatchEvent(new Event('input', { bubbles: true }));
            input.dispatchEvent(new Event('change', { bubbles: true }));
            
            setTimeout(() => {
              const btns = Array.from(document.querySelectorAll('button'));
              const getBtn = btns.find(b => b.innerText.trim() === 'Get');
              if (getBtn) {
                getBtn.click();
              }
            }, 300);
          }
        })()`
      });

      // Poll for downloadUrl or video links appearing in the DOM
      for (let attempt = 0; attempt < 15; attempt++) {
        await new Promise(r => setTimeout(r, 1000));
        const pageCheck = await send('Runtime.evaluate', {
          expression: `(() => {
            const links = Array.from(document.querySelectorAll('a, video, source')).map(a => a.href || a.src).filter(Boolean);
            const media = links.find(u => u.includes('s3dwfubotmark') || (u.includes('diskwala.com') && u.includes('.mp4')));
            return media || null;
          })()`,
          returnByValue: true
        });

        if (pageCheck?.result?.value) {
          fileInfo = { downloadUrl: pageCheck.result.value };
          cleanup();
          buildResponse();
          return;
        }
      }
    };
  });
}

if (process.argv[1] && process.argv[1].endsWith('diskwala_engine.js')) {
  const argId = process.argv[2] || "69176413f37dbe35e7b299a5";
  resolveDiskwalaLink(argId).then(res => {
    console.log(JSON.stringify(res));
    process.exit(0);
  }).catch(err => {
    console.log(JSON.stringify({ success: false, error: err.message }));
    process.exit(1);
  });
}
