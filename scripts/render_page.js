// scripts/render_page.js
// Versão ajustada: para hosts prioritários aceita status<400 imediatamente (p/ velocidade),
// host-specific strategy order, e logging mais claro.

const fs = require('fs');
const path = require('path');
const https = require('https');
const { chromium } = require('playwright');

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function simpleHttpGet(url, outPath, headers = {}, timeout = 35000) {
  return new Promise((resolve, reject) => {
    try {
      const opts = new URL(url);
      opts.headers = Object.assign({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9'
      }, headers);

      const req = https.get(opts, (res) => {
        const status = res.statusCode || 0;
        if (status >= 400) {
          res.resume();
          return reject(new Error(`HTTP status ${status}`));
        }
        const chunks = [];
        res.on('data', c => chunks.push(c));
        res.on('end', () => {
          try {
            const buf = Buffer.concat(chunks);
            const data = buf.toString('utf8');
            fs.writeFileSync(outPath, data, { encoding: 'utf-8' });
            resolve({ ok: true, status });
          } catch (e) { reject(e); }
        });
      });

      req.on('error', (err) => reject(err));
      req.setTimeout(timeout, () => { req.destroy(new Error('timeout')); });
    } catch (e) { reject(e); }
  });
}

async function launchContext(strat) {
  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    headless: true
  });

  const context = await browser.newContext({
    userAgent: strat.userAgent,
    locale: strat.locale || 'en-US',
    viewport: strat.viewport || { width: 1280, height: 800 },
    extraHTTPHeaders: strat.extraHTTPHeaders || {}
  });

  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
    Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4] });
  });

  return { browser, context };
}

async function tryNavigate(url, strat) {
  let bc = null;
  try {
    const { browser, context } = await launchContext(strat);
    bc = { browser, context };
    const page = await context.newPage();

    await page.route('**/*', (route) => {
      const req = route.request();
      const rurl = req.url();
      const resource = req.resourceType();
      const blockedDomains = ['googlesyndication','doubleclick','google-analytics','adsystem','adservice','scorecardresearch','facebook.net','facebook.com','ads-twitter','amazon-adsystem'];
      for (const d of blockedDomains) if (rurl.includes(d)) return route.abort();
      if (strat.blockImages && (resource === 'image' || resource === 'media')) return route.abort();
      if (strat.blockStyles && (resource === 'stylesheet' || resource === 'font')) return route.abort();
      return route.continue();
    });

    page.setDefaultNavigationTimeout(strat.timeout || 45000);
    console.log(`  -> Navigating with waitUntil="${strat.waitUntil}" (timeout ${strat.timeout})`);
    const resp = await page.goto(url, { waitUntil: strat.waitUntil, timeout: strat.timeout, referer: strat.referer || undefined });
    const status = resp ? resp.status() : null;
    return { ok: true, page, browser, context, status };
  } catch (err) {
    if (bc && bc.browser) {
      try { await bc.browser.close(); } catch(e){ }
    }
    return { ok: false, error: err };
  }
}

function looksLikeBlockPage(html) {
  if (!html || html.length < 200) return true;
  const lowered = html.toLowerCase();
  const blockers = ['access denied','forbidden','blocked','cloudflare','bot detected','captcha','please enable javascript','are you human','request blocked','you don\'t have permission'];
  for (const b of blockers) if (lowered.includes(b)) return true;
  return false;
}

async function render(url, outPath) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });

  const commonHeaders = { 'accept-language': 'en-US,en;q=0.9' };

  const strategies = {
    A: {
      name: 'A - fast (block images & styles)',
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0 Safari/537.36',
      extraHTTPHeaders: commonHeaders,
      blockImages: true,
      blockStyles: true,
      waitUntil: 'domcontentloaded',
      timeout: 20000
    },
    B: {
      name: 'B - allow styles/fonts (recommended)',
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
      extraHTTPHeaders: Object.assign({}, commonHeaders, { 'sec-ch-ua': '"Chromium";v="120", "Google Chrome";v="120"' }),
      blockImages: true,
      blockStyles: false,
      waitUntil: 'networkidle',
      timeout: 50000,
      referer: url
    },
    C: {
      name: 'C - human-like (allow everything, longer)',
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
      extraHTTPHeaders: commonHeaders,
      blockImages: false,
      blockStyles: false,
      waitUntil: 'networkidle',
      timeout: 90000,
      referer: url
    }
  };

  //HOST PREFERENCE: try the strategy that tends to work first for each host
  const hostPrefs = {
    'dzone.com': ['B','C','A'],
    'eetimes.com': ['B','C','A'],
    'mdpi.com': ['C','B','A'],
    'medscape.com': ['C','B','A'],
    'stocktwits.com': ['A','B','C'],
    'journals.lww.com': ['C','B','A'],
    // fallback default: A,B,C
  };

  // Hosts for which we will accept/save immediately when main response.status < 400
  // (this is the speed optimization you asked for)
  const acceptOn200Hosts = ['dzone.com','eetimes.com','mdpi.com','medscape.com','stocktwits.com','journals.lww.com'];

  const hostKey = Object.keys(hostPrefs).find(h => url.includes(h));
  const order = hostKey ? hostPrefs[hostKey] : ['A','B','C'];

  let lastErr = null;

  // Special-case: for eetimes try simple GET first (sometimes playwright/http2 fails in GH runners)
  if (url.includes('eetimes.com')) {
    console.warn('Host is eetimes.com — attempting simple HTTPS GET fallback first (special-case)');
    try {
      const tmpOut = outPath + '.httpget.tmp';
      const r = await simpleHttpGet(url, tmpOut, {}, 45000);
      if (r && r.ok) {
        const html = fs.readFileSync(tmpOut, 'utf8');
        if (!looksLikeBlockPage(html) && html.length > 2048) {
          fs.renameSync(tmpOut, outPath);
          console.log(`eetimes: simple HTTPS GET succeeded -> ${outPath} (status ${r.status})`);
          return;
        } else {
          try { fs.unlinkSync(tmpOut); } catch(e){ }
        }
      }
    } catch (e) {
      console.warn('eetimes: simple HTTPS GET failed or returned small/block content:', e && e.message ? e.message : e);
    }
  }

  for (let idx = 0; idx < order.length; idx++) {
    const key = order[idx];
    const strat = strategies[key];
    console.log(`Strategy attempt ${idx+1}/${order.length}: ${strat.name} for ${url}`);
    const result = await tryNavigate(url, strat);

    if (!result.ok) {
      console.warn(`  Strategy ${strat.name} failed to navigate: ${result.error && result.error.message ? result.error.message : result.error}`);
      lastErr = result.error;
      await delay(500 * (idx+1));
      continue;
    }

    const { page, browser, status } = result;
    console.log(`  Main response status: ${status}`);

    // If host is in acceptOn200Hosts and status < 400 => accept immediately (speed)
    if (status && status < 400 && acceptOn200Hosts.find(h=>url.includes(h))) {
      try {
        const content = await page.content();
        fs.writeFileSync(outPath, content, { encoding: 'utf-8' });
        console.log(`Quick-accepted ${url} -> ${outPath} (status: ${status}) for host in acceptOn200Hosts`);
        try { await browser.close(); } catch(e){}
        return;
      } catch (e) {
        console.warn('Quick-accept failed to write content:', e && e.message ? e.message : e);
        try { await browser.close(); } catch(e){}
        lastErr = e;
        continue;
      }
    }

    if (status === 403) {
      console.warn(`  Got 403 on ${url} with strategy ${strat.name} — will try next or fallback`);
      try { await browser.close(); } catch(e){}
      lastErr = new Error('403 Forbidden');
      await delay(400);
      continue;
    }

    await delay(800 + idx * 400);

    try {
      await page.waitForSelector('article, main, #content, .post, .news, .press, .investors_events_bodybox', { timeout: 1500 }).catch(()=>{});
      const content = await page.content();
      if (looksLikeBlockPage(content)) {
        console.warn(`  Render looks like block page (detected) for ${url} using ${strat.name}`);
        try { await browser.close(); } catch(e){}
        lastErr = new Error('Detected block page after render');
        await delay(300);
        continue;
      }
      fs.writeFileSync(outPath, content, { encoding: 'utf-8' });
      const stats = fs.statSync(outPath);
      if (stats.size < 5120) {
        const sample = fs.readFileSync(outPath, 'utf8');
        if (looksLikeBlockPage(sample)) {
          fs.unlinkSync(outPath);
          console.warn(`  Rendered file too small and looks like a block page -> removed: ${outPath}`);
          lastErr = new Error('Rendered file small / block page');
          try { await browser.close(); } catch(e){}
          continue;
        }
      }
      console.log(`Rendered ${url} -> ${outPath} (status: ${status}) using strategy: ${strat.name}`);
      try { await browser.close(); } catch(e){}
      return;
    } catch (err) {
      console.error(`  Error writing content after strategy ${strat.name}: ${err && err.message ? err.message : err}`);
      lastErr = err;
      try { await browser.close(); } catch(e){}
      await delay(300);
      continue;
    }
  }

  console.warn('Playwright attempts exhausted — trying simple HTTPS GET fallback (only accept HTTP < 400)');
  try {
    await simpleHttpGet(url, outPath, {}, 50000);
    const html = fs.readFileSync(outPath, 'utf8');
    if (looksLikeBlockPage(html)) {
      fs.unlinkSync(outPath);
      throw new Error('Fallback HTTP returned block page');
    }
    const stats = fs.statSync(outPath);
    if (stats.size < 5120) {
      fs.unlinkSync(outPath);
      throw new Error('Fallback HTTP returned very small page');
    }
    console.log(`Fallback HTTPS GET succeeded: ${url} -> ${outPath}`);
    return;
  } catch (e) {
    console.warn('Fallback HTTPS GET failed or returned block page:', e && e.message ? e.message : e);
    lastErr = e;
  }

  throw lastErr || new Error('All strategies failed');
}

(async () => {
  try {
    const args = process.argv.slice(2);
    if (args.length < 2) {
      console.error('Usage: node scripts/render_page.js <url> <output_path>');
      process.exit(2);
    }
    const [url, outPath] = args;
    await render(url, outPath);
    process.exit(0);
  } catch (err) {
    console.error('Render failed:', err && (err.stack || err.message) ? (err.stack || err.message) : err);
    process.exit(1);
  }
})();
