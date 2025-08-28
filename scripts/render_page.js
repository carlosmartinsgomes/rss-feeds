// scripts/render_page.js
// Uso: node scripts/render_page.js <url> <output_path>
// Versão optimizada: per-host strategy set, timeouts reduzidos, fallback http-get para BusinessWire

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const https = require('https');

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function simpleHttpGet(url, outPath, headers = {}) {
  return new Promise((resolve, reject) => {
    const opts = new URL(url);
    opts.headers = Object.assign({
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9'
    }, headers);

    const req = https.get(opts, (res) => {
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => data += chunk);
      res.on('end', () => {
        try {
          fs.writeFileSync(outPath, data, { encoding: 'utf-8' });
          resolve({ ok: true, status: res.statusCode });
        } catch (e) {
          reject(e);
        }
      });
    });
    req.on('error', (err) => reject(err));
    req.setTimeout(20000, () => { req.destroy(new Error('timeout')); });
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

  // lightweight stealth
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
  });

  return { browser, context };
}

async function tryNavigate(url, strat, outPath) {
  let bc = null;
  try {
    const { browser, context } = await launchContext(strat);
    bc = { browser, context };
    const page = await context.newPage();

    // routing - basic blocking
    await page.route('**/*', (route) => {
      const req = route.request();
      const rurl = req.url();
      const resource = req.resourceType();
      const blocked = ['googlesyndication','doubleclick','google-analytics','adsystem','adservice','scorecardresearch','facebook.net','facebook.com','ads-twitter'];
      for (const d of blocked) if (rurl.includes(d)) return route.abort();
      if (strat.blockImages && (resource === 'image' || resource === 'media')) return route.abort();
      if (strat.blockStyles && (resource === 'stylesheet' || resource === 'font')) return route.abort();
      return route.continue();
    });

    page.setDefaultNavigationTimeout(strat.timeout || 30000);

    console.log(`  -> Navigating with waitUntil="${strat.waitUntil}" (timeout ${strat.timeout})`);
    const resp = await page.goto(url, { waitUntil: strat.waitUntil, timeout: strat.timeout, referer: strat.referer || undefined });
    const status = resp ? resp.status() : null;
    return { ok: true, page, browser, context, status };
  } catch (err) {
    if (bc && bc.browser) {
      try { await bc.browser.close(); } catch(e){}
    }
    return { ok: false, error: err };
  }
}

async function render(url, outPath) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });

  const commonHeaders = { 'accept-language': 'en-US,en;q=0.9' };

  // Strategies (smaller timeouts)
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
      timeout: 40000,
      referer: url
    }
    // not using C as default to save time
  };

  // per-host preferred strategies + quick timeouts
  const hostPrefs = {
    'inmodeinvestors.com': ['B'],      // use only B
    'darkreading.com': ['A'],          // use only A
    'iotworldtoday.com': ['A'],        // use only A
    // BusinessWire: try B then A (both short)
    'businesswire.com': ['B','A']
  };

  const hostKey = Object.keys(hostPrefs).find(h => url.includes(h));
  const order = hostKey ? hostPrefs[hostKey] : ['A','B'];

  let lastErr = null;
  for (let i = 0; i < order.length; i++) {
    const key = order[i];
    const strat = strategies[key];
    console.log(`Strategy attempt ${i+1}/${order.length}: ${strat.name} for ${url}`);
    const res = await tryNavigate(url, strat, outPath);

    if (!res.ok) {
      console.warn(`  Strategy ${strat.name} failed to navigate: ${res.error && res.error.message ? res.error.message : res.error}`);
      lastErr = res.error;
      // small backoff but short
      await delay(500 * (i+1));
      continue;
    }

    const { page, browser, status } = res;
    console.log(`  Main response status: ${status}`);

    if (status === 403) {
      console.warn(`  Got 403 on ${url} with strategy ${strat.name} — will try next or fallback`);
      try { await browser.close(); } catch(e){}
      lastErr = new Error('403 Forbidden');
      await delay(400);
      continue;
    }

    // small settle
    await delay(700);

    // non-fatal wait for some content
    try {
      await page.waitForSelector('article, main, #content, .post, .press, .news, .investors_events_bodybox', { timeout: 1500 });
    } catch(e){}

    try {
      const content = await page.content();
      fs.writeFileSync(outPath, content, { encoding: 'utf-8' });
      console.log(`Rendered ${url} -> ${outPath} (status: ${status}) using ${strat.name}`);
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

  // If we reach here, Playwright attempts failed. Try Node HTTPS GET for businesswire or as last resort.
  const lower = url.toLowerCase();
  if (lower.includes('businesswire.com')) {
    console.warn('Playwright failed for BusinessWire — trying simple HTTPS GET fallback with browser-like headers');
    try {
      const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
      };
      const r = await simpleHttpGet(url, outPath, headers);
      if (r && r.ok) {
        console.log(`Fallback HTTPS GET succeeded: ${url} -> ${outPath} (status ${r.status})`);
        return;
      }
    } catch (e) {
      console.warn('Fallback HTTPS GET failed:', e && e.message ? e.message : e);
      lastErr = e;
    }
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
