// scripts/render_page.js
// Uso: node scripts/render_page.js <url> <output_path>
// Versão combinada: stealth + multi-strategy + host-specific strategy order + logs

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function launchAndPrepare(options) {
  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    headless: true
  });
  const context = await browser.newContext({
    userAgent: options.userAgent,
    locale: options.locale || 'en-US',
    viewport: options.viewport || { width: 1280, height: 800 },
    extraHTTPHeaders: options.extraHTTPHeaders || {}
  });

  // stealth-ish init
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4] });
    // small rand to appear less botlike
    Object.defineProperty(navigator, 'hardwareConcurrency', { get: () => 4 });
  });

  return { browser, context };
}

async function tryNavigateWithOptions(url, outPath, strat) {
  const { browser, context } = await launchAndPrepare(strat);
  let page = null;
  try {
    page = await context.newPage();

    await page.route('**/*', (route) => {
      const req = route.request();
      const rurl = req.url();
      const type = req.resourceType();

      const blockedDomains = ['googlesyndication','doubleclick','google-analytics','adsystem','adservice','scorecardresearch','facebook.net','facebook.com','ads-twitter'];
      for (const d of blockedDomains) if (rurl.includes(d)) return route.abort();

      if (strat.blockImages && (type === 'image' || type === 'media')) return route.abort();
      if (strat.blockStyles && (type === 'stylesheet' || type === 'font')) return route.abort();

      return route.continue();
    });

    page.setDefaultNavigationTimeout(strat.timeout || 90000);

    console.log(`  -> Navigating with waitUntil="${strat.waitUntil}" (timeout ${strat.timeout || 90000})`);
    const resp = await page.goto(url, { waitUntil: strat.waitUntil, timeout: strat.timeout || 90000, referer: strat.referer || undefined });
    const status = resp ? resp.status() : null;
    return { ok: true, status, browser, context, page };
  } catch (err) {
    if (page) try { await page.close(); } catch(e){}
    await browser.close();
    return { ok: false, error: err };
  }
}

async function render(url, outPath) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });

  // default strategies
  const commonHeaders = { 'accept-language': 'en-US,en;q=0.9' };

  const strategies = [
    {
      name: 'A - fast, block images & styles (default fast)',
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36',
      extraHTTPHeaders: commonHeaders,
      blockImages: true,
      blockStyles: true,
      waitUntil: 'domcontentloaded',
      timeout: 70000,
      referer: undefined
    },
    {
      name: 'B - allow styles/fonts, referer, Chrome UA (recommended for sites sensitive to CSS)',
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
      extraHTTPHeaders: Object.assign({}, commonHeaders, { 'sec-ch-ua': '"Chromium";v="120", "Google Chrome";v="120"' }),
      blockImages: true,
      blockStyles: false,
      waitUntil: 'networkidle',
      timeout: 90000,
      referer: url
    },
    {
      name: 'C - human-like, allow everything, longer wait',
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15',
      extraHTTPHeaders: commonHeaders,
      blockImages: false,
      blockStyles: false,
      waitUntil: 'networkidle',
      timeout: 110000,
      referer: url
    }
  ];

  // host-specific preferred strategy order (names correspond to strategies array indexes)
  const hostPreferences = {
    'darkreading.com': [1, 0, 2],       // try B then A then C
    'iotworldtoday.com': [1, 2, 0],     // try B then C then A
    'businesswire.com': [1, 2, 0],      // allow styles/fonts for BusinessWire
    'inmodeinvestors.com': [1, 0, 2]    // allow styles for inmode
  };

  const hostKey = Object.keys(hostPreferences).find(h => url.includes(h));
  let order = hostKey ? hostPreferences[hostKey] : [0,1,2];

  let lastErr = null;
  for (let idx = 0; idx < order.length; idx++) {
    const sIndex = order[idx];
    const strat = strategies[sIndex];
    console.log(`Strategy attempt ${idx+1}/${order.length}: ${strat.name} for ${url}`);
    const result = await tryNavigateWithOptions(url, outPath, strat);

    if (!result.ok) {
      console.warn(`  Strategy ${strat.name} failed to navigate: ${result.error && result.error.message ? result.error.message : result.error}`);
      lastErr = result.error;
      // small backoff
      await delay(1000 * (idx+1));
      continue;
    }

    // got a page/browser handle
    const { page, browser, status } = result;
    console.log(`  Main response status: ${status}`);

    if (status === 403) {
      console.warn(`  Got 403 on ${url} with strategy ${strat.name}`);
      await browser.close();
      lastErr = new Error('403 Forbidden');
      await delay(1500);
      continue;
    }

    // wait a bit more for dynamic content to settle
    await delay(1200 + idx * 500);

    // try to wait for common content selectors, non-fatal
    try {
      await page.waitForSelector('article, main, #content, .post, .press, .news, .investors_events_bodybox', { timeout: 7000 });
    } catch (e) {
      // non-fatal
    }

    try {
      const content = await page.content();
      fs.writeFileSync(outPath, content, { encoding: 'utf-8' });
      console.log(`Rendered ${url} -> ${outPath} (status: ${status}) using strategy: ${strat.name}`);
      await browser.close();
      return;
    } catch (err) {
      console.error(`  Error writing content after strategy ${strat.name}: ${err && err.message ? err.message : err}`);
      lastErr = err;
      try { await browser.close(); } catch(e){}
      await delay(500);
      continue;
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
