// scripts/render_page.js
// Uso: node scripts/render_page.js <url> <output_path>
// Versão melhorada: stealth-like, melhor logging, tenta 3x, mostra status HTTP.

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

async function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function render(url, outPath) {
  const outDir = path.dirname(outPath);
  fs.mkdirSync(outDir, { recursive: true });

  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    headless: true
  });

  // context-level headers & options
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    locale: 'en-US',
    viewport: { width: 1280, height: 800 },
    // extra http headers
    extraHTTPHeaders: {
      'accept-language': 'en-US,en;q=0.9',
      'sec-ch-ua': '"Chromium";v="120", "Google Chrome";v="120"',
      'sec-ch-ua-platform': '"Windows"'
    }
  });

  const page = await context.newPage();

  // stealth-ish: hide webdriver, provide plugins/languages
  await page.addInitScript(() => {
    // navigator.webdriver
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    // basic plugins/langs
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
    Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
  });

  // resource routing: block heavy/tracking by domain/type but allow css/fonts
  await page.route('**/*', (route) => {
    const req = route.request();
    const url = req.url();
    const resourceType = req.resourceType();

    // block some known ad/tracker domains
    const blocked = ['googlesyndication', 'doubleclick', 'google-analytics', 'adsystem', 'adservice', 'scorecardresearch', 'facebook.net', 'facebook.com', 'ads-twitter'];
    for (const b of blocked) {
      if (url.includes(b)) return route.abort();
    }

    // block images/media to speed up, but allow stylesheet & font
    if (resourceType === 'image' || resourceType === 'media') {
      return route.abort();
    }
    return route.continue();
  });

  page.setDefaultNavigationTimeout(90000); // 90s

  let lastErr = null;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      console.log(`Render attempt ${attempt} -> ${url}`);
      // set a referrer to appear more human-like
      const resp = await page.goto(url, { waitUntil: 'networkidle', timeout: 70000 });
      const status = resp ? resp.status() : null;
      console.log(`Main response status: ${status}`);

      // if we got a 403 at the main request, bail early but try again
      if (status === 403) {
        console.warn(`Got 403 on ${url} (attempt ${attempt})`);
        lastErr = new Error('403 Forbidden');
        await delay(2000 * attempt);
        continue;
      }

      // give it a little time for dynamic content to populate
      await delay(1200 + attempt * 300);

      // try to wait for a common content container (non-fatal)
      try {
        await page.waitForSelector('article, main, #content, .investors_events_bodybox, .investors_events_content_boxes', { timeout: 7000 });
      } catch (e) {
        // não fatal, continuamos
      }

      const content = await page.content();
      fs.writeFileSync(outPath, content, { encoding: 'utf-8' });
      console.log(`Rendered ${url} -> ${outPath} (status: ${status})`);
      await browser.close();
      return;
    } catch (err) {
      console.error(`Render error (attempt ${attempt}) for ${url}:`, err && err.message ? err.message : err);
      lastErr = err;
      // small backoff
      await delay(2000 * attempt);
    }
  }

  await browser.close();
  throw lastErr || new Error('Unknown render error');
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
    console.error('Render failed:', err && err.stack ? err.stack : err);
    process.exit(1);
  }
})();
