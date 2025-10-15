// scripts/render_page.js
// Usage: node render_page.js "<url>" "scripts/rendered/out.html"

const fs = require('fs');
const path = require('path');
const url = process.argv[2];
const out = process.argv[3] || 'scripts/rendered/rendered.html';
const playwright = require('playwright');

if (!url) {
  console.error('Usage: node render_page.js "<url>" [outpath]');
  process.exit(2);
}

(async () => {
  const browser = await playwright.chromium.launch({ headless: true, args: ['--no-sandbox'] });
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36'
  });
  const page = await context.newPage();

  try {
    console.log('Starting render for:', url);

    // generous timeout
    const NAV_TIMEOUT = 45000;

    // goto with networkidle, fallback to load if networkidle times out
    try {
      await page.goto(url, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT });
    } catch (e) {
      try {
        await page.goto(url, { waitUntil: 'load', timeout: NAV_TIMEOUT });
      } catch (ee) {
        console.warn('Initial page.goto failed:', ee.message || ee);
      }
    }

    // try a set of selectors to click on cookie/consent dialogs
    const consentSelectors = [
      'button:has-text("Accept all")',
      'button:has-text("Accept")',
      'button:has-text("I agree")',
      'button[aria-label*="Accept"]',
      'button[id*="accept"]',
      'button[name*="agree"]',
      'button:has-text("Allow all")',
      'button:has-text("Accept cookies")',
      'button[data-testid="consent-accept-button"]',
      'input[type="button"][value*="Accept"]',
      '[role="button"]:has-text("Accept all")'
    ];

    for (const sel of consentSelectors) {
      try {
        const el = await page.$(sel);
        if (el) {
          console.log('Clicking consent selector:', sel);
          try { await el.click({ timeout: 3000 }); } catch(e){ try { await page.evaluate(el => el.click(), el); } catch(e2){} }
          // small pause to let DOM change
          await page.waitForTimeout(600);
        }
      } catch (e) {
        // ignore
      }
    }

    // also attempt to dismiss overlays by removing elements matching common classes
    try {
      await page.evaluate(() => {
        const bad = document.querySelectorAll('[id*="consent"], [class*="consent"], [class*="cookie"], [id*="cookie"], [aria-label*="cookie"]');
        for (const n of bad) try { n.remove(); } catch(e) {}
      });
    } catch (e) {}

    // scroll to bottom slowly to trigger lazy loading
    try {
      await page.evaluate(async () => {
        await new Promise((resolve) => {
          let total = 0;
          const distance = 400;
          const timer = setInterval(() => {
            window.scrollBy(0, distance);
            total += distance;
            if (total > document.body.scrollHeight + 2000) { clearInterval(timer); resolve(); }
          }, 200);
          // safety timeout 6s
          setTimeout(() => { clearInterval(timer); resolve(); }, 6000);
        });
      });
    } catch (e) {}

    // wait for news-like anchors to appear
    const newsSelector = 'a[href*="/news/"], a[href*="/article/"], a[href*="/story/"], a[href*="/articles/"]';
    try {
      await page.waitForSelector(newsSelector, { timeout: 10000 });
      console.log('Found news-like anchors on page.');
    } catch (e) {
      console.warn('No news anchors found within timeout; will still save rendered HTML.');
    }

    // give a bit more time for dynamic content after anchors found
    await page.waitForTimeout(800);

    const content = await page.content();

    // ensure output dir exists
    const outdir = path.dirname(out);
    fs.mkdirSync(outdir, { recursive: true });

    fs.writeFileSync(out, content, { encoding: 'utf8' });
    console.log('Rendered', url, '->', out, '(status: saved, bytes:', Buffer.byteLength(content, 'utf8'), ')');

  } catch (err) {
    console.error('Render error:', err);
  } finally {
    try { await context.close(); } catch(e) {}
    try { await browser.close(); } catch(e) {}
  }
})();
