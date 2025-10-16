// scripts/render_page.js
// Domain-aware renderer: uses a Yahoo-specific flow and a generic flow for other sites.
// Usage:
//   node scripts/render_page.js "<url>" "scripts/rendered/out.html"
// If out path omitted, writes to scripts/rendered/<sanitized-host>-<ts>.html

const fs = require('fs');
const path = require('path');
const playwright = require('playwright');

function sanitizeFilename(s) {
  return String(s || '').replace(/[^a-z0-9\-_.]/gi, '_').replace(/_+/g, '_').slice(0, 200);
}

function domainFromUrl(u){
  try { return new URL(u).hostname.toLowerCase(); } catch(e){ return ''; }
}

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

(async () => {
  const argv = process.argv.slice(2);
  if (!argv || argv.length < 1) {
    console.error('Usage: node scripts/render_page.js <url> [outPath]');
    process.exit(2);
  }
  const url = argv[0];
  let out = argv[1] || null;
  if (!out) {
    // auto path
    let u;
    try { u = new URL(url); } catch(e) { u = null; }
    const hostpart = u ? sanitizeFilename(u.hostname + (u.pathname || '')) : sanitizeFilename(url);
    out = path.join('scripts','rendered', `${hostpart}-${Date.now()}.html`);
  }
  try { fs.mkdirSync(path.dirname(out), { recursive: true }); } catch(e){}

  const domain = domainFromUrl(url);
  const isYahoo = domain.includes('finance.yahoo.com') || url.includes('/quotes/');
  const NAV_TIMEOUT = 45000;

  const browser = await playwright.chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-http2'] });
  let anyWarnings = false;

  try {
    // create new context per site to reduce cross-site state
    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
      locale: 'en-US'
    });

    // reduce basic detection
    await context.addInitScript(() => {
      try {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        window.chrome = window.chrome || { runtime: {} };
      } catch(e){}
    });

    const page = await context.newPage();

    // extra headers
    await page.setExtraHTTPHeaders({
      'accept-language': 'en-US,en;q=0.9',
      'sec-ch-ua': '"Chromium";v="140", "Not A;Brand";v="24"',
      'sec-ch-ua-platform': '"Windows"',
      'sec-ch-ua-mobile': '?0'
    });

    console.log('Starting render for:', url, '->', out, ' (yahoo-mode=', isYahoo, ')');

    // PER-DOMAIN: Yahoo flow (use networkidle / wait for news anchors)
    if (isYahoo) {
      try {
        // try networkidle first (good for Yahoo news injection)
        try {
          await page.goto(url, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT });
        } catch (e) {
          // fallback to load
          console.warn('Yahoo: networkidle failed, trying load:', e && e.message ? e.message : e);
          try { await page.goto(url, { waitUntil: 'load', timeout: NAV_TIMEOUT }); } catch(e2){ console.warn('Yahoo: load also failed:', e2 && e2.message ? e2.message : e2); }
        }

        // common cookie accept attempts
        const consentSelectors = [
          'button:has-text("Accept all")','button:has-text("Accept")','button[data-testid="consent-accept-button"]',
          'button:has-text("Allow all")','button:has-text("Accept cookies")'
        ];
        for (const sel of consentSelectors) {
          try {
            const el = await page.$(sel);
            if (el) { try { await el.click({ timeout: 2500 }); } catch(e){ try{ await page.evaluate(el=>el.click(), el); }catch(e2){} } await page.waitForTimeout(500); }
          } catch(e){}
        }

        // Wait longer for news anchors typical selectors
        const newsSelector = 'section[data-test="qsp-news"], ul[data-test="quoteNewsStream"], li.stream-item.story-item, li.js-stream-content';
        let found = false;
        try {
          await page.waitForSelector(newsSelector, { timeout: 15000 });
          found = true;
          await page.waitForTimeout(800); // allow final injection
        } catch (e) {
          // not found -> small extra wait as fallback
          console.warn('Yahoo: news selectors not found within timeout, doing small fallback wait');
          await page.waitForTimeout(1200);
        }

        // final scroll to trigger lazy loads
        try {
          await page.evaluate(async () => {
            for (let i=0;i<6;i++){ window.scrollBy(0, window.innerHeight); await new Promise(r=>setTimeout(r,300)); }
          });
        } catch(e){}

        const content = await page.content();
        fs.writeFileSync(out, content, 'utf8');
        console.log(`Rendered (Yahoo) ${url} -> ${out} (bytes=${Buffer.byteLength(content,'utf8')})`);
      } catch (err) {
        console.warn('Yahoo render error:', err && err.message ? err.message : err);
        anyWarnings = true;
      } finally {
        try { await page.close(); } catch(e) {}
        try { await context.close(); } catch(e) {}
      }

    } else {
      // GENERIC flow for other sites (the previous robust version)
      try {
        // navigation with retries
        let lastErr = null;
        const maxAttempts = 3;
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          try {
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
            await page.waitForTimeout(1200);
            lastErr = null;
            break;
          } catch (e) {
            lastErr = e;
            console.warn(`page.goto attempt ${attempt} failed for ${url}: ${e && e.message ? e.message : e}`);
            await page.waitForTimeout(800 * attempt);
          }
        }
        if (lastErr) {
          console.warn('All page.goto attempts failed (but will still capture content):', lastErr && lastErr.message ? lastErr.message : lastErr);
        }

        // try to close overlays
        const overlaySelectors = [
          'button[aria-label*="close"]','button[aria-label*="Close"]','button[aria-label*="Accept"]','button[aria-label*="Accept cookies"]',
          '.cookie-consent', '.consent-banner', '.newsletter-popup', '.newsletter-modal'
        ];
        for (const sel of overlaySelectors) {
          try {
            const els = await page.$$(sel);
            for (const e of els) { try { await e.click({ timeout: 1200 }); } catch(e2){} }
          } catch(e){}
        }
        await page.waitForTimeout(400);

        // expand "see more" like buttons
        const seeMoreButtons = [
          'button[aria-label*="see more"]','button[aria-label*="See more"]','button[data-more-button]'
        ];
        for (const sel of seeMoreButtons) {
          try {
            const btns = await page.$$(sel);
            for (const b of btns) { try { await b.click({ timeout: 1200 }); } catch(e){} }
          } catch(e){}
        }
        await page.waitForTimeout(400);

        // auto scroll
        try {
          await page.evaluate(async () => {
            for (let i=0;i<8;i++){ window.scrollBy(0, window.innerHeight); await new Promise(r=>setTimeout(r,400)); }
          });
        } catch(e){}

        // load more clicks
        const loadMoreSelectors = ['button.load-more','button[data-control-name="load_more"]','button[aria-label*="Load more"]'];
        for (const sel of loadMoreSelectors) {
          try {
            const btns = await page.$$(sel);
            for (const b of btns) { try { await b.click({ timeout: 1500 }); await page.waitForTimeout(500); } catch(e){} }
          } catch(e){}
        }

        await page.waitForTimeout(700);
        const content = await page.content();
        fs.writeFileSync(out, content, 'utf8');
        const bytes = Buffer.byteLength(content,'utf8');
        if (bytes < 2000) {
          console.warn(`Rendered ${url} -> ${out} (bytes=${bytes}) - SMALL RENDER (server might have rejected or HTTP2 failed)`);
        } else {
          console.log(`Rendered ${url} -> ${out} (bytes=${bytes})`);
        }
      } catch (err) {
        console.warn('Generic render error:', err && err.message ? err.message : err);
        anyWarnings = true;
      } finally {
        try { await page.close(); } catch(e) {}
        try { await context.close(); } catch(e) {}
      }
    }

  } catch (err) {
    console.error('Fatal renderer error:', err && (err.message || err));
    anyWarnings = true;
  } finally {
    try { await browser.close(); } catch(e){}
  }

  if (anyWarnings) {
    console.warn('Render completed with warnings. Check rendered HTML files and logs.');
    process.exit(0);
  } else {
    console.log('Render completed successfully.');
    process.exit(0);
  }
})();
