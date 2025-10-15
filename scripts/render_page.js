// scripts/render_page.js
// Robust page renderer for use in CI (Playwright Chromium).
// Usage:
//   node scripts/render_page.js "<url>" "scripts/rendered/out.html"
//   or: node scripts/render_page.js "<url1>" "<url2>" ...
//
// Behavior summary:
//  - launches Chromium with helpful flags (--disable-http2 etc).
//  - sets extra headers to reduce "Headless" detection.
//  - retries page.goto up to 3 times with incremental backoff.
//  - tries to click consent buttons and remove overlays.
//  - auto-scrolls, expands "see more", tries "load more" buttons.
//  - warns when result HTML is very small (useful to detect failures).

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

function log(...a){ console.log(...a); }
function warn(...a){ console.warn(...a); }
function err(...a){ console.error(...a); }

function sanitizeFilename(s) {
  return String(s || '').replace(/[^a-z0-9\-_.]/gi, '_').replace(/_+/g, '_').slice(0, 200);
}

(async () => {
  try {
    const argv = process.argv.slice(2);
    if (!argv || argv.length === 0) {
      console.log('USO: node scripts/render_page.js <url> <outPath?>   OR   node scripts/render_page.js <url1> <url2> ...');
      process.exit(1);
    }

    // Determine outPath presence
    let explicitOut = null;
    let urls = [];
    if (argv.length >= 2 && (argv[1].endsWith('.html') || argv[1].startsWith('scripts/') || argv[1].startsWith('./') || argv[1].startsWith('/'))) {
      urls = [argv[0]];
      explicitOut = path.resolve(process.cwd(), argv[1]);
    } else {
      urls = argv.slice();
    }

    const renderedDir = path.resolve(process.cwd(), 'scripts', 'rendered');
    try { fs.mkdirSync(renderedDir, { recursive: true }); } catch(e){}

    // HEADLESS toggle via env var HEADLESS (default true). For debug set HEADLESS=false
    const headless = (process.env.HEADLESS === undefined) ? true : (process.env.HEADLESS !== 'false');
    // NAV timeout env override (ms)
    const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || '45000', 10);

    // Launch chromium with flags that help in CI and avoid HTTP2-related errors
    const browser = await chromium.launch({
      headless,
      args: [
        '--no-sandbox',
        '--disable-setuid-sandbox',
        '--disable-http2',
        '--disable-dev-shm-usage',
        '--no-zygote',
        '--disable-site-isolation-trials'
      ]
    });

    let anyFailed = false;

    try {
      const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36'
      });

      // reduce simple headless checks
      await context.addInitScript(() => {
        try {
          Object.defineProperty(navigator, 'webdriver', { get: () => false });
          Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
          Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
          window.chrome = window.chrome || { runtime: {} };
        } catch (e) {}
      });

      // set extra HTTP headers to avoid revealing headless
      try {
        await context.setExtraHTTPHeaders({
          'accept-language': 'en-US,en;q=0.9',
          'sec-ch-ua': '"Chromium";v="140", "Not A;Brand";v="24"',
          'sec-ch-ua-platform': '"Windows"',
          'sec-ch-ua-mobile': '?0'
        });
      } catch (e) {
        // some versions may not support; continue
      }

      const page = await context.newPage();

      for (const url of urls) {
        const start = Date.now();
        let outPath = explicitOut;
        if (!outPath) {
          const u = (() => { try { return new URL(url); } catch(e) { return null; } })();
          const hostpart = u ? sanitizeFilename(u.hostname + (u.pathname || '')) : sanitizeFilename(url);
          const ts = Date.now();
          outPath = path.join(renderedDir, `${hostpart}-${ts}.html`);
        }

        log(`Starting render for: ${url}`);

        // Try page.goto with retries
        let lastErr = null;
        const maxAttempts = 3;
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
          try {
            // prefer networkidle but sometimes it's unreliable; try domcontentloaded first then wait for networkidle afterwards
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
            // small wait to let page hydrate
            await page.waitForTimeout(1200);
            // attempt to wait for networkidle (best effort)
            try {
              await page.waitForLoadState('networkidle', { timeout: 5000 });
            } catch (e) {
              // ignore - networkidle sometimes times out but page may be usable
            }
            lastErr = null;
            break;
          } catch (e) {
            lastErr = e;
            warn(`page.goto attempt ${attempt} failed for ${url}: ${e && e.message ? e.message : e}`);
            // incremental backoff
            await page.waitForTimeout(1000 * attempt);
          }
        }

        if (lastErr) {
          warn(`All page.goto attempts failed for ${url}: ${lastErr && lastErr.message ? lastErr.message : lastErr}`);
          // continue - we'll still try to collect content (may be an error page)
        }

        // Try clicking consent/accept dialogs
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
          '[role="button"]:has-text("Accept all")',
          'button:has-text("Yes, I agree")',
          'button:has-text("Agree")'
        ];

        for (const sel of consentSelectors) {
          try {
            const els = await page.$$(sel);
            if (els && els.length) {
              for (const el of els) {
                try { await el.click({ timeout: 2500 }); } catch(e){ try { await page.evaluate(e => e.click(), el); } catch(e2){} }
              }
              await page.waitForTimeout(500);
            }
          } catch (e) {}
        }

        // Remove obvious overlay elements that hinder rendering
        try {
          await page.evaluate(() => {
            const bad = document.querySelectorAll('[id*="consent"], [class*="consent"], [class*="cookie"], [id*="cookie"], [aria-label*="cookie"], .cookie-banner, .consent-banner, .overlay__container, .modal-backdrop');
            for (const n of bad) try { n.remove(); } catch(e) {}
          });
        } catch (e) {}

        // Expand "see more" / "read more" elements
        const seeMoreButtons = [
          'button[aria-label*="see more"]', 'button[aria-label*="See more"]', 'button.feed-shared-inline-show-more-text__see-more-less-toggle',
          'button[aria-label*="Read more"]', 'button[data-more-button]', 'button.show-more', '.more-link', 'a.show-more'
        ];
        for (const sel of seeMoreButtons) {
          try {
            const btns = await page.$$(sel);
            for (const b of btns) {
              try { await b.click({ timeout: 1200 }); } catch(e) {}
            }
          } catch(e){}
        }

        // auto scroll to trigger lazy loading
        async function autoScroll(maxScrolls = 12, delay = 600) {
          for (let i = 0; i < maxScrolls; i++) {
            try {
              await page.evaluate(() => { window.scrollBy(0, window.innerHeight); });
            } catch(e){}
            await page.waitForTimeout(delay);
          }
        }
        await autoScroll(10, 600);

        // try clicking load-more if present
        const loadMoreSelectors = ['button.load-more', 'button[aria-label*="Load more"]', 'button[data-control-name="load_more"]', '.load-more-button', '.js-load-more'];
        for (const sel of loadMoreSelectors) {
          try {
            const btns = await page.$$(sel);
            for (const b of btns) {
              try { await b.click({ timeout: 1500 }); await page.waitForTimeout(600); } catch(e) {}
            }
          } catch(e){}
        }

        // give a final short wait
        await page.waitForTimeout(700);

        // wait for at least one news-like anchor if present (best effort)
        try {
          await page.waitForSelector('a[href*="/news/"], a[href*="/article/"], a[href*="/story/"], a[href*="/articles/"]', { timeout: 8000 });
        } catch (e) {
          // ignore, still save what's there
        }

        // capture content
        const content = await page.content();

        // write out
        try {
          const dir = path.dirname(outPath);
          fs.mkdirSync(dir, { recursive: true });
          fs.writeFileSync(outPath, content, 'utf8');
          const bytes = Buffer.byteLength(content, 'utf8');
          if (bytes < 2000) {
            warn(`Rendered ${url} -> ${outPath} (status: saved, bytes: ${bytes}) - SMALL RENDER (server likely rejected or HTTP2 failed)`);
          } else {
            log(`Rendered ${url} -> ${outPath} (status: saved, bytes: ${bytes})`);
          }
        } catch (e) {
          warn(`Failed to save rendered content to ${outPath}: ${e && e.message ? e.message : e}`);
          anyFailed = true;
        }

        const elapsed = Math.round((Date.now() - start) / 1000);
        log(`-> Done: ${url} (elapsed ${elapsed}s)`);
      }

      try { await context.close(); } catch(e){}
    } finally {
      try { await browser.close(); } catch(e){}
    }

    if (anyFailed) {
      warn('Some renders failed (see logs). Exiting with code 2.');
      process.exit(2);
    } else {
      log('All renders completed successfully.');
      process.exit(0);
    }

  } catch (err) {
    console.error('Fatal error in render_page.js:', err && (err.message || err));
    try { process.exit(1); } catch(e){}
  }
})();
