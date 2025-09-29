#!/usr/bin/env node
/**
 * scripts/render_page.js (patched)
 *
 * Uso:
 *   node scripts/render_page.js <url> <outPath>
 *   node scripts/render_page.js <url1> <url2> ...
 *
 * Alterações principais:
 *  - waitUntil 'networkidle'
 *  - NAV_TIMEOUT aumentado
 *  - espera por seletores-chaves (para sites que carregam via JS)
 *  - mais auto-scrolling e clicks em "accept cookies"/"load more"
 *  - tentativa agressiva de fechar overlays (clicar/remover)
 */

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
    let outPath = null;
    let urls = [];

    // Determina se caller forneceu outPath (segundo arg é um path tipo .html)
    if (argv.length >= 2 && argv[1] && (argv[1].endsWith('.html') || argv[1].startsWith('scripts/') || argv[1].startsWith('./') || argv[1].startsWith('/'))) {
      urls = [argv[0]];
      outPath = path.resolve(process.cwd(), argv[1]);
    } else {
      urls = argv.slice();
    }

    if (!urls || urls.length === 0) {
      console.log('USO: node render_page.js <url> <outPath>   OR   node render_page.js <url1> <url2> ...');
      process.exit(1);
    }

    const renderedDir = path.resolve(process.cwd(), 'scripts', 'rendered');
    try { fs.mkdirSync(renderedDir, { recursive: true }); } catch(e){}

    const headless = process.env.HEADLESS !== 'false';
    const browser = await chromium.launch({ headless, args: ['--no-sandbox','--disable-setuid-sandbox'] });

    let anyFailed = false;

    try {
      const context = await browser.newContext({
        viewport: { width: 1400, height: 900 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      });

      // reduce basic webdriver detection
      await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        window.chrome = window.chrome || { runtime: {} };
      });

      // small extra headers
      context.setExtraHTTPHeaders({ 'accept-language': 'en-US,en;q=0.9' });

      // NAV timeout maior
      const NAV_TIMEOUT = 60000;

      // selectors common for cookie overlays and "accept"
      const overlaySelectors = [
        '#onetrust-accept-btn-handler', // OneTrust
        'button[aria-label*="accept"]',
        'button[aria-label*="Accept"]',
        'button[aria-label*="ACCEPT"]',
        'button[title*="Accept"]',
        '.cc-accept', '.cookie-accept', '.cookie-consent button', '.accept-cookies', '.consent-accept',
        '.consent-banner button', '.optanon-allow-all', '#agree-button', '.onetrust-close-btn-handler'
      ];

      // see more / load more
      const seeMoreButtons = [
        'button[aria-label*="see more"]', 'button[aria-label*="See more"]',
        'button.feed-shared-inline-show-more-text__see-more-less-toggle',
        'button[data-more-button]', 'a.load-more', 'button.load-more', '.load-more'
      ];

      async function tryClickAll(page, selectors, timeoutEach = 1200) {
        for (const sel of selectors) {
          try {
            const els = await page.$$(sel);
            for (const el of els) {
              try { await el.click({ timeout: timeoutEach }); } catch(e) { /* ignore */ }
            }
          } catch(e){}
        }
      }

      async function removeOverlaysByJS(page) {
        try {
          await page.evaluate(() => {
            const bad = [
              '#onetrust-consent-sdk', '.cookie-consent', '.consent-banner', '.overlay--newsletter', '.newsletter-popup'
            ];
            for (const s of bad) {
              document.querySelectorAll(s).forEach(el => {
                try { el.remove(); } catch(e) {}
              });
            }
            // also un-hide body if blocked by fixed overlay
            document.documentElement.style.overflow = 'auto';
            document.body.style.overflow = 'auto';
          });
        } catch(e){}
      }

      async function autoScroll(page, maxScrolls = 15, delay = 700) {
        for (let i = 0; i < maxScrolls; i++) {
          await page.evaluate(() => window.scrollBy(0, window.innerHeight));
          await page.waitForTimeout(delay);
        }
      }

      for (const url of urls) {
        const start = Date.now();
        let targetOut = outPath;
        if (!targetOut) {
          const u = (() => { try { return new URL(url); } catch(e) { return null; } })();
          const hostpart = u ? sanitizeFilename(u.hostname + (u.pathname || '')) : sanitizeFilename(url);
          const ts = Date.now();
          targetOut = path.join(renderedDir, `${hostpart}-${ts}.html`);
        }

        log(`Starting render for: ${url}`);
        let page = null;
        try {
          page = await context.newPage();

          // try goto with networkidle (wait for XHRs)
          try {
            await page.goto(url, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT });
          } catch(e) {
            // fallback try domcontentloaded then wait
            try { await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT }); } catch(e2){}
          }

          // short initial wait
          await page.waitForTimeout(1200);

          // try click accept cookie buttons aggressively
          await tryClickAll(page, overlaySelectors, 1200);
          // remove overlays via JS as fallback
          await removeOverlaysByJS(page);
          await page.waitForTimeout(400);

          // try "see more" buttons
          await tryClickAll(page, seeMoreButtons, 1200);
          await page.waitForTimeout(400);

          // extra aggressive scrolling + wait for dynamic load
          await autoScroll(page, 12, 800);

          // Special: for some hosts wait for key selectors (helps for EETimes / DarkReading / others)
          try {
            const host = (() => { try { return new URL(url).hostname; } catch(e) { return ''; } })();
            if (host && host.includes('eetimes.com')) {
              // try to wait for either categoryFeatured-block or card-body or headline-title
              try {
                await page.waitForSelector('#main .categoryFeatured-block, #main .segment-one .headline-title, #wallpaper_image .card-body', { timeout: 8000 });
                log('Host-specific selector(s) appeared for eetimes.com');
              } catch(e) {
                log('Host-specific selector(s) did NOT appear within timeout for eetimes.com (continuing)');
              }
            }
            // similar host-specific waits could be added here for other sites
          } catch(e) {}

          // another pass of clicks + scroll (sometimes more content loads after scroll)
          await tryClickAll(page, overlaySelectors, 800);
          await tryClickAll(page, seeMoreButtons, 800);
          await autoScroll(page, 8, 700);

          // final short wait
          await page.waitForTimeout(700);

          // grab final content
          let content = '';
          try {
            // prefer full outerHTML
            content = await page.evaluate(() => document.documentElement.outerHTML);
          } catch (e) {
            try { content = await page.content(); } catch(e2) { content = ''; }
          }

          // write to requested outPath
          try {
            const dir = path.dirname(targetOut);
            fs.mkdirSync(dir, { recursive: true });
            fs.writeFileSync(targetOut, content, 'utf8');
            const stat = fs.statSync(targetOut);
            log(`Rendered ${url} -> ${targetOut} (status: saved, bytes: ${stat.size})`);
          } catch (e) {
            warn(`Failed to save rendered content to ${targetOut}:`, e && e.message ? e.message : e);
            anyFailed = true;
          }

          const elapsed = Math.round((Date.now() - start) / 1000);
          log(`-> Done: ${url} (elapsed ${elapsed}s)`);

        } catch (pageErr) {
          warn(`Render failed for ${url} - ${pageErr && pageErr.message ? pageErr.message : pageErr}`);
          anyFailed = true;
        } finally {
          try { if (page) await page.close(); } catch(e){}
        }
      } // end for urls

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
    err('Fatal error in render_page.js:', err && (err.message || err));
    try { process.exit(1); } catch(e){}
  }
})();
