#!/usr/bin/env node
/**
 * scripts/render_page.js (diagnostic patch)
 *
 * Substitui o ficheiro anterior. Guarda HTML, e quando o resultado for
 * anormalmente pequeno grava também:
 *  - screenshot (.png)
 *  - debug JSON com eventos de rede e console (.debug.json)
 *
 * Uso:
 *  node scripts/render_page.js <url> <outPath>
 *  node scripts/render_page.js <url1> <url2> ...
 *
 * Controla HEADLESS via env HEADLESS (seta 'false' para headful quando possível).
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
    const browser = await chromium.launch({
      headless,
      args: ['--no-sandbox','--disable-setuid-sandbox','--disable-blink-features=AutomationControlled']
    });

    let anyFailed = false;

    try {
      const context = await browser.newContext({
        viewport: { width: 1400, height: 900 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        bypassCSP: true,
        locale: 'en-US',
      });

      // small anti-detect init
      await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        window.chrome = window.chrome || { runtime: {} };
      });

      context.setExtraHTTPHeaders({ 'accept-language': 'en-US,en;q=0.9', 'referer': 'https://www.google.com/' });

      const NAV_TIMEOUT = 90000;

      const overlaySelectors = [
        '#onetrust-accept-btn-handler', 'button[aria-label*="accept"]', '.cc-accept', '.cookie-accept', '.consent-accept',
        '.consent-banner', '.optanon-allow-all', '.onetrust-close-btn-handler', 'button[title*="Accept"]'
      ];
      const seeMoreButtons = ['button[aria-label*="see more"]', 'button[data-more-button]', 'button.load-more', 'a.load-more'];

      async function tryClickAll(page, selectors, timeoutEach = 1200) {
        for (const sel of selectors) {
          try {
            const els = await page.$$(sel);
            for (const el of els) {
              try { await el.click({ timeout: timeoutEach }); } catch(e) {}
            }
          } catch(e){}
        }
      }

      async function removeOverlaysByJS(page) {
        try {
          await page.evaluate(() => {
            const bad = ['#onetrust-consent-sdk', '.cookie-consent', '.consent-banner', '.overlay--newsletter', '.newsletter-popup'];
            bad.forEach(s => document.querySelectorAll(s).forEach(el => el.remove()));
            document.documentElement.style.overflow = 'auto';
            document.body.style.overflow = 'auto';
          });
        } catch(e){}
      }

      async function autoScroll(page, maxScrolls = 20, delay = 700) {
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
        // diagnostic collectors
        const consoleEvents = [];
        const networkEvents = [];
        try {
          page = await context.newPage();

          // attach listeners to collect data
          page.on('console', msg => {
            try {
              consoleEvents.push({ type: 'console', text: msg.text(), location: msg.location(), timestamp: Date.now() });
            } catch(e) {}
          });
          page.on('pageerror', errObj => {
            consoleEvents.push({ type: 'pageerror', text: (errObj && errObj.message) ? errObj.message : String(errObj), timestamp: Date.now() });
          });
          page.on('request', req => {
            networkEvents.push({ type: 'request', url: req.url(), method: req.method(), headers: req.headers(), timestamp: Date.now() });
          });
          page.on('requestfailed', req => {
            networkEvents.push({ type: 'requestfailed', url: req.url(), failureText: req.failure() ? req.failure().errorText : null, timestamp: Date.now() });
          });
          page.on('response', async resp => {
            try {
              const headers = resp.headers();
              networkEvents.push({ type: 'response', url: resp.url(), status: resp.status(), headers, timestamp: Date.now() });
            } catch(e){}
          });

          // try goto networkidle first
          try {
            await page.goto(url, { waitUntil: 'networkidle', timeout: NAV_TIMEOUT });
          } catch(e) {
            try { await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT }); } catch(e2){}
          }

          await page.waitForTimeout(1200);

          // click overlays aggressively
          await tryClickAll(page, overlaySelectors, 1000);
          await removeOverlaysByJS(page);
          await page.waitForTimeout(400);

          // click see more
          await tryClickAll(page, seeMoreButtons, 1000);
          await page.waitForTimeout(400);

          // scroll more
          await autoScroll(page, 12, 800);

          // host-specific wait (EETimes)
          try {
            const host = (() => { try { return new URL(url).hostname; } catch(e) { return ''; } })();
            if (host && host.includes('eetimes.com')) {
              try {
                await page.waitForSelector('#main, #wallpaper_image, .categoryFeatured-block, .card-body', { timeout: 15000 });
                log('Host-specific selector(s) appeared for eetimes.com');
              } catch(e) {
                log('Host-specific selector(s) did NOT appear within timeout for eetimes.com (continuing)');
              }
            }
          } catch(e){}

          // more scrolling + click passes
          await tryClickAll(page, overlaySelectors, 600);
          await tryClickAll(page, seeMoreButtons, 600);
          await autoScroll(page, 6, 600);
          await page.waitForTimeout(700);

          // capture content
          let content = '';
          try {
            content = await page.evaluate(() => document.documentElement.outerHTML);
          } catch(e) {
            try { content = await page.content(); } catch(e2) { content = ''; }
          }

          // write content
          try {
            const dir = path.dirname(targetOut);
            fs.mkdirSync(dir, { recursive: true });
            fs.writeFileSync(targetOut, content, 'utf8');
            const stat = fs.statSync(targetOut);
            log(`Rendered ${url} -> ${targetOut} (status: saved, bytes: ${stat.size})`);

            // if HTML too small, save debug info and screenshot
            if (stat.size < 2000) {
              const base = targetOut.replace(/\.html$/, '');
              const debugPath = `${base}.debug.json`;
              const screenshotPath = `${base}.debug.png`;
              try {
                // screenshot
                await page.screenshot({ path: screenshotPath, fullPage: true }).catch(()=>{});
              } catch(e){}

              // save debug json
              const dbg = {
                url,
                out: targetOut,
                bytes: stat.size,
                elapsed_s: Math.round((Date.now()-start)/1000),
                consoleEvents,
                networkEvents
              };
              try {
                fs.writeFileSync(debugPath, JSON.stringify(dbg, null, 2), 'utf8');
                log(`Wrote debug JSON: ${debugPath}`);
                try { const s = fs.statSync(screenshotPath).size; log(`Wrote screenshot: ${screenshotPath} (bytes: ${s})`); } catch(e){}
              } catch(e) {
                warn('Failed to write debug json:', e && e.message ? e.message : e);
              }
            }
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
