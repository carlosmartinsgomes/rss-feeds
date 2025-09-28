#!/usr/bin/env node
/**
 * scripts/render_page.js
 *
 * Uso:
 *   node scripts/render_page.js <url> <outPath>
 *   node scripts/render_page.js <url1> <url2> ...
 *
 * - Se passares <outPath> (ex: scripts/rendered/modernhealthcare.html) irá gravar
 *   o HTML renderizado nesse ficheiro.
 * - Se passares apenas URLs, irá gravar ficheiros automáticos em ./scripts/rendered/
 *
 * Não gera screenshots. Tenta evitar detecção básica (user agent, navigator.webdriver).
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

    // cria pasta para outputs automáticos se necessário
    const renderedDir = path.resolve(process.cwd(), 'scripts', 'rendered');
    try { fs.mkdirSync(renderedDir, { recursive: true }); } catch(e){}

    const headless = process.env.HEADLESS !== 'false';
    const browser = await chromium.launch({ headless, args: ['--no-sandbox','--disable-setuid-sandbox'] });

    let anyFailed = false;

    try {
      // opcões de context podem ser adicionadas (cookies, storageState)
      const context = await browser.newContext({
        // viewport: { width: 1280, height: 800 },
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      });

      // init script para reduzir detecção básica
      await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        window.chrome = window.chrome || { runtime: {} };
      });

      const page = await context.newPage();
      // extra headers
      await page.setExtraHTTPHeaders({ 'accept-language': 'en-US,en;q=0.9' });
      // small navigation timeout protection
      const NAV_TIMEOUT = 45000;

      for (const url of urls) {
        const start = Date.now();
        let targetOut = outPath;
        if (!targetOut) {
          // create automatic file name
          const u = (() => { try { return new URL(url); } catch(e) { return null; } })();
          const hostpart = u ? sanitizeFilename(u.hostname + (u.pathname || '')) : sanitizeFilename(url);
          const ts = Date.now();
          targetOut = path.join(renderedDir, `${hostpart}-${ts}.html`);
        }

        log(`Starting render for: ${url}`);
        try {
          // goto
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT }).catch(()=>{});
          // small wait for dynamic content
          await page.waitForTimeout(1200);

          // try to close common overlays/cookies/dialogs (non-click fallback to hide)
          const overlaySelectors = [
            'button[aria-label*="close"]', 'button[aria-label*="Close"]',
            'button[aria-label*="dismiss"]', 'button[aria-label*="Dismiss"]',
            'button[aria-label*="Accept"]', 'button[aria-label*="Accept cookies"]',
            'button[data-control-name="accept_cookies"]', '.cookie-consent', '.consent-banner',
            '.newsletter-popup', '.newsletter-modal', '.overlay--newsletter'
          ];
          for (const sel of overlaySelectors) {
            try {
              const els = await page.$$(sel);
              for (const e of els) {
                try { await e.click({ timeout: 1500 }); } catch(e2) { /* ignore click errors */ }
              }
            } catch(e){}
          }
          // small wait after clicks
          await page.waitForTimeout(400);

          // expand "see more" / "read more" type buttons
          const seeMoreButtons = [
            'button[aria-label*="see more"]', 'button[aria-label*="ver mais"]', 'button.feed-shared-inline-show-more-text__see-more-less-toggle',
            'button[aria-label*="See more"]', 'button[data-more-button]'
          ];
          for (const sel of seeMoreButtons) {
            try {
              const btns = await page.$$(sel);
              for (const b of btns) {
                try { await b.click({ timeout: 1200 }); } catch(e) {}
              }
            } catch(e){}
          }
          await page.waitForTimeout(400);

          // auto scroll a bit to load lazy content
          async function autoScroll(maxScrolls = 8, delay = 700) {
            for (let i = 0; i < maxScrolls; i++) {
              await page.evaluate(() => window.scrollBy(0, window.innerHeight));
              await page.waitForTimeout(delay);
            }
          }
          await autoScroll(8, 700);

          // attempt to click "load more" if present
          const loadMoreSelectors = ['button.load-more', 'button[data-control-name="load_more"]', 'button[aria-label*="Load more"]'];
          for (const sel of loadMoreSelectors) {
            try {
              const btns = await page.$$(sel);
              for (const b of btns) {
                try { await b.click({ timeout: 1500 }); await page.waitForTimeout(500); } catch(e) {}
              }
            } catch(e){}
          }

          // final short wait
          await page.waitForTimeout(700);

          // grab final content
          const content = await page.content();

          // write to requested outPath
          try {
            const dir = path.dirname(targetOut);
            fs.mkdirSync(dir, { recursive: true });
            fs.writeFileSync(targetOut, content, 'utf8');
            log(`Rendered ${url} -> ${targetOut} (status: saved)`);
          } catch (e) {
            warn(`Failed to save rendered content to ${targetOut}:`, e && e.message ? e.message : e);
            anyFailed = true;
          }

          const elapsed = Math.round((Date.now() - start) / 1000);
          log(`-> Done: ${url} (elapsed ${elapsed}s)`);
        } catch (pageErr) {
          warn(`Render failed for ${url} - ${pageErr && pageErr.message ? pageErr.message : pageErr}`);
          anyFailed = true;
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
