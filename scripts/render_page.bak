#!/usr/bin/env node
/**
 * scripts/render_page.js
 *
 * Uso:
 *   node scripts/render_page.js <url> <outPath> [waitSelector] [timeout_ms] [maxScrollMs]
 *   node scripts/render_page.js <url1> <url2> ...
 *
 * - Se passares <outPath> (ex: scripts/rendered/modernhealthcare.html) irá gravar
 *   o HTML renderizado nesse ficheiro.
 * - Se passares apenas URLs, irá gravar ficheiros automáticos em ./scripts/rendered/
 *
 * Melhorias incluídas:
 * - optional waitSelector + timeout: espera pelo selector representativo antes de seguir
 * - scroll adaptativo: rola até não haver aumento no scrollHeight (ou até limite)
 * - tenta fechar overlays (click) e como fallback esconde-os via JS
 * - tenta clicar em load-more repetidamente
 * - logging mais detalhado
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

    // Detect if second arg is a path (endswith .html or looks like scripts/...)
    if (argv.length >= 2 && argv[1] && (argv[1].endsWith('.html') || argv[1].startsWith('scripts/') || argv[1].startsWith('./') || argv[1].startsWith('/'))) {
      urls = [argv[0]];
      outPath = path.resolve(process.cwd(), argv[1]);
    } else {
      urls = argv.slice();
    }

    if (!urls || urls.length === 0) {
      console.log('USO: node render_page.js <url> <outPath> [waitSelector] [timeout_ms] [maxScrollMs]');
      process.exit(1);
    }

    // optional parameters for waiting and scrolling
    const waitSelector = argv[2] || null;
    const WAIT_TIMEOUT = parseInt(argv[3] || '30000', 10); // ms
    const MAX_SCROLL_MS = parseInt(argv[4] || '30000', 10); // how long to allow adaptive scrolling

    // create rendered dir
    const renderedDir = path.resolve(process.cwd(), 'scripts', 'rendered');
    try { fs.mkdirSync(renderedDir, { recursive: true }); } catch(e){}

    const headless = process.env.HEADLESS !== 'false';
    const browser = await chromium.launch({ headless, args: ['--no-sandbox','--disable-setuid-sandbox'] });

    let anyFailed = false;

    try {
      const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        // viewport default left to Playwright
      });

      // reduce simple detection
      await context.addInitScript(() => {
        Object.defineProperty(navigator, 'webdriver', { get: () => false });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
        window.chrome = window.chrome || { runtime: {} };
      });

      const page = await context.newPage();
      await page.setExtraHTTPHeaders({ 'accept-language': 'en-US,en;q=0.9' });

      // helpers
      async function tryClickSelectors(selArray) {
        for (const sel of selArray) {
          try {
            const els = await page.$$(sel);
            if (!els || els.length === 0) continue;
            for (const e of els) {
              try { await e.click({ timeout: 1200 }); await page.waitForTimeout(200); } catch(e){ /* ignore */ }
            }
            // if we clicked something, give the page a bit to settle
            await page.waitForTimeout(300);
          } catch (e) {
            // ignore
          }
        }
      }

      async function hideSelectorsViaJS(selArray) {
        try {
          await page.evaluate((sels) => {
            for (const s of sels) {
              try {
                const elts = Array.from(document.querySelectorAll(s || ''));
                for (const el of elts) {
                  // hide element as fallback
                  el.style.display = 'none';
                }
              } catch(e){}
            }
          }, selArray);
          await page.waitForTimeout(200);
        } catch(e){}
      }

      async function adaptiveScroll(maxMs) {
        // scroll until scrollHeight stops increasing or until maxMs reached
        const start = Date.now();
        let lastHeight = await page.evaluate(() => document.body ? document.body.scrollHeight : 0);
        let stableLoops = 0;
        const maxStable = 3; // require a few loops with no change to stop
        while (Date.now() - start < maxMs && stableLoops < maxStable) {
          await page.evaluate(() => window.scrollBy(0, window.innerHeight));
          await page.waitForTimeout(600);
          const newHeight = await page.evaluate(() => document.body ? document.body.scrollHeight : 0);
          if (newHeight > lastHeight) {
            stableLoops = 0;
            lastHeight = newHeight;
            // small extra wait to let lazy fetched items appear
            await page.waitForTimeout(400);
          } else {
            stableLoops += 1;
            // small jitter
            await page.waitForTimeout(250);
          }
        }
        return lastHeight;
      }

      async function clickLoadMoreRepeatedly(buttonSelectors, attempts = 5, perWait = 800) {
        for (let i = 0; i < attempts; i++) {
          let clickedAny = false;
          for (const sel of buttonSelectors) {
            try {
              const els = await page.$$(sel);
              for (const b of els) {
                try { await b.click({ timeout: 1500 }); clickedAny = true; } catch(e) {}
              }
            } catch(e){}
          }
          if (!clickedAny) break;
          await page.waitForTimeout(perWait);
        }
      }

      // selectors heuristics
      const overlaySelectors = [
        'button[aria-label*="close"]', 'button[aria-label*="Close"]',
        'button[aria-label*="dismiss"]', 'button[aria-label*="Dismiss"]',
        'button[aria-label*="Accept"]', 'button[aria-label*="Accept cookies"]',
        'button[data-control-name="accept_cookies"]', '.cookie-consent', '.consent-banner',
        '.newsletter-popup', '.newsletter-modal', '.overlay--newsletter', '.onetrust-close-btn-handler', '#onetrust-accept-btn-handler'
      ];
      const seeMoreButtons = [
        'button[aria-label*="see more"]', 'button[aria-label*="See more"]', 'button.feed-shared-inline-show-more-text__see-more-less-toggle',
        'button[data-more-button]', 'a.load-more', 'button.load-more'
      ];
      const loadMoreSelectors = ['button.load-more', 'button[data-control-name="load_more"]', 'button[aria-label*="Load more"]'];

      const NAV_TIMEOUT = 45000;

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
        try {
          // goto: use domcontentloaded so we can control waits + scrolls manually
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT }).catch(()=>{});
          // short initial wait
          await page.waitForTimeout(1200);

          // 1) try to close overlays by clicking common selectors
          await tryClickSelectors(overlaySelectors);
          // fallback: try hiding them via JS if still present
          await hideSelectorsViaJS(overlaySelectors);

          // 2) try to expand "see more" areas
          await tryClickSelectors(seeMoreButtons);

          // 3) If user provided waitSelector: wait for it (within WAIT_TIMEOUT)
          if (waitSelector) {
            try {
              log(`Waiting for selector "${waitSelector}" (timeout ${WAIT_TIMEOUT}ms) ...`);
              await page.waitForSelector(waitSelector, { timeout: WAIT_TIMEOUT });
              log(`Selector "${waitSelector}" appeared.`);
              // do a short adaptive scroll after it appears
              await adaptiveScroll(4000);
            } catch (e) {
              warn(`waitForSelector("${waitSelector}") timed out — will attempt adaptive scroll anyway.`);
              // proceed with adaptive scroll to try and load more content
              await adaptiveScroll(MAX_SCROLL_MS);
            }
          } else {
            // no waitSelector provided: do a moderate adaptive scroll
            await adaptiveScroll(8000);
          }

          // 4) Try clicking any "load more" buttons repeatably
          await clickLoadMoreRepeatedly(loadMoreSelectors, 6, 700);

          // 5) Final adaptive scroll to catch any last lazy loads
          await adaptiveScroll(5000);

          // short final pause
          await page.waitForTimeout(700);

          // Grab final content
          const content = await page.content();

          // Write output
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
