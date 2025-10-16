// scripts/render_page.js
// Playwright renderer improved:
// - retries for navigation
// - per-domain waitForSelector rules for dynamic sites (Yahoo, Medscape, EETimes, etc.)
// - UA rotation and extra headers
// - anti-bot detection and recovery attempts
// - conservative timeouts (domain-specific to avoid big slowdowns)

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

const DEFAULT_NAV_TIMEOUT = 45000;
const SITE_WAITERS = {
  // domain : { selectors: [ ... ], timeout: ms, extraWaitAfterFound: ms }
  'finance.yahoo.com': { selectors: ['section[data-test="qsp-news"]','ul[data-test="quoteNewsStream"]','li.stream-item.story-item','li.js-stream-content'], timeout: 12000, extraWaitAfterFound: 600 },
  'www.medscape.com': { selectors: ['#archives', '.article-list', 'article', '.module-content'], timeout: 14000, extraWaitAfterFound: 800 },
  'www.eetimes.com': { selectors: ['.river', '.post-list', '.article'], timeout: 10000, extraWaitAfterFound: 600 }
};

const UAS = [
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
];

function domainFromUrl(u){
  try { return new URL(u).hostname.toLowerCase(); } catch(e){ return ''; }
}

function sleep(ms){ return new Promise(r => setTimeout(r, ms)); }

(async function(){
  const argv = process.argv.slice(2);
  if (argv.length < 1) {
    console.error('Usage: node scripts/render_page.js <url> [outPath]');
    process.exit(2);
  }
  const url = argv[0];
  let out = argv[1] || null;
  if (!out) {
    const u = new URL(url);
    const hostpart = (u.hostname + u.pathname).replace(/[^a-z0-9\-_.]/gi, '_').slice(0,200);
    out = path.join('scripts','rendered', `${hostpart}-${Date.now()}.html`);
  }
  try { fs.mkdirSync(path.dirname(out), { recursive: true }); } catch(e){}

  const domain = domainFromUrl(url);
  const siteCfg = SITE_WAITERS[domain] || null;

  const browser = await chromium.launch({ headless: true, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-http2'] });
  let anyWarning = false;
  try {
    let attempt = 0;
    const maxRecoveryAttempts = 3;

    while (attempt < maxRecoveryAttempts) {
      attempt++;
      const ua = UAS[(attempt-1) % UAS.length];
      console.log(`Rendering attempt ${attempt} for ${url} (UA: ${ua})`);
      const context = await browser.newContext({ userAgent: ua, locale: 'en-US' });
      // basic stealth-ish init
      await context.addInitScript(() => {
        try {
          Object.defineProperty(navigator, 'webdriver', { get: () => false });
          Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
          Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4,5] });
          window.chrome = window.chrome || { runtime: {} };
        } catch(e){}
      });
      const page = await context.newPage();
      try {
        await page.setExtraHTTPHeaders({
          'accept-language': 'en-US,en;q=0.9',
          'sec-ch-ua': '"Chromium";v="140", "Not A;Brand";v="24"',
          'sec-ch-ua-platform': '"Windows"',
          'sec-ch-ua-mobile': '?0'
        });

        // navigation with retries
        let navErr = null;
        const navAttempts = 2;
        for (let n=0;n<navAttempts;n++){
          try {
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: DEFAULT_NAV_TIMEOUT });
            navErr = null;
            break;
          } catch(e) {
            navErr = e;
            console.warn(`page.goto attempt ${n+1} failed: ${e && e.message ? e.message : e}`);
            await sleep(800 * (n+1));
          }
        }
        if (navErr) {
          console.warn('All page.goto attempts failed (will continue to try to capture content):', navErr.message || navErr);
        }

        // small automatic clicks for common consent overlays
        const consentSelectors = [
          'button:has-text("Accept all")','button:has-text("Accept")','button:has-text("I agree")','button:has-text("Allow all")',
          'button:has-text("Accept cookies")','button[data-testid="consent-accept-button"]'
        ];
        for (const sel of consentSelectors) {
          try {
            const el = await page.$(sel);
            if (el) {
              console.log('Clicking consent selector:', sel);
              try { await el.click({ timeout: 2000 }); } catch(e){ try{ await page.evaluate(el=>el.click(), el); }catch(e2){} }
              await page.waitForTimeout(400);
            }
          } catch(e){}
        }

        // if siteCfg present, wait for at least one selector
        let foundSelector = false;
        if (siteCfg && Array.isArray(siteCfg.selectors) && siteCfg.selectors.length) {
          for (const sel of siteCfg.selectors) {
            try {
              await page.waitForSelector(sel, { timeout: siteCfg.timeout || 8000 }).then(()=>{ foundSelector = true; });
              if (foundSelector) break;
            } catch(e){}
          }
          if (foundSelector) {
            const extra = siteCfg.extraWaitAfterFound || 300;
            await page.waitForTimeout(extra);
          } else {
            // fallback small wait to allow dynamic loading
            await page.waitForTimeout(900);
          }
        } else {
          // generic wait to allow JS loads (short)
          await page.waitForTimeout(900);
        }

        // try scroll to trigger lazy loads
        try {
          await page.evaluate(async () => {
            const step = window.innerHeight || 800;
            for (let i=0;i<6;i++){
              window.scrollBy(0, step);
              await new Promise(r => setTimeout(r, 300));
            }
          });
        } catch(e){}

        // capture content and inspect for anti-bot signals
        const content = await page.content();
        const low = (content || '').toLowerCase();
        const antibotDetected = low.includes('verify you are human') || low.includes('just a moment') || low.includes('captcha') || low.includes('checking your browser') || low.includes('cloudflare');
        if (antibotDetected) {
          console.warn('ANTIBOT: detected content on attempt', attempt, ', will try recovery with different UA/context');
          anyWarning = true;
          // close page/context and try again with next UA
          try { await page.close(); } catch(e){}
          try { await context.close(); } catch(e){}
          continue; // next attempt
        }

        // Save content and exit loop
        fs.writeFileSync(out, content, { encoding: 'utf8' });
        const bytes = Buffer.byteLength(content, 'utf8');
        console.log(`Rendered ${url} -> ${out} (saved, bytes: ${bytes})`);
        // close and break
        try { await page.close(); } catch(e){}
        try { await context.close(); } catch(e){}
        break;
      } catch (errPage) {
        console.warn('Render error (page-level):', errPage && errPage.message ? errPage.message : errPage);
        try { await page.close(); } catch(e){}
        try { await context.close(); } catch(e){}
        anyWarning = true;
        // loop to next attempt
      }
    } // attempts loop

  } catch (err) {
    console.error('Fatal render error:', err && (err.message || err));
  } finally {
    try { await browser.close(); } catch(e){}
  }

  if (anyWarning) {
    console.warn('Rendering completed with warnings — check logs and rendered files.');
    process.exit(0);
  } else {
    process.exit(0);
  }
})();
