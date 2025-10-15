// scripts/render_page.js
// Robust renderer with anti-bot recovery heuristics for CI (Playwright Chromium).
// Usage:
//   node scripts/render_page.js "<url>" "scripts/rendered/out.html"
//   or: node scripts/render_page.js "<url1>" "<url2>" ...
//
// Notes:
// - HEADLESS env var controls headless mode (HEADLESS=false to see browser).
// - NAV_TIMEOUT env var (ms) can override default 45000.

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

function now() { return (new Date()).toISOString(); }
function log(...a){ console.log(now(), ...a); }
function warn(...a){ console.warn(now(), ...a); }

function sanitizeFilename(s) {
  return String(s || '').replace(/[^a-z0-9\-_.]/gi, '_').replace(/_+/g, '_').slice(0, 200);
}

// detect anti-bot phrases in content (lowercase)
function has_antibot_text(html) {
  if (!html) return false;
  const l = html.toLowerCase();
  const phrases = [
    'verify you are human',
    'checking your browser',
    'access denied',
    'please enable javascript',
    'cloudflare',
    'ddos protection',
    'turn on javascript',
    'are you human',
    'captcha',
    'security check'
  ];
  return phrases.some(p => l.indexOf(p) !== -1);
}

(async () => {
  try {
    const argv = process.argv.slice(2);
    if (!argv || argv.length === 0) {
      console.log('USO: node scripts/render_page.js <url> <outPath?>   OR   node scripts/render_page.js <url1> <url2> ...');
      process.exit(1);
    }

    // parse args
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

    const headless = (process.env.HEADLESS === undefined) ? true : (process.env.HEADLESS !== 'false');
    const NAV_TIMEOUT = parseInt(process.env.NAV_TIMEOUT || '45000', 10);
    // how many anti-bot recovery attempts
    const MAX_ANTIBOT_ATTEMPTS = 3;

    // Candidate user agents to rotate if anti-bot triggered
    const UAs = [
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36'
    ];

    // Launch browser with helpful flags
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
      // We'll create a single context and reuse, but if antibot recovery needs UA change we'll create new context
      let context = await browser.newContext({
        userAgent: UAs[0],
        viewport: { width: 1366, height: 768 }
      });

      // small stealth init
      await context.addInitScript(() => {
        try {
          Object.defineProperty(navigator, 'webdriver', { get: () => false });
          Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
          Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
          window.chrome = window.chrome || { runtime: {} };
        } catch (e) {}
      });

      try {
        await context.setExtraHTTPHeaders({
          'accept-language': 'en-US,en;q=0.9',
          'sec-ch-ua': '"Chromium";v="140", "Not A;Brand";v="24"',
          'sec-ch-ua-platform': '"Windows"',
          'sec-ch-ua-mobile': '?0',
          'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
          'upgrade-insecure-requests': '1'
        });
      } catch(e){ /* ignore */ }

      const page = await context.newPage();

      // helper to attempt to “unstick” anti-bot pages
      async function attempt_antibot_recovery(page, url, preferredSelector) {
        // try multiple recovery strategies
        for (let attempt = 1; attempt <= MAX_ANTIBOT_ATTEMPTS; attempt++) {
          warn(`ANTIBOT: attempt ${attempt} recovery for ${url}`);

          // 1) simple reload
          try { await page.reload({ waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT/2 }); } catch(e){}

          // small wait
          await page.waitForTimeout(1200 + 1000*attempt);

          // 2) simulate small human activity: mouse moves and clicks
          try {
            const box = await page.evaluate(() => ({ w: window.innerWidth, h: window.innerHeight }));
            const w = box.w || 800, h = box.h || 600;
            // move in a curve
            const steps = 10;
            for (let i=0;i<steps;i++){
              const x = Math.floor((w/4) + (w/2) * (i/steps));
              const y = Math.floor((h/4) + (h/2) * Math.abs(Math.sin(i)));
              try { await page.mouse.move(x, y, { steps: 4 }); } catch(e){}
              await page.waitForTimeout(120);
            }
            // click somewhere safe
            try { await page.mouse.click(Math.max(10, Math.floor(w*0.1)), Math.max(10, Math.floor(h*0.1))); } catch(e){}
          } catch(e){}

          // 3) scroll slowly to trigger lazy loads
          try {
            await page.evaluate(async () => {
              await new Promise(r => {
                let total = 0, step = 400;
                const timer = setInterval(() => {
                  window.scrollBy(0, step);
                  total += step;
                  if (total > document.body.scrollHeight + 1000) { clearInterval(timer); r(); }
                }, 300);
                // safety timeout 8s
                setTimeout(() => { clearInterval(timer); r(); }, 8000);
              });
            });
          } catch(e){}

          // 4) wait for preferredSelector if given
          if (preferredSelector) {
            try {
              await page.waitForSelector(preferredSelector, { timeout: 8000 });
              log('ANTIBOT: recovery succeeded (preferred selector found)');
              return true;
            } catch(e) { /* continue attempts */ }
          }

          // 5) if still stuck, try reload with a rotated UA by creating a new context (clean slate)
          if (attempt < MAX_ANTIBOT_ATTEMPTS) {
            const ua = UAs[attempt % UAs.length] || UAs[0];
            warn(`ANTIBOT: recreating context with UA=${ua}`);
            try {
              try { await page.close(); } catch(e){}
              try { await context.close(); } catch(e){}
            } catch(e){}
            context = await browser.newContext({ userAgent: ua, viewport: { width: 1366, height: 768 } });
            await context.addInitScript(() => {
              try {
                Object.defineProperty(navigator, 'webdriver', { get: () => false });
                Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
                Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });
                window.chrome = window.chrome || { runtime: {} };
              } catch (e) {}
            });
            try {
              await context.setExtraHTTPHeaders({
                'accept-language': 'en-US,en;q=0.9',
                'sec-ch-ua': '"Chromium";v="140", "Not A;Brand";v="24"',
                'sec-ch-ua-platform': '"Windows"',
                'sec-ch-ua-mobile': '?0',
              });
            } catch(e){}
            // rebuild page reference
            page = await context.newPage();
            try {
              await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
              await page.waitForTimeout(1200);
            } catch(e){}
          }

          // brief pause before next attempt
          await new Promise(r => setTimeout(r, 1200));
          // check if content no longer appears as antibot
          const html = await page.content();
          if (!has_antibot_text(html)) {
            log('ANTIBOT: page content no longer contains antibot phrases');
            return true;
          }
        } // end attempts

        // if we get here recovery failed
        warn('ANTIBOT: all recovery attempts exhausted');
        return false;
      }

      // main loop for urls
      for (const originalUrl of urls) {
        const url = originalUrl;
        const start = Date.now();
        let outPath = explicitOut;
        if (!outPath) {
          const u = (() => { try { return new URL(url); } catch(e) { return null; } })();
          const hostpart = u ? sanitizeFilename(u.hostname + (u.pathname || '')) : sanitizeFilename(url);
          const ts = Date.now();
          outPath = path.join(renderedDir, `${hostpart}-${ts}.html`);
        }

        log(`Starting render for: ${url}`);

        // try navigation with retries
        let gotoErr = null;
        const maxGotoAttempts = 3;
        for (let attempt = 1; attempt <= maxGotoAttempts; attempt++) {
          try {
            await page.goto(url, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
            await page.waitForTimeout(1200);
            try { await page.waitForLoadState('networkidle', { timeout: 5000 }); } catch(e){}
            gotoErr = null;
            break;
          } catch (e) {
            gotoErr = e;
            warn(`page.goto attempt ${attempt} failed: ${e && e.message ? e.message : e}`);
            await page.waitForTimeout(1000 * attempt);
          }
        }
        if (gotoErr) {
          warn(`page.goto ultimately failed for ${url}: ${gotoErr && gotoErr.message ? gotoErr.message : gotoErr}`);
          // continue anyway (we'll still try to salvage content)
        }

        // try clicking/dismissing consents
        const consentSelectors = [
          'button:has-text("Accept all")','button:has-text("Accept")','button:has-text("I agree")',
          'button[aria-label*="Accept"]','button[id*="accept"]','button[name*="agree"]',
          'button:has-text("Allow all")','button:has-text("Accept cookies")',
          'button[data-testid="consent-accept-button"]','input[type="button"][value*="Accept"]',
          '[role="button"]:has-text("Accept all")'
        ];
        for (const sel of consentSelectors) {
          try {
            const els = await page.$$(sel);
            for (const e of els) {
              try { await e.click({ timeout: 2000 }); } catch(e2){ try { await page.evaluate(el => el.click(), e); } catch(e3){} }
            }
            if (els.length) await page.waitForTimeout(400);
          } catch(e){}
        }

        // remove overlay elements
        try {
          await page.evaluate(() => {
            const bad = document.querySelectorAll('[id*="consent"], [class*="consent"], [class*="cookie"], [id*="cookie"], .cookie-banner, .consent-banner, .overlay, .modal-backdrop');
            for (const n of bad) try { n.remove(); } catch(e) {}
          });
        } catch(e){}

        // expand see-more, click load-more, scroll
        try {
          const seeMore = await page.$$('button[aria-label*="see more"], button[aria-label*="See more"], button.show-more, a.show-more');
          for (const b of seeMore) { try { await b.click({ timeout: 1000 }); } catch(e){} }
        } catch(e){}

        // a longer scroll to load things
        try {
          await page.evaluate(async () => {
            await new Promise(r => {
              let i = 0;
              const t = setInterval(() => {
                window.scrollBy(0, window.innerHeight * 0.9);
                i++;
                if (i > 8) { clearInterval(t); r(); }
              }, 400);
            });
          });
        } catch(e){}

        // give extra time for dynamic content
        await page.waitForTimeout(800);

        // capture initial content and check for anti-bot phrases
        let html = await page.content();
        if (has_antibot_text(html)) {
          warn('Detected anti-bot content for', url);
          // attempt recovery with preferred selector '#archives' (medscape list uses '#archives')
          const recovered = await attempt_antibot_recovery(page, url, '#archives');
          html = await page.content();
          if (!recovered && has_antibot_text(html)) {
            warn('Anti-bot recovery failed for', url, '— saving current HTML (likely block page)');
          } else {
            log('Anti-bot recovery likely successful for', url);
          }
        }

        // write out the html
        try {
          const dir = path.dirname(outPath);
          fs.mkdirSync(dir, { recursive: true });
          fs.writeFileSync(outPath, html, 'utf8');
          const bytes = Buffer.byteLength(html, 'utf8');
          if (bytes < 2000) {
            warn(`Rendered ${url} -> ${outPath} (status: saved, bytes: ${bytes}) - SMALL RENDER`);
          } else {
            log(`Rendered ${url} -> ${outPath} (status: saved, bytes: ${bytes})`);
          }
        } catch (e) {
          warn('Failed to save rendered content:', e && e.message ? e.message : e);
          anyFailed = true;
        }

        const elapsed = Math.round((Date.now() - start)/1000);
        log(`-> Done: ${url} (elapsed ${elapsed}s)`);
      } // end for urls

      try { await context.close(); } catch(e){}
    } finally {
      try { await browser.close(); } catch(e){}
    }

    if (anyFailed) {
      warn('Some renders failed (see logs). Exiting code 2.');
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
