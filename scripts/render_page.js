// scripts/render_page.js
// Robust renderer with selective anti-bot recovery (per-domain).
//
// Usage:
//   node scripts/render_page.js "<url>" "scripts/rendered/out.html"
//   or: node scripts/render_page.js "<url1>" "<url2>" ...
//
// Environment variables (optional):
//   HEADLESS=false    -> run visible browser (useful for debug)
//   NAV_TIMEOUT       -> navigation timeout in ms (default 45000)
//   RECOVERY_DOMAINS  -> comma-separated domains to attempt recovery (default: medscape.com,eetimes.com)
//   NO_RECOVERY_DOMAINS -> comma-separated domains to never attempt recovery (default: yahoo.com,finance.yahoo.com)

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

function now() { return (new Date()).toISOString(); }
function log(...a){ console.log(now(), ...a); }
function warn(...a){ console.warn(now(), ...a); }

function sanitizeFilename(s) {
  return String(s || '').replace(/[^a-z0-9\-_.]/gi, '_').replace(/_+/g, '_').slice(0, 200);
}

function domainOf(u) {
  try { return (new URL(u)).hostname.replace(/^www\./,'').toLowerCase(); } catch(e) { return ''; }
}

function has_antibot_text(html) {
  if (!html) return false;
  const l = html.toLowerCase();
  const phrases = [
    'verify you are human','checking your browser','access denied','please enable javascript',
    'cloudflare','ddos protection','turn on javascript','are you human','captcha','security check',
    'verify that you are not a robot'
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

    // config: domains to attempt light recovery for, and domains to never touch
    const defaultRecovery = ['medscape.com','eetimes.com'];
    const defaultNoRecovery = ['yahoo.com','finance.yahoo.com'];

    const RECOVERY_DOMAINS = (process.env.RECOVERY_DOMAINS ? process.env.RECOVERY_DOMAINS.split(',').map(s=>s.trim()).filter(Boolean) : defaultRecovery);
    const NO_RECOVERY_DOMAINS = (process.env.NO_RECOVERY_DOMAINS ? process.env.NO_RECOVERY_DOMAINS.split(',').map(s=>s.trim()).filter(Boolean) : defaultNoRecovery);

    const UAs = [
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.5845.96 Safari/537.36'
    ];

    const browser = await chromium.launch({ headless, args: ['--no-sandbox','--disable-setuid-sandbox','--disable-http2'] });

    let anyWarnings = false;

    for (const originalUrl of urls) {
      const start = Date.now();
      const domain = domainOf(originalUrl);
      let outPath = explicitOut;
      if (!outPath) {
        const u = (() => { try { return new URL(originalUrl); } catch(e) { return null; } })();
        const hostpart = u ? sanitizeFilename(u.hostname + (u.pathname || '')) : sanitizeFilename(originalUrl);
        const ts = Date.now();
        outPath = path.join(renderedDir, `${hostpart}-${ts}.html`);
      }

      log(`Rendering ${originalUrl} -> ${outPath} (domain=${domain})`);
      // create a fresh context & page per URL to avoid reuse issues
      const context = await browser.newContext({ userAgent: UAs[0], viewport: { width: 1366, height: 768 } });
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
          'sec-ch-ua-mobile': '?0'
        });
      } catch(e){}

      const page = await context.newPage();

      // Navigation with retries
      let gotoErr = null;
      for (let attempt = 1; attempt <= 3; attempt++) {
        try {
          await page.goto(originalUrl, { waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT });
          // small wait for dynamic bits
          await page.waitForTimeout(800);
          gotoErr = null;
          break;
        } catch (e) {
          gotoErr = e;
          warn(`page.goto attempt ${attempt} failed for ${originalUrl}: ${e && e.message ? e.message : e}`);
          await page.waitForTimeout(800 * attempt);
        }
      }
      if (gotoErr) {
        warn(`All page.goto attempts failed for ${originalUrl}: ${gotoErr && gotoErr.message ? gotoErr.message : gotoErr}`);
        anyWarnings = true;
      }

      // try to dismiss common cookie dialogs quickly
      try {
        const consentSelectors = [
          'button:has-text("Accept all")','button:has-text("Accept")','button:has-text("I agree")',
          'button[aria-label*="Accept"]','button[id*="accept"]','button[name*="agree"]',
          'button:has-text("Allow all")','button:has-text("Accept cookies")',
          'button[data-testid="consent-accept-button"]'
        ];
        for (const sel of consentSelectors) {
          try {
            const els = await page.$$(sel);
            for (const e of els) {
              try { await e.click({ timeout: 1200 }); } catch(e2){ try { await page.evaluate(el => el.click(), e); } catch(e3){} }
            }
            if (els.length) await page.waitForTimeout(300);
          } catch(e){}
        }
        // remove basic overlays
        await page.evaluate(() => {
          const bad = document.querySelectorAll('[id*="consent"], [class*="consent"], [class*="cookie"], [id*="cookie"], .cookie-banner, .consent-banner, .overlay, .modal-backdrop');
          for (const n of bad) try { n.remove(); } catch(e) {}
        });
      } catch(e){}

      // short auto-scroll (lightweight)
      try {
        await page.evaluate(async () => {
          await new Promise(r => {
            let i = 0;
            const t = setInterval(() => {
              window.scrollBy(0, window.innerHeight * 0.6);
              i++;
              if (i > 4) { clearInterval(t); r(); }
            }, 250);
          });
        });
      } catch(e){}

      // capture content
      let html = '';
      try {
        html = await page.content();
      } catch(e) {
        warn('Error reading page content:', e && e.message ? e.message : e);
        anyWarnings = true;
      }

      // decide whether to attempt lightweight recovery:
      const shouldRecover = domain && RECOVERY_DOMAINS.includes(domain) && !(NO_RECOVERY_DOMAINS.includes(domain));
      if (shouldRecover && has_antibot_text(html)) {
        warn('Detected anti-bot content for', originalUrl);
        // do a few lightweight recovery steps (reload, mouse moves, additional waits); don't recreate contexts
        let recovered = false;
        try {
          // attempt small recovery sequence
          for (let rtry = 1; rtry <= 2; rtry++) {
            warn(`ANTIBOT: recovery attempt ${rtry} for ${originalUrl}`);
            try { await page.reload({ waitUntil: 'domcontentloaded', timeout: NAV_TIMEOUT/2 }); } catch(e){}
            await page.waitForTimeout(1200 + 800 * rtry);

            // small mouse movements
            try {
              const rect = await page.evaluate(() => ({ w: window.innerWidth, h: window.innerHeight }));
              const w = rect.w || 800, h = rect.h || 600;
              for (let i = 0; i < 6; i++) {
                const x = Math.floor(10 + Math.random() * (w - 20));
                const y = Math.floor(10 + Math.random() * (h - 20));
                try { await page.mouse.move(x, y, { steps: 5 }); } catch(e){}
                await page.waitForTimeout(120);
              }
              try { await page.mouse.click(10,10); } catch(e){}
            } catch(e){}

            // extra scroll
            try {
              await page.evaluate(async () => {
                await new Promise(r => {
                  let i = 0;
                  const t = setInterval(() => {
                    window.scrollBy(0, window.innerHeight * 0.5);
                    i++;
                    if (i > 6) { clearInterval(t); r(); }
                  }, 250);
                });
              });
            } catch(e){}

            // wait for generic content selectors (some domains)
            const preferredMap = {
              'medscape.com': '#archives',
              'eetimes.com': '.archive-list, .cd-article-list, .cards-list'
            };
            const pref = preferredMap[domain] || null;
            if (pref) {
              try {
                await page.waitForSelector(pref, { timeout: 5000 });
                recovered = true;
                log('ANTIBOT: found preferred selector after recovery:', pref);
                break;
              } catch(e){}
            }

            // re-fetch content and check
            try { html = await page.content(); } catch(e){}
            if (!has_antibot_text(html)) { recovered = true; break; }

            await page.waitForTimeout(600);
          }
        } catch(e){
          warn('ANTIBOT: recovery sequence threw:', e && e.message ? e.message : e);
        }

        if (!recovered && has_antibot_text(html)) {
          warn('ANTIBOT: recovery attempts exhausted for', originalUrl);
          anyWarnings = true;
        } else {
          log('ANTIBOT: recovery appears successful (or antibot phrases gone) for', originalUrl);
        }
      }

      // final read (capture up-to-date content)
      try { html = await page.content(); } catch(e){}

      // always save HTML even if small or antibot
      try {
        const dir = path.dirname(outPath);
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(outPath, html, 'utf8');
        const bytes = Buffer.byteLength(html, 'utf8');
        if (bytes < 2000) {
          warn(`Rendered ${originalUrl} -> ${outPath} (saved, bytes: ${bytes}) - SMALL RENDER`);
          anyWarnings = true;
        } else {
          log(`Rendered ${originalUrl} -> ${outPath} (saved, bytes: ${bytes})`);
        }
      } catch (e) {
        warn('Failed to save rendered content:', e && e.message ? e.message : e);
        anyWarnings = true;
      }

      try { await page.close(); } catch(e){}
      try { await context.close(); } catch(e){}
      const elapsed = Math.round((Date.now() - start) / 1000);
      log(`-> Done: ${originalUrl} (elapsed ${elapsed}s)`);
    } // end for urls

    try { await browser.close(); } catch(e){}

    if (anyWarnings) {
      warn('Rendering completed with warnings — check logs and rendered files.');
    } else {
      log('All renders completed successfully.');
    }
    // Important: exit 0 to avoid breaking CI; consumer can check logs/files for bad renders
    process.exit(0);

  } catch (err) {
    console.error('Fatal render error:', err && (err.message || err));
    try { process.exit(0); } catch(e){}
  }
})();
