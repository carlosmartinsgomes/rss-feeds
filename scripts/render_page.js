#!/usr/bin/env node
// scripts/render_page.js
// Uso: node scripts/render_page.js <url> <out_html_path>
// Renderiza a página com Playwright e grava HTML (e screenshot debug).

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

async function ensureDirFor(filePath){
  const dir = path.dirname(filePath);
  fs.mkdirSync(dir, { recursive: true });
}

async function removeOverlays(page){
  try {
    await page.evaluate(() => {
      const sel = [
        '[role="dialog"]', '.newsletter-popup', '.newsletter-modal', '.modal-backdrop',
        'div[class*="subscribe"]', 'div[id*="subscribe"]',
        '.subscription-overlay', '.overlay--newsletter', '.paywall', '.cookie-banner'
      ];
      sel.forEach(s => {
        document.querySelectorAll(s).forEach(n => {
          try { n.remove(); } catch(e){ try { n.style.display='none'; } catch(e){} }
        });
      });
      // hide large fixed subscribe nodes
      document.querySelectorAll('div').forEach(d => {
        try {
          const cs = getComputedStyle(d);
          if (!cs) return;
          if ((cs.position === 'fixed' || cs.position === 'sticky') && cs.zIndex && parseInt(cs.zIndex||0) > 1000) {
            const t = (d.innerText||'').toLowerCase();
            if (t.includes('subscribe') || t.includes('become a member') || t.includes('sign in') || t.includes('accept cookies')) {
              d.remove();
            }
          }
        } catch(e){}
      });
    });
  } catch(e){ /* ignore */ }
}

(async () => {
  const argv = process.argv.slice(2);
  if (!argv[0]) { console.error('Usage: node render_page.js <url> <out_html_path>'); process.exit(2); }
  const url = argv[0];
  const outPath = argv[1] || `scripts/rendered/${(new URL(url)).hostname.replace(/[:\/]/g,'')}.html`;
  try {
    ensureDirFor(outPath);
  } catch(e){}
  const headless = (process.env.HEADLESS === 'false') ? false : true;
  const browser = await chromium.launch({ headless, args:['--no-sandbox','--disable-setuid-sandbox'] });
  try {
    const context = await browser.newContext();
    const page = await context.newPage();
    page.setDefaultNavigationTimeout(45000);
    console.log('Rendering', url, '->', outPath);
    await page.goto(url, { waitUntil: 'domcontentloaded' }).catch(()=>{});
    // wait a little to let JS run
    await page.waitForTimeout(1500);
    // try to close cookie dialogs / overlays
    const clickSelectors = [
      'button[aria-label*="close"]', 'button[aria-label*="Close"]', 'button[aria-label*="dismiss"]',
      'button[data-testid*="close"]', 'button[class*="close"]', 'button[title*="Close"]',
      'button[aria-label*="accept"]', 'button[aria-label*="Accept cookies"]'
    ];
    for (const s of clickSelectors) {
      try {
        const els = await page.$$(s);
        if (els && els.length) {
          for (const e of els) { try { await e.click({ timeout: 1000 }); } catch(e){} }
        }
      } catch(e){}
    }
    // remove overlays programmatically
    await removeOverlays(page);
    // scroll slowly to lazy-load
    await page.evaluate(async () => {
      await new Promise(r => {
        let i = 0;
        const max = 6;
        function step(){
          window.scrollBy(0, window.innerHeight);
          i++;
          if (i >= max) return r();
          setTimeout(step, 400);
        }
        step();
      });
    }).catch(()=>{});
    await page.waitForTimeout(800);
    // save content
    const content = await page.content();
    fs.writeFileSync(outPath, content, 'utf8');
    // try full page screenshot (best-effort)
    try {
      const shotPath = outPath.replace(/\.html?$/i, '') + '.png';
      await page.screenshot({ path: shotPath, fullPage: true }).catch(()=>{});
      console.log('Saved screenshot (if possible).');
    } catch(e){}
    console.log('Wrote', outPath);
    await context.close();
    await browser.close();
    process.exit(0);
  } catch (err) {
    console.error('Render error:', err && (err.stack||err.message||err));
    try { await browser.close(); } catch(e){}
    process.exit(1);
  }
})();
