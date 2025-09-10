#!/usr/bin/env node
// scripts/render_linkedin_extract.js (VERSÃO DEBUG + AUTO-SCROLL + SAVE HTML/PNG)
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const Excel = require('exceljs');

(async () => {
  console.log('START render_linkedin_extract.js - cwd=', process.cwd());
  console.log('argv=', process.argv);

  const scriptDir = __dirname;
  const outDir = path.join(scriptDir, 'output');
  fs.mkdirSync(outDir, { recursive: true });

  const storagePath = path.join(scriptDir, 'linkedin_auth.json'); // se existir, usaremos para login
  const urls = process.argv.slice(2);
  if (urls.length === 0) {
    console.log('USO: node render_linkedin_extract.js <url1> <url2> ...');
    process.exit(1);
  }

  const headless = process.env.HEADLESS !== 'false';
  const browser = await chromium.launch({ headless, args: ['--no-sandbox','--disable-setuid-sandbox'] });

  try {
    let context;
    if (fs.existsSync(storagePath)) {
      console.log('Using existing storageState:', storagePath);
      context = await browser.newContext({ storageState: storagePath });
    } else {
      console.log('No storageState found at', storagePath);
      context = await browser.newContext();
    }

    const page = await context.newPage();
    const email = process.env.LINKEDIN_EMAIL || '';
    const password = process.env.LINKEDIN_PASSWORD || '';

    // tenta login automático se não houver storage e houver credenciais
    if (!fs.existsSync(storagePath) && email && password) {
      try {
        console.log('Tentando login automático...');
        await page.goto('https://www.linkedin.com/login', { waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.fill('input#username, input[name="session_key"]', email).catch(()=>{});
        await page.fill('input#password, input[name="session_password"]', password).catch(()=>{});
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 20000 }).catch(()=>{}),
          page.click('button[type="submit"], button.sign-in-form__submit-button').catch(()=>{})
        ]);
        await page.waitForTimeout(3000);
        // tenta gravar storageState caso login tenha funcionado
        try {
          await context.storageState({ path: storagePath });
          console.log('Gravou storageState em', storagePath);
        } catch(e){ console.warn('Não conseguiu gravar storageState:', e && e.message); }
      } catch (e) {
        console.warn('Login automatico falhou:', e && e.message);
      }
    }

    const results = [];
    const badHrefSubstr = ['help', 'legal', 'cookie', 'privacy', 'terms', 'signin', 'login', 'settings', 'consent', 'preferences', 'policies', 'mailto'];

    // helper: scroll down slowly to load dynamic posts
    async function autoScroll(page, maxScrolls = 12, delay = 800) {
      for (let i = 0; i < maxScrolls; i++) {
        await page.evaluate(() => { window.scrollBy(0, window.innerHeight); });
        await page.waitForTimeout(delay);
      }
    }

    for (const url of urls) {
      console.log('>>> Loading', url);
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 }).catch(()=>{});
        await page.waitForTimeout(1500);

        // close cookie / overlays if present
        const overlaySelectors = [
          'button[aria-label*="close"]', 'button[aria-label*="Close"]',
          'button.artdeco-modal__dismiss', 'button[aria-label*="dismiss"]',
          'button[aria-label*="Accept"]', 'button[aria-label*="Accept cookies"]',
          'button[data-control-name="accept_cookies"]'
        ];
        for (const s of overlaySelectors) {
          try {
            const els = await page.$$(s);
            if (els && els.length) {
              for (const e of els) { try { await e.click({ timeout: 2000 }); } catch(e){} }
            }
          } catch(e){}
        }

        // expand possible "see more" buttons
        const moreBtns = await page.$$(
          'button.feed-shared-inline-show-more-text__see-more-less-toggle, button[aria-label*="see more"], button[aria-label*="ver mais"], button[aria-label*="…mais"], button[aria-label*="See more"]'
        );
        console.log('see-more buttons found (initial):', moreBtns.length);
        for (const b of moreBtns) { try { await b.click({ timeout: 2000 }); } catch(e){} }
        await page.waitForTimeout(800);

        // auto scroll to load more posts (improves chance to load group feed)
        await autoScroll(page, 10, 900);

        // attempt to click "Load more" buttons if present (selector examples)
        const loadMoreSelectors = ['button.load-more', 'button[data-control-name="load_more"]', 'button[aria-label*="Load more"]'];
        for (const sel of loadMoreSelectors) {
          try {
            const btns = await page.$$(sel);
            for (const b of btns) { try { await b.click({ timeout: 2000 }); await page.waitForTimeout(800); } catch(e){} }
          } catch(e){}
        }

        // final wait
        await page.waitForTimeout(1000);

        // SAVE debug HTML & screenshot (úteis para entender o que o runner vê)
        try {
          const timestamp = Date.now();
          const debugHtml = path.join(outDir, `debug-${timestamp}.html`);
          const debugPng = path.join(outDir, `debug-${timestamp}.png`);
          const content = await page.content();
          fs.writeFileSync(debugHtml, content, 'utf8');
          try { await page.screenshot({ path: debugPng, fullPage: true }); } catch(e){}
          console.log('Saved debug HTML and PNG:', debugHtml, debugPng);
        } catch(e){ console.warn('Não conseguiu salvar debug files:', e && e.message); }

        // select candidate post nodes
        const posts = await page.$$eval(
          'div.feed-shared-update-v2, div.occludable-update, div.feed-shared-update, div.update-components-actor__container, div.feed-shared-update-v2--wrapped',
          (nodes, badSubstr) => {
            const out = [];
            const text = el => el ? (el.innerText || el.textContent || '').trim() : '';
            for (const n of nodes) {
              const actorAnchor = n.querySelector('a.update-components-actor__meta-link, a[data-test-app-aware-link], a.update-components-actor__image, a.update-components-actor__meta');
              let title = '', link = '';
              if (actorAnchor) {
                const titleNode = actorAnchor.querySelector('.update-components-actor__title') || actorAnchor;
                title = text(titleNode).replace(/\n+/g,' ').trim();
                link = actorAnchor.getAttribute('href') || '';
              } else {
                const anchors = Array.from(n.querySelectorAll('a'));
                let chosen = null;
                for (const a of anchors) {
                  const h = a.getAttribute && a.getAttribute('href') || '';
                  if (!h) continue;
                  const low = h.toLowerCase();
                  if (badSubstr.some(bs => low.includes(bs))) continue;
                  if (low.startsWith('http') || low.includes('/in/') || low.includes('/groups/') || low.includes('/posts/') || low.includes('/feed/')) {
                    chosen = a; break;
                  }
                  if (!chosen) chosen = a;
                }
                if (chosen) {
                  title = text(chosen).replace(/\n+/g,' ').trim();
                  link = chosen.getAttribute('href') || '';
                } else {
                  const h = n.querySelector('h3, h4, a.title, a');
                  title = text(h);
                  link = h && h.getAttribute ? (h.getAttribute('href')||'') : '';
                }
              }
              const descNode = n.querySelector('.update-components-text, .feed-shared-inline-show-more-text__text, .update-components-commentary, .feed-shared-text, .feed-shared-inline-show-more-text, .commentary, p');
              const description = descNode ? text(descNode).replace(/\n+/g,' ').trim() : '';
              const dateNode = n.querySelector('time, .update-components-actor__sub-description, span[aria-hidden], .timestamp, .feed-shared-actor__sub-description');
              const date = dateNode ? text(dateNode).replace(/\n+/g,' ').trim() : '';
              out.push({
                title: title || '',
                link: link || '',
                date: date || '',
                description: description || '',
                snippet: (n.innerText || '').slice(0,400)
              });
            }
            return out;
          },
          badHrefSubstr
        );

        console.log('Found posts on page:', posts.length);

        for (const p of posts) {
          let href = p.link || '';
          if (href && href.startsWith('/')) {
            try { const base = new URL(url); href = base.origin + href; } catch(e){}
          }
          if (!href || badHrefSubstr.some(bs => href.toLowerCase().includes(bs))) p.link = '';
          else p.link = href;
          results.push(Object.assign({ source: url }, p));
        }

      } catch (err) {
        console.error('Error processing', url, err && err.message);
      }
    } // end urls loop

    try { await context.close(); } catch(e){}
    await browser.close();

    const outJson = path.join(outDir, 'linkedin.json');
    fs.writeFileSync(outJson, JSON.stringify(results, null, 2), 'utf8');
    console.log('Wrote', outJson, 'items=', results.length);

    // grava XLSX
    try {
      const wb = new Excel.Workbook();
      const ws = wb.addWorksheet('linkedin');
      ws.columns = [
        { header: 'title', key: 'title', width: 60 },
        { header: 'link', key: 'link', width: 60 },
        { header: 'date', key: 'date', width: 20 },
        { header: 'description', key: 'description', width: 80 },
        { header: 'source', key: 'source', width: 60 }
      ];
      results.forEach(r => ws.addRow({ title: r.title, link: r.link, date: r.date, description: r.description, source: r.source }));
      const outXlsx = path.join(outDir, 'linkedin.xlsx');
      await wb.xlsx.writeFile(outXlsx);
      console.log('Wrote', outXlsx);
    } catch (e) {
      console.error('Failed to write XLSX:', e && e.message);
    }

    console.log('Done. Extracted items:', results.length);
  } catch (outerErr) {
    console.error('Fatal error in script:', outerErr && outerErr.message);
    try { await browser.close(); } catch(e){}
    process.exit(1);
  }
})();
