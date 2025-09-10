#!/usr/bin/env node
// scripts/render_linkedin_extract.js
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const Excel = require('exceljs');

(async () => {
  console.log('START render_linkedin_extract.js - cwd=', process.cwd());
  console.log('argv=', process.argv);

  const outDir = path.join('scripts','output');
  fs.mkdirSync(outDir, { recursive: true });

  const storagePath = path.join('scripts','linkedin_auth.json'); // se existir, usaremos para login
  const urls = process.argv.slice(2);
  if (urls.length === 0) {
    console.log('USO: node scripts/render_linkedin_extract.js <url1> <url2> ...');
    console.log('Exemplo de URL: https://www.linkedin.com/groups/5146549/');
    process.exit(1);
  }

  // HEADLESS control: se quiseres ver o browser localmente define HEADLESS=false
  const headless = process.env.HEADLESS !== 'false';
  const browser = await chromium.launch({ headless });

  const contextOptions = {};
  if (fs.existsSync(storagePath)) {
    console.log('Using storageState (session):', storagePath);
    contextOptions.storageState = storagePath;
  } else {
    console.log('No storageState found at', storagePath, '- running without login (many posts may be hidden).');
  }

  const context = await browser.newContext(contextOptions);
  const page = await context.newPage();

  const results = [];

  for (const url of urls) {
    console.log('>>> Loading', url);
    try {
      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
      // small wait for dynamic content
      await page.waitForTimeout(1500);

      // expand "see more" buttons so descriptions become visíveis
      const moreButtons = await page.$$(
        'button.feed-shared-inline-show-more-text__see-more-less-toggle, button[aria-label*="see more"], button[aria-label*="ver mais"], button[aria-label*="…mais"]'
      );
      console.log('see-more buttons found:', moreButtons.length);
      for (const b of moreButtons) {
        try { await b.click({ timeout: 2000 }); } catch (e) { /* ignorar */ }
      }
      await page.waitForTimeout(800);

      // select candidate post nodes (tenta várias classes)
      const posts = await page.$$eval(
        'div.feed-shared-update-v2, div.occludable-update, div.feed-shared-update, div.update-components-actor__container, div.feed-shared-update-v2--wrapped',
        nodes => nodes.map(n => {
          const text = el => el ? (el.innerText || el.textContent || '').trim() : '';
          // actor/title link (profile/title)
          const actorAnchor = n.querySelector('a.update-components-actor__meta-link, a[data-test-app-aware-link], a.update-components-actor__image, a.update-components-actor__meta');
          let title = '', link = '';
          if (actorAnchor) {
            // some pages embed actor title inside child span
            const titleNode = actorAnchor.querySelector('.update-components-actor__title') || actorAnchor;
            title = text(titleNode).replace(/\n+/g,' ').trim();
            link = actorAnchor.getAttribute('href') || '';
          } else {
            // fallback: check for heading inside post
            const h = n.querySelector('h3, h4, a.title, a');
            title = text(h);
            link = h && h.getAttribute ? (h.getAttribute('href')||'') : '';
          }

          // description (post text)
          const descNode = n.querySelector('.update-components-text, .feed-shared-inline-show-more-text__text, .update-components-commentary, .feed-shared-text, .feed-shared-inline-show-more-text');
          const description = descNode ? text(descNode) : '';

          // date
          const dateNode = n.querySelector('time, .update-components-actor__sub-description, span[aria-hidden], .timestamp, .feed-shared-actor__sub-description');
          const date = dateNode ? text(dateNode) : '';

          return {
            title: title || '',
            link: link || '',
            date: date || '',
            description: description || '',
            snippet: (n.innerText || '').slice(0,200)
          };
        })
      );

      console.log('Found posts on page:', posts.length);
      for (const p of posts) results.push(Object.assign({ source: url }, p));

    } catch (err) {
      console.error('Error processing', url, err && err.message);
    }
  }

  await browser.close();

  // grava JSON (debug / canonical)
  const outJson = path.join(outDir, 'linkedin.json');
  fs.writeFileSync(outJson, JSON.stringify(results, null, 2), 'utf8');
  console.log('Wrote', outJson);

  // grava XLSX (Excel) usando exceljs
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
    results.forEach(r => {
      ws.addRow({ title: r.title, link: r.link, date: r.date, description: r.description, source: r.source });
    });
    const outXlsx = path.join(outDir, 'linkedin.xlsx');
    await wb.xlsx.writeFile(outXlsx);
    console.log('Wrote', outXlsx);
  } catch (e) {
    console.error('Failed to write XLSX:', e && e.message);
  }

  console.log('Done. Extracted items:', results.length);
})();
