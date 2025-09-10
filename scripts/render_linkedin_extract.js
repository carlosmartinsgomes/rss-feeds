// Requires playwright installed
const { chromium } = require('playwright');

(async () => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext({
    // Se precisares de sessão autenticada, passa cookies ou usa persistent context
    // userDataDir: '/path/to/profile'
  });
  const page = await context.newPage();
  await page.goto('https://www.linkedin.com/groups/5146549/', { waitUntil: 'networkidle' });

  // Clica em todos os "see more" visíveis
  const seeMore = await page.$$('button.feed-shared-inline-show-more-text__see-more-less-toggle');
  for (const b of seeMore) {
    try { await b.click(); await page.waitForTimeout(200); } catch(e){ }
  }

  const posts = await page.$$('div.feed-shared-update-v2, div.occludable-update[role="article"]');
  const out = [];
  for (const p of posts) {
    const author = await p.$eval('.update-components-actor__title span', el => el.textContent).catch(()=> '');
    const authorLink = await p.$eval('a.update-components-actor__meta-link', el => el.href).catch(()=> '');
    const date = await p.$eval('.update-components-actor__sub-description', el => el.textContent).catch(()=> '');
    const description = await p.$eval('div.update-components-text.update-components-update-v2__commentary', el => el.textContent).catch(()=> '');
    out.push({ author: (author||'').trim(), authorLink, date: (date||'').trim(), description: (description||'').trim().slice(0,1000) });
  }
  console.log(JSON.stringify(out, null, 2));
  await browser.close();
})();
