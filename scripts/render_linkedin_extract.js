#!/usr/bin/env node
// scripts/render_linkedin_extract.js
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');
const Excel = require('exceljs');

(async () => {
  console.log('START render_linkedin_extract.js - cwd=', process.cwd());
  console.log('argv=', process.argv);

  // usa __dirname para garantir paths correctos independentemente do working-dir
  const scriptDir = __dirname;
  const outDir = path.join(scriptDir, 'output');
  fs.mkdirSync(outDir, { recursive: true });

  const storagePath = path.join(scriptDir, 'linkedin_auth.json'); // se existir, usaremos para login
  const urls = process.argv.slice(2);
  if (urls.length === 0) {
    console.log('USO: node render_linkedin_extract.js <url1> <url2> ...');
    console.log('Exemplo de URL: https://www.linkedin.com/groups/5146549/');
    process.exit(1);
  }

  // HEADLESS control: se quiseres ver o browser localmente define HEADLESS=false
  const headless = process.env.HEADLESS !== 'false';
  const browser = await chromium.launch({ headless, args: ['--no-sandbox','--disable-setuid-sandbox'] });

  try {
    // se houver storageState usa logo, senão cria contexto limpo e talvez login automático
    let context;
    if (fs.existsSync(storagePath)) {
      console.log('Using existing storageState:', storagePath);
      context = await browser.newContext({ storageState: storagePath });
    } else {
      console.log('No storageState found at', storagePath);
      context = await browser.newContext();
    }

    const page = await context.newPage();

    // se não existir storageState e existirem credenciais como env, tentar login e gravar storage
    const email = process.env.LINKEDIN_EMAIL || '';
    const password = process.env.LINKEDIN_PASSWORD || '';
    if (!fs.existsSync(storagePath) && email && password) {
      console.log('No storageState -> attempting login using LINKEDIN_EMAIL / LINKEDIN_PASSWORD env vars');
      try {
        await page.goto('https://www.linkedin.com/login', { waitUntil: 'domcontentloaded', timeout: 30000 });
        await page.fill('input#username, input[name="session_key"]', email).catch(()=>{});
        await page.fill('input#password, input[name="session_password"]', password).catch(()=>{});
        await Promise.all([
          page.waitForNavigation({ waitUntil: 'domcontentloaded', timeout: 20000 }).catch(()=>{}),
          page.click('button[type="submit"], button.sign-in-form__submit-button').catch(()=>{})
        ]);
        // pequena espera
        await page.waitForTimeout(3000);
        // confirma se login parece bem (procura um elemento visível do feed)
        const loggedIn = await page.url().includes('/feed') || await page.$('div.feed-shared-update-v2, div.occludable-update') ? true : false;
        if (loggedIn) {
          await context.storageState({ path: storagePath });
          console.log('Login parece bem — storageState gravado em', storagePath);
        } else {
          console.warn('Login automátio tentou mas não detectou feed; continue manual ou execute localmente com HEADLESS=false');
        }
      } catch (e) {
        console.warn('Login automatico falhou:', e && e.message);
      }
    } else if (!fs.existsSync(storagePath) && !email && !password && !headless) {
      // caso local com headful browser — dá oportunidade de login manual e grava storage
      console.log('Executando em modo visível sem storageState e sem credenciais — faz o login manualmente no browser (tens 60s) para gravarmos storageState.');
      await page.goto('https://www.linkedin.com/login', { waitUntil: 'domcontentloaded', timeout: 30000 }).catch(()=>{});
      // espera 60s para login manual
      await page.waitForTimeout(60000);
      try {
        await context.storageState({ path: storagePath });
        console.log('Tentativa de salvar storageState após login manual ->', storagePath);
      } catch(e){ console.warn('Não foi possível salvar storageState:', e && e.message); }
    }

    const results = [];

    // função utilitária para filtrar hrefs de navegação/menus/cookies
    const badHrefSubstr = ['help', 'legal', 'cookie', 'privacy', 'terms', 'signin', 'login', 'settings', 'consent', 'preferences', 'policies', 'mailto'];

    for (const url of urls) {
      console.log('>>> Loading', url);
      try {
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
        // small wait for dynamic content
        await page.waitForTimeout(1500);

        // expand "see more" buttons so descriptions become visíveis
        const moreButtons = await page.$$(
          'button.feed-shared-inline-show-more-text__see-more-less-toggle, button[aria-label*="see more"], button[aria-label*="ver mais"], button[aria-label*="…mais"], button[aria-label*="See more"]'
        );
        console.log('see-more buttons found:', moreButtons.length);
        for (const b of moreButtons) {
          try { await b.click({ timeout: 2000 }); } catch (e) { /* ignorar */ }
        }
        await page.waitForTimeout(800);

        // garante que existe algum post carregado (timeout curto)
        try {
          await page.waitForSelector('div.feed-shared-update-v2, div.occludable-update, div.feed-shared-update, div.update-components-actor__container', { timeout: 7000 });
        } catch (e) {
          console.log('Aviso: não detectei seletor de posts rapidamente — pode ser conteúdo protegido/necessária sessão.');
        }

        // colecta posts
        const posts = await page.$$eval(
          'div.feed-shared-update-v2, div.occludable-update, div.feed-shared-update, div.update-components-actor__container',
          (nodes, badSubstr) => {
            const out = [];
            const text = el => el ? (el.innerText || el.textContent || '').trim() : '';
            for (const n of nodes) {
              // actor/title link (profile/title)
              const actorAnchor = n.querySelector('a.update-components-actor__meta-link, a[data-test-app-aware-link], a.update-components-actor__image, a.update-components-actor__meta');
              let title = '', link = '';
              if (actorAnchor) {
                const titleNode = actorAnchor.querySelector('.update-components-actor__title') || actorAnchor;
                title = text(titleNode).replace(/\n+/g,' ').trim();
                link = actorAnchor.getAttribute('href') || '';
              } else {
                // find the main anchor inside the post, preferring anchors that look like real article links
                const anchors = Array.from(n.querySelectorAll('a'));
                let chosen = null;
                for (const a of anchors) {
                  const h = a.getAttribute && a.getAttribute('href') || '';
                  if (!h) continue;
                  const low = h.toLowerCase();
                  // skip bad substrings (navigation, policy, help)
                  if (badSubstr.some(bs => low.includes(bs))) continue;
                  // prefer anchors that look like content (contain '/in/' or '/groups/' or start with http)
                  if (low.startsWith('http') || low.includes('/in/') || low.includes('/groups/') || low.includes('/posts/') || low.includes('/feed/')) {
                    chosen = a;
                    break;
                  }
                  // fallback to first non-bad anchor
                  if (!chosen) chosen = a;
                }
                if (chosen) {
                  title = text(chosen).replace(/\n+/g,' ').trim();
                  link = chosen.getAttribute('href') || '';
                } else {
                  // fallback: heading or text inside post
                  const h = n.querySelector('h3, h4, a.title, a');
                  title = text(h);
                  link = h && h.getAttribute ? (h.getAttribute('href')||'') : '';
                }
              }

              // description (post text)
              const descNode = n.querySelector('.update-components-text, .feed-shared-inline-show-more-text__text, .update-components-commentary, .feed-shared-text, .feed-shared-inline-show-more-text, .commentary, p');
              const description = descNode ? text(descNode).replace(/\n+/g,' ').trim() : '';

              // date (try several)
              const dateNode = n.querySelector('time, .update-components-actor__sub-description, span[aria-hidden], .timestamp, .feed-shared-actor__sub-description, .feed-shared-actor__sub-description');
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
        // normaliza links: transforma relativos em absolutos onde possível (browser vai fornecer relativos)
        for (const p of posts) {
          // small cleanup on link (remove query tracking anchors if needed)
          let href = p.link || '';
          // se href começar com '/', prefixa o domain da página actual
          if (href && href.startsWith('/')) {
            try {
              const base = new URL(url);
              href = base.origin + href;
            } catch (e) { /* ignore */ }
          }
          // ignora links de navegação/internos (ex.: "/help", "/legal")
          if (!href || badHrefSubstr.some(bs => href.toLowerCase().includes(bs))) {
            // tentar extrair link alternativo do snippet (regex)
            // fallback: deixa em branco
            p.link = '';
          } else {
            p.link = href;
          }
          results.push(Object.assign({ source: url }, p));
        }

      } catch (err) {
        console.error('Error processing', url, err && err.message);
      }
    }

    // close context & browser
    try { await context.close(); } catch(e) {}
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
  } catch (outerErr) {
    console.error('Fatal error in script:', outerErr && outerErr.message);
    try { await browser.close(); } catch(e){}
    process.exit(1);
  }
})();
