/*
 scripts/save_linkedin_storage.js
 Abrir Chromium (visível), fazes login manual no LinkedIn, pressiona ENTER no terminal e guarda storageState em scripts/linkedin_auth.json
*/
const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

(async () => {
  const out = path.join('scripts', 'linkedin_auth.json');
  console.log('Will save storageState to:', out);

  // HEADLESS=false para poderes fazer login manualmente
  const browser = await chromium.launch({ headless: false });
  const context = await browser.newContext();
  const page = await context.newPage();

  console.log('Opening LinkedIn login page...');
  await page.goto('https://www.linkedin.com/login', { waitUntil: 'domcontentloaded' });

  console.log('');
  console.log('>>> Faz login manualmente na janela do browser que abriu. Depois volta a este terminal e pressiona ENTER para continuar e guardar a sessão.');
  console.log('');

  // espera pela tua confirmação manual
  process.stdin.setEncoding('utf8');
  await new Promise(resolve => {
    process.stdin.once('data', () => resolve());
  });

  // salva o estado (cookies + localStorage)
  await context.storageState({ path: out });
  console.log('Saved storage state to', out);

  await browser.close();
  process.exit(0);
})();
