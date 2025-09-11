// scripts/save_linkedin_storage.js
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

  console.log('Opening LinkedIn...');
  await page.goto('https://www.linkedin.com/login', { waitUntil: 'domcontentloaded' });

  console.log('Por favor, faz login manualmente na janela do browser que abriu.');
  console.log('Depois de estares logado e a ver o feed / grupo, espera uns segundos e pressiona ENTER aqui no terminal.');

  // espera pela tua confirmação manual
  process.stdin.setEncoding('utf8');
  await new Promise(resolve => {
    process.stdin.once('data', () => resolve());
  });

  // salva o estado
  await context.storageState({ path: out });
  console.log('Saved storage state to', out);

  await browser.close();
  process.exit(0);
})();
