// rss-feeds/scripts/render_page.js
// Uso: node rss-feeds/scripts/render_page.js <url> <output_path>
// Ex.: node rss-feeds/scripts/render_page.js https://www.darkreading.com/ rss-feeds/scripts/rendered/darkreading.html

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

(async () => {
  try {
    const args = process.argv.slice(2);
    if (args.length < 2) {
      console.error('Usage: node render_page.js <url> <output_path>');
      process.exit(2);
    }
    const [url, outPath] = args;

    // cria diretoria se necessário
    const outDir = path.dirname(outPath);
    fs.mkdirSync(outDir, { recursive: true });

    // inicia browser
    const browser = await chromium.launch({
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
      headless: true
    });

    const context = await browser.newContext({
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36',
      locale: 'en-US'
    });

    const page = await context.newPage();
    // timeout mais longo caso a página demore
    await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
    // small wait para javascript dinâmico (opcional)
    await page.waitForTimeout(1200);

    // pega o HTML renderizado
    const content = await page.content();
    fs.writeFileSync(outPath, content, { encoding: 'utf-8' });
    console.log(`Rendered ${url} -> ${outPath}`);

    await browser.close();
    process.exit(0);

  } catch (err) {
    console.error('Render error:', err);
    process.exit(1);
  }
})();
