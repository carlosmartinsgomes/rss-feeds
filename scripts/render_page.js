// scripts/render_page.js
// Uso: node scripts/render_page.js <url> <output_path>

const fs = require('fs');
const path = require('path');
const { chromium } = require('playwright');

async function render(url, outPath) {
  const outDir = path.dirname(outPath);
  fs.mkdirSync(outDir, { recursive: true });

  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    headless: true
  });

  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36',
    locale: 'en-US'
  });

  const page = await context.newPage();

  // define blocked resource types; adjust for BusinessWire
  let blockedResourceTypes = ['image', 'media'];
  const lower = url.toLowerCase();
  if (lower.includes('businesswire.com')) {
    // BusinessWire often needs CSS/fonts to render the list — allow stylesheet/font
    blockedResourceTypes = ['image', 'media'];
  }

  await page.route('**/*', (route) => {
    const req = route.request();
    const resourceType = req.resourceType();
    const reqUrl = req.url().toLowerCase();

    // small list of tracking/ad domains to block always
    const blockedDomains = [
      'googlesyndication', 'doubleclick', 'google-analytics', 'ads', 'adsystem',
      'akamaihd', 'scorecardresearch', 'adsafeprotected', 'quantserve',
      'facebook.net', 'facebook.com', 'ads-twitter'
    ];
    for (const d of blockedDomains) {
      if (reqUrl.includes(d)) return route.abort();
    }

    if (blockedResourceTypes.includes(resourceType)) {
      return route.abort();
    }
    return route.continue();
  });

  // timeouts
  page.setDefaultNavigationTimeout(90000);

  let lastError = null;
  for (let attempt = 1; attempt <= 2; attempt++) {
    try {
      if (lower.includes('businesswire.com')) {
        console.log(`Attempt ${attempt} navigating to ${url} (networkidle) for BusinessWire...`);
        await page.goto(url, { waitUntil: 'networkidle', timeout: 70000 });
      } else {
        console.log(`Attempt ${attempt} navigating to ${url} (domcontentloaded)...`);
        await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 45000 });
      }

      try {
        await page.waitForSelector('article, main, #content, body', { timeout: 7000 });
      } catch (e) {
        // não fatal
      }

      const content = await page.content();
      fs.writeFileSync(outPath, content, { encoding: 'utf-8' });
      console.log(`Rendered ${url} -> ${outPath}`);
      await browser.close();
      return;
    } catch (err) {
      console.error('Render error:', err.message || err);
      lastError = err;
      await new Promise(r => setTimeout(r, 1500 * attempt));
    }
  }

  await browser.close();
  throw lastError || new Error('Unknown render error');
}

(async () => {
  try {
    const args = process.argv.slice(2);
    if (args.length < 2) {
      console.error('Usage: node scripts/render_page.js <url> <output_path>');
      process.exit(2);
    }
    const [url, outPath] = args;
    await render(url, outPath);
    process.exit(0);
  } catch (err) {
    console.error('Render failed:', err);
    process.exit(1);
  }
})();
