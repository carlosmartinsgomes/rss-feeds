// scripts/render_page.js
// Full replacement with robust fallback handling, decompression, improved heuristics,
// host-specific strategies, and lightweight stealth tweaks.
// (Updated: print configured special hosts; relax block detection; per-host tweaks)

const fs = require('fs');
const path = require('path');
const https = require('https');
const zlib = require('zlib');
const { chromium } = require('playwright');

function delay(ms) { return new Promise(r => setTimeout(r, ms)); }

async function simpleHttpGet(url, outPath, headers = {}, timeout = 35000) {
  return new Promise((resolve, reject) => {
    const opts = new URL(url);
    opts.headers = Object.assign({
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
      'Accept-Encoding': 'gzip, deflate, br',
      'Referer': url,
      'Connection': 'keep-alive'
    }, headers);

    const req = https.get(opts, (res) => {
      const status = res.statusCode || 0;
      if (status >= 400) {
        res.resume();
        return reject(new Error(`HTTP status ${status}`));
      }

      const encoding = (res.headers['content-encoding'] || '').toLowerCase();
      const chunks = [];

      res.on('data', c => chunks.push(c));
      res.on('end', () => {
        try {
          const buf = Buffer.concat(chunks);

          // descompressão quando aplicável
          if (encoding.includes('br')) {
            zlib.brotliDecompress(buf, (err, out) => {
              if (err) return reject(err);
              fs.writeFileSync(outPath, out.toString('utf8'), { encoding: 'utf-8' });
              resolve({ ok: true, status });
            });
          } else if (encoding.includes('gzip')) {
            zlib.gunzip(buf, (err, out) => {
              if (err) return reject(err);
              fs.writeFileSync(outPath, out.toString('utf8'), { encoding: 'utf-8' });
              resolve({ ok: true, status });
            });
          } else if (encoding.includes('deflate')) {
            zlib.inflate(buf, (err, out) => {
              if (err) return reject(err);
              fs.writeFileSync(outPath, out.toString('utf8'), { encoding: 'utf-8' });
              resolve({ ok: true, status });
            });
          } else {
            fs.writeFileSync(outPath, buf.toString('utf8'), { encoding: 'utf-8' });
            resolve({ ok: true, status });
          }
        } catch (e) {
          reject(e);
        }
      });
    });
    req.on('error', (err) => reject(err));
    req.setTimeout(timeout, () => { req.destroy(new Error('timeout')); });
  });
}

async function launchContext(strat) {
  const headlessMode = process.env.PW_HEADLESS === 'false' ? false : true;

  const browser = await chromium.launch({
    args: ['--no-sandbox', '--disable-setuid-sandbox'],
    headless: headlessMode
  });

  const context = await browser.newContext({
    userAgent: strat.userAgent,
    locale: strat.locale || 'en-US',
    viewport: strat.viewport || { width: 1280, height: 800 },
    extraHTTPHeaders: strat.extraHTTPHeaders || {}
  });

  // small stealth tweaks
  await context.addInitScript(() => {
    Object.defineProperty(navigator, 'webdriver', { get: () => false });
    Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
    Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3,4] });
  });

  return { browser, context };
}

async function tryNavigate(url, strat) {
  let bc = null;
  try {
    const { browser, context } = await launchContext(strat);
    bc = { browser, context };
    const page = await context.newPage();

    // route blocks to speed up and reduce noise
    await page.route('**/*', (route) => {
      const req = route.request();
      const rurl = req.url();
      const type = req.resourceType();
      const blockedDomains = ['googlesyndication','doubleclick','google-analytics','adsystem','adservice','scorecardresearch','facebook.net','facebook.com','ads-twitter','amazon-adsystem'];
      for (const d of blockedDomains) if (rurl.includes(d)) return route.abort();
      if (strat.blockImages && (type === 'image' || type === 'media')) return route.abort();
      if (strat.blockStyles && (type === 'stylesheet' || type === 'font')) return route.abort();
      return route.continue();
    });

    page.setDefaultNavigationTimeout(strat.timeout || 30000);
    console.log(`  -> Navigating with waitUntil="${strat.waitUntil}" (timeout ${strat.timeout})`);
    const resp = await page.goto(url, { waitUntil: strat.waitUntil, timeout: strat.timeout, referer: strat.referer || undefined });
    const status = resp ? resp.status() : null;
    return { ok: true, page, browser, context, status };
  } catch (err) {
    if (bc && bc.browser) {
      try { await bc.browser.close(); } catch(e){}
    }
    return { ok: false, error: err };
  }
}

function looksLikeBlockPage(html) {
  if (!html) return true;
  const lowered = html.toLowerCase();
  const blockers = [
    'access denied','forbidden','blocked','bot detected','captcha',
    'please enable javascript','are you human','request blocked',
    "you don't have permission",'site blocked','you have been blocked'
  ];
  let matches = 0;
  for (const b of blockers) if (lowered.includes(b)) matches++;

  // menos agressivo: requer 3+ indicadores OU 2 indicadores num HTML curto
  if (matches >= 3) return true;
  if (matches >= 2 && html.length < 2000) return true;

  return false;
}

async function render(url, outPath) {
  fs.mkdirSync(path.dirname(outPath), { recursive: true });

  const commonHeaders = { 'accept-language': 'en-US,en;q=0.9' };

  const strategies = {
    A: {
      name: 'A - fast (block images & styles)',
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/114.0 Safari/537.36',
      extraHTTPHeaders: commonHeaders,
      blockImages: true,
      blockStyles: true,
      waitUntil: 'domcontentloaded',
      timeout: 20000
    },
    B: {
      name: 'B - allow styles/fonts (recommended)',
      userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
      extraHTTPHeaders: Object.assign({}, commonHeaders, { 'sec-ch-ua': '"Chromium";v="120", "Google Chrome";v="120"' }),
      blockImages: true,
      blockStyles: false,
      waitUntil: 'networkidle',
      timeout: 50000,
      referer: url
    },
    C: {
      name: 'C - human-like (allow everything, longer)',
      userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/605.1.15',
      extraHTTPHeaders: commonHeaders,
      blockImages: false,
      blockStyles: false,
      waitUntil: 'networkidle',
      timeout: 90000,
      referer: url
    }
  };

   // hosts que precisam de tratamento especial (ordem preferida)
  const hostPrefs = {
    'inmodeinvestors.com': ['B'],
    'darkreading.com': ['B','A','C'],
    'iotworldtoday.com': ['B','A'],
    'businesswire.com': ['B','A'],
    'stocktwits.com': ['A','B','C'],
    'dzone.com': ['B','C','A'],
    'eetimes.com': ['B','C','A'],
    // theinformation removido por pedido
    'medscape.com': ['C','B','A'],
    'mdpi.com': ['C','B','A'],
    'journals.lww.com': ['C','B','A']
  };

  // Log inicial: lista de hosts com tratamento especial (para ver no log mesmo se não houver render desses hosts)
  console.log('Configured special hosts:', Object.keys(hostPrefs).join(', '));

  const hostKey = Object.keys(hostPrefs).find(h => url.includes(h));
  const order = hostKey ? hostPrefs[hostKey] : ['A','B','C'];

  console.log(`Host key matched: ${hostKey || '(none)'}; strategy order: ${order.join(',')}`);

  let lastErr = null;
  for (let idx = 0; idx < order.length; idx++) {
    const key = order[idx];
    const strat = strategies[key];
    console.log(`Strategy attempt ${idx+1}/${order.length}: ${strat.name} for ${url}`);
    const result = await tryNavigate(url, strat);

    if (!result.ok) {
      console.warn(`  Strategy ${strat.name} failed to navigate: ${result.error && result.error.message ? result.error.message : result.error}`);
      lastErr = result.error;
      await delay(500 * (idx+1));
      continue;
    }

    const { page, browser, status } = result;
    console.log(`  Main response status: ${status}`);

    if (status === 403) {
      console.warn(`  Got 403 on ${url} with strategy ${strat.name} — will try next or fallback`);
      try { await browser.close(); } catch(e){}
      lastErr = new Error('403 Forbidden');
      await delay(400);
      continue;
    }

    // JS settle time
    await delay(800 + idx * 400);

    try {
      await page.waitForSelector('article, main, #content, .post, .news, .press, .investors_events_bodybox', { timeout: 1500 }).catch(()=>{});
    } catch(e){}

    try {
      const content = await page.content();

      // detecção menos agressiva de "block page"
      if (looksLikeBlockPage(content)) {
        console.warn(`  Render looks like block page (detected) for ${url} using ${strat.name}`);
        try { await browser.close(); } catch(e){}
        lastErr = new Error('Detected block page after render');
        await delay(300);
        continue;
      }

      fs.writeFileSync(outPath, content, { encoding: 'utf-8' });

      // validação conservadora do tamanho do ficheiro
      try {
        const stats = fs.statSync(outPath);
        if (stats.size < 600) {
          const sample = fs.readFileSync(outPath, 'utf8');
          if (looksLikeBlockPage(sample)) {
            fs.unlinkSync(outPath);
            console.warn(`  Rendered file muito pequeno e parece bloqueado -> removed: ${outPath}`);
            lastErr = new Error('Rendered file small / block page');
            try { await browser.close(); } catch(e){}
            continue;
          }
        }
      } catch(e){}

      console.log(`Rendered ${url} -> ${outPath} (status: ${status}) using strategy: ${strat.name}`);
      try { await browser.close(); } catch(e){}
      return;
    } catch (err) {
      console.error(`  Error writing content after strategy ${strat.name}: ${err && err.message ? err.message : err}`);
      lastErr = err;
      try { await browser.close(); } catch(e){}
      await delay(300);
      continue;
    }
  }

  // fallback HTTP
  console.warn('Playwright attempts exhausted — trying simple HTTPS GET fallback (only accept HTTP < 400)');
  try {
    await simpleHttpGet(url, outPath, {}, 50000);
    const html = fs.readFileSync(outPath, 'utf8');
    if (looksLikeBlockPage(html)) {
      fs.unlinkSync(outPath);
      throw new Error('Fallback HTTP returned block page');
    }
    const stats = fs.statSync(outPath);
    if (stats.size < 600) {
      fs.unlinkSync(outPath);
      throw new Error('Fallback HTTP returned very small page');
    }
    console.log(`Fallback HTTPS GET succeeded: ${url} -> ${outPath}`);
    return;
  } catch (e) {
    console.warn('Fallback HTTPS GET failed or returned block page:', e && e.message ? e.message : e);
    lastErr = e;
  }

  throw lastErr || new Error('All strategies failed');
}

(async () => {
  try {
    const args = process.argv.slice(2);
    if (args.length < 2) {
      console.error('Usage: node scripts/render_page.js <url> <output_path>');
      process.exit(2);
    }
    const [url, outPath] = args;
    console.log(`Starting render for: ${url}`);
    await render(url, outPath);
    process.exit(0);
  } catch (err) {
    console.error('Render failed:', err && (err.stack || err.message) ? (err.stack || err.message) : err);
    process.exit(1);
  }
})();
