#!/usr/bin/env node
// scripts/render_page.js (versão melhorada + stealth + estratégias A..E)
const fs = require('fs');
const { chromium } = require('playwright');
const http = require('http');
const https = require('https');
const urlmod = require('url');

if (process.argv.length < 4) {
  console.error('Usage: node scripts/render_page.js "<url>" "<out_file>"');
  process.exit(2);
}
const TARGET = process.argv[2];
const OUT = process.argv[3];

function shortHost(u){
  try { return new URL(u).hostname.replace(/^www\./,'').toLowerCase(); } catch(e){ return String(u).toLowerCase(); }
}

const hostPrefs = {
  'businesswire.com': ['B','A','C','D','E'],
  'inmodeinvestors.com': ['B'],
  'iotworldtoday.com': ['A','B'],
  'darkreading.com': ['A','B','C'],
  'dzone.com': ['B','C','A'],
  'eetimes.com': ['C','B','A','D','E'],
  'mdpi.com': ['C','B','A'],
  'medscape.com': ['C','B','A'],
  'stocktwits.com': ['A','B','C'],
  'journals.lww.com': ['C','B','A']
};

const acceptOn200Hosts = new Set(Object.keys(hostPrefs));

function looksLikeBlockPage(html) {
  if (!html) return true;
  const sample = html.slice(0, 4000).toLowerCase();
  if (sample.includes('access denied') || sample.includes('blocked') ||
      sample.includes('verify you are a human') || sample.includes('captcha') ||
      sample.includes('cloudflare') || sample.includes('forbidden') ||
      sample.includes('request blocked') || sample.includes('bot challenge')) {
    return true;
  }
  return false;
}

async function simpleGet(url, timeoutMs = 15000) {
  return new Promise((resolve, reject) => {
    const parsed = urlmod.parse(url);
    const lib = parsed.protocol === 'http:' ? http : https;
    const options = { timeout: timeoutMs, headers: {
      'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
      'accept-language': 'en-US,en;q=0.9'
    } };
    const req = lib.get(url, options, res => {
      const status = res.statusCode;
      let data = '';
      res.setEncoding('utf8');
      res.on('data', chunk => { if (data.length < 20000) data += chunk; });
      res.on('end', () => resolve({ status, body: data }));
    });
    req.on('timeout', () => { req.destroy(); reject(new Error('timeout')); });
    req.on('error', err => reject(err));
  });
}

async function renderWithPlaywright(url, options) {
  const launchArgs = [
    '--no-sandbox',
    '--disable-setuid-sandbox',
    '--disable-dev-shm-usage',
    '--disable-features=site-per-process,IsolateOrigins,StrictOriginPolicy',
    '--disable-background-networking',
    '--disable-default-apps',
    '--disable-popup-blocking',
    '--disable-extensions',
    '--no-first-run',
    '--no-zygote',
    '--single-process'
  ];

  const browser = await chromium.launch({ args: launchArgs, headless: true });
  try {
    const UA_DESKTOP = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36';
    const userAgent = options.userAgent || UA_DESKTOP;
    const viewport = options.mobileViewport || { width: 1280, height: 800 };

    const context = await browser.newContext({
      userAgent,
      locale: 'en-US',
      viewport,
      bypassCSP: true,
      javaScriptEnabled: true,
      extraHTTPHeaders: {
        'accept-language': 'en-US,en;q=0.9',
        'upgrade-insecure-requests': '1'
      }
    });

    // aumentar timeouts globais (alguns sites demoram)
    try { context.setDefaultNavigationTimeout(options.timeout || 60000); } catch(_) {}
    try { context.setDefaultTimeout(options.timeout || 60000); } catch(_) {}

    const page = await context.newPage();

    // stealth tweaks (aplica quando solicitado ou sempre - mantemos opcional)
    if (options.stealth) {
      try {
        await page.addInitScript(() => {
          // navigator.webdriver false
          Object.defineProperty(navigator, 'webdriver', { get: () => false });

          // mimic plugins/languages
          Object.defineProperty(navigator, 'languages', { get: () => ['en-US','en'] });
          Object.defineProperty(navigator, 'plugins', { get: () => [1,2,3] });

          // minimal chrome object
          window.chrome = { runtime: {} };

          // navigator.permissions hack to avoid some JS checks
          const origQuery = navigator.permissions && navigator.permissions.query;
          if (origQuery) {
            const original = origQuery.bind(navigator.permissions);
            navigator.permissions.query = (parameters) =>
              parameters && parameters.name === 'notifications'
                ? Promise.resolve({ state: Notification.permission })
                : original(parameters);
          }
        });
      } catch(e){ /* ignore stealth init errors */ }
    }

    // routing: bloqueia trackers sempre e imagens/fonts quando blockResources = true
    await page.route('**/*', (route) => {
      try {
        const req = route.request();
        const urlr = req.url();
        const t = req.resourceType();

        // trackers / ad hosts (sempre bloquear para reduzir execução de JS de deteção)
        if (/doubleclick\.net|analytics|adsrvr|clickagy|clarity\.ms|googlesyndication|adsystem|adservice|scorecardresearch|quantserve|adroll/.test(urlr)) {
          return route.abort();
        }

        // se pedirem bloqueio de recursos pesados, aborta imagens/fonts/media/styles para acelerar
        if (options.blockResources) {
          if (t === 'image' || t === 'stylesheet' || t === 'font' || t === 'media') {
            return route.abort();
          }
        }

        return route.continue();
      } catch(e) {
        try { return route.continue(); } catch(_) {}
      }
    });

    // configura timeout por navegação no page (reforça)
    page.setDefaultNavigationTimeout(options.timeout || 60000);

    const gotoOpts = { timeout: options.timeout || 60000, waitUntil: options.waitUntil || 'networkidle' };
    const response = await page.goto(url, gotoOpts);
    const status = response ? response.status() : null;
    const html = await page.content();

    try { await context.close(); } catch(_) {}
    return { status, html };
  } finally {
    try { await browser.close(); } catch(_) {}
  }
}

async function main(){
  const host = shortHost(TARGET);
  console.log('Starting render for:', TARGET);
  console.log('Host key matched:', host, '; hostPrefs:', hostPrefs[host] ? hostPrefs[host].join(',') : '(none)');

  // Quick GET for priority hosts (but com UA real)
  if (acceptOn200Hosts.has(host)) {
    try {
      const res = await simpleGet(TARGET, 12000);
      console.log('Quick GET status:', res.status);
      if (res.status && res.status < 400) {
        console.log('Quick GET succeeded for priority host -> writing output and exiting');
        fs.writeFileSync(OUT, res.body, 'utf8');
        console.log(`Rendered ${TARGET} -> ${OUT} (quick GET, status ${res.status})`);
        return 0;
      }
      console.log('Quick GET did not produce acceptable HTML; falling back to Playwright');
    } catch (e) {
      console.log('Quick GET failed (will fallback to Playwright):', e.message || e);
    }
  }

  const strategyOrder = hostPrefs[host] || ['A','B','C'];
  const strategyMap = {
    'A': { blockResources: true, waitUntil: 'domcontentloaded', timeout: 20000, userAgent: undefined },
    'B': { blockResources: false, waitUntil: 'networkidle', timeout: 45000, userAgent: undefined },
    'C': { blockResources: false, waitUntil: 'networkidle', timeout: 90000, userAgent: undefined },
    // D = stealth / anti-headless tweaks
    'D': { blockResources: false, waitUntil: 'networkidle', timeout: 60000, userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36',
      stealth: true },

    // E = mobile emulation
    'E': { blockResources: false, waitUntil: 'networkidle', timeout: 45000, userAgent:
      'Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Mobile Safari/537.36',
      mobileViewport: { width: 390, height: 844 } }
  };

  for (let i=0;i<strategyOrder.length;i++){
    const s = strategyOrder[i];
    const opts = strategyMap[s];
    if (!opts) {
      console.log(`Unknown strategy ${s} – skipping`);
      continue;
    }
    console.log(`Strategy attempt ${i+1}/${strategyOrder.length}: ${s} for ${TARGET}`);
    try {
      const { status, html } = await renderWithPlaywright(TARGET, opts);
      console.log('  Main response status:', status);
      // Hosts favoritos: aceita logo qualquer 2xx/3xx (<400)
      if (acceptOn200Hosts.has(host) && status && status < 400) {
        fs.writeFileSync(OUT, html, 'utf8');
        console.log(`Rendered ${TARGET} -> ${OUT} (status: ${status}) using strategy: ${s} (priority accept)`);
        return 0;
      }
      // hosts não prioritários: require status < 400 e não parecer block page
      if (!acceptOn200Hosts.has(host)) {
        if (status && status < 400 && !looksLikeBlockPage(html)) {
          fs.writeFileSync(OUT, html, 'utf8');
          console.log(`Rendered ${TARGET} -> ${OUT} (status: ${status}) using strategy: ${s}`);
          return 0;
        } else {
          console.log(`  Render looks like block page (detected) for ${TARGET} using ${s}`);
        }
      } else {
        console.log('  Priority host but not acceptable status -> continue');
      }
    } catch (err) {
      console.log(`  Strategy ${s} failed:`, err && err.message ? err.message : err);
    }
  }

  console.log('Playwright attempts exhausted — trying simple HTTPS GET fallback (only accept HTTP < 400)');
  try {
    const res = await simpleGet(TARGET, 20000);
    console.log('Fallback GET status:', res.status);
    if (res.status && res.status < 400 && (!looksLikeBlockPage(res.body) || acceptOn200Hosts.has(host))) {
      fs.writeFileSync(OUT, res.body, 'utf8');
      console.log(`Rendered ${TARGET} -> ${OUT} (fallback HTTP GET, status ${res.status})`);
      return 0;
    } else {
      console.log('Fallback HTTPS GET failed or returned block page:', res.status);
      throw new Error('Fallback HTTP returned block page or bad status');
    }
  } catch (e) {
    console.error('Render failed:', e && e.message ? e.message : e);
    throw e;
  }
}

main().then(() => process.exit(0)).catch(err => {
  console.error('render failed for', TARGET);
  console.error(err && err.stack ? err.stack : err);
  process.exit(3);
});
