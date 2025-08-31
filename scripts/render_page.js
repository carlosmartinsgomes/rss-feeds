#!/usr/bin/env node
// scripts/render_page.js
// Usage: node scripts/render_page.js "<url>" "<out_file>"

const fs = require('fs');
const { chromium } = require('playwright'); // assume installed in scripts/ node_modules or repo root
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

// Hosts we want to prioritize: accept on first successful HTTP < 400 and disable block-detection for them.
// You can tweak per-host strategy order here: each entry is an array with order e.g. ['B','A','C']
const hostPrefs = {
  'businesswire.com': ['B','A','C'],
  'inmodeinvestors.com': ['B'],
  'iotworldtoday.com': ['A','B'],
  'darkreading.com': ['A','B','C'],
  'dzone.com': ['B','C','A'],
  'eetimes.com': ['B','C','A'],
  'mdpi.com': ['C','B','A'],
  'medscape.com': ['C','B','A'],
  'stocktwits.com': ['A','B','C'],
  'journals.lww.com': ['C','B','A']
};

// hosts for which we accept any HTTP < 400 immediately and skip block detection
const acceptOn200Hosts = new Set(Object.keys(hostPrefs));

// block page detection function (returns true if page looks like a block page)
function looksLikeBlockPage(html) {
  if (!html) return true;
  const lower = html.slice(0, 2000).toLowerCase();
  // common block indicators
  if (lower.includes('access denied') || lower.includes('blocked') || lower.includes('bot') ||
      lower.includes('verify you are a human') || lower.includes('captcha') || lower.includes('cloudflare')) {
    return true;
  }
  return false;
}

async function simpleGet(url, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const parsed = urlmod.parse(url);
    const lib = parsed.protocol === 'http:' ? http : https;
    const req = lib.get(url, { timeout: timeoutMs, headers: { 'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36' } }, res => {
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
  const browser = await chromium.launch({ args: ['--no-sandbox','--disable-setuid-sandbox'] });
  const context = await browser.newContext({ userAgent: options.userAgent || undefined });
  const page = await context.newPage();

  if (options.blockResources) {
    await page.route('**/*', (route) => {
      const r = route.request();
      const t = r.resourceType();
      if (t === 'stylesheet' || t === 'image' || t === 'font' || t === 'media') {
        route.abort();
      } else {
        route.continue();
      }
    });
  }

  try {
    const gotoOpts = { timeout: options.timeout || 50000, waitUntil: options.waitUntil || 'networkidle' };
    const response = await page.goto(url, gotoOpts);
    const status = response ? response.status() : null;
    const html = await page.content();
    await browser.close();
    return { status, html };
  } catch (e) {
    try { await browser.close(); } catch(_) {}
    throw e;
  }
}

async function main(){
  const host = shortHost(TARGET);
  console.log('Starting render for:', TARGET);
  console.log('Host key matched:', host, '; hostPrefs:', hostPrefs[host] ? hostPrefs[host].join(',') : '(none)');

  // If host in acceptOn200Hosts we try a quick HTTP GET first (faster) and accept if status < 400.
  if (acceptOn200Hosts.has(host)) {
    try {
      const res = await simpleGet(TARGET, 10000);
      console.log('Quick GET status:', res.status);
      if (res.status && res.status < 400) {
        console.log('Quick GET succeeded for priority host -> writing output and exiting');
        fs.writeFileSync(OUT, res.body, 'utf8');
        console.log(`Rendered ${TARGET} -> ${OUT} (quick GET, status ${res.status})`);
        return 0;
      }
      console.log('Quick GET did not produce acceptable HTML; falling back to Playwright');
    } catch (e) {
      console.log('Quick GET failed (will fallback to Playwright):', e.message);
    }
  }

  // Determine preferred strategy order
  const strategyOrder = hostPrefs[host] || ['A','B','C'];

  // Strategy definitions:
  // A - fast: block styles/fonts/images, waitUntil=domcontentloaded (short timeout)
  // B - allow styles/fonts, waitUntil=networkidle (recommended)
  // C - human-like: allow everything, longer timeout
  const strategyMap = {
    'A': { blockResources: true, waitUntil: 'domcontentloaded', timeout: 20000 },
    'B': { blockResources: false, waitUntil: 'networkidle', timeout: 50000 },
    'C': { blockResources: false, waitUntil: 'networkidle', timeout: 90000 }
  };

  // Try each strategy in order
  for (let i=0;i<strategyOrder.length;i++){
    const s = strategyOrder[i];
    const opts = strategyMap[s];
    console.log(`Strategy attempt ${i+1}/${strategyOrder.length}: ${s} - ${s === 'A' ? 'fast (block images & styles)' : s === 'B' ? 'allow styles/fonts (recommended)' : 'human-like (allow everything, longer)'} for ${TARGET}`);
    try {
      const { status, html } = await renderWithPlaywright(TARGET, opts);
      console.log('  -> Navigating with waitUntil="' + opts.waitUntil + `" (timeout ${opts.timeout})`);
      console.log('  Main response status:', status);
      // if host is a priority host and we got status < 400, accept immediately (skip block detection)
      if (acceptOn200Hosts.has(host) && status && status < 400) {
        fs.writeFileSync(OUT, html, 'utf8');
        console.log(`Rendered ${TARGET} -> ${OUT} (status: ${status}) using strategy: ${s} (priority accept)`);
        return 0;
      }
      // for non-priority hosts we detect block pages
      if (!acceptOn200Hosts.has(host)) {
        if (status && status < 400 && !looksLikeBlockPage(html)) {
          fs.writeFileSync(OUT, html, 'utf8');
          console.log(`Rendered ${TARGET} -> ${OUT} (status: ${status}) using strategy: ${s}`);
          return 0;
        } else {
          console.log(`  Render looks like block page (detected) for ${TARGET} using ${s}`);
          // continue to next strategy
        }
      } else {
        // priority host but status >=400 -> continue trying
        console.log('  Priority host but not acceptable status -> continue');
      }
    } catch (err) {
      console.log(`  Strategy ${s} failed:`, err && err.message ? err.message : err);
    }
  }

  // Playwright attempts exhausted — try simple HTTPS GET fallback (only accept HTTP < 400)
  console.log('Playwright attempts exhausted — trying simple HTTPS GET fallback (only accept HTTP < 400)');
  try {
    const res = await simpleGet(TARGET, 20000);
    console.log('Fallback GET status:', res.status);
    if (res.status && res.status < 400 && (!looksLikeBlockPage(res.body) || acceptOn200Hosts.has(host))) {
      // write file
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
