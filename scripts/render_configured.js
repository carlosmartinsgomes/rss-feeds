// scripts/render_configured.js
// Uso: node scripts/render_configured.js
// Ler sites.json, sanitizar se necessário, e correr render_page.js apenas para hosts whitelisteds.

const fs = require('fs');
const cp = require('child_process');
const path = require('path');

function log(...a){ console.log(...a); }
function warn(...a){ console.warn(...a); }
function err(...a){ console.error(...a); }

function hostOf(urlString) {
  try {
    const u = new URL(String(urlString));
    return (u.hostname || '').toLowerCase().replace(/^www\./,'');
  } catch (e) {
    const m = String(urlString || '').toLowerCase().match(/:\/\/([^\/:?#]+)/);
    return m ? m[1].replace(/^www\./,'') : '';
  }
}

function hostMatchesAllowed(hostname, allowedHosts) {
  if (!hostname) return false;
  return allowedHosts.some(a => hostname === a || hostname.endsWith('.' + a));
}

// WHITELIST: apenas estes hosts serão processados por este passo
const allowedHosts = [
  'dzone.com',
  'https://www.eetimes.com/category/news-analysis/',
  'https://www.edsurge.com/news',
  'mdpi.com',
  'medscape.com',
  'stocktwits.com',
  'journals.lww.com',
  'inmodeinvestors.com',
  'aibusiness.com',
  'businesswire.com'
];

const cwd = process.cwd();
let scriptsDir = 'scripts';
const candidates = ['scripts', 'rss-feeds/scripts', './scripts', './rss-feeds/scripts'];
for (const c of candidates) {
  try {
    if (fs.existsSync(path.join(c, 'sites.json')) && fs.existsSync(path.join(c, 'render_page.js'))) {
      scriptsDir = c;
      break;
    }
  } catch(e){}
}
scriptsDir = scriptsDir.replace(/\/+$/, '');
const sitesFile = path.join(scriptsDir, 'sites.json');

if (!fs.existsSync(sitesFile)) {
  warn('sites.json not found at', sitesFile, '-> skipping configured renders');
  process.exit(0);
}

let raw = null;
try {
  raw = fs.readFileSync(sitesFile, 'utf8');
} catch (e) {
  err('Failed to read sites.json:', e && e.message ? e.message : e);
  process.exit(0);
}

let sitesObj = null;
try {
  sitesObj = JSON.parse(raw);
} catch (e) {
  warn('Failed to parse sites.json in Node directly:', e && e.message ? e.message : e);
  // basic sanitize: remove // comments and /* */ blocks and trailing commas
  try {
    let s = raw.replace(/\/\/.*(?=\n)/g, '');
    s = s.replace(/\/\*[\s\S]*?\*\//g, '');
    s = s.replace(/,\s*(\}|])/g, '$1');
    sitesObj = JSON.parse(s);
    log('Parsed sites.json after basic sanitize.');
  } catch (e2) {
    warn('Failed to parse sites.json in Node even after sanitize:', e2 && e2.message ? e2.message : e2);
    warn('Snippet (first 400 chars):', raw.slice(0,400).replace(/\r/g,'\\r').replace(/\n/g,'\\n\n'));
    warn('Continuing without configured renders (sites.json parse failed).');
    sitesObj = [];
  }
}

let sites = (sitesObj && sitesObj.sites) ? sitesObj.sites : (Array.isArray(sitesObj) ? sitesObj : []);

const renderScript = path.join(scriptsDir, 'render_page.js');
if (!fs.existsSync(renderScript)) {
  warn(`render_page.js not found at ${renderScript} -> skipping configured renders`);
  process.exit(0);
}

try { fs.mkdirSync(path.join(scriptsDir, 'rendered'), { recursive: true }); } catch(e){}

const explicitRaw = (process.env.EXPLICIT_URLS || '').split(/\r?\n/).map(s => s.trim()).filter(Boolean);

for (const s of sites) {
  try {
    if (!s || !s.render_file) continue;
    const urlRaw = (s.url || '').trim();
    if (!urlRaw) continue;
    const host = hostOf(urlRaw);
    if (!hostMatchesAllowed(host, allowedHosts)) {
      log('Skipping (not in configured whitelist):', urlRaw);
      continue;
    }
    if (explicitRaw.includes(urlRaw)) {
      log('Skipping configured render for (explicit handled):', urlRaw);
      continue;
    }
    const out = s.render_file;
    log('Rendering', urlRaw, '->', out);
    try {
      // chama render_page.js com node (mesmo que render_page invoque playwright)
      cp.execFileSync(process.execPath, [renderScript, urlRaw, out], { stdio: 'inherit', timeout: 180000 });
      log('-> Done:', urlRaw);
    } catch (ex) {
      warn('Render failed for', urlRaw, '-', ex && ex.message ? ex.message : ex);
    }
  } catch (ex2) {
    warn('Error processing site entry:', ex2 && ex2.message ? ex2.message : ex2);
  }
}

log('render_configured.js finished.');
process.exit(0);
