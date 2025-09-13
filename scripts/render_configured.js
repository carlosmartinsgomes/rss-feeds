// scripts/render_configured.js
// Lê sites.json, filtra pela whitelist e executa render_page.js para cada site.render_file
// Non-fatal: captura erros e continua.

const fs = require('fs');
const cp = require('child_process');
const path = require('path');

async function main(){
  try {
    const SCRIPTS = process.env.SCRIPTS || 'scripts';
    const explicitRaw = process.env.EXPLICIT_URLS || '';
    const explicit = explicitRaw.split(/\r?\n/).map(s => s.trim()).filter(Boolean);

    const allowedHosts = [
      'dzone.com',
      'eetimes.com',
      'mdpi.com',
      'medscape.com',
      'stocktwits.com',
      'journals.lww.com'
    ];

    function hostOf(urlString) {
      try { return (new URL(String(urlString))).hostname.replace(/^www\./,'').toLowerCase(); }
      catch(e){
        const m = String(urlString || '').toLowerCase().match(/:\/\/([^\/:?#]+)/);
        return m ? m[1].replace(/^www\./,'') : '';
      }
    }
    function hostMatchesAllowed(hostname){
      if(!hostname) return false;
      return allowedHosts.some(a => hostname === a || hostname.endsWith('.' + a));
    }

    let scriptsDir = SCRIPTS;
    // basic candidates fallback
    const candidates = ['scripts', 'rss-feeds/scripts', './scripts', './rss-feeds/scripts'];
    if (!scriptsDir) {
      for (const c of candidates) {
        if (fs.existsSync(path.join(c,'sites.json')) && fs.existsSync(path.join(c,'render_page.js'))) {
          scriptsDir = c;
          break;
        }
      }
    }
    scriptsDir = String(scriptsDir || 'scripts').replace(/\/+$/,'');

    const sitesFile = path.join(scriptsDir, 'sites.json');
    if (!fs.existsSync(sitesFile)) {
      console.warn('sites.json not found at', sitesFile, '-> skipping configured renders');
      return;
    }

    // read & parse with basic sanitize
    let raw = fs.readFileSync(sitesFile, 'utf8');
    let sitesObj = null;
    try {
      sitesObj = JSON.parse(raw);
    } catch(e) {
      // sanitize and retry
      try {
        let s = raw.replace(/\/\/.*(?=\n)/g,'');
        s = s.replace(/\/\*[\s\S]*?\*\//g,'');
        s = s.replace(/,\s*(\}|])/g,'$1');
        sitesObj = JSON.parse(s);
        console.log('Parsed sites.json after basic sanitize (node).');
      } catch (e2) {
        console.error('Failed to parse sites.json in Node even after sanitize:', e2 && e2.message ? e2.message : e2);
        console.warn('Snippet (first 400 chars):', (raw||'').slice(0,400).replace(/\r/g,'\\r').replace(/\n/g,'\\n\n'));
        console.warn('Continuing without configured renders (sites.json parse failed).');
        sitesObj = { sites: [] };
      }
    }

    let sites = (sitesObj && sitesObj.sites) ? sitesObj.sites : (Array.isArray(sitesObj) ? sitesObj : []);
    const renderScript = path.join(scriptsDir, 'render_page.js');
    if (!fs.existsSync(renderScript)) {
      console.warn(`render_page.js not found at ${renderScript} -> skipping configured renders`);
      return;
    }

    try { fs.mkdirSync(path.join(scriptsDir,'rendered'), { recursive:true }); } catch(e){}

    for (const s of sites.filter(x => x && x.render_file)) {
      try {
        const urlRaw = (s.url || '').trim();
        if (!urlRaw) continue;
        const host = hostOf(urlRaw);

        // only whitelist hosts
        if (!hostMatchesAllowed(host)) {
          console.log('Skipping (not in configured whitelist):', urlRaw);
          continue;
        }
        // skip explicit urls handled earlier
        if (explicit.includes(urlRaw)) {
          console.log('Skipping configured render for (explicit handled):', urlRaw);
          continue;
        }

        const out = s.render_file;
        console.log('Rendering', urlRaw, '->', out);
        const cmd = `node "${renderScript}" "${String(urlRaw).replace(/"/g,'\\"')}" "${String(out).replace(/"/g,'\\"')}"`;
        try {
          cp.execSync(cmd, { stdio: 'inherit', timeout: 180000 });
          console.log('-> Done:', urlRaw);
        } catch (err) {
          console.error('Render failed for', urlRaw, '-', (err && err.message) ? err.message : err);
          // continue
        }
      } catch(err) {
        console.error('Unexpected error processing site entry:', err && err.message ? err.message : err);
      }
    }

  } catch(e){
    console.error('Fatal error in render_configured.js:', e && e.message ? e.message : e);
    // do NOT throw – must exit 0 to let workflow continue
  }
}

main().then(()=>{ console.log('render_configured.js finished'); process.exit(0); });
