// scripts/test_special_hosts.js
// Executa render_page.js contra os hosts listados em hostPrefs (ou sites.json)
// e grava logs separados. Uso: node scripts/test_special_hosts.js

const { spawn } = require('child_process');
const fs = require('fs');
const path = require('path');

const hostPrefs = [
  'stocktwits.com',
  'dzone.com',
  'eetimes.com',
  'medscape.com',
  'mdpi.com',
  'journals.lww.com'
];

// Se tiveres um sites.json com entradas, preferimos ler dele.
let sites = [];
try {
  const s = JSON.parse(fs.readFileSync(path.join(__dirname, 'sites.json'), 'utf8'));
  if (Array.isArray(s)) {
    sites = s;
  } else if (s.sites && Array.isArray(s.sites)) {
    sites = s.sites;
  } else {
    // fallback
    sites = [];
  }
} catch (e) {
  // não existe sites.json ou está mal formatado -> fallback manual abaixo
  sites = [];
}

// fallback manual: usar URLs conhecidas (ajusta conforme necessário)
if (sites.length === 0) {
  sites = [
    { id: 'stocktwits', url: 'https://stocktwits.com/symbol/PUBM' },
    { id: 'dzone', url: 'https://dzone.com/list' },
    { id: 'eetimes', url: 'https://www.eetimes.com/category/news-analysis/' },
    { id: 'medscape', url: 'https://www.medscape.com/index/list_13470_0' },
    { id: 'mdpi', url: 'https://www.mdpi.com/' },
    { id: 'journals_lww', url: 'https://journals.lww.com/' }
  ];
}

// filtrar apenas os sites que contêm um dos hostPrefs
const targets = sites.filter(s => {
  return hostPrefs.some(h => (s.url || s).includes(h));
});

if (targets.length === 0) {
  console.log('None of the configured sites match hostPrefs. Please update scripts/sites.json or the fallback list in this script.');
  process.exit(1);
}

console.log(`Found ${targets.length} target(s) to test:`);
targets.forEach(t => console.log(' -', t.url || t));

async function runOne(url, outPath) {
  return new Promise((resolve) => {
    const cmd = 'node';
    const args = [path.join(__dirname, 'render_page.js'), url, outPath];
    console.log(`\n--> running: ${cmd} ${args.join(' ')}`);
    const p = spawn(cmd, args, { stdio: ['ignore', 'pipe', 'pipe'], env: process.env });

    let out = '';
    let err = '';

    p.stdout.on('data', d => { process.stdout.write(d); out += d.toString(); });
    p.stderr.on('data', d => { process.stderr.write(d); err += d.toString(); });

    p.on('close', (code) => {
      console.log(`Process exited with ${code} for ${url}`);
      resolve({ url, code, out, err });
    });
  });
}

(async () => {
  const results = [];
  for (const t of targets) {
    const url = t.url || t;
    const safeName = (t.id || url.replace(/[:\/]+/g,'_')).replace(/[^a-zA-Z0-9\-_\.]/g,'');
    const outPath = path.join(__dirname, 'rendered_tests', `${safeName}.html`);
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    // executar e aguardar
    const r = await runOne(url, outPath);
    results.push(r);
    // pequena pausa entre hosts
    await new Promise(res => setTimeout(res, 700));
  }

  // resumo
  console.log('\n--- SUMMARY ---');
  for (const r of results) {
    console.log(`${r.url} -> exit ${r.code} | output length: ${r.out.length} | err length: ${r.err.length}`);
  }

  // gravar resultado em ficheiro
  fs.writeFileSync(path.join(__dirname, 'render_test_results.json'), JSON.stringify(results.map(x=>({url:x.url, code:x.code})), null, 2));
  console.log('Wrote render_test_results.json');
})();
