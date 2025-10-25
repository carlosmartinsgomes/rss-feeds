// scripts/run_renders.js
// Executa vários renders em paralelo com limite de concorrência.
// Uso: node scripts/run_renders.js
// Define SCRIPTS via env (ex: SCRIPTS=scripts) e RENDER_CONCURRENCY (ex: 4)

const { spawn } = require('child_process');
const path = require('path');

const SCRIPTS = process.env.SCRIPTS || 'scripts';
const renderScript = path.join(SCRIPTS, 'render_page.js');

// Lista de pares [url, output] — adapta se quiseres adicionar/retirar URLs
const RENDER_TARGETS = [
  ["https://inmodeinvestors.com/press-release", `${SCRIPTS}/rendered/inmode-press.html`],
  ["https://www.inmodemd.com/clinical-papers/", `${SCRIPTS}/rendered/inmodemd.html`],
  ["https://www.darkreading.com/latest-news", `${SCRIPTS}/rendered/darkreading.html`],
  ["https://adage.com/news/", `${SCRIPTS}/rendered/adage.html`],
  ["https://digiday.com/", `${SCRIPTS}/rendered/digiday.html`],
  ["https://www.modernhealthcare.com/latest-news/", `${SCRIPTS}/rendered/modernhealthcare.html`],
  ["https://aibusiness.com/latest-news", `${SCRIPTS}/rendered/aibusiness.html`],
  ["https://dzone.com/list", `${SCRIPTS}/rendered/dzone.html`],
  ["https://www.eetimes.com/category/news-analysis/", `${SCRIPTS}/rendered/eetimes.html`],
  ["https://www.edsurge.com/news", `${SCRIPTS}/rendered/edsurge.html`],
  ["https://www.mdpi.com/rss/journal/jaestheticmed", `${SCRIPTS}/rendered/mdpi.html`],
  ["https://www.medscape.com/index/list_13470_0", `${SCRIPTS}/rendered/medscape0.html`],
  ["https://www.medscape.com/index/list_13470_1", `${SCRIPTS}/rendered/medscape1.html`],
  ["https://stocktwits.com/symbol/PUBM", `${SCRIPTS}/rendered/stocktwits.html`],
  ["https://journals.lww.com/plasreconsurg/_layouts/15/OAKS.Journals/feed.aspx?FeedType=LatestArticles", `${SCRIPTS}/rendered/journals-current.html`],
  ["https://journals.lww.com/plasreconsurg/_layouts/15/OAKS.Journals/feed.aspx?FeedType=CurrentIssue", `${SCRIPTS}/rendered/journals-latest.html`],
  ["https://finance.yahoo.com/quotes/TTD,PUBM,INMD,CRWD,FTNT,DDOG,PINS,MNDY,HUBS,ETSY,ONON,RBRK,ANET,DUOL,GTLB,ALAB,AXON,DAVA,EPAM,HIMS,LUV,PAYC/", `${SCRIPTS}/rendered/finance-yahoo.html`],
  ["https://www.exchangewire.com/?s=pubmatic", `${SCRIPTS}/rendered/exchangewire-pubm.html`],
  ["https://www.exchangewire.com/?s=trade+desk", `${SCRIPTS}/rendered/exchangewire-ttd.html`],
  ["https://www.exchangewire.com/?s=pinterest", `${SCRIPTS}/rendered/exchangewire-pins.html`]
];

const CONCURRENCY = Number(process.env.RENDER_CONCURRENCY || 4);

function runOne(url, out) {
  return new Promise((resolve) => {
    const p = spawn('node', [renderScript, url, out], { stdio: 'inherit' });
    p.on('close', (code) => resolve({ url, out, code }));
    p.on('error', (err) => resolve({ url, out, code: 99, err }));
  });
}

async function runAll() {
  const queue = RENDER_TARGETS.slice();
  const running = [];
  const results = [];

  while (queue.length || running.length) {
    while (queue.length && running.length < CONCURRENCY) {
      const [url, out] = queue.shift();
      const p = runOne(url, out).then(res => {
        const idx = running.indexOf(p);
        if (idx >= 0) running.splice(idx, 1);
        results.push(res);
      });
      running.push(p);
    }
    // espera que pelo menos 1 termine
    await Promise.race(running.map(r => r.catch(()=>{})));
  }
  return results;
}

runAll().then(results => {
  console.log("All done. Results:");
  results.forEach(r => console.log(`${r.url} -> code ${r.code}`));
  const failed = results.filter(r => r.code && r.code !== 0);
  if (failed.length) {
    console.error(`Failed ${failed.length} renders.`);
    process.exit(2);
  } else {
    process.exit(0);
  }
}).catch(err => {
  console.error("Runner error:", err);
  process.exit(3);
});
