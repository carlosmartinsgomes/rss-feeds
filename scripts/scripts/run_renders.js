// scripts/run_renders.js
const { spawn } = require('child_process');
const path = require('path');

const SCRIPTS = process.env.SCRIPTS || 'scripts';
const renderScript = path.join(SCRIPTS, 'render_page.js');

// lista de pares url|out (copiar do workflow original)
const RENDER_TARGETS = [
  ["https://www.businesswire.com/newsroom", `${SCRIPTS}/rendered/businesswire-page1.html`],
  ["https://www.businesswire.com/newsroom?page=2", `${SCRIPTS}/rendered/businesswire-page2.html`],
  ["https://www.businesswire.com/newsroom?page=3", `${SCRIPTS}/rendered/businesswire-page3.html`],
  ["https://www.businesswire.com/newsroom?page=4", `${SCRIPTS}/rendered/businesswire-page4.html`],
  ["https://www.businesswire.com/newsroom?page=5", `${SCRIPTS}/rendered/businesswire-page5.html`],
  ["https://inmodeinvestors.com/press-release", `${SCRIPTS}/rendered/inmode-press.html`],
  ["https://www.darkreading.com/", `${SCRIPTS}/rendered/darkreading.html`],
  ["https://www.iotworldtoday.com/", `${SCRIPTS}/rendered/iotworldtoday.html`]
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
        // remove from running when done
        const idx = running.indexOf(p);
        if (idx >= 0) running.splice(idx, 1);
        results.push(res);
      });
      running.push(p);
    }
    // wait for any to finish
    await Promise.race(running.map(r => r.catch(()=>{})));
  }
  return results;
}

runAll().then(results => {
  console.log("All done. Results:", results.map(r=>`${r.url} => ${r.code}`));
  const failed = results.filter(r => r.code && r.code !== 0);
  if (failed.length) process.exit(2);
}).catch(err => {
  console.error("Runner error:", err);
  process.exit(3);
});
