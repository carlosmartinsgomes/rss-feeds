// scripts/test_special_hosts.js
// Simple runner to call render_page.js for a set of known-problematic URLs
const { spawnSync } = require('child_process');
const path = require('path');
const fs = require('fs');

const tests = [
  { id: 'stocktwits', url: 'https://stocktwits.com/symbol/PUBM' },
  { id: 'dzone', url: 'https://dzone.com/list' },
  { id: 'eetimes', url: 'https://www.eetimes.com/category/news-analysis/' },
  { id: 'medscape', url: 'https://www.medscape.com/index/list_13470_0' },
  { id: 'mdpi', url: 'https://www.mdpi.com/rss/journal/jaestheticmed' },
  { id: 'journals_lww', url: 'https://journals.lww.com/plasreconsurg/_layouts/15/OAKS.Journals/feed.aspx?FeedType=LatestArticles' }
];

fs.mkdirSync(path.join('scripts','rendered_tests'), { recursive: true });

for (const t of tests) {
  console.log('--- Running test for', t.id, t.url);
  const out = path.join('scripts','rendered_tests', `${t.id}.html`);
  const r = spawnSync('node', ['scripts/render_page.js', t.url, out], { stdio: 'inherit', env: process.env, timeout: 180000 });
  if (r.error) {
    console.error('Error spawning render_page.js', r.error);
  } else {
    console.log('Exit code:', r.status);
  }
  console.log('--- finished', t.id, '\n');
}
