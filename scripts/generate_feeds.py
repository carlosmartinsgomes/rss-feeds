# scripts/generate_feeds.py
import json, os, requests
from bs4 import BeautifulSoup
from feedgen.feed import FeedGenerator
from datetime import datetime

os.makedirs('feeds', exist_ok=True)
with open('sites.json','r',encoding='utf-8') as f:
    sites = json.load(f)

headers = {'User-Agent':'rss-generator/1.0 (+https://github.com)'}  # educado

def make_feed(site):
    url = site['url']
    r = requests.get(url, headers=headers, timeout=15)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, 'html.parser')

    item_selector = site.get('item_selector', 'article')
    items = soup.select(item_selector)[:20]

    fg = FeedGenerator()
    fg.title(site.get('title', url))
    fg.link(href=url)
    fg.description(site.get('description','Generated feed'))

    for it in items:
        title_tag = it.find(['h1','h2','h3','a'])
        link = None
        if title_tag and title_tag.find('a'):
            title = title_tag.get_text(strip=True)
            link = title_tag.find('a').get('href')
        else:
            title = title_tag.get_text(strip=True) if title_tag else it.get_text(strip=True)[:60]
        if link and link.startswith('/'):
            from urllib.parse import urljoin
            link = urljoin(url, link)
        time_tag = it.find('time')
        pubdate = None
        if time_tag and time_tag.get('datetime'):
            pubdate = time_tag.get('datetime')
        fe = fg.add_entry()
        fe.title(title or 'No title')
        fe.link(href=link or url)
        fe.pubDate(pubdate or datetime.utcnow())

    outname = f"feeds/{site['id']}.xml"
    fg.rss_file(outname)
    print("Wrote", outname)

for s in sites:
    try:
        make_feed(s)
    except Exception as e:
        print("Error for", s.get('id'), e)
