#!/usr/bin/env python3
# scripts/generate_feeds.py
import os, json, re, sys
from bs4 import BeautifulSoup
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime
from urllib.parse import urljoin

ROOT = os.path.dirname(__file__)
SITES_JSON = os.path.join(ROOT, 'sites.json')

def load_sites():
    j = json.load(open(SITES_JSON, 'r', encoding='utf-8'))
    return j.get('sites', [])

def fetch_html(url, timeout=20):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def text_of_node(node):
    if node is None:
        return ''
    return ' '.join(node.stripped_strings)

def extract_items_from_html(html, cfg):
    soup = BeautifulSoup(html, 'html.parser')
    container_sel = cfg.get('item_container') or 'article'
    nodes = []
    for sel in [s.strip() for s in container_sel.split(',')]:
        nodes.extend(soup.select(sel))
    items = []
    for node in nodes:
        title = ''
        link = ''
        date = ''
        desc = ''
        title_sel = cfg.get('title')
        link_sel = cfg.get('link')
        desc_sel = cfg.get('description')

        # Title
        if title_sel:
            for s in [t.strip() for t in title_sel.split(',')]:
                el = node.select_one(s)
                if el:
                    title = el.get_text(strip=True)
                    break
        else:
            t = node.find(['h1','h2','h3','a'])
            if t:
                title = t.get_text(strip=True)

        # Link
        if link_sel:
            # link selector like "a@href" or ".c-title a@href"
            parts = [p.strip() for p in link_sel.split(',')]
            for ps in parts:
                if '@' in ps:
                    sel, attr = ps.split('@',1)
                    el = node.select_one(sel.strip())
                    if el and el.has_attr(attr):
                        link = urljoin(cfg.get('url',''), el.get(attr))
                        break
                else:
                    el = node.select_one(ps)
                    if el:
                        link = urljoin(cfg.get('url',''), el.get('href') or '')
                        if link:
                            break
        else:
            a = node.find('a')
            if a and a.has_attr('href'):
                link = urljoin(cfg.get('url',''), a.get('href'))

        # Description
        if desc_sel:
            for s in [t.strip() for t in desc_sel.split(',')]:
                el = node.select_one(s)
                if el:
                    desc = el.get_text(" ", strip=True)
                    break
        else:
            p = node.find('p')
            if p:
                desc = p.get_text(" ", strip=True)

        # Date (best effort)
        for dsel in [cfg.get('date',''), 'time', '.date', 'span.date']:
            if not dsel:
                continue
            try:
                el = node.select_one(dsel)
                if el:
                    date = el.get_text(strip=True)
                    break
            except Exception:
                continue

        # fallback: full text
        full_text = (title or '') + ' ' + (desc or '') + ' ' + text_of_node(node)

        items.append({
            'title': title,
            'link': link,
            'description': desc,
            'date': date,
            'full_text': full_text
        })
    return items

def parse_feed(items):
    # items expected as dicts with keys title, link, description, date, full_text
    return items

def matches_filters(item, cfg):
    kw = cfg.get('filters', {}).get('keywords', [])
    exclude = cfg.get('filters', {}).get('exclude', [])
    if not kw and not exclude:
        return True
    text = ' '.join([item.get('title','') or '', item.get('description','') or '', item.get('full_text','') or '', item.get('link','') or '']).lower()
    # include — at least one keyword must appear if keywords not empty
    if kw:
        matched = False
        for k in kw:
            if k.lower() in text:
                matched = True
                break
        if not matched:
            return False
    # exclude — if any exclude present in text, skip
    for ex in exclude:
        if ex.lower() in text:
            return False
    return True

def build_feed(name, cfg, items):
    fg = FeedGenerator()
    fg.title(name)
    fg.link(href=cfg.get('url',''), rel='alternate')
    fg.description(f'Feed gerado para {name}')
    count = 0
    for it in items:
        fe = fg.add_entry()
        fe.title(it.get('title') or 'No title')
        if it.get('link'):
            fe.link(href=it.get('link'))
        fe.description(it.get('description') or it.get('full_text') or '')
        # pubDate: try to leave raw string
        if it.get('date'):
            try:
                fe.pubDate(it.get('date'))
            except Exception:
                pass
        count += 1
    outdir = os.path.join(ROOT, '..', 'feeds') if os.path.exists(os.path.join(ROOT,'..','feeds')) else os.path.join(ROOT, '..', 'feeds')
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f'{name}.xml')
    fg.rss_file(outpath)
    print(f'Wrote {outpath}')

def main():
    sites = load_sites()
    print(f'Loaded {len(sites)} site configurations from {SITES_JSON}')
    for cfg in sites:
        name = cfg.get('name')
        url = cfg.get('url')
        print(f'--- Processing {name} ({url}) ---')
        html = None
        # prefer rendered file if exists
        rf = cfg.get('render_file')
        if rf:
            if not os.path.isabs(rf) and not rf.startswith('scripts'):
                rf = os.path.join('scripts', rf)
            if os.path.exists(rf):
                try:
                    html = open(rf, 'r', encoding='utf-8').read()
                    print(f'Using rendered file: {rf} for {name}')
                except Exception as e:
                    print('Failed reading rendered file:', e)
                    html = None
            else:
                print(f'No rendered file found at {rf} for {name}')
        if html is None:
            # fallback to requests
            try:
                print(f'Fetching {url} via requests...')
                html = fetch_html(url)
            except Exception as e:
                print(f'Request error for {url}: {e}')
                html = ''
        items = []
        if html:
            try:
                items = extract_items_from_html(html, cfg)
                if not items:
                    # fallback generic li selector
                    soup = BeautifulSoup(html, 'html.parser')
                    nodes = soup.select('li')
                    if nodes:
                        print(f'Fallback: found {len(nodes)} nodes with selector \'li\'')
                        for n in nodes[:200]:
                            title = n.get_text(" ", strip=True)[:200]
                            link = ''
                            a = n.find('a')
                            if a and a.has_attr('href'):
                                link = a.get('href')
                            items.append({'title': title, 'link': link, 'description': '', 'date': '', 'full_text': title})
            except Exception as e:
                print('Error parsing HTML:', e)
                items = []
        else:
            items = []

        print(f'Found {len(items)} items for {name} (raw)')

        # apply filters
        matched = []
        kw = cfg.get('filters', {}).get('keywords', [])
        print(f'Applying {len(kw)} keyword filters for {name}: {kw}')
        for it in items:
            if matches_filters(it, cfg):
                matched.append(it)

        print(f'{len(matched)} items matched filters for {name}')
        if not matched and items:
            print(f'No items matched filters for {name} — falling back to all {len(items)} items')
            matched = items

        # write feed
        build_feed(name, cfg, matched)

if __name__ == '__main__':
    main()
