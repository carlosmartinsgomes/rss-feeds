#!/usr/bin/env python3 
# scripts/generate_feeds.py
import os, json, re, sys
from bs4 import BeautifulSoup
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime
from urllib.parse import urljoin
import warnings
from dateutil import parser as dateparser
from dateutil import tz as date_tz

# silence UnknownTimezoneWarning and map "ET" to America/New_York
try:
    from dateutil import _parser as _dateutil__parser
    UnknownTimezoneWarning = _dateutil__parser.UnknownTimezoneWarning
    warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)
except Exception:
    warnings.filterwarnings("ignore", message="tzname .* identified but not understood")

_default_tzinfos = {"ET": date_tz.gettz("America/New_York")}
_original_parse = dateparser.parse

def _parse_with_default_tzinfos(timestr, *args, **kwargs):
    if "tzinfos" not in kwargs or kwargs["tzinfos"] is None:
        kwargs["tzinfos"] = _default_tzinfos
    return _original_parse(timestr, *args, **kwargs)

dateparser.parse = _parse_with_default_tzinfos

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
    for sel in [s.strip() for s in container_sel.split(',') if s.strip()]:
        try:
            nodes.extend(soup.select(sel))
        except Exception:
            continue
    items = []
    for node in nodes:
        title = ''
        link = ''
        date = ''
        desc = ''
        topic = ''

        title_sel = cfg.get('title')
        link_sel = cfg.get('link')
        desc_sel = cfg.get('description')
        topic_sel = cfg.get('topic')

        # Title
        if title_sel:
            for s in [t.strip() for t in title_sel.split(',') if t.strip()]:
                try:
                    el = node.select_one(s)
                except Exception:
                    el = None
                if el:
                    title = el.get_text(strip=True)
                    break
        else:
            t = node.find(['h1','h2','h3','a'])
            if t:
                title = t.get_text(strip=True)

        # Link
        if link_sel:
            parts = [p.strip() for p in link_sel.split(',') if p.strip()]
            for ps in parts:
                if '@' in ps:
                    sel, attr = ps.split('@',1)
                    sel = sel.strip() or 'a'
                    try:
                        el = node.select_one(sel)
                    except Exception:
                        el = None
                    if el and el.has_attr(attr):
                        link = urljoin(cfg.get('url',''), el.get(attr))
                        break
                else:
                    try:
                        el = node.select_one(ps)
                    except Exception:
                        el = None
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
            for s in [t.strip() for t in desc_sel.split(',') if t.strip()]:
                try:
                    el = node.select_one(s)
                except Exception:
                    el = None
                if el:
                    desc = el.get_text(" ", strip=True)
                    break
        else:
            p = node.find('p')
            if p:
                desc = p.get_text(" ", strip=True)

        # Topic (optional)
        if topic_sel:
            for s in [t.strip() for t in topic_sel.split(',') if t.strip()]:
                try:
                    el = node.select_one(s)
                except Exception:
                    el = None
                if el:
                    topic = el.get_text(strip=True)
                    break

        # Date (best-effort) - try selectors then try ancestors
        date_selectors = [s.strip() for s in ((cfg.get('date') or '').split(',')) if s.strip()]
        date_selectors += ['.article-info .date', 'span.date', '.date', 'time', '.timestamp']
        # try within node
        for dsel in date_selectors:
            try:
                el = node.select_one(dsel)
            except Exception:
                el = None
            if el:
                date = el.get_text(strip=True)
                break

        # try ancestors (up to 3 levels) if not found
        if not date:
            ancestor = node
            for _ in range(3):
                if not getattr(ancestor, 'parent', None):
                    break
                ancestor = ancestor.parent
                if ancestor is None:
                    break
                for dsel in date_selectors:
                    try:
                        el = ancestor.select_one(dsel)
                    except Exception:
                        el = None
                    if el:
                        date = el.get_text(strip=True)
                        break
                if date:
                    break

        # fallback: full text
        full_text = (title or '') + ' ' + (desc or '') + ' ' + text_of_node(node)

        items.append({
            'title': title,
            'link': link,
            'description': desc,
            'date': date,
            'topic': topic,
            'full_text': full_text
        })
    return items

def parse_feed(items):
    return items

def matches_filters_debug(item, cfg):
    kw = cfg.get('filters', {}).get('keywords', [])
    exclude = cfg.get('filters', {}).get('exclude', [])
    if not kw and not exclude:
        return True, None
    text_title = (item.get('title','') or '').lower()
    text_desc = (item.get('description','') or '').lower()
    text_full = (item.get('full_text','') or '').lower()
    text_link = (item.get('link','') or '').lower()

    # include
    if kw:
        for k in kw:
            kl = k.lower()
            if kl in text_title:
                return True, f"keyword '{k}' in title"
            if kl in text_desc:
                return True, f"keyword '{k}' in description"
            if kl in text_full:
                return True, f"keyword '{k}' in full_text"
            if kl in text_link:
                return True, f"keyword '{k}' in link"
        return False, None

    # exclude
    for ex in exclude:
        if ex.lower() in text_title or ex.lower() in text_desc or ex.lower() in text_full:
            return False, f"exclude '{ex}' matched"
    return True, None

def dedupe_items(items):
    unique = {}
    out = []
    for it in (items or []):
        key = (it.get('link') or '').strip().lower()
        if not key:
            key = (it.get('title','') or '').strip().lower()[:200]
        if not key:
            key = f"__no_key__{len(out)}"
        if key not in unique:
            unique[key] = True
            out.append(it)
    return out

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
        # add topic as category if present
        if it.get('topic'):
            try:
                fe.category(term=it.get('topic'))
            except Exception:
                pass
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
                            items.append({'title': title, 'link': link, 'description': '', 'date': '', 'topic': '', 'full_text': title})
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
            keep, reason = matches_filters_debug(it, cfg)
            if keep:
                it['matched_reason'] = reason
                matched.append(it)

        print(f'{len(matched)} items matched filters for {name}')
        if not matched and items:
            print(f'No items matched filters for {name} — falling back to all {len(items)} items')
            matched = items

        matched = dedupe_items(matched)

        build_feed(name, cfg, matched)

if __name__ == '__main__':
    main()
