#!/usr/bin/env python3 
# scripts/generate_feeds.py
import os, json, re, sys
from bs4 import BeautifulSoup
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime, timedelta
from urllib.parse import urljoin
import warnings
from dateutil import parser as dateparser
from dateutil import tz as date_tz

# small compatibility shim to silence UnknownTimezoneWarning for "ET"
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

def try_parse_relative_date(s):
    """
    Tentativa simples de interpretar '13 hours ago', '18 h', '2 days', '1 hour ago',
    'há 2 dias' etc. Retorna datetime ou None.
    """
    if not s or not isinstance(s, str):
        return None
    s2 = s.strip().lower()
    # common english patterns
    m = re.search(r'(\d+)\s*(h|hr|hrs|hour|hours)\b', s2)
    if m:
        hours = int(m.group(1))
        return datetime.now(tz=date_tz.tzlocal()) - timedelta(hours=hours)
    m = re.search(r'(\d+)\s*(m|min|mins|minute|minutes)\b', s2)
    if m:
        mins = int(m.group(1))
        return datetime.now(tz=date_tz.tzlocal()) - timedelta(minutes=mins)
    m = re.search(r'(\d+)\s*(d|day|days|dias)\b', s2)
    if m:
        days = int(m.group(1))
        return datetime.now(tz=date_tz.tzlocal()) - timedelta(days=days)
    # patterns like "18 h •" or "18 h"
    m = re.search(r'(\d+)\s*h\b', s2)
    if m:
        return datetime.now(tz=date_tz.tzlocal()) - timedelta(hours=int(m.group(1)))
    # try Portuguese "há 2 dias", "2 horas"
    m = re.search(r'h[áa]\s*(\d+)\s*(h|hora|horas|min|m|dias?)', s2)
    if m:
        val = int(m.group(1))
        unit = m.group(2)
        if unit.startswith('h') or unit in ('hora','horas','h'):
            return datetime.now(tz=date_tz.tzlocal()) - timedelta(hours=val)
        if unit.startswith('m') or unit == 'min':
            return datetime.now(tz=date_tz.tzlocal()) - timedelta(minutes=val)
        if unit.startswith('d'):
            return datetime.now(tz=date_tz.tzlocal()) - timedelta(days=val)
    return None

def parse_date_string(s):
    if not s:
        return None
    s = str(s).strip()
    # try direct parse via dateutil
    try:
        dt = dateparser.parse(s)
        if isinstance(dt, datetime):
            return dt
    except Exception:
        pass
    # try heuristics for relative strings
    dt = try_parse_relative_date(s)
    if dt:
        return dt
    return None

def extract_items_from_html(html, cfg):
    soup = BeautifulSoup(html, 'html.parser')
    container_sel = cfg.get('item_container') or 'article'
    nodes = []
    for sel in [s.strip() for s in container_sel.split(',')]:
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

        # Title
        if title_sel:
            for s in [t.strip() for t in title_sel.split(',')]:
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
            parts = [p.strip() for p in link_sel.split(',')]
            for ps in parts:
                if '@' in ps:
                    sel, attr = ps.split('@',1)
                    try:
                        el = node.select_one(sel.strip())
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
                        href = el.get('href') or ''
                        link = urljoin(cfg.get('url',''), href)
                        if link:
                            break
        else:
            a = node.find('a')
            if a and a.has_attr('href'):
                link = urljoin(cfg.get('url',''), a.get('href'))

        # Description
        if desc_sel:
            for s in [t.strip() for t in desc_sel.split(',')]:
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

        # Date (best effort)
        for dsel in [cfg.get('date',''), 'time', '.date', 'span.date', '.timestamp', '.article-info .date']:
            if not dsel:
                continue
            try:
                el = node.select_one(dsel)
                if el:
                    date = el.get_text(strip=True)
                    break
            except Exception:
                continue

        # Topic (try common eyebrow/topic selectors)
        try:
            tsel = node.select_one('.eyebrow a, .eyebrow, .eyebrow.regular-eyebrow a, .eyebrow.red a, .upper-title a')
            if tsel:
                topic = tsel.get_text(strip=True)
        except Exception:
            topic = ''

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
    # items expected as dicts with keys title, link, description, date, full_text, topic (optional)
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

    # include keywords
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

# dedupe helper
def normalize_link_for_dedupe(l):
    if not l:
        return ''
    try:
        return re.sub(r'#.*$', '', l.strip().lower())
    except Exception:
        return l.strip().lower()

def dedupe_items(items):
    unique = {}
    out = []
    for it in (items or []):
        key = normalize_link_for_dedupe(it.get('link') or '')
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
     # description
     fe.description(it.get('description') or it.get('full_text') or '') 

     # category/topic -> write as category if present
     topic = (it.get('topic') or '').strip()
     if topic:
         try:
             # feedgen allows add_category via entry.category(term=...)
             fe.category(term=topic)
         except Exception:
             # fallback: append to description so it's not lost
             fe.description((topic + ' - ' + (fe.description() or '')).strip())

     # pubDate: try to parse into datetime and set; if fails, skip
     raw_date = it.get('date')
     if raw_date:
         try:
            parsed_dt = parse_date_string(raw_date)
            if parsed_dt and isinstance(parsed_dt, datetime):
                fe.pubDate(parsed_dt)
            else:
                # last resort: try dateparser.parse again and set if possible
                try:
                    parsed_dt2 = dateparser.parse(raw_date)
                    if isinstance(parsed_dt2, datetime):
                        fe.pubDate(parsed_dt2)
                except Exception:
                    pass
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
            keep, reason = matches_filters_debug(it, cfg)
            if keep:
                 it['matched_reason'] = reason
                 matched.append(it)  

        print(f'{len(matched)} items matched filters for {name}')
        if not matched and items:
            print(f'No items matched filters for {name} — falling back to all {len(items)} items')
            matched = items

        # dedupe
        matched = dedupe_items(matched)

        # write feed
        build_feed(name, cfg, matched)

if __name__ == '__main__':
    main()
