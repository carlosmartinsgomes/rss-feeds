#!/usr/bin/env python3
# scripts/feeds_to_excel.py
# Gera feeds_summary.xlsx a partir de feeds/*.xml e (especial) do HTML renderizado
# do modernhealthcare em scripts/rendered/modernhealthcare.html.
#
# Requer: pandas, openpyxl, beautifulsoup4, lxml
# Uso: python3 scripts/feeds_to_excel.py

import os
import glob
import re
import sys
from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parent
FEEDS_DIR = (ROOT.parent / 'feeds') if (ROOT.parent / 'feeds').exists() else (ROOT / '..' / 'feeds')
FEEDS_DIR = Path(FEEDS_DIR).resolve()
RENDERED_DIR = ROOT / 'rendered'
MH_RENDERED = RENDERED_DIR / 'modernhealthcare.html'
OUT_XLSX = Path.cwd() / 'feeds_summary.xlsx'

BAD_HREF_RE = re.compile(r'(^#|^javascript:|mailto:|/help|/legal|cookie|privacy|terms|signin|login|settings|/consent|/preferences|/policies|/subscribe)', re.I)
BLACKLIST_TITLE = [re.compile(r'^\s*category\s*$', re.I),
                   re.compile(r'^\s*healthcare news\s*$', re.I),
                   re.compile(r'^\s*latest news\s*$', re.I),
                   re.compile(r'^\s*image\s*$', re.I),
                   re.compile(r'^\s*read more\s*$', re.I)]

def txt_of(el):
    if not el:
        return ''
    return ' '.join(el.stripped_strings)

def safe_abs(href, base):
    try:
        return str((base.joinpath(href) if not re.match(r'^https?://', href) else href))
    except Exception:
        try:
            # fallback URL join style
            from urllib.parse import urljoin
            return urljoin(str(base), href)
        except Exception:
            return (href or '').strip()

def parse_xml_feeds():
    rows = []
    for path in sorted(glob.glob(str(FEEDS_DIR / '*.xml'))):
        filename = Path(path).name
        try:
            tree = ET.parse(path)
            root = tree.getroot()
            # try RSS items then Atom entries
            items = root.findall('.//item') or root.findall('.//{http://www.w3.org/2005/Atom}entry')
            for it in items:
                title = ''
                link = ''
                date = ''
                desc = ''
                # title
                t = it.find('title')
                if t is None:
                    t = it.find('{http://www.w3.org/2005/Atom}title')
                if t is not None and t.text:
                    title = t.text.strip()
                # link (text or href)
                l = it.find('link')
                if l is not None:
                    if l.text and l.text.strip():
                        link = l.text.strip()
                    elif 'href' in l.attrib:
                        link = l.attrib.get('href','').strip()
                if not link:
                    # try link with href attribute deeper
                    el = it.find(".//link[@href]")
                    if el is not None and 'href' in el.attrib:
                        link = el.attrib.get('href','').strip()
                # date
                d = it.find('pubDate') or it.find('date') or it.find('pubDate')
                if d is not None and d.text:
                    date = d.text.strip()
                # description
                desc_el = it.find('description') or it.find('summary') or it.find('content')
                if desc_el is not None and desc_el.text:
                    desc = desc_el.text.strip()
                rows.append({
                    'source': filename.replace('.xml',''),
                    'title': title,
                    'link': link,
                    'date': date,
                    'description': desc,
                    'origin': path
                })
        except Exception as e:
            print(f'Warning: failed to parse {path}: {e}', file=sys.stderr)
    return rows

# --- ModernHealthcare HTML parser (mirror do console IIFE) ---
def parse_modernhealthcare_rendered(html_path, max_items=10):
    rows = []
    try:
        s = Path(html_path).read_text(encoding='utf-8', errors='ignore')
    except Exception as e:
        print(f'Could not read {html_path}: {e}', file=sys.stderr)
        return rows

    soup = BeautifulSoup(s, 'lxml')
    base = None
    # try to determine base (from <base> or from canonical link)
    base_tag = soup.find('base')
    if base_tag and base_tag.get('href'):
        base = base_tag.get('href')
    else:
        # try canonical or og:url
        can = soup.find('link', rel='canonical')
        if can and can.get('href'):
            base = can.get('href')
        else:
            og = soup.find('meta', property='og:url') or soup.find('meta', attrs={'name':'og:url'})
            if og and og.get('content'):
                base = og.get('content')
    if not base:
        # fallback to common host
        base = 'https://www.modernhealthcare.com/'

    def is_bad_title(t):
        if not t:
            return True
        if len(t.strip()) < 6:
            return True
        for re_ in BLACKLIST_TITLE:
            if re_.search(t):
                return True
        if re.match(r'^(category|image|home|latest|subscribe|return)$', t.strip(), re.I):
            return True
        return False

    # remove obvious modal-like nodes (best-effort)
    try:
        for sel in ['div[class*="subscribe"]', 'div[id*="subscribe"]', '.newsletter-popup', '.newsletter-modal',
                    '.subscription-overlay', '.overlay--newsletter', '[data-testid*="modal"]', '[role="dialog"]',
                    '.modal-backdrop', '.modal', '.paywall', '.newsletter']:
            for n in soup.select(sel):
                try:
                    n.decompose()
                except Exception:
                    try:
                        n.extract()
                    except Exception:
                        pass
    except Exception:
        pass

    # title elements
    title_selectors = 'span.u-text-text-dark, a[aria-label^="Title"] span, .news-title.fs-5, .news-title'
    titleEls = []
    try:
        titleEls = soup.select(title_selectors)
    except Exception:
        # fallback broad
        titleEls = soup.find_all(['h1', 'h2', 'h3', 'a'])

    seen = set()
    def find_wrapper(el):
        if el is None:
            return None
        cur = el
        for i in range(8):
            if cur is None:
                break
            if getattr(cur, 'name', '').lower() == 'article':
                return cur
            cls = cur.get('class') or []
            cls = [c for c in cls] if cls else []
            if any(c in ('u-border-b','views-row','col-lg-6','square-one','view-content') for c in cls):
                return cur
            cur = cur.parent
        # fallback: closest article
        art = el.find_parent('article')
        if art:
            return art
        # last fallback
        return el

    def find_link(wrapper, titleEl):
        if wrapper is None:
            return ''
        # prefer anchor containing the title
        # BeautifulSoup: find parent anchor
        a_parent = titleEl.find_parent('a') if titleEl else None
        if a_parent and a_parent.get('href'):
            h = a_parent.get('href')
            if not BAD_HREF_RE.search(h):
                return safe_abs(h, base)
        order = ['a.content-list-title[href]', 'a[aria-label^="Title"]', 'a[href].overlay', '.content-list-title a[href]', 'a[href]']
        for sel in order:
            try:
                el = wrapper.select_one(sel)
            except Exception:
                el = None
            if el:
                h = el.get('href') or ''
                if h and not BAD_HREF_RE.search(h):
                    return safe_abs(h, base)
        # any anchor fallback
        anyA = wrapper.find('a', href=True)
        if anyA:
            h = anyA.get('href') or ''
            if h and not BAD_HREF_RE.search(h):
                return safe_abs(h, base)
        return ''

    def find_date(wrapper):
        if wrapper is None:
            return ''
        cand_selectors = ['.u-whitespace-nowrap', 'time', '[datetime]', '.date', '.timestamp', '.post-date', '.day_list', '.time_list']
        # search inside closest article first
        anc = wrapper.find_parent('article') or wrapper
        for sel in cand_selectors:
            try:
                el = anc.select_one(sel)
            except Exception:
                el = None
            if el:
                t = txt_of(el)
                t = re.sub(r'^\|\s*', '', t).strip()
                if t and not re.search(r'subscribe|image', t, re.I):
                    return t
        # fallback: regex date in wrapper text
        raw = txt_of(wrapper)
        m = re.search(r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}(?:\s+\d{1,2}:\d{2}\s*(?:AM|PM))?', raw, re.I)
        if m:
            return m.group(0)
        return ''

    def find_description(wrapper):
        if wrapper is None:
            return ''
        sels = ['div.u-h-auto.u-w-full.u-font-secondary p', 'div.field.field--name-field-subheader.field--item', '.dek', '.summary', '.body_list', '.news-content p', '.content-list-meta + p', 'p']
        for sel in sels:
            try:
                el = wrapper.select_one(sel)
            except Exception:
                el = None
            if el:
                t = txt_of(el)
                if t and not re.search(r'subscribe|image', t, re.I):
                    return t
        # fallback first <p>
        p = wrapper.find('p')
        if p:
            t = txt_of(p)
            if t and not re.search(r'subscribe', t, re.I):
                return t
        return ''

    for el in titleEls:
        try:
            title_text = txt_of(el).strip()
            if is_bad_title(title_text):
                continue
            wrapper = find_wrapper(el)
            if wrapper is None:
                continue
            link = find_link(wrapper, el)
            if (not link) and (not title_text):
                continue
            if BAD_HREF_RE.search(link):
                continue
            key = (link or title_text).rstrip('/')
            if key in seen:
                continue
            seen.add(key)
            date = find_date(wrapper) or ''
            desc = find_description(wrapper) or ''
            rows.append({
                'source': 'modernhealthcare',
                'title': title_text,
                'link': link,
                'date': date,
                'description': desc,
                'origin': str(html_path)
            })
            if len(rows) >= max_items:
                break
        except Exception as e:
            # ignore problematic element
            continue

    # if not enough items, try wrapper scanning (article, .u-border-b, .views-row, .view-content > div, .col-lg-6)
    if len(rows) < max_items:
        candidates = soup.select('article, .u-border-b, .views-row, .view-content > div, .col-lg-6')
        for wrapper in candidates:
            if len(rows) >= max_items:
                break
            try:
                anyA = wrapper.find('a', href=True)
                anyLink = safe_abs(anyA.get('href'), base) if anyA else ''
                if anyLink and anyLink in seen:
                    continue
                # try to extract title by common selectors inside wrapper
                tsel = None
                for sel in ['span.u-text-text-dark', 'a[aria-label^="Title"] span', '.news-title', '.content-list-title a', 'a.title', 'h2', 'h3']:
                    try:
                        tsel = wrapper.select_one(sel)
                    except Exception:
                        tsel = None
                    if tsel:
                        break
                title_text = txt_of(tsel) if tsel else ''
                if is_bad_title(title_text):
                    continue
                link = find_link(wrapper, tsel) or anyLink
                if BAD_HREF_RE.search(link):
                    continue
                key = (link or title_text).rstrip('/')
                if not key or key in seen:
                    continue
                seen.add(key)
                date = find_date(wrapper) or ''
                desc = find_description(wrapper) or ''
                rows.append({
                    'source': 'modernhealthcare',
                    'title': title_text,
                    'link': link,
                    'date': date,
                    'description': desc,
                    'origin': str(html_path)
                })
            except Exception:
                continue

    # final anchor fallback to reach MAX
    if len(rows) < max_items:
        for a in soup.select('#main-content a[href]'):
            if len(rows) >= max_items:
                break
            try:
                h = a.get('href') or ''
                if not h:
                    continue
                if BAD_HREF_RE.search(h):
                    continue
                abs_h = safe_abs(h, base)
                if abs_h in seen:
                    continue
                t = txt_of(a)
                if not t or len(t.strip()) < 6:
                    continue
                seen.add(abs_h)
                rows.append({
                    'source': 'modernhealthcare',
                    'title': t,
                    'link': abs_h,
                    'date': '',
                    'description': '',
                    'origin': str(html_path)
                })
            except Exception:
                continue

    print(f'ModernHealthcare extraction count: {len(rows)}', file=sys.stderr)
    return rows

def dedupe_rows(rows):
    seen = set()
    out = []
    for r in rows:
        key = (r.get('link') or '').strip().lower()
        if not key:
            key = (r.get('title') or '').strip().lower()[:200]
        if not key:
            # create placeholder
            key = f"__no_key__{len(out)}"
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out

def main():
    all_rows = []

    # 1) parse existing feeds XML files
    xml_rows = parse_xml_feeds()
    print(f'Parsed {len(xml_rows)} items from XML feeds', file=sys.stderr)
    all_rows.extend(xml_rows)

    # 2) special: parse rendered modernhealthcare HTML if present
    if MH_RENDERED.exists():
        mh_rows = parse_modernhealthcare_rendered(MH_RENDERED, max_items=10)
        # prefer MH items at top (but avoid duplicates)
        print(f'Parsed {len(mh_rows)} items from rendered modernhealthcare', file=sys.stderr)
        # prepend modernhealthcare items so they appear in summary (optional)
        all_rows = mh_rows + all_rows

    # 3) dedupe preserving order
    out_rows = dedupe_rows(all_rows)
    print(f'After dedupe -> {len(out_rows)} items', file=sys.stderr)

    # 4) to DataFrame and Excel
    if not out_rows:
        print('No items found to write to Excel.', file=sys.stderr)
    df = pd.DataFrame(out_rows)
    # ensure columns exist and order them
    cols = ['source','title','link','date','description','origin']
    for c in cols:
        if c not in df.columns:
            df[c] = ''
    df = df[cols]
    df.to_excel(OUT_XLSX, index=False)
    print('Wrote', OUT_XLSX, file=sys.stderr)

if __name__ == '__main__':
    main()
