#!/usr/bin/env python3
# scripts/feeds_to_excel.py
# Requisitos: feedparser, pandas, beautifulsoup4, openpyxl, python-dateutil, requests
#
# Este ficheiro foi adaptado para, no caso do site "modernhealthcare",
# reproduzir em Python a mesma lógica do snippet JS que correste no console:
# - fetch da página https://www.modernhealthcare.com/latest-news/
# - remoção tentativa de modais/overlays
# - detecção de títulos em ordem DOM, subida de wrapper, extração de link/date/description
# - até MAX items (10)

import os
import glob
import json
import feedparser
import pandas as pd
import html
import re
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from urllib.parse import urljoin

OUT_XLSX = "feeds_summary.xlsx"
FEEDS_DIR = "feeds"
SITES_JSON_PATHS = ["scripts/sites.json", "rss-feeds/scripts/sites.json", "sites.json"]

# Regex para detectar datas comuns: "September 11, 2025 08:17 PM", "2025-09-11", "11 Sep 2025", etc.
_DATE_RE = re.compile(
    r'\b(?:'
    r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)'
    r'\s+\d{1,2},\s+\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM|am|pm))?)?'
    r'|\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?'
    r'|\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}'
    r')',
    flags=re.IGNORECASE
)

def strip_html_short(html_text, max_len=300):
    if not html_text:
        return ""
    t = html.unescape(html_text)
    try:
        s = BeautifulSoup(t, "html.parser").get_text(separator=" ", strip=True)
    except Exception:
        s = re.sub(r"<[^>]+>", "", t)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        return s[:max_len].rstrip() + "…"
    return s

def load_sites_item_container():
    for p in SITES_JSON_PATHS:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as fh:
                    raw = fh.read()
                obj = json.loads(raw)
                sites = obj.get("sites", obj if isinstance(obj, list) else [])
                mapping = {}
                for s in sites:
                    name = s.get("name") if isinstance(s, dict) else None
                    if name:
                        mapping[name] = s.get("item_container", "") or ""
                return mapping
            except Exception:
                continue
    return {}

def find_date_in_text(text):
    if not text:
        return None
    m = _DATE_RE.search(text)
    candidate = None
    if m:
        candidate = m.group(0)
    else:
        # tentativa adicional: usar dateutil fuzzy com todo o texto (mais custoso, mas último recurso)
        try:
            dt = dateparser.parse(text, fuzzy=True)
            if dt:
                return dt.isoformat(sep=' ')
        except Exception:
            pass
        return None

    # tenta parse com dateutil (fuzzy) para normalizar
    try:
        dt = dateparser.parse(candidate, fuzzy=True)
        if dt:
            return dt.isoformat(sep=' ')
    except Exception:
        return candidate
    return candidate

def find_date_from_xml_item(xml_soup, entry_title, entry_link):
    """
    Procura no XML (BeautifulSoup xml parser) um <item> que corresponda ao entry
    (por título ou link) e tenta extrair uma data no bloco <description> ou texto do item.
    """
    if xml_soup is None:
        return None
    for item in xml_soup.find_all('item'):
        t_el = item.find('title')
        d_el = item.find('description')
        link_el = item.find('link')
        t_text = t_el.get_text(" ", strip=True) if t_el else ''
        d_text = d_el.get_text(" ", strip=True) if d_el else ''
        l_text = link_el.get_text(" ", strip=True) if link_el else ''

        def norm(x): return (x or '').strip().lower()

        # match by link exact
        if entry_link and l_text and norm(entry_link) == norm(l_text):
            combined = ' '.join([t_text, d_text])
            found = find_date_in_text(combined)
            if found:
                return found

        # fuzzy match by title
        if entry_title and t_text:
            nt = norm(t_text)
            ne = norm(entry_title)
            if ne == nt or ne in nt or nt in ne:
                combined = ' '.join([t_text, d_text])
                found = find_date_in_text(combined)
                if found:
                    return found
    return None

# ---- novo: scraping moderno do modernhealthcare (replicar snippet JS) ----
def scrape_modernhealthcare_items(max_items=10, base_url='https://www.modernhealthcare.com/latest-news/'):
    """
    Tenta reproduzir em Python o comportamento do snippet JS usado no console.
    Retorna lista de dicts: { title, link, date, description }
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    try:
        r = requests.get(base_url, headers=headers, timeout=15)
        r.raise_for_status()
        html_text = r.text
    except Exception as e:
        print("scrape_modernhealthcare: fetch failed:", e)
        return []

    soup = BeautifulSoup(html_text, 'html.parser')

    # tentativa de "remover" modais/overlays estáticos do HTML
    modal_selectors = [
        'div[class*="subscribe"]', 'div[id*="subscribe"]', '.newsletter-popup', '.newsletter-modal',
        '.subscription-overlay', '.newsletter', '.modal', '.modal-backdrop', '[role="dialog"]'
    ]
    for sel in modal_selectors:
        for n in soup.select(sel):
            try:
                n.decompose()
            except Exception:
                try:
                    n.extract()
                except Exception:
                    pass

    # também tenta remover iframes com subscribe/eventos
    for f in soup.find_all('iframe'):
        src = (f.get('src') or '').lower()
        if ('subscribe' in src) or ('newsletter' in src) or ('events' in src):
            try:
                f.decompose()
            except Exception:
                pass

    main = soup.select_one('#main-content') or soup

    # encontrar title elements em ordem DOM: span.u-text-text-dark, etc.
    title_selectors = 'span.u-text-text-dark, a[aria-label^="Title"] span, .news-title.fs-5, .news-title'
    title_els = main.select(title_selectors)

    def txt(el):
        if not el:
            return ''
        return ' '.join(el.get_text(" ", strip=True).split())

    def is_bad_title(t):
        if not t: return True
        t0 = t.strip().lower()
        if len(t0) < 6: return True
        if t0 in ('category', 'healthcare news', 'latest news', 'image', 'subscribe', 'return to homepage'):
            return True
        return False

    def find_wrapper(el):
        cur = el
        for _ in range(8):
            if cur is None:
                break
            if cur.name == 'article':
                return cur
            classes = ' '.join(cur.get('class') or [])
            if any(c in classes for c in ('u-border-b', 'views-row', 'col-lg-6', 'square-one', 'view-content')):
                return cur
            cur = cur.parent
        # fallback: closest article or views-row
        a = el.find_parent(['article'])
        if a:
            return a
        v = el.find_parent(class_='views-row')
        if v:
            return v
        return el.parent or el

    def find_link(wrapper, title_el=None):
        # prefer anchor that wraps title_el
        if title_el:
            a = title_el.find_parent('a', href=True)
            if a:
                h = a.get('href') or ''
                if h and not re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I):
                    return urljoin(base_url, h)
        # try a.content-list-title[href], a[aria-label^="Title"], any a[href]
        for sel in ('a.content-list-title[href]', 'a[aria-label^="Title"]', 'a[href].overlay', '.content-list-title a[href]', 'a[href]'):
            a = wrapper.select_one(sel)
            if a and a.get('href'):
                h = a.get('href')
                if h and not re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I):
                    return urljoin(base_url, h)
        # fallback: any anchor
        a = wrapper.find('a', href=True)
        if a:
            h = a.get('href') or ''
            if h and not re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I):
                return urljoin(base_url, h)
        return ''

    def find_date(wrapper):
        # try group-author-line spans
        anc_article = wrapper.find_parent('article') or wrapper
        group = anc_article.select_one('div.group-author-line, div.field.field--name-field-author, .group-author-line')
        if group:
            spans = [txt(s) for s in group.select('span') if txt(s)]
            if len(spans) >= 2:
                # join reasonable spans, protect against 'subscribe' texts
                joined = ' | '.join(spans).strip()
                if joined and not re.search(r'subscribe|homepage|member', joined, re.I):
                    return re.sub(r'\s*\|\s*', ' | ', joined)
        # common date selectors
        for sel in ('.u-whitespace-nowrap', 'time', 'time[datetime]', '.date', '.timestamp', '.post-date', '.day_list', '.time_list'):
            el = wrapper.select_one(sel)
            if el:
                t = txt(el).lstrip('|').strip()
                if t and not re.search(r'subscribe|image', t, re.I):
                    return t
        # fallback: search for date-like text in wrapper text
        raw = txt(wrapper)
        m = re.search(r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
                      r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}(?:\s+\d{1,2}:\d{2}\s*(?:AM|PM))?', raw, re.I)
        if m:
            return m.group(0)
        return ''

    def find_description(wrapper):
        for sel in ('div.u-h-auto.u-w-full.u-font-secondary p', 'div.field.field--name-field-subheader.field--item', '.dek', '.summary', '.body_list', '.news-content p', '.content-list-meta + p', 'p'):
            el = wrapper.select_one(sel)
            if el:
                t = txt(el)
                if t and not re.search(r'subscribe|image', t, re.I):
                    return t
        # fallback: first paragraph
        p = wrapper.find('p')
        if p:
            t = txt(p)
            if t and not re.search(r'subscribe', t, re.I):
                return t
        return ''

    items = []
    seen = set()

    # iterate title elements in DOM order
    for el in title_els:
        ttext = txt(el)
        if is_bad_title(ttext):
            continue
        wrapper = find_wrapper(el)
        if not wrapper:
            continue
        link = find_link(wrapper, el)
        if link and re.search(r'(/subscribe|subscribe)', link, re.I):
            continue
        key = (link or ttext).rstrip('/')
        if not key or key in seen:
            continue
        date = find_date(wrapper) or ''
        desc = find_description(wrapper) or ''
        items.append({'title': ttext, 'link': link, 'date': date, 'description': desc})
        seen.add(key)
        if len(items) >= max_items:
            break

    # if not enough items, scan wrapper candidates
    if len(items) < max_items:
        candidates = main.select('article, .u-border-b, .views-row, .view-content > div, .col-lg-6') or []
        for wrapper in candidates:
            if len(items) >= max_items:
                break
            # try to extract title
            tsel = wrapper.select_one('span.u-text-text-dark, a[aria-label^="Title"] span, .news-title, .content-list-title a, a.title, h2, h3')
            ttext = txt(tsel) if tsel else ''
            if is_bad_title(ttext):
                continue
            link = find_link(wrapper, tsel)
            key = (link or ttext).rstrip('/')
            if not key or key in seen:
                continue
            date = find_date(wrapper) or ''
            desc = find_description(wrapper) or ''
            items.append({'title': ttext, 'link': link, 'date': date, 'description': desc})
            seen.add(key)

    # final anchor fallback to reach max
    if len(items) < max_items:
        for a in main.select('a[href]'):
            if len(items) >= max_items:
                break
            h = (a.get('href') or '').strip()
            if not h: continue
            if re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I): continue
            full = urljoin(base_url, h)
            if full in seen: continue
            t = txt(a)
            if not t or len(t) < 6: continue
            items.append({'title': t, 'link': full, 'date': '', 'description': ''})
            seen.add(full)

    return items[:max_items]

def parse_feed_file_with_fallback(ff):
    rows = []
    base = os.path.basename(ff)
    site_name = os.path.splitext(base)[0]
    parsed = feedparser.parse(ff)
    entries = parsed.entries if hasattr(parsed, "entries") else []

    raw_xml = ''
    try:
        raw_xml = open(ff, 'r', encoding='utf-8').read()
    except Exception:
        raw_xml = ''
    xml_soup = None
    if raw_xml:
        try:
            xml_soup = BeautifulSoup(raw_xml, 'xml')
        except Exception:
            xml_soup = None

    # special-case: modernhealthcare -> scrape the site and build rows from that (replica JS snippet)
    if site_name == 'modernhealthcare':
        print("parse_feed_file_with_fallback: detected modernhealthcare feed -> scraping live page for items")
        try:
            items = scrape_modernhealthcare_items(max_items=10)
            if items:
                for it in items:
                    rows.append({
                        "site": site_name,
                        "title": it.get('title',''),
                        "link (source)": it.get('link',''),
                        "pubDate": it.get('date',''),
                        "description (short)": strip_html_short(it.get('description','')),
                        "item_container": "",  # left blank; you can fill if needed
                        "topic": "N/A"
                    })
                return rows
            else:
                print("scrape_modernhealthcare_items returned no items, falling back to feed parsing.")
        except Exception as e:
            print("Error scraping modernhealthcare:", e)
            # fallback to feed parsing below

    # default behavior: parse feed entries with fallbacks
    for e in entries:
        title = (e.get("title", "") or "").strip()
        link = (e.get("link", "") or "")
        pub = (e.get("published", "") or e.get("pubDate", "") or e.get("updated", "") or "")
        desc = (e.get("summary", "") or e.get("description", "") or "")
        desc_short = strip_html_short(desc, max_len=300)

        # topic: tags/categories
        topic = "N/A"
        if e.get("tags"):
            try:
                t = e.get("tags")
                if isinstance(t, list) and len(t) > 0:
                    first = t[0]
                    if isinstance(first, dict) and first.get('term'):
                        topic = first.get('term')
                    elif isinstance(first, str):
                        topic = first
            except Exception:
                topic = "N/A"

        # fallback: check raw xml item
        if not pub and xml_soup:
            fallback = find_date_from_xml_item(xml_soup, title, link)
            if fallback:
                pub = fallback

        # second fallback: search in combined title/desc text
        if not pub:
            combined = " ".join([title or "", desc or ""])
            maybe = find_date_in_text(combined)
            if maybe:
                pub = maybe

        rows.append({
            "site": site_name,
            "title": title,
            "link (source)": link,
            "pubDate": pub,
            "description (short)": desc_short,
            "item_container": "",
            "topic": topic or "N/A"
        })
    return rows

def main():
    site_item_map = load_sites_item_container()
    all_rows = []
    feed_files = sorted(glob.glob(os.path.join(FEEDS_DIR, "*.xml")))
    if not feed_files:
        print("No feed files found in", FEEDS_DIR)
    for ff in feed_files:
        try:
            rows = parse_feed_file_with_fallback(ff)
            base = os.path.basename(ff)
            site_name = os.path.splitext(base)[0]
            ic = site_item_map.get(site_name, "")
            for r in rows:
                r["item_container"] = ic
            all_rows.extend(rows)
        except Exception as exc:
            print("Error parsing feed", ff, ":", exc)
    if not all_rows:
        print("No items found across feeds.")
    cols = ["site", "title", "link (source)", "pubDate", "description (short)", "item_container", "topic"]
    df = pd.DataFrame(all_rows, columns=cols)
    df.to_excel(OUT_XLSX, index=False)
    print(f"Wrote {OUT_XLSX} ({len(df)} rows)")

if __name__ == "__main__":
    main()
