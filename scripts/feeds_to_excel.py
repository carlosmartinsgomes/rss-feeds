#!/usr/bin/env python3
# scripts/feeds_to_excel.py
# Requisitos: feedparser, pandas, beautifulsoup4, openpyxl, python-dateutil, requests
#
# Função: lê feeds/*.xml, converte para .xlsx. Para dois sites específicos
# (mobihealthnews, modernhealthcare) tenta extrair diretamente da página live
# usando BeautifulSoup para reproduzir os snippets JS que testaste no console.

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
        try:
            dt = dateparser.parse(text, fuzzy=True)
            if dt:
                return dt.isoformat(sep=' ')
        except Exception:
            pass
        return None
    try:
        dt = dateparser.parse(candidate, fuzzy=True)
        if dt:
            return dt.isoformat(sep=' ')
    except Exception:
        return candidate
    return candidate

def find_date_from_xml_item(xml_soup, entry_title, entry_link):
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
        if entry_link and l_text and norm(entry_link) == norm(l_text):
            combined = ' '.join([t_text, d_text])
            found = find_date_in_text(combined)
            if found:
                return found
        if entry_title and t_text:
            nt = norm(t_text)
            ne = norm(entry_title)
            if ne == nt or ne in nt or nt in ne:
                combined = ' '.join([t_text, d_text])
                found = find_date_in_text(combined)
                if found:
                    return found
    return None

# ----------------- site-specific scrapers -----------------

def _safe_get(url, timeout=12):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text

def scrape_modernhealthcare_items(max_items=10, base_url='https://www.modernhealthcare.com/latest-news/'):
    try:
        html = _safe_get(base_url)
    except Exception as e:
        print("modernhealthcare: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, 'html.parser')

    # try to remove obvious modals/subscribe containers present in static HTML
    for sel in ['div[class*="subscribe"]', '.newsletter-popup', '.modal', '[role="dialog"]']:
        for n in soup.select(sel):
            try: n.decompose()
            except: pass

    main = soup.select_one('#main-content') or soup

    def txt(el):
        if not el: return ''
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
            if cur is None: break
            if cur.name == 'article': return cur
            classes = ' '.join(cur.get('class') or [])
            if any(c in classes for c in ('u-border-b', 'views-row', 'col-lg-6', 'square-one', 'view-content')):
                return cur
            cur = cur.parent
        a = el.find_parent(['article'])
        if a: return a
        v = el.find_parent(class_='views-row')
        if v: return v
        return el.parent or el

    def find_link(wrapper, title_el=None):
        if title_el:
            a = title_el.find_parent('a', href=True)
            if a:
                h = a.get('href') or ''
                if h and not re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I):
                    return urljoin(base_url, h)
        for sel in ('a.content-list-title[href]', 'a[aria-label^="Title"]', 'a[href].overlay', '.content-list-title a[href]', 'a[href]'):
            a = wrapper.select_one(sel)
            if a and a.get('href'):
                h = a.get('href')
                if h and not re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I):
                    return urljoin(base_url, h)
        a = wrapper.find('a', href=True)
        if a:
            h = a.get('href') or ''
            if h and not re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I):
                return urljoin(base_url, h)
        return ''

    def find_date(wrapper):
        anc_article = wrapper.find_parent('article') or wrapper
        group = anc_article.select_one('div.group-author-line, div.field.field--name-field-author, .group-author-line')
        if group:
            spans = [txt(s) for s in group.select('span') if txt(s)]
            if len(spans) >= 2:
                joined = ' | '.join(spans).strip()
                if joined and not re.search(r'subscribe|homepage|member', joined, re.I):
                    return re.sub(r'\s*\|\s*', ' | ', joined)
        for sel in ('.u-whitespace-nowrap', 'time', '.date', '.timestamp', '.post-date', '.day_list', '.time_list'):
            el = wrapper.select_one(sel)
            if el:
                t = txt(el).lstrip('|').strip()
                if t and not re.search(r'subscribe|image', t, re.I):
                    return t
        raw = txt(wrapper)
        m = re.search(r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
                      r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}(?:\s+\d{1,2}:\d{2}\s*(?:AM|PM))?', raw, re.I)
        if m: return m.group(0)
        return ''

    def find_description(wrapper):
        for sel in ('div.u-h-auto.u-w-full.u-font-secondary p', 'div.field.field--name-field-subheader.field--item', '.dek', '.summary', '.body_list', '.news-content p', '.content-list-meta + p', 'p'):
            el = wrapper.select_one(sel)
            if el:
                t = txt(el)
                if t and not re.search(r'subscribe|image', t, re.I):
                    return t
        p = wrapper.find('p')
        if p:
            t = txt(p)
            if t and not re.search(r'subscribe', t, re.I):
                return t
        return ''

    title_els = main.select('span.u-text-text-dark, a[aria-label^="Title"] span, .news-title.fs-5, .news-title')
    items = []
    seen = set()

    for el in title_els:
        ttext = txt(el)
        if is_bad_title(ttext): continue
        wrapper = find_wrapper(el)
        if not wrapper: continue
        link = find_link(wrapper, el)
        if link and re.search(r'(/subscribe|subscribe)', link, re.I): continue
        key = (link or ttext).rstrip('/')
        if not key or key in seen: continue
        date = find_date(wrapper) or ''
        desc = find_description(wrapper) or ''
        items.append({'title': ttext, 'link': link, 'date': date, 'description': desc})
        seen.add(key)
        if len(items) >= max_items: break

    if len(items) < max_items:
        candidates = main.select('article, .u-border-b, .views-row, .view-content > div, .col-lg-6') or []
        for wrapper in candidates:
            if len(items) >= max_items: break
            tsel = wrapper.select_one('span.u-text-text-dark, a[aria-label^="Title"] span, .news-title, .content-list-title a, a.title, h2, h3')
            ttext = txt(tsel) if tsel else ''
            if is_bad_title(ttext): continue
            link = find_link(wrapper, tsel)
            key = (link or ttext).rstrip('/')
            if not key or key in seen: continue
            date = find_date(wrapper) or ''
            desc = find_description(wrapper) or ''
            items.append({'title': ttext, 'link': link, 'date': date, 'description': desc})
            seen.add(key)

    # anchor fallback
    if len(items) < max_items:
        for a in main.select('a[href]'):
            if len(items) >= max_items: break
            h = (a.get('href') or '').strip()
            if not h or re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I): continue
            full = urljoin(base_url, h)
            if full in seen: continue
            t = txt(a)
            if not t or len(t) < 6: continue
            items.append({'title': t, 'link': full, 'date': '', 'description': ''})
            seen.add(full)

    return items[:max_items]

def scrape_mobihealth_items(max_items=11, base_url='https://www.mobihealthnews.com/'):
    try:
        html = _safe_get(base_url)
    except Exception as e:
        print("mobihealthnews: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, 'html.parser')
    # remove obvious forms/modals
    for sel in ['.mktoForm', '.newsletter-popup', '.modal', '[role="dialog"]']:
        for n in soup.select(sel):
            try: n.decompose()
            except: pass

    main = soup.select_one('#main-content') or soup

    def txt(el):
        if not el: return ''
        return ' '.join(el.get_text(" ", strip=True).split())

    def find_top_wrappers():
        top = (main.select_one('.views-element-container.block-views-blocktop-stories-news-grid-global')
               or main.select_one('.block--mhn-top-stories-news-grid-global')
               or main.select_one('.block-views-blocktop-stories-news-grid-global')
               or None)
        if top:
            els = top.select('.news-title.fs-5, .news-title, .overlay .news-title, a > .news-title')
            wrappers = []
            seen = set()
            for t in els:
                # climb to a wrapper that is meaningful
                cur = t
                for _ in range(8):
                    if not cur: break
                    if cur.name == 'a' and cur.get('href'): break
                    classes = ' '.join(cur.get('class') or [])
                    if any(c in classes for c in ('col-lg-6', 'square-one', 'views-row', 'article')):
                        break
                    cur = cur.parent
                wrap = cur or t
                if wrap in seen: continue
                seen.add(wrap)
                wrappers.append((wrap, t))
                if len(wrappers) >= 5: break
            return wrappers
        return []

    def find_wrapper_for_list_item(el):
        # climb until views-row or article
        cur = el
        for _ in range(6):
            if not cur: break
            if cur.name == 'article': return cur
            if 'views-row' in (cur.get('class') or []): return cur
            cur = cur.parent
        return el

    def find_link(wrap):
        # prefer content-list-title anchor, else first anchor
        for sel in ('a.content-list-title[href]', 'a[href].overlay', '.content-list-title a[href]', 'a[href]'):
            a = wrap.select_one(sel)
            if a and a.get('href'):
                h = a.get('href')
                if h and not re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I):
                    return urljoin(base_url, h)
        return ''

    def find_date(wrap):
        # group-author-line spans or day_list/time_list
        group = wrap.select_one('div.group-author-line, div.field.field--name-field-author')
        if group:
            spans = [txt(s) for s in group.select('span') if txt(s)]
            if len(spans) >= 2:
                # often format has '|' separators
                joined = ' | '.join(spans).strip()
                return re.sub(r'\s*\|\s*', ' | ', joined)
        for sel in ('span.post-date', 'span.day_list', 'span.time_list', 'time'):
            el = wrap.select_one(sel)
            if el:
                t = txt(el).lstrip('|').strip()
                if t: return t
        raw = txt(wrap)
        m = re.search(r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
                      r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}', raw, re.I)
        if m: return m.group(0)
        return ''

    def find_description(wrap):
        for sel in ('div.field.field--name-field-subheader.field--item', 'div.body_list', '.content-list-meta + p', '.news-content p', '.dek', 'p'):
            el = wrap.select_one(sel)
            if el:
                t = txt(el)
                if t: return t
        return ''

    items = []
    seen = set()

    # top wrappers first
    top_wrappers = find_top_wrappers()
    for wrap, title_el in top_wrappers:
        if len(items) >= max_items: break
        title = txt(title_el) or ''
        if not title or len(title) < 4: continue
        wrapper = find_wrapper_for_list_item(wrap)
        link = find_link(wrapper) or ''
        if link and re.search(r'(/subscribe|/home)', link, re.I): continue
        key = (link or title).rstrip('/')
        if key in seen: continue
        date = find_date(wrapper) or ''
        desc = find_description(wrapper) or ''
        items.append({'title': title, 'link': link, 'date': date, 'description': desc})
        seen.add(key)

    # regular list items
    if len(items) < max_items:
        list_nodes = main.select('div.view-content > div, .view-content > div, .views-row, article') or []
        for n in list_nodes:
            if len(items) >= max_items: break
            wrapper = find_wrapper_for_list_item(n)
            title_el = wrapper.select_one('.news-title, a.title, .content-list-title a, a > .news-title, .news-title.fs-5')
            title = txt(title_el) if title_el else txt(wrapper.select_one('a'))
            if not title or len(title) < 4: continue
            link = find_link(wrapper) or ''
            key = (link or title).rstrip('/')
            if key in seen: continue
            date = find_date(wrapper) or ''
            desc = find_description(wrapper) or ''
            items.append({'title': title, 'link': link, 'date': date, 'description': desc})
            seen.add(key)

    # final anchor fallback
    if len(items) < max_items:
        for a in main.select('a[href]'):
            if len(items) >= max_items: break
            h = (a.get('href') or '').strip()
            if not h or re.search(r'(^#|/subscribe|mailto:|javascript:)', h, re.I): continue
            full = urljoin(base_url, h)
            if full in seen: continue
            t = txt(a)
            if not t or len(t) < 6: continue
            items.append({'title': t, 'link': full, 'date': '', 'description': ''})
            seen.add(full)

    return items[:max_items]

# ----------------- end site-specific scrapers -----------------

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

    # site-specific overrides: try scrapers that mimic the console snippets
    if site_name == 'modernhealthcare':
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
                        "item_container": "",
                        "topic": "N/A"
                    })
                return rows
        except Exception as e:
            print("modernhealthcare scraper error:", e)

    if site_name == 'mobihealthnews':
        try:
            items = scrape_mobihealth_items(max_items=11)
            if items:
                for it in items:
                    rows.append({
                        "site": site_name,
                        "title": it.get('title',''),
                        "link (source)": it.get('link',''),
                        "pubDate": it.get('date',''),
                        "description (short)": strip_html_short(it.get('description','')),
                        "item_container": "",
                        "topic": "N/A"
                    })
                return rows
        except Exception as e:
            print("mobihealthnews scraper error:", e)

    # default: read feed entries (fallback)
    for e in entries:
        title = (e.get("title", "") or "").strip()
        link = (e.get("link", "") or "")
        pub = (e.get("published", "") or e.get("pubDate", "") or e.get("updated", "") or "")
        desc = (e.get("summary", "") or e.get("description", "") or "")
        desc_short = strip_html_short(desc, max_len=300)

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

        if not pub and xml_soup:
            fallback = find_date_from_xml_item(xml_soup, title, link)
            if fallback:
                pub = fallback

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
