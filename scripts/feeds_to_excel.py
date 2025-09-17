#!/usr/bin/env python3
# scripts/feeds_to_excel.py
# Requisitos: feedparser, pandas, beautifulsoup4, openpyxl, python-dateutil, requests

import os
import glob
import json
import feedparser
import pandas as pd
import html
import re
import requests
import warnings
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from dateutil import tz as date_tz
from urllib.parse import urljoin, urlparse

# evitar UnknownTimezoneWarning do dateutil (mensagem do runner)
try:
    from dateutil import _parser as _dateutil__parser
    UnknownTimezoneWarning = _dateutil__parser.UnknownTimezoneWarning
    warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)
except Exception:
    warnings.filterwarnings("ignore", message="tzname .* identified but not understood")

OUT_XLSX = "feeds_summary.xlsx"
FEEDS_DIR = "feeds"
SITES_JSON_PATHS = ["scripts/sites.json", "rss-feeds/scripts/sites.json", "sites.json"]

# --- adicionar mapping tzinfos básico para evitar UnknownTimezoneWarning ---
_DEFAULT_TZINFOS = {
    "ET": date_tz.gettz("America/New_York"),
    "CET": date_tz.gettz("Europe/Paris"),
    "GMT": date_tz.gettz("GMT"),
    "UTC": date_tz.gettz("UTC"),
}

# Regex para detectar datas comuns
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
            dt = dateparser.parse(text, fuzzy=True, tzinfos=_DEFAULT_TZINFOS)
            if dt:
                return dt.isoformat(sep=' ')
        except Exception:
            pass
        return None

    try:
        dt = dateparser.parse(candidate, fuzzy=True, tzinfos=_DEFAULT_TZINFOS)
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


def parse_feed_file_with_fallback(ff):
    """
    Mantém comportamento antigo para todos os sites *exceto* os que têm scrapers especiais.
    """
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

    for e in entries:
        title = (e.get("title", "") or "").strip()
        link = (e.get("link", "") or "")
        # defensive modernhealthcare filtering in case feed contains noisy items
        if title and title.lower().strip() in ("no title", "return to homepage"):
            continue
        if not title and (link.endswith("modernhealthcare.com") or link.rstrip('/') == "https://www.modernhealthcare.com"):
            continue
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


# ---------------------------
# SCRAPER específico MOBIHEALTH
# ---------------------------
def abs_url(href, base):
    try:
        return urljoin(base, href or '')
    except Exception:
        return href or ''


def text_of(el):
    try:
        return (el.get_text(" ", strip=True) if el else "").strip()
    except Exception:
        return ""


def scrape_mobihealth_listing(base_url="https://www.mobihealthnews.com/", max_items=11, timeout=10):
    """
    Faz fetch e devolve lista ordenada de items (title, link, date, description) até max_items.
    """
    try:
        r = requests.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print("scrape_mobihealth_listing: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, "html.parser")

    # localizar topContainer (várias alternativas)
    topContainer = soup.select_one('.views-element-container.block-views-blocktop-stories-news-grid-global') \
                   or soup.select_one('.block--mhn-top-stories-news-grid-global') \
                   or soup.select_one('.block-views-blocktop-stories-news-grid-global') \
                   or None

    if topContainer:
        topTitleEls = [el for el in topContainer.select('.news-title.fs-5, .news-title, .overlay .news-title') if el]
    else:
        topTitleEls = [el for el in soup.select('.news-title.fs-5, .news-title') if el]

    # find wrapper for top titles
    def find_top_wrapper(el):
        if el is None:
            return None
        cur = el
        for _ in range(8):
            if cur is None:
                break
            if cur.name == 'a' and cur.get('href'):
                return cur
            classes = cur.get('class') or []
            if 'col-lg-6' in classes or 'square-one' in classes or 'views-row' in classes or cur.name == 'article':
                return cur
            if topContainer is not None and cur.parent == topContainer:
                return cur
            cur = cur.parent
        art = el.find_parent(['article', 'div'], class_='views-row')
        if art:
            return art
        closest_a = el.find_parent('a')
        return closest_a or el

    topWrappers = []
    seen_top = set()
    for t in topTitleEls:
        w = find_top_wrapper(t)
        if not w:
            continue
        wid = id(w)
        if wid in seen_top:
            continue
        seen_top.add(wid)
        topWrappers.append((w, t))
        if len(topWrappers) >= 5:
            break

    sel_candidates = '#main-content .view-content > .view-content > div, #main-content .view-content > div, .view-content > div, .view-content .views-row, article, .views-row'
    candidates = [c for c in soup.select(sel_candidates)]

    topNodesSet = set([id(w) for (w, _) in topWrappers])

    regularNodes = []
    for n in candidates:
        in_top = False
        for tw in topWrappers:
            try:
                if tw[0] and tw[0].find_all and (n in tw[0].find_all(True) or n == tw[0]):
                    in_top = True
                    break
            except Exception:
                continue
        if in_top:
            continue
        if id(n) in topNodesSet:
            continue
        regularNodes.append(n)

    def find_link(wrap):
        if wrap is None:
            return ''
        if wrap.name == 'a' and wrap.get('href'):
            return abs_url(wrap.get('href'), base_url)
        sel_order = ['a.content-list-title[href]', 'a[href].overlay', 'a[href]', '.content-list-title a[href]']
        for s in sel_order:
            try:
                a = wrap.select_one(s)
            except Exception:
                a = None
            if a and a.get('href'):
                return abs_url(a.get('href'), base_url)
        any_a = wrap.select_one('a[href]')
        if any_a and any_a.get('href'):
            return abs_url(any_a.get('href'), base_url)
        return ''

    def find_description(wrap):
        if wrap is None:
            return ''
        sels = [
            'div.field.field--name-field-subheader.field--item',
            'div.body_list',
            '.content-list-meta + p',
            '.news-content p',
            '.dek',
            '.field--name-field-subheader',
            '.content-list-meta .field--item'
        ]
        for s in sels:
            try:
                el = wrap.select_one(s)
            except Exception:
                el = None
            if el:
                t = text_of(el)
                if t:
                    return t
        p = wrap.select_one('p')
        if p:
            return text_of(p)
        return ''

    def find_date(wrap):
        if wrap is None:
            return ''
        ancArticle = wrap.find_parent('article') or wrap
        try:
            group = ancArticle.select_one('div.group-author-line, div.field.field--name-field-author')
        except Exception:
            group = None
        if group:
            spans = [text_of(s) for s in group.select('span') if text_of(s)]
            if len(spans) >= 6:
                day = spans[4].lstrip('|').strip()
                time = spans[5].lstrip('|').strip()
                if day and time and day != time:
                    return f"{day} | {time}"
                if day:
                    return day
        dayEl = wrap.select_one('span.day_list, .day_list, span.post-date, .post-date, time')
        timeEl = wrap.select_one('span.time_list, .time_list, time')
        day = text_of(dayEl) if dayEl is not None else ''
        time = text_of(timeEl) if timeEl is not None else ''
        if day:
            day = day.lstrip('|').strip()
        if time:
            time = time.lstrip('|').strip()
        if day and time and day != time:
            return f"{day} | {time}"
        if day:
            return day
        if time:
            return time
        anyTime = wrap.select_one('time, .timestamp, .date')
        if anyTime:
            return text_of(anyTime).lstrip('|').strip()
        return ''

    def find_title(wrapper, knownTitleEl=None):
        if knownTitleEl is not None and text_of(knownTitleEl):
            return text_of(knownTitleEl)
        sels = ['.news-title.fs-5', '.news-title', '.content-list-title a', 'a.title', '.content-list-title', 'h2', 'h3', 'h4']
        for s in sels:
            try:
                el = wrapper.select_one(s)
            except Exception:
                el = None
            if el and text_of(el):
                return text_of(el)
        a = wrapper.select_one('a[href]')
        if a and text_of(a):
            return text_of(a)
        return ''

    items = []
    seen = set()

    def push_if_new(obj):
        key = (obj.get('link') or '').rstrip('/') or (obj.get('title') or '')[:200]
        if not key:
            return False
        if key in seen:
            return False
        seen.add(key)
        items.append(obj)
        return True

    # top wrappers
    for (w, titleEl) in topWrappers:
        if len(items) >= max_items:
            break
        title = find_title(w, titleEl) or ''
        link = find_link(w) or ''
        date = find_date(w) or ''
        description = find_description(w) or ''
        push_if_new({'title': title, 'link': link, 'date': date, 'description': description, 'source': 'top'})

    # regular nodes
    for n in regularNodes:
        if len(items) >= max_items:
            break
        title = find_title(n) or ''
        link = find_link(n) or ''
        date = find_date(n) or ''
        description = find_description(n) or ''
        push_if_new({'title': title, 'link': link, 'date': date, 'description': description, 'source': 'list'})

    # fallback anchors
    if len(items) < max_items:
        for a in soup.select('a[href]'):
            if len(items) >= max_items:
                break
            href = abs_url(a.get('href'), base_url)
            if not href or href in seen:
                continue
            title = text_of(a) or ''
            if not title:
                continue
            push_if_new({'title': title, 'link': href, 'date': '', 'description': '', 'source': 'anchor-fallback'})

    return items


# ---------------------------
# SCRAPERS específicos MEDTECHDIVE
# ---------------------------
def scrape_medtech_home(base_url="https://www.medtechdive.com/", timeout=10):
    """
    Extrai o 'hero' article (um item) da homepage.
    """
    try:
        r = requests.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print("scrape_medtech_home: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, "html.parser")

    titleEl = soup.select_one("#hero-item-title > a") or soup.select_one(".hero .hero-title a") or soup.select_one("a.analytics.t-dash-hero-title")
    descEl = soup.select_one("#skip-link-target > section > div > div > div > div:nth-child(1) > section > p") \
             or soup.select_one(".hero-article__teaser") \
             or soup.select_one(".hero .dek, .hero p")

    def clean_text(node):
        if not node:
            return ""
        tmp = BeautifulSoup(str(node), "html.parser")
        for a in tmp.select("a"):
            a.extract()
        return tmp.get_text(" ", strip=True)

    title = titleEl.get_text(" ", strip=True) if titleEl else ""
    link = urljoin(base_url, titleEl.get("href")) if titleEl and titleEl.get("href") else ""
    description = clean_text(descEl) if descEl else ""
    date = ""  # hero summary frequentemente não tem data

    return [{"title": title, "link": link, "date": date, "description": description, "source": "hero"}]


def scrape_medtech_topic(base_url="https://www.medtechdive.com/topic/medical-devices/", max_items=7, timeout=10):
    """
    Extrai primeiros max_items do topic medical-devices.
    """
    try:
        r = requests.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print("scrape_medtech_topic: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, "html.parser")

    list_root = soup.select_one('#main-content > ul') or soup.select_one('#main-content ul') or soup.select_one('#main-content')
    if not list_root:
        candidates = soup.select('ul li, article, .feed__item, .result')
    else:
        if list_root.name == 'ul':
            candidates = list_root.select('> li') if list_root.select('> li') else list_root.find_all('li')
        else:
            candidates = list_root.select('li') or list_root.select('article') or list_root.select('.feed__item')

    items = []
    seen = set()
    for node in candidates:
        if len(items) >= max_items:
            break
        titleEl = node.select_one('div.medium-8.columns > h3 > a') or node.select_one('h3 a') or node.select_one('a')
        descEl = node.select_one('div.medium-8.columns > p') or node.select_one('p.feed__description') or node.select_one('p')

        if not titleEl:
            continue
        title = titleEl.get_text(" ", strip=True)
        href = titleEl.get("href") or ""
        link = urljoin(base_url, href)
        desc_html = ""
        try:
            tmp = BeautifulSoup(str(descEl), "html.parser") if descEl else None
            if tmp:
                for a in tmp.select("a"):
                    a.extract()
                desc_html = tmp.get_text(" ", strip=True)
        except Exception:
            desc_html = descEl.get_text(" ", strip=True) if descEl else ""

        key = (link or title).rstrip('/')
        if not key or key in seen:
            continue
        seen.add(key)
        items.append({"title": title, "link": link, "date": "", "description": desc_html, "source": "topic-list"})

    return items


# ---------------------------
# SCRAPER específico MODERNHEALTHCARE (rendered HTML)
# ---------------------------
def scrape_modern_rendered(rendered_path, base_url="https://www.modernhealthcare.com/latest-news/", max_items=10):
    """
    Parse a rendered modernhealthcare HTML and return a list of items.
    """
    def txt(el):
        try:
            return (el.get_text(" ", strip=True) if el else "").strip()
        except Exception:
            return ""

    badHrefRe = re.compile(r'(^#|^javascript:|mailto:|/help|/legal|cookie|privacy|terms|signin|login|settings|/consent|/preferences|/policies|/subscribe)', re.I)
    blacklistTitle = [re.compile(r'^\s*category\s*$', re.I), re.compile(r'^\s*healthcare news\s*$', re.I),
                      re.compile(r'^\s*latest news\s*$', re.I), re.compile(r'^\s*image\s*$', re.I),
                      re.compile(r'^\s*read more\s*$', re.I)]

    try:
        raw = open(rendered_path, 'r', encoding='utf-8').read()
    except Exception as e:
        print("scrape_modern_rendered: failed to read rendered_path:", e)
        return []

    soup = BeautifulSoup(raw, 'html.parser')
    main = soup.select_one('#main-content') or soup

    titleEls = list(main.select('span.u-text-text-dark, a[aria-label^="Title"] span, .news-title.fs-5, .news-title'))
    items = []
    seen = set()

    def is_bad_title(t):
        if not t: return True
        if len(t.strip()) < 6: return True
        for re_ in blacklistTitle:
            if re_.match(t): return True
        if re.match(r'^(category|image|home|latest|subscribe|return)$', t.strip(), re.I):
            return True
        return False

    def find_wrapper(el):
        cur = el
        for _ in range(8):
            if cur is None: break
            if cur.name == 'article':
                return cur
            classes = cur.get('class') or []
            if any(c in ('u-border-b', 'views-row', 'col-lg-6', 'square-one', 'view-content') for c in classes):
                return cur
            cur = cur.parent
        return el.closest('article, .views-row, .u-border-b, .col-lg-6, .square-one') or el

    def abs_href(h):
        try:
            return urljoin(base_url, h or '')
        except Exception:
            return (h or '').strip()

    def find_link(wrapper, titleEl):
        if not wrapper: return ''
        if titleEl:
            a = titleEl.find_parent('a')
            if a and a.has_attr('href'):
                h = a.get('href') or ''
                if h and not badHrefRe.search(h): return abs_href(h)
        order = ['a.content-list-title[href]', 'a[aria-label^="Title"]', 'a[href].overlay', '.content-list-title a[href]', 'a[href]']
        for sel in order:
            a = wrapper.select_one(sel)
            if a and a.has_attr('href'):
                h = a.get('href') or ''
                if h and not badHrefRe.search(h): return abs_href(h)
        anyA = wrapper.select_one('a[href]')
        if anyA:
            h = anyA.get('href') or ''
            if h and not badHrefRe.search(h): return abs_href(h)
        return ''

    def find_date(wrapper):
        if not wrapper: return ''
        cand = wrapper.select_one('.u-whitespace-nowrap, time, time[datetime], .date, .timestamp, .post-date, .day_list, .time_list')
        if cand:
            t = txt(cand).lstrip('|').strip()
            if t and 'subscribe' not in t.lower():
                return t
        m = re.search(r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}', txt(wrapper))
        if m:
            return m.group(0)
        return ''

    def find_description(wrapper):
        if not wrapper: return ''
        for sel in ['div.u-h-auto.u-w-full.u-font-secondary p', 'div.field.field--name-field-subheader.field--item', '.dek', '.summary', '.news-content p', '.content-list-meta + p', 'p']:
            el = wrapper.select_one(sel)
            if el:
                t = txt(el)
                if t and 'subscribe' not in t.lower():
                    return t
        return ''

    for el in titleEls:
        try:
            title_text = txt(el)
            if is_bad_title(title_text):
                continue
            wrapper = find_wrapper(el)
            if not wrapper:
                continue
            link = find_link(wrapper, el)
            if badHrefRe.search(link):
                continue
            key = (link or title_text).rstrip('/')
            if not key or key in seen:
                continue
            seen.add(key)
            date = find_date(wrapper) or ''
            desc = find_description(wrapper) or ''
            items.append({'title': title_text, 'link': link, 'date': date, 'description': desc, 'source': 'rendered'})
            if len(items) >= max_items:
                break
        except Exception:
            continue

    if len(items) < max_items:
        for a in main.select('a[href]'):
            if len(items) >= max_items: break
            h = a.get('href') or ''
            abs_h = abs_href(h)
            t = txt(a)
            if not t or len(t) < 6: continue
            if badHrefRe.search(abs_h): continue
            key = abs_h.rstrip('/')
            if key in seen: continue
            seen.add(key)
            items.append({'title': t, 'link': abs_h, 'date': '', 'description': '', 'source': 'anchor-fallback'})

    out = []
    for it in items:
        out.append({
            'title': (it.get('title') or '').strip(),
            'link': (it.get('link') or '').strip(),
            'date': (it.get('date') or '').strip(),
            'description': (it.get('description') or '').strip(),
            'source': it.get('source', '')
        })
    print(f"scrape_modern_rendered: found {len(out)} items from {rendered_path}")
    return out


# ---------------------------
# FIM DO SCRAPERS
# ---------------------------

def main():
    site_item_map = load_sites_item_container()
    all_rows = []

    # --- special: include modernhealthcare rendered HTML (se existir) no topo ---
    mh_rendered_path = os.path.join("scripts", "rendered", "modernhealthcare.html")
    if os.path.exists(mh_rendered_path):
        try:
            mh_items = scrape_modern_rendered(mh_rendered_path, base_url="https://www.modernhealthcare.com/latest-news/", max_items=10)
            for it in mh_items:
                all_rows.append({
                    "site": "modernhealthcare",
                    "title": it.get("title", "") or "",
                    "link (source)": it.get("link", "") or "",
                    "pubDate": it.get("date", "") or "",
                    "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                    "item_container": site_item_map.get("modernhealthcare", ""),
                    "topic": "N/A"
                })
            print(f"Added {len(mh_items)} modernhealthcare items from rendered HTML (preload)")
        except Exception as e:
            print("Error scraping modernhealthcare rendered html:", e)

    feed_files = sorted(glob.glob(os.path.join(FEEDS_DIR, "*.xml")))
    if not feed_files:
        print("No feed files found in", FEEDS_DIR)

    for ff in feed_files:
        try:
            base = os.path.basename(ff)
            site_name = os.path.splitext(base)[0]
            ic = site_item_map.get(site_name, "")

            # special: modernhealthcare - prefer rendered HTML (if present)
            if site_name == "modernhealthcare":
                rendered_path = os.path.join('scripts', 'rendered', 'modernhealthcare.html')
                try:
                    if os.path.exists(rendered_path):
                        mh_items = scrape_modern_rendered(rendered_path, base_url="https://www.modernhealthcare.com/latest-news/", max_items=11)
                        for it in mh_items:
                            t = it.get('title', '').strip()
                            link = it.get('link', '').strip()
                            if not t or t.lower() in ("no title", "return to homepage", "category"):
                                continue
                            all_rows.append({
                                "site": site_name,
                                "title": t,
                                "link (source)": link,
                                "pubDate": it.get('date', ''),
                                "description (short)": strip_html_short(it.get('description', ''), max_len=300),
                                "item_container": ic,
                                "topic": "N/A"
                            })
                        # skip parsing XML feed for this site
                        print(f"Using rendered HTML for {site_name}: added {len(mh_items)} items")
                        continue
                except Exception as e:
                    print("Error scraping modernhealthcare rendered html (per-file):", e)
                    # fallthrough to XML parsing fallback below

            # special: medtechdive homepage (hero)
            if site_name == "medtechdive":
                med_items = scrape_medtech_home(base_url="https://www.medtechdive.com/", timeout=10)
                for it in med_items:
                    all_rows.append({
                        "site": site_name,
                        "title": it.get("title", "") or "",
                        "link (source)": it.get("link", "") or "",
                        "pubDate": it.get("date", "") or "",
                        "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                        "item_container": ic,
                        "topic": "N/A"
                    })
                print(f"Added {len(med_items)} medtechdive (home hero) items")
                continue

            # special: medtechdive topic medical-devices (first 7)
            if site_name == "medtechdive-devices":
                med_items = scrape_medtech_topic(base_url="https://www.medtechdive.com/topic/medical-devices/", max_items=7, timeout=10)
                for it in med_items:
                    all_rows.append({
                        "site": site_name,
                        "title": it.get("title", "") or "",
                        "link (source)": it.get("link", "") or "",
                        "pubDate": it.get("date", "") or "",
                        "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                        "item_container": ic,
                        "topic": "N/A"
                    })
                print(f"Added {len(med_items)} medtechdive-devices items")
                continue

            # special: if mobihealthnews, ignore the XML entries and build rows directly
            if site_name == "mobihealthnews":
                mobi_items = scrape_mobihealth_listing(base_url="https://www.mobihealthnews.com/", max_items=11, timeout=10)
                for it in mobi_items:
                    rows = {
                        "site": site_name,
                        "title": it.get("title", "") or "",
                        "link (source)": it.get("link", "") or "",
                        "pubDate": it.get("date", "") or "",
                        "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                        "item_container": ic,
                        "topic": "N/A"
                    }
                    all_rows.append(rows)
                print(f"Added {len(mobi_items)} mobihealthnews items (scraped live)")
                continue

            # otherwise, behavior unchanged
            rows = parse_feed_file_with_fallback(ff)
            for r in rows:
                r["item_container"] = ic
                all_rows.append(r)

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
