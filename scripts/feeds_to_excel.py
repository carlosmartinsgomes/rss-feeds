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
import re as _re
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
    
ROOT = os.path.dirname(os.path.abspath(__file__))  # scripts/ (ajusta se o ficheiro estiver noutro local)

OUT_XLSX = "feeds_summary.xlsx"
FEEDS_DIR = "feeds"
# procuramos sites.json em vários lugares (mantém compatibilidade com CI / runner)
SITES_JSON_PATHS = [
    os.path.join('scripts', 'sites.json'),
    os.path.join('rss-feeds', 'scripts', 'sites.json'),
    'sites.json'
]

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
    """
    Retorna mapping site_name -> item_container (usado no feeds_to_excel para preencher item_container).
    Procura sites.json nas paths SITES_JSON_PATHS.
    """
    for p in SITES_JSON_PATHS:
        try:
            if not os.path.exists(p):
                continue
            with open(p, 'r', encoding='utf-8') as fh:
                raw = fh.read()
            obj = json.loads(raw)
            sites = obj.get('sites', obj if isinstance(obj, list) else [])
            mapping = {}
            for s in sites:
                if not isinstance(s, dict):
                    continue
                name = s.get('name')
                if not name:
                    continue
                mapping[name] = s.get('item_container', '') or ''
            return mapping
        except Exception:
            # continua para o próximo path
            continue
    return {}

def load_sites_config_map(sites_json_path=None):
    """
    Carrega o sites.json e devolve dict { site_name: site_cfg }.
    Se sites_json_path for None, tenta as SITES_JSON_PATHS.
    """
    paths = [sites_json_path] if sites_json_path else SITES_JSON_PATHS
    for p in paths:
        try:
            if not p or not os.path.exists(p):
                continue
            with open(p, 'r', encoding='utf-8') as fh:
                j = json.load(fh)
            sites = j.get('sites', []) if isinstance(j, dict) else j
            cfg_map = {s.get('name'): s for s in sites if isinstance(s, dict) and s.get('name')}
            return cfg_map
        except Exception:
            continue
    return {}

# carrega o mapa de configuração uma vez (usa ROOT se necessário)
# tenta primeiro os paths relativos já definidos; manter compatibilidade com código que usa 'ROOT'
try:
    SITES_CFG_MAP = load_sites_config_map()
except Exception:
    SITES_CFG_MAP = {}

def matches_filters_for_row(row, site_cfg):
    """
    Mesmo comportamento do generate_feeds.matches_filters_debug,
    mas recebe row (dict) e site_cfg (dict). Retorna matched_reason or None.
    """
    if not site_cfg:
        return None
    kw_list = site_cfg.get('filters', {}).get('keywords', []) or []
    exclude_list = site_cfg.get('filters', {}).get('exclude', []) or []
    if not kw_list and not exclude_list:
        return None  # sem filtros -> sinal para aceitar tudo

    # prepara os textos
    text_map = {
        'title': (row.get('title','') or '').lower(),
        'description': (row.get('description','') or row.get('description (short)','') or '').lower(),
        'full_text': (row.get('full_text','') or '').lower(),
        'link': (row.get('link (source)','') or row.get('link','') or '').lower(),
        'topic': (row.get('topic','') or '').lower()
    }

    # keywords
    if kw_list:
        for k in kw_list:
            if not k: continue
            kl = str(k).lower()
            for field in ('title','description','full_text','link','topic'):
                if kl in text_map.get(field,''):
                    return f"{kl}@{field}"
        return None

    # excludes
    for ex in exclude_list:
        if not ex: continue
        el = str(ex).lower()
        for field in ('title','description','full_text','link','topic'):
            if el in text_map.get(field,''):
                return f"exclude:{el}@{field}"
    return None


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

# --- helper: detectar URLs / hosts dentro do ficheiro de feed (para acionar scrapers especiais) ---
def detect_profile_url_in_feed_file(feedfile_path, host_substrings):
    """
    Tenta detectar uma URL do tipo 'profile' (ou um host) dentro do ficheiro de feed.
    host_substrings: lista de substrings a procurar, ex: ['thedrum.com/profile', 'semiengineering.com']
    Retorna a primeira URL encontrada (string absoluta) ou None.
    """
    # 1) tentar feedparser rápido
    try:
        parsed = feedparser.parse(feedfile_path)
        # tentar feed.link
        feed_link = getattr(parsed, 'feed', {}).get('link') if getattr(parsed, 'feed', None) else None
        if feed_link:
            for s in host_substrings:
                if s in (feed_link or ''):
                    return feed_link
        # tentar primeira entry
        entries = getattr(parsed, 'entries', None) or []
        if entries:
            first = entries[0]
            for k in ('link','id','guid'):
                v = first.get(k) if isinstance(first, dict) else None
                if v:
                    for s in host_substrings:
                        if s in (v or ''):
                            return v
    except Exception:
        pass

    # 2) abrir ficheiro raw e procurar substrings / urls simples
    try:
        raw = open(feedfile_path, 'r', encoding='utf-8', errors='ignore').read()
        # procura http(s) urls contendo as substrings
        for s in host_substrings:
            if s in raw:
                # tentar extrair a primeira http... que contenha s
                m = re.search(r'https?://[^"\'>\s]*' + re.escape(s.split(s.split('/')[-1])[-1]) + r'[^"\'>\s]*', raw)
                if m:
                    return m.group(0)
                # fallback: devolver apenas a substring encontrada (o chamador pode normalizar)
                return s
    except Exception:
        pass
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
        # fallback amplo se não encontrarmos o nó esperado
        candidates = soup.select('ul li, article, .feed__item, .result')
    else:
        if list_root.name == 'ul':
            # obter os <li> filhos directos (sem usar selectors que comecem por '>')
            candidates = list_root.find_all('li', recursive=False)
            # se não houver filhos directos, aceitar qualquer li dentro do nó
            if not candidates:
                candidates = list_root.find_all('li')
        else:
            # procurar itens dentro do nó (mais genérico)
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
# SCRAPER específico MEDIAPOST (novo)
# (mantive o teu código mediapost tal como o tinhas)
# ---------------------------

def scrape_mediapost_listing(base_url="https://www.mediapost.com/news/", max_items=30, timeout=10):
    """
    Fetch the Mediapost /news/ page and return items list with fields:
    {title, link, date, description, source}
    Heuristics mirror the console snippet you tested.
    """
    try:
        r = requests.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print("scrape_mediapost_listing: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, "html.parser")

    def txt(el):
        try:
            return (el.get_text(" ", strip=True) if el else "").strip()
        except Exception:
            return ""

    def is_in_header_footer(el):
        if not el:
            return False
        cur = el
        for _ in range(8):
            if cur is None:
                break
            tag = getattr(cur, 'name', '').lower()
            cls = " ".join(cur.get('class') or []).lower()
            role = cur.get('role') if cur and hasattr(cur, 'get') else None
            if tag in ('header', 'footer'):
                return True
            if role and re.search(r'navigation|banner|menu|search|complementary', str(role), re.I):
                return True
            if re.search(r'nav|breadcrumb|masthead|site-header|menu|toolbar|subnav|topbar|footer', cls):
                return True
            cur = cur.parent
        return False

    def looks_like_article_href(h):
        if not h:
            return False
        h = h.lower()
        if not ('/publications/' in h or '/news/' in h):
            return False
        # look for numeric id segments like /760257/ or /409171/
        if re.search(r'/\d{3,}/?$', h) or re.search(r'/article/\d{3,}', h):
            return True
        # allow /publications/.../some-slug.html often contains article pages (fallback)
        if '/publications/' in h and (h.endswith('.html') or re.search(r'/[^/]+\.html$', h)):
            return True
        return False

    anchors = soup.select('a[href]')
    article_anchors = []
    for a in anchors:
        try:
            if is_in_header_footer(a):
                continue
            h = a.get('href') or ''
            if not h:
                continue
            if looks_like_article_href(h):
                article_anchors.append(a)
        except Exception:
            continue

    seen = set()
    items = []

    def canonicalize(href):
        try:
            u = urlparse(href)
            clean = u.scheme + "://" + u.netloc + u.path
            return clean.rstrip('/')
        except Exception:
            return href.split('?')[0].rstrip('/')

    for a in article_anchors:
        if len(items) >= max_items:
            break
        try:
            raw_href = a.get('href') or ''
            href = urljoin(base_url, raw_href)
            canon = canonicalize(href)
            if canon in seen:
                continue
            title = txt(a)
            if not title or len(title) < 6:
                continue
            # wrapper: prefer article, li, div with article classes
            # Encontrar o ancestral mais apropriado que contenha a âncora
            def find_best_wrapper_for_anchor(a):
                cur = a
                best = None
                for _ in range(8):
                    if cur is None:
                        break
                    if cur.name in ('li', 'article'):
                        best = cur
                    # se este ancestor já contiver um parágrafo/descrip/time, preferimos escolhê-lo
                    try:
                        if cur.select_one('p.short, p.lede, .short, .summary, .dek, .feed__description, .article-teaser, time, .byline'):
                            return cur
                    except Exception:
                        pass
                    cur = cur.parent
                return best or a.find_parent(['article', 'li']) or a.parent

            wrapper = find_best_wrapper_for_anchor(a)

            # --- DESCRIPTION (várias heurísticas) ---
            description = ''
            try:
                # procura apenas DENTRO do wrapper — evita "roubar" do próximo artigo
                for sel in ['p.short', 'p.lede', '.short', '.summary', '.dek', '.article-teaser', '.feed__description', '.teaser', 'p']:
                    el = wrapper.select_one(sel) if wrapper else None
                    if el:
                        t = txt(el)
                        if t and not re.search(r'subscribe|advertis|read more', t, re.I):
                            description = t.strip()
                            break
            except Exception:
                description = ''

            # fallback restrito: se nada dentro do wrapper, procura em um possível elemento irmão ANTERIOR (não o próximo)
            if not description:
                try:
                    prev = wrapper.find_previous_sibling() if wrapper else None
                    if prev:
                        p = prev.select_one('p.short, p, .summary, .dek')
                        if p:
                            dd = txt(p)
                            if dd and not re.search(r'subscribe|advertis|read more', dd, re.I):
                                description = dd.strip()
                except Exception:
                    pass

            # --- DATE (várias heurísticas) ---
            date = ''
            try:
                # procura dentro do wrapper por elementos óbvios de data/byline/time
                for ds in ['time', '.byline', '.date', '.published', '.timestamp', '.article-byline']:
                    el = wrapper.select_one(ds) if wrapper else None
                    if el:
                        t = txt(el)
                        if t:
                            # se estilo "By Name - 8 hours ago", extrair a parte depois do traço
                            if ' - ' in t:
                                date = t.split(' - ')[-1].strip()
                            else:
                                # remover "By Name" inicial se existir
                                dclean = re.sub(r'^\s*By\s+[^-]+-\s*', '', t).strip()
                                dclean = re.sub(r'^\s*By\s+[^,]+\s*', '', dclean).strip()
                                date = dclean
                            if date:
                                break
            except Exception:
                date = ''

            # fallback regex no texto do wrapper para "8 hours ago" ou datas completas
            if not date:
                try:
                    rawtxt = txt(wrapper or a)
                    m = re.search(r'\b\d+\s+(?:hours?|days?|minutes?)\s+ago\b', rawtxt, re.I) \
                        or re.search(r'\b(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}\b', rawtxt)
                    if m:
                        date = m.group(0)
                except Exception:
                    pass

            # --- última opção (opcional): buscar meta/time na página do artigo apenas se description ou date ainda vazios ---
            if (not description or not date) and href and len(items) < max_items:
                try:
                    resp = requests.get(href, headers={'User-Agent': 'Mozilla/5.0'}, timeout=6)
                    if resp.status_code == 200 and resp.text:
                        sa = BeautifulSoup(resp.text, 'html.parser')
                        if not description:
                            md = sa.select_one('meta[property="og:description"], meta[name="description"]')
                            if md and md.get('content'):
                                dd = md.get('content').strip()
                                if dd and not re.search(r'subscribe|advertis|read more', dd, re.I):
                                    description = dd
                        if not date:
                            ttag = sa.select_one('time[datetime], time')
                            if ttag:
                                date = txt(ttag).strip()
                            else:
                                by = sa.select_one('.byline, .article-byline, .published')
                                if by:
                                    dclean = txt(by).strip()
                                    if ' - ' in dclean:
                                        date = dclean.split(' - ')[-1].strip()
                                    else:
                                        date = re.sub(r'^\s*By\s+[^-]+\s*', '', dclean).strip()
                except Exception:
                    pass

            # última limpeza
            if description:
                description = description.replace('\n', ' ').strip()
            if date:
                date = date.replace('\n', ' ').strip()

            seen.add(canon)
            items.append({
                'title': title.strip(),
                'link': href,
                'date': date.strip(),
                'description': description.strip(),
                'source': 'mediapost'
            })
        except Exception:
            continue

    print(f"scrape_mediapost_listing: found {len(items)} items from {base_url}")
    return items[:max_items]

# ---------------------------
# SCRAPER específico SEMIENGINEERING
# ---------------------------
def scrape_semiengineering_listing(rendered_path=None, base_url="https://semiengineering.com/", max_items=36, timeout=10):
    """
    Parse a rendered semiengineering HTML (if rendered_path given) or fetch the live page.
    Returns list of dicts {title, link, date, description, source} up to max_items.
    """
    from bs4 import BeautifulSoup
    from urllib.parse import urljoin
    import re, requests, os

    def txt(el):
        try:
            return (el.get_text(" ", strip=True) if el else "").strip()
        except Exception:
            return ""

    def abs_url(href):
        try:
            return urljoin(base_url, (href or '').strip())
        except Exception:
            return (href or '').strip()

    def clean_text_exclude_small(node):
        if not node:
            return ''
        # create small soup and remove author-like smalls
        s = BeautifulSoup(str(node), "html.parser")
        for rem in s.select("small, .loop_post_meta, .post_meta, .byline, .post-meta, .loop_post_excerpt"):
            try:
                rem.decompose()
            except Exception:
                try:
                    rem.extract()
                except Exception:
                    pass
        return " ".join(s.get_text(separator=" ").split()).strip()

    def likely_author_text(s):
        if not s:
            return False
        ss = s.strip()
        if len(ss) < 20:
            return True
        if re.match(r'^\s*By\s+', ss, re.I):
            return True
        # short uppercase-ish string => likely author
        if re.match(r'^[A-ZÀ-Ý\-\s\.]{3,40}$', ss) and len(ss.split()) <= 4:
            return True
        return False

    def get_description_smart(wrapper):
        if not wrapper:
            return ''
        sels = ['.special_reports_slides_exceprt', '.post_snippet_l', '.post_snippet_r', '.post_snippet', '.post_snippet_wrap', '.loop_post_excerpt', '.excerpt', '.summary', '.dek', 'p']
        for s in sels:
            try:
                el = wrapper.select_one(s)
                if el:
                    t = clean_text_exclude_small(el).strip()
                    if t and not likely_author_text(t):
                        return t
            except Exception:
                pass
        # fallback: wrapper inner text minus small/byline and minus repeating title
        try:
            clone = BeautifulSoup(str(wrapper), "html.parser")
            for rem in clone.select("small, .loop_post_meta, .post_meta, .byline, .post-meta"):
                try:
                    rem.decompose()
                except Exception:
                    pass
            candidate = " ".join(clone.get_text(separator=" ").split()).strip()
            # remove title prefix if present
            h = wrapper.select_one("h3, h2, a[title]")
            if h:
                ttitle = (txt(h) or "").strip()
                if ttitle and candidate.lower().startswith(ttitle.lower()):
                    candidate = candidate[len(ttitle):].strip()
            if candidate and not likely_author_text(candidate) and len(candidate) > 10:
                return candidate
        except Exception:
            pass
        return ''

    def is_likely_author_link(a):
        if not a:
            return False
        h = (a.get("href") or "").lower()
        if re.search(r'/author/|/tag/|/category/', h):
            return True
        t = (txt(a) or "")
        if t and len(t) < 30 and re.match(r'^[A-Z0-9\-\s\']+$', t) and len(t.split()) <= 4:
            return True
        return False

    def extract_from_wrapper(wrapper, prefer_anchor=None, source_label='list', seen_local=None):
        try:
            if not wrapper:
                return None
            a = prefer_anchor
            if not a:
                a_tag = None
                # prefer anchors that wrap H3/H2
                a_tag = wrapper.select_one('a[href] > h3, a[href] > h2')
                if a_tag:
                    a = a_tag.find_parent('a')
                if not a:
                    a = wrapper.select_one('h3 a, h2 a, a[title], a[href]')
                if not a:
                    anchors = wrapper.select('a[href]')
                    for a0 in anchors:
                        if (txt(a0) or '').strip() and not is_likely_author_link(a0):
                            a = a0
                            break
                    if not a and anchors:
                        a = anchors[0]
            if not a:
                return None
            raw_href = (a.get('href') or '').strip()
            if not raw_href or re.search(r'(^#|^javascript:|mailto:|/help|/legal|cookie|privacy|terms|signin|login|settings|/consent|/preferences|/policies)', raw_href, re.I):
                return None
            if is_likely_author_link(a):
                return None
            link = abs_url(raw_href)

            # title
            h = wrapper.select_one('h3, h2') or (a.select_one('h3, h2') if a else None)
            title = txt(h) if h else (a.get('title') or txt(a) or txt(wrapper.select_one('h3, h2') or '')).strip()
            if not title or len(title) < 3:
                return None

            description = get_description_smart(wrapper) or ''
            # date heuristics
            date = ''
            meta = wrapper.select_one('.loop_post_meta small, .byline small, .byline, time, .post-meta small, .post_meta small')
            if meta:
                date = txt(meta).strip()
                # strip leading "By X - " if present
                date = re.sub(r'^\s*By\s+[^-]+-?\s*', '', date, flags=re.I).strip()
            if not date:
                m = re.search(r'\b(?:\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}|\w{3,9}\s+\d{1,2},\s+\d{4}|\d{1,2}h\s+ago|\d+\s+hours?\s+ago)', wrapper.get_text(" ", strip=True) or '', re.I)
                if m:
                    date = m.group(0)

            key = (link or title).rstrip('/').strip()
            if not key or (seen_local is not None and key in seen_local):
                return None
            if seen_local is not None:
                seen_local.add(key)
            return {'title': title.strip(), 'link': link.strip(), 'date': (date or '').strip(), 'description': (description or '').strip(), 'source': source_label}
        except Exception:
            return None

    # read HTML (rendered_path takes precedence)
    html = ''
    if rendered_path and os.path.exists(rendered_path):
        try:
            html = open(rendered_path, 'r', encoding='utf-8').read()
        except Exception:
            html = ''
    if not html:
        try:
            r = requests.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
            r.raise_for_status()
            html = r.text
        except Exception:
            return []

    soup = BeautifulSoup(html, "html.parser")
    seen_local = set()
    out = []

    # 1) special reports (up to 3)
    try:
        special_selectors = ['.special_reports_slides .special_reports_slides_item', '.special_reports_slides_item', '.special_reports_slides_post_title', '.special-reports .special_reports_slides_item', '#slide1, #slide2, #slide3']
        special_items = []
        for sel in special_selectors:
            try:
                found = soup.select(sel)
                if found:
                    normalized = [f.find_parent(class_='special_reports_slides_item') or f for f in found]
                    for n in normalized:
                        if n and n not in special_items:
                            special_items.append(n)
                            if len(special_items) >= 3:
                                break
                if len(special_items) >= 3:
                    break
            except Exception:
                pass
        # fallback: anchors inside container
        if len(special_items) < 3:
            cont = soup.select_one('.special_reports_slides, .special_reports_slides_wrap, #special_reports, .special-reports')
            if cont:
                for a in cont.select('a[href]'):
                    if len(special_items) >= 3:
                        break
                    wrap = a.find_parent(class_='special_reports_slides_item') or a.find_parent('li') or a.parent
                    if wrap is not None and wrap not in special_items:
                        special_items.append(wrap)

        # collect possible external excerpts (they may be outside the slide wrappers)
        all_excerpts = soup.select('.special_reports_slides_exceprt, .special_reports_slides_excerpt, .post_snippet_l, .post_snippet, .excerpt')

        for idx, it_el in enumerate(special_items[:3]):
            try:
                a = it_el.select_one('.special_reports_slides_post_title a, a[href]') or it_el.select_one('a[href]')
                title = txt(a) or txt(it_el.select_one('.special_reports_slides_post_title')) or txt(it_el.select_one('h3,h2')) or ''
                link = abs_url(a.get('href')) if a and a.has_attr('href') else ''
                # 1) try internal excerpt selectors
                desc = ''
                desc_el = None
                for s in ('.special_reports_slides_exceprt', '.special_reports_slides_excerpt', '.post_snippet_l', '.post_snippet', '.excerpt'):
                    try:
                        desc_el = it_el.select_one(s)
                    except Exception:
                        desc_el = None
                    if desc_el:
                        desc = clean_text_exclude_small(desc_el)
                        if desc:
                            break

                matched = False
                # 2) if not found, try external excerpts matching href
                if not desc and all_excerpts and link:
                    for ex in all_excerpts:
                        try:
                            aex = ex.select_one('a[href]')
                            if aex and aex.has_attr('href'):
                                href_ex = abs_url(aex.get('href') or '')
                                if href_ex and href_ex.rstrip('/') == link.rstrip('/'):
                                    desc = clean_text_exclude_small(ex)
                                    matched = True
                                    break
                        except Exception:
                            continue

                # 3) if still not found, try title-words heuristic on external excerpts
                if not desc and all_excerpts and title:
                    tt_words = [w for w in re.split(r'\W+', title.lower()) if w]
                    if tt_words:
                        sample = tt_words[:3]
                        for ex in all_excerpts:
                            try:
                                ex_text = clean_text_exclude_small(ex).lower()
                                if all(s in ex_text for s in sample):
                                    desc = clean_text_exclude_small(ex)
                                    matched = True
                                    break
                            except Exception:
                                continue

                # 4) fallback by position (1st excerpt -> 1st special item)
                if not desc and all_excerpts:
                    try:
                        if idx < len(all_excerpts):
                            desc = clean_text_exclude_small(all_excerpts[idx])
                    except Exception:
                        pass

                # 5) last resort: extract from wrapper generically
                if not desc:
                    desc = get_description_smart(it_el) or ''
                    # if still looks like author-only, try removing small and taking remainder
                    if likely_author_text(desc):
                        # try removing small/byline from wrapper and get remainder
                        try:
                            tmp = clean_text_exclude_small(it_el)
                            if tmp and not likely_author_text(tmp) and len(tmp) > 10:
                                desc = tmp
                        except Exception:
                            pass

                key = (link or title).rstrip('/').strip()
                if key and key not in seen_local:
                    seen_local.add(key)
                    out.append({'title': title.strip(), 'link': link.strip(), 'date': '', 'description': (desc or '').strip(), 'source': 'special-reports'})
                if len(out) >= max_items:
                    return out[:max_items]
            except Exception:
                continue
    except Exception:
        pass

    # 2) headings-based extraction for main sections
    try:
        headings = [h for h in soup.select('h2,h3,h4, .snippet_header, .section-heading') if txt(h)]
        section_keywords = ['special reports','top stories','latest news','latest','opinion','research','startup corner','startups','business news']
        for i, h in enumerate(headings):
            htext = txt(h).lower().strip()
            if not any(k in htext for k in section_keywords):
                continue
            source_label = h.decode_contents() if hasattr(h, 'decode_contents') else str(h)
            next_h = headings[i+1] if i+1 < len(headings) else None
            node = h.find_next_sibling()
            count = 0
            while node and node is not next_h and count < 400 and len(out) < max_items:
                try:
                    prefer_sel = 'div.post_snippet_l, div.post_snippet_r, .post_snippet, .post_snippet_wrap, article, li, .loop_post, .loop_post_item, .post, .news-item, .item, .teaser, .card'
                    candidates = node.select(prefer_sel) if hasattr(node, 'select') else []
                    if not candidates:
                        candidates = [node]
                    for cand in candidates:
                        if len(out) >= max_items:
                            break
                        it = extract_from_wrapper(cand, None, source_label, seen_local)
                        if it:
                            out.append(it)
                except Exception:
                    pass
                node = node.find_next_sibling()
                count += 1
    except Exception:
        pass

    # 3) fallback anchors
    if len(out) < max_items:
        main = soup.select_one('#main-content') or soup.body or soup
        anchors = [a for a in main.select('a[href]') if (txt(a) or '').strip() and not is_likely_author_link(a)]
        for a in anchors:
            if len(out) >= max_items:
                break
            href = (a.get('href') or '').lower()
            if re.search(r'translate|googlesyndication|#|/translate|/category-main-page|/category-|/tag-|/author-', href):
                continue
            w = a.find_parent('article') or a.find_parent('li') or a.parent
            it = extract_from_wrapper(w, a, 'fallback-anchor', seen_local)
            if it:
                out.append(it)

    return out[:max_items]

# ---------------------------
# SCRAPER específico THE DRUM (profiles 'featured')
# ---------------------------

# Mapeamento: as chaves devem corresponder aos basenames dos ficheiros feeds/*.xml (sem .xml)
THE_DRUM_PROFILE_URLS = {
    "thedrum-pubmatic": "https://www.thedrum.com/profile/pubmatic/featured",
    "thedrum-the-trade-desk": "https://www.thedrum.com/profile/the-trade-desk/featured",
    # adiciona mais chaves conforme os teus ficheiros feeds/....xml
}

def scrape_thedrum_profile(base_url, max_items=5, timeout=10):
    """
    Fetch and parse a TheDrum profile 'featured' page and return up to max_items dicts:
    {'title','link','date','description','source'}
    Observações: a estrutura do TheDrum coloca o título/descrição na mesma div; usamos heurísticas
    para extrair description, date e link.
    """
    try:
        r = requests.get(base_url, headers={"User-Agent": "Mozilla/5.0"}, timeout=timeout)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print("scrape_thedrum_profile: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, "html.parser")

    def txt(el):
        try:
            return (el.get_text(" ", strip=True) if el else "").strip()
        except Exception:
            return ""

    def abs_url(href):
        try:
            return urljoin(base_url, (href or "").strip())
        except Exception:
            return (href or "").strip()

    # selectors baseados no exemplo que forneceste
    card_sel = ".td-company-profile__company-details__article-card"
    wrapper_sel = ".td-company-profile__company-details__article-card__wrapper"
    title_sel = ".td-company-profile__company-details__article-card__wrapper__title"
    footer_sel = ".td-company-profile__company-details__article-card__wrapper__footer"

    cards = []
    for c in soup.select(card_sel):
        w = c.select_one(wrapper_sel) or c
        cards.append(w)
    if not cards:
        cards = soup.select(wrapper_sel)[:max_items]

    items = []
    seen = set()
    for w in cards:
        if len(items) >= max_items:
            break
        try:
            desc_el = w.select_one(title_sel) or w
            description = txt(desc_el)
            date_el = w.select_one(footer_sel) or w.select_one("time, .meta, .post-date")
            date = txt(date_el) if date_el else ""
            a = w.select_one("a[href]") or w.find_parent("a")
            if not a:
                # try find anchor in parent card
                parent_card = w.find_parent(class_=lambda x: x and "td-company-profile__company-details__article-card" in x)
                if parent_card:
                    a = parent_card.select_one("a[href]")
            link = abs_url(a.get("href")) if a and a.has_attr("href") else ""
            key = (link or description).rstrip("/").strip()
            if not key or key in seen:
                continue
            seen.add(key)
            title = description  # conforme conversámos: description também serve de title
            items.append({"title": title or "", "link": link or "", "date": date or "", "description": description or "", "source": "thedrum-profile"})
        except Exception:
            continue

    return items[:max_items]


# ---------------------------
# FIM DO SCRAPERS
# ---------------------------

def main():
    
    site_item_map = load_sites_item_container()
    all_rows = []


    feed_files = sorted(glob.glob(os.path.join(FEEDS_DIR, "*.xml")))
    if not feed_files:
        print("No feed files found in", FEEDS_DIR)


    # --- loop pelos feeds (o teu código aqui deve permanecer tal como o tens) ---
    for ff in feed_files:
        try:
            base = os.path.basename(ff)
            site_name = os.path.splitext(base)[0]
            ic = site_item_map.get(site_name, "")
            

            # --- DETECÇÃO AUTOMÁTICA ROBUSTA: procurar TheDrum profile/url dentro do feed ---
            try:
                td_url = None
                td_url = detect_profile_url_in_feed_file(ff, ['thedrum.com/profile', 'thedrum.com'])
                if not td_url:
                    try:
                        parsed = feedparser.parse(ff)
                        entries = getattr(parsed, 'entries', []) or []
                        for e in entries:
                            cand = e.get('link') or e.get('id') or e.get('guid') or ''
                            if cand and '/profile/' in cand:
                                td_url = cand
                                break
                    except Exception:
                        td_url = None

                profile_base = None
                if td_url:
                    m = re.search(r'(https?://[^/]+/profile/[^/]+)', td_url, re.I)
                    if m:
                        profile_base = m.group(1).rstrip('/')
                    else:
                        m2 = re.search(r'/profile/([^/\s"\'>]+)', td_url, re.I)
                        if m2:
                            slug = m2.group(1).strip('/')
                            profile_base = f"https://www.thedrum.com/profile/{slug}"
                        else:
                            slug_guess = site_name.replace('_','-').replace(' ','-').strip('-')
                            if slug_guess and len(slug_guess) > 2:
                                profile_base = f"https://www.thedrum.com/profile/{slug_guess}"

                if profile_base:
                    if not profile_base.endswith('/featured'):
                        profile_base = profile_base.rstrip('/') + '/featured'
                    try:
                        td_items = scrape_thedrum_profile(base_url=profile_base, max_items=5, timeout=10)
                        if td_items:
                            for it in td_items:
                                all_rows.append({
                                    "site": site_name,
                                    "title": it.get("title", "") or "",
                                    "link (source)": it.get("link", "") or "",
                                    "pubDate": it.get("date", "") or "",
                                    "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                                    "item_container": ic,
                                    "topic": "N/A"
                                })
                            print(f"Added {len(td_items)} items for {site_name} (TheDrum profile detected: {profile_base})")
                            # saltar parsing XML para este feed
                            continue
                    except Exception as e:
                        print("Error scraping TheDrum profile for", site_name, "url:", profile_base, ":", e)
            except Exception:
                pass

            # --- special: The Drum profile featured pages mapping
            if site_name in THE_DRUM_PROFILE_URLS:
                try:
                    url = THE_DRUM_PROFILE_URLS[site_name]
                    td_items = scrape_thedrum_profile(base_url=url, max_items=5, timeout=10)
                    for it in td_items:
                        all_rows.append({
                            "site": site_name,
                            "title": it.get("title", "") or "",
                            "link (source)": it.get("link", "") or "",
                            "pubDate": it.get("date", "") or "",
                            "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                            "item_container": ic,
                            "topic": "N/A"
                        })
                    # skip XML parsing for this site (we already added rows)
                    print(f"Added {len(td_items)} items for {site_name} (TheDrum profile scrape)")
                    continue
                except Exception as e:
                    print("Error scraping TheDrum profile for", site_name, ":", e)
                    # fall through to XML parsing as fallback
    
            # --- special: mediapost (scrape listing page directly) ---
            if site_name == "mediapost":
                try:
                    mp_items = scrape_mediapost_listing(base_url="https://www.mediapost.com/news/", max_items=30)
                    site_cfg = SITES_CFG_MAP.get(site_name, {})
                    added = 0
                    for it in mp_items:
                        t = it.get('title', '').strip()
                        link = it.get('link', '').strip()
                        if not t or t.lower() in ("no title", "return to homepage", "category"):
                            continue
                        rows = {
                            "site": site_name,
                            "title": t,
                            "link (source)": link,
                            "pubDate": it.get('date', ''),
                            "description (short)": strip_html_short(it.get('description', ''), max_len=300),
                            "item_container": ic,
                            "topic": "N/A"
                        }
                        # aplica filtros
                        has_filters = bool(site_cfg.get('filters', {}).get('keywords') or site_cfg.get('filters', {}).get('exclude'))
                        match = matches_filters_for_row(rows, site_cfg)
                        if has_filters:
                            if not match:
                                continue
                            rows['match'] = match
                        else:
                            rows['match'] = ''
                        all_rows.append(rows)
                        added += 1
                    # skip parsing XML for mediapost
                    print(f"Added {added} mediapost items (scraped live)")
                    continue
                except Exception as e:
                    print("Error scraping mediapost listing:", e)
                    # fall through to XML parsing as fallback

    
            # --- special: modernhealthcare - prefer rendered HTML (if present) ---
            if site_name == "modernhealthcare":
                rendered_path = os.path.join('scripts', 'rendered', 'modernhealthcare.html')
                try:
                    if os.path.exists(rendered_path):
                        mh_items = scrape_modern_rendered(rendered_path, base_url="https://www.modernhealthcare.com/latest-news/", max_items=11)
                        site_cfg = SITES_CFG_MAP.get(site_name, {})
                        added = 0
                        for it in mh_items:
                            t = it.get('title', '').strip()
                            link = it.get('link', '').strip()
                            if not t or t.lower() in ("no title", "return to homepage", "category"):
                                continue
                            rows = {
                                "site": site_name,
                                "title": t,
                                "link (source)": link,
                                "pubDate": it.get('date', ''),
                                "description (short)": strip_html_short(it.get('description', ''), max_len=300),
                                "item_container": ic,
                                "topic": "N/A"
                            }
                            has_filters = bool(site_cfg.get('filters', {}).get('keywords') or site_cfg.get('filters', {}).get('exclude'))
                            match = matches_filters_for_row(rows, site_cfg)
                            if has_filters:
                                if not match:
                                    continue
                                rows['match'] = match
                            else:
                                rows['match'] = ''
                            all_rows.append(rows)
                            added += 1
                        print(f"Using rendered HTML for {site_name}: added {added} items")
                        continue
                except Exception as e:
                    print("Error scraping modernhealthcare rendered html (per-file):", e)
                    # fallthrough to XML parsing fallback below

    
            # --- special: medtechdive homepage (hero) ---
            if site_name == "medtechdive":
                med_items = scrape_medtech_home(base_url="https://www.medtechdive.com/", timeout=10)
                site_cfg = SITES_CFG_MAP.get(site_name, {})
                added = 0
                for it in med_items:
                    rows = {
                        "site": site_name,
                        "title": it.get("title", "") or "",
                        "link (source)": it.get("link", "") or "",
                        "pubDate": it.get("date", "") or "",
                        "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                        "item_container": ic,
                        "topic": "N/A"
                    }
                    has_filters = bool(site_cfg.get('filters', {}).get('keywords') or site_cfg.get('filters', {}).get('exclude'))
                    match = matches_filters_for_row(rows, site_cfg)
                    if has_filters:
                        if not match:
                            continue
                        rows['match'] = match
                    else:
                        rows['match'] = ''
                    all_rows.append(rows)
                    added += 1
                print(f"Added {added} medtechdive (home hero) items")
                continue

    
            # --- special: medtechdive topic medical-devices (first 7) ---
            if site_name == "medtechdive-devices":
                med_items = scrape_medtech_topic(base_url="https://www.medtechdive.com/topic/medical-devices/", max_items=7, timeout=10)
                site_cfg = SITES_CFG_MAP.get(site_name, {})
                added = 0
                for it in med_items:
                    rows = {
                        "site": site_name,
                        "title": it.get("title", "") or "",
                        "link (source)": it.get("link", "") or "",
                        "pubDate": it.get("date", "") or "",
                        "description (short)": strip_html_short(it.get("description", "") or "", max_len=300),
                        "item_container": ic,
                        "topic": "N/A"
                    }
                    has_filters = bool(site_cfg.get('filters', {}).get('keywords') or site_cfg.get('filters', {}).get('exclude'))
                    match = matches_filters_for_row(rows, site_cfg)
                    if has_filters:
                        if not match:
                            continue
                        rows['match'] = match
                    else:
                        rows['match'] = ''
                    all_rows.append(rows)
                    added += 1
                print(f"Added {added} medtechdive-devices items")
                continue

    
            # --- special: mobihealthnews (scraped live) ---
            if site_name == "mobihealthnews":
                mobi_items = scrape_mobihealth_listing(base_url="https://www.mobihealthnews.com/", max_items=11, timeout=10)
                site_cfg = SITES_CFG_MAP.get(site_name, {})
                added = 0
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
                    # aplica filtros (se existirem)
                    match = matches_filters_for_row(rows, site_cfg)
                    if site_cfg.get('filters', {}).get('keywords') or site_cfg.get('filters', {}).get('exclude'):
                        # se filtro definido, só adiciona se houver match (não queremos fallback)
                        if not match:
                            continue
                        rows['match'] = match
                    else:
                        # sem filtros -> mantém (mas preenche match vazia)
                        rows['match'] = ''
                    all_rows.append(rows)
                    added += 1
                print(f"Added {added} mobihealthnews items (scraped live)")
                continue

    
            # --- special: semiengineering -> use page scraper (rendered if available, else live fetch) ---
            if site_name == "semiengineering":
                try:
                    rendered_path = os.path.join('scripts', 'rendered', 'semiengineering.html')
                    if os.path.exists(rendered_path):
                        se_items = scrape_semiengineering_listing(rendered_path, base_url="https://semiengineering.com/", max_items=36)
                    else:
                        # try site homepage
                        se_items = scrape_semiengineering_listing(None, base_url="https://semiengineering.com/", max_items=36)
                    site_cfg = SITES_CFG_MAP.get(site_name, {})
                    added = 0
                    for it in se_items:
                        rows = {
                            "site": site_name,
                            "title": it.get("title","") or "",
                            "link (source)": it.get("link","") or "",
                            "pubDate": it.get("date","") or "",
                            "description (short)": strip_html_short(it.get("description","") or "", max_len=300),
                            "item_container": ic,
                            "topic": "N/A"
                        }
                        has_filters = bool(site_cfg.get('filters', {}).get('keywords') or site_cfg.get('filters', {}).get('exclude'))
                        match = matches_filters_for_row(rows, site_cfg)
                        if has_filters:
                            if not match:
                                continue
                            rows['match'] = match
                        else:
                            rows['match'] = ''
                        all_rows.append(rows)
                        added += 1
                    # skip XML parsing for semiengineering
                    print(f"Added {added} semiengineering items")
                    continue
                except Exception as e:
                    print("Error scraping semiengineering:", e)


            # --- fallback: parse feed XML com fallback ---
            rows = parse_feed_file_with_fallback(ff)

            # DEBUG: mostrar quantas rows devolvidas e primeiras entradas (ajuda a diagnosticar porque o xlsx pode não conter o esperado)
            try:
                print(f"PARSING FEEDS_TO_EXCEL DEBUG -> file={ff} | site={site_name} | rows_returned={len(rows)}")
                if rows:
                    for i, r in enumerate(rows[:8]):
                        title_preview = (r.get('title') or '')[:160].replace('\n',' ')
                        link_preview = (r.get('link') or '')
                        date_preview = (r.get('pubDate') or r.get('date') or '')
                        desc_preview = (r.get('description') or r.get('description (short)') or r.get('full_text',''))[:200].replace('\n',' ')
                        print(f"  row[{i}] title='{title_preview}' link='{link_preview}' pubDate='{date_preview}' desc_preview='{desc_preview}'")
                else:
                    # tentamos ver o que feedparser vê (diagnóstico)
                    import feedparser as _fp
                    parsed = _fp.parse(ff)
                    entries = getattr(parsed, 'entries', []) or []
                    print(f"  feedparser entries: {len(entries)}")
                    for i,e in enumerate(entries[:8]):
                        t = e.get('title','')
                        l = e.get('link','') or e.get('id','')
                        p = e.get('published','') or e.get('updated','')
                        s = (e.get('summary','') or '')[:200].replace('\n',' ')
                        print(f"   entry[{i}] title='{t[:140]}' link='{l}' published='{p}' summary_preview='{s}'")
            except Exception as dd:
                print("  Error debug printing feed contents:", dd)

            for r in rows:
                r["item_container"] = ic
                all_rows.append(r)

        except Exception as exc:
            print("Error parsing feed", ff, ":", exc)

    # --- finalizar e gravar (fora do loop) ---
    if not all_rows:
        print("No items found across feeds.")

    # ---------------------------------------------------------
    # Normalização dos rows: garantir chaves, extrair matched reason
    # ---------------------------------------------------------
    # helper simples para gerar description (short) caso falte
    def _strip_html_short_simple(text, max_len=300):
        try:
            s = _re.sub(r'<[^>]+>', '', text or '')
            s = ' '.join(s.split())
            return s[:max_len]
        except Exception:
            return (text or '')[:max_len]
    
    try:
        mr_re = _re.compile(r'\[MatchedReason:\s*(.+?)\]', _re.I)
        for r in all_rows:
            # garantir topic
            if 'topic' not in r or r.get('topic') is None:
                r['topic'] = r.get('topic', '') or ''
    
            # garantir description (campo usado por filtros)
            if not r.get('description'):
                r['description'] = r.get('description (short)', '') or r.get('description', '') or ''
    
            # extrair matched reason se estiver embutido na description
            if not r.get('match'):
                src_desc = r.get('description','') or ''
                m = mr_re.search(src_desc)
                if m:
                    reason = m.group(1).strip()
                    r['match'] = reason
                    # remover o sufixo da description (limpar)
                    r['description'] = mr_re.sub('', src_desc).strip()
                    # actualizar description (short)
                    r['description (short)'] = _strip_html_short_simple(r['description'], max_len=300)
                else:
                    # usar matched_reason se existir sob outro nome
                    r['match'] = (r.get('matched_reason') or r.get('matched_reason_raw') or '') or ''
    
            # garantir description (short) existe
            if not r.get('description (short)'):
                r['description (short)'] = _strip_html_short_simple(r.get('description',''), max_len=300)
    
            # garantir chaves base
            if 'site' not in r:
                r['site'] = r.get('site') or ''
            if 'title' not in r:
                r['title'] = r.get('title') or ''
            if 'link (source)' not in r:
                r['link (source)'] = r.get('link') or r.get('link (source)') or ''
            if 'pubDate' not in r:
                r['pubDate'] = r.get('pubDate') or r.get('date') or ''
    except Exception as _e:
        print("Normalization step failed:", _e)



    cols = ["site", "title", "link (source)", "pubDate", "description (short)", "item_container", "topic", "match"]


    # DEBUG: verificar se 'pd' ainda é o pandas e se all_rows tem a forma correcta
    import traceback
    try:
        if not hasattr(pd, 'DataFrame'):
            print("ERROR: 'pd' não parece ser o pandas. type(pd) =", type(pd))
        print("DEBUG: all_rows length =", len(all_rows))
    except Exception as e:
        print("DEBUG CHECK FAILED:", e)

    try:
        df = pd.DataFrame(all_rows, columns=cols)
    except Exception as e:
        print("Failed to create DataFrame:", e)
        traceback.print_exc()
        try:
            # mostrar uma amostra para diagnosticar
            print("Sample of all_rows (first 5):")
            for i, r in enumerate(all_rows[:5]):
                print(i, repr(r))
        except Exception:
            pass
        return

    try:
        outpath = os.path.abspath(OUT_XLSX)
        print("Saving Excel to:", outpath)
        df.to_excel(outpath, index=False)
        print(f"Wrote {outpath} ({len(df)} rows)")
        # listar ficheiros do directório actual para confirmar que existe
        try:
            print("Files in cwd:", os.listdir('.'))
        except Exception:
            pass
    except Exception as e:
        print("Failed to write Excel file:", e)
        traceback.print_exc()
        return


# === certifique-se de que esta linha ESTÁ na coluna 0 (não indentada) ===
if __name__ == "__main__":
    main()
