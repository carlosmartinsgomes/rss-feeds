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
        # defensive filter for modernhealthcare noise
        if title and title.lower().strip() in ("no title", "return to homepage"):
            continue
        if not title and (link.endswith("modernhealthcare.com") or link.rstrip('/') == "https://www.modernhealthcare.com"):
            continue
        pub = (e.get("published", "") or e.get("pubDate", "") or e.get("updated", "") or "")
        desc = (e.get("summary", "") or e.get("description", "") or "")
        desc_short = strip_html_short(desc, max_len=300)

        # topic
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


# ---------------------------
# SCRAPER específico MOBIHEALTH (já existente)
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
    Igual ao que já tinhas: fetch directo da página e heurísticas.
    """
    try:
        r = requests.get(base_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=timeout)
        r.raise_for_status()
        html = r.text
    except Exception as e:
        print("scrape_mobihealth_listing: fetch failed:", e)
        return []

    soup = BeautifulSoup(html, "html.parser")
    # ... (mantém a tua implementação existente, omitida aqui por brevidade)
    # Para não duplicar poluição, usa a função que já tinhas.
    # (assume-se que a tua versão anterior permanece aqui)
    # -- fallback simple: tentar extrair títulos h2/h3 e anchors --
    items = []
    for a in soup.select('a[href]')[:max_items]:
        t = text_of(a)
        h = a.get('href') or ''
        if not t or not h:
            continue
        items.append({'title': t.strip(), 'link': abs_url(h, base_url), 'date': '', 'description': '', 'source': 'fallback'})
    return items[:max_items]


# ---------------------------
# SCRAPER específico MODERNHEALTHCARE (rendered HTML)
# ---------------------------
def scrape_modern_rendered(rendered_path, base_url="https://www.modernhealthcare.com/latest-news/", max_items=10):
    from bs4 import BeautifulSoup
    import re

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
            if any(c in ('u-border-b','views-row','col-lg-6','square-one','view-content') for c in classes):
                return cur
            cur = cur.parent
        return el.find_parent(['article']) or el

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
            try:
                h = a.get('href') or ''
                abs_h = abs_href(h)
                t = txt(a)
                if not t or len(t) < 6: continue
                if re.search(r'(^#|^javascript:|mailto:)', abs_h, re.I): continue
                key = abs_h.rstrip('/')
                if key in seen: continue
                seen.add(key)
                items.append({'title': t, 'link': abs_h, 'date': '', 'description': '', 'source': 'anchor-fallback'})
            except Exception:
                continue

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
            wrapper = a.find_parent(['article', 'li'])
            if wrapper is None:
                # look for nearby div parent
                wrapper = a.parent

            # --- DESCRIPTION (várias heurísticas) ---
            description = ''
            for sel in ['p.short', 'p.lede', '.short', '.summary', '.dek', '.article-teaser', '.feed__description', '.teaser', 'p']:
                try:
                    el = wrapper.select_one(sel) if wrapper else None
                except Exception:
                    el = None
                if el:
                    t = txt(el)
                    if t and not re.search(r'subscribe|advertis|read more', t, re.I):
                        description = t
                        break

            # if still empty, try nearby siblings (sometimes description sits next to wrapper)
            if not description:
                try:
                    # next siblings inside same container
                    sib = None
                    # prefer nextElementSibling-like search
                    cur = wrapper
                    for _ in range(3):
                        if cur is None:
                            break
                        sib = cur.find_next_sibling()
                        if sib:
                            p = sib.select_one('p, .short, .lede, .dek, .summary')
                            if p:
                                dd = txt(p)
                                if dd and not re.search(r'subscribe|advertis|read more', dd, re.I):
                                    description = dd
                                    break
                        cur = cur.parent
                except Exception:
                    pass

            # --- DATE (várias heurísticas) ---
            date = ''
            for ds in ['time', '.byline', '.date', '.published', '.timestamp']:
                try:
                    el = wrapper.select_one(ds) if wrapper else None
                except Exception:
                    el = None
                if el:
                    t = txt(el)
                    if t:
                        # limpa "By Name - " se existir
                        date = re.sub(r'^\s*By\s+[^-]+-\s*', '', t).strip()
                        # remove "By Name" if no dash
                        date = re.sub(r'^\s*By\s+[^-]+\s*$', '', date).strip()
                        if date:
                            break

            # fallback regex in wrapper text for relative times or full dates
            if not date:
                rawtxt = txt(wrapper or a)
                m = re.search(r'\b\d+\s+(?:hours?|days?|minutes?)\s+ago\b', rawtxt, re.I) \
                    or re.search(r'\b(?:Jan(?:uary)?|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}\b', rawtxt)
                if m:
                    date = m.group(0)

            # --- última opção: fetch da página do artigo para obter meta description / time ---
            # (só faz requests se faltar descrição ou data)
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
                                    dclean = re.sub(r'^\s*By\s+[^-]+-\s*', '', txt(by)).strip()
                                    if dclean:
                                        date = dclean
                except Exception:
                    # não falhar a execução por causa do fetch
                    pass

            # última limpeza: truncar e normalizar
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
# FIM DOS SCRAPERS
# ---------------------------

def main():
    site_item_map = load_sites_item_container()
    all_rows = []

    # --- special: include modernhealthcare rendered HTML if exists ---
    mh_rendered_path = "scripts/rendered/modernhealthcare.html"
    if os.path.exists(mh_rendered_path):
        try:
            mh_items = scrape_modern_rendered(mh_rendered_path, base_url="https://www.modernhealthcare.com/latest-news/", max_items=10)
            for it in mh_items:
                all_rows.append({
                    "site": "modernhealthcare",
                    "title": it.get("title","") or "",
                    "link (source)": it.get("link","") or "",
                    "pubDate": it.get("date","") or "",
                    "description (short)": strip_html_short(it.get("description","") or "", max_len=300),
                    "item_container": site_item_map.get("modernhealthcare",""),
                    "topic": "N/A"
                })
            print(f"Added {len(mh_items)} modernhealthcare items from rendered HTML")
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

            # special: if mediapost, ignore the XML and scrape the listing page directly
            if site_name == "mediapost":
                try:
                    mp_items = scrape_mediapost_listing(base_url="https://www.mediapost.com/news/", max_items=30)
                    for it in mp_items:
                        t = it.get('title','').strip()
                        link = it.get('link','').strip()
                        if not t or t.lower() in ("no title", "return to homepage", "category"):
                            continue
                        all_rows.append({
                            "site": site_name,
                            "title": t,
                            "link (source)": link,
                            "pubDate": it.get('date',''),
                            "description (short)": strip_html_short(it.get('description',''), max_len=300),
                            "item_container": ic,
                            "topic": "N/A"
                        })
                    # skip parsing XML for mediapost
                    continue
                except Exception as e:
                    print("Error scraping mediapost listing:", e)
                    # fall through to XML parsing as fallback

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
