#!/usr/bin/env python3
# scripts/generate_feeds.py
# Versão com scraping PMN/MAUDE mais robusto + debug para diagnosticar "0 items found".
# Requisitos: requests, beautifulsoup4, feedgen, python-dateutil, playwright

import os
import json
import re
import sys
from bs4 import BeautifulSoup
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlencode
import warnings
from dateutil import parser as dateparser
from dateutil import tz as date_tz
# playwright import (assume instalado no runner)
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
import time

# timezone parsing shim
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
SITES_JSON = os.path.join(ROOT, 'sites.json') if os.path.exists(os.path.join(ROOT, 'sites.json')) else os.path.join(ROOT, '..', 'sites.json')
FEEDS_DIR = os.path.join(ROOT, '..', 'feeds')
OUT_XLSX = os.path.join(ROOT, '..', 'feeds_summary.xlsx')

COMMON_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
}

_bad_href_re = re.compile(r'(^#|/help|/legal|cookie|privacy|terms|signin|login|settings|/consent|/preferences|/policies|mailto:)', re.I)

def load_sites():
    try:
        with open(SITES_JSON, 'r', encoding='utf-8') as fh:
            j = json.load(fh)
        return j.get('sites', [])
    except Exception as e:
        print('Failed to load sites.json:', e)
        return []

# ---------------- JSON helpers (mantidos) ----------------
def get_json_path_value(obj, path):
    if obj is None or not path:
        return None
    cur = obj
    if isinstance(path, str) and re.search(r'\s+OR\s+', path, flags=re.I):
        for p in re.split(r'\s+OR\s+', path, flags=re.I):
            v = get_json_path_value(obj, p.strip())
            if v not in (None, ''):
                return v
        return None
    parts = re.split(r'\.(?![^\[]*\])', path)
    for part in parts:
        if not part:
            continue
        m = re.match(r'([^\[]+)\[(\d+)\]$', part)
        if m:
            key = m.group(1)
            idx = int(m.group(2))
            if isinstance(cur, dict):
                cur = cur.get(key)
            else:
                return None
            if isinstance(cur, list):
                if idx < len(cur):
                    cur = cur[idx]
                else:
                    return None
            else:
                return None
        else:
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        if cur is None:
            return None
    return cur

def parse_field_from_json(entry, spec):
    if not spec or entry is None:
        return None
    v = get_json_path_value(entry, spec)
    if isinstance(v, list):
        flat = []
        for el in v:
            if isinstance(el, (str, int, float)):
                flat.append(str(el))
            elif isinstance(el, dict):
                for cand in ('device_name', 'name', 'brand_name', 'title', 'applicant'):
                    if cand in el and el[cand]:
                        flat.append(str(el[cand]))
                        break
        return ', '.join(flat) if flat else None
    if isinstance(v, dict):
        for cand in ('device_name', 'name', 'title', 'summary', 'applicant'):
            if cand in v and v[cand]:
                return str(v[cand])
        try:
            return json.dumps(v, ensure_ascii=False)
        except Exception:
            return None
    if v is None:
        return None
    return str(v)

# ---------------- HTTP helper ----------------
def fetch_url_response(url, timeout=20, session=None):
    s = session or requests
    headers = COMMON_HEADERS
    r = s.get(url, headers=headers, timeout=timeout)
    return r

def _page_looks_empty(html_text):
    if not html_text:
        return True
    low = html_text.lower()
    for token in ('0 records found', 'no records found', 'no matching records', 'no record found', 'no results found', 'please enter'):
        if token in low:
            return True
    return False

# ---------------- detail page resolvers (PMN / MAUDE) ----------------
def try_resolve_pmn_page(k_number, session, timeout=12):
    if not k_number:
        return None, None, None, None
    kn = str(k_number).strip()
    digits = ''.join(re.findall(r'\d+', kn))
    candidates = []
    if kn.upper().startswith('K'):
        candidates.append(kn.upper())
    if digits:
        candidates.append(digits.zfill(6))
        candidates.append(digits)
    candidates.append(kn)
    tried = set()
    for c in candidates:
        if not c or c in tried:
            continue
        tried.add(c)
        for url in (f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={c}",
                    f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID=K{c}"):
            try:
                r = session.get(url, headers=COMMON_HEADERS, timeout=timeout)
                status = r.status_code
                html = r.text if r.status_code == 200 else ''
                if status != 200:
                    continue
                if _page_looks_empty(html):
                    continue
                soup = BeautifulSoup(html, 'html.parser')
                title = ''
                h1 = soup.find('h1')
                if h1 and h1.get_text(strip=True):
                    title = h1.get_text(strip=True)
                else:
                    ttag = soup.find('title')
                    title = ttag.get_text(strip=True) if ttag else ''
                txt = soup.get_text(" ", strip=True)
                dec_match = re.search(r'Decision Date[:\s]*([A-Za-z0-9, \-/]+)', txt, re.IGNORECASE)
                decision_date = dec_match.group(1).strip() if dec_match else ''
                dec_iso = ''
                if decision_date:
                    try:
                        dec_iso = dateparser.parse(decision_date).isoformat()
                    except Exception:
                        dec_iso = decision_date
                # description heuristics
                desc = ''
                for lbl in ('Statement or Summary', 'Statement', 'Summary'):
                    node = soup.find(text=re.compile(re.escape(lbl), re.IGNORECASE))
                    if node:
                        parent = getattr(node, 'parent', None)
                        if parent:
                            nxt = parent.find_next('p')
                            if nxt and nxt.get_text(strip=True):
                                desc = nxt.get_text(" ", strip=True)
                                break
                if not desc:
                    body = soup.get_text(" ", strip=True)
                    desc = (body[:500] + '...') if len(body) > 500 else body
                return url, title or '', dec_iso or '', desc or ''
            except Exception:
                continue
    return None, None, None, None

def try_resolve_maude_page(mdr_id, product_code, session, timeout=12):
    if not mdr_id:
        return None, None, None, None
    pc = (product_code or '').strip()
    url = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={mdr_id}&pc={pc}"
    try:
        r = session.get(url, headers=COMMON_HEADERS, timeout=timeout)
        if r.status_code != 200:
            return None, None, None, None
        html = r.text
        if _page_looks_empty(html):
            return None, None, None, None
        soup = BeautifulSoup(html, 'html.parser')
        title = soup.find('title').get_text(strip=True) if soup.find('title') else ''
        txt = soup.get_text(" ", strip=True)
        date_match = re.search(r'Date Received[:\s]*([A-Za-z0-9, \-/]+)', txt, re.IGNORECASE)
        date_val = date_match.group(1).strip() if date_match else ''
        date_iso = ''
        if date_val:
            try:
                date_iso = dateparser.parse(date_val).isoformat()
            except Exception:
                date_iso = date_val
        desc = ''
        m = re.search(r'Description of Event or Problem[:\s]*(.{20,400})', txt, re.IGNORECASE)
        if m:
            desc = m.group(1).strip()
        if not desc:
            desc = (txt[:500] + '...') if len(txt) > 500 else txt
        return url, title, date_iso, desc
    except Exception:
        return None, None, None, None

# ---------------- Robust scraping of listing pages (Playwright) ----------------
def _playwright_headless_from_env():
    # if DEBUG=1 or DEBUG=true then run non-headless for debugging
    v = os.getenv('DEBUG', '').strip().lower()
    return not (v in ('1','true','on'))

def _safe_click_search(page):
    """ tenta clicar num botão 'Search' com vários fallbacks """
    try:
        # procurar input/button com value/texto Search
        btn = page.query_selector('input[type="submit"][value*="Search"], button:has-text("Search"), a:has-text("Search")')
        if btn:
            try:
                btn.click(timeout=5000)
                return True
            except Exception:
                try:
                    page.evaluate('(el)=>el.click()', btn)
                    return True
                except Exception:
                    pass
        # fallback: clicar por texto
        try:
            page.click('text="Search"', timeout=5000)
            return True
        except Exception:
            pass
    except Exception:
        pass
    return False

def _try_set_per_page(page, per_page):
    """ tenta encontrar select relacionado com 'Records per' / 'Results per' e seleccionar per_page """
    if not per_page:
        return False
    try:
        per_page = str(per_page)
    except Exception:
        per_page = None
    if not per_page:
        return False
    try:
        # tentativas com seletores plausíveis
        locs = [
            'select[name="recordsPerReportPage"]',
            'select[name="recordsPerPage"]',
            'select[name="perPage"]',
            'select#perPage',
            'select'
        ]
        for sel in locs:
            try:
                locator = page.locator(sel)
                if locator.count() and locator.first().is_visible():
                    try:
                        locator.first().select_option(per_page)
                        return True
                    except Exception:
                        # alternativa: set value via evaluate
                        page.evaluate(f"""() => {{
                            var s = document.querySelector('{sel}');
                            if(s) s.value = '{per_page}';
                        }}""")
                        return True
            except Exception:
                continue
        # Try to locate by label text
        try:
            label_loc = page.locator('text="Records per Report Page", text="Results per Page", text="Records per Page"').first()
            if label_loc:
                # procurar select próximo
                el = label_loc.evaluate_handle('el => { var p = el.parentElement || el.closest("form") || document; return p.querySelector("select") }')
                if el:
                    # select_handle = el.as_element()
                    page.evaluate(f"""(sel)=>{{ sel.value = '{per_page}'; }}""", el)
                    return True
        except Exception:
            pass
    except Exception:
        pass
    return False

def scrape_pmn_with_playwright(scrape_url, max_items=100, pages_to_try=10, wait_selector=None, timeout=60000, per_page=None):
    items = []
    ids_seen = []
    headless = _playwright_headless_from_env()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        # aumentar default navigation timeout
        page.set_default_navigation_timeout(timeout if timeout else 60000)
        page.set_default_timeout(timeout if timeout else 60000)
        for pnum in range(1, pages_to_try + 1):
            if re.search(r'PAGENUM=\d+', scrape_url, flags=re.IGNORECASE):
                url = re.sub(r'PAGENUM=\d+', f'PAGENUM={pnum}', scrape_url, flags=re.IGNORECASE)
            else:
                sep = '&' if '?' in scrape_url else '?'
                url = f"{scrape_url}{sep}PAGENUM={pnum}"

            try:
                # tentar domcontentloaded primeiro (networkidle pode falhar)
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                except PlaywrightTimeoutError:
                    try:
                        page.goto(url, wait_until="load", timeout=timeout)
                    except Exception as e:
                        print("PLAYWRIGHT PMN: initial goto failed", e)
                        continue
                # tentativa de ajustar per_page e clicar Search (se estiver na página inicial)
                try:
                    _try_set_per_page(page, per_page)
                    _safe_click_search(page)
                except Exception:
                    pass

                # esperar por algo que indique resultados (links para pmn.cfm)
                if not wait_selector:
                    wait_selector = 'a[href*="pmn.cfm?ID="], a[href*="pmn.cfm?ID=K"]'
                try:
                    page.wait_for_selector(wait_selector, timeout=10000)
                except PlaywrightTimeoutError:
                    # OK: não abortar; pode haver links gerados dinamicamente
                    pass

            except PlaywrightTimeoutError:
                # tenta novamente sem esperar networkidle
                print("PLAYWRIGHT PMN: goto timeout; continuing to next try")
            except Exception as e:
                print("PLAYWRIGHT PMN: goto failed", e)

            # extrai links do DOM
            try:
                anchors = page.query_selector_all('a')
                for a in anchors:
                    try:
                        href = a.get_attribute('href') or ''
                        if 'pmn.cfm' in href:
                            m = re.search(r'pmn\.cfm\?ID=([A-Za-z0-9\-]+)', href, flags=re.I)
                            if m:
                                rid = m.group(1)
                                if rid not in ids_seen:
                                    ids_seen.append(rid)
                    except Exception:
                        continue
            except Exception:
                pass

            # fallback scanning do conteúdo
            try:
                content = page.content()
                for m in re.findall(r'pmn\.cfm\?ID=([A-Za-z0-9\-]+)', content, flags=re.I):
                    if m not in ids_seen:
                        ids_seen.append(m)
            except Exception:
                pass

            if len(ids_seen) >= max_items:
                break

            time.sleep(0.2)

        browser.close()

    # resolve detalhes das IDs recolhidas
    session = requests.Session()
    for rid in ids_seen[:max_items]:
        try:
            url_detail, title, date_iso, desc = try_resolve_pmn_page(rid, session)
            link = url_detail or f'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={rid}'
            items.append({'title': title or '', 'link': link, 'description': desc or '', 'date': date_iso or ''})
        except Exception:
            continue

    # ordena por date desc se houver datas
    def _key(it):
        try:
            return dateparser.parse(it.get('date') or '')
        except Exception:
            return datetime.min
    items = sorted(items, key=_key, reverse=True)
    return items[:max_items]

def scrape_maude_with_playwright(scrape_url, max_items=100, pages_to_try=10, wait_selector=None, timeout=60000, per_page=None):
    """
    Renderiza com Playwright a página MAUDE e extrai links com 'Detail.CFM?MDRFOI__ID='.
    Tenta seleccionar per_page e clicar Search antes de extrair.
    """
    items = []
    found = []
    headless = _playwright_headless_from_env()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context()
        page = context.new_page()
        page.set_default_navigation_timeout(timeout if timeout else 60000)
        page.set_default_timeout(timeout if timeout else 60000)
        for pnum in range(1, pages_to_try + 1):
            if re.search(r'PAGENUM=\d+', scrape_url, flags=re.IGNORECASE):
                url = re.sub(r'PAGENUM=\d+', f'PAGENUM={pnum}', scrape_url, flags=re.IGNORECASE)
            else:
                sep = '&' if '?' in scrape_url else '?'
                url = f"{scrape_url}{sep}PAGENUM={pnum}"

            try:
                try:
                    page.goto(url, wait_until="domcontentloaded", timeout=timeout)
                except PlaywrightTimeoutError:
                    try:
                        page.goto(url, wait_until="load", timeout=timeout)
                    except Exception as e:
                        print("PLAYWRIGHT MAUDE: initial goto failed", e)
                        continue

                # tenta seleccionar per_page e clicar Search
                try:
                    _try_set_per_page(page, per_page)
                    _safe_click_search(page)
                except Exception:
                    pass

                if not wait_selector:
                    wait_selector = 'a[href*="Detail.CFM?MDRFOI__ID="]'
                try:
                    page.wait_for_selector(wait_selector, timeout=10000)
                except PlaywrightTimeoutError:
                    pass

            except PlaywrightTimeoutError:
                print("PLAYWRIGHT MAUDE: goto timeout; continuing")
            except Exception as e:
                print("PLAYWRIGHT MAUDE: goto failed", e)

            # extrai anchors
            try:
                anchors = page.query_selector_all('a')
                for a in anchors:
                    try:
                        href = a.get_attribute('href') or ''
                        if 'Detail.CFM' in href or 'MDRFOI__ID' in href:
                            m = re.search(r'MDRFOI__ID=([0-9]+)', href, flags=re.I)
                            pc = ''
                            if m:
                                idnum = m.group(1)
                                pc_m = re.search(r'pc=([A-Za-z0-9]+)', href, flags=re.I)
                                if pc_m:
                                    pc = pc_m.group(1)
                                if not any(x[0] == idnum for x in found):
                                    found.append((idnum, pc))
                    except Exception:
                        continue
            except Exception:
                pass

            # fallback scan content
            try:
                content = page.content()
                for m in re.findall(r'Detail\.CFM\?MDRFOI__ID=([0-9]+)(?:&pc=([A-Za-z0-9]+))?', content, flags=re.I):
                    idnum = m[0]
                    pc = m[1] or ''
                    if not any(x[0] == idnum for x in found):
                        found.append((idnum, pc))
            except Exception:
                pass

            if len(found) >= max_items:
                break

            time.sleep(0.2)

        browser.close()

    # resolve cada par id/pc com try_resolve_maude_page
    session = requests.Session()
    for idnum, pc in found[:max_items]:
        try:
            url_detail, title, date_iso, desc = try_resolve_maude_page(idnum, pc, session)
            link = url_detail or f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={idnum}&pc={pc}"
            mdr_id_num = int(re.sub(r'\D', '', str(idnum))) if re.search(r'\d', str(idnum)) else None
            items.append({'title': title or '', 'link': link, 'description': desc or '', 'date': date_iso or '', 'mdr_id_num': mdr_id_num})
        except Exception:
            continue

    # ordenar por mdr_id desc
    items = sorted(items, key=lambda it: it.get('mdr_id_num') or 0, reverse=True)
    return items[:max_items]


# ---------------- JSON extractor (mantive tua lógica) ----------------
def extract_items_from_json_obj(jobj, cfg):
    items = []
    container = cfg.get('item_container') or 'results'
    containers = [c.strip() for c in container.split(',')] if isinstance(container, str) else container
    nodes = None
    for cand in containers:
        try:
            val = get_json_path_value(jobj, cand)
            if isinstance(val, list):
                nodes = val
                break
        except Exception:
            continue
    if nodes is None:
        nodes = jobj.get('results') if isinstance(jobj.get('results'), list) else []
    print(f"Detected JSON container '{containers}' -> {len(nodes)} items")
    session = requests.Session()
    for entry in nodes:
        try:
            title = parse_field_from_json(entry, cfg.get('title') or '') or ''
            raw_link_val = parse_field_from_json(entry, cfg.get('link') or '') or ''
            desc = parse_field_from_json(entry, cfg.get('description') or '') or ''
            if not desc:
                for cand in ('statement_or_summary','summary','event_description','mdr_text'):
                    v = parse_field_from_json(entry, cand)
                    if v:
                        desc = v
                        break
            date_raw = parse_field_from_json(entry, cfg.get('date') or '') or ''
            if not date_raw:
                for cand in ('decision_date','date_received','report_date','date_report','date_of_event'):
                    v = parse_field_from_json(entry, cand)
                    if v:
                        date_raw = v
                        break
            date = ''
            date_obj = None
            if date_raw:
                s = str(date_raw).strip()
                if re.match(r'^\d{8}$', s):
                    date = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
                    try:
                        date_obj = dateparser.parse(date)
                    except Exception:
                        date_obj = None
                else:
                    try:
                        date_obj = dateparser.parse(s)
                        date = date_obj.isoformat()
                    except Exception:
                        date = s
            link = ''
            mdr_id_num = None
            url_lower = (cfg.get('url') or '').lower()
            if 'api.fda.gov' in url_lower:
                if '/device/510k' in url_lower:
                    knum = parse_field_from_json(entry, 'k_number') or raw_link_val or ''
                    if cfg.get('json_detail_fetch', False) and knum:
                        resolved_url, rtitle, rdate, rdesc = try_resolve_pmn_page(knum, session)
                        if resolved_url:
                            link = resolved_url
                            if rtitle and not title: title = rtitle
                            if rdate and not date: date = rdate
                            if rdesc and not desc: desc = rdesc
                    if not link and knum:
                        digits = ''.join(re.findall(r'\d+', str(knum)))
                        if digits:
                            link = f'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={digits.zfill(6)}'
                        else:
                            link = f'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={knum}'
                elif '/device/event' in url_lower:
                    cand_id = parse_field_from_json(entry, 'mdr_report_key') or parse_field_from_json(entry, 'report_number') or parse_field_from_json(entry, 'event_key') or raw_link_val or ''
                    product_code = parse_field_from_json(entry, 'device[0].device_report_product_code') or parse_field_from_json(entry, 'product_code') or ''
                    m = re.findall(r'\d+', str(cand_id))
                    if m:
                        longest = max(m, key=len)
                        try:
                            mdr_id_num = int(longest)
                        except Exception:
                            mdr_id_num = None
                    if cfg.get('json_detail_fetch', False) and cand_id:
                        resolved_url, rtitle, rdate, rdesc = try_resolve_maude_page(cand_id, product_code, session)
                        if resolved_url:
                            link = resolved_url
                            if rtitle and not title: title = rtitle
                            if rdate and not date: date = rdate
                            if rdesc and not desc: desc = rdesc
                    if not link and cand_id:
                        pc = product_code or ''
                        link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={cand_id}&pc={pc}"
                        if mdr_id_num is None and m:
                            try:
                                mdr_id_num = int(max(m, key=len))
                            except Exception:
                                mdr_id_num = None
            if not link and raw_link_val:
                if str(raw_link_val).lower().startswith('http'):
                    link = raw_link_val
                else:
                    link = urljoin(cfg.get('url',''), str(raw_link_val))
            full_text = (title or '') + ' ' + (desc or '') + ' ' + json.dumps(entry, ensure_ascii=False)[:1500]
            item = {
                'title': title or '',
                'link': link or '',
                'description': desc or '',
                'date': date or '',
                'date_obj': date_obj,
                'full_text': full_text or '',
                '_raw_entry': entry
            }
            if mdr_id_num is not None:
                item['mdr_id_num'] = mdr_id_num
            items.append(item)
        except Exception:
            continue

    # ordering (same semantics)
    sort_cfg = cfg.get('json_sort') or None
    if not sort_cfg:
        if '/device/510k' in (cfg.get('url') or '').lower():
            sort_cfg = 'decision_date:desc'
        elif '/device/event' in (cfg.get('url') or '').lower():
            sort_cfg = 'mdr_id:desc'
    if sort_cfg:
        field, _, direction = sort_cfg.partition(':')
        reverse = (direction.lower() == 'desc')
        if field in ('mdr_id', 'mdr_id_num'):
            items = sorted(items, key=lambda it: it.get('mdr_id_num') or 0, reverse=reverse)
        elif field in ('decision_date','date_received','date','report_date'):
            def _k(it):
                try:
                    if it.get('date_obj') is not None:
                        return it.get('date_obj')
                    if it.get('date'):
                        return dateparser.parse(it.get('date'))
                except Exception:
                    pass
                return datetime.min
            items = sorted(items, key=_k, reverse=reverse)
        else:
            items = sorted(items, key=lambda it: (it.get(field) or '').lower(), reverse=reverse)

    max_items = cfg.get('max_items') or cfg.get('max') or 100
    try:
        max_items = int(max_items)
    except Exception:
        max_items = 100
    if len(items) > max_items:
        items = items[:max_items]
    print(f"After sorting/truncation returning {len(items)} items (max_items={max_items})")
    return items

# ---------------- small HTML extractor fallback ----------------
def extract_items_from_html(html_text, cfg):
    soup = BeautifulSoup(html_text, 'html.parser')
    container_sel = cfg.get('item_container') or 'article'
    nodes = []
    for sel in [s.strip() for s in str(container_sel).split(',')]:
        try:
            found = soup.select(sel)
            if found:
                nodes.extend(found)
        except Exception:
            continue
    items = []
    for node in nodes:
        try:
            title = node.get_text(" ", strip=True)[:300]
            link = ''
            a = node.find('a', href=True)
            if a:
                link = urljoin(cfg.get('url',''), a.get('href'))
            desc = ''
            p = node.find('p')
            if p:
                desc = p.get_text(" ", strip=True)
            date = ''
            items.append({'title': title, 'link': link, 'description': desc, 'date': date, 'full_text': title + ' ' + desc})
        except Exception:
            continue
    return items

# ---------------- dedupe / feed builder ----------------
def dedupe_items(items):
    unique = {}
    out = []
    for it in (items or []):
        key = it.get('link') or (it.get('title', '')[:200].strip().lower())
        key = (key or '').strip()
        if key not in unique:
            unique[key] = True
            out.append(it)
    return out

def build_feed(name, cfg, items):
    fg = FeedGenerator()
    fg.title(name)
    fg.link(href=cfg.get('url', ''), rel='alternate')
    fg.description(f'Feed gerado para {name}')
    fg.generator('generate_feeds.py')
    max_items = cfg.get('max_items') or cfg.get('max') or None
    if max_items:
        try:
            max_items = int(max_items)
        except Exception:
            max_items = None
    if max_items and len(items) > max_items:
        items = items[:max_items]
    count = 0
    for it in items:
        try:
            fe = fg.add_entry()
            fe.title(it.get('title') or 'No title')
            if it.get('link'):
                try:
                    fe.link(href=it.get('link'))
                except Exception:
                    pass
            desc_to_use = it.get('description') or ''
            if it.get('matched_reason'):
                desc_to_use = (desc_to_use + ' ').strip() + f" [MatchedReason: {it.get('matched_reason')}]"
            fe.description(desc_to_use)
            if it.get('date'):
                try:
                    dt = dateparser.parse(it.get('date'))
                    fe.pubDate(dt)
                except Exception:
                    try:
                        fe.pubDate(it.get('date'))
                    except Exception:
                        pass
            count += 1
        except Exception:
            continue
    outdir = os.path.join(ROOT, '..', 'feeds')
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f'{name}.xml')
    fg.rss_file(outpath)
    print(f'Wrote {outpath} ({count} entries)')

# ---------------- main ----------------
def main():
    sites = load_sites()
    print(f'Loaded {len(sites)} site configurations from {SITES_JSON}')
    for cfg in sites:
        name = cfg.get('name')
        url = cfg.get('url')
        if not name or not url:
            continue
        print(f'--- Processing {name} ({url}) ---')

        u = url
        parsed = urlparse(u)
        if parsed.netloc == 'api.fda.gov' and not parsed.query:
            params = {'limit': 100}
            if '/device/510k' in parsed.path:
                params['sort'] = 'decision_date:desc'
            elif '/device/event' in parsed.path:
                params['sort'] = 'date_received:desc'
            u = u + ('?' + urlencode(params))
            print(f'Adjusted API URL to: {u}')

        prefer_scrape = bool(cfg.get('prefer_scrape', False))
        scrape_url = cfg.get('scrape_url') or ''
        items = []

        # determine numeric settings from cfg (fallbacks)
        try:
            cfg_max_items = int(cfg.get('max_items') or cfg.get('max') or 100)
        except Exception:
            cfg_max_items = 100
        try:
            cfg_pages_to_try = int(cfg.get('pages_to_try') or 8)
        except Exception:
            cfg_pages_to_try = 8

        # get per_page for UI interaction
        cfg_per_page = cfg.get('per_page') or cfg.get('perpage') or None

        if prefer_scrape and scrape_url:
            print(f'Prefer scrape enabled for {name}, scraping {scrape_url} ...')
            try:
                if 'cfpmn' in scrape_url.lower() or 'pmn.cfm' in scrape_url.lower():
                    items = scrape_pmn_with_playwright(scrape_url, max_items=cfg_max_items, pages_to_try=cfg_pages_to_try, per_page=cfg_per_page, timeout=120000)
                elif 'cfmaude' in scrape_url.lower() or 'results.cfm' in scrape_url.lower() or 'search.cfm' in scrape_url.lower() or 'detail.cfm' in scrape_url.lower():
                    items = scrape_maude_with_playwright(scrape_url, max_items=cfg_max_items, pages_to_try=cfg_pages_to_try, per_page=cfg_per_page, timeout=120000)
                else:
                    # fallback: try a single requests fetch then inspect to decide
                    try:
                        sess = requests.Session()
                        r = sess.get(scrape_url, headers={'User-Agent':'Mozilla/5.0'}, timeout=18)
                        r.raise_for_status()
                        html = r.text
                        if 'pmn' in html.lower():
                            items = scrape_pmn_with_playwright(scrape_url, max_items=cfg_max_items, pages_to_try=cfg_pages_to_try, per_page=cfg_per_page, timeout=120000)
                        elif 'maude' in html.lower() or 'detail.cfm' in html.lower():
                            items = scrape_maude_with_playwright(scrape_url, max_items=cfg_max_items, pages_to_try=cfg_pages_to_try, per_page=cfg_per_page, timeout=120000)
                        else:
                            print(f'Prefer scrape: page does not appear to contain PMN/MAUDE markers (html_len={len(html)})')
                            items = []
                    except Exception as e:
                        print('Prefer scrape: error fetching scrape_url with requests', e)
                        items = []
            except ImportError as ie:
                print('Prefer scrape: Playwright not available (ImportError). Falling back to non-rendered fetch.', ie)
                try:
                    resp = fetch_url_response(scrape_url)
                    txt = resp.text if hasattr(resp,'text') else str(resp)
                    if txt and (txt.strip().startswith('{') or 'pmn' in txt.lower() or 'maude' in txt.lower()):
                        if 'pmn' in txt.lower():
                            items = extract_items_from_html(txt, cfg)
                        else:
                            items = extract_items_from_html(txt, cfg)
                    else:
                        items = []
                except Exception as e:
                    print('Fallback fetch error:', e)
                    items = []
            except Exception as e:
                print('Prefer scrape: unexpected error while scraping with Playwright:', e)
                items = []
        else:
            # API/json path (original logic)
            try:
                print(f'Fetching {u} via requests...')
                resp = fetch_url_response(u)
                content_type = resp.headers.get('Content-Type','') if hasattr(resp,'headers') else ''
                txt = resp.text if hasattr(resp,'text') else str(resp)
            except Exception as e:
                print(f'Request error for {u}: {e}')
                txt = ''
                content_type = ''
            if txt:
                is_json = False
                try:
                    if 'application/json' in content_type.lower():
                        is_json = True
                    else:
                        s = txt.lstrip()
                        if s.startswith('{') or s.startswith('['):
                            _ = json.loads(s)
                            is_json = True
                except Exception:
                    is_json = False
                if is_json:
                    print(f"Detected JSON response for {name}; parsing with JSON handler")
                    try:
                        jobj = json.loads(txt)
                        items = extract_items_from_json_obj(jobj, cfg)
                    except Exception as e:
                        print('Error parsing JSON response:', e)
                        items = []
                else:
                    try:
                        items = extract_items_from_html(txt, cfg)
                    except Exception as e:
                        print('Error parsing HTML:', e)
                        items = []
            else:
                items = []

        print(f'Found {len(items)} items for {name} (raw)')

        force_latest = bool(cfg.get('force_latest', False))
        matched = []
        if force_latest and items:
            max_items = cfg_max_items
            matched = items[:max_items]
            print(f'force_latest=True for {name}: taking top {len(matched)} items (no filters applied)')
        else:
            kw = cfg.get('filters', {}).get('keywords', []) or []
            print(f'Applying {len(kw)} keyword filters for {name}')
            for it in items:
                keep = True
                reason = None
                if kw:
                    keep = False
                    for k in kw:
                        kl = k.lower()
                        if kl in (it.get('title') or '').lower() or kl in (it.get('description') or '').lower() or kl in (it.get('full_text') or '').lower() or kl in (it.get('link') or '').lower():
                            keep = True
                            reason = f"keyword '{k}' matched"
                            break
                if keep:
                    if reason:
                        it['matched_reason'] = reason
                    matched.append(it)
        print(f'{len(matched)} items matched for {name}')

        deduped = dedupe_items(matched)
        build_feed(name, cfg, deduped)

    print('All done.')

if __name__ == '__main__':
    main()
