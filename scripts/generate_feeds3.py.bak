#!/usr/bin/env python3
# scripts/generate_feeds.py  (adaptado para JSON APIs + heurísticas FDA)
# Gere feeds RSS simples a partir de sites listados em sites.json
# Requisitos: requests, beautifulsoup4, feedgen, python-dateutil

import os
import json
import re
import sys
from bs4 import BeautifulSoup
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import glob

# small compatibility shim to silence UnknownTimezoneWarning for "ET"
import warnings
from dateutil import parser as dateparser
from dateutil import tz as date_tz

# try to silence UnknownTimezoneWarning specifically if available
try:
    from dateutil import _parser as _dateutil__parser
    UnknownTimezoneWarning = _dateutil__parser.UnknownTimezoneWarning
    warnings.filterwarnings("ignore", category=UnknownTimezoneWarning)
except Exception:
    warnings.filterwarnings("ignore", message="tzname .* identified but not understood")

# map "ET" -> America/New_York by default for parse
_default_tzinfos = {"ET": date_tz.gettz("America/New_York")}

_original_parse = dateparser.parse


def _parse_with_default_tzinfos(timestr, *args, **kwargs):
    if "tzinfos" not in kwargs or kwargs["tzinfos"] is None:
        kwargs["tzinfos"] = _default_tzinfos
    return _original_parse(timestr, *args, **kwargs)


dateparser.parse = _parse_with_default_tzinfos

ROOT = os.path.dirname(__file__)
SITES_JSON = os.path.join(ROOT, 'sites.json')
FEEDS_DIR = os.path.join(ROOT, '..', 'feeds')
OUT_XLSX = os.path.join(ROOT, '..', 'feeds_summary.xlsx')

# regex para filtrar hrefs inúteis (ajusta conforme necessário)
_bad_href_re = re.compile(r'(^#|/help|/legal|cookie|privacy|terms|signin|login|settings|/consent|/preferences|/policies|mailto:)', re.I)

# ---------------- Helpers JSON field parsing ----------------
def get_json_path_value(obj, path):
    """
    path: dot-separated keys, e.g. 'openfda.device_name' or 'device[0].device_name'
    returns string or list or None
    """
    if obj is None:
        return None
    cur = obj
    # support brackets like device[0]
    for part in path.split('.'):
        if isinstance(cur, list):
            # If current is list, try to use int index if provided, else map over elements
            try:
                idx = int(part)
                cur = cur[idx]
                continue
            except Exception:
                # try to apply part to each element and return first non-null
                vals = []
                for el in cur:
                    try:
                        if isinstance(el, dict) and part in el:
                            vals.append(el.get(part))
                        else:
                            # can't find
                            pass
                    except Exception:
                        pass
                if vals:
                    cur = vals
                    continue
                else:
                    return None
        # handle key with array index like 'device[0]'
        m = re.match(r'([^\[]+)\[(\d+)\]$', part)
        if m:
            key = m.group(1)
            idx = int(m.group(2))
            if isinstance(cur, dict) and key in cur:
                arr = cur.get(key)
                if isinstance(arr, list) and len(arr) > idx:
                    cur = arr[idx]
                    continue
                else:
                    return None
            else:
                return None
        # normal dict access
        if isinstance(cur, dict) and part in cur:
            cur = cur.get(part)
            continue
        else:
            # can't follow path
            return None
    return cur

def parse_field_from_json(item, spec):
    """
    spec: e.g. "openfda.device_name OR device_name"
    tenta as alternativas em ordem e retorna primeira que tenha valor (string).
    Se for lista junta com ', '.
    """
    if not spec:
        return ''
    # split by case-insensitive ' OR '
    parts = [p.strip() for p in re.split(r'\s+OR\s+|\s+\|\s+|\s*,\s*', spec, flags=re.I) if p.strip()]
    for p in parts:
        v = get_json_path_value(item, p)
        if v is None:
            continue
        if isinstance(v, list):
            # flatten simple primitives
            flat = []
            for el in v:
                if isinstance(el, (str, int, float)):
                    flat.append(str(el))
                elif isinstance(el, dict):
                    # pick possible 'name' or 'device_name'
                    for cand in ('device_name','name','brand_name'):
                        if cand in el:
                            flat.append(str(el[cand]))
                            break
            if flat:
                return ', '.join(flat)
            else:
                continue
        if isinstance(v, dict):
            # try common fields
            for cand in ('device_name','name','title','summary'):
                if cand in v and v[cand]:
                    return str(v[cand])
            # fallback to JSON dump small
            try:
                s = json.dumps(v, ensure_ascii=False)
                return s
            except Exception:
                continue
        # primitive
        if str(v).strip():
            return str(v).strip()
    return ''

# ---------------- JSON -> items extraction ----------------
# ---------------- new helpers for resolving FDA detail pages ----------------
def _is_page_not_found_or_empty(html_text):
    # heurísticas simples para detectar "0 records found" ou páginas vazias do site FDA
    if not html_text:
        return True
    low = html_text.lower()
    for token in ('0 records found', 'no records found', 'no matching records', 'no record found', 'no data found'):
        if token in low:
            return True
    # também rejeita páginas muito curtas
    if len(low) < 500:
        # pequenas páginas podem ser válidas, mas para o caso FDA muitas pages são maiores
        return False
    return False

def try_resolve_pmn_page(k_number, session, debug=False):
    """
    Tenta várias formas de construir um link pmn.cfm para um dado k_number.
    Retorna tuple (url, title, decision_date) do primeiro sucesso, ou (None, None, None).
    """
    if not k_number:
        return None, None, None

    # clean
    kn = str(k_number).strip()
    # normalize: if already like 'K223369' keep; also extract digits
    digits = ''.join(re.findall(r'\d+', kn))
    candidates = []
    # candidate 1: K + digits (if user provided 'K...' ou 'k...')
    if kn.upper().startswith('K'):
        candidates.append(kn.upper())
    # candidate 2: digits only zero-padded 6
    if digits:
        candidates.append(digits.zfill(6))
    # candidate 3: raw digits (no padding)
    if digits:
        candidates.append(digits)
    # candidate 4: the raw k_number
    candidates.append(kn)

    tried = set()
    for c in candidates:
        if not c or c in tried:
            continue
        tried.add(c)
        # try both patterns: ID={c} and ID=K{c} (some pages expect Kxxxxx)
        patterns = [f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={c}",
                    f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID=K{c}"]
        for url in patterns:
            try:
                r = session.get(url, timeout=12)
                if r.status_code != 200:
                    continue
                html = r.text
                # quick check for "no records"
                if '0 records found' in html.lower() or 'no records found' in html.lower():
                    continue
                # heuristic success: parse title and decision date
                # title often in a <h1> or in <div class="...title..."> - fallback to <title>
                soup = BeautifulSoup(html, 'html.parser')
                title = None
                # try common places
                h1 = soup.find('h1')
                if h1 and h1.get_text(strip=True):
                    title = h1.get_text(strip=True)
                if not title:
                    # some PMN pages use <font size> or <div class="bodycopy"> - fallback to html <title>
                    ttag = soup.find('title')
                    if ttag and ttag.get_text(strip=True):
                        title = ttag.get_text(strip=True)
                # decision date heuristics: search for text "Decision Date" or 'Decision Date:'
                dec_date = None
                txt = soup.get_text(" ", strip=True)
                m = re.search(r'Decision Date[:\s]*([A-Za-z0-9, \-/]+)', txt, re.IGNORECASE)
                if m:
                    dec_date = m.group(1).strip()
                # other fallback: 'Decision:' token
                if not dec_date:
                    m2 = re.search(r'Decision[:\s]*([A-Za-z0-9, \-/]+)', txt, re.IGNORECASE)
                    if m2:
                        dec_date = m2.group(1).strip()
                # if we found a title or content assume it is valid
                if title or (dec_date and len(dec_date) > 3):
                    return url, (title or ''), (dec_date or '')
            except Exception:
                continue
    return None, None, None

def try_resolve_maude_page(mdr_id, product_code, session, debug=False):
    """
    Tenta construir Detail.CFM?MDRFOI__ID={mdr_id}&pc={product_code}
    retorna (url, title, date_received)
    """
    if not mdr_id:
        return None, None, None
    pcs = product_code or ''
    # Try exact template requested by user
    url = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={mdr_id}&pc={pcs}"
    try:
        r = session.get(url, timeout=12)
        if r.status_code == 200:
            html = r.text
            if '0 records found' in html.lower() or 'no records found' in html.lower():
                return None, None, None
            soup = BeautifulSoup(html, 'html.parser')
            # title heuristics
            title = ''
            ttag = soup.find('title')
            if ttag:
                title = ttag.get_text(strip=True)
            # date: search for 'Date Received' or 'Event Date' text
            txt = soup.get_text(" ", strip=True)
            date_match = re.search(r'Date Received[:\s]*([A-Za-z0-9,\-/]+)', txt, re.IGNORECASE)
            if not date_match:
                date_match = re.search(r'Date of Event[:\s]*([A-Za-z0-9,\-/]+)', txt, re.IGNORECASE)
            date_val = date_match.group(1).strip() if date_match else ''
            return url, title, date_val
    except Exception:
        return None, None, None
    return None, None, None


# ---------------- Updated JSON extraction with resolution + ordering ----------------
# ----------------- helpers para resolver páginas FDA -----------------
def _page_looks_empty(html_text):
    if not html_text:
        return True
    low = html_text.lower()
    for token in ('0 records found', 'no records found', 'no matching records', 'no record found'):
        if token in low:
            return True
    # não considerar uma página curta automaticamente vazia; deixamos heurística simples
    return False

def try_resolve_pmn_page(k_number, session, debug=False):
    """
    Tenta construir pmn.cfm válido a partir de k_number (ex: 'K223369' ou '223369').
    Retorna (url, title, decision_date, description) ou (None, None, None, None).
    """
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
        patterns = [
            f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={c}",
            f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID=K{c}"
        ]
        for url in patterns:
            try:
                r = session.get(url, timeout=12)
                if r.status_code != 200:
                    continue
                html = r.text
                if _page_looks_empty(html):
                    continue
                soup = BeautifulSoup(html, 'html.parser')
                # try common title positions
                title = ''
                h1 = soup.find('h1')
                if h1 and h1.get_text(strip=True):
                    title = h1.get_text(strip=True)
                if not title:
                    ttag = soup.find('title')
                    title = ttag.get_text(strip=True) if ttag else ''

                # decision date heuristics
                txt = soup.get_text(" ", strip=True)
                dec_match = re.search(r'Decision Date[:\s]*([A-Za-z0-9, \-/]+)', txt, re.IGNORECASE)
                decision_date = dec_match.group(1).strip() if dec_match else ''
                # description heuristics: summary near 'Summary' or 'Statement' headings
                desc = ''
                # procurar por rótulos "Summary" ou "Statement of" ou "Summary:" e apanhar parágrafo seguinte
                for lbl in ('Summary', 'Statement', 'Statement or Summary', 'Statement of'):
                    node = soup.find(text=re.compile(r'\b' + re.escape(lbl) + r'\b', re.IGNORECASE))
                    if node:
                        # pega próximo parágrafo ou próximo elemento de texto
                        parent = getattr(node, 'parent', None)
                        if parent:
                            nxt = parent.find_next('p')
                            if nxt and nxt.get_text(strip=True):
                                desc = nxt.get_text(" ", strip=True)
                                break
                # fallback: usar primeiras 300 chars do text se desc vazio
                if not desc:
                    body = soup.get_text(" ", strip=True)
                    desc = (body[:500] + '...') if len(body) > 500 else body

                return url, title or '', decision_date or '', desc or ''
            except Exception:
                continue
    return None, None, None, None


def try_resolve_maude_page(mdr_id, product_code, session, debug=False):
    """
    Tenta construir Detail.CFM link para MAUDE e validar.
    Retorna (url, title, date_received, description) ou (None,...).
    """
    if not mdr_id:
        return None, None, None, None
    pc = (product_code or '').strip()
    url = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={mdr_id}&pc={pc}"
    try:
        r = session.get(url, timeout=12)
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
        desc = ''
        # procurar por 'Description of Event or Problem' no texto mdr_text
        m = re.search(r'Description of Event or Problem[:\s]*(.{20,400})', txt, re.IGNORECASE)
        if m:
            desc = m.group(1).strip()[:500]
        if not desc:
            desc = (txt[:500] + '...') if len(txt) > 500 else txt
        return url, title, date_val, desc
    except Exception:
        return None, None, None, None

# ---------------- helpers para extrair campos JSON ----------------
def get_json_path_value(j, path):
    """
    Permite paths simples com '.' e índices tipo device[0].field
    ou simplesmente nomes alternados 'a OR b' -> tenta primeiro que exista.
    """
    if not path:
        return None
    # suportar 'OR' alternativo
    if ' OR ' in path:
        for p in [s.strip() for s in path.split(' OR ')]:
            v = get_json_path_value(j, p)
            if v is not None and v != '':
                return v
        return None
    # se for uma lista de campos separados por ',' devolve primeira não-vazia (não usado aqui)
    if isinstance(path, str) and ',' in path:
        for p in [s.strip() for s in path.split(',')]:
            v = get_json_path_value(j, p)
            if v is not None and v != '':
                return v
        return None
    # path com pontos e índices
    cur = j
    for part in re.split(r'\.(?![^\[]*\])', path):  # split on dots not inside []
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
    if not spec:
        return None
    if isinstance(spec, str):
        return get_json_path_value(entry, spec)
    return None


# ---------------- nova função JSON -> items ----------------
def extract_items_from_json_obj(jobj, cfg):
    items = []
    container = cfg.get('item_container') or 'results'
    # supporte strings com vírgula
    if isinstance(container, str):
        containers = [c.strip() for c in container.split(',') if c.strip()]
    else:
        containers = container if isinstance(container, list) else [container]

    # localizar o nodo certo
    nodes = None
    for cand in containers:
        val = get_json_path_value(jobj, cand) if isinstance(cand, str) else None
        if isinstance(val, list):
            nodes = val
            break
    if nodes is None:
        nodes = jobj.get('results') if isinstance(jobj.get('results'), list) else []

    print(f"Detected JSON container '{containers}' -> {len(nodes)} items")

    title_spec = cfg.get('title') or ''
    link_spec = cfg.get('link') or ''
    date_spec = cfg.get('date') or ''
    desc_spec = cfg.get('description') or ''

    session = requests.Session()
    json_detail_fetch = bool(cfg.get('json_detail_fetch', False))
    # guarda se precisarmos de ordenar por mdr_id_num
    for entry in nodes:
        try:
            title = parse_field_from_json(entry, title_spec) or ''
            raw_link_val = parse_field_from_json(entry, link_spec) or ''
            desc = parse_field_from_json(entry, desc_spec) or ''
            # fallback para description
            if not desc:
                for cand in ('statement_or_summary', 'summary', 'event_description', 'mdr_text'):
                    v = parse_field_from_json(entry, cand)
                    if v:
                        desc = v
                        break

            date_raw = parse_field_from_json(entry, date_spec) or ''
            if not date_raw:
                for cand in ('decision_date', 'date_received', 'report_date', 'date_report', 'date_of_event'):
                    v = parse_field_from_json(entry, cand)
                    if v:
                        date_raw = v
                        break
            date = ''
            if date_raw:
                s = str(date_raw).strip()
                if re.match(r'^\d{8}$', s):
                    date = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
                else:
                    try:
                        date = dateparser.parse(s).isoformat()
                    except Exception:
                        date = s

            # link resolving
            link = ''
            mdr_id_num = None
            if 'api.fda.gov' in cfg.get('url',''):
                if '/device/510k' in cfg.get('url',''):
                    knum = parse_field_from_json(entry, 'k_number') or raw_link_val or ''
                    if json_detail_fetch and knum:
                        resolved_url, rtitle, rdate, rdesc = try_resolve_pmn_page(knum, session)
                        if resolved_url:
                            link = resolved_url
                            if rtitle:
                                title = rtitle
                            if rdate and not date:
                                try:
                                    date = dateparser.parse(rdate).isoformat()
                                except Exception:
                                    date = rdate
                            if rdesc and not desc:
                                desc = rdesc
                    if not link and knum:
                        digits = ''.join(re.findall(r'\d+', knum))
                        if digits:
                            link = f'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={digits.zfill(6)}'
                        else:
                            link = f'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={knum}'
                elif '/device/event' in cfg.get('url',''):
                    # candidate id para MAUDE
                    cand_id = parse_field_from_json(entry, 'mdr_report_key') or parse_field_from_json(entry, 'report_number') or parse_field_from_json(entry, 'event_key') or raw_link_val or ''
                    # tenta extrair product code
                    product_code = parse_field_from_json(entry, 'device[0].device_report_product_code') or parse_field_from_json(entry, 'product_code') or ''
                    # get numeric id if present
                    digits = ''.join(re.findall(r'\d+', str(cand_id)))
                    if digits:
                        mdr_id_num = int(digits)
                    if json_detail_fetch and cand_id:
                        resolved_url, rtitle, rdate, rdesc = try_resolve_maude_page(cand_id, product_code, session)
                        if resolved_url:
                            link = resolved_url
                            if rtitle:
                                title = rtitle
                            if rdate and not date:
                                try:
                                    date = dateparser.parse(rdate).isoformat()
                                except Exception:
                                    date = rdate
                            if rdesc and not desc:
                                desc = rdesc
                    if not link and cand_id:
                        pc = product_code or ''
                        link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={cand_id}&pc={pc}"
                        if mdr_id_num is None and digits:
                            mdr_id_num = int(digits)

            # generic fallback
            if not link and raw_link_val:
                if str(raw_link_val).lower().startswith('http'):
                    link = raw_link_val
                else:
                    link = urljoin(cfg.get('url',''), str(raw_link_val))

            full_text = (title or '') + ' ' + (desc or '') + ' ' + json.dumps(entry, ensure_ascii=False)[:1500]
            itm = {
                'title': title or '',
                'link': link or '',
                'description': desc or '',
                'date': date or '',
                'full_text': full_text or '',
                '_raw_entry': entry
            }
            if mdr_id_num is not None:
                itm['mdr_id_num'] = mdr_id_num
            items.append(itm)
        except Exception:
            continue

    # ordering: respeitar cfg.json_sort (ex: "decision_date:desc" ou "mdr_id:asc")
    sort_cfg = cfg.get('json_sort') or cfg.get('sort_by') or None
    if not sort_cfg:
        if '/device/510k' in cfg.get('url',''):
            sort_cfg = 'decision_date:desc'
        elif '/device/event' in cfg.get('url',''):
            # para replicar teu Excel: MAUDE era crescente (mais recentes no final)
            sort_cfg = 'mdr_id:asc'
    if sort_cfg:
        field, _, direction = sort_cfg.partition(':')
        reverse = (direction.lower() == 'desc')
        if field == 'mdr_id':
            items = sorted(items, key=lambda it: it.get('mdr_id_num') or 0, reverse=reverse)
        elif field in ('decision_date', 'date_received', 'date', 'report_date'):
            def _k(it):
                try:
                    return dateparser.parse(it.get('date') or '')
                except Exception:
                    return it.get('date') or ''
            items = sorted(items, key=_k, reverse=reverse)
    return items


# ---------------- Existing HTML extraction (mantive com pequenas melhorias) ----------------
def fetch_html(url, timeout=20):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r

def text_of_node(node):
    if node is None:
        return ''
    return ' '.join(node.stripped_strings)

def normalize_link_for_dedupe(href):
    if not href:
        return ''
    try:
        p = urlparse(href)
        if not p.scheme:
            href = 'https://' + href.lstrip('/')
            p = urlparse(href)
        qs = dict(parse_qsl(p.query, keep_blank_values=True))
        qs = {k: v for k, v in qs.items() if not k.lower().startswith('utm') and k.lower() not in ('fbclid', 'gclid')}
        new_q = urlencode(qs, doseq=True)
        cleaned = urlunparse((p.scheme.lower(), p.netloc.lower(), p.path.rstrip('/'), '', new_q, ''))
        return cleaned
    except Exception:
        return href.strip().lower()

def extract_items_from_html(html, cfg):
    # identical logic as your original but left intact; using soup.select etc.
    soup = BeautifulSoup(html, 'html.parser')
    container_sel = cfg.get('item_container') or 'article'
    nodes = []
    for sel in [s.strip() for s in str(container_sel).split(',')]:
        try:
            found = soup.select(sel)
            if found:
                nodes.extend(found)
        except Exception:
            continue
    if not nodes:
        for fallback in ('li', 'article', 'div'):
            try:
                found = soup.select(fallback)
                if found:
                    nodes.extend(found)
            except Exception:
                continue

    try:
        sel_list = [s.strip() for s in str(container_sel).split(',') if s.strip()]
        counts = []
        for s in sel_list:
            try:
                c = len(soup.select(s))
            except Exception:
                c = 0
            counts.append((s, c))
        total_nodes = len(nodes)
        print("extract_items_from_html debug selectors counts:", counts, "total_nodes:", total_nodes)
    except Exception:
        pass

    items = []
    for node in nodes:
        try:
            title = ''
            link = ''
            date = ''
            desc = ''
            title_sel = cfg.get('title')
            link_sel = cfg.get('link')
            desc_sel = cfg.get('description')

            # Title extraction
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
                t = node.find(['h1', 'h2', 'h3', 'a'])
                if t:
                    title = t.get_text(strip=True)

            # Link extraction (support a@href)
            if link_sel:
                parts = [p.strip() for p in link_sel.split(',')]
                for ps in parts:
                    if '@' in ps:
                        sel, attr = ps.split('@', 1)
                        try:
                            el = node.select_one(sel.strip())
                        except Exception:
                            el = None
                        if el and el.has_attr(attr):
                            candidate = el.get(attr) or ''
                            if candidate and not _bad_href_re.search(candidate):
                                link = urljoin(cfg.get('url', ''), candidate)
                                break
                    else:
                        try:
                            el = node.select_one(ps)
                        except Exception:
                            el = None
                        if el:
                            candidate = el.get('href') or ''
                            if candidate and not _bad_href_re.search(candidate):
                                link = urljoin(cfg.get('url', ''), candidate)
                                break
            else:
                a = node.find('a')
                if a and a.has_attr('href'):
                    candidate = a.get('href')
                    if candidate and not _bad_href_re.search(candidate):
                        link = urljoin(cfg.get('url', ''), candidate)

            if not link:
                try:
                    for a in node.find_all('a', href=True):
                        h = a.get('href') or ''
                        if h and not _bad_href_re.search(h):
                            link = urljoin(cfg.get('url', ''), h)
                            break
                except Exception:
                    pass

            # Description extraction
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

            # Date: similar approach as original script (search in node, then ancestors)
            date_selectors = []
            if cfg.get('date'):
                date_selectors = [s.strip() for s in cfg.get('date').split(',') if s.strip()]
            date_selectors += ['time', '.date', 'span.date', '.timestamp']

            def find_date_in(element):
                for ds in date_selectors:
                    try:
                        el = element.select_one(ds)
                    except Exception:
                        el = None
                    if el:
                        txt = el.get_text(strip=True)
                        if txt:
                            return txt
                return None

            date = find_date_in(node) or ''
            if not date:
                ancestor = node
                for _ in range(3):
                    ancestor = getattr(ancestor, 'parent', None)
                    if ancestor is None:
                        break
                    found = find_date_in(ancestor)
                    if found:
                        date = found
                        break
            if not date:
                try:
                    prev_sib = node.find_previous_sibling()
                    if prev_sib:
                        found = find_date_in(prev_sib)
                        if found:
                            date = found
                    if not date:
                        next_sib = node.find_next_sibling()
                        if next_sib:
                            found = find_date_in(next_sib)
                            if found:
                                date = found
                except Exception:
                    pass

            full_text = (title or '') + ' ' + (desc or '') + ' ' + text_of_node(node)

            items.append({
                'title': title or '',
                'link': link or '',
                'description': desc or '',
                'date': date or '',
                'full_text': full_text or ''
            })
        except Exception:
            continue

    return items

# ---------------- matching / filtering ----------------
def matches_filters_debug(item, cfg):
    kw = cfg.get('filters', {}).get('keywords', []) or []
    exclude = cfg.get('filters', {}).get('exclude', []) or []
    if not kw and not exclude:
        return True, None
    text_title = (item.get('title', '') or '').lower()
    text_desc = (item.get('description', '') or '').lower()
    text_full = (item.get('full_text', '') or '').lower()
    text_link = (item.get('link', '') or '').lower()

    # include keywords (OR semantics)
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

# ---- helper: dedupe lista de items (mantem a primeira aparição) ----
def dedupe_items(items):
    unique = {}
    out = []
    for it in (items or []):
        key = normalize_link_for_dedupe(it.get('link') or '')
        if not key:
            key = (it.get('title', '') or '').strip().lower()[:200]
        if not key:
            key = f"__no_key__{len(out)}"
        if key not in unique:
            unique[key] = True
            out.append(it)
    return out

# ----- BUILD FEED: inclui matched_reason na description para auditoria ----
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
        print(f"Truncating items for {name} to max_items={max_items}")
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
            # append matched reason to description so Excel can show it later
            reason = it.get('matched_reason')
            desc_to_use = it.get('description') or ''
            if reason:
                desc_to_use = (desc_to_use + ' ').strip() + f" [MatchedReason: {reason}]"
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

    outdir = os.path.join(ROOT, '..', 'feeds') if os.path.exists(os.path.join(ROOT, '..', 'feeds')) else os.path.join(ROOT, '..', 'feeds')
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f'{name}.xml')
    fg.rss_file(outpath)
    print(f'Wrote {outpath} ({count} entries)')

# ---------------- main ----------------
def load_sites():
    try:
        with open(SITES_JSON, 'r', encoding='utf-8') as fh:
            j = json.load(fh)
        return j.get('sites', [])
    except Exception as e:
        print('Failed to load sites.json:', e)
        return []

def main():
    sites = load_sites()
    print(f'Loaded {len(sites)} site configurations from {SITES_JSON}')
    for cfg in sites:
        name = cfg.get('name')
        url = cfg.get('url')
        if not name or not url:
            continue
        print(f'--- Processing {name} ({url}) ---')

        # If calling openFDA and no query string present, append sensible defaults
        u = url
        parsed = urlparse(u)
        if parsed.netloc == 'api.fda.gov' and not parsed.query:
            # add limit and sort defaults depending on endpoint
            params = {'limit': 100}
            if '/device/510k' in parsed.path:
                params['sort'] = 'decision_date:desc'
            elif '/device/event' in parsed.path:
                params['sort'] = 'date_received:desc'
            # rebuild url
            sep = '&' if '?' in u else '?'
            u = u + ('?' + urlencode(params))
            print(f'Adjusted API URL to: {u}')

        html_or_response = None
        try:
            print(f'Fetching {u} via requests...')
            resp = fetch_html(u)
            content_type = resp.headers.get('Content-Type','') if hasattr(resp,'headers') else ''
            txt = resp.text if hasattr(resp,'text') else str(resp)
        except Exception as e:
            print(f'Request error for {u}: {e}')
            txt = ''
            content_type = ''

        items = []
        if txt:
            # Try to detect JSON
            is_json = False
            try:
                if 'application/json' in content_type.lower():
                    is_json = True
                else:
                    # quick test: starts with { or [
                    s = txt.lstrip()
                    if s.startswith('{') or s.startswith('['):
                        # try parse
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

            # debug sample
            try:
                print(f"DEBUG: sample extracted items for {name} (first 20):")
                for i, it in enumerate(items[:20]):
                    t = (it.get('title') or '')[:200]
                    l = (it.get('link') or '')[:200]
                    d_present = bool(it.get('description'))
                    print(f"  [{i}] title='{t}' link='{l}' desc_present={d_present}")
            except Exception:
                pass
        else:
            items = []

        print(f'Found {len(items)} items for {name} (raw)')

        # apply filters (IMPORTANT: no fallback to all items if none matched)
        matched = []
        kw = cfg.get('filters', {}).get('keywords', []) or []
        print(f'Applying {len(kw)} keyword filters for {name}: {kw}')
        for it in items:
            keep, reason = matches_filters_debug(it, cfg)
            if keep:
                it['matched_reason'] = reason
                matched.append(it)

        print(f'{len(matched)} items matched filters for {name}')
        # IMPORTANT: do NOT fallback to all items if none matched (user requirement).
        # If you want previous behavior add cfg.filters.fallback_to_all = True
        if not matched:
            fallback_flag = bool(cfg.get('filters', {}).get('fallback_to_all', False))
            if fallback_flag and items:
                print(f'No items matched filters for {name} — falling back to all {len(items)} items because fallback_to_all=True')
                matched = items
            else:
                print(f'No items matched filters for {name} — NOT falling back (fallback_to_all not set).')
                matched = []


        # dedupe
        deduped = dedupe_items(matched)

        # write feed
        build_feed(name, cfg, deduped)

    print('All done.')

if __name__ == '__main__':
    main()
