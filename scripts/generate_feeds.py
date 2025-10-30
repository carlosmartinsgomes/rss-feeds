#!/usr/bin/env python3
# scripts/generate_feeds.py
# Versão robusta: correções para Yahoo multi-quote "Related News" + debug por-item

import os
import json
import re
import sys
from bs4 import BeautifulSoup
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode
import warnings
from dateutil import parser as dateparser
from dateutil import tz as date_tz
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# silence UnknownTimezoneWarning if present
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
FEEDS_DIR = os.path.join(ROOT, '..', 'feeds')

_bad_href_re = re.compile(r'(^#|/help|/legal|cookie|privacy|terms|signin|login|settings|/consent|/preferences|/policies|mailto:)', re.I)

def load_sites():
    try:
        with open(SITES_JSON, 'r', encoding='utf-8') as fh:
            j = json.load(fh)
        return j.get('sites', [])
    except Exception as e:
        print('Failed to load sites.json:', e)
        return []

def fetch_url(url, timeout=20):
    """
    Fetch URL.
    - Special-case: strict PubMed detection -> robust Retry session + longer timeout.
    - All other hosts: single fast requests.get() (no retry adapter) to avoid slowing the run.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': '*/*'
    }

    # robust domain detection
    try:
        parsed = urlparse(url)
        host = (parsed.hostname or '').lower()
    except Exception:
        host = (url or '').lower()

    is_pubmed = host == 'pubmed.ncbi.nlm.nih.gov' or host.endswith('.pubmed.ncbi.nlm.nih.gov')

    if is_pubmed:
        # Log so we can see in CI logs which URLs used PubMed mode
        print(f"fetch_url: using PubMed retry mode for {url}")

        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(['GET', 'HEAD', 'OPTIONS']),
            raise_on_status=False,
            respect_retry_after_header=True
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount('https://', adapter)
        session.mount('http://', adapter)

        effective_timeout = max(timeout, 45)
        try:
            r = session.get(url, headers=headers, timeout=effective_timeout)
            r.raise_for_status()
            return r
        finally:
            try:
                session.close()
            except Exception:
                pass
    else:
        # Fast path for all other sites: single request, no retry adapter
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception:
            # re-raise so original caller sees the same exceptions/logging behaviour
            raise


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

# ---------------- JSON helpers ----------------
def get_value_from_record(record, path):
    if not path or record is None:
        return None
    parts = [p for p in re.split(r'\.(?![^\[]*\])', path) if p]
    cur = record
    try:
        for p in parts:
            if isinstance(p, str) and re.search(r'\s+OR\s+', p, flags=re.I):
                p = p.split(' OR ')[0].strip()
            m = re.match(r'^([^\[]+)\[(\d+)\]$', p)
            if m:
                key = m.group(1); idx = int(m.group(2))
                if isinstance(cur, dict):
                    cur = cur.get(key)
                if isinstance(cur, list):
                    if 0 <= idx < len(cur):
                        cur = cur[idx]
                    else:
                        return None
                else:
                    return None
                continue
            if isinstance(cur, dict):
                cur = cur.get(p)
            elif isinstance(cur, list):
                if not cur:
                    return None
                if isinstance(cur[0], dict) and p in cur[0]:
                    vals = []
                    for it in cur:
                        v = it.get(p)
                        if v is not None:
                            vals.append(v)
                    if vals:
                        if all(not isinstance(v, (dict, list)) for v in vals):
                            return "; ".join(str(v) for v in vals if v)
                        else:
                            cur = vals
                    else:
                        cur = None
                else:
                    cur = cur[0]
                    if isinstance(cur, dict):
                        cur = cur.get(p)
            else:
                return None
        if cur is None:
            return None
        if isinstance(cur, list):
            txts = []
            for it in cur:
                if isinstance(it, dict):
                    continue
                if it is None:
                    continue
                txts.append(str(it))
            return "; ".join(txts) if txts else None
        else:
            return str(cur)
    except Exception:
        return None

def _normalize_date_if_needed(selector_expr, value):
    if not value:
        return value
    v = str(value).strip()
    m = re.match(r'^(\d{4})(\d{2})(\d{2})$', v)
    if m:
        try:
            return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        except Exception:
            pass
    if re.match(r'^\d{4}-\d{2}-\d{2}$', v):
        return v
    try:
        dt = dateparser.parse(v)
        if dt:
            return dt.isoformat()
    except Exception:
        pass
    return v

def choose_first_available(record, selector_expr):
    if not selector_expr:
        return None
    if re.search(r'\s+AND\s+', selector_expr, flags=re.I):
        parts = [p.strip() for p in re.split(r'\s+AND\s+', selector_expr, flags=re.I) if p.strip()]
        out_parts = []
        for part in parts:
            val = choose_first_available(record, part)
            if val not in (None, '', '[]'):
                out_parts.append(val)
        if not out_parts:
            return None
        joined = ' | '.join(out_parts)
        return _normalize_date_if_needed(selector_expr, joined)
    for alt in [s.strip() for s in re.split(r'\s+OR\s+', selector_expr, flags=re.I)]:
        if not alt:
            continue
        v = get_value_from_record(record, alt)
        if v not in (None, '', '[]'):
            return _normalize_date_if_needed(selector_expr, v)
    return None

def parse_field_from_json(entry, spec):
    if not spec or entry is None:
        return None
    return choose_first_available(entry, spec)

# ---------------- JSON extractor ----------------
def extract_items_from_json(json_obj, cfg):
    items = []
    if not isinstance(json_obj, dict):
        return items
    containers = cfg.get('item_container') or ''
    container_paths = []
    if isinstance(containers, list):
        container_paths = containers
    elif isinstance(containers, str):
        for s in [t.strip() for t in containers.split(',') if t.strip()]:
            container_paths.append(s)
    found_records = []
    for path in container_paths:
        if not path:
            continue
        cur = json_obj
        parts = path.split('.')
        for p in parts:
            if cur is None:
                break
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                break
        if isinstance(cur, list):
            found_records.extend(cur)
        elif isinstance(cur, dict):
            if 'results' in cur and isinstance(cur['results'], list):
                found_records.extend(cur['results'])
            else:
                found_records.append(cur)
    if not found_records:
        for try_name in ("results", "data", "items"):
            if try_name in json_obj and isinstance(json_obj[try_name], list):
                found_records = json_obj[try_name]
                break
    if not found_records and isinstance(json_obj, list):
        found_records = json_obj
    if not found_records:
        return items

    # debug sample for MAUDE-like names
    if cfg.get('name') and 'maude' in cfg.get('name', '').lower():
        try:
            sample = found_records[0]
            print(f"DEBUG SAMPLE RECORD for {cfg.get('name')}: {json.dumps(sample, ensure_ascii=False)[:2000]}")
        except Exception:
            pass

    for idx, rec in enumerate(found_records):
        try:
            title = choose_first_available(rec, cfg.get('title', '')) or ''
            link = choose_first_available(rec, cfg.get('link', '')) or ''
            date = choose_first_available(rec, cfg.get('date', '')) or ''
            desc = choose_first_available(rec, cfg.get('description', '')) or ''

            url_lower = (cfg.get('url') or '').lower()

            # --- MAUDE: robust MDR id extraction & link building ---
            item_mdr_num = None
            if '/device/event' in url_lower or any(get_value_from_record(rec, k) for k in ('mdr_report_key','report_number','event_key')):
                cand_id = choose_first_available(rec, 'mdr_report_key OR report_number OR event_key') or ''
                cand_id_raw = str(cand_id or '').strip()
                idnum = None
                if cand_id_raw:
                    m8 = re.search(r'(\d{8,})', cand_id_raw)
                    if m8:
                        idnum = m8.group(1)
                    else:
                        groups = re.findall(r'\d+', cand_id_raw)
                        if groups:
                            total_len = sum(len(g) for g in groups)
                            if total_len >= 6 and total_len <= 12 and len(groups) >= 2:
                                idnum = ''.join(groups)
                            else:
                                idnum = max(groups, key=len)
                if idnum:
                    try:
                        item_mdr_num = int(idnum)
                    except Exception:
                        item_mdr_num = None
                    pc = choose_first_available(rec, 'product_code OR device[0].device_report_product_code') or ''
                    link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={idnum}&pc={pc}"

            # --- 510k: if link looks like plain K-number, build URL ---
            if link:
                link_str = str(link).strip()
                m_k = re.match(r'^[Kk]?\s*0*([0-9]+)$', link_str)
                if m_k:
                    kdigits = m_k.group(1)
                    link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={kdigits}"
            else:
                if '/device/510k' in url_lower or '510k' in cfg.get('name','').lower():
                    k_candidate = choose_first_available(rec, 'k_number OR pma_pmn_number OR k_number[0]') or ''
                    k_candidate = str(k_candidate or '').strip()
                    if k_candidate:
                        m_k2 = re.search(r'([0-9]{4,})', k_candidate)
                        if m_k2:
                            kdigits = re.sub(r'\D','', k_candidate)
                            link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={kdigits}"
                if '/device/event' in url_lower and not link:
                    raw_id = choose_first_available(rec, 'mdr_report_key OR report_number OR event_key') or ''
                    raw_id_str = str(raw_id or '').strip()
                    if raw_id_str:
                        link = f"urn:maude:{raw_id_str}"

            if not title:
                for cand in ('device.brand_name', 'device.generic_name', 'device.openfda.device_name', 'product_problems', 'event_type', 'mdr_text[0].text', 'manufacturer_d_name', 'report_number'):
                    t = choose_first_available(rec, cand)
                    if t:
                        title = t
                        break
            if not title:
                title = 'No title'

            if not link:
                link = f"urn:record:{cfg.get('name')}:{idx}"

            if date:
                date = _normalize_date_if_needed(cfg.get('date',''), date)
            else:
                for cand in ('date_received','report_date','date_added','date_of_event','date'):
                    dd = choose_first_available(rec, cand)
                    if dd:
                        date = _normalize_date_if_needed(cand, dd)
                        break

            full_text = ' '.join([t for t in (title, desc, json.dumps(rec, ensure_ascii=False)[:1000]) if t])

            item = {
                'title': title or '',
                'link': link or '',
                'description': desc or '',
                'date': date or '',
                'full_text': full_text or '',
                '_raw_entry': rec
            }
            if item_mdr_num:
                item['mdr_id_num'] = item_mdr_num

            items.append(item)
        except Exception as e:
            print(f"extract_items_from_json: skipping record idx {idx} due to error: {e}")
            continue

    sort_cfg = cfg.get('json_sort') or None
    if not sort_cfg:
        if '/device/510k' in (cfg.get('url') or '').lower():
            sort_cfg = 'decision_date:desc'
        elif '/device/event' in (cfg.get('url') or '').lower():
            sort_cfg = 'mdr_id:desc'
    if sort_cfg:
        field, _, direction = sort_cfg.partition(':')
        reverse = (direction.lower() == 'desc')
        if field in ('mdr_id','mdr_id_num'):
            items = sorted(items, key=lambda it: it.get('mdr_id_num') or 0, reverse=reverse)
        elif field in ('decision_date','date_received','date','report_date'):
            def _k(it):
                try:
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

# ---------------- HTML extraction (robusta + debug Yahoo) ----------------
def extract_items_from_html(html, cfg):
    """
    Extrai items (title, link, description, date, full_text) de um HTML dado e uma config de site.
    Versão consolidada: combina heurísticas antigas (fallbacks de title/desc/date por ancestrais/irmãos)
    com as heurísticas novas (select_and_get, preferência por /news/, evitar partners/sessionId, debug).
    """
    # detectar XML/feed (mantem comportamento)
    is_xml = False
    preview = (html or '').lstrip()[:200].lower()
    if preview.startswith('<?xml') or '<rss' in preview or '<feed' in preview:
        is_xml = True

    items = []
    try:
        if is_xml:
            soup = BeautifulSoup(html, 'xml')
            container_sel = cfg.get('item_container') or ''
            nodes = []
            if container_sel:
                for s in [t.strip() for t in container_sel.split(',') if t.strip()]:
                    try:
                        found = soup.find_all(s)
                        if found:
                            nodes.extend(found)
                    except Exception:
                        pass
                    try:
                        found2 = soup.select(s)
                        if found2:
                            nodes.extend(found2)
                    except Exception:
                        pass
            if not nodes:
                nodes = soup.find_all(['item', 'entry']) or []

            nodes = [n for n in nodes if n is not None]
            for node in nodes:
                try:
                    title = ''
                    link = ''
                    date = ''
                    desc = ''
                    tnode = node.find('title')
                    if tnode and tnode.string:
                        title = tnode.string.strip()
                    else:
                        tnode = node.find(['name', 'headline'])
                        if tnode and getattr(tnode, 'string', None):
                            title = tnode.string.strip()
                        else:
                            title = (node.get_text(" ", strip=True) or '')[:1000]
                    lnode = node.find('link')
                    if lnode:
                        href = lnode.get('href') or lnode.get('HREF') or None
                        if href:
                            link = urljoin(cfg.get('url', ''), href)
                        else:
                            txt = (lnode.string or '').strip()
                            if txt:
                                link = urljoin(cfg.get('url', ''), txt)
                            else:
                                alt = node.find('link', attrs={'rel': 'alternate'})
                                if alt and alt.get('href'):
                                    link = urljoin(cfg.get('url', ''), alt.get('href'))
                    if not link:
                        g = node.find('guid')
                        if g and getattr(g, 'string', None):
                            candidate = g.string.strip()
                            if candidate and not _bad_href_re.search(candidate):
                                link = urljoin(cfg.get('url', ''), candidate)
                    dnode = node.find('description') or node.find('summary')
                    if dnode and getattr(dnode, 'string', None):
                        desc = dnode.string.strip()
                    dnode = node.find('pubDate') or node.find('published') or node.find('updated') or node.find('dc:date')
                    if dnode and getattr(dnode, 'string', None):
                        date = dnode.string.strip()
                    full_text = (title or '') + ' ' + (desc or '') + ' ' + (node.get_text(" ", strip=True) or '')
                    items.append({'title': title or '', 'link': link or '', 'description': desc or '', 'date': date or '', 'full_text': full_text or ''})
                except Exception:
                    continue
            return items

        soup = BeautifulSoup(html, 'html.parser')

        # prepare container nodes
        container_sel = cfg.get('item_container') or 'article'
        nodes = []
        for sel in [s.strip() for s in container_sel.split(',') if s.strip()]:
            try:
                found = soup.select(sel)
                if found:
                    nodes.extend(found)
            except Exception:
                continue

        # fallback generic: se nada encontrado, tenta 'li' e 'article' e 'div'
        if not nodes:
            for fallback in ('li', 'article', 'div'):
                try:
                    found = soup.select(fallback)
                    if found:
                        nodes.extend(found)
                except Exception:
                    continue

        # debug counts (opcional)
        try:
            sel_list = [s.strip() for s in container_sel.split(',') if s.strip()]
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

        # helper: selector@attr support
        def select_and_get(el, selector_with_attr):
            if not selector_with_attr:
                return None
            if '@' in selector_with_attr:
                sel, attr = selector_with_attr.split('@', 1)
                sel = sel.strip()
                attr = attr.strip()
            else:
                sel = selector_with_attr.strip()
                attr = 'text'
            try:
                if not sel:
                    return None
                found = el.select_one(sel)
                if not found:
                    return None
                if attr == 'text':
                    return found.get_text(" ", strip=True) or None
                else:
                    return found.get(attr) or None
            except Exception:
                return None

        # helper: encontra melhor href entre anchors (evita session/partners, prefere /news/)
        def find_best_href(node):
            anchors = []
            try:
                for a in node.find_all('a', href=True):
                    h = a.get('href') or ''
                    if not h:
                        continue
                    h_low = h.lower()
                    if 'sessionid' in h_low or 'partners' in h_low or 'uk.yahoo.com' in h_low:
                        continue
                    full = urljoin(cfg.get('url', ''), h)
                    anchors.append((full, h_low, a))
            except Exception:
                pass
            if not anchors:
                return '', None
            for pref in ('/news/', '/articles/', '/story/', '/article/'):
                for full, low, a in anchors:
                    if pref in low:
                        return full, a
            for full, low, a in anchors:
                if 'finance.yahoo.com' in low or 'yahoo.com' in low:
                    return full, a
            return anchors[0][0], anchors[0][2]

        # detect Yahoo multi-quote (either by cfg name or url)
        is_yahoo = False
        try:
            url_lower = (cfg.get('url') or '').lower()
            if 'finance.yahoo.com/quotes' in url_lower or (cfg.get('name') or '').lower() == 'yahoo-multiquote-news':
                is_yahoo = True
        except Exception:
            is_yahoo = False

        # iterate nodes and extract fields (reintroduzemos fallbacks antigos)
        for node_idx, node in enumerate(nodes):
            try:
                title = ''
                link = ''
                date = ''
                desc = ''
                topic = ''

                title_sel = cfg.get('title')
                link_sel = cfg.get('link')
                desc_sel = cfg.get('description')
                topic_sel = cfg.get('topic')

                # --- Yahoo-specific extraction (prefer these values if present) ---
                if is_yahoo:
                    try:
                        pub_sel = cfg.get('publishing_selector') or 'div.publishing'
                        pub_val = None
                        # allow comma separated selectors in publishing_selector
                        for ps in [p.strip() for p in str(pub_sel).split(',') if p.strip()]:
                            pub_val = select_and_get(node, ps)
                            if pub_val:
                                # keep original behavior (pub_val was put in title previously)
                                # we do NOT assign here - we'll swap at the end to guarantee consistent fallbacks
                                title = pub_val
                                break
                    except Exception:
                        pass

                    try:
                        tax_sel = cfg.get('taxonomy_selector') or 'div.taxonomy-links'
                        tax_node = None
                        try:
                            tax_node = node.select_one(tax_sel)
                        except Exception:
                            tax_node = None
                        syms = []
                        if tax_node:
                            # first try .symbol elements
                            try:
                                for sp in tax_node.select('.symbol')[:5]:
                                    st = sp.get_text(" ", strip=True)
                                    if st:
                                        syms.append(st)
                            except Exception:
                                pass
                            # fallback: ticker-container links
                            if not syms:
                                try:
                                    for a in tax_node.select('a[data-testid="ticker-container"]')[:5]:
                                        s = a.get('title') or a.get_text(" ", strip=True)
                                        if s:
                                            syms.append(s.strip())
                                except Exception:
                                    pass
                        if syms:
                            # join up to first 3 tickers
                            topic = ', '.join(syms[:3])
                    except Exception:
                        pass
                    # note: do NOT early-skip fallbacks - if these yielded nothing, later generic logic will try.

                # ------------- TITLE -------------
                if not title:
                    if title_sel:
                        for s in [t.strip() for t in str(title_sel).split(',') if t.strip()]:
                            val = select_and_get(node, s)
                            if val:
                                title = val
                                break
                if not title:
                    # try common headline tags
                    t = node.find(['h1', 'h2', 'h3', 'h4', 'a'])
                    if t and t.get_text(strip=True):
                        title = t.get_text(" ", strip=True)

                # fallback: if anchor with title attribute exists, use it
                if not title:
                    try:
                        a_try = node.select_one('a[title], a.title')
                        if a_try and a_try.has_attr('title'):
                            at = (a_try.get('title') or '').strip()
                            if at:
                                title = at
                    except Exception:
                        pass

                # ------------- LINK -------------
                if link_sel:
                    parts = [p.strip() for p in link_sel.split(',')]
                    for ps in parts:
                        if '@' in ps:
                            sel, attr = ps.split('@', 1)
                            sel = sel.strip(); attr = attr.strip()
                            try:
                                el = node.select_one(sel)
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

                if not link:
                    chosen, chosen_anchor = find_best_href(node)
                    link = chosen or ''

                # ------------- DESCRIPTION -------------
                if desc_sel:
                    for s in [t.strip() for t in str(desc_sel).split(',') if t.strip()]:
                        val = select_and_get(node, s)
                        if val:
                            desc = val
                            break
                if not desc:
                    p = node.find('p')
                    if p and p.get_text(strip=True):
                        desc = p.get_text(" ", strip=True)

                # if desc equals title or is empty try fallbacks (old behavior)
                if (not desc) or (title and desc and desc.strip() == title.strip()):
                    try:
                        # try anchor title attribute
                        a_try = node.select_one('a.link, h4 a.title, a.title, a')
                        if a_try and a_try.has_attr('title'):
                            a_title_attr = (a_try.get('title') or '').strip()
                            if a_title_attr and a_title_attr != title:
                                desc = a_title_attr
                        # previous paragraph
                        if (not desc) or (desc.strip() == title.strip()):
                            pprev = node.find_previous('p')
                            if pprev and pprev.get_text(strip=True):
                                ptxt = pprev.get_text(" ", strip=True)
                                if ptxt and ptxt != title:
                                    desc = ptxt
                        # next paragraph
                        if (not desc) or (desc.strip() == title.strip()):
                            pnext = node.find_next('p')
                            if pnext and pnext.get_text(strip=True):
                                ptxt = pnext.get_text(" ", strip=True)
                                if ptxt and ptxt != title:
                                    desc = ptxt
                    except Exception:
                        pass

                # ------------- TOPIC -------------
                if not topic:
                    try:
                        topic_sel = cfg.get('topic')
                        if topic_sel:
                            for s in [t.strip() for t in str(topic_sel).split(',') if t.strip()]:
                                try:
                                    val = select_and_get(node, s)
                                except Exception:
                                    val = None
                                if val:
                                    topic = val
                                    break
                        # fallbacks comuns: meta keywords, tag classes, category elements
                        if not topic:
                            try:
                                meta = node.select_one('meta[name="keywords"], meta[property="article:tag"], .tag, .tags, .category')
                                if meta:
                                    topic = (meta.get('content') or meta.get_text(" ", strip=True) or '').strip()
                            except Exception:
                                topic = topic or ''
                        # última tentativa: procurar um .category/.tag dentro do node
                        if not topic:
                            try:
                                tag_el = node.select_one('.category, .tag, .tags, .topic')
                                if tag_el:
                                    topic = tag_el.get_text(" ", strip=True) or ''
                            except Exception:
                                pass
                    except Exception:
                        topic = topic or ''

                # ------------- DATE -------------
                date = ''
                if cfg.get('date'):
                    for s in [t.strip() for t in str(cfg.get('date')).split(',') if t.strip()]:
                        val = select_and_get(node, s)
                        if val:
                            date = val
                            break

                # heuristics: search node, ancestors up to 3 levels, siblings
                if not date:
                    def find_date_in(element):
                        for ds in ['time', 'span.time', 'time[datetime]', '.date', 'span.date', '.timestamp', '.pubdate', 'small']:
                            try:
                                el = element.select_one(ds)
                            except Exception:
                                el = None
                            if el:
                                txt = (el.get('datetime') or el.get_text(" ", strip=True) or '').strip()
                                if txt:
                                    return txt
                        # also try attributes like data-date
                        try:
                            for attr in ('data-date','data-datetime','datetime'):
                                if element and getattr(element, 'attrs', None) and element.attrs.get(attr):
                                    return element.attrs.get(attr)
                        except Exception:
                            pass
                        return None

                    found = find_date_in(node)
                    if not found:
                        ancestor = node
                        for _ in range(3):
                            ancestor = getattr(ancestor, 'parent', None)
                            if ancestor is None:
                                break
                            found = find_date_in(ancestor)
                            if found:
                                break
                    if not found:
                        try:
                            prev_sib = node.find_previous_sibling()
                            if prev_sib:
                                found = find_date_in(prev_sib)
                            if not found:
                                next_sib = node.find_next_sibling()
                                if next_sib:
                                    found = find_date_in(next_sib)
                        except Exception:
                            found = None
                    if found:
                        date = found

                # fallback: if still none, try any iso-like text in node
                if not date:
                    txt_all = node.get_text(" ", strip=True)
                    m = re.search(r'(\d{4}-\d{2}-\d{2})', txt_all)
                    if m:
                        date = m.group(1)
                    else:
                        m2 = re.search(r'(\d{4}\d{2}\d{2})', txt_all)
                        if m2:
                            date = m2.group(1)

                # ------------- FINAL NORMALIZATIONS & FALLBACKS -------------
                # if title is empty, try several candidates
                if not title:
                    for cand in ('a[title]', 'h3 a', '.headline', '.title', 'img[alt]', '.provider'):
                        try:
                            tval = None
                            if cand == 'img[alt]':
                                img = node.select_one('img[alt]')
                                if img and img.get('alt'):
                                    tval = img.get('alt')
                            else:
                                tval = select_and_get(node, cand + '@text') if '@' not in cand else select_and_get(node, cand)
                            if tval:
                                title = tval
                                break
                        except Exception:
                            pass
                if not title:
                    title = 'No title'

                if not link:
                    # build urn (so dedupe still works)
                    link = f"urn:node:{cfg.get('name')}:{node_idx}"

                # === SWAP title <-> desc for Yahoo as requested ===
                # keep all fallbacks intact, then invert the two fields for Yahoo nodes
                if is_yahoo:
                    try:
                        title, desc = (desc or ''), (title or '')
                    except Exception:
                        pass

                full_text = (title or '') + ' ' + (desc or '') + ' ' + text_of_node(node)

                # debug per-node lightweight
                try:
                    if cfg.get('name','').lower() == 'yahoo-multiquote-news':
                        anchors_count = len(node.find_all('a', href=True))
                        print(f"YAHOO: node idx={node_idx} anchors={anchors_count} chosen_link='{(link or '')[:140]}' title_len={len(title)} desc_len={len(desc)} topic='{topic}'")
                except Exception:
                    pass

                items.append({'title': title or '', 'link': link or '', 'description': desc or '', 'date': date or '', 'full_text': full_text or '', 'topic': topic or ''})

            except Exception:
                # não deixes um node problemático bloquear todo o resto
                continue

    except Exception as e:
        print('extract_items_from_html: unexpected error', e)
        return items

    return items


def dedupe_items(items, cfg=None):
    do_dedupe = True
    if isinstance(cfg, dict) and cfg.get('dedupe') is False:
        do_dedupe = False
    if not do_dedupe:
        return items[:]
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

            # se houver matched_reason, adicionar também como category (evita perda por truncamento do description)
            try:
                mr = it.get('matched_reason')
                if mr:
                    # o feedgen aceita fe.category(term=...)
                    # guardamos a string tal como veio (pode ser "kw@field;kw2@field2")
                    fe.category(term=str(mr))
            except Exception:
                pass

            # se houver topic no item, adiciona-o como category com prefixo 'topic:' para ser recuperável pelo parser
            try:
                tval = it.get('topic')
                if tval:
                    # guardamos como "topic:VALUE" para ser facilmente identificado no parsing posterior
                    fe.category(term=f"topic:{tval}")
            except Exception:
                pass

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

def matches_filters_debug(item, cfg):
    """
    Verifica keywords/excludes do cfg nos campos do item.
    Retorna (keep:bool, reason:str_or_None)
      - reason -> formato 'kw1@field;kw2@field2' (todas as matches) ou 'exclude:term@field'
      - Se não houver filtros no cfg devolve (True, None)
    Campos verificados (ordem): title, description, full_text, link, topic
    """
    kw_list = cfg.get('filters', {}).get('keywords', []) or []
    exclude_list = cfg.get('filters', {}).get('exclude', []) or []

    # sem filtros -> mantém (comportamento antigo)
    if not kw_list and not exclude_list:
        return True, None

    # preparar textos (lowercase)
    text_map = {
        'title': (item.get('title','') or '').lower(),
        'description': (item.get('description','') or item.get('description (short)','') or '').lower(),
        'full_text': (item.get('full_text','') or '').lower(),
        'link': (item.get('link','') or item.get('link (source)','') or '').lower(),
        'topic': (item.get('topic','') or '').lower()
    }

    # keywords: coleciona todas as matches (mantendo ordem keywords -> fields)
    if kw_list:
        matches = []
        for k in kw_list:
            if not k:
                continue
            kl = str(k).lower()
            for field in ('title','description','full_text','link','topic'):
                if kl in text_map.get(field,''):
                    matches.append(f"{kl}@{field}")
        if matches:
            # devolve True e todas as matches separadas por ';'
            return True, ";".join(matches)
        else:
            return False, None

    # excludes: se bater, rejeita (retorna False) e inclui a razão (pode haver múltiplos; junta-se por ';')
    excl_matches = []
    for ex in exclude_list:
        if not ex:
            continue
        el = str(ex).lower()
        for field in ('title','description','full_text','link','topic'):
            if el in text_map.get(field,''):
                excl_matches.append(f"exclude:{el}@{field}")
    if excl_matches:
        # qualquer exclude faz rejeição (mantemos essa regra)
        return False, ";".join(excl_matches)

    return True, None


# --- helper: aplicar filtros e garantir matched_reason -----------------------
def apply_filters_and_mark(items, cfg):
    """
    Recebe lista de items (cada item é dict) e cfg (site cfg).
    Retorna a lista de items filtrados (sem fallback quando houver filtros),
    e garante que items que match têm item['matched_reason'] e ['matched_reason_raw'].
    - Se não houver filtros configurados -> devolve todos os items sem matched_reason.
    """
    if not isinstance(items, list):
        return []

    kw = cfg.get('filters', {}).get('keywords', []) or []
    exclude = cfg.get('filters', {}).get('exclude', []) or []

    # se não há filtros configurados -> devolve todos (sem alteração)
    if not kw and not exclude:
        # ensure matched_reason fields exist but empty (optional)
        for it in items:
            if 'matched_reason' not in it:
                it['matched_reason'] = ''
            if 'matched_reason_raw' not in it:
                it['matched_reason_raw'] = ''
        return items

    filtered = []
    for it in items:
        try:
            keep, reason = matches_filters_debug(it, cfg)
        except Exception:
            keep, reason = False, None
        if keep:
            # normalizar reason
            try:
                it['matched_reason'] = reason or ''
                it['matched_reason_raw'] = reason or ''
            except Exception:
                it['matched_reason'] = ''
                it['matched_reason_raw'] = ''
            filtered.append(it)

    # sem fallback automático — se não houve matches devolve lista vazia
    return filtered
# -----------------------------------------------------------------------------


def main():
    sites = load_sites()
    print(f'Loaded {len(sites)} site configurations from {SITES_JSON}')
    for cfg in sites:
        name = cfg.get('name')
        url = cfg.get('url')
        if not name or not url:
            continue
        print(f'--- Processing {name} ({url}) ---')
        html = None
        resp = None
        rf = cfg.get('render_file')
        if rf:
            rf_path = rf
            if not os.path.isabs(rf_path) and not rf_path.startswith('scripts'):
                rf_path = os.path.join('scripts', rf_path)
            if os.path.exists(rf_path):
                try:
                    html = open(rf_path, 'r', encoding='utf-8').read()
                    print(f'Using rendered file: {rf_path} for {name}')
                except Exception as e:
                    print('Failed reading rendered file:', e)
                    html = None
            else:
                print(f'No rendered file found at {rf_path} for {name}')
        if html is None:
            try:
                # determine per-site timeout (respect cfg['timeout'] if present)
                default_timeout = int(cfg.get('timeout', 20))
                # special-case Yahoo multi-quote -> increase timeout
                url_lower = (url or '').lower()
                if 'finance.yahoo.com/quotes' in url_lower or name.lower() == 'yahoo-multiquote-news':
                    timeout = max(default_timeout, 60)   # 60s for Yahoo (ajusta se quiseres)
                else:
                    timeout = default_timeout

                print(f'Fetching {url} via requests with timeout={timeout}...')
                resp = fetch_url(url, timeout=timeout)
            except Exception as e:
                print(f'Request error for {url}: {e}')
                resp = None

        items = []
        if html:
            try:
                items = extract_items_from_html(html, cfg)
            except Exception as e:
                print('Error parsing rendered HTML:', e)
                items = []
        elif resp is not None:
            ctype = resp.headers.get('Content-Type','').lower()
            body = resp.text or ''
            is_json = False
            if 'application/json' in ctype or body.strip().startswith('{') or body.strip().startswith('['):
                is_json = True
            if is_json:
                print(f"Detected JSON response for {name}; parsing with JSON handler")
                try:
                    json_obj = resp.json()
                except Exception:
                    try:
                        json_obj = json.loads(body)
                    except Exception:
                        json_obj = None
                if json_obj is not None:
                    items = extract_items_from_json(json_obj, cfg)
                else:
                    print(f"Warning: failed to parse JSON for {name}; falling back to HTML parsing")
                    items = extract_items_from_html(body, cfg)
            else:
                try:
                    items = extract_items_from_html(body, cfg)
                except Exception as e:
                    print('Error parsing HTML response:', e)
                    items = []
        else:
            items = []

        print(f'Found {len(items)} items for {name} (raw)')

        kw = cfg.get('filters', {}).get('keywords', []) or []
        print(f'Applying {len(kw)} keyword filters for {name}: {kw}')
        # use helper to apply filters and annotate matched_reason
        matched = apply_filters_and_mark(items, cfg)
        print(f'{len(matched)} items matched filters for {name}')
        # Novo comportamento: se não houve matches, NÃO fazemos fallback.
        if not matched:
            print(f'No items matched filters for {name}; writing empty feed (no fallback).')

        deduped = dedupe_items(matched, cfg)

        build_feed(name, cfg, deduped)

    print('All done.')

if __name__ == '__main__':
    main()
