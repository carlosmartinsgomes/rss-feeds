#!/usr/bin/env python3
# scripts/generate_feeds.py
# Versão com heurísticas específicas para Yahoo Related News:
#  - tenta localizar a secção "Related News" e extrair os itens <li>
#  - coloca nos titles a linha de metadados (ex: "Insider Monkey • 2h ago")
#  - coloca na description o headline + informação de ticker/percent extraída

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
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': '*/*'
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

# ---------------- JSON helpers (kept generic) ----------------
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

# ---------------- JSON extractor (kept) ----------------
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

# ---------------- HTML extraction (with Yahoo special-case) ----------------
def extract_items_from_html(html, cfg):
    """
    Extrai items (title, link, description, date, full_text) de um HTML dado e uma config de site.
    Inclui heurísticas específicas para 'yahoo-multiquote-news' para:
      - localizar o bloco 'Related News' (se existir)
      - extrair os <li> desse bloco
      - trocar title/description para que title contenha a linha de metadados e description o headline + ticker
    """
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
                        tnode = node.find(['name','headline'])
                        if tnode and getattr(tnode,'string',None):
                            title = tnode.string.strip()
                        else:
                            title = (node.get_text(" ", strip=True) or '')[:1000]
                    lnode = node.find('link')
                    if lnode:
                        href = lnode.get('href') or lnode.get('HREF') or None
                        if href:
                            link = urljoin(cfg.get('url',''), href)
                        else:
                            txt = (lnode.string or '').strip()
                            if txt:
                                link = urljoin(cfg.get('url',''), txt)
                            else:
                                alt = node.find('link', attrs={'rel':'alternate'})
                                if alt and alt.get('href'):
                                    link = urljoin(cfg.get('url',''), alt.get('href'))
                    if not link:
                        g = node.find('guid')
                        if g and getattr(g,'string',None):
                            candidate = g.string.strip()
                            if candidate and not _bad_href_re.search(candidate):
                                link = urljoin(cfg.get('url',''), candidate)
                    dnode = node.find('description') or node.find('summary')
                    if dnode and getattr(dnode,'string',None):
                        desc = dnode.string.strip()
                    dnode = node.find('pubDate') or node.find('published') or node.find('updated') or node.find('dc:date')
                    if dnode and getattr(dnode,'string',None):
                        date = dnode.string.strip()
                    full_text = (title or '') + ' ' + (desc or '') + ' ' + (node.get_text(" ", strip=True) or '')
                    items.append({'title': title or '', 'link': link or '', 'description': desc or '', 'date': date or '', 'full_text': full_text or ''})
                except Exception:
                    continue
            return items

        # HTML path
        soup = BeautifulSoup(html, 'html.parser')

        # ---------- YAHOO SPECIAL CASE ----------
        # If this cfg is the yahoo-multiquote-news entry, attempt to find the "Related News"
        nodes = []
        if cfg.get('name','').lower() == 'yahoo-multiquote-news':
            # 1) Try to locate a container whose heading contains "Related" or "Related News"
            related_container = None
            for htag in soup.find_all(text=re.compile(r'Related\s+News|Related', re.I)):
                try:
                    parent = getattr(htag, 'parent', None)
                    # prefer a header's next sibling UL/OL or parent with UL/LI
                    if parent:
                        # look for next ul/ol
                        nxt = parent.find_next_sibling()
                        if nxt and nxt.name in ('ul','ol'):
                            related_container = nxt
                            break
                        # or look for an ancestor with ul/ol children
                        anc = parent
                        for _ in range(4):
                            anc = getattr(anc, 'parent', None)
                            if anc is None:
                                break
                            if anc.find('ul') or anc.find('ol'):
                                related_container = anc
                                break
                        if related_container:
                            break
                except Exception:
                    continue
            # 2) fallback: look for an element with "related" in class or id
            if not related_container:
                candidate = soup.select_one('[class*="related"], [id*="related"], [data-test*="related"]')
                if candidate:
                    related_container = candidate
            # 3) fallback: select main-content lists
            if related_container:
                # collect LI inside it
                lis = related_container.find_all('li')
                if not lis:
                    # maybe direct anchors
                    anchors = related_container.find_all('a')
                    for a in anchors:
                        # create a dummy wrapper element to keep interface uniform
                        wrapper = BeautifulSoup('<div></div>', 'html.parser').div
                        wrapper.append(a)
                        nodes.append(wrapper)
                else:
                    nodes = lis
            else:
                # no clear related container found; fallback to generic selectors provided in cfg
                pass

        # If not yahoo case or fallback, proceed with generic container selection
        if not nodes:
            container_sel = cfg.get('item_container') or 'article'
            for sel in [s.strip() for s in container_sel.split(',')]:
                try:
                    found = soup.select(sel)
                    if found:
                        nodes.extend(found)
                except Exception:
                    continue
            if not nodes:
                for fallback in ('li','article','div'):
                    try:
                        found = soup.select(fallback)
                        if found:
                            nodes.extend(found)
                    except Exception:
                        continue

        # debug counts
        try:
            sel_list = [s.strip() for s in (cfg.get('item_container') or '').split(',') if s.strip()]
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

        # helper: select and get (supports '@attr' in the cfg selectors)
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
                    full = urljoin(cfg.get('url',''), h)
                    anchors.append((full, h_low, a))
            except Exception:
                pass
            if not anchors:
                return '', None
            # prefer news/article/story
            for pref in ('/news/','/article/','/story/','/articles/'):
                for full, low, a in anchors:
                    if pref in low:
                        return full, a
            # otherwise return anchor that contains ticker-like substrings in nearby text (heuristic)
            for full, low, a in anchors:
                if re.search(r'\b[A-Z]{1,5}\b', a.get_text(" ", strip=True) or ''):
                    return full, a
            return anchors[0][0], anchors[0][2]

        # iterate nodes
        for node in nodes:
            try:
                title = ''
                link = ''
                date = ''
                desc = ''
                title_sel = cfg.get('title')
                link_sel = cfg.get('link')
                desc_sel = cfg.get('description')

                # Title extraction (supports selector@attr)
                if title_sel:
                    for s in [t.strip() for t in str(title_sel).split(',') if t.strip()]:
                        val = select_and_get(node, s)
                        if val:
                            title = val
                            break
                else:
                    t = node.find(['h1','h2','h3','a'])
                    if t:
                        title = t.get_text(strip=True)

                # Link extraction: try provided selectors first
                if link_sel:
                    parts = [p.strip() for p in link_sel.split(',')]
                    for ps in parts:
                        if '@' in ps:
                            sel, attr = ps.split('@',1)
                            sel = sel.strip(); attr = attr.strip()
                            try:
                                el = node.select_one(sel)
                            except Exception:
                                el = None
                            if el and el.has_attr(attr):
                                candidate = el.get(attr) or ''
                                if candidate and not _bad_href_re.search(candidate):
                                    link = urljoin(cfg.get('url',''), candidate)
                                    break
                        else:
                            try:
                                el = node.select_one(ps)
                            except Exception:
                                el = None
                            if el:
                                candidate = el.get('href') or ''
                                if candidate and not _bad_href_re.search(candidate):
                                    link = urljoin(cfg.get('url',''), candidate)
                                    break

                # Fallback: pick best anchor in node
                if not link:
                    chosen, chosen_anchor = find_best_href(node)
                    link = chosen

                # Description extraction
                if desc_sel:
                    for s in [t.strip() for t in str(desc_sel).split(',') if t.strip()]:
                        val = select_and_get(node, s)
                        if val:
                            desc = val
                            break
                else:
                    p = node.find('p')
                    if p:
                        desc = p.get_text(" ", strip=True)

                # Date extraction attempt
                date = ''
                if cfg.get('date'):
                    for s in [t.strip() for t in str(cfg.get('date')).split(',') if t.strip()]:
                        val = select_and_get(node, s)
                        if val:
                            date = val
                            break
                if not date:
                    for candidate_sel in ['time','span.time','time[datetime]','.date','span.date','.timestamp']:
                        try:
                            nd = node.select_one(candidate_sel)
                        except Exception:
                            nd = None
                        if nd:
                            dt = (nd.get('datetime') or nd.get_text(" ", strip=True) or '').strip()
                            if dt:
                                date = dt
                                break

                # ---------- Yahoo-specific post-processing ----------
                if cfg.get('name','').lower() == 'yahoo-multiquote-news':
                    # Prefer headline from h3 or a text if we have it
                    headline = ''
                    hl = node.find(['h3','h2','h4'])
                    if hl:
                        headline = hl.get_text(" ", strip=True)
                    else:
                        # some items put the headline inside the anchor
                        if chosen_anchor is not None:
                            headline = (chosen_anchor.get_text(" ", strip=True) or '').strip()
                    # publisher / meta line: try to get small/span with provider/time info
                    meta_txt = ''
                    # common patterns: small, .provider, .source, .publisher, .provider-name, .published
                    for c in ['small','span.source','span.provider','div.provider','span.published','span.byline','div.byline']:
                        try:
                            el = node.select_one(c)
                        except Exception:
                            el = None
                        if el:
                            meta_txt = el.get_text(" ", strip=True)
                            if meta_txt:
                                break
                    # if still empty, look for short text elements near headline
                    if not meta_txt and hl:
                        nxt = hl.find_next_sibling()
                        if nxt:
                            meta_txt = nxt.get_text(" ", strip=True)
                    # ticker/percent: scan node text for patterns like "DDOG -2.02%" or "DDOG -2.02% OPAI.PVT"
                    ticker_txt = ''
                    txt_all = node.get_text(" ", strip=True)
                    # collect things that look like TICKER +/-% (heuristic)
                    m_tick = re.findall(r'\b([A-Z]{1,6})\b\s*[-–]\s*\d{1,3}\.\d{1,2}%', txt_all)
                    if m_tick:
                        # reconstruct occurrences (grab full matches)
                        all_matches = re.findall(r'\b([A-Z]{1,6})\b\s*[-–]\s*\d{1,3}\.\d{1,2}%', txt_all)
                        # simpler: take the first full textual match
                        ft = re.search(r'\b([A-Z]{1,6})\b\s*[-–]\s*\d{1,3}\.\d{1,2}%', txt_all)
                        if ft:
                            # find surrounding substring of up to 30 chars
                            span = ft.span()
                            start = max(0, span[0]-20); end = min(len(txt_all), span[1]+20)
                            ticker_txt = txt_all[start:end].strip()
                    # fallback: small uppercase tokens like "DDOG -2.02%OPAI.PVT" may be contiguous; try to grab short uppercase groups
                    if not ticker_txt:
                        m2 = re.search(r'([A-Z]{1,6}\s*[-–]\s*\d{1,3}\.\d{1,2}%\b.*?)(?:\s{2,}|\n|$)', txt_all)
                        if m2:
                            ticker_txt = m2.group(1).strip()

                    # now build desired fields:
                    # - title should be the meta line (publisher/time) if present, else fallback to 'headline source'
                    # - description should be headline + ' ' + ticker_txt (if present)
                    final_title = meta_txt or headline or title or 'No title'
                    final_desc = ''
                    if headline:
                        final_desc = headline
                    elif desc:
                        final_desc = desc
                    # append ticker text if found
                    if ticker_txt:
                        final_desc = (final_desc + ' ' + ticker_txt).strip()
                    # apply back
                    title = final_title
                    desc = final_desc

                full_text = (title or '') + ' ' + (desc or '') + ' ' + text_of_node(node)

                items.append({'title': title or '', 'link': link or '', 'description': desc or '', 'date': date or '', 'full_text': full_text or ''})

            except Exception:
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
    kw = cfg.get('filters', {}).get('keywords', []) or []
    exclude = cfg.get('filters', {}).get('exclude', []) or []
    if not kw and not exclude:
        return True, None
    text_title = (item.get('title','') or '').lower()
    text_desc = (item.get('description','') or '').lower()
    text_full = (item.get('full_text','') or '').lower()
    text_link = (item.get('link','') or '').lower()
    if kw:
        for k in kw:
            kl = k.lower()
            if kl in text_title: return True, f"keyword '{k}' in title"
            if kl in text_desc: return True, f"keyword '{k}' in description"
            if kl in text_full: return True, f"keyword '{k}' in full_text"
            if kl in text_link: return True, f"keyword '{k}' in link"
        return False, None
    for ex in exclude:
        if ex.lower() in text_title or ex.lower() in text_desc or ex.lower() in text_full:
            return False, f"exclude '{ex}' matched"
    return True, None

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
                print(f'Fetching {url} via requests...')
                resp = fetch_url(url)
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

        matched = []
        kw = cfg.get('filters', {}).get('keywords', []) or []
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

        deduped = dedupe_items(matched, cfg)

        build_feed(name, cfg, deduped)

    print('All done.')

if __name__ == '__main__':
    main()
