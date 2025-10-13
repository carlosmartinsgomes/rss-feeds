#!/usr/bin/env python3
# scripts/generate_feeds.py
# Versão com correções para MAUDE/510k: link-building MAUDE, title fallbacks, date normalization, debug.

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
            if not txts:
                return None
            return "; ".join(txts)
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
    if selector_expr and 'date' in selector_expr.lower():
        try:
            dt = dateparser.parse(v)
            if hasattr(dt, 'isoformat'):
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
        joined = _normalize_date_if_needed(selector_expr, joined)
        return joined
    for alt in [s.strip() for s in re.split(r'\s+OR\s+', selector_expr, flags=re.I)]:
        if not alt:
            continue
        v = get_value_from_record(record, alt)
        if v not in (None, '', '[]'):
            v = _normalize_date_if_needed(selector_expr, v)
            return v
    return None

def parse_field_from_json(entry, spec):
    if not spec or entry is None:
        return None
    return choose_first_available(entry, spec)

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

    # DEBUG: if site looks like MAUDE, print first record (helps debug in Actions logs)
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

            # ----- MAUDE link building fallback -----
            # If the config URL looks like device/event or there are mdr keys, construct Detail.CFM link
            url_lower = (cfg.get('url') or '').lower()
            if ('/device/event' in url_lower) or any(get_value_from_record(rec, k) for k in ('mdr_report_key', 'report_number', 'event_key')):
                cand_id = choose_first_available(rec, 'mdr_report_key OR report_number OR event_key') or ''
                if cand_id:
                    digits = re.findall(r'\d+', str(cand_id))
                    if digits:
                        idnum = max(digits, key=len)
                        pc = choose_first_available(rec, 'product_code OR device[0].device_report_product_code') or ''
                        link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfMAUDE/Detail.CFM?MDRFOI__ID={idnum}&pc={pc}"
                        # attach numeric id for ordering if needed
                        try:
                            item_mdr_num = int(idnum)
                        except Exception:
                            item_mdr_num = None
                    else:
                        item_mdr_num = None
                else:
                    item_mdr_num = None
            else:
                item_mdr_num = None

            # ----- 510k link building fallback for k_number fields -----
            if not link and ('/device/510k' in url_lower or '510k' in cfg.get('name','').lower()):
                k_candidate = choose_first_available(rec, 'k_number OR k_number[0]') or ''
                if k_candidate:
                    kdigits = re.sub(r'\D','', str(k_candidate))
                    if kdigits:
                        link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={kdigits}"

            # ----- title fallback heuristics for MAUDE / events -----
            if not title:
                for cand in ('device.brand_name', 'device.generic_name', 'device.openfda.device_name', 'product_problems', 'event_type', 'mdr_text[0].text', 'manufacturer_d_name', 'report_number'):
                    t = choose_first_available(rec, cand)
                    if t:
                        title = t
                        break
            # ensure title not empty
            if not title:
                title = 'No title'

            # ensure link has something (avoid empty link which can collapse dedupe)
            if not link:
                # try to create a stable fallback URN so dedupe can distinguish records
                # prefer mdr id if present, else index-based URN
                if item_mdr_num:
                    link = f"urn:maude:{item_mdr_num}"
                else:
                    link = f"urn:record:{cfg.get('name')}:{idx}"

            # normalize date (YYYYMMDD -> ISO etc.)
            if date:
                date = _normalize_date_if_needed(cfg.get('date',''), date)
            else:
                # try common candidate fields if cfg did not capture
                for cand in ('date_received', 'report_date', 'date_reported', 'date'):
                    dd = choose_first_available(rec, cand)
                    if dd:
                        date = _normalize_date_if_needed(cand, dd)
                        break

            # build full_text
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
            # continue robustly but log minimal info
            print(f"extract_items_from_json: skipping record idx {idx} due to error: {e}")
            continue

    # Sorting logic (respect json_sort or sensible defaults)
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
                    if it.get('date'):
                        return dateparser.parse(it.get('date'))
                    if it.get('date_obj'):
                        return it.get('date_obj')
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

# ---------------- HTML extraction (mantive como tens) ----------------
def extract_items_from_html(html, cfg):
    # simplified: keep your working behavior (not changed)
    is_xml = False
    preview = (html or '').lstrip()[:200].lower()
    if preview.startswith('<?xml') or '<rss' in preview or '<feed' in preview:
        is_xml = True
    items = []
    try:
        if is_xml:
            soup = BeautifulSoup(html, 'xml')
            nodes = soup.find_all(['item','entry']) or []
            for node in nodes:
                try:
                    title = (node.find('title').string or '').strip() if node.find('title') else node.get_text(" ",strip=True)[:1000]
                    link = ''
                    lnode = node.find('link')
                    if lnode:
                        href = lnode.get('href') or (lnode.string or '').strip()
                        if href:
                            link = urljoin(cfg.get('url',''), href)
                    desc = ''
                    if node.find('description'):
                        desc = (node.find('description').string or '').strip()
                    date = (node.find('pubDate').string or '').strip() if node.find('pubDate') else ''
                    items.append({'title': title, 'link': link, 'description': desc, 'date': date, 'full_text': (title + ' ' + desc)})
                except Exception:
                    continue
            return items
        soup = BeautifulSoup(html, 'html.parser')
        container_sel = cfg.get('item_container') or 'article'
        nodes = []
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
        for node in nodes:
            try:
                title = ''
                link = ''
                desc = ''
                date = ''
                title_sel = cfg.get('title')
                link_sel = cfg.get('link')
                desc_sel = cfg.get('description')
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
                    t = node.find(['h1','h2','h3','a'])
                    title = t.get_text(strip=True) if t else ''
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
                if not link:
                    a = node.find('a', href=True)
                    if a:
                        candidate = a.get('href') or ''
                        if candidate and not _bad_href_re.search(candidate):
                            link = urljoin(cfg.get('url',''), candidate)
                if desc_sel:
                    for s in [t.strip() for t in desc_sel.split(',')]:
                        try:
                            el = node.select_one(s)
                        except Exception:
                            el = None
                        if el:
                            desc = el.get_text(" ",strip=True)
                            break
                else:
                    p = node.find('p')
                    if p:
                        desc = p.get_text(" ",strip=True)
                # date heuristics (simple)
                date = ''
                if node.find('time'):
                    date = node.find('time').get_text(strip=True)
                items.append({'title': title or 'No title', 'link': link or '', 'description': desc or '', 'date': date or '', 'full_text': (title + ' ' + desc)})
            except Exception:
                continue
    except Exception as e:
        print('extract_items_from_html: unexpected error', e)
        return items
    return items

def dedupe_items(items):
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
            if kl in text_title:
                return True, f"keyword '{k}' in title"
            if kl in text_desc:
                return True, f"keyword '{k}' in description"
            if kl in text_full:
                return True, f"keyword '{k}' in full_text"
            if kl in text_link:
                return True, f"keyword '{k}' in link"
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
        deduped = dedupe_items(matched)
        build_feed(name, cfg, deduped)
    print('All done.')

if __name__ == '__main__':
    main()
