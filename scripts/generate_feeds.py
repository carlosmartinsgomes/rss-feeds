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
def extract_items_from_json_obj(jobj, cfg):
    """
    jobj: loaded JSON (dict)
    cfg: site config
    returns list of item dicts: {'title','link','description','date','full_text'}
    """
    items = []
    container = cfg.get('item_container') or 'results'
    # support container being list or comma-separated
    if isinstance(container, list):
        container_candidates = container
    else:
        container_candidates = [c.strip() for c in str(container).split(',') if c.strip()]

    nodes = None
    # Try each candidate path until we find a list
    for cand in container_candidates:
        # cand may be 'results' or 'data.results' etc.
        val = get_json_path_value(jobj, cand)
        if isinstance(val, list):
            nodes = val
            break
        # if val is dict maybe has 'results' inside
        if isinstance(val, dict):
            if 'results' in val and isinstance(val['results'], list):
                nodes = val['results']
                break
    if nodes is None:
        # fallback to top-level 'results'
        if 'results' in jobj and isinstance(jobj['results'], list):
            nodes = jobj['results']
        else:
            # can't find list
            nodes = []

    # debug
    print(f"Detected JSON container '{container_candidates}' -> {len(nodes)} items")

    # helper to build link heuristics for FDA
    def build_fda_link(entry, cfg_url, candidate_value):
        # if candidate_value already looks like http, return as is
        if not candidate_value:
            return ''
        if candidate_value.lower().startswith('http'):
            return candidate_value
        # heuristics for 510k: key often k_number e.g. "K923368". extract digits and zero-pad to 6 -> build pmn URL
        try:
            if 'api.fda.gov' in cfg_url and '/device/510k' in cfg_url:
                # try k_number, pmn, or other id
                k = candidate_value
                digits = ''.join(re.findall(r'\d+', k))
                if digits:
                    digits = digits.zfill(6)
                    return f'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={digits}'
            # heuristics for event (MAUDE)
            if 'api.fda.gov' in cfg_url and '/device/event' in cfg_url:
                # prefer report_number or mdr_report_key
                idv = candidate_value
                # if look like '2032227-2020-110170' just insert
                return f'https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfmaude/detail.cfm?mdr_id={idv}'
        except Exception:
            pass
        # fallback: if just an id, return it as-is
        return candidate_value

    # field specs
    title_spec = cfg.get('title') or ''
    link_spec = cfg.get('link') or ''
    date_spec = cfg.get('date') or ''
    desc_spec = cfg.get('description') or ''

    # allow link_spec to be like 'k_number OR event_id' (we'll resolve)
    for entry in nodes:
        try:
            title = parse_field_from_json(entry, title_spec) if title_spec else ''
            raw_link_val = parse_field_from_json(entry, link_spec) if link_spec else ''
            link = build_fda_link(entry, cfg.get('url',''), raw_link_val)

            # description: try spec, else try 'summary', 'statement_or_summary', 'event_description' etc.
            desc = parse_field_from_json(entry, desc_spec) if desc_spec else ''
            if not desc:
                for cand in ('summary','statement_or_summary','event_description','mdr_text','product_description'):
                    v = parse_field_from_json(entry, cand)
                    if v:
                        desc = v
                        break

            # date: try spec; if numeric YMD parse
            date_raw = parse_field_from_json(entry, date_spec) if date_spec else ''
            if not date_raw:
                for cand in ('decision_date','date_received','report_date','date_report','date_of_event'):
                    v = parse_field_from_json(entry, cand)
                    if v:
                        date_raw = v
                        break
            # try to normalize date like '19920310' -> '1992-03-10'
            date = ''
            if date_raw:
                s = date_raw.strip()
                # if purely digits and length 8 -> YYYYMMDD
                if re.match(r'^\d{8}$', s):
                    date = f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
                else:
                    # try parse with dateutil
                    try:
                        dt = dateparser.parse(s)
                        date = dt.isoformat()
                    except Exception:
                        date = s

            full_text = (title or '') + ' ' + (desc or '') + ' ' + json.dumps(entry, ensure_ascii=False)[:2000]

            items.append({
                'title': title or '',
                'link': link or '',
                'description': desc or '',
                'date': date or '',
                'full_text': full_text or '',
                '_raw_entry': entry
            })
        except Exception:
            continue

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
            print(f'No items matched filters for {name} — returning 0 items (no fallback).')
            matched = []

        # dedupe
        deduped = dedupe_items(matched)

        # write feed
        build_feed(name, cfg, deduped)

    print('All done.')

if __name__ == '__main__':
    main()
