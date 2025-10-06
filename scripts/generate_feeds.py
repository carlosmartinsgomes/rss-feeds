#!/usr/bin/env python3
# scripts/generate_feeds.py
# Gere feeds RSS simples a partir de sites listados em sites.json
# Melhorias: suporte JSON (openFDA), normalização de datas YYYYMMDD,
# heurísticas de description/link para FDA 510k/MAUDE, e NÃO fazer fallback
# para todos os items se nenhum filtro bater.

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
# and to give dateutil a sensible tzinfo for "ET" without changing all parse(...) calls.
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


def fetch_html(url, timeout=20):
    return fetch_url(url, timeout=timeout).text


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


# ---------------------- JSON HANDLER ADDED ----------------------
def get_value_from_record(record, path):
    if not path or record is None:
        return None
    path = path.strip()
    parts = []
    for part in re.split(r'\.(?![^\[]*\])', path):
        parts.append(part)

    cur = record
    try:
        for p in parts:
            if ' OR ' in p:
                p = p.split(' OR ')[0].strip()
            m = re.match(r'^([^\[]+)\[(\d+)\]$', p)
            if m:
                key = m.group(1)
                idx = int(m.group(2))
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


def choose_first_available(record, selector_expr):
    if not selector_expr:
        return None
    for alt in [s.strip() for s in re.split(r'\s+OR\s+', selector_expr, flags=re.I)]:
        if not alt:
            continue
        val = get_value_from_record(record, alt)
        if val not in (None, "", "[]"):
            return val
    return None


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
    else:
        container_paths = []

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

    for rec in found_records:
        try:
            title = choose_first_available(rec, cfg.get('title', '')) or ''
            link = choose_first_available(rec, cfg.get('link', '')) or ''
            date = choose_first_available(rec, cfg.get('date', '')) or ''
            desc = choose_first_available(rec, cfg.get('description', '')) or ''

            name_lower = (cfg.get('name') or '').lower()

            # Fallbacks inteligentes para description (quando config não cobriu)
            if not desc:
                if '510k' in name_lower or 'fda510k' in name_lower:
                    for k in ('statement_or_summary', 'decision_description', 'applicant', 'device_name', 'openfda.device_name'):
                        v = get_value_from_record(rec, k)
                        if v:
                            desc = v
                            break
                elif 'maude' in name_lower or 'event' in name_lower:
                    for k in ('mdr_text[0].text', 'mdr_text', 'device[0].generic_name', 'device[0].brand_name', 'device'):
                        v = get_value_from_record(rec, k)
                        if v:
                            desc = v
                            break

            # Normalize date if looks like YYYYMMDD
            if date and re.match(r'^\d{8}$', date):
                try:
                    date = datetime.strptime(date, '%Y%m%d').date().isoformat()
                except Exception:
                    pass

            # Heurística para transformar ids em links úteis (openFDA queries)
            if link and not link.startswith('http'):
                lraw = link.strip()
                # 510k identifiers (Kxxxxx or digits)
                if re.match(r'^[Kk]?\d+$', lraw) and ('510k' in name_lower or 'fda510k' in name_lower or 'k_number' in (cfg.get('link') or '')):
                    knum = lraw.upper()
                    if not knum.startswith('K'):
                        knum = 'K' + knum
                    # Gera um link de consulta openFDA por k_number (fiável)
                    link = f"https://api.fda.gov/device/510k.json?k_number={knum}"
                # MAUDE style report numbers (2032227-2020-110170) -> query report_number
                elif '-' in lraw or re.match(r'^\d{6,}-\d{4}-\d+$', lraw):
                    q = lraw
                    link = f"https://api.fda.gov/device/event.json?search=report_number:{q}"
                # numeric mdr key -> query by mdr_report_key
                elif lraw.isdigit():
                    link = f"https://api.fda.gov/device/event.json?search=mdr_report_key:{lraw}"
                else:
                    # se não sabemos o que é, deixamos tal como está (possivelmente um token)
                    pass

            full_text = ' '.join([t for t in (title, desc, json.dumps(rec, ensure_ascii=False)[:1000]) if t])

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
# ---------------------- END JSON HANDLER ----------------------


def extract_items_from_html(html, cfg):
    preview = (html or '').lstrip()[:200].lower()
    is_xml = preview.startswith('<?xml') or '<rss' in preview or '<feed' in preview
    if is_xml:
        print(f"extract_items_from_html debug: is_xml=True html_len={len(html or '')}")
    else:
        print(f"extract_items_from_html debug: is_xml=False html_len={len(html or '')}")

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
            print("extract_items_from_html debug selectors counts:", [(cfg.get('item_container') or 'item/entry', len(nodes))], " total_nodes:", len(nodes))
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
                    dnode = node.find('description')
                    if dnode and getattr(dnode, 'string', None):
                        desc = dnode.string.strip()
                    else:
                        sn = node.find('summary')
                        if sn and getattr(sn, 'string', None):
                            desc = sn.string.strip()
                        else:
                            ce = node.find(lambda tag: (tag.name or '').lower() == 'content:encoded')
                            if ce and getattr(ce, 'string', None):
                                desc = ce.string.strip()
                            else:
                                c = node.find(lambda tag: (tag.name or '').lower() in ('content', 'content:encoded', 'dc:description'))
                                if c and getattr(c, 'string', None):
                                    desc = c.string.strip()
                    dnode = node.find('pubDate') or node.find('published') or node.find('updated') or node.find('dc:date')
                    if dnode and getattr(dnode, 'string', None):
                        date = dnode.string.strip()
                    else:
                        date = (node.find('updated') or node.find('published') or node.find('pubDate') or None)
                        if date and getattr(date, 'string', None):
                            date = date.string.strip()
                        else:
                            date = ''
                    full_text = (title or '') + ' ' + (desc or '') + ' ' + (node.get_text(" ", strip=True) or '')
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
            for fallback in ('li', 'article', 'div'):
                try:
                    found = soup.select(fallback)
                    if found:
                        nodes.extend(found)
                except Exception:
                    continue

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

        for node in nodes:
            try:
                title = ''
                link = ''
                date = ''
                desc = ''
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
                    t = node.find(['h1', 'h2', 'h3', 'a'])
                    if t:
                        title = t.get_text(strip=True)

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

                if desc and title and desc.strip() == title.strip():
                    try:
                        a_try = node.select_one('a.link, h4 a.title, a.title, a')
                        if a_try and a_try.has_attr('title'):
                            a_title_attr = (a_try.get('title') or '').strip()
                            if a_title_attr and a_title_attr != title:
                                desc = a_title_attr
                        if (not desc or desc.strip() == title.strip()):
                            pprev = node.find_previous('p')
                            if pprev:
                                ptxt = pprev.get_text(" ", strip=True)
                                if ptxt and ptxt != title:
                                    desc = ptxt
                        if (not desc or desc.strip() == title.strip()):
                            pnext = node.find_next('p')
                            if pnext:
                                ptxt = pnext.get_text(" ", strip=True)
                                if ptxt and ptxt != title:
                                    desc = ptxt
                    except Exception:
                        pass

                date = ''
                tried_selectors = []
                date_selectors = []
                if cfg.get('date'):
                    date_selectors = [s.strip() for s in cfg.get('date').split(',') if s.strip()]
                date_selectors += ['time', '.date', 'span.date', '.timestamp']

                def find_date_in(element):
                    for ds in date_selectors:
                        tried_selectors.append(ds)
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

    except Exception as e:
        print('extract_items_from_html: unexpected error', e)
        return items

    return items


def parse_feed(items):
    return items


def matches_filters_debug(item, cfg):
    kw = cfg.get('filters', {}).get('keywords', []) or []
    exclude = cfg.get('filters', {}).get('exclude', []) or []
    if not kw and not exclude:
        return True, None
    text_title = (item.get('title', '') or '').lower()
    text_desc = (item.get('description', '') or '').lower()
    text_full = (item.get('full_text', '') or '').lower()
    text_link = (item.get('link', '') or '').lower()

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
            desc_to_use = it.get('description') or ''
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
            ctype = resp.headers.get('Content-Type', '').lower()
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

        # NOVA BEHAVIOR: se existirem filtros e NENHUM bate, não fazemos fallback para TODOS os items.
        print(f'{len(matched)} items matched filters for {name}')
        if not matched:
            # Se não existem filtros (kw está vazio) então matches_filters_debug já devolveu True e matched conterá items.
            # Aqui significa que havia filtros mas nada bateu — devolvemos zero items (não fallback).
            if kw:
                print(f'No items matched filters for {name} — returning 0 items (no fallback).')
                matched = []
            else:
                # sem filtros configurados, mantemos todos os items
                print(f'No filters configured for {name}; keeping all {len(items)} items')
                matched = items

        deduped = dedupe_items(matched)

        build_feed(name, cfg, deduped)

    print('All done.')


if __name__ == '__main__':
    main()
