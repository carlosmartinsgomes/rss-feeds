#!/usr/bin/env python3
# scripts/generate_feeds.py
# Gere feeds RSS simples a partir de sites listados em sites.json
# Melhorias: procura de datas em ancestrais/irmãos e fallback de description
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


def fetch_html(url, timeout=20):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9'
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.text


def text_of_node(node):
    if node is None:
        return ''
    return ' '.join(node.stripped_strings)


def normalize_link_for_dedupe(href):
    """
    Normaliza um link para dedupe: remove parâmetros UTM, fragmentos, baixa para lowercase,
    mantém host + path + maybe query essentials. Retorna empty string se href vazio.
    """
    if not href:
        return ''
    try:
        p = urlparse(href)
        # se faltar esquema, assume https
        if not p.scheme:
            href = 'https://' + href.lstrip('/')
            p = urlparse(href)
        # remove utm* e outros tracking padrão
        qs = dict(parse_qsl(p.query, keep_blank_values=True))
        qs = {k: v for k, v in qs.items() if not k.lower().startswith('utm') and k.lower() not in ('fbclid', 'gclid')}
        # rebuild
        new_q = urlencode(qs, doseq=True)
        cleaned = urlunparse((p.scheme.lower(), p.netloc.lower(), p.path.rstrip('/'), '', new_q, ''))
        return cleaned
    except Exception:
        return href.strip().lower()


def collect_article_nodes_in_dom_order(soup, container_cfg, base_url=None):
    """
    Recebe BeautifulSoup soup e um item_container string (lista separada por vírgulas).
    Retorna uma lista de nodes em ordem DOM, tentando evitar duplicados triviais
    (preferindo nodes com link e/ou título).
    """
    selectors = [s.strip() for s in (container_cfg or '').split(',') if s.strip()]
    counts = []
    all_nodes = []
    # compute counts per selector for debug
    for sel in selectors:
        try:
            found = soup.select(sel)
            counts.append((sel, len(found)))
        except Exception:
            counts.append((sel, 0))

    # Try combined selection (preserves DOM order)
    combined = ', '.join(selectors) if selectors else ''
    nodes = []
    if combined:
        try:
            nodes = soup.select(combined)
        except Exception:
            nodes = []

    # If combined returned nothing, fallback to aggregating per selector (keeps insertion order)
    if not nodes:
        for sel in selectors:
            try:
                found = soup.select(sel)
                if found:
                    nodes.extend(found)
            except Exception:
                continue

    # If still nothing, fallback to common generic tags
    if not nodes:
        for fallback in ('li', 'article', 'div'):
            try:
                found = soup.select(fallback)
                if found:
                    nodes.extend(found)
            except Exception:
                continue

    # Deduplicate nodes quickly by normalized link or small title snippet while keeping DOM order
    deduped = []
    seen_keys = set()
    for n in nodes:
        key = ''
        try:
            a = n.select_one('a[href]')
            if a and a.has_attr('href'):
                href = a.get('href') or ''
                if href:
                    # normalize full absolute url
                    try:
                        href_abs = urljoin(base_url or '', href)
                    except Exception:
                        href_abs = href
                    key = normalize_link_for_dedupe(href_abs)
        except Exception:
            key = ''

        if not key:
            # try small title snippet
            try:
                title_el = n.select_one('a') or n.select_one('h4') or n.select_one('h3') or n
                title_txt = (title_el.get_text(" ", strip=True)[:200].strip().lower() if title_el else '')
                if title_txt:
                    key = title_txt
            except Exception:
                key = ''

        if not key:
            # fallback unique node id
            key = f"__node_{id(n)}"

        if key in seen_keys:
            continue
        seen_keys.add(key)
        deduped.append(n)

    # debug info: return counts and deduped nodes via attributes? We'll print counts here.
    total_unique = len(deduped)
    # attach debug info as attribute for caller if desired (not necessary, but print now)
    print(f"extract_items_from_html debug selectors counts: {counts} total_nodes: {total_unique}")
    return deduped


def extract_items_from_html(html, cfg):
    """
    Extrai items (title, link, description, date, full_text) de um HTML dado e uma config de site.
    cfg é um dict com possiveis chaves: item_container, title, link, description, date, url
    """
    soup = BeautifulSoup(html, 'html.parser')
    container_sel = cfg.get('item_container') or 'article'

    # Collect nodes in DOM order with dedupe
    nodes = collect_article_nodes_in_dom_order(soup, container_sel, base_url=cfg.get('url'))

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

            # Link extraction
            # support selectors like "a@href" or ".c-title a@href"
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

            # if link still empty, try to find any anchor in node but avoid bad hrefs
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

            # If desc == title, try to obtain a better description (fallbacks)
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

            # Date (best effort)
            date = ''
            tried_selectors = []
            date_selectors = []
            if cfg.get('date'):
                date_selectors = [s.strip() for s in cfg.get('date').split(',') if s.strip()]
            # ensure defaults
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

            # 1) try node itself
            date = find_date_in(node) or ''
            # 2) if not found, try up to 3 ancestors
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
            # 3) try siblings (previous / next)
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

            # fallback: full text
            full_text = (title or '') + ' ' + (desc or '') + ' ' + text_of_node(node)

            items.append({
                'title': title or '',
                'link': link or '',
                'description': desc or '',
                'date': date or '',
                'full_text': full_text or ''
            })
        except Exception:
            # ignorar item problemático, continuar
            continue

    return items


def parse_feed(items):
    # items expected as dicts with keys title, link, description, date, full_text
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
    """
    Remove duplicados por link normalizado (mantem a primeira ocorrencia).
    items: lista de dicts com keys 'link' e 'title' (ou similares).
    Retorna lista preservando ordem de aparicao.
    """
    unique = {}
    out = []
    for it in (items or []):
        key = normalize_link_for_dedupe(it.get('link') or '')
        if not key:
            # fallback para título curto
            key = (it.get('title', '') or '').strip().lower()[:200]
        if not key:
            # sem chave válida: gera um placeholder incremental
            key = f"__no_key__{len(out)}"
        if key not in unique:
            unique[key] = True
            out.append(it)
    return out


# ----- BUILD FEED: substituída para evitar usar full_text como fallback ----
def build_feed(name, cfg, items):
    """
    items: lista de dicts com keys 'title','link','description','date','full_text'
    Esta versão:
      - respeita cfg.get('max_items')
      - NÃO usa full_text como fallback para description (evita repetições do title)
      - escreve o feed RSS e faz debug mínimo
    """
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

    # truncar se necessário (mantém a ordem)
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
            # descrição: APENAS description explícita (sem fallback para full_text)
            desc_to_use = it.get('description') or ''
            fe.description(desc_to_use)
            # pubDate: tentar parse
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
            # continuar mesmo que um item cause erro
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
        # prefer rendered file if exists
        rf = cfg.get('render_file')
        if rf:
            # normalize path: allow "scripts/rendered/..." or "rendered/..."
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
            # fallback to requests
            try:
                print(f'Fetching {url} via requests...')
                html = fetch_html(url)
            except Exception as e:
                print(f'Request error for {url}: {e}')
                html = ''

        items = []
        if html:
            try:
                items = extract_items_from_html(html, cfg)

                # --- DEBUG: imprimir amostra dos items extraidos DO HTML (antes de filtros) ---
                try:
                    print(f"DEBUG: sample extracted items for {name} (first 20):")
                    for i, it in enumerate(items[:20]):
                        t = (it.get('title') or '')[:200]
                        l = (it.get('link') or '')[:200]
                        d_present = bool(it.get('description'))
                        print(f"  [{i}] title='{t}' link='{l}' desc_present={d_present}")
                except Exception:
                    pass

                if not items:
                    # fallback generic li selector
                    soup = BeautifulSoup(html, 'html.parser')
                    nodes = soup.select('li')
                    if nodes:
                        print(f'Fallback: found {len(nodes)} nodes with selector \'li\'')
                        for n in nodes[:200]:
                            title = n.get_text(" ", strip=True)[:200]
                            link = ''
                            a = n.find('a')
                            if a and a.has_attr('href'):
                                h = a.get('href') or ''
                                if not _bad_href_re.search(h):
                                    link = urljoin(cfg.get('url', ''), h)
                            items.append({'title': title, 'link': link, 'description': '', 'date': '', 'full_text': title})
            except Exception as e:
                print('Error parsing HTML:', e)
                items = []
        else:
            items = []

        print(f'Found {len(items)} items for {name} (raw)')

        # apply filters
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

        # dedupe (final)
        deduped = dedupe_items(matched)

        # write feed
        build_feed(name, cfg, deduped)

    print('All done.')


if __name__ == '__main__':
    main()
