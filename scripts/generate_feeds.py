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


def fetch_url(url, timeout=20):
    """
    Substitui a antiga fetch_html: devolve objecto Response.
    Mantém headers user-agent.
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0 Safari/537.36',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept': '*/*'
    }
    r = requests.get(url, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r


def fetch_html(url, timeout=20):
    """
    Mantive para compatibilidade: devolve apenas texto (usado quando render_file está presente).
    """
    return fetch_url(url, timeout=timeout).text


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


# ---------------------- JSON HANDLER ADDED ----------------------
def get_value_from_record(record, path):
    """
    path: 'openfda.device_name' or 'device_name' or 'mdr_text[0].text'
    retorna uma string (se for lista, junta com '; ' ou escolhe o primeiro)
    """
    if not path or record is None:
        return None
    path = path.strip()
    # support bracket indices like mdr_text[0].text
    # split on '.' but keep bracketed indices
    parts = []
    for part in re.split(r'\.(?![^\[]*\])', path):
        parts.append(part)

    cur = record
    try:
        for p in parts:
            # handle OR if someone passed an expression (should be handled above)
            if ' OR ' in p:
                p = p.split(' OR ')[0].strip()
            # handle bracket indices
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

            # normal path
            if isinstance(cur, dict):
                cur = cur.get(p)
            elif isinstance(cur, list):
                # if cur is a list and p is a key name, try to map each item if dicts, else take first element
                if not cur:
                    return None
                # if list of dicts and first dict has key p, use list of those values
                if isinstance(cur[0], dict) and p in cur[0]:
                    vals = []
                    for it in cur:
                        v = it.get(p)
                        if v is not None:
                            vals.append(v)
                    if vals:
                        # flatten simple lists of strings
                        if all(not isinstance(v, (dict, list)) for v in vals):
                            return "; ".join(str(v) for v in vals if v)
                        else:
                            cur = vals
                    else:
                        cur = None
                else:
                    # fallback: take first element and continue
                    cur = cur[0]
                    # attempt to get attribute from it
                    if isinstance(cur, dict):
                        cur = cur.get(p)
            else:
                # cannot traverse further
                return None

        # final normalization
        if cur is None:
            return None
        if isinstance(cur, list):
            # join stringifiable values
            txts = []
            for it in cur:
                if isinstance(it, dict):
                    # attempt to get text-ish keys?
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
    """
    selector_expr pode ser "openfda.device_name OR device_name OR k_number"
    tenta cada alternativa e retorna o primeiro valor não-nulo.
    """
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
    """
    Extrai items de um JSON (openFDA etc) segundo cfg (mesmo formato sites.json).
    Retorna lista de dicts: {'title','link','description','date','full_text'}
    """
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

    # try each container path
    for path in container_paths:
        if not path:
            continue
        cur = json_obj
        # allow simple 'results' or nested 'meta.results' etc
        parts = path.split('.')
        for p in parts:
            if cur is None:
                break
            if isinstance(cur, dict):
                cur = cur.get(p)
            else:
                # if cur is list and p is numeric? break
                break
        if isinstance(cur, list):
            found_records.extend(cur)
        elif isinstance(cur, dict):
            # if dict contains 'results' as list, use it
            if 'results' in cur and isinstance(cur['results'], list):
                found_records.extend(cur['results'])
            else:
                # single record
                found_records.append(cur)

    # fallback common names
    if not found_records:
        for try_name in ("results", "data", "items"):
            if try_name in json_obj and isinstance(json_obj[try_name], list):
                found_records = json_obj[try_name]
                break

    # final fallback: if top-level is a list
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

            # small heuristic to build link for known FDA k_number values
            link_cfg = cfg.get('link', '') or ''
            if link and not link.startswith('http'):
                if 'k_number' in link_cfg or cfg.get('name', '').lower().startswith('fda510k'):
                    # normalizar K prefix se necessário (remove spaces)
                    k = link.strip()
                    # if looks like K923368 or 923368, build pmn url
                    if re.match(r'^[Kk]?\d+$', k) or re.match(r'^[Kk]\d+$', k):
                        knum = k.upper().lstrip('K')
                        # construct a simple lookup URL (pragmatic)
                        link = f"https://www.accessdata.fda.gov/scripts/cdrh/cfdocs/cfpmn/pmn.cfm?ID={knum}"
                # else leave as-is

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
    """
    Extrai items (title, link, description, date, full_text) de um HTML ou XML dado e uma config de site.
    cfg é um dict com possiveis chaves: item_container, title, link, description, date, url
    """
    # detectar se é um feed XML/Atom
    is_xml = False
    preview = (html or '').lstrip()[:200].lower()
    if preview.startswith('<?xml') or '<rss' in preview or '<feed' in preview:
        is_xml = True

    # debug
    if is_xml:
        print(f"extract_items_from_html debug: is_xml=True html_len={len(html or '')}")
    else:
        print(f"extract_items_from_html debug: is_xml=False html_len={len(html or '')}")

    items = []
    try:
        if is_xml:
            # usar parser XML para feeds
            soup = BeautifulSoup(html, 'xml')
            container_sel = cfg.get('item_container') or ''
            nodes = []
            # se o item_container for especificado como nomes de tags (ex: "item" ou "entry")
            if container_sel:
                for s in [t.strip() for t in container_sel.split(',') if t.strip()]:
                    # tentativa 1: tratar como tag name
                    try:
                        found = soup.find_all(s)
                        if found:
                            nodes.extend(found)
                    except Exception:
                        pass
                    # tentativa 2: tratar como CSS selector (pouco comum em xml, mas tentamos)
                    try:
                        found2 = soup.select(s)
                        if found2:
                            nodes.extend(found2)
                    except Exception:
                        pass
            # fallback common feed tags
            if not nodes:
                nodes = soup.find_all(['item', 'entry']) or []

            # remover None/duplicados
            nodes = [n for n in nodes if n is not None]
            # debug
            print("extract_items_from_html debug selectors counts:", [(cfg.get('item_container') or 'item/entry', len(nodes))], " total_nodes:", len(nodes))

            for node in nodes:
                try:
                    title = ''
                    link = ''
                    date = ''
                    desc = ''

                    # title: <title> or <headline> or <title> child
                    tnode = node.find('title')
                    if tnode and tnode.string:
                        title = tnode.string.strip()
                    else:
                        # atom: may have <headline> or <name> or <summary>
                        tnode = node.find(['name', 'headline'])
                        if tnode and getattr(tnode, 'string', None):
                            title = tnode.string.strip()
                        else:
                            # as last resort, text of node
                            title = (node.get_text(" ", strip=True) or '')[:1000]

                    # link: <link> content OR <link href="..."> (Atom)
                    lnode = node.find('link')
                    if lnode:
                        # atom-style: <link href="..." rel="alternate"/>
                        href = lnode.get('href') or lnode.get('HREF') or None
                        if href:
                            link = urljoin(cfg.get('url', ''), href)
                        else:
                            # sometimes <link> contains url text
                            txt = (lnode.string or '').strip()
                            if txt:
                                link = urljoin(cfg.get('url', ''), txt)
                            else:
                                # in RSS sometimes there are multiple link tags; try attributes of others
                                # check for link with rel='alternate'
                                alt = node.find('link', attrs={'rel': 'alternate'})
                                if alt and alt.get('href'):
                                    link = urljoin(cfg.get('url', ''), alt.get('href'))
                    # fallback: <guid>
                    if not link:
                        g = node.find('guid')
                        if g and getattr(g, 'string', None):
                            candidate = g.string.strip()
                            if candidate and not _bad_href_re.search(candidate):
                                link = urljoin(cfg.get('url', ''), candidate)
                    # if still not found, check for any <link> text children
                    if not link:
                        # sometimes link is inside <url> or <link> children as text
                        ltxt = node.find(['link', 'url'])
                        if ltxt and getattr(ltxt, 'string', None):
                            candidate = ltxt.string.strip()
                            if candidate and not _bad_href_re.search(candidate):
                                link = urljoin(cfg.get('url', ''), candidate)

                    # description: <description>, <summary>, <content:encoded>
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
                                # try <content> or <dc:description>
                                c = node.find(lambda tag: (tag.name or '').lower() in ('content', 'content:encoded', 'dc:description'))
                                if c and getattr(c, 'string', None):
                                    desc = c.string.strip()

                    # date: pubDate, published, updated, lastBuildDate
                    dnode = node.find('pubDate') or node.find('published') or node.find('updated') or node.find('dc:date')
                    if dnode and getattr(dnode, 'string', None):
                        date = dnode.string.strip()
                    else:
                        # sometimes date is an attribute
                        # e.g. <updated> or <published> with text
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

        # ----------------------------------------------------------
        # HTML path (existing behavior)
        # ----------------------------------------------------------
        soup = BeautifulSoup(html, 'html.parser')
        container_sel = cfg.get('item_container') or 'article'
        nodes = []
        for sel in [s.strip() for s in container_sel.split(',')]:
            try:
                found = soup.select(sel)
                if found:
                    nodes.extend(found)
            except Exception:
                # selector inválido -> ignora
                continue

        # fallback generic: se nada encontrado, tenta 'li' e 'article'
        if not nodes:
            for fallback in ('li', 'article', 'div'):
                try:
                    found = soup.select(fallback)
                    if found:
                        nodes.extend(found)
                except Exception:
                    continue

        # debug: counts per selector (simple)
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

    except Exception as e:
        print('extract_items_from_html: unexpected error', e)
        # no matter what, devolver o que tiver sido apanhado
        return items

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


# ----- BUILD FEED: substituída para evitar usar full_text como fallback -----
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
        resp = None
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
            # fallback to requests; use fetch_url to inspect headers
            try:
                print(f'Fetching {url} via requests...')
                resp = fetch_url(url)
                # do not call resp.raise_for_status() again, fetch_url already did
            except Exception as e:
                print(f'Request error for {url}: {e}')
                resp = None

        items = []
        # if we have a rendered HTML file, treat as HTML
        if html:
            try:
                items = extract_items_from_html(html, cfg)
            except Exception as e:
                print('Error parsing rendered HTML:', e)
                items = []
        elif resp is not None:
            # Decide JSON vs HTML based on Content-Type or content startswith
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
                # treat as HTML
                try:
                    items = extract_items_from_html(body, cfg)
                except Exception as e:
                    print('Error parsing HTML response:', e)
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

        # dedupe
        deduped = dedupe_items(matched)

        # write feed
        build_feed(name, cfg, deduped)

    print('All done.')


if __name__ == '__main__':
    main()
