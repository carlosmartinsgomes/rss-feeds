#!/usr/bin/env python3
# scripts/generate_feeds.py
# Gere feeds RSS simples a partir de sites listados em sites.json
# Melhorias: procura de datas em ancestrais/irmãos, fallback de description,
# escreve feeds_debug.json com items (inclui tentativa de fetch do artigo para full_text).

import os
import json
import re
import sys
import time
from bs4 import BeautifulSoup
import requests
from feedgen.feed import FeedGenerator
from datetime import datetime
from urllib.parse import urljoin, urlparse, urlunparse, parse_qsl, urlencode

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
# debug JSON path (one level up from scripts/)
DEBUG_JSON = os.path.join(ROOT, '..', 'feeds_debug.json')

# regex para filtrar hrefs inúteis (ajusta conforme necessário)
_bad_href_re = re.compile(r'(^#|^javascript:|mailto:|/help|/legal|cookie|privacy|terms|signin|login|settings|/consent|/preferences|/policies)', re.I)


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


def extract_items_from_html(html, cfg):
    """
    Extrai items (title, link, description, date, full_text) de um HTML dado e uma config de site.
    cfg é um dict com possiveis chaves: item_container, title, link, description, date, url
    """
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


def build_feed(name, cfg, items):
    fg = FeedGenerator()
    fg.title(name)
    fg.link(href=cfg.get('url', ''), rel='alternate')
    fg.description(f'Feed gerado para {name}')
    count = 0
    for it in items:
        fe = fg.add_entry()
        fe.title(it.get('title') or 'No title')
        if it.get('link'):
            try:
                fe.link(href=it.get('link'))
            except Exception:
                pass
        # description: prefer description, fallback full_text
        fe.description(it.get('description') or it.get('full_text') or '')
        # pubDate: try to set raw string if possible (feedgen can accept string)
        if it.get('date'):
            try:
                # attempt to parse to RFC-2822 by trying dateutil.parse, fallback to raw
                try:
                    dt = dateparser.parse(it.get('date'))
                    fe.pubDate(dt)
                except Exception:
                    fe.pubDate(it.get('date'))
            except Exception:
                pass
        count += 1
    outdir = os.path.join(ROOT, '..', 'feeds') if os.path.exists(os.path.join(ROOT, '..', 'feeds')) else os.path.join(ROOT, '..', 'feeds')
    os.makedirs(outdir, exist_ok=True)
    outpath = os.path.join(outdir, f'{name}.xml')
    fg.rss_file(outpath)
    print(f'Wrote {outpath}')


# Heurística: tenta buscar o artigo completo e extrair corpo principal
def try_fetch_article_body(link, site_base_url=None, timeout=12):
    if not link:
        return ''
    try:
        # evita fetchs óbvios para domínios externos (opcional)
        if site_base_url:
            try:
                base_host = urlparse(site_base_url).netloc.lower()
                link_host = urlparse(link).netloc.lower()
                # se ambos definidos e diferentes -> ainda assim tentamos (alguns artigos redirecionam)
                # mas podes optar por evitar fetchs a domains externos: uncomment next lines to block
                # if base_host and link_host and base_host != link_host:
                #     return ''
            except Exception:
                pass
        html = fetch_html(link, timeout=timeout)
        soup = BeautifulSoup(html, 'html.parser')
        # selectors comuns para corpo do artigo
        selectors = [
            'article',
            'div[data-component-id*="body"]',
            '.field--name-body',
            '.field.field--name-body',
            '.article-body',
            '.article-content',
            '.post-content',
            '.entry-content',
            '.content-body',
            '.field--name-field-article-body',
            '#content'
        ]
        for sel in selectors:
            try:
                el = soup.select_one(sel)
            except Exception:
                el = None
            if el:
                # remove scripts/iframes/styles
                for bad in el.select('script, style, iframe, noscript'):
                    bad.decompose()
                txt = el.get_text(" ", strip=True)
                if txt and len(txt) > 80:
                    return txt
        # fallback: igreja de body text (limpa)
        body = soup.body
        if body:
            for bad in body.select('script, style, iframe, noscript'):
                bad.decompose()
            txt = body.get_text(" ", strip=True)
            return txt[:20000] if txt else ''
        return ''
    except Exception:
        return ''


def main():
    sites = load_sites()
    print(f'Loaded {len(sites)} site configurations from {SITES_JSON}')

    # collect debug info to write feeds_debug.json
    debug_sites = []

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

        # dedupe
        deduped = dedupe_items(matched)

        # Try to improve full_text by fetching article page when the list-item full_text is small
        for idx, it in enumerate(deduped):
            try:
                ft = (it.get('full_text') or '').strip()
                # if short or empty, attempt to fetch the article page body (but only for same-site links typically)
                if (not ft or len(ft) < 200) and it.get('link'):
                    u_base = cfg.get('url') or url
                    # only try fetch for http(s) links
                    try:
                        parsed = urlparse(it.get('link'))
                        if parsed.scheme in ('http', 'https'):
                            print(f'Attempting article fetch for item {idx} -> {it.get("link")}')
                            body = try_fetch_article_body(it.get('link'), site_base_url=u_base, timeout=10)
                            if body and len(body) > 80:
                                it['full_text'] = body
                                print(' -> fetched full article text (len=%d)' % len(body))
                                # small polite pause to avoid hammering
                                time.sleep(0.2)
                    except Exception:
                        pass
            except Exception:
                pass

        # write feed
        build_feed(name, cfg, deduped)

        # append debug info
        debug_sites.append({
            'site': name,
            'url': url,
            'items': deduped
        })

    # write debug JSON for feeds_to_excel fallback
    try:
        out_debug = os.path.abspath(os.path.join(DEBUG_JSON))
        with open(out_debug, 'w', encoding='utf-8') as fh:
            json.dump({'sites': debug_sites}, fh, ensure_ascii=False, indent=2)
        print(f'Wrote debug JSON: {out_debug}')
    except Exception as e:
        print('Failed to write debug JSON:', e)

    print('All done.')


if __name__ == '__main__':
    main()
