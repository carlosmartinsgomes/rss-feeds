#!/usr/bin/env python3
# scripts/diagnose_rendered_logs.py
# Gera diagnóstico POR LOG (stdout) para sites cujo HTML já foi renderizado em scripts/rendered.
# Usage:
#   python3 scripts/diagnose_rendered_logs.py <site-name>      # ex: yahoo-multiquote-news
#   python3 scripts/diagnose_rendered_logs.py all              # roda para todos os sites
#
# Saída: texto com marcadores fáceis de copiar/colar para análises posteriores.
# Não escreve ficheiros.

import os, sys, json, re, glob
from bs4 import BeautifulSoup

ROOT = os.path.dirname(__file__)
SITES_JSON = os.path.join(ROOT, 'sites.json')
RENDERED_DIR = os.path.join(ROOT, 'rendered')

_antibot_phrases = [
    'verify you are human', 'verify that you are not a robot', 'checking your browser',
    'access denied', 'cloudflare', 'captcha', 'please enable javascript',
    'security check', 'you are being redirected', 'are you human', 'complete the security check'
]

def load_sites():
    if not os.path.exists(SITES_JSON):
        print(f"ERROR: sites.json not found at {SITES_JSON}", file=sys.stderr)
        sys.exit(1)
    with open(SITES_JSON, 'r', encoding='utf-8') as fh:
        j = json.load(fh)
    return {s.get('name'): s for s in j.get('sites', [])}

def find_rendered_file(cfg, name):
    rf = cfg.get('render_file')
    if rf:
        # try given path
        rf_path = rf if os.path.isabs(rf) else os.path.join(os.path.dirname(__file__), rf)
        if os.path.exists(rf_path):
            return rf_path
        alt = os.path.join(RENDERED_DIR, os.path.basename(rf))
        if os.path.exists(alt):
            return alt
    # fallback: look for any rendered file containing name or hostname
    url = cfg.get('url','')
    host = ''
    try:
        from urllib.parse import urlparse
        host = urlparse(url).hostname or ''
    except Exception:
        pass
    candidates = []
    patterns = [f"*{name}*.html", f"*{host}*.html", f"*{os.path.basename(host)}*.html"]
    for p in patterns:
        candidates.extend(glob.glob(os.path.join(RENDERED_DIR, p)))
    if candidates:
        candidates = sorted(candidates, key=lambda p: os.path.getsize(p), reverse=True)
        return candidates[0]
    return None

def contains_antibot(text):
    if not text:
        return False
    t = text.lower()
    return any(ph in t for ph in _antibot_phrases)

def get_text_or_attr(el, sel):
    if not sel:
        return None
    if '@' in sel:
        sel_part, attr = sel.split('@',1)
        sel_part = sel_part.strip()
        attr = attr.strip()
        found = el.select_one(sel_part) if sel_part else None
        if not found:
            return None
        return found.get(attr)
    else:
        found = el.select_one(sel)
        if not found:
            return None
        return found.get_text(" ", strip=True)

def simple_extract_items_from_html(html, cfg, max_items=200):
    soup = BeautifulSoup(html, 'html.parser')
    container_sel = cfg.get('item_container') or 'article'
    nodes = []
    for sel in [s.strip() for s in str(container_sel).split(',') if s.strip()]:
        try:
            found = soup.select(sel)
            if found:
                nodes.extend(found)
        except Exception:
            pass
    if not nodes:
        for fb in ('li','article','div'):
            try:
                f = soup.select(fb)
                if f:
                    nodes.extend(f)
            except Exception:
                pass
    items = []
    for node in nodes[:max_items]:
        title = ''
        link = ''
        desc = ''
        date = ''
        # title
        tsel = cfg.get('title','')
        for s in [t.strip() for t in str(tsel).split(',') if t.strip()]:
            try:
                v = get_text_or_attr(node, s)
                if v:
                    title = v; break
            except: pass
        # link
        lsel = cfg.get('link','')
        for s in [t.strip() for t in str(lsel).split(',') if t.strip()]:
            try:
                if '@' in s:
                    v = get_text_or_attr(node, s)
                else:
                    el = node.select_one(s)
                    v = el.get('href') if el and el.has_attr('href') else None
                if v:
                    link = v; break
            except: pass
        # desc
        dsel = cfg.get('description','')
        for s in [t.strip() for t in str(dsel).split(',') if t.strip()]:
            try:
                v = get_text_or_attr(node, s)
                if v:
                    desc = v; break
            except: pass
        # date
        dtsel = cfg.get('date','')
        for s in [t.strip() for t in str(dtsel).split(',') if t.strip()]:
            try:
                v = get_text_or_attr(node, s)
                if v:
                    date = v; break
            except: pass
        if not title:
            a = node.find(['h1','h2','h3','a'])
            if a:
                title = a.get_text(" ", strip=True)
        if not link:
            a = node.find('a', href=True)
            if a:
                link = a.get('href')
        snippet = node.get_text(" ", strip=True)[:300]
        items.append({'title': title or '', 'link': link or '', 'date': date or '', 'description': desc or '', 'snippet': snippet})
    return items, len(nodes)

def print_block_marker(kind, site):
    print("="*80)
    print(f"===DIAG_{kind}_SITE={site}===")
    print("="*80)

def analyze_rendered(name, cfg, path):
    print_block_marker("START", name)
    print(f"rendered_path: {path}")
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as fh:
            html = fh.read()
    except Exception as e:
        print("ERROR: cannot read rendered file:", e)
        print_block_marker("END", name)
        return

    size = len(html)
    print(f"rendered_bytes: {size}")
    antibot = contains_antibot(html)
    print(f"ANTIBOT_DETECTED: {antibot}")
    # excerpt head/tail
    head = html[:5000]
    tail = html[-3000:] if len(html) > 3000 else ''
    print_block_marker("HTML_EXCERPT_HEAD", name)
    print(head.replace('\n',' '))
    print_block_marker("HTML_EXCERPT_TAIL", name)
    print(tail.replace('\n',' '))
    # selector counts
    soup = BeautifulSoup(html, 'html.parser')
    selectors = []
    if cfg.get('item_container'):
        selectors.extend([s.strip() for s in str(cfg.get('item_container')).split(',') if s.strip()])
    # add common alternatives for diagnostics (yahoo etc)
    extras = ['section[data-test="qsp-news"] ul li', 'ul[data-test="quoteNewsStream"] li',
              'li.stream-item.story-item', 'li.js-stream-content']
    for e in extras:
        if e not in selectors:
            selectors.append(e)
    print_block_marker("SELECTOR_COUNTS", name)
    total_nodes = 0
    for sel in selectors:
        try:
            c = len(soup.select(sel))
        except Exception:
            c = -1
        total_nodes += max(0, c)
        print(f"SELECTOR_COUNT: '{sel}' -> {c}")
    print(f"SELECTOR_COUNTS_SUM: {total_nodes}")
    # anchors
    anchors = []
    for a in soup.find_all('a', href=True):
        href = a.get('href') or ''
        txt = a.get_text(" ", strip=True)[:200]
        anchors.append((href, txt))
    print_block_marker("ANCHORS_SUMMARY", name)
    print(f"ANCHORS_FOUND: {len(anchors)}")
    for i,(h,t) in enumerate(anchors[:120]):
        print(f"ANCHOR[{i}]\t{h}\t| text={t}")
    # item simulation
    items, nodes_scanned = simple_extract_items_from_html(html, cfg, max_items=300)
    print_block_marker("EXTRACTOR_SIMULATION", name)
    print(f"ITEM_NODES_SCANNED: {nodes_scanned}")
    print(f"ITEMS_FOUND: {len(items)}")
    for i,it in enumerate(items[:200]):
        print(f"ITEM[{i}]\tTITLE={it['title']}\tLINK={it['link']}\tDATE={it['date']}\tDESC={it['description']}\tSNIPPET={it['snippet'][:200]}")
    # suspicious
    lower = html.lower()
    suspicious = []
    for ph in ['verify you are human','cloudflare','captcha','security check','access denied','please enable javascript']:
        if ph in lower:
            suspicious.append(ph)
    if suspicious:
        print_block_marker("SUSPICIOUS_PHRASES", name)
        for s in suspicious:
            print(f"SUSPICIOUS: {s}")
    print_block_marker("END", name)

def main():
    sites = load_sites()
    args = sys.argv[1:]
    if not args:
        print("USAGE: diagnose_rendered_logs.py <site-name>|all")
        print("Available sites:", ", ".join(sorted(sites.keys())))
        sys.exit(1)
    targets = []
    if args[0].lower() == 'all':
        targets = list(sites.keys())
    else:
        for a in args:
            if a in sites:
                targets.append(a)
            else:
                print(f"Unknown site: {a}", file=sys.stderr)
    for name in targets:
        cfg = sites[name]
        rf = find_rendered_file(cfg, name)
        if not rf:
            print_block_marker("START", name)
            print(f"rendered_path: MISSING (checked cfg.render_file and scripts/rendered/* for patterns)")
            print_block_marker("END", name)
            continue
        analyze_rendered(name, cfg, rf)

if __name__ == '__main__':
    main()
