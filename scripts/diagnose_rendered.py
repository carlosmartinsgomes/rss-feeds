#!/usr/bin/env python3
"""
scripts/diagnose_rendered.py

Usage:
  python3 scripts/diagnose_rendered.py yahoo-multiquote-news
  python3 scripts/diagnose_rendered.py medscape-derm0
  python3 scripts/diagnose_rendered.py all

Outputs:
  - prints a readable diagnostic to stdout
  - writes scripts/diagnose_<site>.csv with candidate items (title, link, description, date, snippet)
  - writes scripts/diagnose_<site>_anchors.txt listing first anchors found
"""

import os, sys, json, re, csv, glob
from bs4 import BeautifulSoup

ROOT = os.path.dirname(__file__)
SITES_JSON = os.path.join(ROOT, 'sites.json')
RENDERED_DIR = os.path.join(ROOT, 'rendered')
OUT_DIR = os.path.join(ROOT, '')

_antibot_phrases = [
    'verify you are human', 'verify that you are not a robot', 'checking your browser',
    'access denied', 'cloudflare', 'captcha', 'please enable javascript', 'security check',
    'you are being redirected', 'are you human', 'complete the security check'
]

def load_sites():
    with open(SITES_JSON, 'r', encoding='utf-8') as fh:
        j = json.load(fh)
    return {s.get('name'): s for s in j.get('sites', [])}

def find_rendered_file(cfg, name):
    # prefer explicit render_file path in cfg
    rf = cfg.get('render_file')
    if rf:
        rf_path = rf if os.path.isabs(rf) else os.path.join(os.path.dirname(__file__), rf)
        if os.path.exists(rf_path):
            return rf_path
        # try relative to scripts/rendered
        alt = os.path.join(RENDERED_DIR, os.path.basename(rf))
        if os.path.exists(alt):
            return alt
    # fallback: look for any rendered file containing the site hostname or name
    url = cfg.get('url', '')
    candidates = []
    try:
        host = ''
        if url:
            from urllib.parse import urlparse
            host = urlparse(url).hostname or ''
        patterns = [f"*{name}*.html", f"*{host}*.html", f"*{os.path.basename(host)}*.html"]
        for p in patterns:
            candidates.extend(glob.glob(os.path.join(RENDERED_DIR, p)))
    except Exception:
        pass
    # return largest candidate (more bytes) if many
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
    # accepts "selector@attr" or "selector" (text)
    if '@' in sel:
        s, attr = sel.split('@',1)
        s = s.strip(); attr = attr.strip()
        found = el.select_one(s) if s else None
        if not found: return None
        return found.get(attr)
    else:
        found = el.select_one(sel)
        if not found: return None
        return found.get_text(" ", strip=True)

def simple_extract_items_from_html(html, cfg):
    # a light-weight mirroring of the extractor for diagnostic purposes
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
    # fallback: collect <li>, <article>, <div> if none found
    if not nodes:
        for fb in ('li', 'article', 'div'):
            try:
                f = soup.select(fb)
                if f:
                    nodes.extend(f)
            except Exception:
                pass
    items = []
    for node in nodes:
        title = ''
        link = ''
        desc = ''
        date = ''
        # try cfg selectors
        for s in [t.strip() for t in str(cfg.get('title','')).split(',') if t.strip()]:
            try:
                v = get_text_or_attr(node, s)
                if v:
                    title = v; break
            except: pass
        for s in [t.strip() for t in str(cfg.get('link','')).split(',') if t.strip()]:
            try:
                if '@' in s:
                    v = get_text_or_attr(node, s)
                else:
                    el = node.select_one(s)
                    v = el.get('href') if el and el.has_attr('href') else None
                if v:
                    link = v; break
            except: pass
        for s in [t.strip() for t in str(cfg.get('description','')).split(',') if t.strip()]:
            try:
                v = get_text_or_attr(node, s)
                if v:
                    desc = v; break
            except: pass
        for s in [t.strip() for t in str(cfg.get('date','')).split(',') if t.strip()]:
            try:
                v = get_text_or_attr(node, s)
                if v:
                    date = v; break
            except: pass
        if not title:
            # fallbacks
            a = node.find(['h1','h2','h3','a'])
            if a:
                title = a.get_text(" ", strip=True)
        if not link:
            a = node.find('a', href=True)
            if a:
                link = a.get('href')
        snippet = node.get_text(" ", strip=True)[:200]
        items.append({'title': title or '', 'link': link or '', 'description': desc or '', 'date': date or '', 'snippet': snippet})
    return items, nodes

def analyze_rendered(name, cfg, path):
    print("\n" + "="*80)
    print(f"DIAGNOSE: site='{name}' render_path='{path}'")
    print("="*80)
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as fh:
            html = fh.read()
    except Exception as e:
        print("ERROR: cannot read rendered file:", e); return

    size = len(html)
    print(f"File size: {size} bytes")
    print("Antibot phrases present:", contains_antibot(html))
    print("\n--- excerpt (first 2000 chars) ---\n")
    print(html[:2000].replace('\n',' ')[:2000])
    print("\n--- end excerpt ---\n")

    soup = BeautifulSoup(html, 'html.parser')

    # selectors to test
    test_selectors = []
    if cfg.get('item_container'):
        test_selectors.extend([s.strip() for s in str(cfg.get('item_container')).split(',') if s.strip()])
    # extra common selectors
    extra = ['section[data-test="qsp-news"] ul li', 'ul[data-test="quoteNewsStream"] li', 'li.stream-item.story-item', 'li.js-stream-content']
    for e in extra:
        if e not in test_selectors:
            test_selectors.append(e)

    counts = []
    total_nodes = 0
    for sel in test_selectors:
        try:
            c = len(soup.select(sel))
        except Exception:
            c = -1
        counts.append((sel, c))
        total_nodes += max(0, c)
    print("Selector counts:")
    for sel,c in counts:
        print(f"  {sel!r:40} -> {c}")
    print("Total nodes (sum of selector matches):", total_nodes)

    # list anchors
    anchors = []
    for a in soup.find_all('a', href=True):
        href = a.get('href') or ''
        txt = a.get_text(" ", strip=True)[:80]
        anchors.append((href, txt))
    print(f"\nFound {len(anchors)} anchors (showing first 60):")
    for i,(h,t) in enumerate(anchors[:60]):
        print(f"  {i:02d}. href={h[:140]}  text={t}")

    # run lightweight extractor
    items, nodes = simple_extract_items_from_html(html, cfg)
    print(f"\nExtractor simulation: found {len(items)} candidate items (nodes scanned: {len(nodes)})")
    # write CSV
    csvfn = os.path.join(OUT_DIR, f"scripts/diagnose_{name}.csv")
    with open(csvfn, 'w', newline='', encoding='utf-8') as cf:
        w = csv.DictWriter(cf, fieldnames=['title','link','date','description','snippet'])
        w.writeheader()
        for it in items:
            w.writerow({'title': it['title'], 'link': it['link'], 'date': it['date'], 'description': it['description'], 'snippet': it['snippet']})
    print("Wrote CSV:", csvfn)
    anchorsfn = os.path.join(OUT_DIR, f"scripts/diagnose_{name}_anchors.txt")
    with open(anchorsfn, 'w', encoding='utf-8') as af:
        for h,t in anchors:
            af.write(h + "\t" + t + "\n")
    print("Wrote anchors list:", anchorsfn)

    # show some suspicious repeated results (anti-bot signatures)
    lower = html.lower()
    suspicious = []
    for ph in ['verify you are human','cloudflare','captcha','security check','access denied']:
        if ph in lower:
            suspicious.append(ph)
    if suspicious:
        print("\nWARNING: anti-bot / blocking phrases detected:", suspicious)

def main():
    if not os.path.exists(SITES_JSON):
        print("ERROR: sites.json not found at", SITES_JSON); sys.exit(1)
    sites = load_sites()
    args = sys.argv[1:]
    if not args:
        print("USAGE: diagnose_rendered.py <site-name>|all")
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
                print("Unknown site:", a)
    for name in targets:
        cfg = sites[name]
        rf = find_rendered_file(cfg, name)
        if not rf:
            print(f"\nNo rendered file found for site '{name}' (looked for cfg.render_file or files in scripts/rendered).")
            continue
        analyze_rendered(name, cfg, rf)

if __name__ == '__main__':
    main()
