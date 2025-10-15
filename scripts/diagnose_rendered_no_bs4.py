# na raiz do repo: cria o ficheiro diretamente
mkdir -p scripts
cat > scripts/diagnose_rendered_no_bs4.py <<'PY'
#!/usr/bin/env python3
# scripts/diagnose_rendered_no_bs4.py
# Diagnóstico leve sem dependências externas (usa stdlib).
# Produz blocos marcados que devem aparecer nos logs do CI.

import os, sys, json, glob, re
from html import unescape

ROOT_DIR = os.path.dirname(__file__)
SITES_JSON_CANDIDATES = [
    os.path.join(ROOT_DIR, '..', 'scripts', 'sites.json'),
    os.path.join(ROOT_DIR, '..', 'sites.json'),
    os.path.join(ROOT_DIR, 'sites.json'),
]
RENDERED_DIR = os.path.join(ROOT_DIR, 'rendered')

_antibot_phrases = [
    'verify you are human', 'verify that you are not a robot', 'checking your browser',
    'access denied', 'cloudflare', 'captcha', 'please enable javascript',
    'security check', 'you are being redirected', 'are you human'
]

def load_sites():
    for p in SITES_JSON_CANDIDATES:
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8') as fh:
                    j = json.load(fh)
                return {s.get('name'): s for s in j.get('sites', [])}
            except Exception:
                continue
    print("ERROR: sites.json not found in expected locations.", file=sys.stderr)
    return {}

def find_rendered_file(cfg, name):
    rf = cfg.get('render_file')
    if rf:
        candidate = rf if os.path.isabs(rf) else os.path.join(os.path.dirname(__file__), rf)
        if os.path.exists(candidate):
            return candidate
        alt = os.path.join(RENDERED_DIR, os.path.basename(rf))
        if os.path.exists(alt):
            return alt
    # fallback: search scripts/rendered by host or name
    url = cfg.get('url','')
    host = ''
    try:
        from urllib.parse import urlparse
        host = (urlparse(url).hostname or '').replace(':','')
    except Exception:
        host = ''
    patterns = [f"*{name}*.html", f"*{host}*.html", f"*{host.replace('.','_')}*.html"]
    for pat in patterns:
        l = glob.glob(os.path.join(RENDERED_DIR, pat))
        if l:
            # prefer largest (most content)
            l = sorted(l, key=lambda p: os.path.getsize(p), reverse=True)
            return l[0]
    return None

def contains_antibot(text):
    if not text: return False
    lo = text.lower()
    return any(p in lo for p in _antibot_phrases)

def strip_tags(text):
    # remove html tags quickly
    return re.sub(r'<[^>]+>', '', text)

def anchor_extract_all(html):
    # simple regex-based anchor extraction (approximate)
    anchors = []
    for m in re.finditer(r'<a\b([^>]*)>(.*?)</a>', html, flags=re.I|re.S):
        attrs = m.group(1)
        inner = strip_tags(m.group(2)).strip()
        href_m = re.search(r'href\s*=\s*([\'"])(.*?)\1', attrs, flags=re.I|re.S)
        href = href_m.group(2) if href_m else ''
        anchors.append((unescape(href.strip()), unescape(inner or '').strip()))
    return anchors

def count_selector_occurrences(html, sel):
    sel = (sel or '').strip()
    if not sel:
        return 0
    # heuristics for common selectors:
    # id selector: #foo
    if sel.startswith('#'):
        name = sel[1:]
        return len(re.findall(r'id\s*=\s*["\']%s["\']' % re.escape(name), html, flags=re.I))
    # attribute selector like section[data-test='qsp-news']
    m = re.match(r'([a-z0-9]+)?\s*\[\s*([a-zA-Z0-9_\-:]+)\s*=\s*[\'"]?([^\'"\]]+)[\'"]?\s*\]', sel)
    if m:
        attr = m.group(2); val = m.group(3)
        return len(re.findall(r'%s\s*=\s*["\']%s["\']' % (re.escape(attr), re.escape(val)), html, flags=re.I))
    # class selector .foo or tag.class1.class2
    if '.' in sel:
        parts = sel.split('.')
        tag = parts[0] if not sel.startswith('.') else None
        classes = parts[1:] if not sel.startswith('.') else parts
        if tag:
            # count occurrences of <tag ... class="...cls..."
            cnt = 0
            for cls in classes:
                cnt += len(re.findall(r'<%s\b[^>]*class\s*=\s*["\'][^"\']*\b%s\b[^"\']*["\']' % (re.escape(tag), re.escape(cls)), html, flags=re.I))
            # approximate by min of counts
            return min(cnt, 999999)
        else:
            # just class counts
            cnt = 0
            for cls in classes:
                cnt += len(re.findall(r'class\s*=\s*["\'][^"\']*\b%s\b[^"\']*["\']' % re.escape(cls), html, flags=re.I))
            return cnt
    # fallback: count occurrences of opening tag
    if re.match(r'^[a-zA-Z0-9]+$', sel):
        return len(re.findall(r'<%s\b' % re.escape(sel), html, flags=re.I))
    # if it's a complex css selector, fallback to substring occurrences
    return html.count(sel)

def print_block_marker(kind, site):
    print("="*80)
    print(f"===DIAG_{kind}_SITE={site}===")
    print("="*80)

def analyze_rendered(name, cfg, path):
    print_block_marker("START", name)
    print("rendered_path:", path)
    try:
        with open(path, 'r', encoding='utf-8', errors='replace') as fh:
            html = fh.read()
    except Exception as e:
        print("ERROR: cannot read rendered file:", e)
        print_block_marker("END", name)
        return
    size = len(html)
    print("rendered_bytes:", size)
    antibot = contains_antibot(html)
    print("ANTIBOT_DETECTED:", antibot)
    head = html[:4000]
    tail = html[-2000:] if len(html) > 2000 else ''
    print_block_marker("HTML_EXCERPT_HEAD", name)
    print(head.replace('\n',' '))
    print_block_marker("HTML_EXCERPT_TAIL", name)
    print(tail.replace('\n',' '))
    # selector diagnostics
    selectors = []
    if cfg.get('item_container'):
        selectors += [s.strip() for s in str(cfg.get('item_container')).split(',') if s.strip()]
    # add some extra selectors commonly used
    extra = ['section[data-test=\"qsp-news\"] ul li', 'ul[data-test=\"quoteNewsStream\"] li', 'li.stream-item.story-item', 'li.js-stream-content']
    for e in extra:
        if e not in selectors:
            selectors.append(e)
    print_block_marker("SELECTOR_COUNTS", name)
    total_nodes = 0
    for sel in selectors:
        try:
            c = count_selector_occurrences(html, sel)
        except Exception:
            c = -1
        total_nodes += max(0, c)
        print(f"SELECTOR_COUNT: '{sel}' -> {c}")
    print("SELECTOR_COUNTS_SUM:", total_nodes)
    anchors = anchor_extract_all(html)
    print_block_marker("ANCHORS_SUMMARY", name)
    print("ANCHORS_FOUND:", len(anchors))
    for i,(h,t) in enumerate(anchors[:120]):
        print(f"ANCHOR[{i}]\t{h}\t| text={t[:200]}")
    # try simple extraction simulation (title/link/desc/date)
    print_block_marker("EXTRACTOR_SIMULATION", name)
    # very simple node scan: find <li ...> blocks as candidate items
    li_blocks = re.findall(r'(<li\b[^>]*>.*?</li>)', html, flags=re.I|re.S)[:300]
    items = []
    for idx, block in enumerate(li_blocks):
        # find first anchor title, link
        a_m = re.search(r'<a\b([^>]*)>(.*?)</a>', block, flags=re.I|re.S)
        title = ''
        link = ''
        if a_m:
            attrs = a_m.group(1)
            inner = strip_tags(a_m.group(2)).strip()
            href_m = re.search(r'href\s*=\s*([\'"])(.*?)\1', attrs, flags=re.I|re.S)
            href = href_m.group(2) if href_m else ''
            title = unescape(inner)
            link = href
        desc = strip_tags(block)[:200].strip()
        items.append({'title': title, 'link': link, 'desc': desc})
        if idx >= 199: break
    print("ITEM_NODES_SCANNED:", len(li_blocks))
    print("ITEMS_FOUND:", len(items))
    for i,it in enumerate(items[:200]):
        print(f"ITEM[{i}]\tTITLE={it['title']}\tLINK={it['link']}\tDESC={it['desc'][:200]}")
    # suspicious phrases
    lower = html.lower()
    suspicious = [ph for ph in _antibot_phrases if ph in lower]
    if suspicious:
        print_block_marker("SUSPICIOUS_PHRASES", name)
        for s in suspicious:
            print("SUSPICIOUS:", s)
    print_block_marker("END", name)

def main():
    sites = load_sites()
    if not sites:
        print("No sites loaded; aborting.", file=sys.stderr); sys.exit(1)
    args = sys.argv[1:]
    if not args:
        print("USAGE: diagnose_rendered_no_bs4.py <site-name>|all")
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
                print("Unknown site:", a, file=sys.stderr)
    for name in targets:
        cfg = sites[name]
        rf = find_rendered_file(cfg, name)
        if not rf:
            print_block_marker("START", name)
            print("rendered_path: MISSING (checked cfg.render_file and scripts/rendered/* )")
            print_block_marker("END", name)
            continue
        analyze_rendered(name, cfg, rf)

if __name__ == '__main__':
    main()
PY
