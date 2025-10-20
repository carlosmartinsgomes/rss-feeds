#!/usr/bin/env python3
# scripts/diag_verbose.py
# Usar: python3 scripts/diag_verbose.py all
#       python3 scripts/diag_verbose.py prnewswire adexchanger-feed

import os, sys, json, re, glob
import feedparser
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse

ROOT = os.path.dirname(__file__)
FEEDS_DIR = os.path.abspath(os.path.join(ROOT, '..', 'feeds'))
SITES_JSON_PATHS = [
    os.path.join('scripts', 'sites.json'),
    os.path.join('rss-feeds', 'scripts', 'sites.json'),
    'sites.json'
]

def load_sites_map():
    for p in SITES_JSON_PATHS:
        if os.path.exists(p):
            try:
                with open(p, 'r', encoding='utf-8') as fh:
                    j = json.load(fh)
                sites = j.get('sites', j if isinstance(j, list) else [])
                m = {s.get('name'): s for s in sites if isinstance(s, dict) and s.get('name')}
                return m
            except Exception:
                continue
    return {}

SITES = load_sites_map()

def print_block(s): 
    print(s, flush=True)

def preview_text(t, head=400, tail=300):
    if not t:
        return "<EMPTY>"
    t = str(t)
    if len(t) <= head+tail+40:
        return t
    return (t[:head].rstrip() + "\n\n...[TRIMMED]...\n\n" + t[-tail:])

# same matching logic simplified from your code
def compute_match_from_cfg(entry_row, site_cfg):
    if not site_cfg:
        return None
    kw_list = site_cfg.get('filters', {}).get('keywords', []) or []
    exclude_list = site_cfg.get('filters', {}).get('exclude', []) or []
    if not kw_list and not exclude_list:
        return None
    text_map = {
        'title': (entry_row.get('title','') or '').lower(),
        'description': (entry_row.get('description','') or entry_row.get('description (short)','') or '').lower(),
        'full_text': (entry_row.get('full_text','') or '').lower(),
        'link': (entry_row.get('link (source)','') or entry_row.get('link','') or '').lower(),
        'topic': (entry_row.get('topic','') or '').lower()
    }
    if kw_list:
        out = []
        for k in kw_list:
            if not k: continue
            kl = str(k).lower()
            for field in ('title','description','full_text','link','topic'):
                if kl in text_map.get(field,''):
                    out.append(f"{kl}@{field}")
        return ";".join(out) if out else None
    for ex in exclude_list:
        if not ex: continue
        el = str(ex).lower()
        for field in ('title','description','full_text','link','topic'):
            if el in text_map.get(field,''):
                return f"exclude:{el}@{field}"
    return None

def find_rendered_candidates(cfg):
    # check cfg.render_file then scripts/rendered/* by site name and hostname
    candidates = []
    rf = cfg.get('render_file')
    if rf:
        rf_path = rf
        if not os.path.isabs(rf_path) and not rf_path.startswith('scripts'):
            rf_path = os.path.join('scripts', rf_path)
        if os.path.exists(rf_path):
            candidates.append(os.path.abspath(rf_path))
    # search by hostname fragments in scripts/rendered
    try:
        rendered_dir = os.path.join('scripts','rendered')
        if os.path.isdir(rendered_dir):
            for f in os.listdir(rendered_dir):
                if f.lower().endswith('.html') and (cfg.get('name','').lower() in f.lower() or (cfg.get('url','').split('/')[2] if cfg.get('url') and '/' in cfg.get('url') else '') in f.lower()):
                    candidates.append(os.path.abspath(os.path.join(rendered_dir, f)))
    except Exception:
        pass
    return candidates

def diag_site(site_name):
    cfg = SITES.get(site_name, {})
    print_block(f"===DIAG_START_SITE={site_name}===")
    print_block("="*79)
    # feed file
    feed_path = os.path.abspath(os.path.join(FEEDS_DIR, f"{site_name}.xml"))
    if os.path.exists(feed_path):
        sz = os.path.getsize(feed_path)
        print_block(f"feed_path: {feed_path} (size: {sz} bytes)")
        # preview head/tail
        try:
            raw = open(feed_path, 'r', encoding='utf-8').read()
            print_block("feed_preview_head:")
            print_block(preview_text(raw[:20000], head=800, tail=0))
        except Exception as e:
            print_block(f"feed read error: {e}")
    else:
        print_block(f"feed_path: MISSING -> {feed_path}")

    # rendered candidates
    rcands = find_rendered_candidates(cfg)
    if rcands:
        for r in rcands:
            try:
                s = os.path.getsize(r)
                print_block(f"rendered_path: {r} (size {s})")
                txt = open(r, 'r', encoding='utf-8', errors='ignore').read()
                print_block("rendered_head:")
                print_block(preview_text(txt, head=1200, tail=400))
                # anti-bot signs
                antibot = False
                for token in ['Just a moment','verify you are human','cdn-cgi/challenge-platform','Ray ID','Cloudflare']:
                    if token.lower() in txt.lower():
                        print_block(f"ANTIBOT_SIGN: found token '{token}' in rendered file")
                        antibot = True
                if not antibot:
                    print_block("ANTIBOT_SIGN: None detected in rendered file")
            except Exception as e:
                print_block(f"rendered file read error: {e}")
    else:
        print_block("rendered_path: MISSING (checked cfg.render_file and scripts/rendered/* )")

    # if feed exists parse it
    if os.path.exists(feed_path):
        try:
            parsed = feedparser.parse(feed_path)
            entries = getattr(parsed, 'entries', []) or []
            print_block(f"feedparser entries count: {len(entries)}")
            for i,e in enumerate(entries[:12]):
                title = (e.get('title') or '')[:240]
                link = e.get('link') or e.get('id') or ''
                summary = (e.get('summary') or e.get('description') or '')[:800]
                tags = e.get('tags') or []
                tag_terms = []
                for tg in tags:
                    if isinstance(tg, dict):
                        tag_terms.append(tg.get('term') or tg.get('label') or '')
                    else:
                        tag_terms.append(str(tg))
                print_block(f" ENTRY[{i}] title='{title}' link='{link}' tags={tag_terms}")
                # show if description still contains MatchedReason pattern
                if summary and re.search(r'\[MatchedReason:', summary, re.I):
                    snippet = re.search(r'\[MatchedReason:[^\]]*', summary, re.I)
                    print_block(f"   -> summary contains '[MatchedReason:' snippet='{snippet.group(0) if snippet else '...'}'")
                # show full summary preview
                print_block("   summary_preview:")
                print_block(preview_text(summary, head=600, tail=200))
            if len(entries) > 12:
                print_block(f"  ... (only showed first 12 entries of {len(entries)})")
        except Exception as e:
            print_block(f"feedparser parse error: {e}")

    # Try a direct fetch of the configured URL (useful if no rendered file)
    url = cfg.get('url')
    if url:
        try:
            h = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
            resp = requests.get(url, headers=h, timeout=20)
            print_block(f"HTTP fetch {url} -> status {resp.status_code} content-type: {resp.headers.get('Content-Type','')}, bytes: {len(resp.content)}")
            body = resp.text or ''
            # small checks
            if any(x in body for x in ['Just a moment', 'verify you are human', 'cdn-cgi/challenge-platform', 'Ray ID', 'Cloudflare']):
                print_block("ANTIBOT_DETECTED_IN_FETCH: True (body contains Cloudflare/anti-bot markers)")
                snippet = re.search(r'(Just a moment|verify you are human|cdn-cgi/challenge-platform|Ray ID|Cloudflare)[\s\S]{0,200}', body, re.I)
                if snippet:
                    print_block("ANTIBOT_SNIPPET: " + snippet.group(0)[:500])
            else:
                print_block("ANTIBOT_DETECTED_IN_FETCH: False")
            # XML or HTML check
            if body.strip().startswith('<?xml') or '<rss' in body.lower() or '<feed' in body.lower():
                try:
                    p = feedparser.parse(body)
                    print_block("live fetch parsed as feed -> entries: " + str(len(getattr(p,'entries',[]) or [])))
                except Exception:
                    pass
            else:
                # show HTML preview
                print_block("live fetch HTML preview head:")
                print_block(preview_text(body, head=1200, tail=400))
        except Exception as e:
            print_block(f"HTTP fetch error for {url}: {e}")
    else:
        print_block("No url in site config to fetch.")

    # Try to compute a match for each feed entry using site cfg filters (if feed exists)
    if os.path.exists(feed_path) and site_name in SITES:
        try:
            parsed = feedparser.parse(feed_path)
            entries = getattr(parsed, 'entries', []) or []
            print_block("Running local match-recalc vs sites.json filters for first 12 entries:")
            for i,e in enumerate(entries[:12]):
                row = {
                    'title': e.get('title','') or '',
                    'description': e.get('summary','') or e.get('description','') or '',
                    'description (short)': (e.get('summary','') or '')[:400],
                    'full_text': ((e.get('summary','') or '') + ' ' + (e.get('title','') or ''))[:2000],
                    'link (source)': e.get('link','') or e.get('id','') or '',
                    'topic': ''
                }
                # reconstruct tags as topic candidates
                tags = e.get('tags') or []
                tterms = []
                for tg in tags:
                    if isinstance(tg, dict):
                        tterms.append(tg.get('term') or tg.get('label') or '')
                    else:
                        tterms.append(str(tg))
                if tterms:
                    row['topic'] = tterms[0]
                calc = compute_match_from_cfg(row, SITES.get(site_name))
                print_block(f"  ENTRY[{i}] calc_match={calc} tags={tterms}")
        except Exception as e:
            print_block(f"match-recalc error: {e}")

    print_block("="*79)
    print_block(f"===DIAG_END_SITE={site_name}===")
    print_block("")


def main():
    args = sys.argv[1:] or ['all']
    to_check = []
    if 'all' in args:
        to_check = sorted(list(SITES.keys()))
    else:
        to_check = args
    for s in to_check:
        if s not in SITES:
            print_block(f"Skipping unknown site {s} (not in sites.json)")
            continue
        diag_site(s)

if __name__ == "__main__":
    main()
