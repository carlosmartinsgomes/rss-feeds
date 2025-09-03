#!/usr/bin/env python3
# scripts/merge_feeds.py
"""
Lê todos os feeds em feeds/*.xml (produzidos pelo generate_feeds.py),
faz merge, deduplica e escreve um RSS final em dist/merged-feed.xml.

Resiliência: se um ficheiro falhar a ler/parsear, regista erro e continua.
Deduplica por: guid -> link -> title+date (assinatura).
Ordena por pubDate desc e limita por config (default 100).
"""

import os
import glob
import sys
import traceback
from datetime import datetime, timezone
import feedparser
from feedgen.feed import FeedGenerator
from dateutil import parser as dateparser
import json

ROOT = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(ROOT, '..'))
FEEDS_DIR = os.path.join(REPO_ROOT, 'feeds')
OUT_DIR_DEFAULT = os.path.join(REPO_ROOT, 'dist')
OUT_FILE_DEFAULT = os.path.join(OUT_DIR_DEFAULT, 'merged-feed.xml')
CONFIG_FILE = os.path.join(REPO_ROOT, 'merge-config.json')

# defaults
MAX_ITEMS_DEFAULT = 100
SITE_TITLE_DEFAULT = "Merged Feed"
SITE_URL_DEFAULT = ""

def load_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                cfg = json.load(f)
            return cfg
        except Exception as e:
            print("Warning: failed to parse merge-config.json, using defaults:", e)
    return {}

def parse_date(s):
    if not s:
        return None
    try:
        dt = dateparser.parse(s)
        if dt.tzinfo is None:
            # assume UTC to be safe
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        try:
            # feedparser sometimes returns struct_time
            return datetime.fromtimestamp(dateparser.parse(s).timestamp(), tz=timezone.utc)
        except Exception:
            return None

def item_signature(entry):
    # create fallback signature
    title = (entry.get('title') or '').strip()
    link = (entry.get('link') or '').strip()
    date = entry.get('published') or entry.get('updated') or entry.get('pubDate') or ''
    return f"{title}|||{link}|||{date}"

def read_feed_file(path):
    try:
        d = feedparser.parse(path)
        if d.bozo:
            # feedparser sets bozo true on parse errors; but may still have entries
            print(f"Warning: bozo/parsing issue in {path}: {getattr(d, 'bozo_exception', 'unknown')}")
        return d
    except Exception as e:
        print(f"ERROR: failed to parse feed {path}: {e}")
        traceback.print_exc()
        return None

def collect_items_from_feed(feed, source_name):
    items = []
    if not feed:
        return items
    entries = feed.entries or []
    for e in entries:
        title = e.get('title') or ''
        link = e.get('link') or ''
        # try common GUID/id fields
        guid = e.get('id') or e.get('guid') or e.get('link') or None
        published = e.get('published') or e.get('updated') or e.get('pubDate') or ''
        dt = parse_date(published)
        # fallback: try entry.get('published_parsed') -> struct_time
        if dt is None:
            try:
                if hasattr(e, 'published_parsed') and e.published_parsed:
                    import time
                    dt = datetime.fromtimestamp(time.mktime(e.published_parsed), tz=timezone.utc)
            except Exception:
                dt = None
        description = e.get('summary') or e.get('description') or ''
        items.append({
            'title': title,
            'link': link,
            'guid': guid or item_signature(e),
            'published': dt,
            'published_raw': published,
            'description': description,
            'source': source_name,
            'raw': e
        })
    return items

def main():
    cfg = load_config()
    max_items = cfg.get('max_total_items') or cfg.get('max_items') or MAX_ITEMS_DEFAULT
    out_path = os.path.abspath(cfg.get('output') or OUT_FILE_DEFAULT)
    site_title = cfg.get('site_title') or SITE_TITLE_DEFAULT
    site_url = cfg.get('site_url') or SITE_URL_DEFAULT

    # ensure out dir exists
    out_dir = os.path.dirname(out_path)
    os.makedirs(out_dir, exist_ok=True)

    # collect candidate feed files from feeds/ dir
    feed_files = sorted(glob.glob(os.path.join(FEEDS_DIR, '*.xml')))
    if not feed_files:
        print(f"No feed files found in {FEEDS_DIR}. Nothing to merge.")
    else:
        print(f"Found {len(feed_files)} feed files in {FEEDS_DIR}")

    all_items = []
    seen = set()
    for fpath in feed_files:
        fname = os.path.basename(fpath)
        source_name = os.path.splitext(fname)[0]
        print(f"Parsing feed file: {fpath} (source: {source_name})")
        try:
            feed = read_feed_file(fpath)
            if feed is None:
                print(f" -> skipping {fpath} (parse returned None)")
                continue
            items = collect_items_from_feed(feed, source_name)
            print(f" -> extracted {len(items)} items from {fname}")
            for it in items:
                key = (it['guid'] or it['link'] or item_signature(it))
                if key in seen:
                    continue
                seen.add(key)
                all_items.append(it)
        except Exception as e:
            print(f"ERROR while processing {fpath}: {e}")
            traceback.print_exc()
            continue

    # sort by published date desc. Items without date -> treat as very old
    def sort_key(it):
        if it['published'] is None:
            # push to end; use epoch 1970
            return datetime(1970,1,1, tzinfo=timezone.utc)
        return it['published']

    all_items.sort(key=sort_key, reverse=True)

    # apply limit
    final_items = all_items[:int(max_items)]

    print(f"Total items after merge/dedupe: {len(all_items)} -> writing {len(final_items)} items to {out_path}")

    # build RSS with feedgen
    fg = FeedGenerator()
    fg.title(site_title)
    if site_url:
        fg.link(href=site_url, rel='alternate')
    fg.link(href='urn:merged-feed', rel='self')
    fg.description(cfg.get('site_description') or f'Merged feed ({len(final_items)} items)')

    for it in final_items:
        fe = fg.add_entry()
        fe.title(it['title'] or 'No title')
        if it['link']:
            fe.link(href=it['link'])
        fe.guid(it['guid'])
        try:
            if it['published']:
                fe.pubDate(it['published'].isoformat())
            else:
                # if no published, use raw if exists
                if it.get('published_raw'):
                    try:
                        fe.pubDate(it['published_raw'])
                    except Exception:
                        pass
        except Exception:
            pass
        fe.description(it['description'] or '')

    fg.rss_file(out_path)
    print("Merged feed written to:", out_path)

if __name__ == '__main__':
    main()
