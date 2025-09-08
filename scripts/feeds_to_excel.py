#!/usr/bin/env python3
# scripts/feeds_to_excel.py
# Requisitos: feedparser, pandas, beautifulsoup4, openpyxl

import os
import glob
import json
import feedparser
import pandas as pd
import html
import re
from bs4 import BeautifulSoup

OUT_XLSX = "feeds_summary.xlsx"
FEEDS_DIR = "feeds"
SITES_JSON_PATHS = ["scripts/sites.json", "rss-feeds/scripts/sites.json", "sites.json"]

def strip_html_short(html_text, max_len=300):
    if not html_text:
        return ""
    # decode entities
    t = html.unescape(html_text)
    # remove tags using BeautifulSoup if available
    try:
        s = BeautifulSoup(t, "html.parser").get_text(separator=" ", strip=True)
    except Exception:
        s = re.sub(r"<[^>]+>", "", t)
    s = re.sub(r"\s+", " ", s).strip()
    if len(s) > max_len:
        return s[:max_len].rstrip() + "…"
    return s

def load_sites_item_container():
    for p in SITES_JSON_PATHS:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as fh:
                    raw = fh.read()
                obj = json.loads(raw)
                sites = obj.get("sites", obj if isinstance(obj, list) else [])
                mapping = {}
                for s in sites:
                    name = s.get("name") if isinstance(s, dict) else None
                    if name:
                        mapping[name] = s.get("item_container", "") or ""
                return mapping
            except Exception:
                continue
    return {}

def main():
    site_item_map = load_sites_item_container()
    rows = []
    feed_files = sorted(glob.glob(os.path.join(FEEDS_DIR, "*.xml")))
    if not feed_files:
        print("No feed files found in", FEEDS_DIR)
    for ff in feed_files:
        base = os.path.basename(ff)
        site_name = os.path.splitext(base)[0]
        # try parse feed file with feedparser
        parsed = feedparser.parse(ff)
        entries = parsed.entries if hasattr(parsed, "entries") else []
        if not entries:
            # still try to parse items via simple XML fallback (not implemented — skip)
            continue
        for e in entries:
            title = e.get("title", "") or ""
            link = e.get("link", "") or ""
            # pubDate - try common keys
            pub = e.get("published", "") or e.get("pubDate", "") or e.get("updated", "")
            # description/summary
            desc = e.get("summary", "") or e.get("description", "") or ""
            desc_short = strip_html_short(desc, max_len=300)
            item_container = site_item_map.get(site_name, "")
            rows.append({
                "site": site_name,
                "title": title,
                "link (source)": link,
                "pubDate": pub,
                "description (short)": desc_short,
                "item_container": item_container
            })
    if not rows:
        print("No items found across feeds.")
    df = pd.DataFrame(rows, columns=["site", "title", "link (source)", "pubDate", "description (short)", "item_container"])
    df.to_excel(OUT_XLSX, index=False)
    print(f"Wrote {OUT_XLSX} ({len(df)} rows)")

if __name__ == "__main__":
    main()
