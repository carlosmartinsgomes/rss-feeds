#!/usr/bin/env python3
# scripts/feeds_to_excel.py
# Requisitos: feedparser, pandas, beautifulsoup4, openpyxl, python-dateutil

import os
import glob
import json
import feedparser
import pandas as pd
import html
import re
from bs4 import BeautifulSoup
from dateutil import parser as dateparser

OUT_XLSX = "feeds_summary.xlsx"
FEEDS_DIR = "feeds"
SITES_JSON_PATHS = ["scripts/sites.json", "rss-feeds/scripts/sites.json", "sites.json"]

# regex para detectar datas típicas (ex: September 11, 2025 08:17 PM, Sep 11, 2025, 2025-09-11, 11 Sep 2025, etc.)
# Nota: não cobre absolutamente todos os formatos, mas captura muitos casos comuns.
_DATE_RE = re.compile(
    r'\b(?:' +
    r'(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|'
    r'Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)\s+\d{1,2},\s+\d{4}(?:\s+\d{1,2}:\d{2}(?::\d{2})?(?:\s*(?:AM|PM|am|pm))?)?)'
    r'|(?:\d{4}-\d{2}-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2})?)?)'
    r'|(?:\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4})'
    r')',
    flags=re.IGNORECASE
)

def strip_html_short(html_text, max_len=300):
    if not html_text:
        return ""
    t = html.unescape(html_text)
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

def find_date_in_text(text):
    if not text:
        return None
    m = _DATE_RE.search(text)
    if not m:
        return None
    candidate = m.group(0)
    try:
        # tenta parse inteligente; se conseguir devolve string legível (ISO)
        dt = dateparser.parse(candidate, fuzzy=True)
        if dt:
            # preferimos string legível curta
            return dt.isoformat(sep=' ')
    except Exception:
        pass
    # se parse falhar, devolve o trecho bruto
    return candidate

def find_date_from_xml_item(xml_soup, entry_title, entry_link):
    """
    Procura no XML (BeautifulSoup xml parser) um <item> que corresponda ao entry
    (por título ou link) e tenta extrair uma data no bloco <description> ou texto do item.
    """
    for item in xml_soup.find_all('item'):
        # tenta extrair title/link dentro do item xml
        t_el = item.find('title')
        d_el = item.find('description')
        link_el = item.find('link')
        t_text = t_el.get_text(" ", strip=True) if t_el else ''
        d_text = d_el.get_text(" ", strip=True) if d_el else ''
        l_text = link_el.get_text(" ", strip=True) if link_el else ''

        # normalizar e comparar
        def norm(x):
            return (x or '').strip().lower()
        if entry_link and entry_link.strip():
            if norm(entry_link) == norm(l_text):
                # match by link -> try description/text for date
                combined = ' '.join([t_text, d_text])
                found = find_date_in_text(combined)
                if found:
                    return found
        # fallback match by title (fuzzy)
        if entry_title and t_text and (norm(entry_title) == norm(t_text) or norm(entry_title) in norm(t_text) or norm(t_text) in norm(entry_title)):
            combined = ' '.join([t_text, d_text])
            found = find_date_in_text(combined)
            if found:
                return found
    # nada encontrado
    return None

def parse_feed_file_with_fallback(ff):
    """
    Faz parse com feedparser e devolve uma lista de rows (dictionaries) com campos:
    site, title, link (source), pubDate, description (short), item_container, topic
    """
    rows = []
    base = os.path.basename(ff)
    site_name = os.path.splitext(base)[0]
    parsed = feedparser.parse(ff)
    entries = parsed.entries if hasattr(parsed, "entries") else []
    # load raw xml once for fallback extraction
    raw_xml = ''
    try:
        raw_xml = open(ff, 'r', encoding='utf-8').read()
    except Exception:
        raw_xml = ''
    xml_soup = None
    if raw_xml:
        try:
            xml_soup = BeautifulSoup(raw_xml, 'xml')
        except Exception:
            xml_soup = None

    for e in entries:
        title = (e.get("title", "") or "").strip()
        # link: feedparser sometimes uses 'link' or 'links'
        link = (e.get("link", "") or "")
        # pubDate candidates
        pub = (e.get("published", "") or e.get("pubDate", "") or e.get("updated", "") or "")
        # description/summary
        desc = (e.get("summary", "") or e.get("description", "") or "")
        desc_short = strip_html_short(desc, max_len=300)

        # topic: try tags/categories
        topic = "N/A"
        if e.get("tags"):
            try:
                t = e.get("tags")
                if isinstance(t, list) and len(t) > 0 and isinstance(t[0], dict) and t[0].get('term'):
                    topic = t[0].get('term')
                elif isinstance(t, list) and isinstance(t[0], str):
                    topic = t[0]
            except Exception:
                topic = "N/A"

        # fallback: if no pub found, try to search in raw xml item text
        if not pub and xml_soup:
            fallback = find_date_from_xml_item(xml_soup, title, link)
            if fallback:
                pub = fallback

        # final cleanup: if still no pub, try to extract a date-like substring from description/title
        if not pub:
            combined = " ".join([title or "", desc or ""])
            maybe = find_date_in_text(combined)
            if maybe:
                pub = maybe

        rows.append({
            "site": site_name,
            "title": title,
            "link (source)": link,
            "pubDate": pub,
            "description (short)": desc_short,
            "item_container": "",  # preenchido depois com mapping se tiver
            "topic": topic or "N/A"
        })
    return rows

def main():
    site_item_map = load_sites_item_container()
    all_rows = []
    feed_files = sorted(glob.glob(os.path.join(FEEDS_DIR, "*.xml")))
    if not feed_files:
        print("No feed files found in", FEEDS_DIR)
    for ff in feed_files:
        try:
            rows = parse_feed_file_with_fallback(ff)
            # fill item_container from sites.json mapping if available
            base = os.path.basename(ff)
            site_name = os.path.splitext(base)[0]
            ic = site_item_map.get(site_name, "")
            for r in rows:
                r["item_container"] = ic
            all_rows.extend(rows)
        except Exception as exc:
            print("Error parsing feed", ff, ":", exc)
    if not all_rows:
        print("No items found across feeds.")
    # order columns and write xlsx
    cols = ["site", "title", "link (source)", "pubDate", "description (short)", "item_container", "topic"]
    df = pd.DataFrame(all_rows, columns=cols)
    df.to_excel(OUT_XLSX, index=False)
    print(f"Wrote {OUT_XLSX} ({len(df)} rows)")

if __name__ == "__main__":
    main()
