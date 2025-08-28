#!/usr/bin/env python3
# scripts/generate_feeds.py
# Versão melhorada: logging, seletores alternativos e fallback para evitar feeds vazios.

import os
import json
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime
import sys

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
SITES_JSON = os.path.join(SCRIPT_DIR, 'sites.json')
FEEDS_DIR = os.path.join(REPO_ROOT, 'feeds')

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

os.makedirs(FEEDS_DIR, exist_ok=True)

def load_sites(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data.get('sites', [])
    except Exception as e:
        print(f"ERROR: cannot load sites.json at {path}: {e}")
        return []

def read_html_from_render_file(render_file):
    if not render_file:
        return None
    candidate_paths = [
        os.path.join(REPO_ROOT, render_file),
        os.path.join(SCRIPT_DIR, render_file),
        render_file
    ]
    for rp in candidate_paths:
        if os.path.exists(rp):
            try:
                with open(rp, 'r', encoding='utf-8') as f:
                    return f.read()
            except Exception as e:
                print(f"Warning: cannot read rendered file {rp}: {e}")
                continue
    return None

def fetch_html_via_requests(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Request error for {url}: {e}")
        return None

def select_text(item, selector):
    if not selector:
        return ''
    if '@' in selector:
        sel, attr = selector.split('@', 1)
        el = item.select_one(sel)
        if el and el.has_attr(attr):
            return el[attr].strip()
        elif el:
            return el.get_text(" ", strip=True)
        else:
            return ''
    else:
        el = item.select_one(selector)
        return el.get_text(" ", strip=True) if el else ''

def extract_items_from_html(html, site_cfg):
    soup = BeautifulSoup(html, 'html.parser')
    container_sel = site_cfg.get('item_container')
    items = []

    if container_sel:
        nodes = soup.select(container_sel)
    else:
        nodes = []

    # try fallback selectors if none found
    if not nodes or len(nodes) == 0:
        fallback_containers = ['article', 'li', 'div.c-compact-river__entry', '.c-compact-river__entry', 'div.story', '.story']
        for fsel in fallback_containers:
            nodes = soup.select(fsel)
            if nodes:
                print(f"Fallback: found {len(nodes)} nodes with selector '{fsel}'")
                break

    for node in nodes:
        title = select_text(node, site_cfg.get('title'))
        link = select_text(node, site_cfg.get('link'))
        date = select_text(node, site_cfg.get('date'))
        desc = select_text(node, site_cfg.get('description'))
        # normalize relative links
        if link and link.startswith('/') and site_cfg.get('url'):
            link = site_cfg.get('url').rstrip('/') + link
        if title or link or desc:
            items.append({
                'title': title.strip() if title else '',
                'link': link.strip() if link else '',
                'date': date.strip() if date else '',
                'description': desc.strip() if desc else ''
            })
    return items

def matches_keywords(item, keywords):
    if not keywords:
        return True
    text = " ".join([ (item.get('title') or ''), (item.get('description') or '') ]).lower()
    for kw in keywords:
        if kw.lower() in text:
            return True
    return False

def write_rss(site_name, site_url, items):
    rss = ET.Element('rss', version='2.0')
    channel = ET.SubElement(rss, 'channel')
    ET.SubElement(channel, 'title').text = site_name
    ET.SubElement(channel, 'link').text = site_url or ''
    ET.SubElement(channel, 'description').text = f'Feed gerado para {site_name}'

    for it in items:
        item = ET.SubElement(channel, 'item')
        ET.SubElement(item, 'title').text = it.get('title') or ''
        ET.SubElement(item, 'link').text = it.get('link') or ''
        desc = ET.SubElement(item, 'description')
        desc.text = it.get('description') or ''
        if it.get('date'):
            ET.SubElement(item, 'pubDate').text = it.get('date')

    tree = ET.ElementTree(rss)
    out_path = os.path.join(FEEDS_DIR, f"{site_name}.xml")
    tree.write(out_path, encoding='utf-8', xml_declaration=True)
    print(f"Wrote {out_path}")

def process_site(site_cfg):
    name = site_cfg.get('name') or 'site'
    url = site_cfg.get('url') or ''
    render_file = site_cfg.get('render_file')
    print(f"\n--- Processing {name} ({url}) ---")

    html = None
    if render_file:
        html = read_html_from_render_file(render_file)
        if html:
            print(f"Using rendered file: {render_file} for {name}")
        else:
            print(f"No rendered file found at {render_file} for {name}")

    if not html:
        print(f"Fetching {url} via requests...")
        html = fetch_html_via_requests(url)
        if not html:
            print(f"Failed to fetch {url}")
            # write empty feed (or skip) — keep minimal valid RSS
            write_rss(name, url, [])
            return

    items = extract_items_from_html(html, site_cfg)
    print(f"Found {len(items)} items for {name} (raw)")

    keywords = site_cfg.get('filters', {}).get('keywords', [])
    if keywords:
        print(f"Applying {len(keywords)} keyword filters for {name}: {keywords}")
    filtered = [it for it in items if matches_keywords(it, keywords)]
    print(f"{len(filtered)} items matched filters for {name}")

    # fallback: if filter removed everything, use all items (prevent empty feeds)
    if not filtered and items:
        print(f"No items matched filters for {name} — falling back to all {len(items)} items")
        filtered = items

    write_rss(name, url, filtered)

def main():
    sites = load_sites(SITES_JSON)
    print(f"Loaded {len(sites)} site configurations from {SITES_JSON}")
    for s in sites:
        try:
            process_site(s)
        except Exception as e:
            print(f"Error processing site {s.get('name')}: {e}")

if __name__ == '__main__':
    main()
