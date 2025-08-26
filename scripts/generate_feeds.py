#!/usr/bin/env python3
# rss-feeds/scripts/generate_feeds.py
# Gera feeds RSS a partir de sites descritos em sites.json
# Usa render_file (HTML local) se presente; caso contrário faz HTTP GET.
# Aplica filtros por keywords (case-insensitive) no title+description.

import os
import json
import requests
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
from datetime import datetime

# ------- Config simples (mude se quiser) -------
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))         # .../rss-feeds/scripts
REPO_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))      # .../rss-feeds
FEEDS_DIR = os.path.join(REPO_ROOT, 'feeds')
SITES_JSON = os.path.join(REPO_ROOT, 'scripts', 'sites.json')   # onde espera o sites.json

# Headers "B" para tentar evitar 403 (approach B)
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

# garante pasta feeds
os.makedirs(FEEDS_DIR, exist_ok=True)

def load_sites(path):
    with open(path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    return data.get('sites', [])

def read_html_from_render_file(render_file):
    # render_file pode ser caminho relativo ao repo root
    if not render_file:
        return None
    if not os.path.isabs(render_file):
        render_path = os.path.join(REPO_ROOT, render_file)
    else:
        render_path = render_file
    if os.path.exists(render_path):
        with open(render_path, 'r', encoding='utf-8') as f:
            return f.read()
    return None

def fetch_html_via_requests(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.text
    except Exception as e:
        print(f"Request error for {url}: {e}")
        return None

def select_text(item, selector):
    """selector pode ter o formato 'sel' ou 'sel@attr'"""
    if not selector:
        return ''
    if '@' in selector:
        sel, attr = selector.split('@', 1)
        el = item.select_one(sel)
        if el and el.has_attr(attr):
            return el[attr].strip()
        elif el:
            # fallback: texto do elemento
            return el.get_text(" ", strip=True)
        else:
            return ''
    else:
        el = item.select_one(selector)
        return el.get_text(" ", strip=True) if el else ''

def extract_items_from_html(html, site_cfg):
    soup = BeautifulSoup(html, 'html.parser')
    container_sel = site_cfg.get('item_container')
    if not container_sel:
        return []
    nodes = soup.select(container_sel)
    items = []
    for node in nodes:
        title = select_text(node, site_cfg.get('title'))
        link = select_text(node, site_cfg.get('link'))
        date = select_text(node, site_cfg.get('date'))
        desc = select_text(node, site_cfg.get('description'))
        # se link for relativo, tente tornar absoluto usando site url:
        if link and link.startswith('/') and site_cfg.get('url'):
            link = site_cfg.get('url').rstrip('/') + link
        if title or link:
            items.append({
                'title': title,
                'link': link,
                'date': date,
                'description': desc
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
    # RSS minimalista
    rss = ET.Element('rss', version='2.0')
    channel = ET.SubElement(rss, 'channel')
    ET.SubElement(channel, 'title').text = site_name
    ET.SubElement(channel, 'link').text = site_url or ''
    ET.SubElement(channel, 'description').text = f'Feed gerado para {site_name}'

    for it in items:
        item = ET.SubElement(channel, 'item')
        ET.SubElement(item, 'title').text = it.get('title') or ''
        ET.SubElement(item, 'link').text = it.get('link') or ''
        # descrição em CDATA-like (simples)
        desc = ET.SubElement(item, 'description')
        desc.text = it.get('description') or ''
        if it.get('date'):
            # tenta formatar data legível; se não, coloca raw
            ET.SubElement(item, 'pubDate').text = it.get('date')

    tree = ET.ElementTree(rss)
    out_path = os.path.join(FEEDS_DIR, f"{site_name}.xml")
    tree.write(out_path, encoding='utf-8', xml_declaration=True)
    print(f"Wrote {out_path}")

def process_site(site_cfg):
    name = site_cfg.get('name') or 'site'
    url = site_cfg.get('url') or ''
    render_file = site_cfg.get('render_file')
    # 1) tenta ler ficheiro renderizado (se especificado)
    html = None
    if render_file:
        html = read_html_from_render_file(render_file)
        if html:
            print(f"Using rendered file: {render_file} for {name}")
    # 2) se não houver render file ou não existe, faz request
    if not html:
        html = fetch_html_via_requests(url)
        if not html:
            print(f"Failed to fetch {url}")
            return

    items = extract_items_from_html(html, site_cfg)
    keywords = site_cfg.get('filters', {}).get('keywords', [])
    filtered = [it for it in items if matches_keywords(it, keywords)]

    # se preferir guardar sem filtro quando filtered vazio, substitua por items
    write_rss(name, url, filtered)

def main():
    sites = load_sites(SITES_JSON)
    for s in sites:
        try:
            process_site(s)
        except Exception as e:
            print(f"Error processing site {s.get('name')}: {e}")

if __name__ == '__main__':
    main()

