#!/usr/bin/env python3
# parse_merged.py
# Usage: python3 parse_merged.py path/to/dist/merged-feed.xml > merged_table.md

import sys
from xml.etree import ElementTree as ET
from urllib.parse import urlparse
import html
import csv

def short(text, n=200):
    if text is None:
        return ''
    t = text.strip()
    return (t[:n] + '...') if len(t) > n else t

def domain_from_url(u):
    if not u:
        return ''
    try:
        p = urlparse(u)
        return p.netloc or ''
    except Exception:
        return ''

def item_text(elem, tag):
    t = elem.find(tag)
    return t.text if t is not None and t.text is not None else ''

def main(path):
    xml = open(path, 'r', encoding='utf-8').read()
    # try to strip obvious problematic nulls
    xml = xml.replace('\r\n', '\n')
    root = ET.fromstring(xml)
    chan = root.find('channel')
    if chan is None:
        print("# Error: no <channel> found", file=sys.stderr)
        return

    items = chan.findall('item')
    # write markdown table to stdout
    headers = ['index','title','link','source','pubDate','description (short)','item_container']
    # markdown header
    print('| ' + ' | '.join(headers) + ' |')
    print('|' + '|'.join(['---']*len(headers)) + '|')

    for i, it in enumerate(items, start=1):
        title = html.unescape(item_text(it, 'title')).replace('\n',' ').strip()
        link = item_text(it, 'link').strip()
        guid = item_text(it, 'guid').strip()
        description = html.unescape(item_text(it, 'description')).replace('\n',' ').strip()
        pubDate = item_text(it, 'pubDate').strip()
        # prefer link for domain, fallback to guid
        src = domain_from_url(link or guid)
        # short description
        dshort = short(description, 200).replace('|','\\|')
        # escape pipes in title/link
        title_safe = title.replace('|','\\|')
        link_safe = link.replace('|','\\|')
        pubDate_safe = pubDate.replace('|','\\|')
        # item_container left blank (merged feed doesn't include it)
        print(f'| {i} | {title_safe} | {link_safe} | {src} | {pubDate_safe} | {dshort} |  |')

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 parse_merged.py path/to/dist/merged-feed.xml", file=sys.stderr)
        sys.exit(2)
    main(sys.argv[1])
