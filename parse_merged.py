#!/usr/bin/env python3
# parse_merged.py
import sys
from xml.etree import ElementTree as ET
from urllib.parse import urlparse
def text_of(n):
    return (n.text or '').strip() if n is not None else ''
def domain(u):
    try:
        return urlparse(u).netloc or ''
    except:
        return ''
if len(sys.argv)<2:
    print("Uso: python3 parse_merged.py /caminho/merged-feed.xml")
    sys.exit(1)
p=sys.argv[1]
max_items = 200  # ajusta se quiseres menos/mais
print("| # | domain | title | pubDate | link | description (preview) |")
print("|---|--------|-------|---------|------|----------------------|")
count=0
for ev,el in ET.iterparse(p,events=('end',)):
    if el.tag.endswith('item'):
        t = text_of(el.find('title'))
        d = text_of(el.find('pubDate'))
        l = text_of(el.find('link')) or text_of(el.find('guid'))
        desc = text_of(el.find('description'))
        desc_preview = (desc[:220] + '...') if len(desc)>220 else desc
        dom = domain(l)
        count += 1
        print(f"| {count} | {dom} | {t.replace('|',' ')} | {d} | {l} | {desc_preview.replace('|',' ')} |")
        el.clear()
        if count>=max_items:
            break
print(f"# items shown: {count}")
