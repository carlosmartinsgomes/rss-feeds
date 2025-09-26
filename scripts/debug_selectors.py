# scripts/debug_selectors.py
import sys, json
from bs4 import BeautifulSoup

SITES = {
  "dzone": {
    "item_container": "#ftl-tagging div.article-block, div.article-block, .article-block, .media-right",
    "title": "a[id^='title--articles-'], a.article-title, a.article-title.link-hover-underline-blue, h3 a, h2 a",
    "description": "div.article-desc, .article-desc, p.article-desc",
    "date": "div.article-date, .article-date, time"
  },
  "darkreading": {
    "item_container": "div.ContentPreview, div.ListPreview-ContentWrapper, div.ListContent-Content, div.ArticlePreview-Body, div.ContentCard-Body",
    "title": "a.ArticlePreview-Title, a.ContentCard-Title, a.ListPreview-Title, h4 a, h3 a",
    "description": "p.ArticlePreview-Summary, .ArticlePreview-Summary",
    "date": "span.ArticlePreview-Date, span.ContentCard-Date, .ListPreview-Date"
  },
  "datacenterknowledge": {
    "item_container": "div.ContentPreview, div.ListContent-Content, div.ListPreview-ContentWrapper, div.ContentCard-Body",
    "title": "a.ArticlePreview-Title, a.ContentCard-Title, a.ListPreview-Title, h4 a",
    "description": "p.ArticlePreview-Summary, .ArticlePreview-Summary",
    "date": "span.ArticlePreview-Date, span.ContentCard-Date, .ListPreview-Date"
  }
}

def test_file(path, site_key):
    html = open(path, 'r', encoding='utf-8').read()
    soup = BeautifulSoup(html, 'html.parser')
    cfg = SITES[site_key]
    container_sel = cfg['item_container']
    nodes = []
    for sel in [s.strip() for s in container_sel.split(',')]:
        found = soup.select(sel)
        if found:
            print(f"selector '{sel}' found {len(found)} nodes")
            nodes.extend(found[:200])
        else:
            print(f"selector '{sel}' found 0")
    # if nodes few, try global anchors
    if len(nodes) <= 1:
        anchors = soup.select(cfg['title'])
        print("global title anchors:", len(anchors))
        if anchors:
            for a in anchors[:200]:
                wrap = a.find_parent('div', class_='article-block') or a.find_parent('li') or a.find_parent('div', class_='ContentCard-Body')
                if wrap:
                    nodes.append(wrap)
    print("TOTAL nodes to inspect:", len(nodes))
    # show first 10 extracted fields
    for i, n in enumerate(nodes[:10]):
        t = n.select_one(cfg['title'])
        link = t.get('href') if t and t.has_attr('href') else ''
        desc = ''
        dsel = cfg.get('description')
        if dsel:
            d = n.select_one(dsel)
            if d:
                desc = ' '.join(d.stripped_strings)[:300]
        date = ''
        dsel2 = cfg.get('date')
        if dsel2:
            dd = n.select_one(dsel2)
            if dd:
                date = ' '.join(dd.stripped_strings)
        print(f"#{i} title: {(t.text.strip() if t else '---')} | link: {link} | date: {date} | desc_preview: {desc[:80]}")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 scripts/debug_selectors.py <html_file> <site_key>")
    else:
        test_file(sys.argv[1], sys.argv[2])
