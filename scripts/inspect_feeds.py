# scripts/inspect_feeds.py
import glob
import xml.etree.ElementTree as ET

for path in sorted(glob.glob("feeds/*.xml")):
    print("====", path, "====")
    try:
        tree = ET.parse(path)
        root = tree.getroot()
        # procura items (RSS) e entries (Atom)
        items = root.findall('.//item') or root.findall('.//{http://www.w3.org/2005/Atom}entry')
        print("Total items:", len(items))
        for i, it in enumerate(items[:5]):
            title = it.find('title')
            if title is None:
                # try Atom title
                title = it.find('{http://www.w3.org/2005/Atom}title')
            ttext = title.text.strip() if title is not None and title.text else "(no title)"
            link = it.find('link')
            if link is not None and link.text:
                ltext = link.text.strip()
            else:
                # try to find link href
                link_href = it.find(".//link[@href]")
                ltext = link_href.get('href') if link_href is not None else "(no link)"
            print(f"  #{i+1}: {ttext} -> {ltext}")
    except Exception as e:
        print("  ERROR reading:", e)
    print()
