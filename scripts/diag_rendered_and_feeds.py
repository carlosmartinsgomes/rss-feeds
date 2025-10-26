# scripts/diag_rendered_and_feeds.py
# Fornece diagnóstico detalhado: para cada site em sites.json
# - se tem render_file e se existe em scripts/rendered
# - se existe feeds/{site}.xml e entries count via feedparser
# - imprime primeiro(s) títulos e os UIDs (normalizados) para diagnóstico

import os, json, sys
from pathlib import Path

try:
    import feedparser
except Exception:
    print("Warning: feedparser not installed (install in workflow). Continuing with limited checks.")

def normalize_title(t):
    import re
    if not t: return ''
    t = str(t).strip().lower()
    t = re.sub(r'[^\w\s]', ' ', t, flags=re.UNICODE)
    t = re.sub(r'\s+', ' ', t).strip()
    return t

cwd = os.getcwd()
print("DIAG: cwd =", cwd)

cfg_path = "scripts/sites.json"
if not os.path.exists(cfg_path):
    print("DIAG: scripts/sites.json NOT FOUND -> cannot iterate sites")
    sys.exit(0)

try:
    cfg_raw = json.load(open(cfg_path, encoding="utf-8"))
except Exception as e:
    print("DIAG: error loading sites.json:", e)
    sys.exit(0)

sites = []
# handle sites.json top-level either list or dict with key "sites"
if isinstance(cfg_raw, dict) and "sites" in cfg_raw and isinstance(cfg_raw["sites"], list):
    sites = cfg_raw["sites"]
elif isinstance(cfg_raw, list):
    sites = cfg_raw
else:
    # if it's a dict with site-name keys, convert to list
    if isinstance(cfg_raw, dict):
        for k,v in cfg_raw.items():
            if isinstance(v, dict):
                v["name"] = v.get("name", k)
                sites.append(v)

print("DIAG: interpreted sites count:", len(sites))

rendered_dir = Path("scripts/rendered")
print("DIAG: scripts/rendered exists:", rendered_dir.exists())
if rendered_dir.exists():
    files = list(rendered_dir.glob("*"))
    print("DIAG: number of files under scripts/rendered:", len(files))
    print("DIAG: sample rendered files:", [str(p.name) for p in files[:20]])

feeds_dir = Path("feeds")
feeds_files = list(feeds_dir.glob("*.xml")) if feeds_dir.exists() else []
print("DIAG: feeds/*.xml count =", len(feeds_files))
for f in feeds_files[:40]:
    print("-", str(f.name), "size=", f.stat().st_size)

# iterate sites and print status
for s in sites:
    name = s.get("name") or s.get("id") or "<noname>"
    render_file = s.get("render_file")
    selectors = s.get("selectors") or s.get("useful_keys") or None
    print("\n--- SITE:", name)
    print(" render_file (declared):", repr(render_file))
    rf_found = None
    if render_file:
        # check both as given and under scripts/
        if os.path.exists(render_file):
            rf_found = render_file
        elif os.path.exists(os.path.join("scripts", render_file)):
            rf_found = os.path.join("scripts", render_file)
    print(" render_file found at:", rf_found, "size=", os.path.getsize(rf_found) if rf_found else 0)
    # alt_render: any file in scripts/rendered that startswith site name
    alt = None
    if rendered_dir.exists():
        for p in rendered_dir.glob(f"{name}*"):
            alt = str(p)
            break
    print(" alt_render (scripts/rendered/...):", alt)
    print(" selectors preview:", selectors if selectors else "<none>")

    # check feed file
    feed_path = feeds_dir / f"{name}.xml"
    if not feed_path.exists():
        print(" no feed xml found for site at feeds/{name}.xml".format(name=name))
        continue
    print(" feed xml exists:", str(feed_path), "size=", feed_path.stat().st_size)
    # try parse feed
    try:
        import feedparser
        parsed = feedparser.parse(str(feed_path))
        cnt = len(parsed.entries or [])
        print(" feedparser_entries_count:", cnt)
        if cnt:
            for i, e in enumerate(parsed.entries[:5]):
                title = e.get("title") or ""
                link = e.get("link") or ""
                summary = (e.get("summary") or "")[:200]
                uid = normalize_title(title)
                print(f" ENTRY[{i}] title={title!r} link={link!r} uid_norm={uid!r} summary_tail={summary!r}")
    except Exception as e:
        print(" feedparser parse error:", e)

print("\nDIAG: done.")
