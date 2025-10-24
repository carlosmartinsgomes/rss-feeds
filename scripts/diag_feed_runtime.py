# scripts/diag_feed_runtime.py
# Diagnóstico runtime: compara sites.json, scripts/rendered, and feeds/*.xml
# Executa: python3 scripts/diag_feed_runtime.py
import os, json, sys, glob
from pathlib import Path

def safe_load_json(p):
    try:
        return json.load(open(p,'r',encoding='utf-8'))
    except Exception as e:
        print("  Error loading json", p, ":", e)
        return None

def head_of_file(p, n=200):
    try:
        with open(p,'rb') as fh:
            data = fh.read(n)
            try:
                return data.decode('utf-8',errors='replace')
            except:
                return str(data[:n])
    except Exception as e:
        return f"<err reading {p}: {e}>"

print("DIAG: cwd =", os.getcwd())
sites_path = "scripts/sites.json"
if not os.path.exists(sites_path):
    print("DIAG: scripts/sites.json NOT FOUND")
    sys.exit(0)

cfg = safe_load_json(sites_path)
top_type = type(cfg).__name__
print("DIAG: sites.json top-level type:", top_type)

# normalize: list
sites = []
if isinstance(cfg, list):
    sites = cfg
elif isinstance(cfg, dict):
    if "sites" in cfg and isinstance(cfg["sites"], list):
        sites = cfg["sites"]
    else:
        # convert mapping name->obj
        for k,v in cfg.items():
            if isinstance(v, dict):
                obj = v.copy()
                if "name" not in obj:
                    obj["name"] = k
                sites.append(obj)
print("DIAG: interpreted sites count:", len(sites))

# list rendered files
rendered_dir = Path("scripts/rendered")
rendered_exists = rendered_dir.exists()
print("DIAG: scripts/rendered exists:", rendered_exists)
if rendered_exists:
    rfiles = list(rendered_dir.glob("**/*"))
    print("DIAG: number of files under scripts/rendered:", len(rfiles))
    sample = [str(p) for p in rfiles[:10]]
    print("DIAG: sample rendered files:", sample)

# show feeds xml files present
feeds_dir = Path("feeds")
feed_files = sorted(glob.glob("feeds/*.xml"))
print("DIAG: feeds/*.xml count =", len(feed_files))
for fx in feed_files[:10]:
    print("  -", fx, " size=", os.path.getsize(fx))

import feedparser

# For each site from sites.json, print details and corresponding feed xml if exists
for i,s in enumerate(sites):
    name = s.get("name") or s.get("site") or f"<site_{i}>"
    rf = s.get("render_file")
    ic = s.get("item_container")
    title_sel = s.get("title")
    link_sel = s.get("link")
    print("\n" + "="*70)
    print(f"SITE[{i}] name: {name!r}")
    print(f"  declared render_file: {rf!r}")
    if rf:
        # check multiple possible locations
        candidates = [rf, os.path.join("scripts", rf), os.path.join("scripts","rendered", rf)]
        found = None
        for c in candidates:
            if os.path.exists(c):
                found = c
                break
        print("  render_file found at:", found)
        if found:
            print("  head of rendered file (first 800 bytes):")
            print(head_of_file(found, n=800))
    else:
        print("  no render_file declared")
    print("  selectors: item_container=", ic, " title=", title_sel, " link=", link_sel)
    # find feed file with matching name
    candidate_feed = f"feeds/{name}.xml"
    if os.path.exists(candidate_feed):
        print("  feed xml exists:", candidate_feed, " size=", os.path.getsize(candidate_feed))
        # show head
        print("  head of feed xml (first 800 bytes):")
        print(head_of_file(candidate_feed, n=800))
        # parse feed and show entries summary
        try:
            d = feedparser.parse(candidate_feed)
            print("  feedparser_entries_count:", len(d.entries))
            for j, e in enumerate(d.entries[:7]):
                t = e.get("title","")
                link = e.get("link","")
                summary = e.get("summary","")
                print(f"    ENTRY[{j}] title={t!r} link={link!r} summary_tail={summary[:140]!r}")
        except Exception as e:
            print("  feedparser error:", e)
    else:
        print("  no feed xml found for site at feeds/{name}.xml")
print("\nDIAG finished.")
