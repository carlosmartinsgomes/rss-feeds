#!/usr/bin/env python3
"""
Diagnostic script for RSS-feeds repo run state.

Place as: scripts/diag_all_run_state.py

Purpose:
 - Run as a single workflow step to produce a thorough diagnostic.
 - Outputs verbose log to stdout and to diag-output.txt in repo root.
 - Uses only Python stdlib. Optionally downloads artifact zip contents
   for deeper inspection if DIAG_DOWNLOAD_ARTIFACTS=1 is set in env.

How to run in a single workflow step:
  - name: Full repo diagnostic (single step)
    run: python3 scripts/diag_all_run_state.py
    env:
      GITHUB_TOKEN: ${{ secrets.GITHUB_TOKEN }}
      GITHUB_REPOSITORY: ${{ github.repository }}
      DIAG_DOWNLOAD_ARTIFACTS: '0'   # set to '1' to download zip contents for matched artifacts

Notes:
 - Script avoids printing the GITHUB_TOKEN itself; it only checks presence.
 - If DIAG_DOWNLOAD_ARTIFACTS=1 the script may download (and extract) artifact zips into ./diag_artifacts/
"""

from __future__ import print_function
import os, sys, json, time, hashlib, zipfile, io
from urllib import request, parse, error
import xml.etree.ElementTree as ET
import shutil
from datetime import datetime

OUTPATH = "diag-output.txt"

def writeln(f, s=""):
    ts = datetime.utcnow().isoformat() + "Z"
    line = f"[{ts}] {s}"
    print(line)
    if f:
        f.write(line + "\n")

def safe_read_bytes(path, n=512):
    try:
        with open(path, "rb") as fh:
            return fh.read(n)
    except Exception as e:
        return b""

def sha1_of_file(path):
    h = hashlib.sha1()
    try:
        with open(path, "rb") as fh:
            while True:
                b = fh.read(8192)
                if not b:
                    break
                h.update(b)
        return h.hexdigest()
    except Exception:
        return None

def list_files_summary(root, max_items=200):
    out = []
    for dirpath, dirs, files in os.walk(root):
        for fn in files:
            path = os.path.join(dirpath, fn)
            try:
                size = os.path.getsize(path)
            except Exception:
                size = -1
            out.append((path, size))
            if len(out) >= max_items:
                return out
    return out

# ---------------- GitHub API helpers (stdlib)
GITHUB_API_BASE = "https://api.github.com"

def gh_api_get(repo, token, path, params=None):
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    url = GITHUB_API_BASE.rstrip("/") + path
    if params:
        url += "?" + parse.urlencode(params)
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=30) as resp:
            data = resp.read()
            charset = resp.headers.get_content_charset() or "utf-8"
            return resp.getcode(), json.loads(data.decode(charset)), resp.headers
    except error.HTTPError as he:
        try:
            body = he.read().decode('utf-8', errors='ignore')
        except Exception:
            body = ""
        return he.code, {"message": f"HTTPError: {he.reason}", "body": body}, he.headers
    except Exception as e:
        return None, {"error": str(e)}, {}

# ---------------- main
def main():
    f = open(OUTPATH, "w", encoding="utf-8")
    try:
        writeln(f, "DIAGNOSTIC START")
        # environment overview
        GITHUB_REPOSITORY = os.environ.get("GITHUB_REPOSITORY","(unset)")
        GITHUB_RUN_ID = os.environ.get("GITHUB_RUN_ID","(unset)")
        GITHUB_WORKSPACE = os.environ.get("GITHUB_WORKSPACE", os.getcwd())
        GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # do not print value
        DIAG_DOWNLOAD = os.environ.get("DIAG_DOWNLOAD_ARTIFACTS","0") in ("1","true","True","yes","on","Y")
        writeln(f, f"Env: GITHUB_REPOSITORY={GITHUB_REPOSITORY}, GITHUB_RUN_ID={GITHUB_RUN_ID}, GITHUB_WORKSPACE={GITHUB_WORKSPACE}")
        writeln(f, f"GITHUB_TOKEN present: {'YES' if GITHUB_TOKEN else 'NO'}")
        writeln(f, f"DIAG_DOWNLOAD_ARTIFACTS = {DIAG_DOWNLOAD}")

        writeln(f, "---- repo files top-level summary ----")
        # list top-level few files and key dirs
        try:
            for entry in sorted(os.listdir("."))[:200]:
                p = entry
                writeln(f, "  " + p)
        except Exception as e:
            writeln(f, "  (ls root failed: " + str(e) + ")")

        # show scripts and scripts/rendered
        writeln(f, "---- scripts/ and scripts/rendered/ ----")
        if os.path.isdir("scripts"):
            for entry in sorted(os.listdir("scripts"))[:500]:
                p = os.path.join("scripts", entry)
                try:
                    s = os.path.getsize(p) if os.path.isfile(p) else None
                except Exception:
                    s = None
                writeln(f, f"  scripts/{entry}  size={s}")
        else:
            writeln(f, "  scripts/ directory NOT FOUND")

        # scripts/rendered detail
        rendered_dir = os.path.join("scripts","rendered")
        if os.path.isdir(rendered_dir):
            files = sorted(os.listdir(rendered_dir))
            writeln(f, f"scripts/rendered exists, count={len(files)}")
            for fn in files:
                p = os.path.join(rendered_dir, fn)
                size = os.path.getsize(p)
                h = sha1_of_file(p) or "(sha1-error)"
                head = safe_read_bytes(p, 320).decode('utf-8', errors='replace')
                writeln(f, f"  rendered/{fn} size={size} sha1={h}")
                writeln(f, f"    head_preview: {repr(head[:200])}")
        else:
            writeln(f, "scripts/rendered MISSING")

        # feeds xml summary
        writeln(f, "---- feeds/*.xml summary ----")
        feeds_dir = "feeds"
        if os.path.isdir(feeds_dir):
            feed_files = sorted([os.path.join(feeds_dir, x) for x in os.listdir(feeds_dir) if x.endswith(".xml")])
            writeln(f, f"feeds xml count = {len(feed_files)}")
            for fx in feed_files:
                try:
                    size = os.path.getsize(fx)
                except Exception:
                    size = -1
                head = safe_read_bytes(fx, 800).decode('utf-8', errors='replace')
                # quick parse to count <item>
                items = 0
                try:
                    tree = ET.parse(fx)
                    root = tree.getroot()
                    items = len(root.findall(".//item"))
                except Exception:
                    # fallback naive count
                    items = head.count("<item")
                writeln(f, f"  {fx} size={size} items_count={items}")
                writeln(f, f"    head_preview: {repr(head[:240])}")
        else:
            writeln(f, "feeds/ directory MISSING")

        # excel presence
        writeln(f, "---- Excel / xlsx checks ----")
        for candidate in ("feeds_summary.xlsx","feeds-summary.xlsx"):
            if os.path.exists(candidate):
                s = os.path.getsize(candidate)
                writeln(f, f"  Found excel: {candidate} size={s}")
            else:
                writeln(f, f"  Not found: {candidate}")

        # .github/data/sent_ids.json
        writeln(f, "---- .github/data/sent_ids.json ----")
        sent_path = os.path.join(".github","data","sent_ids.json")
        if os.path.exists(sent_path):
            try:
                with open(sent_path,"r",encoding="utf-8") as fh:
                    j = json.load(fh)
                writeln(f, f"  sent_ids.json exists, count={len(j)}")
                sample = j[:40]
                writeln(f, f"  sample (first 40): {sample}")
            except Exception as e:
                writeln(f, f"  error reading sent_ids.json: {e}")
        else:
            writeln(f, "  sent_ids.json NOT FOUND")

        # quick stats on excel mapping to email table: inspect feeds_summary.xlsx if present (light)
        writeln(f, "---- Quick excel column check (if openpyxl available) ----")
        try:
            import openpyxl
            xname = None
            for cx in ("feeds_summary.xlsx","feeds-summary.xlsx"):
                if os.path.exists(cx):
                    xname = cx; break
            if xname:
                wb = openpyxl.load_workbook(xname, read_only=True)
                ws = wb.active
                headers = [str(c.value) for c in next(ws.iter_rows(max_row=1, values_only=True))]
                writeln(f, f"  excel columns (first row): {headers}")
            else:
                writeln(f, "  no excel found to inspect with openpyxl")
        except Exception as e:
            writeln(f, f"  openpyxl not available or excel inspect failed: {e}")

        # ---------------- GitHub API: list artifacts & recent runs
        writeln(f, "---- GitHub artifacts & workflow runs (metadata via API) ----")
        if not GITHUB_REPOSITORY:
            writeln(f, "  GITHUB_REPOSITORY unset; skipping GitHub API checks")
        else:
            token = GITHUB_TOKEN
            # list repo artifacts
            code, data, headers = gh_api_get(GITHUB_REPOSITORY, token, f"/repos/{GITHUB_REPOSITORY}/actions/artifacts", {"per_page":100})
            if code in (200,201) and isinstance(data, dict):
                arts = data.get("artifacts", [])
                writeln(f, f"  artifacts_total_count = {len(arts)} (showing up to 80)")
                for a in arts[:80]:
                    writeln(f, f"    artifact: name={a.get('name')} id={a.get('id')} size={a.get('size_in_bytes')} created_at={a.get('created_at')} expired={a.get('expired')}")
            else:
                writeln(f, f"  artifacts API call failed: status={code} body={repr(data)[:800]}")

            # list recent workflow runs (for correlation)
            code, runs_data, headers = gh_api_get(GITHUB_REPOSITORY, token, f"/repos/{GITHUB_REPOSITORY}/actions/runs", {"per_page":50})
            if code in (200,201) and isinstance(runs_data, dict):
                runs = runs_data.get("workflow_runs", [])
                writeln(f, f"  recent workflow runs (count={len(runs)}; showing up to 50):")
                for r in runs[:50]:
                    writeln(f, f"    run id={r.get('id')} name={r.get('name')} event={r.get('event')} status={r.get('status')} conclusion={r.get('conclusion')} created_at={r.get('created_at')}")
            else:
                writeln(f, f"  workflow runs API call failed: status={code} body={repr(runs_data)[:800]}")

            # search for artifacts named like 'rendered' or 'rendered-html' or 'feeds_summary' in repo artifacts
            if code in (200,201) and isinstance(data, dict):
                target_names = ["rendered-html","rendered","feeds_summary","feeds-summary","feeds-summary-"]
                found = []
                for a in arts:
                    nm = a.get("name","")
                    if any(x in nm for x in target_names):
                        found.append(a)
                writeln(f, f"  artifacts matching target names count={len(found)}")
                for a in found[:80]:
                    writeln(f, f"    MATCH artifact: name={a.get('name')} id={a.get('id')} created_at={a.get('created_at')} size={a.get('size_in_bytes')}")
            # optionally download a few artifacts to inspect zip content paths
            if DIAG_DOWNLOAD:
                diag_dir = "diag_artifacts"
                shutil.rmtree(diag_dir, ignore_errors=True)
                os.makedirs(diag_dir, exist_ok=True)
                writeln(f, f"  DIAG_DOWNLOAD enabled -> will attempt to download matched artifacts (rendered, feeds_summary) into {diag_dir}")
                for a in (arts or [])[:30]:
                    name = a.get("name","")
                    if any(x in name for x in ["rendered","feeds_summary","feeds-summary"]):
                        aid = a.get("id")
                        zip_url = f"{GITHUB_API_BASE}/repos/{GITHUB_REPOSITORY}/actions/artifacts/{aid}/zip"
                        hdrs = {"Authorization": "Bearer " + token} if token else {}
                        req = request.Request(zip_url, headers=hdrs, method="GET")
                        try:
                            writeln(f, f"    Downloading artifact id={aid} name={name} ...")
                            with request.urlopen(req, timeout=120) as resp:
                                data = resp.read()
                            z = zipfile.ZipFile(io.BytesIO(data))
                            members = z.namelist()
                            writeln(f, f"      zip entries count={len(members)} sample(20)={members[:20]}")
                            # extract
                            outsub = os.path.join(diag_dir, f"artifact-{aid}")
                            os.makedirs(outsub, exist_ok=True)
                            z.extractall(outsub)
                            writeln(f, f"      extracted to {outsub}")
                            # inspect extracted tree top-level
                            for top in sorted(os.listdir(outsub))[:80]:
                                writeln(f, f"        extracted top: {top}")
                                # if a top-level rendered or scripts/rendered exists, show sample files
                                top_path = os.path.join(outsub, top)
                                if os.path.isdir(top_path):
                                    for fn in sorted(os.listdir(top_path))[:20]:
                                        p = os.path.join(top_path, fn)
                                        try:
                                            s = os.path.getsize(p)
                                        except Exception:
                                            s = -1
                                        writeln(f, f"          {top}/{fn} size={s}")
                        except Exception as e:
                            writeln(f, f"      download/extract failed for artifact id={aid}: {e}")

        # final cross-checks: compare set of feed names vs rendered names
        writeln(f, "---- Cross-check: feeds vs rendered filenames ----")
        feed_basenames = set()
        if os.path.isdir("feeds"):
            for fn in os.listdir("feeds"):
                if fn.endswith(".xml"):
                    feed_basenames.add(fn[:-4])
        writeln(f, f"  feeds count = {len(feed_basenames)} sample: {sorted(list(feed_basenames))[:50]}")

        rendered_basenames = set()
        if os.path.isdir(rendered_dir):
            for fn in os.listdir(rendered_dir):
                name_nosfx = fn.rsplit(".",1)[0]
                rendered_basenames.add(name_nosfx)
        writeln(f, f"  rendered count = {len(rendered_basenames)} sample: {sorted(list(rendered_basenames))[:50]}")

        only_in_feeds = sorted(list(feed_basenames - rendered_basenames))
        only_in_rendered = sorted(list(rendered_basenames - feed_basenames))
        writeln(f, f"  only_in_feeds (count={len(only_in_feeds)}) sample: {only_in_feeds[:80]}")
        writeln(f, f"  only_in_rendered (count={len(only_in_rendered)}) sample: {only_in_rendered[:80]}")

        writeln(f, "---- End diagnostic ----")
        writeln(f, f"Detailed output saved to {OUTPATH}")
        return 0
    finally:
        f.close()

if __name__ == "__main__":
    rc = main()
    sys.exit(rc)
