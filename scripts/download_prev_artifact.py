#!/usr/bin/env python3
"""
Robust downloader for previous artifacts by name.

Usage:
  python3 scripts/download_prev_artifact.py <artifact-name>

Behaviour:
 - Uses GITHUB_TOKEN and GITHUB_RUN_ID and GITHUB_REPOSITORY from env.
 - Finds the artifact with the requested name whose created_at is the
   newest but strictly BEFORE the current run's created_at.
 - Downloads and extracts zip. For feeds_summary it will rename the
   extracted xlsx (if found) to prev_feeds_summary.xlsx in the workspace.
 - For sent-ids it will extract .github/data/sent_ids.json if present.
 - For rendered-html it will extract its contents into scripts/rendered/
 - Exits 0 on success, non-zero on fatal error.
"""
from __future__ import print_function
import os, sys, json, zipfile, io, shutil
from urllib import request, parse, error
from datetime import datetime

GITHUB_API = "https://api.github.com"

def _env(name):
    return os.environ.get(name, "")

def gh_get(path, token, params=None):
    url = GITHUB_API.rstrip("/") + path
    if params:
        url += "?" + parse.urlencode(params)
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=30) as r:
            data = r.read().decode("utf-8")
            return r.getcode(), json.loads(data)
    except error.HTTPError as he:
        body = he.read().decode("utf-8", errors="ignore")
        print(f"HTTPError {he.code}: {body}", file=sys.stderr)
        return he.code, None
    except Exception as e:
        print("gh_get exception:", e, file=sys.stderr)
        return None, None

def download_artifact_zip(repo, artifact_id, token):
    url = f"{GITHUB_API}/repos/{repo}/actions/artifacts/{artifact_id}/zip"
    headers = {"Accept": "application/zip"}
    if token:
        headers["Authorization"] = "Bearer " + token
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=120) as r:
            return r.read()
    except Exception as e:
        print("download_artifact_zip failed:", e, file=sys.stderr)
        return None

def find_prev_artifact(repo, token, artifact_name, current_run_created_at):
    # list artifacts (paginated - get first 100)
    status, data = gh_get(f"/repos/{repo}/actions/artifacts", token, {"per_page":100})
    if status != 200 or not data:
        print("Failed to list artifacts", file=sys.stderr)
        return None
    arts = data.get("artifacts", [])
    # filter by name and created_at < current_run_created_at
    prevs = []
    for a in arts:
        if a.get("name") != artifact_name:
            continue
        created = a.get("created_at")
        if not created:
            continue
        # compare ISO datetimes lexicographically ok for Z times, but parse to be safe
        try:
            ca = datetime.fromisoformat(created.replace("Z","+00:00"))
            if ca < current_run_created_at:
                prevs.append((ca, a))
        except Exception:
            continue
    if not prevs:
        return None
    # choose the latest (max by created)
    prevs.sort(key=lambda x: x[0], reverse=True)
    return prevs[0][1]

def main(argv):
    if len(argv) < 2:
        print("Usage: download_prev_artifact.py <artifact-name>", file=sys.stderr)
        return 2
    name = argv[1]
    repo = _env("GITHUB_REPOSITORY")
    run_id = _env("GITHUB_RUN_ID")
    token = _env("GITHUB_TOKEN")
    if not repo or not run_id or not token:
        print("Missing GITHUB_REPOSITORY or GITHUB_RUN_ID or GITHUB_TOKEN in env", file=sys.stderr)
        return 3

    # get current run created_at
    status, run_data = gh_get(f"/repos/{repo}/actions/runs/{run_id}", token)
    if status != 200 or not run_data:
        print("Failed to get run metadata", file=sys.stderr)
        return 4
    run_created = run_data.get("created_at")
    if not run_created:
        print("Run created_at missing", file=sys.stderr)
        return 5
    try:
        run_created_dt = datetime.fromisoformat(run_created.replace("Z","+00:00"))
    except Exception:
        print("Failed to parse run created_at", file=sys.stderr)
        return 6

    art = find_prev_artifact(repo, token, name, run_created_dt)
    if not art:
        print(f"No previous artifact named '{name}' found (prior to this run).")
        return 0

    aid = art.get("id")
    print(f"Found candidate artifact id={aid} name={art.get('name')} created_at={art.get('created_at')}")
    zbytes = download_artifact_zip(repo, aid, token)
    if not zbytes:
        print("Download failed for artifact zip", file=sys.stderr)
        return 7

    # extract zip to tmp dir, inspect contents
    import zipfile, os
    z = zipfile.ZipFile(io.BytesIO(zbytes))
    members = z.namelist()
    print("Zip members sample:", members[:20])
    outdir = os.path.join("prev_artifacts", f"artifact-{aid}")
    os.makedirs(outdir, exist_ok=True)
    z.extractall(outdir)
    # now search for sensible files:
    # - if artifact is feeds_summary, find any .xlsx and rename to prev_feeds_summary.xlsx in workspace root
    if name in ("feeds_summary","feeds-summary"):
        found = False
        for root, dirs, files in os.walk(outdir):
            for fn in files:
                if fn.lower().endswith(".xlsx"):
                    src = os.path.join(root, fn)
                    dst = os.path.join(os.getcwd(), "prev_feeds_summary.xlsx")
                    shutil.copy2(src, dst)
                    print(f"Extracted prev xlsx -> {dst}")
                    found = True
                    break
            if found:
                break
        if not found:
            print("No xlsx found inside feeds_summary artifact zip")
    # - if artifact is sent-ids, extract .github/data/sent_ids.json into that path
    if name.startswith("sent"):
        for root, dirs, files in os.walk(outdir):
            for fn in files:
                if fn.endswith("sent_ids.json") or fn.endswith("sent_ids.json".replace("_","-")) or fn.endswith("sent_ids.json"):
                    src = os.path.join(root, fn)
                    tgt_dir = os.path.join(".github","data")
                    os.makedirs(tgt_dir, exist_ok=True)
                    dst = os.path.join(tgt_dir, "sent_ids.json")
                    shutil.copy2(src, dst)
                    print(f"Extracted sent_ids -> {dst}")

    # - if artifact is rendered-html, try to move extracted rendered/ or scripts/rendered into scripts/rendered
    if name in ("rendered-html","rendered"):
        # find any top-level rendered/ or scripts/rendered/ inside outdir
        target = os.path.join("scripts","rendered")
        os.makedirs(target, exist_ok=True)
        moved = 0
        # copy files preserving names (overwrite)
        for root, dirs, files in os.walk(outdir):
            for fn in files:
                rel = os.path.relpath(root, outdir)
                # avoid copying weird meta files; place everything into scripts/rendered/
                src = os.path.join(root, fn)
                dst = os.path.join(target, fn)
                try:
                    shutil.copy2(src, dst)
                    moved += 1
                except Exception:
                    pass
        print(f"Copied {moved} files into {target}")

    print("Done.")
    return 0

if __name__ == "__main__":
    rc = main(sys.argv)
    sys.exit(rc)
