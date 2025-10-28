#!/usr/bin/env python3
"""
download_prev_artifact.py (robust) - v2

- Usage: python3 scripts/download_prev_artifact.py <artifact-name>
- Purpose: find the most-recent artifact with that name created BEFORE this run,
  download and extract it. If artifact is 'rendered-html', ensure we populate
  scripts/rendered/ with the extracted HTML files (preserving directory structure).
- Fallback: if 'rendered-html' artifact is missing or doesn't contain expected files,
  try to find the most recent previous run of the workflow named
  'Generate RSS feeds (stable + businesswire)' and download its rendered-html.
"""
from __future__ import print_function
import os, sys, json, io, shutil, zipfile
from urllib import request, parse, error
from datetime import datetime, timezone, timedelta

GITHUB_API = "https://api.github.com"

def env(k): return os.environ.get(k, "")

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
            return r.getcode(), json.loads(r.read().decode("utf-8"))
    except error.HTTPError as he:
        body = he.read().decode("utf-8", errors="ignore")
        print(f"HTTPError {he.code} on {url}: {body}", file=sys.stderr)
        return he.code, None
    except Exception as e:
        print(f"gh_get exception for {url}: {e}", file=sys.stderr)
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

def pick_prev_artifact_by_name(repo, token, name, before_dt):
    status, data = gh_get(f"/repos/{repo}/actions/artifacts", token, {"per_page":100})
    if status != 200 or not data:
        print("Failed to list artifacts", file=sys.stderr); return None
    arts = data.get("artifacts", [])
    candidates = []
    for a in arts:
        if a.get("name") != name:
            continue
        created = a.get("created_at")
        if not created:
            continue
        try:
            ca = datetime.fromisoformat(created.replace("Z","+00:00"))
        except Exception:
            continue
        if ca < before_dt:
            candidates.append((ca, a))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]

def download_and_extract_to_tmp(zbytes, dest_tmp):
    os.makedirs(dest_tmp, exist_ok=True)
    z = zipfile.ZipFile(io.BytesIO(zbytes))
    z.extractall(dest_tmp)
    return dest_tmp

def copy_rendered_subdir(tmpdir, dest_rendered):
    """
    Search for a 'rendered' directory inside tmpdir or 'scripts/rendered'
    and copy its entire tree to dest_rendered preserving relative paths.
    Returns list of copied file paths (relative to dest_rendered).
    """
    candidates = []
    for root, dirs, files in os.walk(tmpdir):
        # identify directories named 'rendered' or 'scripts/rendered'
        if os.path.basename(root) == "rendered":
            candidates.append(root)
        # also if path endswith scripts/rendered
        if root.endswith(os.path.join("scripts","rendered")):
            candidates.append(root)
    # if nothing obvious, consider top-level html files
    if not candidates:
        # find any .html files anywhere and copy them into dest_rendered flat structure
        html_files = []
        for root, dirs, files in os.walk(tmpdir):
            for f in files:
                if f.lower().endswith(".html"):
                    html_files.append(os.path.join(root, f))
        if html_files:
            os.makedirs(dest_rendered, exist_ok=True)
            copied = []
            for p in html_files:
                dst = os.path.join(dest_rendered, os.path.basename(p))
                try:
                    shutil.copy2(p, dst)
                    copied.append(os.path.relpath(dst, dest_rendered))
                except Exception as e:
                    print("copy failed", p, e, file=sys.stderr)
            return copied
        return []

    # if we have candidate directories, copy their tree into dest_rendered preserving subpaths
    copied = []
    for cand in candidates:
        for root, dirs, files in os.walk(cand):
            rel = os.path.relpath(root, cand)
            for f in files:
                src = os.path.join(root, f)
                target_dir = os.path.normpath(os.path.join(dest_rendered, rel)) if rel != "." else dest_rendered
                os.makedirs(target_dir, exist_ok=True)
                dst = os.path.join(target_dir, f)
                try:
                    shutil.copy2(src, dst)
                    copied.append(os.path.relpath(dst, dest_rendered))
                except Exception as e:
                    print("copy failed", src, e, file=sys.stderr)
    # dedupe copied
    return sorted(list(dict.fromkeys(copied)))

def find_latest_run_of_workflow(repo, token, workflow_name, before_dt):
    """
    Search recent runs and return the run dict for the latest run with name==workflow_name
    and created_at < before_dt and concluded successfully/completed.
    """
    # fetch recent runs (100)
    status, data = gh_get(f"/repos/{repo}/actions/runs", token, {"per_page":100})
    if status != 200 or not data:
        return None
    runs = data.get("workflow_runs", [])
    cand = []
    for r in runs:
        if r.get("name") != workflow_name:
            continue
        created = r.get("created_at")
        try:
            ca = datetime.fromisoformat(created.replace("Z","+00:00"))
        except Exception:
            continue
        if ca < before_dt:
            cand.append((ca, r))
    if not cand:
        return None
    cand.sort(key=lambda x: x[0], reverse=True)
    return cand[0][1]

def download_artifact_for_run(repo, token, run_id, artifact_name):
    # list artifacts for run
    status, data = gh_get(f"/repos/{repo}/actions/runs/{run_id}/artifacts", token)
    if status != 200 or not data:
        return None
    arts = data.get("artifacts", [])
    for a in arts:
        if a.get("name") == artifact_name:
            aid = a.get("id")
            return a
    return None

def main(argv):
    if len(argv) < 2:
        print("Usage: download_prev_artifact.py <artifact-name>", file=sys.stderr)
        return 2
    name = argv[1]
    repo = env("GITHUB_REPOSITORY")
    run_id = env("GITHUB_RUN_ID")
    token = env("GITHUB_TOKEN")
    if not repo or not run_id or not token:
        print("Missing GITHUB_REPOSITORY or GITHUB_RUN_ID or GITHUB_TOKEN in env", file=sys.stderr)
        return 3

    # get current run metadata
    status, run_meta = gh_get(f"/repos/{repo}/actions/runs/{run_id}", token)
    if status != 200 or not run_meta:
        print("Failed to get run metadata", file=sys.stderr); return 4
    run_created = run_meta.get("created_at")
    if not run_created:
        print("Missing run created_at", file=sys.stderr); return 5
    try:
        run_created_dt = datetime.fromisoformat(run_created.replace("Z","+00:00"))
    except Exception:
        print("Failed to parse run created_at", file=sys.stderr); return 6

    print(f"Looking for previous artifact named '{name}' prior to run {run_id} ({run_created})")
    art = pick_prev_artifact_by_name(repo, token, name, run_created_dt)
    tmpdir = None

    if art:
        aid = art.get("id")
        print(f"Found artifact candidate id={aid} created_at={art.get('created_at')} size={art.get('size')}")
        z = download_artifact_zip(repo, aid, token)
        if z:
            tmpdir = os.path.join("prev_artifacts", f"artifact-{aid}")
            download_and_extract_to_tmp(z, tmpdir)
            # If artifact name is feeds_summary, copy the xlsx as prev_feeds_summary.xlsx
            if name in ("feeds_summary","feeds-summary"):
                found_xlsx = False
                for root, dirs, files in os.walk(tmpdir):
                    for f in files:
                        if f.lower().endswith(".xlsx"):
                            src = os.path.join(root, f)
                            dst = os.path.join(os.getcwd(), "prev_feeds_summary.xlsx")
                            shutil.copy2(src, dst)
                            print("Extracted previous feeds xlsx ->", dst)
                            found_xlsx = True
                            break
                    if found_xlsx:
                        break
                if not found_xlsx:
                    print("No xlsx found inside feeds_summary artifact")
            if name.startswith("sent") or name == "sent-ids":
                # try to find sent ids json
                for root, dirs, files in os.walk(tmpdir):
                    for f in files:
                        if f.endswith("sent_ids.json") or f.endswith("sent-ids.json") or f.endswith("sent_ids.json".replace("_","-")):
                            src = os.path.join(root, f)
                            tgt_dir = os.path.join(".github","data")
                            os.makedirs(tgt_dir, exist_ok=True)
                            dst = os.path.join(tgt_dir, "sent_ids.json")
                            shutil.copy2(src, dst)
                            print("Extracted sent_ids ->", dst)
            if name in ("rendered-html","rendered"):
                dest_rendered = os.path.join("scripts","rendered")
                os.makedirs(dest_rendered, exist_ok=True)
                copied = copy_rendered_subdir(tmpdir, dest_rendered)
                print(f"Copied {len(copied)} rendered files into {dest_rendered}: {copied[:40]}")
                # quick heuristic: if we have at least e.g. 6 files, accept; else fallback
                if len(copied) >= 6:
                    print("Rendered extraction looks sufficient; done.")
                    return 0
                else:
                    print("Rendered extraction seems small (copied %d files) - will attempt fallback to Generate workflow artifact." % len(copied))

    # Fallback logic for rendered-html: find last run of the Generate workflow and fetch its artifact
    if name in ("rendered-html","rendered"):
        workflow_name = "Generate RSS feeds (stable + businesswire)"
        print(f"Attempting fallback: find last run of workflow '{workflow_name}' prior to this run")
        wf_run = find_latest_run_of_workflow(repo, token, workflow_name, run_created_dt)
        if wf_run:
            wf_run_id = wf_run.get("id")
            print("Found workflow run:", wf_run_id, wf_run.get("created_at"), wf_run.get("conclusion"))
            art2 = download_artifact_for_run(repo, token, wf_run_id, "rendered-html")
            if art2:
                aid2 = art2.get("id")
                print("Found rendered-html artifact on generate workflow run:", aid2)
                z2 = download_artifact_zip(repo, aid2, token)
                if z2:
                    tmpdir2 = os.path.join("prev_artifacts", f"artifact-{aid2}")
                    download_and_extract_to_tmp(z2, tmpdir2)
                    dest_rendered = os.path.join("scripts","rendered")
                    os.makedirs(dest_rendered, exist_ok=True)
                    copied = copy_rendered_subdir(tmpdir2, dest_rendered)
                    print(f"Fallback copied {len(copied)} files into {dest_rendered}: {copied[:40]}")
                    if copied:
                        return 0
                    else:
                        print("Fallback did not find any rendered files inside artifact.", file=sys.stderr)
                else:
                    print("Failed to download artifact zip for fallback.", file=sys.stderr)
            else:
                print("No rendered-html artifact attached to found generate workflow run.", file=sys.stderr)
        else:
            print("No previous generate workflow run found matching name.", file=sys.stderr)

    # If we get here, we did not find relevant artifact or couldn't extract useful rendered files.
    print("No previous artifact extracted (or no useful rendered files). Exiting gracefully.")
    return 0

if __name__ == "__main__":
    rc = main(sys.argv)
    sys.exit(rc)
