#!/usr/bin/env python3
# Robust download_prev_artifact.py (improved)
# See conversation: prefers matching 'feeds_summary' OR 'feeds-summary-<run>-<nr>' etc,
# uses pagination, multiple Accept attempts, and copies any rendered HTML into scripts/rendered.

import os, sys, io, zipfile, shutil, json, time
from datetime import datetime
from urllib import request, parse, error

try:
    import requests
except Exception:
    requests = None

GITHUB_API = "https://api.github.com"

def env(k): return os.environ.get(k, "")

def gh_get(path, token, params=None):
    url = GITHUB_API.rstrip("/") + path
    if params:
        url += "?" + parse.urlencode(params)
    headers = {"Accept": "application/vnd.github+json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    try:
        req = request.Request(url, headers=headers, method="GET")
        with request.urlopen(req, timeout=30) as r:
            return r.getcode(), json.loads(r.read().decode("utf-8"))
    except error.HTTPError as he:
        body = he.read().decode("utf-8", errors="ignore")
        print(f"HTTPError {he.code} on {url}: {body}", file=sys.stderr)
        return he.code, None
    except Exception as e:
        print(f"gh_get exception for {url}: {e}", file=sys.stderr)
        return None, None

def list_artifacts_paginated(repo, token, per_page=100, max_pages=10):
    artifacts = []
    page = 1
    while page <= max_pages:
        status, data = gh_get(f"/repos/{repo}/actions/artifacts", token, {"per_page": per_page, "page": page})
        if status != 200 or not data:
            break
        arts = data.get("artifacts", []) or []
        if not arts:
            break
        artifacts.extend(arts)
        if len(arts) < per_page:
            break
        page += 1
    return artifacts

def download_bytes_via_requests(url, token, accept=None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if accept:
        headers["Accept"] = accept
    for attempt in range(4):
        try:
            r = requests.get(url, headers=headers, timeout=120, stream=True)
            if r.status_code == 200:
                return r.content
            else:
                print(f"requests download returned status {r.status_code} (attempt {attempt+1}) for {url}")
            time.sleep(1)
        except Exception as e:
            print("requests.get exception:", e, "attempt", attempt+1)
            time.sleep(1)
    return None

def download_bytes_via_urllib(url, token, accept=None):
    headers = {}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if accept:
        headers["Accept"] = accept
    req = request.Request(url, headers=headers, method="GET")
    try:
        with request.urlopen(req, timeout=120) as r:
            return r.read()
    except Exception as e:
        print("urllib download exception:", e)
        return None

def download_artifact_zip_bytes(artifact, repo, token):
    url = artifact.get("archive_download_url") or artifact.get("url")
    if not url:
        print("artifact has no archive_download_url/url", artifact)
        return None
    accepts = [ "application/zip", "application/octet-stream", None ]
    for acc in accepts:
        if requests:
            b = download_bytes_via_requests(url, token, accept=acc)
        else:
            b = download_bytes_via_urllib(url, token, accept=acc)
        if b:
            return b
        else:
            print(f"Download attempt with Accept={acc} failed for artifact id={artifact.get('id')}")
    return None

def extract_zip_to_tmp(zbytes, tmpdir):
    os.makedirs(tmpdir, exist_ok=True)
    try:
        z = zipfile.ZipFile(io.BytesIO(zbytes))
        z.extractall(tmpdir)
        return True
    except Exception as e:
        print("zip extraction failed:", e)
        return False

def find_rendered_roots(tmpdir):
    candidates = []
    for root, dirs, files in os.walk(tmpdir):
        base = os.path.basename(root).lower()
        if base == "rendered":
            candidates.append(root)
        if root.replace("\\","/").lower().endswith("/scripts/rendered"):
            candidates.append(root)
    if not candidates:
        for root, dirs, files in os.walk(tmpdir):
            html_count = sum(1 for f in files if f.lower().endswith(".html"))
            if html_count >= 1:
                candidates.append(root)
    return list(dict.fromkeys(candidates))

def copy_candidates_to_scripts_rendered(tmpdir, dest="scripts/rendered"):
    os.makedirs(dest, exist_ok=True)
    roots = find_rendered_roots(tmpdir)
    copied = []
    for root in roots:
        rel_root = os.path.relpath(root, tmpdir)
        for r, dirs, files in os.walk(root):
            rel = os.path.relpath(r, root)
            for f in files:
                if not f.lower().endswith(".html"):
                    continue
                src = os.path.join(r, f)
                target_dir = os.path.normpath(os.path.join(dest, rel)) if rel and rel != "." else dest
                os.makedirs(target_dir, exist_ok=True)
                dst = os.path.join(target_dir, f)
                try:
                    shutil.copy2(src, dst)
                    copied.append(os.path.relpath(dst, dest))
                except Exception as e:
                    print("copy failed:", src, e)
    if not copied:
        for root, dirs, files in os.walk(tmpdir):
            for f in files:
                if f.lower().endswith(".html"):
                    src = os.path.join(root, f)
                    dst = os.path.join(dest, f)
                    try:
                        shutil.copy2(src, dst)
                        copied.append(os.path.relpath(dst, dest))
                    except Exception as e:
                        print("copy top-level html failed:", src, e)
    return sorted(list(dict.fromkeys(copied)))

def match_artifact_name(target_name, candidate_name):
    if not candidate_name:
        return False
    if candidate_name == target_name:
        return True
    # allow common variants:
    # feeds_summary  <-> feeds-summary-<run>-<nr>
    t = target_name.replace('_','-').lower()
    c = candidate_name.replace('_','-').lower()
    if c.startswith(t + "-"):
        return True
    if t in c:
        # cautious contains match (helps with 'feeds_summary' vs 'feeds-summary-...'), but avoid accidental false positives
        return True
    return False

def pick_candidates_before(artifacts, before_dt, name):
    cand = []
    for a in artifacts:
        aname = a.get("name") or ""
        if not match_artifact_name(name, aname):
            continue
        ca = a.get("created_at")
        if not ca:
            continue
        try:
            ca_dt = datetime.fromisoformat(ca.replace("Z","+00:00"))
        except Exception:
            continue
        if ca_dt < before_dt:
            cand.append((ca_dt, a))
    cand.sort(key=lambda x: x[0], reverse=True)
    return [a for _, a in cand]

def find_generate_workflow_runs(repo, token, per_page=100):
    status, data = gh_get(f"/repos/{repo}/actions/runs", token, {"per_page": per_page})
    if status != 200 or not data:
        return []
    runs = data.get("workflow_runs", [])
    return runs

def find_artifact_on_run(repo, token, run_id, artifact_name):
    status, data = gh_get(f"/repos/{repo}/actions/runs/{run_id}/artifacts", token)
    if status != 200 or not data:
        return None
    arts = data.get("artifacts", [])
    for a in arts:
        if match_artifact_name(artifact_name, a.get("name")):
            return a
    return None

def main(argv):
    if len(argv) < 2:
        print("Usage: download_prev_artifact.py <artifact-name>", file=sys.stderr); return 2
    name = argv[1]
    repo = env("GITHUB_REPOSITORY")
    run_id = env("GITHUB_RUN_ID")
    token = env("GITHUB_TOKEN")
    if not repo or not run_id or not token:
        print("Missing GITHUB_REPOSITORY or GITHUB_RUN_ID or GITHUB_TOKEN in env", file=sys.stderr); return 3

    status, run_meta = gh_get(f"/repos/{repo}/actions/runs/{run_id}", token)
    if status != 200 or not run_meta:
        print("Failed to get run metadata", file=sys.stderr); return 4
    run_created = run_meta.get("created_at")
    if not run_created:
        print("Missing run created_at", file=sys.stderr); return 5
    run_created_dt = datetime.fromisoformat(run_created.replace("Z","+00:00"))
    print(f"Run #{run_id} created_at={run_created} - looking for artifacts named '{name}' BEFORE this timestamp")

    artifacts = list_artifacts_paginated(repo, token, per_page=100, max_pages=10)
    candidates = pick_candidates_before(artifacts, run_created_dt, name)
    print(f"Found {len(candidates)} artifact candidates named '{name}' prior to this run (scanning recent artifacts).")

    expected_min_rendered = 6
    for a in candidates:
        aid = a.get("id")
        created = a.get("created_at")
        size = a.get("size")
        print(f"Trying artifact id={aid} name={a.get('name')} created_at={created} size={size}")
        zbytes = None
        try:
            zbytes = download_artifact_zip_bytes(a, repo, token)
        except Exception as e:
            print("download error:", e)
            zbytes = None
        if not zbytes:
            print(f"Could not download artifact id={aid} (trying next candidate)")
            continue
        tmpdir = os.path.join("prev_artifacts", f"artifact-{aid}")
        ok = extract_zip_to_tmp(zbytes, tmpdir)
        if not ok:
            print("Zip extract failed; trying next candidate")
            continue
        copied = copy_candidates_to_scripts_rendered(tmpdir, dest=os.path.join("scripts","rendered"))
        print(f"Copied {len(copied)} rendered files from artifact {aid}: {copied[:80]}")
        if len(copied) >= expected_min_rendered:
            print("Sufficient rendered files found -> done.")
            return 0
        else:
            print(f"Only copied {len(copied)} files (need >= {expected_min_rendered}) -> trying next candidate")

    if name in ("rendered-html","rendered","feeds_summary","feeds-summary"):
        print("No adequate direct artifact found. Attempting fallback: scan Generate workflow runs for a matching artifact.")
        runs = find_generate_workflow_runs(repo, token, per_page=200)
        gen_runs = []
        for r in runs:
            if r.get("name") == "Generate RSS feeds (stable + businesswire)":
                ca = r.get("created_at")
                try:
                    ca_dt = datetime.fromisoformat(ca.replace("Z","+00:00"))
                except Exception:
                    continue
                if ca_dt < run_created_dt:
                    gen_runs.append((ca_dt, r))
        gen_runs.sort(key=lambda x: x[0], reverse=True)
        print(f"Found {len(gen_runs)} previous Generate workflow runs to try.")
        for ca_dt, r in gen_runs:
            rid = r.get("id")
            print(f"Checking generate workflow run id={rid} created_at={r.get('created_at')} conclusion={r.get('conclusion')}")
            art = find_artifact_on_run(repo, token, rid, name)
            if not art:
                print("No matching artifact attached to this generate run -> next run")
                continue
            try:
                zbytes = download_artifact_zip_bytes(art, repo, token)
            except Exception as e:
                print("download error for artifact on run", rid, e)
                zbytes = None
            if not zbytes:
                print("Could not download this artifact -> next run")
                continue
            tmpdir = os.path.join("prev_artifacts", f"artifact-{art.get('id')}")
            if not extract_zip_to_tmp(zbytes, tmpdir):
                print("extract failed -> next run")
                continue
            copied = copy_candidates_to_scripts_rendered(tmpdir, dest=os.path.join("scripts","rendered"))
            print(f"Fallback copied {len(copied)} files from generate-run artifact {art.get('id')}: {copied[:80]}")
            if copied:
                print("Fallback succeeded -> done.")
                return 0
            else:
                print("Fallback artifact had no html files -> next run")
    print("No previous artifact extracted (or no useful rendered files). Exiting gracefully.")
    return 0

if __name__ == "__main__":
    rc = main(sys.argv)
    sys.exit(rc)
