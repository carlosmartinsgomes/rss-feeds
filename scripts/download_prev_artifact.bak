#!/usr/bin/env python3
"""
scripts/download_prev_artifact.py <artifact_name>

Find the most recent artifact in the repo with the given name whose created_at
is strictly BEFORE the current run's created_at, download the zip and extract
its contents into the current working directory.

Requires environment:
  - GITHUB_TOKEN (a token with repo/actions read)
  - GITHUB_REPOSITORY (owner/repo) - usually set in Actions env
  - GITHUB_RUN_ID (current run id) - set in Actions env
"""
import sys, os, requests, zipfile, io, datetime

def die(msg):
    print("ERROR:", msg)
    sys.exit(2)

if len(sys.argv) < 2:
    die("usage: download_prev_artifact.py <artifact_name>")

artifact_name = sys.argv[1]

token = os.environ.get("GITHUB_TOKEN") or os.environ.get("INPUT_GITHUB_TOKEN")
repo = os.environ.get("GITHUB_REPOSITORY")
run_id = os.environ.get("GITHUB_RUN_ID")

if not token:
    die("GITHUB_TOKEN not set in environment")
if not repo:
    die("GITHUB_REPOSITORY not set in environment")
if not run_id:
    die("GITHUB_RUN_ID not set in environment")

API_BASE = "https://api.github.com"

sess = requests.Session()
sess.headers.update({
    "Authorization": f"Bearer {token}",
    "Accept": "application/vnd.github+json",
    "User-Agent": "download-prev-artifact-script"
})

# Get current run created_at
r = sess.get(f"{API_BASE}/repos/{repo}/actions/runs/{run_id}")
if r.status_code != 200:
    die(f"failed to fetch run info: {r.status_code} {r.text}")
run_info = r.json()
run_created_at = run_info.get("created_at")
if not run_created_at:
    die("could not determine current run created_at")
run_created_at_dt = datetime.datetime.fromisoformat(run_created_at.replace("Z","+00:00"))

# list artifacts (pagination simple: per_page=100)
artifacts = []
page = 1
while True:
    resp = sess.get(f"{API_BASE}/repos/{repo}/actions/artifacts?per_page=100&page={page}")
    if resp.status_code != 200:
        die(f"failed to list artifacts: {resp.status_code} {resp.text}")
    j = resp.json()
    artifacts.extend(j.get("artifacts", []))
    if not j.get("artifacts") or len(j.get("artifacts")) < 100:
        break
    page += 1

# filter by name and created_at < run_created_at
candidates = []
for a in artifacts:
    if a.get("name") != artifact_name:
        continue
    ca = a.get("created_at")
    if not ca:
        continue
    ca_dt = datetime.datetime.fromisoformat(ca.replace("Z","+00:00"))
    if ca_dt < run_created_at_dt:
        candidates.append((ca_dt, a))

if not candidates:
    print(f"No previous artifact named '{artifact_name}' found (created before current run).")
    sys.exit(0)

# pick most recent one (max created_at)
candidates.sort(key=lambda x: x[0], reverse=True)
chosen = candidates[0][1]
print(f"Found artifact '{artifact_name}' id={chosen.get('id')} created_at={chosen.get('created_at')} size={chosen.get('size_in_bytes')}")

download_url = chosen.get("archive_download_url")
if not download_url:
    die("artifact missing archive_download_url")

# download the zip
resp = sess.get(download_url, stream=True)
if resp.status_code not in (200, 302):
    die(f"failed to download artifact archive: {resp.status_code} {resp.text}")

# follow redirect if GitHub returns it
if resp.status_code == 302:
    redirect_url = resp.headers.get("Location")
    resp = sess.get(redirect_url, stream=True)
    if resp.status_code != 200:
        die(f"redirect download failed: {resp.status_code} {resp.text}")

buf = io.BytesIO(resp.content)
try:
    z = zipfile.ZipFile(buf)
except Exception as e:
    die(f"failed to open artifact zip: {e}")

# extract all files into cwd (overwrites)
print("Extracting artifact contents into current working directory...")
for zi in z.infolist():
    name = zi.filename
    print("  extracting", name)
    z.extract(zi, ".")
print("Done.")
sys.exit(0)
