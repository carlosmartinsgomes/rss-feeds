#!/usr/bin/env python3
# scripts/download_prev_artifact.py
"""
Download the most recent artifact whose name matches the given name (or contains it)
from the repository's artifacts, extract it into the current working directory,
and print clear diagnostic messages.

Usage:
  python3 scripts/download_prev_artifact.py rendered-html
  python3 scripts/download_prev_artifact.py feeds_summary
Environment:
  GITHUB_REPOSITORY (owner/repo) - provided by Actions
  GITHUB_TOKEN - required to access artifacts API
"""
import os
import sys
import requests
import zipfile
import io
import time

GITHUB_API = "https://api.github.com"

def die(msg, code=1):
    print("ERROR:", msg)
    sys.exit(code)

def getenv_first(*names, default=None):
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return default

def find_artifact(repo, token, name_query, per_page=100):
    headers = {"Authorization": f"Bearer {token}", "Accept":"application/vnd.github+json"}
    url = f"{GITHUB_API}/repos/{repo}/actions/artifacts?per_page={per_page}"
    artifacts = []
    try:
        r = requests.get(url, headers=headers, timeout=30)
        r.raise_for_status()
    except Exception as e:
        die(f"Failed to list artifacts: {e}")

    data = r.json()
    artifacts.extend(data.get("artifacts", []))

    # pagination (only if there are more pages)
    while data.get("total_count", 0) > len(artifacts):
        # Try to follow 'next' from headers if present
        if 'next' not in r.links:
            break
        nxt = r.links['next']['url']
        try:
            r = requests.get(nxt, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()
            artifacts.extend(data.get("artifacts", []))
        except Exception:
            break

    # Filter: exact name match preferred, else contains
    exact = [a for a in artifacts if a.get("name") == name_query]
    if exact:
        # pick latest by created_at
        exact.sort(key=lambda a: a.get("created_at",""), reverse=True)
        return exact[0]

    contains = [a for a in artifacts if name_query in a.get("name","")]
    if contains:
        contains.sort(key=lambda a: a.get("created_at",""), reverse=True)
        return contains[0]

    return None

def download_and_extract(repo, token, artifact_id, dest_dir="."):
    headers = {"Authorization": f"Bearer {token}", "Accept":"application/vnd.github+json"}
    url = f"{GITHUB_API}/repos/{repo}/actions/artifacts/{artifact_id}/zip"
    try:
        r = requests.get(url, headers=headers, stream=True, timeout=120)
        r.raise_for_status()
    except Exception as e:
        die(f"Failed to download artifact id={artifact_id}: {e}")

    print(f"Downloaded artifact id={artifact_id}, size={r.headers.get('Content-Length','?')} bytes (streaming)...")
    z = zipfile.ZipFile(io.BytesIO(r.content))
    members = z.namelist()
    print(f"ZIP contains {len(members)} entries, extracting to '{dest_dir}' ...")
    # Extract preserving directories
    z.extractall(dest_dir)
    print("Extraction complete. Sample files:")
    for m in members[:40]:
        print("  ", m)
    return members

def main():
    if len(sys.argv) < 2:
        die("Usage: download_prev_artifact.py <artifact-name>")

    artifact_name = sys.argv[1]
    repo = getenv_first("GITHUB_REPOSITORY")
    token = getenv_first("GITHUB_TOKEN", "GITHUB_TOKEN")
    if not repo:
        die("GITHUB_REPOSITORY not set in environment (owner/repo).")
    if not token:
        die("GITHUB_TOKEN not set in environment (required to fetch artifacts).")

    print(f"Looking for artifact matching: {artifact_name} in repo {repo} ...")
    art = find_artifact(repo, token)
    # If we didn't find any, search with name query
    if art is None:
        # try again but filter by name_query in artifacts (explicit loop)
        print("No artifacts found in first page; aborting search.")
        die("No artifacts found.")
    # But we used find_artifact without passing query: fix — call with name
    art = find_artifact(repo, token, artifact_name)
    if art is None:
        print(f"No artifact matching '{artifact_name}' found (checked recent artifacts).")
        sys.exit(0)

    aid = art.get("id")
    aname = art.get("name")
    created = art.get("created_at")
    size = art.get("size_in_bytes", 0)
    print(f"Found artifact '{aname}' id={aid} created_at={created} size={size}")
    # Download and extract into repo root; if artifact is a folder (rendered/*), ensure scripts/rendered exists
    try:
        members = download_and_extract(repo, token, aid, ".")
    except Exception as e:
        die(f"Download/extract failed: {e}")

    # If extraction produced a top-level 'rendered' dir, move/rename if needed
    # (We don't do moving here — generate_feeds.py expects scripts/rendered/)
    print("Done. Please confirm scripts/rendered/* exists if you expected rendered HTMLs.")

if __name__ == "__main__":
    main()
