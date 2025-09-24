#!/usr/bin/env python3
# scripts/download_artifact_xml.py
# Usage:
#   export GITHUB_TOKEN=ghp_...
#   python3 scripts/download_artifact_xml.py --repo owner/repo --artifact-name feeds --outdir tmp_artifacts
#
# If you want the latest run, omit --run-id and script will pick the latest run with that artifact name.

import os
import sys
import argparse
import requests
import zipfile
import io
import time

API = "https://api.github.com"

def headers(token):
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}

def find_artifact(repo, artifact_name, token, run_id=None):
    owner_repo = repo
    if run_id:
        url = f"{API}/repos/{owner_repo}/actions/runs/{run_id}/artifacts"
    else:
        url = f"{API}/repos/{owner_repo}/actions/artifacts"
    r = requests.get(url, headers=headers(token))
    r.raise_for_status()
    data = r.json()
    artifacts = data.get("artifacts", [])
    for a in artifacts:
        if a.get("name") == artifact_name or artifact_name in a.get("name",""):
            return a
    # if not exact match, try substring match
    for a in artifacts:
        if artifact_name in a.get("name",""):
            return a
    return None

def download_and_extract(artifact, token, outdir):
    download_url = artifact.get("archive_download_url")
    r = requests.get(download_url, headers=headers(token), stream=True)
    r.raise_for_status()
    z = zipfile.ZipFile(io.BytesIO(r.content))
    z.extractall(outdir)
    return [os.path.join(outdir, f) for f in z.namelist()]

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--repo", required=True, help="owner/repo, e.g. myorg/myrepo")
    p.add_argument("--artifact-name", required=True, help="artifact name to find (e.g. feeds or feeds_adage)")
    p.add_argument("--run-id", help="optional workflow run id")
    p.add_argument("--outdir", default="tmp_artifacts", help="where to save extracted files")
    args = p.parse_args()

    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if not token:
        print("ERROR: set GITHUB_TOKEN environment variable with a token that can access the repo.")
        sys.exit(2)

    os.makedirs(args.outdir, exist_ok=True)
    print("Listing artifacts for repo", args.repo)
    art = find_artifact(args.repo, args.artifact_name, token, run_id=args.run_id)
    if not art:
        print("Artifact not found with name containing:", args.artifact_name)
        sys.exit(3)
    print("Found artifact:", art.get("name"), "id:", art.get("id"))
    print("Downloading and extracting to", args.outdir)
    files = download_and_extract(art, token, args.outdir)
    print("Extracted files:")
    for f in files:
        print(" -", f)
    print("\nNow you can open the extracted .xml files and paste here the relevant ones (adage.xml, digiday.xml).")

if __name__ == "__main__":
    main()
