# .github/scripts/download_prev_artifact.py
import os
import sys
import requests
import zipfile
import io

"""
Usage: python download_prev_artifact.py <artifact_name>
Will attempt to download the most recent artifact with that name and
extract any .xlsx file into the current working dir as prev_feeds_summary.xlsx
"""

def main():
    if len(sys.argv) < 2:
        print("Usage: download_prev_artifact.py <artifact_name>")
        return 1
    artifact_name = sys.argv[1]
    repo = os.environ.get("GITHUB_REPOSITORY")
    token = os.environ.get("GITHUB_TOKEN")
    if not repo or not token:
        print("GITHUB_REPOSITORY or GITHUB_TOKEN missing, skipping download.")
        return 0

    api = f"https://api.github.com/repos/{repo}/actions/artifacts?per_page=100"
    headers = {"Authorization": f"token {token}", "Accept": "application/json"}
    r = requests.get(api, headers=headers, timeout=30)
    if r.status_code != 200:
        print("Failed to list artifacts:", r.status_code, r.text)
        return 0
    data = r.json()
    artifacts = data.get("artifacts", []) or []
    # filter by name and not expired
    candidates = [a for a in artifacts if a.get("name") == artifact_name and not a.get("expired", False)]
    if not candidates:
        print("No previous artifact found with name:", artifact_name)
        return 0
    # choose most recent by created_at
    candidates.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    chosen = candidates[0]
    download_url = chosen.get("archive_download_url")
    if not download_url:
        print("No archive_download_url for chosen artifact")
        return 0

    print(f"Downloading artifact id={chosen.get('id')} created_at={chosen.get('created_at')}")
    # download zip
    dl = requests.get(download_url, headers=headers, stream=True, timeout=60)
    if dl.status_code not in (200, 302):
        print("Failed to download artifact:", dl.status_code, dl.text)
        return 0
    # GitHub returns a redirect for the actual zip; handle by streaming content
    z = zipfile.ZipFile(io.BytesIO(dl.content))
    extracted = False
    for name in z.namelist():
        if name.lower().endswith(".xlsx"):
            print("Extracting", name, "-> prev_feeds_summary.xlsx")
            with z.open(name) as fh, open("prev_feeds_summary.xlsx", "wb") as out:
                out.write(fh.read())
            extracted = True
            break
    if not extracted:
        print("No .xlsx found inside artifact zip; skipping.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
