# salvar como check_ads_txt.py
# Requisitos: python3, pip install requests
import requests
import csv
import sys
from concurrent.futures import ThreadPoolExecutor

DOMAINS_FILE = "domains.txt"   # um domínio por linha
OUTPUT_CSV = "ads_txt_pubmatic.csv"
TIMEOUT = 8
WORKERS = 20

def check_domain(domain):
    url = f"https://{domain}/ads.txt"
    try:
        r = requests.get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            return domain, False, r.status_code, ""
        body = r.text.lower()
        has_pubmatic = "pubmatic" in body
        return domain, has_pubmatic, 200, body[:2000].replace("\n"," ")
    except Exception as e:
        return domain, False, "err", str(e)

def main():
    with open(DOMAINS_FILE) as f:
        domains = [line.strip() for line in f if line.strip()]
    results = []
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        for domain, has, status, snippet in ex.map(check_domain, domains):
            results.append((domain, has, status, snippet))
            print(domain, "=>", has, status)
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as out:
        w = csv.writer(out)
        w.writerow(["domain","has_pubmatic","status","snippet"])
        w.writerows(results)
    print("Wrote", OUTPUT_CSV)

if __name__ == "__main__":
    main()
