#!/usr/bin/env python3
import argparse
import sys
import os
import json
from datetime import datetime
from collections import defaultdict
import time

import requests
import pandas as pd
from requests.exceptions import ConnectionError, ReadTimeout

WAYBACK_TIMEMAP = "https://web.archive.org/web/timemap/json/{}"
WAYBACK_CDX = "https://web.archive.org/cdx/search/cdx"


# -------------------------
# Wayback helpers (timemap + CDX)
# -------------------------

def get_timemap_snapshots(url: str, timeout: int = 8):
    """Tenta obter timestamps via timemap (rápido mas instável)."""
    tm_url = WAYBACK_TIMEMAP.format(url)

    for attempt in range(2):
        try:
            r = requests.get(tm_url, timeout=timeout)
            if r.status_code != 200:
                print(f"[WARN] timemap status {r.status_code} for {url}", flush=True)
                return []
            try:
                data = r.json()
            except Exception as e:
                print(f"[ERR] timemap JSON parse failed for {url}: {e}", flush=True)
                return []
            snaps = []
            for row in data[1:]:
                if len(row) < 2:
                    continue
                ts = row[1]
                if isinstance(ts, str) and len(ts) >= 14:
                    snaps.append(ts)
            snaps.sort()
            return snaps
        except Exception as e:
            print(f"[WAYBACK] Timemap timeout/conn (attempt {attempt+1}/2) for {url}: {e}", flush=True)

    return []


def get_cdx_snapshots(url: str, start_year: int, end_year: int, timeout: int = 10):
    """Fallback via CDX quando timemap falha."""
    params = {
        "url": url,
        "output": "json",
        "filter": "statuscode:200",
        "from": str(start_year),
        "to": str(end_year),
        "limit": "5000",
    }
    try:
        r = requests.get(WAYBACK_CDX, params=params, timeout=timeout)
    except Exception as e:
        print(f"[ERR] CDX request failed for {url}: {e}", flush=True)
        return []

    if r.status_code != 200:
        print(f"[WARN] CDX status {r.status_code} for {url}", flush=True)
        return []

    try:
        data = r.json()
    except Exception as e:
        print(f"[ERR] CDX JSON parse failed for {url}: {e}", flush=True)
        return []

    snaps = []
    for row in data:
        if not isinstance(row, list) or len(row) < 2:
            continue
        ts = row[1]
        if isinstance(ts, str) and len(ts) >= 14:
            snaps.append(ts)

    return sorted(set(snaps))


def monthly_sampling(timestamps, max_candidates_per_month=3):
    """Agrupa timestamps por mês e escolhe até 3 mais recentes."""
    by_month = defaultdict(list)
    for ts in timestamps:
        year = int(ts[0:4])
        month = int(ts[4:6])
        by_month[(year, month)].append(ts)

    sampled = {}
    for ym, tss in by_month.items():
        sampled[ym] = sorted(tss, reverse=True)[:max_candidates_per_month]
    return sampled


def fetch_ads_txt_snapshot(url: str, timestamp: str, timeout: int = 12):
    """Vai buscar o ads.txt de um snapshot específico."""
    wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"

    for attempt in range(2):
        try:
            r = requests.get(wb_url, timeout=timeout)
            if r.status_code == 200:
                text = r.text.strip()
                if len(text) < 5:
                    print(f"[WARN] snapshot too small (suspect) for {url} @ {timestamp}", flush=True)
                    return None
                return text
            else:
                print(f"[WARN] snapshot status {r.status_code} for {url} @ {timestamp}", flush=True)
                return None

        except Exception as e:
            print(f"[ERR] snapshot fetch error (attempt {attempt+1}/2) for {url} @ {timestamp}: {e}", flush=True)
            time.sleep(1.0)

    return None


# -------------------------
# PubMatic + concorrência scoring
# -------------------------

def compute_pubmatic_score(ads_txt: str):
    """Extrai métricas financeiras relevantes do ads.txt."""
    if ads_txt is None:
        return {
            "pubmatic_direct": 0,
            "pubmatic_reseller": 0,
            "pubmatic_total": 0,
            "competitors": 0,
            "total_lines": 0,
            "pubmatic_direct_share": 0.0,
            "pubmatic_total_share": 0.0,
            "competitors_share": 0.0,
        }

    lines = [
        l.strip()
        for l in ads_txt.splitlines()
        if l.strip() and not l.strip().startswith("#")
    ]
    total = len(lines)

    competitors_domains = [
        "rubiconproject.com", "magnite.com", "telaria.com", "spotx.tv", "spotxchange.com",
        "openx.com",
        "indexexchange.com", "casalemedia.com",
        "appnexus.com", "xandr.com",
        "triplelift.com",
        "sharethrough.com",
        "sovrn.com", "lijit.com",
        "adform.com"
    ]

    pub_direct = 0
    pub_reseller = 0
    competitors = 0

    for l in lines:
        ll = l.lower()

        if "pubmatic.com" in ll:
            if "direct" in ll:
                pub_direct += 1
            elif "reseller" in ll:
                pub_reseller += 1
            else:
                pub_reseller += 1
            continue

        if any(c in ll for c in competitors_domains):
            competitors += 1

    pub_total = pub_direct + pub_reseller

    return {
        "pubmatic_direct": pub_direct,
        "pubmatic_reseller": pub_reseller,
        "pubmatic_total": pub_total,
        "competitors": competitors,
        "total_lines": total,
        "pubmatic_direct_share": pub_direct / total if total > 0 else 0.0,
        "pubmatic_total_share": pub_total / total if total > 0 else 0.0,
        "competitors_share": competitors / total if total > 0 else 0.0,
    }


# -------------------------
# Domain analysis
# -------------------------

def analyze_domain(domain: str, start_year: int, end_year: int):
    print(f"[INFO] Domain {domain}", flush=True)

    variants = [
        f"https://www.{domain}/ads.txt",
        f"https://{domain}/ads.txt",
    ]

    base_url = None
    timestamps = []

    for url in variants:
        print(f"[WAYBACK] Trying timemap for {url}", flush=True)
        ts = get_timemap_snapshots(url)
        if not ts:
            print(f"[WAYBACK] Timemap empty, trying CDX for {url}", flush=True)
            ts = get_cdx_snapshots(url, start_year, end_year)
        if ts:
            base_url = url
            timestamps = ts
            print(f"[WAYBACK] Using variant {url} with {len(ts)} snapshots", flush=True)
            break

    if not timestamps:
        print(f"[WARN] No snapshots for ANY variant of {domain}", flush=True)
        return []

    filtered = [ts for ts in timestamps if start_year <= int(ts[0:4]) <= end_year]
    if not filtered:
        print(f"[WARN] No snapshots in range for {domain}", flush=True)
        return []

    monthly = monthly_sampling(filtered)
    print(f"[INFO] {domain}: {len(monthly)} monthly snapshots", flush=True)

    history = []
    last_share = None

    for (year, month), ts_list in sorted(monthly.items()):
        ts = None
        ads = None

        for candidate_ts in ts_list:
            print(f"[WAYBACK] Trying {domain} {year}-{month:02d} @ {candidate_ts}", flush=True)
            ads = fetch_ads_txt_snapshot(base_url, candidate_ts)
            time.sleep(0.4)
            if ads:
                ts = candidate_ts
                break

        if ts is None:
            print(f"[WARN] No valid snapshot for {domain} {year}-{month:02d}", flush=True)
            continue

        score = compute_pubmatic_score(ads)
        changed = (last_share is None) or (score["pubmatic_total_share"] != last_share)

        history.append({
            "domain": domain,
            "year": year,
            "month": month,
            "timestamp": ts,
            "pubmatic_direct": score["pubmatic_direct"],
            "pubmatic_reseller": score["pubmatic_reseller"],
            "pubmatic_total": score["pubmatic_total"],
            "pubmatic_direct_share": score["pubmatic_direct_share"],
            "pubmatic_total_share": score["pubmatic_total_share"],
            "competitors": score["competitors"],
            "competitors_share": score["competitors_share"],
            "total_lines": score["total_lines"],
            "changed_vs_prev": changed,
        })

        last_share = score["pubmatic_total_share"]

    return history


# -------------------------
# Main
# -------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Wayback SPO Analyzer (ultra-robusto)")
    p.add_argument("--domains-file", required=True)
    p.add_argument("--log-file", default=None)
    p.add_argument("--out", required=True)
    p.add_argument("--start-year", type=int, default=2020)
    p.add_argument("--end-year", type=int, default=datetime.utcnow().year)
    return p.parse_args()


def read_domains(domains_file):
    with open(domains_file, "r", encoding="utf-8") as fh:
        return [l.strip() for l in fh if l.strip()]


def main():
    args = parse_args()

    print("[BOOT] Script started", flush=True)
    domains = read_domains(args.domains_file)
    print(f"[BOOT] Loaded {len(domains)} domains", flush=True)

    log_data = {}
    if args.log_file and os.path.exists(args.log_file):
        try:
            with open(args.log_file, "r", encoding="utf-8") as fh:
                log_data = json.load(fh)
        except:
            log_data = {}

    all_rows = []

    for idx, domain in enumerate(domains, start=1):
        print(f"[BOOT] ---- Domain {idx}/{len(domains)}: {domain} ----", flush=True)
        try:
            history = analyze_domain(domain, args.start_year, args.end_year)
            all_rows.extend(history)
            log_data[domain] = {
                "last_run": datetime.utcnow().isoformat(),
                "entries": len(history),
            }
            if args.log_file:
                with open(args.log_file, "w", encoding="utf-8") as fh:
                    json.dump(log_data, fh, indent=2)
        except Exception as e:
            print(f"[ERR] Domain {domain} analysis error: {e}", flush=True)

        time.sleep(1.5)

    if not all_rows:
        print("[WARN] No data collected", flush=True)
        return

    df = pd.DataFrame(all_rows)
    df.sort_values(["domain", "year", "month"], inplace=True)

    print(f"[INFO] Writing report -> {args.out}", flush=True)
    df.to_excel(args.out, index=False)
    print(f"[INFO] Report written -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
