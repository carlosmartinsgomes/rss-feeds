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
    """
    Tenta obter timestamps via timemap (rápido mas instável).
    Timeout curto e poucas tentativas.
    Retorna lista de strings 'YYYYMMDDhhmmss' ou [].
    """
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
        except (ConnectionError, ReadTimeout) as e:
            print(f"[WAYBACK] Timemap timeout/conn (attempt {attempt+1}/2) for {url}: {e}", flush=True)
        except Exception as e:
            print(f"[ERR] timemap request failed for {url}: {e}", flush=True)
            return []
    return []


def get_cdx_snapshots(url: str, start_year: int, end_year: int, timeout: int = 10):
    """
    Fallback via CDX quando timemap falha.
    Retorna lista de timestamps 'YYYYMMDDhhmmss' ou [].
    """
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
    # CDX formato típico: [urlkey, timestamp, original, mimetype, statuscode, digest, length]
    for row in data:
        if not isinstance(row, list) or len(row) < 2:
            continue
        ts = row[1]
        if isinstance(ts, str) and len(ts) >= 14:
            snaps.append(ts)

    snaps = sorted(set(snaps))
    return snaps


def monthly_sampling(timestamps, max_candidates_per_month=3):
    """
    Agrupa timestamps por (ano, mês) e escolhe até max_candidates_per_month
    mais recentes por mês.
    """
    by_month = defaultdict(list)
    for ts in timestamps:
        year = int(ts[0:4])
        month = int(ts[4:6])
        by_month[(year, month)].append(ts)

    sampled = {}
    for ym, tss in by_month.items():
        tss_sorted = sorted(tss, reverse=True)
        sampled[ym] = tss_sorted[:max_candidates_per_month]
    return sampled


def fetch_ads_txt_snapshot(url: str, timestamp: str, timeout: int = 15):
    """
    Vai buscar o ads.txt de um snapshot específico.
    Retries curtos, proteção contra connection refused.
    """
    wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"

    for attempt in range(2):
        try:
            r = requests.get(wb_url, timeout=timeout)
            if r.status_code == 200:
                return r.text
            else:
                print(f"[WARN] snapshot status {r.status_code} for {url} @ {timestamp}", flush=True)
                return None
        except ConnectionError as e:
            print(f"[ERR] connection error for {url} @ {timestamp}: {e}", flush=True)
            return None
        except ReadTimeout as e:
            print(f"[ERR] snapshot timeout (attempt {attempt+1}/2) for {url} @ {timestamp}: {e}", flush=True)
            if attempt == 1:
                return None
            time.sleep(1.0)
        except Exception as e:
            print(f"[ERR] fetch snapshot failed (attempt {attempt+1}/2) for {url} @ {timestamp}: {e}", flush=True)
            if attempt == 1:
                return None
            time.sleep(1.0)

    return None


# -------------------------
# PubMatic scoring
# -------------------------

def compute_pubmatic_score(ads_txt: str):
    """
    Extrai métricas PubMatic de um ads.txt.
    Retorna dict com:
      - pubmatic_lines
      - total_lines
      - share (pubmatic_lines / total_lines)
    """
    if ads_txt is None:
        return {"pubmatic_lines": 0, "total_lines": 0, "share": 0.0}

    lines = [
        l.strip()
        for l in ads_txt.splitlines()
        if l.strip() and not l.strip().startswith("#")
    ]
    total = len(lines)
    pub_lines = [l for l in lines if "pubmatic.com" in l.lower()]
    pub_count = len(pub_lines)
    share = pub_count / total if total > 0 else 0.0

    return {
        "pubmatic_lines": pub_count,
        "total_lines": total,
        "share": share,
    }


# -------------------------
# Domain analysis
# -------------------------

def analyze_domain(domain: str, start_year: int, end_year: int):
    """
    Pipeline otimizado: sampling mensal via timemap + fallback CDX + métrica PubMatic.
    Retorna lista de registos (dicts) para o domínio.
    """
    print(f"[INFO] Domain {domain}", flush=True)

    # Variantes reduzidas (https apenas)
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
            print(f"[WAYBACK] Timemap failed/empty for {url}, trying CDX...", flush=True)
            ts = get_cdx_snapshots(url, start_year, end_year)
        if ts:
            base_url = url
            timestamps = ts
            print(f"[WAYBACK] Using variant {url} with {len(ts)} snapshots", flush=True)
            break

    if not timestamps:
        print(f"[WARN] No snapshots (timemap or CDX) for ANY variant of {domain}", flush=True)
        return []

    # filtrar por intervalo de anos (extra segurança)
    filtered = [
        ts for ts in timestamps
        if start_year <= int(ts[0:4]) <= end_year
    ]
    if not filtered:
        print(f"[WARN] No snapshots in range {start_year}-{end_year} for {domain}", flush=True)
        return []

    monthly = monthly_sampling(filtered)
    print(f"[INFO] {domain}: {len(monthly)} monthly snapshots in range", flush=True)

    history = []
    last_share = None

    for (year, month), ts_list in sorted(monthly.items()):
        ts = None
        ads = None

        for candidate_ts in ts_list:
            print(f"[WAYBACK] Trying {domain} {year}-{month:02d} @ {candidate_ts}", flush=True)
            ads = fetch_ads_txt_snapshot(base_url, candidate_ts)
            time.sleep(0.4)  # cooldown entre snapshots
            if ads:
                ts = candidate_ts
                break

        if ts is None or ads is None:
            print(f"[WARN] No valid snapshot for {domain} {year}-{month:02d}", flush=True)
            continue

        score = compute_pubmatic_score(ads)
        changed = (last_share is None) or (score["share"] != last_share)

        history.append({
            "domain": domain,
            "year": year,
            "month": month,
            "timestamp": ts,
            "pubmatic_share": score["share"],
            "pubmatic_lines": score["pubmatic_lines"],
            "total_lines": score["total_lines"],
            "changed_vs_prev": changed,
        })

        last_share = score["share"]

    return history


# -------------------------
# Log handling (mantido mas neutro)
# -------------------------

def load_log(log_file):
    if not log_file or not os.path.exists(log_file):
        return {}
    try:
        with open(log_file, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception:
        return {}


def save_log(log_file, data):
    if not log_file:
        return
    try:
        with open(log_file, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"[WARN] Failed to save log {log_file}: {e}", flush=True)


# -------------------------
# Main
# -------------------------

def parse_args():
    p = argparse.ArgumentParser(description="Wayback SPO Analyzer (sampling mensal, PubMatic focus)")
    p.add_argument(
        "--domains-file",
        required=True,
        help="Ficheiro com lista de domínios (um por linha, sem http/https)."
    )
    p.add_argument(
        "--log-file",
        default=None,
        help="Ficheiro JSON para log de progresso (opcional)."
    )
    p.add_argument(
        "--out",
        required=True,
        help="Ficheiro Excel de saída (ex: wayback_output.xlsx)."
    )
    p.add_argument(
        "--start-year",
        type=int,
        default=2020,
        help="Ano inicial para análise (default: 2020)."
    )
    p.add_argument(
        "--end-year",
        type=int,
        default=datetime.utcnow().year,
        help="Ano final para análise (default: ano atual)."
    )
    return p.parse_args()


def read_domains(domains_file):
    with open(domains_file, "r", encoding="utf-8") as fh:
        lines = [l.strip() for l in fh if l.strip()]
    return lines


def main():
    args = parse_args()

    print("[BOOT] Script started", flush=True)
    print(f"[BOOT] args.domains_file = {args.domains_file}", flush=True)
    print(f"[BOOT] args.log_file = {args.log_file}", flush=True)
    print(f"[BOOT] args.out = {args.out}", flush=True)
    print(f"[BOOT] start_year={args.start_year} end_year={args.end_year}", flush=True)

    print("[BOOT] Vou ler domains...", flush=True)
    domains = read_domains(args.domains_file)
    print(f"[BOOT] Li {len(domains)} domains", flush=True)

    print("[BOOT] Vou carregar log...", flush=True)
    log_data = load_log(args.log_file)
    print("[BOOT] Log carregado", flush=True)

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
            save_log(args.log_file, log_data)
        except Exception as e:
            print(f"[ERR] Domain {domain} analysis error: {e}", flush=True)
        time.sleep(1.5)  # cooldown entre domínios

    if not all_rows:
        print("[WARN] Nenhum dado recolhido. Nada para escrever no Excel.", flush=True)
        return

    df = pd.DataFrame(all_rows)
    df.sort_values(["domain", "year", "month"], inplace=True)

    print(f"[INFO] Writing report -> {args.out}", flush=True)
    df.to_excel(args.out, index=False)
    print(f"[INFO] Report written -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
