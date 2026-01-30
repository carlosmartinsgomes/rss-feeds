#!/usr/bin/env python3
import argparse
import sys
import os
import json
from datetime import datetime
from collections import defaultdict

import requests
import pandas as pd

WAYBACK_TIMEMAP = "https://web.archive.org/web/timemap/json/{}"


# -------------------------
# Wayback timemap helpers
# -------------------------

def get_timemap_snapshots(url: str, timeout: int = 60):
    """
    Obtém todos os timestamps do Wayback via timemap (muito mais leve que CDX search).
    Retorna lista de strings 'YYYYMMDDhhmmss' ordenadas.
    """
    tm_url = WAYBACK_TIMEMAP.format(url)
    try:
        r = requests.get(tm_url, timeout=timeout)
    except Exception as e:
        print(f"[ERR] timemap request failed for {url}: {e}", flush=True)
        return []

    if r.status_code != 200:
        print(f"[WARN] timemap status {r.status_code} for {url}", flush=True)
        return []

    try:
        data = r.json()
    except Exception as e:
        print(f"[ERR] timemap JSON parse failed for {url}: {e}", flush=True)
        return []

    snaps = []
    # primeira linha é header, o resto são snapshots
    for row in data[1:]:
        # formato típico: [original_url, timestamp, ...]
        if len(row) < 2:
            continue
        ts = row[1]
        if isinstance(ts, str) and len(ts) >= 14:
            snaps.append(ts)

    snaps.sort()
    return snaps


def monthly_sampling(timestamps):
    """
    Escolhe 1 snapshot por mês (sampling mensal).
    timestamps: lista de strings 'YYYYMMDDhhmmss'
    devolve: dict {(ano, mes): timestamp_escolhido}
    """
    by_month = defaultdict(list)
    for ts in timestamps:
        year = int(ts[0:4])
        month = int(ts[4:6])
        by_month[(year, month)].append(ts)

    sampled = {}
    for ym, tss in by_month.items():
        tss.sort()
        # escolhe o snapshot mais recente do mês
        sampled[ym] = tss[-1]
    return sampled


def fetch_ads_txt_snapshot(url: str, timestamp: str, timeout: int = 60):
    """
    Vai buscar o ads.txt de um snapshot específico.
    """
    wb_url = f"https://web.archive.org/web/{timestamp}id_/{url}"
    try:
        r = requests.get(wb_url, timeout=timeout)
    except Exception as e:
        print(f"[ERR] fetch snapshot failed for {url} @ {timestamp}: {e}", flush=True)
        return None

    if r.status_code != 200:
        print(f"[WARN] snapshot status {r.status_code} for {url} @ {timestamp}", flush=True)
        return None

    return r.text


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
    Pipeline otimizado: sampling mensal via timemap + métrica PubMatic.
    Retorna lista de registos (dicts) para o domínio.
    """
    base_url = f"https://www.{domain}/ads.txt"
    print(f"[INFO] Domain {domain}", flush=True)
    print(f"[WAYBACK] Getting timemap for {base_url}", flush=True)

    timestamps = get_timemap_snapshots(base_url)
    if not timestamps:
        print(f"[WARN] No snapshots for {domain}", flush=True)
        return []

    # filtrar por intervalo de anos
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

    for (year, month), ts in sorted(monthly.items()):
        print(f"[WAYBACK] Fetching {domain} {year}-{month:02d} @ {ts}", flush=True)
        ads = fetch_ads_txt_snapshot(base_url, ts)
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
# Log handling (opcional)
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

    # Ler domínios
    print("[BOOT] Vou ler domains...", flush=True)
    domains = read_domains(args.domains_file)
    print(f"[BOOT] Li {len(domains)} domains", flush=True)

    # Carregar log (se existir)
    print("[BOOT] Vou carregar log...", flush=True)
    log_data = load_log(args.log_file)
    print("[BOOT] Log carregado", flush=True)

    all_rows = []

    for domain in domains:
        # se quiseres, podes usar o log para skipar domínios já processados
        try:
            history = analyze_domain(domain, args.start_year, args.end_year)
            all_rows.extend(history)
            # atualizar log
            log_data[domain] = {
                "last_run": datetime.utcnow().isoformat(),
                "entries": len(history),
            }
            save_log(args.log_file, log_data)
        except Exception as e:
            print(f"[ERR] Domain {domain} analysis error: {e}", flush=True)

    if not all_rows:
        print("[WARN] Nenhum dado recolhido. Nada para escrever no Excel.", flush=True)
        return

    # Converter para DataFrame e exportar
    df = pd.DataFrame(all_rows)
    df.sort_values(["domain", "year", "month"], inplace=True)

    print(f"[INFO] Writing report -> {args.out}", flush=True)
    df.to_excel(args.out, index=False)
    print(f"[INFO] Report written -> {args.out}", flush=True)


if __name__ == "__main__":
    main()
