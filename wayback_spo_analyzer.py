#!/usr/bin/env python3
import argparse
import sys
import os
import json
from datetime import datetime
from collections import defaultdict

import requests
import pandas as pd
import time
from requests.exceptions import ConnectionError


WAYBACK_TIMEMAP = "https://web.archive.org/web/timemap/json/{}"


# -------------------------
# Wayback timemap helpers
# -------------------------

def get_timemap_snapshots(url: str, timeout: int = 60):
    tm_url = WAYBACK_TIMEMAP.format(url)

    for attempt in range(3):
        try:
            r = requests.get(tm_url, timeout=timeout)
            if r.status_code == 200:
                break
            else:
                print(f"[WARN] timemap status {r.status_code} for {url}", flush=True)
                return []
        except Exception as e:
            print(f"[WAYBACK] Timemap timeout (attempt {attempt+1}/3) for {url}", flush=True)
            if attempt == 2:
                return []
            continue

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



def monthly_sampling(timestamps, max_candidates_per_month=3):
    by_month = defaultdict(list)
    for ts in timestamps:
        year = int(ts[0:4])
        month = int(ts[4:6])
        by_month[(year, month)].append(ts)

    sampled = {}
    for ym, tss in by_month.items():
        # ordenar do mais recente para o mais antigo
        tss_sorted = sorted(tss, reverse=True)
        # limitar o número de snapshots testados por mês
        sampled[ym] = tss_sorted[:max_candidates_per_month]
    return sampled

def weekly_sampling(timestamps, max_candidates_per_week=2):
    """
    Agrupa snapshots por semana dentro de um mês.
    Exemplo: Janeiro 2025 → semanas 1,2,3,4,5.
    """
    by_week = defaultdict(list)

    for ts in timestamps:
        y = int(ts[0:4])
        m = int(ts[4:6])
        d = int(ts[6:8])

        # calcular número da semana dentro do mês
        # semana 1 = dias 1–7, semana 2 = dias 8–14, etc.
        week = (d - 1) // 7 + 1

        by_week[(y, m, week)].append(ts)

    sampled = {}
    for key, tss in by_week.items():
        tss_sorted = sorted(tss, reverse=True)
        sampled[key] = tss_sorted[:max_candidates_per_week]

    return sampled


def fetch_ads_txt_snapshot(url: str, timestamp: str, timeout: int = 15):
    # Tentar 3 variantes do snapshot (id_, normal, if_)
    snapshot_variants = [
        f"https://web.archive.org/web/{timestamp}id_/{url}",
        f"https://web.archive.org/web/{timestamp}/{url}",
        f"https://web.archive.org/web/{timestamp}if_/{url}",
    ]

    for attempt in range(3):
        for wb_url in snapshot_variants:
            try:
                r = requests.get(wb_url, timeout=timeout)
                if r.status_code != 200:
                    continue

                # DECODIFICAÇÃO ROBUSTA
                try:
                    text = r.content.decode("utf-8", errors="replace")
                except:
                    try:
                        text = r.content.decode("latin-1", errors="replace")
                    except:
                        continue

                lower = text.lower()

                # DETEÇÃO DE HTML
                if "<html" in lower or "<body" in lower or "<!doctype" in lower:
                    continue

                # DETEÇÃO DE ERROS DO WAYBACK
                error_signatures = [
                    "memento not found",
                    "resource not in archive",
                    "does not have an archive",
                    "wayback machine doesn't have",
                    "robots.txt",
                    "blocked",
                    "file not found",
                    "not found",
                    "redirecting",
                    "<meta http-equiv",
                    "refresh content",
                ]
                if any(sig in lower for sig in error_signatures):
                    continue

                # TEM DE TER PELO MENOS UMA VÍRGULA
                if "," not in text:
                    continue

                # TEM DE TER PELO MENOS UM SSP CONHECIDO
                valid_ssps = [
                    "pubmatic.com",
                    "rubiconproject.com",
                    "openx.com",
                    "indexexchange.com",
                    "appnexus.com",
                    "xandr.com",
                    "triplelift.com",
                    "sharethrough.com",
                    "sovrn.com",
                    "adform.com",
                ]
                if not any(ssp in lower for ssp in valid_ssps):
                    continue

                return text

            except Exception:
                time.sleep(1)
                continue

    return None


# -------------------------
# PubMatic scoring
# -------------------------

def compute_pubmatic_score(ads_txt: str):
    """
    Extrai métricas financeiras relevantes do ads.txt:
      - PubMatic DIRECT / RESELLER
      - Concorrentes principais (DIRECT / RESELLER)
      - Totais por SSP
      - Shares financeiros
    """
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

    # limpar e ignorar comentários
    lines = [
        l.strip()
        for l in ads_txt.splitlines()
        if l.strip() and not l.strip().startswith("#")
    ]
    

    # Mapa de concorrentes → SSP
    competitor_map = {
        "rubiconproject.com": "magnite",
        "magnite.com": "magnite",
        "telaria.com": "magnite",
        "spotx.tv": "magnite",
        "spotxchange.com": "magnite",

        "openx.com": "openx",

        "indexexchange.com": "index",
        "casalemedia.com": "index",

        "appnexus.com": "xandr",
        "xandr.com": "xandr",

        "triplelift.com": "triplelift",

        "sharethrough.com": "sharethrough",

        "sovrn.com": "sovrn",
        "lijit.com": "sovrn",

        "adform.com": "adform",
    }

    # Inicializar contadores detalhados
    competitors_detail = {
        f"{ssp}_direct": 0
        for ssp in ["magnite", "openx", "index", "xandr", "triplelift", "sharethrough", "sovrn", "adform"]
    }
    competitors_detail.update({
        f"{ssp}_reseller": 0
        for ssp in ["magnite", "openx", "index", "xandr", "triplelift", "sharethrough", "sovrn", "adform"]
    })

    pub_direct = 0
    pub_reseller = 0
    competitors_total = 0

    # ----------------------------------------
    # REMOVER COMENTÁRIOS PARCIAIS E DUPLICADOS
    # ----------------------------------------
    clean_lines = []
    seen = set()
    
    for l in lines:
        # remover comentários no fim da linha
        base = l.split("#")[0].strip()
        if not base:
            continue
    
        # evitar duplicados
        if base not in seen:
            seen.add(base)
            clean_lines.append(base)
    
    lines = clean_lines
    
    # ----------------------------------------
    # NORMALIZAR FORMATAÇÃO ESTRANHA
    # ----------------------------------------
    normalized = []
    for l in lines:
        # substituir separadores estranhos por vírgulas
        l = l.replace(";", ",").replace(":", ",").replace(" - ", ",").replace(" ", ",")
        # remover vírgulas duplicadas
        while ",," in l:
            l = l.replace(",,", ",")
        normalized.append(l)
    
    lines = normalized

    total = len(lines)

    # ----------------------------------------
    # LOOP PRINCIPAL DE PARSING
    # ----------------------------------------
    for l in lines:
        ll = l.lower()


        # PubMatic
        if "pubmatic.com" in ll:
            if "direct" in ll:
                pub_direct += 1
            elif "reseller" in ll:
                pub_reseller += 1
            else:
                pub_reseller += 1
            continue

        # Concorrentes detalhados
        for domain, ssp in competitor_map.items():
            if domain in ll:
                competitors_total += 1
                if "direct" in ll:
                    competitors_detail[f"{ssp}_direct"] += 1
                elif "reseller" in ll:
                    competitors_detail[f"{ssp}_reseller"] += 1
                else:
                    competitors_detail[f"{ssp}_reseller"] += 1
                break

    pub_total = pub_direct + pub_reseller

    return {
        "pubmatic_direct": pub_direct,
        "pubmatic_reseller": pub_reseller,
        "pubmatic_total": pub_total,

        "competitors": competitors_total,
        "total_lines": total,

        "pubmatic_direct_share": pub_direct / total if total > 0 else 0.0,
        "pubmatic_total_share": pub_total / total if total > 0 else 0.0,
        "competitors_share": competitors_total / total if total > 0 else 0.0,

        # adicionar métricas detalhadas
        **competitors_detail,
    }


# -------------------------
# Domain analysis
# -------------------------

def analyze_domain(domain: str, start_year: int, start_month: int, end_year: int, end_month: int):
    """
    Pipeline otimizado: sampling mensal via timemap + métrica PubMatic.
    Retorna lista de registos (dicts) para o domínio.
    """
    print(f"[INFO] Domain {domain}", flush=True)

    # Tentar variantes de URL
    variants = [
        f"https://www.{domain}/ads.txt",
        f"https://{domain}/ads.txt",
        f"http://www.{domain}/ads.txt",
        f"http://{domain}/ads.txt",
    ]
    
    base_url = None
    timestamps = []
    
    for url in variants:
        print(f"[WAYBACK] Trying variant {url}", flush=True)
        ts = get_timemap_snapshots(url)
        if ts:
            base_url = url
            timestamps = ts
            print(f"[WAYBACK] Using variant {url}", flush=True)
            break
    
    if not timestamps:
        print(f"[WARN] No snapshots for ANY variant of {domain}", flush=True)
        return []


    # filtrar por intervalo de anos
    filtered = []
    for ts in timestamps:
        y = int(ts[0:4])
        m = int(ts[4:6])
    
        if (y > start_year or (y == start_year and m >= start_month)) and \
           (y < end_year   or (y == end_year   and m <= end_month)):
            filtered.append(ts)

    if not filtered:
        print(f"[WARN] No snapshots in range {start_year}-{end_year} for {domain}", flush=True)
        return []

    # Janeiro 2025 e Janeiro 2026 → análise semanal
    if (start_year == 2025 and start_month == 1) or (start_year == 2026 and start_month == 1):
        sampled = weekly_sampling(filtered)
        print(f"[INFO] {domain}: {len(sampled)} weekly snapshots in range", flush=True)
    else:
        sampled = monthly_sampling(filtered)
        print(f"[INFO] {domain}: {len(sampled)} monthly snapshots in range", flush=True)


    history = []
    last_share = None

    for key, ts_list in sorted(sampled.items()):
        year = key[0]
        month = key[1]
        # se for semanal, key = (year, month, week)
        week = key[2] if len(key) == 3 else None

        ts = None
        ads = None
        time.sleep(1)
    
        # tentar snapshots do mês até encontrar um válido
        for candidate_ts in ts_list:
            print(f"[WAYBACK] Trying {domain} {year}-{month:02d} @ {candidate_ts}", flush=True)
            ads = fetch_ads_txt_snapshot(base_url, candidate_ts)
            time.sleep(0.5)
            if ads:
                ts = candidate_ts
                break
    
        if ts is None or ads is None:
            print(f"[WARN] No valid snapshot for {domain} {year}-{month:02d}", flush=True)
            continue
    
        # já temos ads válido, não é preciso novo fetch
        score = compute_pubmatic_score(ads)


        changed = (last_share is None) or (score["pubmatic_total_share"] != last_share)


        history.append({
            "domain": domain,
            "year": year,
            "month": month,
            "week": week,
            "timestamp": ts,
        
            # PubMatic
            "pubmatic_direct": score["pubmatic_direct"],
            "pubmatic_reseller": score["pubmatic_reseller"],
            "pubmatic_total": score["pubmatic_total"],
            "pubmatic_direct_share": score["pubmatic_direct_share"],
            "pubmatic_total_share": score["pubmatic_total_share"],
        
            # Concorrência agregada
            "competitors": score["competitors"],
            "competitors_share": score["competitors_share"],
        
            # Concorrência detalhada
            "magnite_direct": score["magnite_direct"],
            "magnite_reseller": score["magnite_reseller"],
            "openx_direct": score["openx_direct"],
            "openx_reseller": score["openx_reseller"],
            "index_direct": score["index_direct"],
            "index_reseller": score["index_reseller"],
            "xandr_direct": score["xandr_direct"],
            "xandr_reseller": score["xandr_reseller"],
            "triplelift_direct": score["triplelift_direct"],
            "triplelift_reseller": score["triplelift_reseller"],
            "sharethrough_direct": score["sharethrough_direct"],
            "sharethrough_reseller": score["sharethrough_reseller"],
            "sovrn_direct": score["sovrn_direct"],
            "sovrn_reseller": score["sovrn_reseller"],
            "adform_direct": score["adform_direct"],
            "adform_reseller": score["adform_reseller"],
        
            # Total
            "total_lines": score["total_lines"],
        
            "changed_vs_prev": changed,
        })



        last_share = score["pubmatic_total_share"]


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

    p.add_argument("--domains-file", required=True)
    p.add_argument("--log-file", default=None)
    p.add_argument("--out", required=True)

    p.add_argument("--start-year", type=int, default=2023)
    p.add_argument("--start-month", type=int, default=1)
    
    p.add_argument("--end-year", type=int, default=2026)
    p.add_argument("--end-month", type=int, default=1)


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
            history = analyze_domain(
                domain,
                args.start_year,
                args.start_month,
                args.end_year,
                args.end_month
            )

            all_rows.extend(history)
            # atualizar log
            log_data[domain] = {
                "last_run": datetime.utcnow().isoformat(),
                "entries": len(history),
            }
            save_log(args.log_file, log_data)
        except Exception as e:
            print(f"[ERR] Domain {domain} analysis error: {e}", flush=True)
    
        # ⭐ COOLDOWN ENTRE DOMÍNIOS — colocar AQUI
        time.sleep(2)
    
    
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
