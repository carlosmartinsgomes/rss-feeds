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

def get_timemap_snapshots(url: str, timeout: int = 20):
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


def fetch_ads_txt_snapshot(url: str, timestamp: str, timeout: int = 20):
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
