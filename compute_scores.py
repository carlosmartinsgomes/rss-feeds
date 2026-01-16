#!/usr/bin/env python3
import os
import json
import argparse
from datetime import datetime
from pathlib import Path

import pandas as pd


DEFAULT_OUTDIR = os.environ.get("OUTDIR", "output")
DEFAULT_CONFIG = "targets.json"


def find_latest_run_summary(outdir: str):
    """
    Procura o diretório do dia mais recente (YYYY-MM-DD) dentro de outdir
    e, dentro dele, o subdiretório de run mais recente (timestamp),
    e devolve o caminho para run_summary.json.
    """
    base = Path(outdir)
    if not base.exists():
        raise FileNotFoundError(f"OUTDIR '{outdir}' não existe")

    # diretórios de dia: YYYY-MM-DD
    day_dirs = [d for d in base.iterdir() if d.is_dir()]
    if not day_dirs:
        raise FileNotFoundError("Nenhum diretório de dia encontrado em OUTDIR")

    # ordenar por nome (YYYY-MM-DD) e ficar com o mais recente
    day_dirs_sorted = sorted(day_dirs, key=lambda p: p.name)
    latest_day = day_dirs_sorted[-1]

    # dentro do dia, procurar subdiretórios de run (timestamp)
    run_dirs = [d for d in latest_day.iterdir() if d.is_dir()]
    if not run_dirs:
        raise FileNotFoundError(f"Nenhum run encontrado para {latest_day.name}")

    run_dirs_sorted = sorted(run_dirs, key=lambda p: p.name)
    latest_run = run_dirs_sorted[-1]

    summary_path = latest_run / "run_summary.json"
    if not summary_path.exists():
        raise FileNotFoundError(f"run_summary.json não encontrado em {latest_run}")

    return str(summary_path), latest_day.name, latest_run.name


def load_targets(config_path: str):
    if not os.path.exists(config_path):
        return {}
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)


def flatten_runs(summary_json_path: str):
    """
    Lê o run_summary.json (lista de agregados por página) e devolve
    uma lista de linhas "flattened", uma por run individual.
    """
    with open(summary_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    flat_rows = []
    for agg in data:
        domain = agg.get("domain")
        page_label = agg.get("page_label")
        geo = agg.get("geo")
        iteration = agg.get("iteration")
        for run in agg.get("runs", []):
            row = {
                "domain": domain,
                "page_label": page_label,
                "geo": geo,
                "iteration": iteration,
                "timestamp": run.get("timestamp"),
                "total_requests": run.get("total_requests"),
                "pubmatic_requests": run.get("pubmatic_requests"),
                "pubmatic_adtech_share": run.get("pubmatic_adtech_share"),
                "pub_bids": run.get("pub_bids"),
                "pub_wins": run.get("pub_wins"),
                "pub_win_rate": run.get("pub_win_rate"),
                "avg_bid_latency_ms": run.get("avg_bid_latency_ms"),
                "p95_bid_latency_ms": run.get("p95_bid_latency_ms"),
                "bidder_count_avg": run.get("bidder_count_avg"),
                "direct_wins": run.get("direct_wins"),
                "reseller_wins": run.get("reseller_wins"),
                "refresh_wins": run.get("refresh_wins"),
                "ssp_financials": run.get("ssp_financials"),
                "ssp_share_of_voice": run.get("ssp_share_of_voice"),
            }
            flat_rows.append(row)

    return flat_rows


def compute_pubmatic_vs_market_metrics(df: pd.DataFrame):
    """
    A partir de ssp_financials (dict por linha), extrai:
    - avg_cpm_pubmatic
    - avg_cpm_market
    - share_of_voice_pubmatic
    """
    def extract_cpm_pubmatic(s):
        if not isinstance(s, dict):
            return None
        pm = s.get("pubmatic")
        if isinstance(pm, dict):
            return pm.get("avg_cpm")
        return None

    def extract_cpm_market(s):
        if not isinstance(s, dict):
            return None
        # média dos outros SSPs (excluindo pubmatic)
        vals = []
        for k, v in s.items():
            if k == "pubmatic":
                continue
            if isinstance(v, dict) and v.get("avg_cpm") is not None:
                vals.append(v["avg_cpm"])
        if not vals:
            return None
        return sum(vals) / len(vals)

    def extract_sov_pubmatic(sov):
        if not isinstance(sov, dict):
            return None
        return sov.get("pubmatic")

    df["avg_cpm_pubmatic"] = df["ssp_financials"].apply(extract_cpm_pubmatic)
    df["avg_cpm_market"] = df["ssp_financials"].apply(extract_cpm_market)
    df["sov_pubmatic"] = df["ssp_share_of_voice"].apply(extract_sov_pubmatic)

    return df


def apply_scoring(df: pd.DataFrame, targets: dict):
    """
    Calcula scores por publisher:
    - share_delta: pubmatic_adtech_share vs média do dia
    - price_delta: avg_cpm_pubmatic vs avg_cpm_market
    - winrate_delta: pub_win_rate vs média do dia
    - score_publisher: combinação ponderada
    - score_weighted: score_publisher * weight_pct
    """
    # mapear weight_pct a partir do targets.json
    weights_map = {
        p["domain"]: float(p.get("weight_pct", 0.0)) / 100.0
        for p in targets.get("publishers", [])
        if "domain" in p
    }
    df["weight_pct"] = df["domain"].map(weights_map).fillna(0.0)

    # métricas base
    # share
    if "pubmatic_adtech_share" in df.columns:
        mean_share = df["pubmatic_adtech_share"].replace(0, pd.NA).dropna().mean()
    else:
        mean_share = None

    # win rate
    if "pub_win_rate" in df.columns:
        mean_winrate = df["pub_win_rate"].replace(0, pd.NA).dropna().mean()
    else:
        mean_winrate = None

    # deltas
    if mean_share and mean_share != 0:
        df["share_delta"] = df["pubmatic_adtech_share"] / mean_share - 1
    else:
        df["share_delta"] = 0.0

    # price_delta: PubMatic vs mercado
    def price_delta_row(row):
        pm = row.get("avg_cpm_pubmatic")
        mk = row.get("avg_cpm_market")
        try:
            if pm is None or mk is None or mk == 0:
                return 0.0
            return pm / mk - 1.0
        except Exception:
            return 0.0

    df["price_delta"] = df.apply(price_delta_row, axis=1)

    if mean_winrate and mean_winrate != 0:
        df["winrate_delta"] = df["pub_win_rate"] / mean_winrate - 1
    else:
        df["winrate_delta"] = 0.0

    # score por publisher
    df["score_publisher"] = (
        0.4 * df["share_delta"] +
        0.4 * df["price_delta"] +
        0.2 * df["winrate_delta"]
    )

    # score ponderado
    df["score_weighted"] = df["score_publisher"] * df["weight_pct"]

    # score global diário
    score_global = df["score_weighted"].sum()

    # linha global
    global_row = {
        "domain": "__GLOBAL_DAILY__",
        "pubmatic_adtech_share": None,
        "pub_win_rate": None,
        "avg_cpm_pubmatic": None,
        "avg_cpm_market": None,
        "sov_pubmatic": None,
        "weight_pct": 1.0,
        "share_delta": None,
        "price_delta": None,
        "winrate_delta": None,
        "score_publisher": None,
        "score_weighted": score_global,
    }

    df = pd.concat([df, pd.DataFrame([global_row])], ignore_index=True)
    return df, score_global


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--outdir",
        default=DEFAULT_OUTDIR,
        help="Diretório base onde o scan_page.py escreve (default: output)",
    )
    parser.add_argument(
        "--config",
        default=DEFAULT_CONFIG,
        help="Path para targets.json (default: targets.json)",
    )
    args = parser.parse_args()

    summary_path, day_str, run_ts = find_latest_run_summary(args.outdir)
    print(f"Usando summary: {summary_path} (dia={day_str}, run={run_ts})")

    targets = load_targets(args.config)
    flat_rows = flatten_runs(summary_path)
    if not flat_rows:
        raise RuntimeError("Nenhum run encontrado no summary para scoring")

    df = pd.DataFrame(flat_rows)

    # enriquecer com métricas PubMatic vs mercado
    df = compute_pubmatic_vs_market_metrics(df)

    # aplicar scoring
    df_scored, score_global = apply_scoring(df, targets)

    # escrever Excel de scoring
    scores_dir = Path(args.outdir) / day_str / run_ts
    scores_path = scores_dir / "scores_pubmatic_vs_market.xlsx"
    df_scored.to_excel(scores_path, index=False)

    # ---------------------------------------------------------
    # Guardar score global diário num histórico simples
    # ---------------------------------------------------------
    history_path = Path(args.outdir) / "scores_history.csv"
    today = day_str  # já vem em formato YYYY-MM-DD

    new_row = pd.DataFrame([{
        "date": today,
        "score_daily": score_global
    }])

    if history_path.exists():
        hist = pd.read_csv(history_path)
        hist = pd.concat([hist, new_row], ignore_index=True)
    else:
        hist = new_row

    hist.to_csv(history_path, index=False)

    # Criar flag para evitar duplicações (por dia)
    flag_path = Path(args.outdir) / day_str / "score_done.flag"
    with open(flag_path, "w") as f:
        f.write("done")

    print(f"Score global diário (ponderado): {score_global:.4f}")
    print(f"Scores escritos em: {scores_path}")
    print(f"Score diário registado em: {history_path}")
    print(f"Flag criado em: {flag_path}")



if __name__ == "__main__":
    main()
