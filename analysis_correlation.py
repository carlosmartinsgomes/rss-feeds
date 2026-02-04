#!/usr/bin/env python3
import pandas as pd

# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------

print("[CORR] Loading quarterly signals...")
signals = pd.read_excel("pubmatic_index.xlsx", sheet_name="signal_quarterly")

print("[CORR] Loading PubMatic earnings...")
earnings = pd.read_excel("data/dados_pubmatic.xlsx")

# ---------------------------------------------------------
# NORMALIZE QUARTER FORMAT
# ---------------------------------------------------------

signals["quarter"] = signals["quarter"].astype(str)
earnings["quarter"] = earnings["quarter"].astype(str)

# ---------------------------------------------------------
# MERGE DATASETS
# ---------------------------------------------------------

print("[CORR] Merging signals + earnings...")
df = pd.merge(signals, earnings, on="quarter", how="inner")

print(f"[CORR] Merged rows: {len(df)}")
print(df[["quarter"]])

# ---------------------------------------------------------
# CORRELATION ANALYSIS
# ---------------------------------------------------------

targets = [
    "rev_yoy",
    "guide_yoy_next",
    "rev_surprise",
    "guide_surprise",
    "stock_reaction",
]

signals_to_test = [
    "pub_share_mean_q",
    "comp_share_mean_q",
    "enter_pct_q",
    "exit_pct_q",
    "outperformance_score_q",
    "outperformance_score_q_yoy",
]

print("\n==============================")
print(" CORRELAÇÕES ENTRE SINAIS E EARNINGS ")
print("==============================\n")

for s in signals_to_test:
    print(f"\n--- Correlações para sinal: {s} ---")
    for t in targets:
        corr_value = df[s].corr(df[t])
        print(f"{s}  vs  {t}:   {corr_value:.4f}")

print("\n[CORR] Done.")
