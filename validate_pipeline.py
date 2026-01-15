import os
import json
import glob
import math
import statistics as stats
from datetime import datetime
from collections import defaultdict

import pandas as pd


ARTIFACTS_ROOT = "artifacts"
TARGETS_JSON = "targets.json"
SCORES_HISTORY_CSV = os.path.join(ARTIFACTS_ROOT, "scores_history.csv")


# ---------------------------------------------------------
# Utils
# ---------------------------------------------------------

def list_days():
    if not os.path.isdir(ARTIFACTS_ROOT):
        return []
    return sorted(
        d for d in os.listdir(ARTIFACTS_ROOT)
        if os.path.isdir(os.path.join(ARTIFACTS_ROOT, d))
    )


def safe_read_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        return {"__error__": str(e)}


def zscore_series(values):
    if len(values) < 5:
        return [0] * len(values)
    mean = stats.mean(values)
    stdev = stats.pstdev(values)
    if stdev == 0:
        return [0] * len(values)
    return [(v - mean) / stdev for v in values]


# ---------------------------------------------------------
# 1) Validador automático de HAR
# ---------------------------------------------------------

def validate_har_files():
    print("\n[HAR VALIDATION]")
    issues = []

    for day in list_days():
        day_root = os.path.join(ARTIFACTS_ROOT, day)
        har_files = glob.glob(os.path.join(day_root, "**", "*.har"), recursive=True)

        for har_path in har_files:
            size = os.path.getsize(har_path)
            if size < 5_000:  # heurística: HAR demasiado pequeno
                issues.append((day, har_path, f"HAR too small: {size} bytes"))

            data = safe_read_json(har_path)
            if "__error__" in data:
                issues.append((day, har_path, f"JSON error: {data['__error__']}"))
                continue

            if "log" not in data or "entries" not in data["log"]:
                issues.append((day, har_path, "Missing log/entries in HAR"))
                continue

            if len(data["log"]["entries"]) == 0:
                issues.append((day, har_path, "HAR has zero entries"))

    if not issues:
        print("OK: No critical HAR issues detected.")
    else:
        for day, path, msg in issues:
            print(f"- [{day}] {path}: {msg}")


# ---------------------------------------------------------
# 2) Validador automático de run_summary.xlsx
# ---------------------------------------------------------

def validate_run_summaries():
    print("\n[RUN_SUMMARY VALIDATION]")
    required_cols = {
        "domain",
        "requests",
        "pubmatic_requests",
        "pubmatic_impressions",
        "pubmatic_revenue",
        "market_revenue",
        "pubmatic_cpm",
        "market_cpm",
        "pubmatic_share",
        "pubmatic_win_rate",
    }

    issues = []

    for day in list_days():
        day_root = os.path.join(ARTIFACTS_ROOT, day)
        xlsx_files = glob.glob(os.path.join(day_root, "**", "run_summary.xlsx"), recursive=True)

        if not xlsx_files:
            issues.append((day, None, "Missing run_summary.xlsx"))
            continue

        for xlsx in xlsx_files:
            try:
                df = pd.read_excel(xlsx)
            except Exception as e:
                issues.append((day, xlsx, f"Failed to read: {e}"))
                continue

            missing = required_cols - set(df.columns)
            if missing:
                issues.append((day, xlsx, f"Missing columns: {sorted(missing)}"))

            # checks básicos
            if (df["requests"] <= 0).any():
                issues.append((day, xlsx, "Found requests <= 0"))
            if (df["pubmatic_cpm"] < 0).any() or (df["market_cpm"] < 0).any():
                issues.append((day, xlsx, "Negative CPM values"))
            if (df["pubmatic_share"] < 0).any() or (df["pubmatic_share"] > 1).any():
                issues.append((day, xlsx, "pubmatic_share outside [0,1]"))
            if (df["pubmatic_win_rate"] < 0).any() or (df["pubmatic_win_rate"] > 1).any():
                issues.append((day, xlsx, "pubmatic_win_rate outside [0,1]"))

    if not issues:
        print("OK: No critical run_summary issues detected.")
    else:
        for day, path, msg in issues:
            print(f"- [{day}] {path}: {msg}")


# ---------------------------------------------------------
# 3) Detetor de outliers no score diário
# ---------------------------------------------------------

def detect_daily_score_outliers():
    print("\n[DAILY SCORE OUTLIERS]")

    if not os.path.isfile(SCORES_HISTORY_CSV):
        print("No scores_history.csv found.")
        return

    df = pd.read_csv(SCORES_HISTORY_CSV)
    if "date" not in df.columns or "score_weighted" not in df.columns:
        print("scores_history.csv missing required columns.")
        return

    df = df.sort_values("date")
    scores = df["score_weighted"].tolist()
    zs = zscore_series(scores)

    df["zscore"] = zs
    outliers = df[df["zscore"].abs() >= 2.5]

    if outliers.empty:
        print("OK: No strong outliers detected (|z| >= 2.5).")
    else:
        print("Potential outliers (|z| >= 2.5):")
        for _, row in outliers.iterrows():
            print(f"- {row['date']}: score={row['score_weighted']:.3f}, z={row['zscore']:.2f}")


# ---------------------------------------------------------
# 4) Detetor de inconsistência entre páginas (por publisher)
# ---------------------------------------------------------

def detect_page_inconsistencies():
    print("\n[PAGE-LEVEL INCONSISTENCIES]")

    per_pub_metrics = defaultdict(list)

    for day in list_days():
        day_root = os.path.join(ARTIFACTS_ROOT, day)
        xlsx_files = glob.glob(os.path.join(day_root, "**", "run_summary.xlsx"), recursive=True)
        if not xlsx_files:
            continue

        for xlsx in xlsx_files:
            try:
                df = pd.read_excel(xlsx)
            except Exception:
                continue

            if not {"domain", "pubmatic_cpm", "market_cpm", "pubmatic_share"}.issubset(df.columns):
                continue

            for _, row in df.iterrows():
                domain = row["domain"]
                per_pub_metrics[domain].append(
                    {
                        "day": day,
                        "pubmatic_cpm": row["pubmatic_cpm"],
                        "market_cpm": row["market_cpm"],
                        "pubmatic_share": row["pubmatic_share"],
                    }
                )

    for domain, rows in per_pub_metrics.items():
        if len(rows) < 3:
            continue

        pm_cpms = [r["pubmatic_cpm"] for r in rows if r["pubmatic_cpm"] > 0]
        mk_cpms = [r["market_cpm"] for r in rows if r["market_cpm"] > 0]
        shares = [r["pubmatic_share"] for r in rows if 0 <= r["pubmatic_share"] <= 1]

        if len(pm_cpms) >= 3:
            z_pm = zscore_series(pm_cpms)
            if max(map(abs, z_pm)) > 3:
                print(f"- {domain}: high variance in pubmatic_cpm across pages/days")

        if len(mk_cpms) >= 3:
            z_mk = zscore_series(mk_cpms)
            if max(map(abs, z_mk)) > 3:
                print(f"- {domain}: high variance in market_cpm across pages/days")

        if len(shares) >= 3:
            z_sh = zscore_series(shares)
            if max(map(abs, z_sh)) > 3:
                print(f"- {domain}: high variance in pubmatic_share across pages/days")


# ---------------------------------------------------------
# 5) Detetor de inconsistência entre slots
# ---------------------------------------------------------

def detect_slot_inconsistencies():
    print("\n[SLOT-LEVEL INCONSISTENCIES]")

    # Assumindo que o run_summary.xlsx tem uma coluna "slot" ou "run_id"
    # Se não tiver, podes inferir pelo path ou adicionar no compute_scores.py.
    per_slot_scores = defaultdict(list)

    if not os.path.isfile(SCORES_HISTORY_CSV):
        print("No scores_history.csv found.")
        return

    df = pd.read_csv(SCORES_HISTORY_CSV)
    if not {"date", "slot", "score_weighted"}.issubset(df.columns):
        print("scores_history.csv missing 'slot' or 'score_weighted' for slot analysis.")
        return

    for _, row in df.iterrows():
        key = int(row["slot"])
        per_slot_scores[key].append(row["score_weighted"])

    for slot, values in per_slot_scores.items():
        if len(values) < 5:
            continue
        z = zscore_series(values)
        if max(map(abs, z)) > 2.5:
            print(f"- Slot {slot}: unstable score distribution (|z| > 2.5)")


# ---------------------------------------------------------
# 6) Validador de weight_pct trimestral
# ---------------------------------------------------------

def validate_weight_pct():
    print("\n[WEIGHT_PCT VALIDATION]")

    if not os.path.isfile(TARGETS_JSON):
        print("targets.json not found.")
        return

    with open(TARGETS_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    pubs = data.get("publishers", [])
    weights = [p.get("weight_pct", 0) for p in pubs]
    total = sum(weights)

    print(f"- Sum of weight_pct: {total:.2f}")
    if abs(total - 100) > 1:
        print("WARNING: weight_pct does not sum close to 100%.")
    else:
        print("OK: weight_pct sums approximately to 100%.")

    # opcional: detectar publishers com peso muito baixo/alto
    for p in pubs:
        w = p.get("weight_pct", 0)
        if w < 0.5:
            print(f"  * Very low weight_pct: {p['domain']} ({w:.2f}%)")
        if w > 40:
            print(f"  * Very high weight_pct: {p['domain']} ({w:.2f}%)")


# ---------------------------------------------------------
# 7) Validador de publishers com setups instáveis
# ---------------------------------------------------------

def detect_unstable_setups():
    print("\n[UNSTABLE PUBLISHER SETUPS]")

    per_pub_share = defaultdict(list)
    per_pub_win = defaultdict(list)

    for day in list_days():
        day_root = os.path.join(ARTIFACTS_ROOT, day)
        xlsx_files = glob.glob(os.path.join(day_root, "**", "run_summary.xlsx"), recursive=True)
        if not xlsx_files:
            continue

        for xlsx in xlsx_files:
            try:
                df = pd.read_excel(xlsx)
            except Exception:
                continue

            if not {"domain", "pubmatic_share", "pubmatic_win_rate"}.issubset(df.columns):
                continue

            for _, row in df.iterrows():
                domain = row["domain"]
                share = row["pubmatic_share"]
                win = row["pubmatic_win_rate"]
                if 0 <= share <= 1:
                    per_pub_share[domain].append(share)
                if 0 <= win <= 1:
                    per_pub_win[domain].append(win)

    for domain, shares in per_pub_share.items():
        if len(shares) < 5:
            continue
        z_sh = zscore_series(shares)
        if max(map(abs, z_sh)) > 3:
            print(f"- {domain}: unstable pubmatic_share over time (possible setup changes)")

    for domain, wins in per_pub_win.items():
        if len(wins) < 5:
            continue
        z_w = zscore_series(wins)
        if max(map(abs, z_w)) > 3:
            print(f"- {domain}: unstable pubmatic_win_rate over time (possible auction/setup changes)")


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    print("=== PIPELINE VALIDATION ===")
    validate_har_files()
    validate_run_summaries()
    detect_daily_score_outliers()
    detect_page_inconsistencies()
    detect_slot_inconsistencies()
    validate_weight_pct()
    detect_unstable_setups()
    print("\nDone.")

if __name__ == "__main__":
    main()
