import pandas as pd
import glob
import os

def load_daily_scores(artifacts_root):
    rows = []
    for path in glob.glob(os.path.join(artifacts_root, "**", "run_summary.xlsx"), recursive=True):
        df = pd.read_excel(path)
        global_row = df[df["domain"] == "__GLOBAL_DAILY__"].copy()
        if global_row.empty:
            continue
        # inferir data a partir do path ou do próprio ficheiro (se tiver timestamp)
        # aqui assumimos que tens uma coluna "timestamp" no summary
        ts = df["timestamp"].iloc[0]
        global_row["date"] = pd.to_datetime(ts).date()
        rows.append(global_row[["date", "score_weighted"]])
    return pd.concat(rows, ignore_index=True).sort_values("date")

def aggregate_periods(df):
    df = df.set_index("date").resample("D").mean()  # garantir diário
    out = {}
    out["daily_last"] = df["score_weighted"].iloc[-1]
    out["weekly"] = df["score_weighted"].last("7D").mean()
    out["biweekly"] = df["score_weighted"].last("14D").mean()
    out["monthly"] = df["score_weighted"].last("30D").mean()
    return out

if __name__ == "__main__":
    artifacts_root = "artifacts"  # ajusta ao teu path
    daily = load_daily_scores(artifacts_root)
    agg = aggregate_periods(daily)
    print(agg)
