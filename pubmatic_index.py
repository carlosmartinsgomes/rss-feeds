#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


INPUT_FILE = "data/wayback_output.xlsx"
OUTPUT_FILE = "pubmatic_index.xlsx"


# ---------------------------------------------------------
# LOAD DATA
# ---------------------------------------------------------

def load_wayback_data(path: str = INPUT_FILE) -> pd.DataFrame:
    df = pd.read_excel(path)

    required = [
        "domain",
        "year",
        "month",
        "pubmatic_total_share",
        "competitors_share",
        "total_lines",
        "pubmatic_total",
        "competitors",
    ]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column in wayback_output: {col}")

    if "week" not in df.columns:
        df["week"] = None

    return df


# ---------------------------------------------------------
# MONTHLY INDEX
# ---------------------------------------------------------

def build_monthly_index(df: pd.DataFrame) -> pd.DataFrame:
    monthly = (
        df.groupby(["year", "month"], as_index=False)
        .agg(
            pub_index=("pubmatic_total_share", "mean"),
            pub_share_mean=("pubmatic_total_share", "mean"),
            comp_share_mean=("competitors_share", "mean"),
            domains=("domain", "nunique"),
            avg_total_lines=("total_lines", "mean"),
        )
    )

    monthly["date"] = pd.to_datetime(
        dict(year=monthly["year"], month=monthly["month"], day=1)
    )
    monthly = monthly.sort_values("date").reset_index(drop=True)

    monthly["quarter"] = monthly["date"].dt.to_period("Q").astype(str)

    monthly["pub_index_mom"] = monthly["pub_index"].diff()
    monthly["pub_index_yoy"] = monthly["pub_index"].diff(12)

    return monthly


# ---------------------------------------------------------
# QUARTERLY INDEX
# ---------------------------------------------------------

def build_quarterly_index(monthly: pd.DataFrame) -> pd.DataFrame:
    monthly = monthly.copy()
    monthly["year_quarter"] = monthly["date"].dt.to_period("Q")

    q = (
        monthly.groupby("year_quarter", as_index=False)
        .agg(
            pub_index_q=("pub_index", "mean"),
            pub_share_mean_q=("pub_share_mean", "mean"),
            comp_share_mean_q=("comp_share_mean", "mean"),
            domains_q=("domains", "mean"),
        )
    )

    q["year_quarter"] = q["year_quarter"].astype(str)
    q["order"] = range(len(q))
    q = q.sort_values("order").reset_index(drop=True)

    q["pub_index_qoq"] = q["pub_index_q"].diff()
    q["pub_index_q_yoy"] = q["pub_index_q"].diff(4)

    return q


# ---------------------------------------------------------
# WEEKLY INDEX (2025–2026)
# ---------------------------------------------------------

def build_weekly_january_index(df: pd.DataFrame) -> pd.DataFrame:
    jan = df[(df["month"] == 1) & (df["year"].isin([2025, 2026]))].copy()

    if jan["week"].isna().all():
        return pd.DataFrame()

    weekly = (
        jan.groupby(["year", "month", "week"], as_index=False)
        .agg(
            pub_index=("pubmatic_total_share", "mean"),
            pub_share_mean=("pubmatic_total_share", "mean"),
            comp_share_mean=("competitors_share", "mean"),
            domains=("domain", "nunique"),
            avg_total_lines=("total_lines", "mean"),
        )
    )

    weekly = weekly.sort_values(["week", "year"]).reset_index(drop=True)

    weekly["pub_index_yoy_week"] = None

    for w in sorted(weekly["week"].dropna().unique()):
        mask = weekly["week"] == w
        sub = weekly[mask].sort_values("year")
        if len(sub) >= 2:
            idx_2026 = sub[sub["year"] == 2026].index
            idx_2025 = sub[sub["year"] == 2025].index
            if len(idx_2026) == 1 and len(idx_2025) == 1:
                weekly.loc[idx_2026, "pub_index_yoy_week"] = (
                    weekly.loc[idx_2026, "pub_index"].values[0]
                    - weekly.loc[idx_2025, "pub_index"].values[0]
                )

    return weekly


# ---------------------------------------------------------
# SIGNAL INDEX (MONTHLY)
# ---------------------------------------------------------

def build_signal_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["date"] = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=1)
    )
    df = df.sort_values(["domain", "date"])

    # histórico por domínio
    df["prev_pub_total"] = df.groupby("domain")["pubmatic_total"].shift(1)

    # entrada: estava a 0 (ou nunca tinha aparecido) e passa a > 0
    df["pub_entered"] = (
        (df["prev_pub_total"].fillna(0) == 0) & (df["pubmatic_total"] > 0)
    )

    # saída: estava > 0 e passa explicitamente a 0 (não contamos omissões como saída)
    df["pub_exited"] = (
        (df["prev_pub_total"] > 0) & (df["pubmatic_total"] == 0)
    )

    monthly = (
        df.groupby(["year", "month"], as_index=False)
        .agg(
            pub_share_mean=("pubmatic_total_share", "mean"),
            comp_share_mean=("competitors_share", "mean"),
            enter_pct=("pub_entered", "mean"),
            exit_pct=("pub_exited", "mean"),
            domains=("domain", "nunique"),
        )
    )

    monthly["date"] = pd.to_datetime(
        dict(year=monthly["year"], month=monthly["month"], day=1)
    )
    monthly = monthly.sort_values("date").reset_index(drop=True)

    monthly["quarter"] = monthly["date"].dt.to_period("Q").astype(str)

    monthly["pub_share_delta"] = monthly["pub_share_mean"].diff()
    monthly["comp_share_delta"] = monthly["comp_share_mean"].diff()

    monthly["outperformance_score"] = (
        monthly["pub_share_delta"] - monthly["comp_share_delta"]
    )

    return monthly


# ---------------------------------------------------------
# SIGNAL INDEX (QUARTERLY, FROM MONTHLY SIGNAL)
# ---------------------------------------------------------

def build_quarterly_signal_index(signal_monthly: pd.DataFrame) -> pd.DataFrame:
    m = signal_monthly.copy()

    if "date" not in m.columns:
        m["date"] = pd.to_datetime(
            dict(year=m["year"], month=m["month"], day=1)
        )

    m["quarter"] = m["date"].dt.to_period("Q")

    q = (
        m.groupby("quarter", as_index=False)
        .agg(
            pub_share_mean_q=("pub_share_mean", "mean"),
            comp_share_mean_q=("comp_share_mean", "mean"),
            enter_pct_q=("enter_pct", "mean"),
            exit_pct_q=("exit_pct", "mean"),
            domains_q=("domains", "mean"),
        )
    )

    q = q.sort_values("quarter").reset_index(drop=True)
    q["quarter"] = q["quarter"].astype(str)

    # QoQ em nível
    q["pub_share_delta_q"] = q["pub_share_mean_q"].diff()
    q["comp_share_delta_q"] = q["comp_share_mean_q"].diff()

    # Outperformance trimestral (QoQ)
    q["outperformance_score_q"] = (
        q["pub_share_delta_q"] - q["comp_share_delta_q"]
    )

    # YoY em nível (quarter análogo: lag 4)
    q["pub_share_yoy_q"] = q["pub_share_mean_q"].diff(4)
    q["comp_share_yoy_q"] = q["comp_share_mean_q"].diff(4)

    # YoY do sinal de outperformance (quarter análogo)
    q["outperformance_score_q_yoy"] = q["outperformance_score_q"].diff(4)

    return q


# ---------------------------------------------------------
# SIGNAL INDEX (WEEKLY, YEAR ≥ 2026)
# ---------------------------------------------------------

def build_weekly_signal_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df[df["year"] >= 2026].copy()

    if df["week"].isna().all():
        return pd.DataFrame()

    weekly = (
        df.groupby(["year", "week"], as_index=False)
        .agg(
            pub_share_mean_week=("pubmatic_total_share", "mean"),
            comp_share_mean_week=("competitors_share", "mean"),
            domains_week=("domain", "nunique"),
        )
    )

    weekly = weekly.sort_values(["year", "week"]).reset_index(drop=True)

    weekly["pub_share_delta_week"] = weekly["pub_share_mean_week"].diff()
    weekly["comp_share_delta_week"] = weekly["comp_share_mean_week"].diff()

    weekly["outperformance_score_week"] = (
        weekly["pub_share_delta_week"] - weekly["comp_share_delta_week"]
    )

    weekly["pub_share_yoy_week"] = weekly["pub_share_mean_week"].diff(52)

    return weekly


# ---------------------------------------------------------
# MAIN
# ---------------------------------------------------------

def main():
    print("[INDEX] Loading wayback_output.xlsx...")
    df = load_wayback_data(INPUT_FILE)

    print("[INDEX] Building monthly index...")
    monthly = build_monthly_index(df)

    print("[INDEX] Building quarterly index...")
    quarterly = build_quarterly_index(monthly)

    print("[INDEX] Building weekly January index...")
    weekly_jan = build_weekly_january_index(df)

    print("[INDEX] Building monthly signal index...")
    signal_monthly = build_signal_index(df)

    print("[INDEX] Building quarterly signal index (from monthly signal)...")
    signal_quarterly = build_quarterly_signal_index(signal_monthly)

    print("[INDEX] Building weekly signal index (2026+)...")
    signal_weekly = build_weekly_signal_index(df)

    print(f"[INDEX] Writing index report -> {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        monthly.to_excel(writer, sheet_name="monthly_index", index=False)
        quarterly.to_excel(writer, sheet_name="quarterly_index", index=False)
        signal_monthly.to_excel(writer, sheet_name="signal_monthly", index=False)
        signal_quarterly.to_excel(writer, sheet_name="signal_quarterly", index=False)
        signal_weekly.to_excel(writer, sheet_name="signal_weekly", index=False)
        if not weekly_jan.empty:
            weekly_jan.to_excel(writer, sheet_name="weekly_january", index=False)

    print("[INDEX] Done.")


if __name__ == "__main__":
    main()
