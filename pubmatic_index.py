#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


INPUT_FILE = "data/wayback_output.xlsx"
OUTPUT_FILE = "pubmatic_index.xlsx"


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


def build_quarterly_index(monthly: pd.DataFrame) -> pd.DataFrame:
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
# NEW: SIGNAL INDEX (PRIORITÁRIO)
# ---------------------------------------------------------

def build_signal_index(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["date"] = pd.to_datetime(
        dict(year=df["year"], month=df["month"], day=1)
    )
    df = df.sort_values(["domain", "date"])

    df["prev_pub_total"] = df.groupby("domain")["pubmatic_total"].shift(1)

    df["pub_entered"] = (
        (df["prev_pub_total"].fillna(0) == 0) & (df["pubmatic_total"] > 0)
    )
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
    monthly = monthly.sort_values("date")

    monthly["pub_share_delta"] = monthly["pub_share_mean"].diff()
    monthly["comp_share_delta"] = monthly["comp_share_mean"].diff()

    monthly["outperformance_score"] = (
        monthly["pub_share_delta"] - monthly["comp_share_delta"]
    )

    return monthly


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

    print("[INDEX] Building signal index (PRIORITÁRIO)...")
    signal_index = build_signal_index(df)

    print(f"[INDEX] Writing index report -> {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        monthly.to_excel(writer, sheet_name="monthly_index", index=False)
        quarterly.to_excel(writer, sheet_name="quarterly_index", index=False)
        signal_index.to_excel(writer, sheet_name="signal_index", index=False)
        if not weekly_jan.empty:
            weekly_jan.to_excel(writer, sheet_name="weekly_january", index=False)

    print("[INDEX] Done.")


if __name__ == "__main__":
    main()
