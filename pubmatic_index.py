#!/usr/bin/env python3
import pandas as pd
from pathlib import Path


INPUT_FILE = "wayback_output.xlsx"
OUTPUT_FILE = "pubmatic_index.xlsx"


def load_wayback_data(path: str = INPUT_FILE) -> pd.DataFrame:
    df = pd.read_excel(path)

    # garantir colunas essenciais
    required = [
        "domain",
        "year",
        "month",
        "pubmatic_total_share",
        "competitors_share",
        "total_lines",
    ]
    for col in required:
        if col not in df.columns:
            raise ValueError(f"Missing required column in wayback_output: {col}")

    # se não existir coluna week (pré‑alteração do scraper), cria com None
    if "week" not in df.columns:
        df["week"] = None

    return df


def build_monthly_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Índice mensal agregado:
      - média do share da PubMatic
      - média do share dos concorrentes
      - número de domínios
      - peso médio de linhas (total_lines)
    """
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

    # criar coluna date e quarter
    monthly["date"] = pd.to_datetime(
        dict(year=monthly["year"], month=monthly["month"], day=1)
    )
    monthly = monthly.sort_values("date").reset_index(drop=True)

    # quarter como período
    monthly["quarter"] = monthly["date"].dt.to_period("Q").astype(str)

    # variações mensais (MoM)
    monthly["pub_index_mom"] = monthly["pub_index"].diff()

    # variações YoY (mesmo mês ano anterior)
    monthly["pub_index_yoy"] = monthly["pub_index"].diff(12)

    return monthly


def build_quarterly_index(monthly: pd.DataFrame) -> pd.DataFrame:
    """
    Índice trimestral:
      - média do índice mensal dentro do trimestre
      - Δ QoQ
      - Δ YoY
    """
    # extrair ano e quarter
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

    # Δ QoQ
    q["pub_index_qoq"] = q["pub_index_q"].diff()

    # Δ YoY (mesmo quarter ano anterior → lag 4)
    q["pub_index_q_yoy"] = q["pub_index_q"].diff(4)

    return q


def build_weekly_january_index(df: pd.DataFrame) -> pd.DataFrame:
    """
    Índice semanal apenas para Janeiro 2025 e Janeiro 2026.
    Assume que o scraper já escreveu a coluna 'week'.
    """
    jan = df[(df["month"] == 1) & (df["year"].isin([2025, 2026]))].copy()

    # se não houver week (ou tudo None), não faz sentido continuar
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

    # ordenar por semana e ano
    weekly = weekly.sort_values(["week", "year"]).reset_index(drop=True)

    # Δ YoY semanal (semana de 2026 vs mesma semana de 2025)
    # para cada semana, comparar ano atual com ano anterior
    weekly["pub_index_yoy_week"] = None

    for w in sorted(weekly["week"].dropna().unique()):
        mask = weekly["week"] == w
        sub = weekly[mask].sort_values("year")
        # se houver pelo menos 2 anos, calcula diff
        if len(sub) >= 2:
            # assumindo apenas 2025 e 2026
            idx_2026 = sub[sub["year"] == 2026].index
            idx_2025 = sub[sub["year"] == 2025].index
            if len(idx_2026) == 1 and len(idx_2025) == 1:
                weekly.loc[idx_2026, "pub_index_yoy_week"] = (
                    weekly.loc[idx_2026, "pub_index"].values[0]
                    - weekly.loc[idx_2025, "pub_index"].values[0]
                )

    return weekly


def main():
    print("[INDEX] Loading wayback_output.xlsx...")
    df = load_wayback_data(INPUT_FILE)

    print("[INDEX] Building monthly index...")
    monthly = build_monthly_index(df)

    print("[INDEX] Building quarterly index...")
    quarterly = build_quarterly_index(monthly)

    print("[INDEX] Building weekly January index (2025 vs 2026)...")
    weekly_jan = build_weekly_january_index(df)

    # escrever tudo num Excel
    print(f"[INDEX] Writing index report -> {OUTPUT_FILE}")
    with pd.ExcelWriter(OUTPUT_FILE, engine="openpyxl") as writer:
        monthly.to_excel(writer, sheet_name="monthly_index", index=False)
        quarterly.to_excel(writer, sheet_name="quarterly_index", index=False)
        if not weekly_jan.empty:
            weekly_jan.to_excel(writer, sheet_name="weekly_january", index=False)

    print("[INDEX] Done.")


if __name__ == "__main__":
    main()
