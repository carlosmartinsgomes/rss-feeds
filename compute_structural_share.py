import pandas as pd
import numpy as np
from pathlib import Path

# ============================
# PESOS POR PUBLISHER
# ============================

PUBLISHER_WEIGHTS = {
    "foxnews.com": 0.22,
    "crunchyroll.com": 0.20,
    "mlb.com": 0.18,
    "nypost.com": 0.15,
    "imdb.com": 0.10,
    "nextdoor.com": 0.05,
    "x.com": 0.03,
}

DEFAULT_WEIGHT = 0.0035


def get_weight(domain: str) -> float:
    return PUBLISHER_WEIGHTS.get(domain, DEFAULT_WEIGHT)

def main():
    input_path = Path("wayback_output.xlsx")
    output_path = Path("structural_share_index.xlsx")

    df = pd.read_excel(input_path)

    df["domain"] = df["domain"].str.lower().str.strip()
    df["pub_share"] = df["pubmatic_total_share"]
    df["comp_share"] = df["competitors_share"]

    df["publisher_weight"] = df["domain"].apply(get_weight)
    df["weighted_pub"] = df["pub_share"] * df["publisher_weight"]
    df["weighted_comp"] = df["comp_share"] * df["publisher_weight"]

    df["date"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H%M%S")
    df["quarter"] = df["date"].dt.to_period("Q")

    grouped = df.groupby("quarter").agg(
        struct_pub_share=("weighted_pub", "sum"),
        struct_comp_share=("weighted_comp", "sum"),
        total_weight=("publisher_weight", "sum")
    ).reset_index()

    grouped["struct_pub_share"] /= grouped["total_weight"]
    grouped["struct_comp_share"] /= grouped["total_weight"]
    grouped["struct_outperf"] = grouped["struct_pub_share"] - grouped["struct_comp_share"]
    grouped["struct_outperf_yoy"] = grouped["struct_outperf"].pct_change(4)

    grouped.to_excel(output_path, index=False)

    print("✔ structural_share_index.xlsx criado com sucesso!")

if __name__ == "__main__":
    main()
