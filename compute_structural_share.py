import pandas as pd
import numpy as np
from pathlib import Path

# ============================
# PESOS POR PUBLISHER
# ============================

PUBLISHER_WEIGHTS = {
    # Tier 1 — Gigantes
    "cnn.com": 0.14,
    "nytimes.com": 0.14,
    "foxnews.com": 0.14,

    # Tier 2 — Muito grandes
    "washingtonpost.com": 0.09,
    "nbcnews.com": 0.09,
    "usatoday.com": 0.09,
    "crunchyroll.com": 0.09,
    "mlb.com": 0.09,

    # Tier 3 — Médios
    "nypost.com": 0.06,
    "imdb.com": 0.06,

    # Tier 4 — Pequenos
    "nextdoor.com": 0.03,
    "x.com": 0.03,
}

DEFAULT_WEIGHT = 0.0035



def get_weight(domain: str) -> float:
    return PUBLISHER_WEIGHTS.get(domain, DEFAULT_WEIGHT)

def main():
    input_path = Path("data/wayback_output.xlsx")
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
