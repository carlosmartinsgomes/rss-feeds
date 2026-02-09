import pandas as pd
import numpy as np
from pathlib import Path

# ============================
# PESOS POR PUBLISHER
# ============================

PUBLISHER_WEIGHTS = {
    # Tier 1 — Gigantes absolutos
    "cnn.com": 0.12,
    "nytimes.com": 0.12,
    "foxnews.com": 0.11,
    "yahoo.com": 0.11,
    "msn.com": 0.10,
    "espn.com": 0.10,
    "reuters.com": 0.10,

    # Tier 2 — Muito grandes
    "washingtonpost.com": 0.08,
    "nbcnews.com": 0.08,
    "usatoday.com": 0.07,
    "apnews.com": 0.07,
    "abcnews.go.com": 0.07,
    "cbsnews.com": 0.07,
    "bleacherreport.com": 0.07,
    "cbssports.com": 0.07,
    "si.com": 0.07,
    "weather.com": 0.07,
    "bloomberg.com": 0.07,
    "businessinsider.com": 0.07,
    "marketwatch.com": 0.07,

    # Tier 3 — Grandes
    "accuweather.com": 0.05,
    "investing.com": 0.05,
    "fool.com": 0.05,
    "cnet.com": 0.05,
    "forbes.com": 0.05,
    "variety.com": 0.05,
    "hollywoodreporter.com": 0.05,
    "nfl.com": 0.05,
    "nba.com": 0.05,
    "nhl.com": 0.05,
    "dailymail.co.uk": 0.05,

    # Tier 4 — Médios
    "techcrunch.com": 0.04,
    "arstechnica.com": 0.04,
    "ign.com": 0.04,
    "gamespot.com": 0.04,
    "polygon.com": 0.04,

    # Tier 5 — Pequenos-médios (originais)
    "nypost.com": 0.03,
    "imdb.com": 0.03,
    "nextdoor.com": 0.02,
    "x.com": 0.02,
    "crunchyroll.com": 0.02,
    "mlb.com": 0.02,
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
