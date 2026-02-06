import pandas as pd
from pathlib import Path

PUBLISHER_WEIGHTS = {
    "foxnews.com": 1.0,
    "nypost.com": 1.0,
    "mlb.com": 1.0,
    "crunchyroll.com": 1.0,
    "imdb.com": 1.0,
    "x.com": 1.0,
}
DEFAULT_WEIGHT = 0.3

def get_weight(domain: str) -> float:
    return PUBLISHER_WEIGHTS.get(domain, DEFAULT_WEIGHT)

def main():
    input_path = Path("data/wayback_output.xlsx")          # ajusta ao teu path real
    output_path = Path("data/structural_share_index.xlsx") # output final

    df = pd.read_excel(input_path)

    # garantir colunas necessárias
    # domain, year, month, pubmatic_total_share, competitors_share, etc.
    # Structural share por linha:
    df["structural_share_raw"] = df["pubmatic_total_share"]  # ou outra métrica base
    df["publisher_weight"] = df["domain"].apply(get_weight)
    df["structural_share_weighted"] = df["structural_share_raw"] * df["publisher_weight"]

    # Agregar por mês global (ou por quarter, se preferires)
    monthly = (
        df.groupby(["year", "month"])
          .agg(
              structural_share_weighted_sum=("structural_share_weighted", "sum"),
              weight_sum=("publisher_weight", "sum")
          )
          .reset_index()
    )

    # score médio ponderado
    monthly["structural_share_score"] = (
        monthly["structural_share_weighted_sum"] / monthly["weight_sum"]
    )

    # Se quiseres já em quarter:
    monthly["quarter"] = (
        monthly["year"].astype(str)
        + "Q"
        + ((monthly["month"] - 1) // 3 + 1).astype(str)
    )
    quarterly = (
        monthly.groupby("quarter")
               .agg(structural_share_score_q=("structural_share_score", "mean"))
               .reset_index()
    )

    with pd.ExcelWriter(output_path) as writer:
        monthly.to_excel(writer, sheet_name="monthly", index=False)
        quarterly.to_excel(writer, sheet_name="quarterly", index=False)

if __name__ == "__main__":
    main()
