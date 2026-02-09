import pandas as pd
import numpy as np
from pathlib import Path

# ============================
# PESOS POR PUBLISHER
# ============================

PUBLISHER_WEIGHTS = {
    # Tier 1 — Gigantes absolutos (peso total do tier = 0.35)
    # 7 publishers → 0.35 / 7 = 0.05 cada
    "cnn.com": 0.05,
    "nytimes.com": 0.05,
    "foxnews.com": 0.05,
    "yahoo.com": 0.05,
    "msn.com": 0.05,
    "espn.com": 0.05,
    "reuters.com": 0.05,

    # Tier 2 — Muito grandes (peso total do tier = 0.30)
    # 24 publishers → 0.30 / 24 = 0.0125 cada
    "washingtonpost.com": 0.0125,
    "nbcnews.com": 0.0125,
    "usatoday.com": 0.0125,
    "apnews.com": 0.0125,
    "abcnews.go.com": 0.0125,
    "cbsnews.com": 0.0125,
    "bleacherreport.com": 0.0125,
    "cbssports.com": 0.0125,
    "si.com": 0.0125,
    "weather.com": 0.0125,
    "bloomberg.com": 0.0125,
    "businessinsider.com": 0.0125,
    "marketwatch.com": 0.0125,
    "latimes.com": 0.0125,
    "chicagotribune.com": 0.0125,
    "time.com": 0.0125,
    "politico.com": 0.0125,
    "seattletimes.com": 0.0125,
    "sandiegouniontribune.com": 0.0125,
    "theatlantic.com": 0.0125,
    "newyorker.com": 0.0125,
    "slate.com": 0.0125,
    "vox.com": 0.0125,
    "axios.com": 0.0125,

    # Tier 3 — Grandes (peso total do tier = 0.20)
    # 38 publishers → 0.20 / 38 ≈ 0.0052631579 cada
    "accuweather.com": 0.0052631579,
    "investing.com": 0.0052631579,
    "fool.com": 0.0052631579,
    "cnet.com": 0.0052631579,
    "forbes.com": 0.0052631579,
    "variety.com": 0.0052631579,
    "hollywoodreporter.com": 0.0052631579,
    "nfl.com": 0.0052631579,
    "nba.com": 0.0052631579,
    "nhl.com": 0.0052631579,
    "dailymail.co.uk": 0.0052631579,
    "people.com": 0.0052631579,
    "eonline.com": 0.0052631579,
    "cosmopolitan.com": 0.0052631579,
    "vogue.com": 0.0052631579,
    "buzzfeed.com": 0.0052631579,
    "huffpost.com": 0.0052631579,
    "vice.com": 0.0052631579,
    "newsweek.com": 0.0052631579,
    "thehill.com": 0.0052631579,
    "thecut.com": 0.0052631579,
    "gizmodo.com": 0.0052631579,
    "lifehacker.com": 0.0052631579,
    "pcgamer.com": 0.0052631579,
    "pcmag.com": 0.0052631579,
    "digitaltrends.com": 0.0052631579,
    "howtogeek.com": 0.0052631579,
    "makeuseof.com": 0.0052631579,
    "androidauthority.com": 0.0052631579,
    "9to5google.com": 0.0052631579,
    "notebookcheck.net": 0.0052631579,
    "rottentomatoes.com": 0.0052631579,
    "metacritic.com": 0.0052631579,
    "screenrant.com": 0.0052631579,
    "deadline.com": 0.0052631579,
    "uproxx.com": 0.0052631579,
    "complex.com": 0.0052631579,
    "thewrap.com": 0.0052631579,

    # Tier 4 — Médios (peso total do tier = 0.10)
    # 55 publishers → 0.10 / 55 ≈ 0.0018181818 cada
    "techcrunch.com": 0.0018181818,
    "arstechnica.com": 0.0018181818,
    "ign.com": 0.0018181818,
    "gamespot.com": 0.0018181818,
    "polygon.com": 0.0018181818,
    "miamiherald.com": 0.0018181818,
    "denverpost.com": 0.0018181818,
    "startribune.com": 0.0018181818,
    "boston.com": 0.0018181818,
    "sfchronicle.com": 0.0018181818,
    "mercurynews.com": 0.0018181818,
    "oregonlive.com": 0.0018181818,
    "nj.com": 0.0018181818,
    "nola.com": 0.0018181818,
    "chron.com": 0.0018181818,
    "houstonchronicle.com": 0.0018181818,
    "sun-sentinel.com": 0.0018181818,
    "baltimoresun.com": 0.0018181818,
    "pressdemocrat.com": 0.0018181818,
    "sacbee.com": 0.0018181818,
    "kansascity.com": 0.0018181818,
    "charlotteobserver.com": 0.0018181818,
    "newsobserver.com": 0.0018181818,
    "dallasnews.com": 0.0018181818,
    "star-telegram.com": 0.0018181818,
    "oklahoman.com": 0.0018181818,
    "arkansasonline.com": 0.0018181818,
    "commercialappeal.com": 0.0018181818,
    "indystar.com": 0.0018181818,
    "azcentral.com": 0.0018181818,
    "freep.com": 0.0018181818,
    "detroitnews.com": 0.0018181818,
    "cleveland.com": 0.0018181818,
    "mlive.com": 0.0018181818,
    "pennlive.com": 0.0018181818,
    "inquirer.com": 0.0018181818,
    "philly.com": 0.0018181818,
    "timesunion.com": 0.0018181818,
    "lohud.com": 0.0018181818,
    "recordonline.com": 0.0018181818,
    "newsday.com": 0.0018181818,
    "theadvocate.com": 0.0018181818,
    "al.com": 0.0018181818,
    "wral.com": 0.0018181818,
    "kutv.com": 0.0018181818,
    "ktla.com": 0.0018181818,
    "wxyz.com": 0.0018181818,
    "wtop.com": 0.0018181818,
    "wgnradio.com": 0.0018181818,
    "kron4.com": 0.0018181818,
    "ktvu.com": 0.0018181818,
    "wpri.com": 0.0018181818,
    "wcvb.com": 0.0018181818,
    "wesh.com": 0.0018181818,
    "wtae.com": 0.0018181818,

    # Tier 5 — Pequenos-médios (peso total do tier = 0.05)
    # 6 publishers → 0.05 / 6 ≈ 0.0083333333 cada
    "nypost.com": 0.0083333333,
    "imdb.com": 0.0083333333,
    "nextdoor.com": 0.0083333333,
    "x.com": 0.0083333333,
    "crunchyroll.com": 0.0083333333,
    "mlb.com": 0.0083333333,
}


def get_weight(domain: str) -> float:
    return PUBLISHER_WEIGHTS.get(domain, 0.0)

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
