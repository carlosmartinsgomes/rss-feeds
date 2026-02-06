import pandas as pd
import numpy as np

# ============================
# 1. PESOS POR PUBLISHER
# ============================

weights = {
    "foxnews.com": 0.22,
    "crunchyroll.com": 0.20,
    "mlb.com": 0.18,
    "nypost.com": 0.15,
    "imdb.com": 0.10,
    "nextdoor.com": 0.05,
    "x.com": 0.03,
}

# Os restantes 20 publishers recebem peso mínimo
# Vais preencher automaticamente com 0.0035 cada
MIN_WEIGHT = 0.0035

# ============================
# 2. CARREGAR WAYBACK
# ============================

df = pd.read_excel("wayback_output.xlsx")

# Normalizar domínio
df["domain"] = df["domain"].str.lower().str.strip()

# Identificar publishers sem peso explícito
all_publishers = df["domain"].unique()
remaining = [p for p in all_publishers if p not in weights]

for p in remaining:
    weights[p] = MIN_WEIGHT

# ============================
# 3. CALCULAR SHARE MENSAL
# ============================

df["pub_share"] = df["pubmatic_total_share"]
df["comp_share"] = df["competitors_share"]

# ============================
# 4. AGREGAR POR QUARTER
# ============================

df["date"] = pd.to_datetime(df["timestamp"], format="%Y%m%d%H%M%S")
df["quarter"] = df["date"].dt.to_period("Q")

# ============================
# 5. CALCULAR STRUCTURAL SHARE
# ============================

# Função para calcular weighted mean por quarter
def weighted_mean(group):
    pubs = group["domain"]
    w = np.array([weights[p] for p in pubs])
    w = w / w.sum()  # normalizar dentro do quarter
    return pd.Series({
        "struct_pub_share": np.sum(group["pub_share"] * w),
        "struct_comp_share": np.sum(group["comp_share"] * w),
    })

struct = df.groupby("quarter").apply(weighted_mean).reset_index()

# ============================
# 6. OUTPERFORMANCE
# ============================

struct["struct_outperf"] = struct["struct_pub_share"] - struct["struct_comp_share"]

# YoY
struct["struct_outperf_yoy"] = struct["struct_outperf"].pct_change(4)

# ============================
# 7. EXPORTAR
# ============================

struct.to_excel("pubmatic_index.xlsx", index=False)

print("Novo pubmatic_index.xlsx gerado com sucesso!")
