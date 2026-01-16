import os
import pandas as pd
from datetime import datetime

ARTIFACTS_ROOT = "artifacts"

def find_latest_run_summary():
    """Procura o run_summary.xlsx mais recente do dia."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    day_dir = os.path.join(ARTIFACTS_ROOT, today)

    if not os.path.isdir(day_dir):
        raise FileNotFoundError(f"Nenhum diretório encontrado para {today}")

    # procurar run_summary.xlsx dentro do diretório do dia
    for root, dirs, files in os.walk(day_dir):
        for f in files:
            if f.endswith("run_summary.xlsx"):
                return os.path.join(root, f)

    raise FileNotFoundError("run_summary.xlsx não encontrado para o dia de hoje")


def load_daily_score(path):
    """Extrai o score global diário da linha __GLOBAL_DAILY__."""
    df = pd.read_excel(path)

    row = df[df["domain"] == "__GLOBAL_DAILY__"]
    if row.empty:
        raise ValueError("Linha __GLOBAL_DAILY__ não encontrada no run_summary.xlsx")

    score = float(row["score_weighted"].iloc[0])
    return score


def append_to_history(date_str, score):
    """Guarda o score diário num histórico."""
    history_path = os.path.join(ARTIFACTS_ROOT, "scores_history.csv")

    new_row = pd.DataFrame([{
        "date": date_str,
        "score_daily": score
    }])

    if os.path.exists(history_path):
        hist = pd.read_csv(history_path)
        hist = pd.concat([hist, new_row], ignore_index=True)
    else:
        hist = new_row

    hist.to_csv(history_path, index=False)


def write_flag(date_str):
    """Cria o ficheiro score_done.flag para evitar duplicações."""
    flag_path = os.path.join(ARTIFACTS_ROOT, date_str, "score_done.flag")
    with open(flag_path, "w") as f:
        f.write("done")


def main():
    today = datetime.utcnow().strftime("%Y-%m-%d")
    flag_path = os.path.join(ARTIFACTS_ROOT, today, "score_done.flag")

    # Se já existe flag → já foi calculado hoje
    if os.path.exists(flag_path):
        print("Score diário já calculado hoje.")
        return

    # 1. Encontrar o run_summary.xlsx do dia
    summary_path = find_latest_run_summary()

    # 2. Extrair score diário
    score = load_daily_score(summary_path)

    # 3. Guardar no histórico
    append_to_history(today, score)

    # 4. Criar flag
    write_flag(today)

    print(f"Score diário calculado: {score}")


if __name__ == "__main__":
    main()
