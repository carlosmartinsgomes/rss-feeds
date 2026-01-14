import os
import pandas as pd
from datetime import datetime, timedelta

ARTIFACTS_ROOT = "artifacts"

def load_history():
    """Lê o scores_history.csv com os scores diários."""
    history_path = os.path.join(ARTIFACTS_ROOT, "scores_history.csv")

    if not os.path.exists(history_path):
        raise FileNotFoundError("scores_history.csv não encontrado. Corre compute_scores.py primeiro.")

    df = pd.read_csv(history_path)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    return df


def compute_period(df, days):
    """Calcula a média dos últimos X dias."""
    cutoff = df["date"].max() - timedelta(days=days)
    window = df[df["date"] >= cutoff]

    if window.empty:
        return None

    return window["score_daily"].mean()


def main():
    df = load_history()

    today = df["date"].max().strftime("%Y-%m-%d")

    weekly = compute_period(df, 7)
    biweekly = compute_period(df, 14)
    monthly = compute_period(df, 30)

    out = pd.DataFrame([{
        "date": today,
        "weekly_score": weekly,
        "biweekly_score": biweekly,
        "monthly_score": monthly
    }])

    out_path = os.path.join(ARTIFACTS_ROOT, "scores_aggregated.csv")

    if os.path.exists(out_path):
        existing = pd.read_csv(out_path)
        existing = pd.concat([existing, out], ignore_index=True)
        existing.to_csv(out_path, index=False)
    else:
        out.to_csv(out_path, index=False)

    print("Scores agregados atualizados:")
    print(out)


if __name__ == "__main__":
    main()
