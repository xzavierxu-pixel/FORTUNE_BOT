from pathlib import Path

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent.parent
DATA_DIR = BASE_DIR / "data"
TRADES_PATH = DATA_DIR / "backtesting" / "backtest_trades_qmodel.csv"


def main():
    if not TRADES_PATH.exists():
        print(f"[ERROR] File not found: {TRADES_PATH}")
        return

    print(f"[INFO] Loading trades from {TRADES_PATH} ...")
    df = pd.read_csv(TRADES_PATH)

    for column in ["price", "q_pred", "edge_prob", "pnl", "pnl_pct_of_stake"]:
        df[column] = df[column].astype(float)
    df["y"] = df["y"].astype(int)
    df["direction"] = df["direction"].astype(int)

    df = df[df["direction"] != 0].copy()
    print(f"[INFO] Trades with non-zero direction: {len(df)}")
    if df.empty:
        print("No trades found.")
        return

    df["signed_true_edge"] = df["direction"] * (df["y"] - df["price"])
    print(f"\nGlobal mean signed_true_edge = {df['signed_true_edge'].mean():.4f}")
    print(f"Global mean pnl_pct_of_stake = {df['pnl_pct_of_stake'].mean():.4f}")

    df["abs_edge_bucket"] = pd.qcut(df["edge_prob"].abs(), 5, duplicates="drop")
    bucket = (
        df.groupby("abs_edge_bucket", observed=False)
        .agg(
            n=("pnl", "size"),
            edge_prob_mean=("edge_prob", "mean"),
            signed_true_edge_mean=("signed_true_edge", "mean"),
            roi_mean=("pnl_pct_of_stake", "mean"),
            win_rate=("pnl", lambda values: (values > 0).mean()),
        )
        .reset_index()
    )
    print("\n=== Performance by |edge_prob| buckets ===")
    print(bucket.to_string(index=False, float_format=lambda value: f"{value:.4f}"))

    print("\n=== Split by direction (YES vs NO) ===")
    for direction in [1, -1]:
        subset = df[df["direction"] == direction]
        if subset.empty:
            continue
        side = "YES" if direction == 1 else "NO"
        print(f"\nSide = {side} (n={len(subset)})")
        print(f"  mean signed_true_edge = {subset['signed_true_edge'].mean():.4f}")
        print(f"  mean pnl_pct_of_stake = {subset['pnl_pct_of_stake'].mean():.4f}")
        print(f"  win_rate              = {(subset['pnl'] > 0).mean():.4f}")


if __name__ == "__main__":
    main()
