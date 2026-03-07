import hashlib
import json
import os
import sys

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.utils.data_processing import compute_temporal_split, load_domain_features, load_snapshots

FILTER_PRICE_MIN = 0.01
FILTER_PRICE_MAX = 0.99
MIN_GROUP_ROWS = 30
MIN_TRAIN_ROWS = 15
PRICE_BIN_STEP = 0.03
MIN_VALID_N = 8
EDGE_AB_THRESHOLD = 0.02
EDGE_STD_THRESHOLD = 0.02


def edge_sign(value, eps=1e-6):
    if abs(value) < eps:
        return 0
    return 1 if value > 0 else -1


def parse_bounds(price_label, horizon_label):
    price_parts = price_label.split("-")
    price_min = float(price_parts[0])
    price_max = float(price_parts[1])

    if "<" in horizon_label:
        horizon_min = 0
        horizon_max = int(horizon_label.replace("<", "").replace("h", ""))
    elif ">" in horizon_label:
        horizon_min = int(horizon_label.replace(">", "").replace("h", ""))
        horizon_max = 1000
    else:
        horizon_parts = horizon_label.replace("h", "").split("-")
        horizon_min, horizon_max = int(horizon_parts[0]), int(horizon_parts[1])

    return price_min, price_max, horizon_min, horizon_max


def stable_leaf_id(group_key: str, price_label: str, horizon_label: str) -> int:
    digest = hashlib.sha1(f"{group_key}|{price_label}|{horizon_label}".encode("utf-8")).hexdigest()
    return int(digest[:12], 16)


def load_rule_training_frame() -> pd.DataFrame:
    snapshots = load_snapshots(config.SNAPSHOTS_PATH)
    domain_features = load_domain_features(config.MARKET_DOMAIN_FEATURES_PATH)

    df = snapshots.merge(
        domain_features[["market_id", "domain", "category", "market_type"]],
        on="market_id",
        how="left",
        suffixes=("", "_domain"),
    )

    if "category_domain" in df.columns:
        df["category"] = df["category_domain"].fillna(df["category"])
        df = df.drop(columns=["category_domain"])

    df["domain"] = df.get("domain", "UNKNOWN").fillna("UNKNOWN")
    df["category"] = df.get("category", "UNKNOWN").fillna("UNKNOWN")
    df["market_type"] = df.get("market_type", "UNKNOWN").fillna("UNKNOWN")

    initial_len = len(df)
    df = df[(df["price"] >= FILTER_PRICE_MIN) & (df["price"] < FILTER_PRICE_MAX)].copy()
    print(
        f"[INFO] Filtered {initial_len - len(df)} rows with extreme prices "
        f"(outside [{FILTER_PRICE_MIN}, {FILTER_PRICE_MAX}))."
    )

    df["resolve_time"] = pd.to_datetime(df["resolve_time"], utc=True, format="mixed")
    df["e_sample"] = df["y"] - df["price"]
    p_clip = df["price"].clip(0.001, 0.999)
    df["r_std"] = df["e_sample"] / np.sqrt(p_clip * (1.0 - p_clip))

    price_bins = np.arange(0, 1.0 + PRICE_BIN_STEP, PRICE_BIN_STEP)
    price_labels = [f"{round(value, 2)}-{round(value + PRICE_BIN_STEP, 2)}" for value in price_bins[:-1]]

    horizon_edges = [0] + sorted(config.HORIZONS) + [1000]
    horizon_labels = [f"<{horizon_edges[1]}h"]
    for index in range(1, len(horizon_edges) - 2):
        horizon_labels.append(f"{horizon_edges[index]}-{horizon_edges[index + 1]}h")
    horizon_labels.append(f">{horizon_edges[-2]}h")

    df["price_bin"] = pd.cut(df["price"], bins=price_bins, labels=price_labels, right=False)
    df["horizon_bin"] = pd.cut(df["horizon_hours"], bins=horizon_edges, labels=horizon_labels, right=False)
    df = df.dropna(subset=["price_bin", "horizon_bin"]).copy()

    return df


def build_rules(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    train_end, valid_start = compute_temporal_split(df)
    print(f"[INFO] Rolling split: train <= {train_end}, valid >= {valid_start}")

    train_df = df[df["resolve_time"] <= train_end].copy()
    valid_df = df[df["resolve_time"] >= valid_start].copy()

    group_cols = ["domain", "category", "market_type", "price_bin", "horizon_bin"]
    agg_spec = {
        "n": ("y", "size"),
        "wins": ("y", "sum"),
        "p_mean": ("price", "mean"),
        "edge_raw_mean": ("e_sample", "mean"),
        "edge_std_mean": ("r_std", "mean"),
    }

    full_group = df.groupby(group_cols, observed=False).agg(**agg_spec)
    train_group = train_df.groupby(group_cols, observed=False).agg(**agg_spec)
    valid_group = valid_df.groupby(group_cols, observed=False).agg(**agg_spec)
    grid = full_group.join(train_group, rsuffix="_train").join(valid_group, rsuffix="_valid")

    final_rules = []
    full_report = []

    for index, row in grid.iterrows():
        domain, category, market_type, price_label, horizon_label = index
        n_full = row["n"]
        if pd.isna(n_full) or n_full < MIN_GROUP_ROWS:
            continue

        n_train = row.get("n_train")
        n_valid = row.get("n_valid")
        if pd.isna(n_train) or n_train < MIN_TRAIN_ROWS:
            continue
        if pd.isna(n_valid) or n_valid < MIN_VALID_N:
            continue

        q_train = row["wins_train"] / n_train
        edge_train = q_train - row["p_mean_train"]

        q_valid = row["wins_valid"] / n_valid
        edge_valid = q_valid - row["p_mean_valid"]

        sign_train = edge_sign(edge_train)
        sign_valid = edge_sign(edge_valid)
        if sign_train == 0 or sign_valid == 0 or sign_train != sign_valid:
            continue

        edge_raw_valid = row["edge_raw_mean_valid"]
        edge_std_valid = row["edge_std_mean_valid"]
        if abs(edge_raw_valid) < EDGE_AB_THRESHOLD:
            continue
        if abs(edge_std_valid) < EDGE_STD_THRESHOLD:
            continue

        wins_full = row["wins"]
        p_mean_full = row["p_mean"]
        q_full = wins_full / n_full
        edge_full = q_full - p_mean_full
        edge_raw_full = row["edge_raw_mean"]
        edge_std_full = row["edge_std_mean"]

        direction = edge_sign(edge_full)
        if direction == 0:
            direction = edge_sign(edge_raw_full)
        if direction == 0:
            continue

        if direction >= 0:
            q_trade = q_full
            p_trade = p_mean_full
            edge_net_trade = edge_full
            edge_sample_trade = edge_raw_full
            edge_std_trade = edge_std_full
            roi_trade = edge_raw_full / max(p_mean_full, 1e-6)
        else:
            q_trade = 1.0 - q_full
            p_trade = 1.0 - p_mean_full
            edge_net_trade = -edge_full
            edge_sample_trade = -edge_raw_full
            edge_std_trade = -edge_std_full
            roi_trade = (p_mean_full - (wins_full / n_full)) / max(1.0 - p_mean_full, 1e-6)

        price_min, price_max, horizon_min, horizon_max = parse_bounds(str(price_label), str(horizon_label))
        group_key = f"{domain}|{category}|{market_type}"
        leaf_id = stable_leaf_id(group_key, str(price_label), str(horizon_label))

        rule = {
            "group_key": group_key,
            "domain": domain,
            "category": category,
            "market_type": market_type,
            "leaf_id": leaf_id,
            "n_train": int(n_train),
            "n_valid": int(n_valid),
            "n_full": int(n_full),
            "q_smooth": float(q_full),
            "p_mean": float(p_mean_full),
            "edge_net": float(edge_full),
            "edge_sample": float(edge_raw_full),
            "edge_std": float(edge_std_full),
            "roi": float(roi_trade),
            "direction": int(direction),
            "q_trade": float(q_trade),
            "p_trade": float(p_trade),
            "edge_net_trade": float(edge_net_trade),
            "edge_sample_trade": float(edge_sample_trade),
            "edge_std_trade": float(edge_std_trade),
            "roi_trade": float(roi_trade),
            "rule_score": float(edge_sample_trade * np.sqrt(max(n_valid, 1)) * max(abs(edge_std_trade), 0.1)),
            "rule_bounds": json.dumps(
                {
                    "price_min": price_min,
                    "price_max": price_max,
                    "horizon_min": horizon_min,
                    "horizon_max": horizon_max,
                }
            ),
            "price_min": price_min,
            "price_max": price_max,
            "h_min": horizon_min,
            "h_max": horizon_max,
            "price_bin": str(price_label),
            "horizon_bin": str(horizon_label),
        }
        final_rules.append(rule)
        full_report.append(rule.copy())

    rules_df = pd.DataFrame(final_rules).sort_values("rule_score", ascending=False) if final_rules else pd.DataFrame()
    report_df = pd.DataFrame(full_report)
    return rules_df, report_df


def main():
    df = load_rule_training_frame()
    rules_df, report_df = build_rules(df)

    if rules_df.empty:
        print("[WARN] No rules generated under current thresholds.")
        rules_df = pd.DataFrame(
            columns=[
                "group_key",
                "domain",
                "category",
                "market_type",
                "leaf_id",
                "n_train",
                "n_valid",
                "n_full",
                "q_smooth",
                "p_mean",
                "edge_sample_trade",
                "edge_std_trade",
                "rule_score",
                "direction",
                "rule_bounds",
            ]
        )

    rules_df.to_csv(config.NAIVE_RULES_OUTPUT_PATH, index=False)
    rules_df.to_csv(config.RULES_OUTPUT_PATH, index=False)
    report_df.to_csv(config.NAIVE_RULES_REPORT_PATH, index=False)
    with open(config.NAIVE_RULES_JSON_PATH, "w", encoding="utf-8") as file:
        json.dump(rules_df.to_dict("records"), file, indent=2)

    print(f"[INFO] Saved {len(rules_df)} rules to {config.NAIVE_RULES_OUTPUT_PATH}")
    print(f"[INFO] Saved canonical trading rules to {config.RULES_OUTPUT_PATH}")
    print(f"[INFO] Saved full rule report to {config.NAIVE_RULES_REPORT_PATH}")


if __name__ == "__main__":
    main()
