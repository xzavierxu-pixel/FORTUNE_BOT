import os
import sys
import argparse

import joblib
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.utils.data_processing import (
    build_market_feature_cache,
    compute_temporal_split,
    load_domain_features,
    load_raw_markets,
    load_snapshots,
    preprocess_features,
)
from rule_baseline.utils.modeling import fit_model_payload, predict_probabilities
from rule_baseline.utils.raw_batches import rebuild_canonical_merged

DROP_COLS = {
    "y",
    "resolve_time",
    "market_id",
    "snapshot_time",
    "scheduled_end",
    "snapshot_date",
    "question",
    "description",
    "source_url",
    "source_host",
    "batch_id",
    "batch_fetched_at",
    "batch_window_start",
    "batch_window_end",
    "price_bin",
    "horizon_bin",
    "r_std",
    "delta_hours",
    "domain_market",
    "market_type_market",
    "sub_domain",
    "outcome_pattern",
    "groupItemTitle",
    "gameId",
    "marketMakerAddress",
    "startDate",
    "endDate",
}


def load_rules(path=None) -> pd.DataFrame:
    target = path or config.RULES_OUTPUT_PATH
    if not target.exists():
        raise FileNotFoundError(f"Rules file not found at {target}. Run train_rules_naive_output_rule.py first.")

    rules = pd.read_csv(target)
    required = ["domain", "category", "market_type", "price_min", "price_max", "h_min", "h_max", "q_smooth", "rule_score"]
    missing = [column for column in required if column not in rules.columns]
    if missing:
        raise ValueError(f"Rules file is missing required columns: {missing}")

    rules["domain"] = rules["domain"].fillna("UNKNOWN").astype(str)
    rules["category"] = rules["category"].fillna("UNKNOWN").astype(str)
    rules["market_type"] = rules["market_type"].fillna("UNKNOWN").astype(str)
    return rules


def match_snapshots_to_rules(snapshots: pd.DataFrame, domain_features: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    context = snapshots.merge(
        domain_features[["market_id", "domain", "category", "market_type"]],
        on="market_id",
        how="left",
        suffixes=("", "_domain"),
    )
    if "category_domain" in context.columns:
        context["category"] = context["category_domain"].fillna(context["category"])
        context = context.drop(columns=["category_domain"])

    context["domain"] = context.get("domain", "UNKNOWN").fillna("UNKNOWN").astype(str)
    context["category"] = context.get("category", "UNKNOWN").fillna("UNKNOWN").astype(str)
    context["market_type"] = context.get("market_type", "UNKNOWN").fillna("UNKNOWN").astype(str)

    merged = context.merge(
        rules[
            [
                "domain",
                "category",
                "market_type",
                "leaf_id",
                "price_min",
                "price_max",
                "h_min",
                "h_max",
                "q_smooth",
                "rule_score",
                "direction",
                "group_key",
            ]
        ],
        on=["domain", "category", "market_type"],
        how="inner",
    )

    if merged.empty:
        print("[WARN] No snapshots matched rules on domain/category/market_type.")
        return pd.DataFrame()

    mask = (
        (merged["price"] >= merged["price_min"] - 1e-9)
        & (merged["price"] <= merged["price_max"] + 1e-9)
        & (merged["horizon_hours"] >= merged["h_min"])
        & (merged["horizon_hours"] <= merged["h_max"])
    )
    matched = merged[mask].copy()
    if matched.empty:
        print("[WARN] No snapshots matched rule bounds after domain/category/market_type join.")
        return pd.DataFrame()

    matched = matched.sort_values(
        ["market_id", "snapshot_time", "rule_score"],
        ascending=[True, True, False],
    )
    matched = matched.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)
    print(f"[INFO] Matched {len(matched)} snapshots to rules.")
    return matched


def build_feature_table(
    snapshots: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    domain_features: pd.DataFrame,
    rules: pd.DataFrame,
) -> pd.DataFrame:
    matched = match_snapshots_to_rules(snapshots, domain_features, rules)
    if matched.empty:
        return pd.DataFrame()
    return preprocess_features(matched, market_feature_cache)


def split_train_valid(df_feat: pd.DataFrame):
    train_end, valid_start = compute_temporal_split(df_feat)
    print(f"[INFO] Rolling split: train <= {train_end}, valid >= {valid_start}")
    df_train = df_feat[df_feat["resolve_time"] <= train_end].copy()
    df_valid = df_feat[df_feat["resolve_time"] >= valid_start].copy()
    return df_train, df_valid


def save_model(payload: dict) -> None:
    config.MODELS_DIR.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, config.MODEL_PATH)
    print(f"[INFO] Saved ensemble payload to {config.MODEL_PATH}")


def export_predictions(payload: dict, df_feat: pd.DataFrame) -> None:
    print("[INFO] Generating unified predictions export...")
    q_pred = predict_probabilities(payload, df_feat)

    out = df_feat[
        [
            "market_id",
            "snapshot_time",
            "snapshot_date",
            "resolve_time",
            "scheduled_end",
            "horizon_hours",
            "price",
            "y",
            "domain",
            "category",
            "market_type",
            "leaf_id",
            "direction",
            "group_key",
            "q_smooth",
            "rule_score",
        ]
    ].copy()
    out["q_pred"] = q_pred
    out["edge_prob"] = out["q_pred"] - out["price"]
    out.to_csv(config.PREDICTIONS_PATH, index=False)
    print(f"[INFO] Saved predictions to {config.PREDICTIONS_PATH}")


def parse_args():
    parser = argparse.ArgumentParser(description="Train the snapshot ensemble model.")
    parser.add_argument(
        "--calibration-mode",
        choices=["valid_isotonic", "cv_isotonic", "none"],
        default="cv_isotonic",
        help="Calibration strategy for probability outputs.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    rebuild_canonical_merged()
    snapshots = load_snapshots(config.SNAPSHOTS_PATH)
    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    domain_features = load_domain_features(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, domain_features)
    rules = load_rules(config.RULES_OUTPUT_PATH)

    df_feat = build_feature_table(snapshots, market_feature_cache, domain_features, rules)
    if df_feat.empty:
        print("[ERROR] No feature rows available after rule matching.")
        return

    df_train, df_valid = split_train_valid(df_feat)
    if df_train.empty:
        print("[ERROR] Empty training split.")
        return

    feature_columns = [column for column in df_feat.columns if column not in DROP_COLS]
    payload = fit_model_payload(
        df_train,
        df_valid,
        feature_columns=feature_columns,
        target_column="y",
        calibration_mode=args.calibration_mode,
    )
    save_model(payload)
    export_predictions(payload, df_feat)


if __name__ == "__main__":
    main()
