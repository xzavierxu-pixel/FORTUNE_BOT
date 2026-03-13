from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.datasets.snapshots import load_raw_markets, load_research_snapshots
from rule_baseline.datasets.splits import assign_dataset_split, compute_temporal_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache
from rule_baseline.training.train_snapshot_model import add_training_targets, build_feature_table, load_rules
from rule_baseline.utils import config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate feature-level data quality check artifacts.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    return parser.parse_args()


def load_feature_frame(artifact_mode: str) -> tuple[pd.DataFrame, list[str], dict]:
    artifact_paths = build_artifact_paths(artifact_mode)
    payload = joblib.load(artifact_paths.model_path)

    snapshots = load_research_snapshots()
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_temporal_split(snapshots)
    snapshots = assign_dataset_split(snapshots, split)
    snapshots = snapshots[snapshots["dataset_split"].isin(["train", "valid", "test"])].copy()

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = load_rules(artifact_paths.rules_path)

    df_feat = build_feature_table(snapshots, market_feature_cache, market_annotations, rules)
    if df_feat.empty:
        raise RuntimeError("No feature rows available after rule matching.")
    df_feat = add_training_targets(df_feat)
    return df_feat, list(payload["feature_columns"]), payload


def feature_metadata(feature: str) -> tuple[str, str]:
    if feature in {"horizon_hours", "price", "log_horizon"} or feature.startswith("selected_quote_") or feature in {
        "stale_quote_flag",
        "delta_hours_bucket",
        "snapshot_quality_score",
    }:
        return "快照/报价质量", "描述 snapshot 时点的价格、horizon 和报价质量。"
    if feature.startswith("p_") or feature.startswith("delta_p_") or feature in {
        "term_structure_slope",
        "path_price_mean",
        "path_price_std",
        "path_price_min",
        "path_price_max",
        "path_price_range",
        "price_reversal_flag",
        "price_acceleration",
        "closing_drift",
    }:
        return "期限结构特征", "同一市场不同 horizon 的价格路径和 term structure 派生特征。"
    if feature in {"leaf_id", "price_min", "price_max", "h_min", "h_max", "q_smooth", "rule_score", "direction", "group_key"}:
        return "规则匹配特征", "snapshot 匹配到规则 bucket 后带入模型的规则上下文。"
    if feature in {"domain", "category", "category_raw", "category_parsed", "category_override_flag", "market_type", "sub_domain_market", "source_url_market", "outcome_pattern_market"}:
        return "市场分类特征", "市场来源、分类和 outcome pattern 等标签信息。"
    if feature in {"market_duration_hours", "market_duration_hours_market", "orderPriceMinTickSize", "rewardsMinSize", "rewardsMaxSpread", "line", "groupItemTitle_market", "gameId_market", "has_line"}:
        return "市场静态元数据", "相对静态的 market 结构信息和平台元数据。"
    if feature in {"q_len", "q_chars", "avg_word_len", "max_word_len", "word_diversity", "num_count", "has_number", "has_year", "has_dollar", "has_date", "starts_will", "starts_can", "has_by", "has_above_below", "has_or", "has_and", "punct_count"}:
        return "文本统计特征", "问题文本的长度、数字、日期、句式和标点统计。"
    if feature in {"threshold_max", "threshold_min", "threshold_span", "is_player_prop", "is_team_total", "is_finance_threshold", "is_high_ambiguity"}:
        return "文本阈值/模式特征", "从文本中抽取的阈值、玩法模式和歧义度信号。"
    if feature in {"weak_pos", "outcome_pos", "outcome_neg", "sentiment", "sentiment_abs", "total_sentiment", "certainty", "pos_ratio", "neg_ratio", "sentiment_activity"}:
        return "文本情绪特征", "从 outcome 和措辞中抽取的方向性、确定性和情绪强度。"
    if feature.startswith("cat_") or feature in {"cat_count", "primary_cat_str"}:
        return "关键词类别特征", "按 sports/crypto/politics 等关键词命中的类别强度。"
    if feature in {"log_duration", "dur_very_short", "dur_short", "dur_medium", "dur_long", "engagement_x_duration", "sentiment_x_duration", "vol_x_diversity"}:
        return "持续期/交互特征", "市场持续时间及其与文本活跃度的交互项。"
    if feature.startswith("text_embed_"):
        return "文本哈希向量", "由问题文本生成的哈希 embedding 维度。"
    return "其他特征", "未单独归类的模型输入特征。"


def safe_scalar(value):
    if pd.isna(value):
        return np.nan
    if isinstance(value, (np.floating, float)):
        return float(value)
    if isinstance(value, (np.integer, int)):
        return int(value)
    return str(value)


def describe_numeric(series: pd.Series, prefix: str) -> dict[str, object]:
    stats = series.describe(percentiles=[0.25, 0.5, 0.75])
    return {
        f"{prefix}_count": safe_scalar(stats.get("count")),
        f"{prefix}_mean": safe_scalar(stats.get("mean")),
        f"{prefix}_std": safe_scalar(stats.get("std")),
        f"{prefix}_min": safe_scalar(stats.get("min")),
        f"{prefix}_p25": safe_scalar(stats.get("25%")),
        f"{prefix}_p50": safe_scalar(stats.get("50%")),
        f"{prefix}_p75": safe_scalar(stats.get("75%")),
        f"{prefix}_max": safe_scalar(stats.get("max")),
    }


def describe_categorical(series: pd.Series, prefix: str) -> dict[str, object]:
    non_null = series.dropna().astype(str)
    top = non_null.mode().iloc[0] if not non_null.empty else np.nan
    freq = int((non_null == top).sum()) if pd.notna(top) else 0
    return {
        f"{prefix}_count": int(series.notna().sum()),
        f"{prefix}_unique": int(non_null.nunique()),
        f"{prefix}_top": safe_scalar(top),
        f"{prefix}_freq": freq,
        f"{prefix}_top_share": float(freq / max(len(non_null), 1)),
    }


def build_feature_dqc(df_feat: pd.DataFrame, feature_columns: list[str], payload: dict) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    numeric_features = set(payload.get("numeric_columns", []))
    categorical_features = set(payload.get("categorical_columns", []))

    for feature in feature_columns:
        group_name, meaning = feature_metadata(feature)
        series = df_feat[feature]
        row: dict[str, object] = {
            "feature": feature,
            "feature_group": group_name,
            "feature_meaning": meaning,
            "dtype": str(series.dtype),
            "semantic_type": "numeric" if feature in numeric_features else "categorical" if feature in categorical_features else "unknown",
            "missing_pct_all": float(series.isna().mean()),
        }

        if feature in numeric_features:
            row.update(describe_numeric(pd.to_numeric(series, errors="coerce"), "all"))
        else:
            row.update(describe_categorical(series, "all"))

        split_frames = {
            "train": df_feat[df_feat["dataset_split"] == "train"][feature],
            "valid": df_feat[df_feat["dataset_split"] == "valid"][feature],
            "test": df_feat[df_feat["dataset_split"] == "test"][feature],
        }
        for split_name, split_series in split_frames.items():
            row[f"missing_pct_{split_name}"] = float(split_series.isna().mean())
            if feature in numeric_features:
                row.update(describe_numeric(pd.to_numeric(split_series, errors="coerce"), split_name))
            else:
                row.update(describe_categorical(split_series, split_name))

        if feature in numeric_features:
            train_mean = row.get("train_mean")
            test_mean = row.get("test_mean")
            train_std = row.get("train_std")
            if pd.notna(train_mean) and pd.notna(test_mean):
                row["test_train_mean_gap"] = float(test_mean - train_mean)
            else:
                row["test_train_mean_gap"] = np.nan
            if pd.notna(train_std) and train_std not in {0, 0.0} and pd.notna(row["test_train_mean_gap"]):
                row["test_train_mean_gap_std"] = float(row["test_train_mean_gap"] / train_std)
            else:
                row["test_train_mean_gap_std"] = np.nan
        else:
            row["test_train_top_changed"] = bool(row.get("train_top") != row.get("test_top"))
            row["test_train_unique_gap"] = (
                int(row.get("test_unique", 0)) - int(row.get("train_unique", 0))
            )

        rows.append(row)

    result = pd.DataFrame(rows)
    if "test_train_mean_gap_std" in result.columns:
        result = result.sort_values(
            by=["semantic_type", "test_train_mean_gap_std", "feature"],
            ascending=[True, False, True],
            na_position="last",
        ).reset_index(drop=True)
    return result


def build_numeric_alerts(dqc: pd.DataFrame) -> pd.DataFrame:
    numeric = dqc[dqc["semantic_type"] == "numeric"].copy()
    if numeric.empty:
        return numeric
    numeric["abs_mean_gap_std"] = numeric["test_train_mean_gap_std"].abs()
    return numeric.sort_values("abs_mean_gap_std", ascending=False).reset_index(drop=True)


def safe_write_csv(df: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(path, index=False, encoding="utf-8-sig")
        return path
    except PermissionError:
        fallback = path.with_name(f"{path.stem}.generated{path.suffix}")
        df.to_csv(fallback, index=False, encoding="utf-8-sig")
        return fallback


def main() -> None:
    args = parse_args()
    artifact_paths = build_artifact_paths(args.artifact_mode)
    output_dir = artifact_paths.analysis_dir / "quality_check"
    output_dir.mkdir(parents=True, exist_ok=True)

    df_feat, feature_columns, payload = load_feature_frame(args.artifact_mode)
    dqc = build_feature_dqc(df_feat, feature_columns, payload)
    numeric_alerts = build_numeric_alerts(dqc)

    feature_dqc_path = output_dir / "feature_dqc.csv"
    training_describe_path = artifact_paths.analysis_dir / "training_feature_describe.csv"
    numeric_alerts_path = output_dir / "feature_numeric_drift.csv"

    feature_dqc_written = safe_write_csv(dqc, feature_dqc_path)
    training_describe_written = safe_write_csv(dqc, training_describe_path)
    numeric_alerts_written = safe_write_csv(numeric_alerts, numeric_alerts_path)

    print(f"[INFO] Saved feature DQC to {feature_dqc_written}")
    print(f"[INFO] Updated training feature describe to {training_describe_written}")
    print(f"[INFO] Saved numeric drift ranking to {numeric_alerts_written}")


if __name__ == "__main__":
    main()
