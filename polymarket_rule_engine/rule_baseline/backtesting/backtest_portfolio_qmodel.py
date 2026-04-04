from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from datetime import timedelta
from math import sqrt
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.datasets.artifacts import build_artifact_paths, write_json
from rule_baseline.datasets.snapshots import apply_earliest_market_dedup, load_raw_markets, load_research_snapshots
from rule_baseline.datasets.splits import assign_dataset_split, compute_temporal_split
from rule_baseline.domain_extractor.market_annotations import load_market_annotations
from rule_baseline.features import build_market_feature_cache, preprocess_features
from rule_baseline.models import (
    compute_trade_value_from_q as _compute_trade_value_from_q,
    infer_q_from_trade_value as _infer_q_from_trade_value,
    load_model_artifact,
)
from rule_baseline.models.runtime_adapter import ModelArtifactAdapter, build_legacy_adapter
from rule_baseline.utils import config
from rule_baseline.datasets.raw_market_batches import rebuild_canonical_merged

INITIAL_BANKROLL = 10_000.0
TOP_K_RULES = 100
MAX_DAILY_TRADES = 80
MAX_POSITION_F = 0.02
MAX_DAILY_EXPOSURE_F = 0.5
FEE_RATE = config.FEE_RATE
MIN_RULE_VALID_N = 20
MIN_EDGE_TRADE = 0.02
MIN_STD_TRADE = 0.01
MIN_PROB_EDGE = 0.02
RULE_ROLLING_WINDOW_TRADES = 50
RULE_KILL_THRESHOLD = -0.2
RULE_COOLDOWN_DAYS = 5
KELLY_FRACTION = 0.10
REQUIRED_RULE_COLUMNS = {
    "group_key",
    "domain",
    "category",
    "market_type",
    "leaf_id",
    "price_min",
    "price_max",
    "h_min",
    "h_max",
    "direction",
    "q_full",
    "p_full",
    "edge_full",
    "edge_std_full",
    "edge_lower_bound_full",
    "rule_score",
    "n_full",
}


@dataclass
class BacktestConfig:
    initial_bankroll: float = INITIAL_BANKROLL
    top_k_rules: int = TOP_K_RULES
    max_daily_trades: int = MAX_DAILY_TRADES
    max_position_f: float = MAX_POSITION_F
    max_daily_exposure_f: float = MAX_DAILY_EXPOSURE_F
    fee_rate: float = FEE_RATE
    min_rule_valid_n: int = MIN_RULE_VALID_N
    min_edge_trade: float = MIN_EDGE_TRADE
    min_std_trade: float = MIN_STD_TRADE
    min_prob_edge: float = MIN_PROB_EDGE
    rule_rolling_window_trades: int = RULE_ROLLING_WINDOW_TRADES
    rule_kill_threshold: float = RULE_KILL_THRESHOLD
    rule_cooldown_days: int = RULE_COOLDOWN_DAYS
    kelly_fraction: float = KELLY_FRACTION
    min_trade_confidence: float = 0.01
    max_domain_exposure_f: float = config.MAX_DOMAIN_EXPOSURE_F
    max_category_exposure_f: float = config.MAX_CATEGORY_EXPOSURE_F
    max_cluster_exposure_f: float = config.MAX_CLUSTER_EXPOSURE_F
    max_settlement_exposure_f: float = config.MAX_SETTLEMENT_EXPOSURE_F
    max_side_exposure_f: float = config.MAX_SIDE_EXPOSURE_F
    max_trade_liquidity_f: float = config.MAX_TRADE_LIQUIDITY_F
    max_trade_volume24_f: float = config.MAX_TRADE_VOLUME24_F


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run strict OOS backtest for the q-model strategy.")
    parser.add_argument("--artifact-mode", choices=["offline", "online"], default="offline")
    parser.add_argument("--max-rows", type=int, default=None)
    parser.add_argument("--recent-days", type=int, default=None)
    return parser.parse_args()


def load_rules(path) -> pd.DataFrame:
    rules = pd.read_csv(path)
    missing_columns = sorted(REQUIRED_RULE_COLUMNS - set(rules.columns))
    if missing_columns:
        raise ValueError(
            "Rules file does not match the runtime schema. Missing columns: "
            + ", ".join(missing_columns)
        )

    for column in [
        "n_full",
        "edge_full",
        "edge_std_full",
        "rule_score",
        "price_min",
        "price_max",
        "h_min",
        "h_max",
        "q_full",
        "p_full",
        "edge_lower_bound_full",
    ]:
        if column in rules.columns:
            rules[column] = pd.to_numeric(rules[column], errors="coerce")

    for column in ["domain", "category", "market_type"]:
        rules[column] = rules[column].fillna("UNKNOWN").astype(str)
    rules["direction"] = rules["direction"].astype(int)
    return rules


def load_model_payload(model_path):
    return load_model_artifact(model_path)


def compute_trade_value_from_q(candidates: pd.DataFrame, q_pred: np.ndarray) -> np.ndarray:
    return _compute_trade_value_from_q(candidates, q_pred, direction_column="rule_direction")


def infer_q_from_trade_value(candidates: pd.DataFrame, trade_value_pred: np.ndarray) -> np.ndarray:
    return _infer_q_from_trade_value(candidates, trade_value_pred, direction_column="rule_direction")


def derive_domain_whitelist(rules: pd.DataFrame) -> set[str] | None:
    if rules.empty or "edge_lower_bound_full" not in rules.columns:
        return None

    grouped = (
        rules.assign(
            weighted_edge_component=rules["edge_lower_bound_full"].fillna(0.0)
            * rules["n_full"].clip(lower=1).fillna(1.0)
        )
        .groupby("domain", observed=False)
        .agg(
            weighted_edge_sum=("weighted_edge_component", "sum"),
            weight_sum=("n_full", lambda series: series.clip(lower=1).fillna(1.0).sum()),
        )
        .assign(weighted_edge_lower=lambda frame: frame["weighted_edge_sum"] / frame["weight_sum"].clip(lower=1.0))
        ["weighted_edge_lower"]
        .rename("weighted_edge_lower")
        .reset_index()
    )
    whitelist = set(grouped[grouped["weighted_edge_lower"] > 0]["domain"].astype(str))
    return whitelist or None


def rolling_t_stat(values: list[float]) -> float:
    if len(values) < 5:
        return 0.0
    series = np.asarray(values, dtype=float)
    std = float(series.std(ddof=1))
    if std <= 1e-12:
        return 0.0
    return float(series.mean() / (std / sqrt(len(series))))


def select_top_rules(rules: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    mask = (
        (rules["n_full"] >= cfg.min_rule_valid_n)
        & (rules["edge_full"] >= cfg.min_edge_trade)
        & (rules["edge_std_full"] >= cfg.min_std_trade)
    )
    candidates = rules[mask].copy()
    if candidates.empty:
        raise ValueError("No rules passed validation-period filtering thresholds.")

    candidates = candidates.sort_values("rule_score", ascending=False)
    return candidates.head(min(cfg.top_k_rules, len(candidates))).reset_index(drop=True)


def match_rules(snapshots: pd.DataFrame, rules: pd.DataFrame) -> pd.DataFrame:
    merged = snapshots.merge(
        rules[
            [
                "group_key",
                "domain",
                "category",
                "market_type",
                "leaf_id",
                "price_min",
                "price_max",
                "h_min",
                "h_max",
                "rule_score",
                "direction",
                "q_full",
                "edge_full",
                "edge_std_full",
                "edge_lower_bound_full",
            ]
        ],
        on=["domain", "category", "market_type"],
        how="inner",
    )
    if merged.empty:
        return pd.DataFrame()

    mask = (
        (merged["price"] >= merged["price_min"] - 1e-9)
        & (merged["price"] <= merged["price_max"] + 1e-9)
        & (merged["horizon_hours"] >= merged["h_min"])
        & (merged["horizon_hours"] <= merged["h_max"])
    )
    matched = merged[mask].copy()
    if matched.empty:
        return pd.DataFrame()

    matched = matched.rename(
        columns={
            "leaf_id": "rule_leaf_id",
            "direction": "rule_direction",
            "group_key": "rule_group_key",
        }
    )
    return matched.reset_index(drop=True)


def _resolve_model_artifact(model_artifact) -> ModelArtifactAdapter:
    if isinstance(model_artifact, ModelArtifactAdapter):
        return model_artifact
    if isinstance(model_artifact, dict):
        return build_legacy_adapter(model_artifact, Path("<in-memory>"))
    raise TypeError(f"Unsupported model artifact type: {type(model_artifact)!r}")


def predict_candidates(candidates: pd.DataFrame, market_feature_cache: pd.DataFrame, payload) -> pd.DataFrame:
    model_input = candidates.copy()
    model_input["leaf_id"] = model_input["rule_leaf_id"]
    model_input["direction"] = model_input["rule_direction"]
    model_input["group_key"] = model_input["rule_group_key"]
    df_feat = preprocess_features(model_input, market_feature_cache)
    out = candidates.copy()
    model_artifact = _resolve_model_artifact(payload)
    target_mode = model_artifact.target_mode
    supplemental_cols = [
        "source_host",
        "selected_quote_offset_sec",
        "snapshot_quality_score",
        "closedTime",
        "scheduled_end",
    ]
    for column in supplemental_cols:
        if column in df_feat.columns and column not in out.columns:
            out[column] = df_feat[column].values

    out["q_pred"], out["trade_value_pred"] = model_artifact.predict_outputs(out, df_feat)
    return out


def compute_growth_and_direction(candidates: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    out = candidates.copy()
    q = out["q_pred"].astype(float).values
    p = out["price"].astype(float).values
    rule_dir = out["rule_direction"].astype(int).values
    trade_value_pred = out["trade_value_pred"].astype(float).values
    confidence_discount = np.ones(len(out), dtype=float)
    if "edge_lower_bound_full" in out.columns and "edge_full" in out.columns:
        edge_full = out["edge_full"].astype(float).replace(0.0, np.nan)
        confidence_discount = (
            out["edge_lower_bound_full"].astype(float) / edge_full
        ).clip(lower=0.95, upper=1.0).fillna(0.95).values

    edge_prob = q - p
    direction = np.zeros(len(out), dtype=int)
    f_star = np.zeros(len(out), dtype=float)
    f_exec = np.zeros(len(out), dtype=float)
    g_net = np.full(len(out), -np.inf, dtype=float)
    growth_score = np.full(len(out), -np.inf, dtype=float)

    loss_return = -1.0 - cfg.fee_rate

    for idx, (qi, pi, edge, rule_direction, trade_value_est, discount) in enumerate(
        zip(q, p, edge_prob, rule_dir, trade_value_pred, confidence_discount)
    ):
        if not np.isfinite(qi) or not np.isfinite(pi) or pi <= 0.0 or pi >= 1.0:
            continue
        effective_trade_value = trade_value_est * discount
        if not np.isfinite(effective_trade_value) or effective_trade_value <= 0:
            continue

        if rule_direction == 1:
            if not (edge > cfg.min_prob_edge and qi >= cfg.min_trade_confidence):
                continue
            win_return = (1.0 - pi) / max(pi, 1e-6) - cfg.fee_rate
            q_win = qi
            direction_value = 1
        elif rule_direction == -1:
            if not (edge < -cfg.min_prob_edge and (1.0 - qi) >= cfg.min_trade_confidence):
                continue
            win_return = pi / max(1.0 - pi, 1e-6) - cfg.fee_rate
            q_win = 1.0 - qi
            direction_value = -1
        else:
            continue

        if win_return <= 0:
            continue

        expected_return = q_win * win_return + (1.0 - q_win) * loss_return
        expected_return = min(expected_return, effective_trade_value)
        if expected_return <= 0:
            continue

        denom = win_return * loss_return
        if denom == 0:
            continue

        f_opt = -expected_return / denom
        if not np.isfinite(f_opt) or f_opt <= 0:
            continue

        f_position = min(cfg.kelly_fraction * f_opt, cfg.max_position_f)
        if 1.0 + f_position * loss_return <= 0:
            continue

        g_value = q_win * np.log1p(f_position * win_return) + (1.0 - q_win) * np.log1p(f_position * loss_return)
        if not np.isfinite(g_value) or g_value <= 0:
            continue

        direction[idx] = direction_value
        f_star[idx] = f_opt
        f_exec[idx] = f_position
        g_net[idx] = g_value
        growth_score[idx] = g_value / max(f_position, 1e-12)

    out["edge_prob"] = edge_prob
    out["direction_model"] = direction
    out["edge_final"] = np.where(direction > 0, edge_prob, np.where(direction < 0, -edge_prob, -np.inf))
    out["f_star"] = f_star
    out["f_exec"] = f_exec
    out["g_net"] = g_net
    out["growth_score"] = growth_score
    return out[
        (out["direction_model"] != 0)
        & np.isfinite(out["growth_score"])
        & (out["growth_score"] > 0)
        & np.isfinite(out["edge_final"])
        & (out["edge_final"] > 0)
    ].copy()


def prepare_candidate_book(
    snapshots: pd.DataFrame,
    rules: pd.DataFrame,
    market_feature_cache: pd.DataFrame,
    payload: dict,
    cfg: BacktestConfig,
) -> pd.DataFrame:
    matched = match_rules(snapshots, rules)
    if matched.empty:
        return matched

    domain_whitelist = derive_domain_whitelist(rules)
    if domain_whitelist is not None:
        matched = matched[matched["domain"].astype(str).isin(domain_whitelist)].copy()
    if matched.empty:
        return matched

    matched = matched.sort_values(
        ["market_id", "snapshot_time", "rule_score"],
        ascending=[True, True, False],
    )
    matched = matched.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)
    scored = predict_candidates(matched, market_feature_cache, payload)
    scored = compute_growth_and_direction(scored, cfg)
    if scored.empty:
        return scored

    scored = scored.sort_values(
        ["market_id", "snapshot_time", "edge_final"],
        ascending=[True, True, False],
    )
    scored = scored.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)
    scored = apply_earliest_market_dedup(scored, score_column="edge_final")
    return scored.sort_values(["snapshot_time", "edge_final"], ascending=[True, False]).reset_index(drop=True)


def trade_pnl(direction: int, stake: float, price_yes: float, y: int, fee_rate: float) -> float:
    if direction == 1:
        pnl_raw = stake * (y - price_yes) / max(price_yes, 1e-6)
    else:
        price_no = 1.0 - price_yes
        pnl_raw = stake * (price_yes - y) / max(price_no, 1e-6)
    return pnl_raw - fee_rate * stake


def run_backtest(candidates: pd.DataFrame, cfg: BacktestConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    cash = float(cfg.initial_bankroll)
    equity_records: list[dict] = []
    trade_records: list[dict] = []
    rule_state: dict = {}
    pending_by_settlement: dict = {}

    if candidates.empty:
        return pd.DataFrame(), pd.DataFrame()

    decision_dates = pd.to_datetime(candidates["snapshot_date"], errors="coerce").dt.date
    settlement_dates = pd.to_datetime(candidates["closedTime"], utc=True, errors="coerce").dt.date
    candidates = candidates.copy()
    candidates["snapshot_date"] = decision_dates
    candidates["settlement_date"] = settlement_dates
    all_dates = sorted(set(decision_dates.dropna().unique()) | set(settlement_dates.dropna().unique()))
    for current_date in all_dates:
        settled_today = pending_by_settlement.pop(current_date, [])
        realized_pnl = 0.0
        for settled in settled_today:
            cash += float(settled["stake"]) + float(settled["pnl"])
            realized_pnl += float(settled["pnl"])

            pnl_pct = float(settled["pnl"]) / float(settled["stake"]) if float(settled["stake"]) else 0.0
            rule_key = (settled["rule_group_key"], int(settled["rule_leaf_id"]))
            state = rule_state.setdefault(rule_key, {"returns": [], "kill_until": None})
            state["returns"].append(pnl_pct)
            if len(state["returns"]) > cfg.rule_rolling_window_trades:
                state["returns"] = state["returns"][-cfg.rule_rolling_window_trades :]

            if len(state["returns"]) >= cfg.rule_rolling_window_trades:
                mean_return = float(np.mean(state["returns"]))
                t_stat = rolling_t_stat(state["returns"])
                if mean_return < cfg.rule_kill_threshold / max(cfg.rule_rolling_window_trades, 1) and t_stat < -2.0:
                    state["kill_until"] = current_date + timedelta(days=cfg.rule_cooldown_days)
                    state["returns"] = []

        day_candidates = candidates[candidates["snapshot_date"] == current_date].copy()
        open_positions = [item for items in pending_by_settlement.values() for item in items]
        equity_start = cash + sum(float(item["stake"]) for item in open_positions)
        if day_candidates.empty:
            equity_records.append({"date": current_date, "bankroll": equity_start, "daily_pnl": realized_pnl, "num_trades": 0})
            continue

        active_rows = []
        for _, row in day_candidates.iterrows():
            rule_key = (row["rule_group_key"], int(row["rule_leaf_id"]))
            state = rule_state.get(rule_key)
            if state and state.get("kill_until") and current_date < state["kill_until"]:
                continue
            active_rows.append(row)

        if not active_rows:
            equity_records.append({"date": current_date, "bankroll": equity_start, "daily_pnl": realized_pnl, "num_trades": 0})
            continue

        day_candidates = pd.DataFrame(active_rows).sort_values("edge_final", ascending=False).head(cfg.max_daily_trades)
        bankroll_start = equity_start
        remaining_budget = min(cfg.max_daily_exposure_f * bankroll_start, cash)
        daily_pnl = realized_pnl
        num_trades = 0
        exposure_by_domain: dict[str, float] = {}
        exposure_by_category: dict[str, float] = {}
        exposure_by_cluster: dict[str, float] = {}
        exposure_by_settlement: dict[str, float] = {}
        exposure_by_side: dict[str, float] = {}
        for pending in open_positions:
            exposure_by_domain[str(pending["domain"])] = exposure_by_domain.get(str(pending["domain"]), 0.0) + float(pending["stake"])
            exposure_by_category[str(pending["category"])] = exposure_by_category.get(str(pending["category"]), 0.0) + float(pending["stake"])
            exposure_by_cluster[str(pending["cluster_key"])] = exposure_by_cluster.get(str(pending["cluster_key"]), 0.0) + float(pending["stake"])
            exposure_by_settlement[str(pending["settlement_key"])] = exposure_by_settlement.get(str(pending["settlement_key"]), 0.0) + float(pending["stake"])
            exposure_by_side[str(pending["direction_label"])] = exposure_by_side.get(str(pending["direction_label"]), 0.0) + float(pending["stake"])

        for _, row in day_candidates.iterrows():
            if remaining_budget <= 0:
                break

            direction_label = "YES" if int(row["direction_model"]) > 0 else "NO"
            settlement_ts = pd.to_datetime(row.get("closedTime"), utc=True, errors="coerce")
            settlement_key = settlement_ts.date().isoformat() if pd.notna(settlement_ts) else "UNKNOWN"
            settlement_date = settlement_ts.date() if pd.notna(settlement_ts) else current_date
            cluster_key = f"{row.get('source_host', 'UNKNOWN')}|{row['category']}|{settlement_key}"
            domain_room = cfg.max_domain_exposure_f * bankroll_start - exposure_by_domain.get(str(row["domain"]), 0.0)
            category_room = cfg.max_category_exposure_f * bankroll_start - exposure_by_category.get(str(row["category"]), 0.0)
            cluster_room = cfg.max_cluster_exposure_f * bankroll_start - exposure_by_cluster.get(cluster_key, 0.0)
            settlement_room = cfg.max_settlement_exposure_f * bankroll_start - exposure_by_settlement.get(settlement_key, 0.0)
            side_room = cfg.max_side_exposure_f * bankroll_start - exposure_by_side.get(direction_label, 0.0)

            stake = min(
                float(row["f_exec"]) * bankroll_start,
                remaining_budget,
                domain_room,
                category_room,
                cluster_room,
                settlement_room,
                side_room,
            )
            if stake <= 0:
                continue

            pnl = trade_pnl(int(row["direction_model"]), stake, float(row["price"]), int(row["y"]), cfg.fee_rate)
            cash -= stake
            remaining_budget -= stake
            exposure_by_domain[str(row["domain"])] = exposure_by_domain.get(str(row["domain"]), 0.0) + stake
            exposure_by_category[str(row["category"])] = exposure_by_category.get(str(row["category"]), 0.0) + stake
            exposure_by_cluster[cluster_key] = exposure_by_cluster.get(cluster_key, 0.0) + stake
            exposure_by_settlement[settlement_key] = exposure_by_settlement.get(settlement_key, 0.0) + stake
            exposure_by_side[direction_label] = exposure_by_side.get(direction_label, 0.0) + stake
            num_trades += 1

            pnl_pct = pnl / stake if stake else 0.0
            pending_trade = {
                "stake": float(stake),
                "pnl": float(pnl),
                "domain": str(row["domain"]),
                "category": str(row["category"]),
                "cluster_key": cluster_key,
                "settlement_key": settlement_key,
                "direction_label": direction_label,
                "rule_group_key": row["rule_group_key"],
                "rule_leaf_id": int(row["rule_leaf_id"]),
            }
            pending_by_settlement.setdefault(settlement_date, []).append(pending_trade)

            trade_records.append(
                {
                    "date": current_date,
                    "settlement_date": settlement_date,
                    "snapshot_time": row["snapshot_time"],
                    "market_id": row["market_id"],
                    "domain": row["domain"],
                    "category": row["category"],
                    "market_type": row["market_type"],
                    "horizon_hours": row["horizon_hours"],
                    "price": float(row["price"]),
                    "y": int(row["y"]),
                    "q_pred": float(row["q_pred"]),
                    "trade_value_pred": float(row.get("trade_value_pred", np.nan)),
                    "edge_prob": float(row["edge_prob"]),
                    "edge_final": float(row["edge_final"]),
                    "direction": int(row["direction_model"]),
                    "rule_group_key": row["rule_group_key"],
                    "rule_leaf_id": int(row["rule_leaf_id"]),
                    "rule_score": float(row.get("rule_score", np.nan)),
                    "growth_score": float(row["growth_score"]),
                    "g_net": float(row["g_net"]),
                    "f_star": float(row["f_star"]),
                    "f_exec": float(row["f_exec"]),
                    "stake": float(stake),
                    "pnl": float(pnl),
                    "pnl_pct_of_stake": float(pnl_pct),
                    "earliest_only": True,
                }
            )

        ending_equity = cash + sum(float(item["stake"]) for items in pending_by_settlement.values() for item in items)
        equity_records.append(
            {"date": current_date, "bankroll": ending_equity, "daily_pnl": daily_pnl, "num_trades": num_trades}
        )

    return pd.DataFrame(equity_records), pd.DataFrame(trade_records)


def compute_summary(equity_df: pd.DataFrame, trades_df: pd.DataFrame, cfg: BacktestConfig) -> dict[str, float | int | None]:
    if equity_df.empty:
        return {"total_trades": 0}

    final_bankroll = equity_df["bankroll"].iloc[-1]
    total_pnl = final_bankroll - cfg.initial_bankroll
    roi = total_pnl / cfg.initial_bankroll if cfg.initial_bankroll else 0.0

    equity = equity_df["bankroll"].values
    peak = np.maximum.accumulate(equity)
    drawdown = equity - peak
    max_dd = float(drawdown.min()) if len(drawdown) else 0.0
    max_dd_pct = float((equity / peak - 1.0).min()) if len(peak) else 0.0

    total_trades = len(trades_df)
    win_rate = float((trades_df["pnl"] > 0).mean()) if total_trades else None

    summary = {
        "initial_bankroll": float(cfg.initial_bankroll),
        "final_bankroll": float(final_bankroll),
        "total_pnl": float(total_pnl),
        "total_roi": float(roi),
        "max_drawdown": max_dd,
        "max_drawdown_pct": max_dd_pct,
        "total_trades": int(total_trades),
        "win_rate": win_rate,
    }

    print("\n========== BACKTEST SUMMARY ==========")
    for key, value in summary.items():
        print(f"{key:18s}: {value}")
    print("======================================\n")
    return summary


def summarize_rules(trades_df: pd.DataFrame, output_path) -> None:
    if trades_df.empty:
        pd.DataFrame(columns=["rule_group_key", "rule_leaf_id", "num_trades", "total_pnl", "total_stake", "roi"]).to_csv(
            output_path,
            index=False,
        )
        return

    rows = []
    for (group_key, leaf_id), group in trades_df.groupby(["rule_group_key", "rule_leaf_id"]):
        total_pnl = group["pnl"].sum()
        total_stake = group["stake"].sum()
        roi = total_pnl / total_stake if total_stake else 0.0
        rows.append(
            {
                "rule_group_key": group_key,
                "rule_leaf_id": int(leaf_id),
                "num_trades": len(group),
                "total_pnl": float(total_pnl),
                "total_stake": float(total_stake),
                "roi": float(roi),
            }
        )

    pd.DataFrame(rows).sort_values("total_pnl", ascending=False).to_csv(output_path, index=False)


def main() -> None:
    args = parse_args()
    if args.artifact_mode != "offline":
        raise ValueError("Backtesting is only supported for offline artifacts.")

    cfg = BacktestConfig()
    artifact_paths = build_artifact_paths(args.artifact_mode)
    rebuild_canonical_merged()

    snapshots = load_research_snapshots(max_rows=args.max_rows, recent_days=args.recent_days)
    snapshots = snapshots[snapshots["quality_pass"]].copy()
    split = compute_temporal_split(snapshots)
    snapshots = assign_dataset_split(snapshots, split)
    snapshots = snapshots[snapshots["dataset_split"] == "test"].copy()
    if snapshots.empty:
        raise RuntimeError("No strict test-period snapshots available.")

    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_annotations = load_market_annotations(config.MARKET_DOMAIN_FEATURES_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, market_annotations)
    rules = select_top_rules(load_rules(artifact_paths.rules_path), cfg)
    payload = load_model_payload(artifact_paths.model_path)

    candidate_book = prepare_candidate_book(snapshots, rules, market_feature_cache, payload, cfg)
    artifact_paths.backtest_dir.mkdir(parents=True, exist_ok=True)
    equity_path = artifact_paths.backtest_dir / "backtest_equity_qmodel.csv"
    trades_path = artifact_paths.backtest_dir / "backtest_trades_qmodel.csv"
    rules_path = artifact_paths.backtest_dir / "rule_performance_qmodel.csv"

    if candidate_book.empty:
        equity_df = pd.DataFrame(columns=["date", "bankroll", "daily_pnl", "num_trades"])
        trades_df = pd.DataFrame(columns=["date", "snapshot_time", "market_id", "stake", "pnl"])
        equity_df.to_csv(equity_path, index=False)
        trades_df.to_csv(trades_path, index=False)
        summarize_rules(trades_df, rules_path)
        summary = {"candidate_markets": 0, "candidate_rows": 0, "total_trades": 0}
    else:
        equity_df, trades_df = run_backtest(candidate_book, cfg)
        equity_df.to_csv(equity_path, index=False)
        trades_df.to_csv(trades_path, index=False)
        summarize_rules(trades_df, rules_path)
        summary = compute_summary(equity_df, trades_df, cfg)
        summary["candidate_markets"] = int(candidate_book["market_id"].nunique())
        summary["candidate_rows"] = int(len(candidate_book))

    summary["split_boundaries"] = split.to_dict()
    summary["debug_filters"] = {"max_rows": args.max_rows, "recent_days": args.recent_days}
    write_json(artifact_paths.metadata_dir / "backtest_summary.json", summary)

    print(f"[INFO] Saved equity curve to {equity_path}")
    print(f"[INFO] Saved trade log to {trades_path}")
    print(f"[INFO] Saved rule summary to {rules_path}")


if __name__ == "__main__":
    main()
