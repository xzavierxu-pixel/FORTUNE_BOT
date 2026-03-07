import os
import sys
from dataclasses import dataclass
from datetime import timedelta

import numpy as np
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from rule_baseline.utils import config
from rule_baseline.utils.data_processing import compute_temporal_split, load_domain_features, load_snapshots

INITIAL_BANKROLL = 10_000.0
TOP_K_RULES = 10
MAX_DAILY_TRADES = 20
BASE_F = 0.01
MAX_POSITION_F = 0.05
MAX_DAILY_EXPOSURE_F = 0.20
FEE_RATE = config.FEE_RATE
MIN_RULE_VALID_N = 200
MIN_EDGE_TRADE = 0.03
MIN_STD_TRADE = 0.10
MIN_PROB_EDGE = 0.02
RULE_ROLLING_WINDOW_TRADES = 50
RULE_KILL_THRESHOLD = -0.2
RULE_COOLDOWN_DAYS = 5
GROWTH_F = 0.05


@dataclass
class BacktestConfig:
    initial_bankroll: float = INITIAL_BANKROLL
    top_k_rules: int = TOP_K_RULES
    max_daily_trades: int = MAX_DAILY_TRADES
    base_f: float = BASE_F
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
    growth_f: float = GROWTH_F


def load_rules() -> pd.DataFrame:
    rules = pd.read_csv(config.RULES_OUTPUT_PATH)
    for column in ["n_valid", "edge_sample_trade", "edge_std_trade", "price_min", "price_max", "h_min", "h_max", "q_smooth"]:
        rules[column] = pd.to_numeric(rules[column], errors="coerce")
    for column in ["domain", "category", "market_type"]:
        rules[column] = rules[column].fillna("UNKNOWN").astype(str)
    rules["direction"] = rules["direction"].astype(int)
    return rules


def prepare_snapshots() -> pd.DataFrame:
    snapshots = load_snapshots(config.SNAPSHOTS_PATH)
    domain_features = load_domain_features(config.MARKET_DOMAIN_FEATURES_PATH)
    snapshots = snapshots.merge(
        domain_features[["market_id", "domain", "category", "market_type"]],
        on="market_id",
        how="left",
        suffixes=("", "_domain"),
    )
    if "category_domain" in snapshots.columns:
        snapshots["category"] = snapshots["category_domain"].fillna(snapshots["category"])
        snapshots = snapshots.drop(columns=["category_domain"])
    for column in ["domain", "category", "market_type"]:
        snapshots[column] = snapshots[column].fillna("UNKNOWN").astype(str)
    return snapshots


def select_top_rules(rules: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    mask = (
        (rules["n_valid"] >= cfg.min_rule_valid_n)
        & (rules["edge_sample_trade"] >= cfg.min_edge_trade)
        & (rules["edge_std_trade"] >= cfg.min_std_trade)
    )
    candidates = rules[mask].copy()
    if candidates.empty:
        raise ValueError("No rules passed filtering.")
    if "rule_score" not in candidates.columns:
        candidates["rule_score"] = (
            candidates["edge_sample_trade"] * np.sqrt(candidates["n_valid"]) * np.clip(candidates["edge_std_trade"], 0.1, None)
        )
    return candidates.sort_values("rule_score", ascending=False).head(cfg.top_k_rules).reset_index(drop=True)


def match_rules_for_day(day_snap, rules, rule_state, current_date):
    active_rules = []
    for _, rule in rules.iterrows():
        rule_key = (rule["group_key"], int(rule["leaf_id"]))
        state = rule_state.get(rule_key)
        if state and state.get("kill_until") and current_date < state["kill_until"]:
            continue
        active_rules.append(rule)
    if not active_rules:
        return pd.DataFrame()

    merged = day_snap.merge(
        pd.DataFrame(active_rules)[
            ["group_key", "domain", "category", "market_type", "leaf_id", "price_min", "price_max", "h_min", "h_max", "rule_score", "q_smooth", "direction"]
        ],
        on=["domain", "category", "market_type"],
        how="inner",
    )
    mask = (
        (merged["price"] >= merged["price_min"] - 1e-9)
        & (merged["price"] <= merged["price_max"] + 1e-9)
        & (merged["horizon_hours"] >= merged["h_min"])
        & (merged["horizon_hours"] <= merged["h_max"])
    )
    matched = merged[mask].copy()
    if matched.empty:
        return matched
    matched = matched.rename(columns={"leaf_id": "rule_leaf_id", "direction": "rule_direction", "group_key": "rule_group_key"})
    return matched


def dedup_by_growth_score(candidates):
    if candidates.empty:
        return candidates
    candidates = candidates.sort_values(["market_id", "snapshot_time", "growth_score"], ascending=[True, True, False])
    return candidates.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)


def compute_growth_and_direction(candidates, cfg):
    if candidates.empty:
        return candidates
    out = candidates.copy()
    q = out["q_smooth"].astype(float).values
    p = out["price"].astype(float).values
    rule_dir = out["rule_direction"].astype(int).values

    edge_prob = q - p
    direction = np.zeros(len(out), dtype=int)
    growth_score = np.full(len(out), -np.inf, dtype=float)

    for idx, (qi, pi, edge, rule_direction) in enumerate(zip(q, p, edge_prob, rule_dir)):
        if not np.isfinite(qi) or not np.isfinite(pi):
            continue
        if rule_direction == 1 and edge > cfg.min_prob_edge:
            odds = (1.0 - pi) / max(pi, 1e-6)
            growth_score[idx] = qi * np.log(1.0 + cfg.growth_f * odds) + (1.0 - qi) * np.log(1.0 - cfg.growth_f)
            direction[idx] = 1
        elif rule_direction == -1 and edge < -cfg.min_prob_edge:
            q_no = 1.0 - qi
            p_no = 1.0 - pi
            odds = (1.0 - p_no) / max(p_no, 1e-6)
            growth_score[idx] = q_no * np.log(1.0 + cfg.growth_f * odds) + (1.0 - q_no) * np.log(1.0 - cfg.growth_f)
            direction[idx] = -1

    out["edge_prob"] = edge_prob
    out["direction_model"] = direction
    out["growth_score"] = growth_score
    return out[(out["direction_model"] != 0) & (out["growth_score"] > 0)].copy()


def trade_pnl(direction, stake, price_yes, y, fee_rate):
    if direction == 1:
        pnl_raw = stake * (y - price_yes) / max(price_yes, 1e-6)
    else:
        price_no = 1.0 - price_yes
        pnl_raw = stake * (price_yes - y) / max(price_no, 1e-6)
    return pnl_raw - fee_rate * stake


def run_backtest(snapshots, rules, cfg):
    bankroll = float(cfg.initial_bankroll)
    equity_records = []
    trade_records = []
    rule_state = {}

    for current_date in sorted(snapshots["snapshot_date"].unique()):
        day_snap = snapshots[snapshots["snapshot_date"] == current_date].copy()
        candidates = match_rules_for_day(day_snap, rules, rule_state, current_date)
        candidates = compute_growth_and_direction(candidates, cfg)
        candidates = dedup_by_growth_score(candidates)
        candidates = candidates.sort_values("growth_score", ascending=False).head(cfg.max_daily_trades)

        bankroll_start = bankroll
        max_daily_exposure = cfg.max_daily_exposure_f * bankroll_start
        daily_pnl = 0.0
        daily_exposure = 0.0
        num_trades = 0

        for _, row in candidates.iterrows():
            stake = min(cfg.base_f, cfg.max_position_f) * bankroll_start
            if daily_exposure + stake > max_daily_exposure:
                break
            pnl = trade_pnl(int(row["direction_model"]), stake, float(row["price"]), int(row["y"]), cfg.fee_rate)
            daily_pnl += pnl
            daily_exposure += stake
            num_trades += 1

            pnl_pct = pnl / stake if stake else 0.0
            rule_key = (row["rule_group_key"], int(row["rule_leaf_id"]))
            state = rule_state.setdefault(rule_key, {"returns": [], "kill_until": None})
            state["returns"].append(pnl_pct)
            if len(state["returns"]) > cfg.rule_rolling_window_trades:
                state["returns"] = state["returns"][-cfg.rule_rolling_window_trades :]
            if len(state["returns"]) >= cfg.rule_rolling_window_trades:
                rolling_sum = float(np.sum(state["returns"]))
                if rolling_sum < cfg.rule_kill_threshold and (state["kill_until"] is None or current_date >= state["kill_until"]):
                    state["kill_until"] = current_date + timedelta(days=cfg.rule_cooldown_days)
                    state["returns"] = []

            trade_records.append(
                {
                    "date": current_date,
                    "snapshot_time": row["snapshot_time"],
                    "market_id": row["market_id"],
                    "domain": row["domain"],
                    "category": row["category"],
                    "market_type": row["market_type"],
                    "price": row["price"],
                    "y": row["y"],
                    "q_smooth": row["q_smooth"],
                    "direction": row["direction_model"],
                    "rule_group_key": row["rule_group_key"],
                    "rule_leaf_id": row["rule_leaf_id"],
                    "growth_score": row["growth_score"],
                    "stake": stake,
                    "pnl": pnl,
                    "pnl_pct_of_stake": pnl_pct,
                }
            )

        bankroll += daily_pnl
        equity_records.append({"date": current_date, "bankroll": bankroll, "daily_pnl": daily_pnl, "num_trades": num_trades})

    return pd.DataFrame(equity_records), pd.DataFrame(trade_records)


def main():
    cfg = BacktestConfig()
    snapshots = prepare_snapshots()
    _, valid_start = compute_temporal_split(snapshots)
    snapshots = snapshots[snapshots["closedTime"] >= valid_start].copy()
    rules = select_top_rules(load_rules(), cfg)
    equity_df, trades_df = run_backtest(snapshots, rules, cfg)

    config.BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    equity_df.to_csv(config.BACKTEST_DIR / "backtest_equity_rules_only.csv", index=False)
    trades_df.to_csv(config.BACKTEST_DIR / "backtest_trades_rules_only.csv", index=False)
    print(f"[INFO] Saved rules-only outputs to {config.BACKTEST_DIR}")


if __name__ == "__main__":
    main()
