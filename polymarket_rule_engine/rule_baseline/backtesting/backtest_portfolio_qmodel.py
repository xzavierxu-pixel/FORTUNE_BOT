import os
import sys
from dataclasses import dataclass
from datetime import timedelta

import joblib
import numpy as np
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
from rule_baseline.utils.modeling import predict_probabilities
from rule_baseline.utils.raw_batches import rebuild_canonical_merged

INITIAL_BANKROLL = 10_000.0
TOP_K_RULES = 100
MAX_DAILY_TRADES = 300
BASE_F = 0.01
MAX_POSITION_F = 0.02
MAX_DAILY_EXPOSURE_F = 1.0
FEE_RATE = config.FEE_RATE
MIN_RULE_VALID_N = 100
MIN_EDGE_TRADE = 0.02
MIN_STD_TRADE = 0.01
MIN_PROB_EDGE = 0.05
RULE_ROLLING_WINDOW_TRADES = 50
RULE_KILL_THRESHOLD = -0.2
RULE_COOLDOWN_DAYS = 5
KELLY_FRACTION = 0.25


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
    kelly_fraction: float = KELLY_FRACTION
    min_trade_confidence: float = 0.01


def load_rules(path=None) -> pd.DataFrame:
    target = path or config.RULES_OUTPUT_PATH
    rules = pd.read_csv(target)
    for column in [
        "n_train",
        "n_valid",
        "edge_sample_trade",
        "edge_std_trade",
        "roi_trade",
        "rule_score",
        "price_min",
        "price_max",
        "h_min",
        "h_max",
        "q_smooth",
    ]:
        if column in rules.columns:
            rules[column] = pd.to_numeric(rules[column], errors="coerce")

    for column in ["domain", "category", "market_type"]:
        rules[column] = rules[column].fillna("UNKNOWN").astype(str)
    rules["direction"] = rules["direction"].astype(int)
    return rules


def load_model_payload():
    if not config.MODEL_PATH.exists():
        raise FileNotFoundError(f"Model file not found at {config.MODEL_PATH}.")
    return joblib.load(config.MODEL_PATH)


def prepare_snapshots() -> tuple[pd.DataFrame, pd.DataFrame]:
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

    snapshots["domain"] = snapshots.get("domain", "UNKNOWN").fillna("UNKNOWN").astype(str)
    snapshots["category"] = snapshots.get("category", "UNKNOWN").fillna("UNKNOWN").astype(str)
    snapshots["market_type"] = snapshots.get("market_type", "UNKNOWN").fillna("UNKNOWN").astype(str)
    return snapshots, domain_features


def select_top_rules(rules: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    mask = (
        (rules["n_valid"] >= cfg.min_rule_valid_n)
        & (rules["edge_sample_trade"] >= cfg.min_edge_trade)
        & (rules["edge_std_trade"] >= cfg.min_std_trade)
    )
    candidates = rules[mask].copy()
    if candidates.empty:
        raise ValueError("No rules passed filtering thresholds.")

    if "rule_score" not in candidates.columns:
        candidates["rule_score"] = (
            candidates["edge_sample_trade"]
            * np.sqrt(candidates["n_valid"])
            * np.clip(candidates["edge_std_trade"], 0.1, None)
        )

    candidates = candidates.sort_values("rule_score", ascending=False)
    return candidates.head(min(cfg.top_k_rules, len(candidates))).reset_index(drop=True)


def match_rules_for_day(day_snap: pd.DataFrame, rules: pd.DataFrame, rule_state: dict, current_date, cfg) -> pd.DataFrame:
    if day_snap.empty or rules.empty:
        return pd.DataFrame()

    active_rules = []
    for _, rule in rules.iterrows():
        rule_key = (rule["group_key"], int(rule["leaf_id"]))
        state = rule_state.get(rule_key)
        if state and state.get("kill_until") and current_date < state["kill_until"]:
            continue
        active_rules.append(rule)

    if not active_rules:
        return pd.DataFrame()

    day_snap = day_snap.copy()
    for column in ["domain", "category", "market_type"]:
        day_snap[column] = day_snap[column].astype(str)

    rules_df = pd.DataFrame(active_rules)
    merged = day_snap.merge(
        rules_df[
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
                "q_smooth",
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


def predict_q_pred(candidates: pd.DataFrame, market_feature_cache: pd.DataFrame, payload: dict) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    model_input = candidates.copy()
    rename_map = {
        "rule_leaf_id": "leaf_id",
        "rule_direction": "direction",
        "rule_group_key": "group_key",
    }
    for source_col, target_col in rename_map.items():
        if source_col in model_input.columns and target_col not in model_input.columns:
            model_input[target_col] = model_input[source_col]

    df_feat = preprocess_features(model_input, market_feature_cache)
    q_pred = predict_probabilities(payload, df_feat)

    out = candidates.copy()
    out["q_pred"] = q_pred
    return out


def dedup_by_growth_score(candidates: pd.DataFrame) -> pd.DataFrame:
    if candidates.empty:
        return candidates
    candidates = candidates.sort_values(
        ["market_id", "snapshot_time", "growth_score"],
        ascending=[True, True, False],
    )
    return candidates.drop_duplicates(subset=["market_id", "snapshot_time"], keep="first").reset_index(drop=True)


def compute_growth_and_direction(candidates: pd.DataFrame, cfg: BacktestConfig) -> pd.DataFrame:
    if candidates.empty:
        return candidates

    out = candidates.copy()
    q = out["q_pred"].astype(float).values
    p = out["price"].astype(float).values
    rule_dir = out["rule_direction"].astype(int).values

    edge_prob = q - p
    direction = np.zeros(len(out), dtype=int)
    f_star = np.zeros(len(out), dtype=float)
    f_exec = np.zeros(len(out), dtype=float)
    g_net = np.full(len(out), -np.inf, dtype=float)
    growth_score = np.full(len(out), -np.inf, dtype=float)

    min_edge = float(cfg.min_prob_edge)
    fee_rate = float(cfg.fee_rate)
    kelly_fraction = float(cfg.kelly_fraction)
    loss_return = -1.0 - fee_rate

    for idx, (qi, pi, edge, rule_direction) in enumerate(zip(q, p, edge_prob, rule_dir)):
        if not np.isfinite(qi) or not np.isfinite(pi) or pi <= 0.0 or pi >= 1.0:
            continue

        if rule_direction == 1:
            if not (edge > min_edge and qi >= cfg.min_trade_confidence):
                continue
            win_return = (1.0 - pi) / max(pi, 1e-6) - fee_rate
            q_win = qi
            direction_value = 1
        elif rule_direction == -1:
            if not (edge < -min_edge and (1.0 - qi) >= cfg.min_trade_confidence):
                continue
            win_return = pi / max(1.0 - pi, 1e-6) - fee_rate
            q_win = 1.0 - qi
            direction_value = -1
        else:
            continue

        if win_return <= 0:
            continue

        expected_return = q_win * win_return + (1.0 - q_win) * loss_return
        if expected_return <= 0:
            continue

        denom = win_return * loss_return
        if denom == 0:
            continue

        f_opt = -expected_return / denom
        if not np.isfinite(f_opt) or f_opt <= 0:
            continue

        f_position = min(kelly_fraction * f_opt, cfg.max_position_f)
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
    out["f_star"] = f_star
    out["f_exec"] = f_exec
    out["g_net"] = g_net
    out["growth_score"] = growth_score

    return out[
        (out["direction_model"] != 0) & np.isfinite(out["growth_score"]) & (out["growth_score"] > 0)
    ].copy()


def trade_pnl(direction: int, stake: float, price_yes: float, y: int, fee_rate: float) -> float:
    price_yes = float(price_yes)
    y = int(y)
    if direction == 1:
        pnl_raw = stake * (y - price_yes) / max(price_yes, 1e-6)
    else:
        price_no = 1.0 - price_yes
        pnl_raw = stake * (price_yes - y) / max(price_no, 1e-6)
    return pnl_raw - fee_rate * stake


def run_backtest(snapshots, rules, market_feature_cache, payload, cfg):
    bankroll = float(cfg.initial_bankroll)
    equity_records = []
    trade_records = []
    all_dates = sorted(snapshots["snapshot_date"].unique())
    rule_state = {}

    for current_date in all_dates:
        day_snap = snapshots[snapshots["snapshot_date"] == current_date].copy()
        if day_snap.empty:
            equity_records.append({"date": current_date, "bankroll": bankroll, "daily_pnl": 0.0, "num_trades": 0})
            continue

        candidates = match_rules_for_day(day_snap, rules, rule_state, current_date, cfg)
        if candidates.empty:
            equity_records.append({"date": current_date, "bankroll": bankroll, "daily_pnl": 0.0, "num_trades": 0})
            continue

        candidates = predict_q_pred(candidates, market_feature_cache, payload)
        candidates = compute_growth_and_direction(candidates, cfg)
        if candidates.empty:
            equity_records.append({"date": current_date, "bankroll": bankroll, "daily_pnl": 0.0, "num_trades": 0})
            continue

        candidates = dedup_by_growth_score(candidates)
        candidates = candidates.sort_values("growth_score", ascending=False).head(cfg.max_daily_trades)

        bankroll_start = bankroll
        max_daily_exposure = cfg.max_daily_exposure_f * bankroll_start
        daily_pnl = 0.0
        daily_exposure = 0.0
        num_trades = 0

        for _, row in candidates.iterrows():
            f_exec = min(max(float(row.get("f_exec", cfg.base_f)), 0.0), cfg.max_position_f)
            stake = f_exec * bankroll_start
            if stake <= 0:
                continue
            if daily_exposure + stake > max_daily_exposure:
                break

            direction = int(row["direction_model"])
            pnl = trade_pnl(direction, stake, float(row["price"]), int(row["y"]), cfg.fee_rate)
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
                if rolling_sum < cfg.rule_kill_threshold and (
                    state["kill_until"] is None or current_date >= state["kill_until"]
                ):
                    state["kill_until"] = current_date + timedelta(days=cfg.rule_cooldown_days)
                    state["returns"] = []
                    print(
                        f"[KILL] Rule ({row['rule_group_key']}, leaf {int(row['rule_leaf_id'])}) "
                        f"paused until {state['kill_until']} (rolling ROI sum={rolling_sum:.3f})"
                    )

            trade_records.append(
                {
                    "date": current_date,
                    "snapshot_time": row["snapshot_time"],
                    "market_id": row["market_id"],
                    "domain": row["domain"],
                    "category": row["category"],
                    "market_type": row["market_type"],
                    "horizon_hours": row["horizon_hours"],
                    "price": float(row["price"]),
                    "y": int(row["y"]),
                    "q_pred": float(row["q_pred"]),
                    "edge_prob": float(row["edge_prob"]),
                    "direction": direction,
                    "rule_group_key": row["rule_group_key"],
                    "rule_leaf_id": int(row["rule_leaf_id"]),
                    "rule_score": float(row.get("rule_score", np.nan)),
                    "growth_score": float(row.get("growth_score", np.nan)),
                    "g_net": float(row.get("g_net", np.nan)),
                    "f_star": float(row.get("f_star", np.nan)),
                    "f_exec": f_exec,
                    "stake": stake,
                    "pnl": pnl,
                    "pnl_pct_of_stake": pnl_pct,
                }
            )

        bankroll += daily_pnl
        equity_records.append(
            {"date": current_date, "bankroll": bankroll, "daily_pnl": daily_pnl, "num_trades": num_trades}
        )

    return pd.DataFrame(equity_records), pd.DataFrame(trade_records)


def compute_summary(equity_df: pd.DataFrame, trades_df: pd.DataFrame, cfg: BacktestConfig):
    if equity_df.empty:
        print("[WARN] Empty equity curve.")
        return

    final_bankroll = equity_df["bankroll"].iloc[-1]
    total_pnl = final_bankroll - cfg.initial_bankroll
    roi = total_pnl / cfg.initial_bankroll if cfg.initial_bankroll else 0.0

    equity = equity_df["bankroll"].values
    peak = np.maximum.accumulate(equity)
    drawdown = equity - peak
    max_dd = drawdown.min()
    max_dd_pct = max_dd / peak[np.argmin(drawdown)] if peak.size > 0 else 0.0

    total_trades = len(trades_df)
    if total_trades > 0:
        win_rate = (trades_df["pnl"] > 0).mean()
        per_trade_ret = trades_df["pnl_pct_of_stake"].replace([np.inf, -np.inf], np.nan).dropna()
        sharpe = (
            per_trade_ret.mean() / per_trade_ret.std() * np.sqrt(len(per_trade_ret))
            if len(per_trade_ret) > 1 and per_trade_ret.std() > 0
            else np.nan
        )
    else:
        win_rate = np.nan
        sharpe = np.nan

    print("\n========== BACKTEST SUMMARY ==========")
    print(f"Initial bankroll : {cfg.initial_bankroll:,.2f}")
    print(f"Final bankroll   : {final_bankroll:,.2f}")
    print(f"Total PnL        : {total_pnl:,.2f}")
    print(f"Total ROI        : {roi * 100:,.2f}%")
    print(f"Max Drawdown     : {max_dd:,.2f} ({max_dd_pct * 100:,.2f}%)")
    print(f"Total trades     : {total_trades}")
    print(f"Win rate         : {win_rate * 100:,.2f}%")
    print(f"Per-trade Sharpe : {sharpe:,.3f}")
    print("======================================\n")


def summarize_rules(trades_df: pd.DataFrame):
    if trades_df.empty:
        print("[WARN] No trades for rule summary.")
        return

    rows = []
    for (group_key, leaf_id), group in trades_df.groupby(["rule_group_key", "rule_leaf_id"]):
        total_pnl = group["pnl"].sum()
        total_stake = group["stake"].sum()
        roi = total_pnl / total_stake if total_stake else 0.0
        returns = group["pnl_pct_of_stake"].replace([np.inf, -np.inf], np.nan).dropna().values
        if returns.size > 0:
            equity = np.cumprod(1.0 + returns)
            peak = np.maximum.accumulate(equity)
            drawdown = equity / peak - 1.0
            max_drawdown = drawdown.min()
            sharpe = np.mean(returns) / np.std(returns) * np.sqrt(returns.size) if returns.size > 1 and np.std(returns) > 0 else np.nan
        else:
            max_drawdown = np.nan
            sharpe = np.nan

        rows.append(
            {
                "rule_group_key": group_key,
                "rule_leaf_id": int(leaf_id),
                "num_trades": len(group),
                "total_pnl": total_pnl,
                "total_stake": total_stake,
                "roi": roi,
                "max_drawdown": max_drawdown,
                "sharpe": sharpe,
            }
        )

    pd.DataFrame(rows).to_csv(config.BACKTEST_DIR / "rule_performance_qmodel.csv", index=False)
    print(f"[INFO] Saved rule-level performance to {config.BACKTEST_DIR / 'rule_performance_qmodel.csv'}")


def main():
    cfg = BacktestConfig()
    rebuild_canonical_merged()

    snapshots, domain_features = prepare_snapshots()
    raw_markets = load_raw_markets(config.RAW_MERGED_PATH)
    market_feature_cache = build_market_feature_cache(raw_markets, domain_features)
    rules = load_rules(config.RULES_OUTPUT_PATH)
    payload = load_model_payload()

    _, valid_start = compute_temporal_split(snapshots)
    snapshots = snapshots[snapshots["resolve_time"] >= valid_start].copy()
    if snapshots.empty:
        print("[ERROR] No validation-period snapshots available.")
        return

    top_rules = select_top_rules(rules, cfg)
    equity_df, trades_df = run_backtest(snapshots, top_rules, market_feature_cache, payload, cfg)

    config.BACKTEST_DIR.mkdir(parents=True, exist_ok=True)
    equity_df.to_csv(config.BACKTEST_DIR / "backtest_equity_qmodel.csv", index=False)
    trades_df.to_csv(config.BACKTEST_DIR / "backtest_trades_qmodel.csv", index=False)

    compute_summary(equity_df, trades_df, cfg)
    summarize_rules(trades_df)


if __name__ == "__main__":
    main()
