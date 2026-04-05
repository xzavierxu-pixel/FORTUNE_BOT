"""Selection and allocation helpers for online hourly scoring."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from execution_engine.integrations.providers.balance_provider import build_balance_provider
from execution_engine.runtime.config import PegConfig
from execution_engine.runtime.state import StateStore


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _to_int(value: Any, default: int | None = None) -> int | None:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def filter_candidates_by_growth_score(
    candidates: pd.DataFrame,
    *,
    min_growth_score: float = 0.2,
) -> pd.DataFrame:
    if candidates.empty or "growth_score" not in candidates.columns:
        return candidates.copy()
    scores = pd.to_numeric(candidates["growth_score"], errors="coerce")
    return candidates.loc[scores > float(min_growth_score)].copy()


def select_target_side(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.copy()
    if "direction_model" in out.columns:
        direction_raw = out["direction_model"]
    else:
        direction_raw = pd.Series(0, index=out.index, dtype="int64")
    direction = pd.to_numeric(direction_raw, errors="coerce").fillna(0).astype(int)
    select_outcome_0 = direction > 0
    raw_price = pd.to_numeric(out.get("price"), errors="coerce").fillna(0.0) if "price" in out.columns else pd.Series(0.0, index=out.index)
    raw_q_pred = pd.to_numeric(out.get("q_pred"), errors="coerce").fillna(0.0) if "q_pred" in out.columns else pd.Series(0.0, index=out.index)
    out["selected_token_id"] = out["token_0_id"].where(select_outcome_0, out["token_1_id"])
    out["selected_outcome_label"] = out["outcome_0_label"].where(select_outcome_0, out["outcome_1_label"])
    out["position_side"] = select_outcome_0.map({True: "OUTCOME_0", False: "OUTCOME_1"})
    out["price"] = raw_price.where(select_outcome_0, 1.0 - raw_price).clip(lower=0.0, upper=1.0)
    out["q_pred"] = raw_q_pred.where(select_outcome_0, 1.0 - raw_q_pred).clip(lower=0.0, upper=1.0)
    out["edge_prob"] = out["q_pred"] - out["price"]
    return out


def allocate_candidates(
    candidates: pd.DataFrame,
    cfg: PegConfig,
    state: StateStore,
    bt_cfg: Any,
) -> pd.DataFrame:
    filtered_candidates = filter_candidates_by_growth_score(
        candidates,
        min_growth_score=float(getattr(cfg, "online_min_growth_score", 0.2)),
    )
    if filtered_candidates.empty:
        return filtered_candidates

    balance_provider = build_balance_provider(cfg)
    available_cash = balance_provider.get_available_usdc()
    if not cfg.dry_run and cfg.clob_enabled:
        bankroll = max(_to_float(available_cash), 0.0)
        remaining_cash = bankroll
    else:
        bankroll = float(cfg.initial_bankroll_usdc)
        if bankroll <= 0:
            return pd.DataFrame()
        available_capacity = max(0.0, bankroll - float(state.net_exposure_usdc))
        remaining_cash = (
            min(float(available_cash), available_capacity)
            if available_cash is not None
            else available_capacity
        )
    selected_rows: List[Dict[str, Any]] = []
    ranked = filtered_candidates.copy()
    if "snapshot_time" not in ranked.columns:
        ranked["snapshot_time"] = pd.NaT
    if "edge_final" not in ranked.columns:
        ranked["edge_final"] = 0.0
    ranked = ranked.sort_values(
        by=["snapshot_time", "edge_final", "market_id"],
        ascending=[True, False, True],
    )
    for _, row in ranked.iterrows():
        if remaining_cash <= 0:
            break

        settlement_ts = pd.to_datetime(row.get("closedTime"), utc=True, errors="coerce")
        settlement_key = settlement_ts.date().isoformat() if pd.notna(settlement_ts) else "UNKNOWN"
        cluster_key = f"{row.get('source_host', 'UNKNOWN')}|{row.get('category', 'UNKNOWN')}|{settlement_key}"
        desired_stake = float(row.get("f_exec", 0.0)) * bankroll
        stake = min(
            desired_stake,
            bt_cfg.max_position_f * bankroll,
            cfg.max_trade_amount_usdc,
            remaining_cash,
        )
        if stake <= 0:
            continue

        selected = row.to_dict()
        selected["stake_usdc"] = float(stake)
        selected["settlement_key"] = settlement_key
        selected["cluster_key"] = cluster_key
        selected_rows.append(selected)
        remaining_cash -= stake

    return pd.DataFrame(selected_rows)


def build_selection_decisions(
    model_outputs: pd.DataFrame,
    selected: pd.DataFrame,
    cfg: PegConfig,
    *,
    min_growth_score: float = 0.2,
) -> pd.DataFrame:
    if model_outputs.empty:
        return pd.DataFrame()

    selected_outputs = model_outputs.copy()
    selected_execution = selected.copy()
    selected_lookup = {
        (
            str(row.get("market_id") or ""),
            str(row.get("snapshot_time") or ""),
            str(row.get("rule_group_key") or ""),
            _to_int(row.get("rule_leaf_id"), default=0),
        ): row
        for row in selected_execution.to_dict(orient="records")
    }

    records: List[Dict[str, Any]] = []
    for row in selected_outputs.to_dict(orient="records"):
        key = (
            str(row.get("market_id") or ""),
            str(row.get("snapshot_time") or ""),
            str(row.get("rule_group_key") or ""),
            _to_int(row.get("rule_leaf_id"), default=0),
        )
        picked = selected_lookup.get(key)
        execution_row = picked or row
        growth_score = _to_float(execution_row.get("growth_score"), default=0.0)
        selected_for_submission = picked is not None
        if selected_for_submission:
            selection_reason = "allocated"
        elif growth_score <= min_growth_score:
            selection_reason = "growth_below_threshold"
        elif growth_score <= 0:
            selection_reason = "no_positive_growth"
        else:
            selection_reason = "not_allocated"

        records.append(
            {
                "run_id": cfg.run_id,
                "batch_id": str(row.get("batch_id") or ""),
                "market_id": str(row.get("market_id") or ""),
                "selected_token_id": str(execution_row.get("selected_token_id") or ""),
                "selected_outcome_label": str(execution_row.get("selected_outcome_label") or ""),
                "selected_for_submission": selected_for_submission,
                "selection_reason": selection_reason,
                "stake_usdc": _to_float((picked or {}).get("stake_usdc")),
                "growth_score": growth_score,
                "edge_final": _to_float(execution_row.get("edge_final")),
                "f_exec": _to_float(execution_row.get("f_exec")),
                "q_pred": _to_float(execution_row.get("q_pred"), default=0.5),
                "trade_value_pred": _to_float(execution_row.get("trade_value_pred")),
                "price": _to_float(execution_row.get("price")),
                "horizon_hours": _to_float(row.get("horizon_hours")),
                "direction_model": _to_int(execution_row.get("direction_model"), default=0),
                "position_side": str(execution_row.get("position_side") or ""),
                "category": str(row.get("category") or ""),
                "domain": str(row.get("domain") or ""),
                "market_type": str(row.get("market_type") or ""),
                "rule_group_key": str(row.get("rule_group_key") or ""),
                "rule_leaf_id": _to_int(row.get("rule_leaf_id"), default=0),
                "first_seen_at_utc": str(row.get("first_seen_at_utc") or ""),
                "snapshot_time_utc": str(row.get("snapshot_time") or ""),
                "market_close_time_utc": str(row.get("closedTime") or ""),
                "settlement_key": str((picked or {}).get("settlement_key") or ""),
                "cluster_key": str((picked or {}).get("cluster_key") or ""),
            }
        )
    return pd.DataFrame(records)

