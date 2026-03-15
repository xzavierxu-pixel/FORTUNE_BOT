"""Selection and allocation helpers for online hourly scoring."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from execution_engine.integrations.providers.balance_provider import FileBalanceProvider
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


def select_target_side(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    out = frame.copy()
    direction = pd.to_numeric(out.get("direction_model"), errors="coerce").fillna(0).astype(int)
    select_outcome_0 = direction > 0
    out["selected_token_id"] = out["token_0_id"].where(select_outcome_0, out["token_1_id"])
    out["selected_outcome_label"] = out["outcome_0_label"].where(select_outcome_0, out["outcome_1_label"])
    out["position_side"] = select_outcome_0.map({True: "OUTCOME_0", False: "OUTCOME_1"})
    return out


def allocate_candidates(
    candidates: pd.DataFrame,
    cfg: PegConfig,
    state: StateStore,
    bt_cfg: Any,
) -> pd.DataFrame:
    if candidates.empty:
        return candidates.copy()

    balance_provider = FileBalanceProvider(cfg.balances_path)
    available_cash = balance_provider.get_available_usdc()
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
    ranked = candidates.copy()
    if "snapshot_time" not in ranked.columns:
        ranked["snapshot_time"] = pd.NaT
    ranked = ranked.sort_values(
        by=["snapshot_time", "market_id"],
        ascending=[True, True],
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
) -> pd.DataFrame:
    if model_outputs.empty:
        return pd.DataFrame()

    selected_outputs = select_target_side(model_outputs)
    selected_lookup = {
        (str(row.get("market_id") or ""), str(row.get("snapshot_time") or "")): row
        for row in selected.to_dict(orient="records")
    }

    records: List[Dict[str, Any]] = []
    for row in selected_outputs.to_dict(orient="records"):
        key = (str(row.get("market_id") or ""), str(row.get("snapshot_time") or ""))
        picked = selected_lookup.get(key)
        growth_score = _to_float(row.get("growth_score"), default=0.0)
        selected_for_submission = picked is not None
        if selected_for_submission:
            selection_reason = "allocated"
        elif growth_score <= 0:
            selection_reason = "no_positive_growth"
        else:
            selection_reason = "not_allocated"

        records.append(
            {
                "run_id": cfg.run_id,
                "batch_id": str(row.get("batch_id") or ""),
                "market_id": str(row.get("market_id") or ""),
                "selected_token_id": str(row.get("selected_token_id") or ""),
                "selected_outcome_label": str(row.get("selected_outcome_label") or ""),
                "selected_for_submission": selected_for_submission,
                "selection_reason": selection_reason,
                "stake_usdc": _to_float((picked or {}).get("stake_usdc")),
                "growth_score": growth_score,
                "f_exec": _to_float(row.get("f_exec")),
                "q_pred": _to_float(row.get("q_pred"), default=0.5),
                "trade_value_pred": _to_float(row.get("trade_value_pred")),
                "price": _to_float(row.get("price")),
                "horizon_hours": _to_float(row.get("horizon_hours")),
                "direction_model": _to_int(row.get("direction_model"), default=0),
                "position_side": str(row.get("position_side") or ""),
                "category": str(row.get("category") or ""),
                "domain": str(row.get("domain") or ""),
                "market_type": str(row.get("market_type") or ""),
                "rule_group_key": str(row.get("rule_group_key") or ""),
                "rule_leaf_id": _to_int(row.get("rule_leaf_id"), default=0),
                "market_close_time_utc": str(row.get("closedTime") or ""),
                "settlement_key": str((picked or {}).get("settlement_key") or ""),
                "cluster_key": str((picked or {}).get("cluster_key") or ""),
            }
        )
    return pd.DataFrame(records)

