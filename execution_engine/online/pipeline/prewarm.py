"""Preload shared runtime state for the low-latency online submit path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from execution_engine.online.execution.submission_support import load_fee_rate
from execution_engine.online.scoring.rule_runtime import (
    FeatureContract,
    RuleRuntime,
    get_feature_contract,
    load_model_payload,
    load_rule_runtime,
)
from execution_engine.online.scoring.rules import RuleHorizonProfile, load_rule_horizon_profile, load_rules_frame
from execution_engine.runtime.config import PegConfig


@dataclass(frozen=True)
class OnlineRuntimeContainer:
    cfg: PegConfig
    rule_runtime: RuleRuntime
    rules_frame: pd.DataFrame
    horizon_profile: RuleHorizonProfile
    model_payload: dict[str, Any]
    feature_contract: FeatureContract
    fee_rate: float


def build_runtime_container(cfg: PegConfig) -> OnlineRuntimeContainer:
    rules_frame = load_rules_frame(cfg)
    model_payload = load_model_payload(cfg)
    return OnlineRuntimeContainer(
        cfg=cfg,
        rule_runtime=load_rule_runtime(cfg),
        rules_frame=rules_frame,
        horizon_profile=load_rule_horizon_profile(cfg),
        model_payload=model_payload,
        feature_contract=get_feature_contract(model_payload),
        fee_rate=load_fee_rate(cfg),
    )
