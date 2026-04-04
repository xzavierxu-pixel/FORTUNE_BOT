from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

from rule_baseline.models.runtime_bundle import (
    FeatureContract,
    build_runtime_bundle_paths,
    is_runtime_bundle,
    load_calibrator,
    load_feature_contract,
    read_bundle_json,
    resolve_runtime_bundle_dir,
)
from rule_baseline.models.scoring import compute_trade_value_from_q, infer_q_from_trade_value
from rule_baseline.models.tree_ensembles import apply_probability_calibrator, predict_probabilities, predict_regression


def _load_tabular_predictor(path: Path):
    try:
        from autogluon.tabular import TabularPredictor
    except ImportError as exc:
        raise ImportError(
            "AutoGluon runtime is required to load q_model_bundle artifacts. "
            "Install autogluon.tabular in the runtime environment."
        ) from exc
    return TabularPredictor.load(str(path))


@dataclass
class ModelArtifactAdapter:
    backend: str
    target_mode: str
    feature_contract: FeatureContract
    runtime_manifest: dict[str, Any]
    artifact_path: Path
    legacy_payload: dict[str, Any] | None = None
    predictor: Any | None = None
    calibrator: Any | None = None
    calibrator_meta: dict[str, Any] | None = None

    def persist(self) -> None:
        if self.predictor is None:
            return
        persist = getattr(self.predictor, "persist", None)
        if callable(persist):
            persist()

    def align_features(self, df_feat: pd.DataFrame) -> pd.DataFrame:
        out = df_feat.copy()
        categorical = set(self.feature_contract.categorical_columns)
        numeric = set(self.feature_contract.numeric_columns)
        for column in self.feature_contract.feature_columns:
            if column not in out.columns:
                out[column] = "UNKNOWN" if column in categorical else 0.0
        for column in self.feature_contract.numeric_columns:
            out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0)
        for column in self.feature_contract.categorical_columns:
            out[column] = out[column].astype("string").fillna("UNKNOWN").astype(object)
        return out.loc[:, list(self.feature_contract.feature_columns)]

    def _predict_legacy_q(self, aligned: pd.DataFrame) -> np.ndarray:
        assert self.legacy_payload is not None
        if self.target_mode == "q":
            return predict_probabilities(self.legacy_payload, aligned)
        if self.target_mode == "residual_q":
            residual_pred = predict_regression(self.legacy_payload, aligned)
            price = aligned["price"].astype(float).values if "price" in aligned.columns else np.zeros(len(aligned))
            return np.clip(price + residual_pred, 0.0, 1.0)
        raise ValueError(f"Legacy payload target_mode '{self.target_mode}' does not produce q directly.")

    def _predict_bundle_q(self, aligned: pd.DataFrame) -> np.ndarray:
        if self.predictor is None:
            raise RuntimeError("AutoGluon predictor has not been loaded.")
        raw_prob = self.predictor.predict_proba(aligned, as_pandas=False, as_multiclass=False)
        raw_prob = np.asarray(raw_prob, dtype=float)
        calibrator = self.calibrator
        calibrator_meta = self.calibrator_meta or {}
        if calibrator is not None and calibrator_meta.get("type") == "grouped":
            prob = raw_prob.copy()
            group_column = str(calibrator_meta.get("group_column") or "")
            if group_column and group_column in aligned.columns:
                groups = pd.Series(aligned[group_column]).reset_index(drop=True).astype(str)
                for group_value, indices in groups.groupby(groups, sort=False).indices.items():
                    index_array = np.asarray(indices, dtype=int)
                    group_calibrator = calibrator.get(str(group_value)) if isinstance(calibrator, dict) else None
                    fallback = calibrator.get("__global__") if isinstance(calibrator, dict) else None
                    selected = group_calibrator or fallback
                    if selected is None:
                        continue
                    prob[index_array] = apply_probability_calibrator(selected, raw_prob[index_array])
                return np.clip(prob, 0.0, 1.0)
        if calibrator is not None:
            return np.clip(apply_probability_calibrator(calibrator, raw_prob), 0.0, 1.0)
        return np.clip(raw_prob, 0.0, 1.0)

    def predict_q(self, df_feat: pd.DataFrame) -> np.ndarray:
        aligned = self.align_features(df_feat)
        if self.backend == "legacy_payload":
            return self._predict_legacy_q(aligned)
        if self.backend == "autogluon_q_bundle":
            return self._predict_bundle_q(aligned)
        raise ValueError(f"Unsupported backend: {self.backend}")

    def predict_trade_value(self, candidates: pd.DataFrame, df_feat: pd.DataFrame) -> np.ndarray:
        aligned = self.align_features(df_feat)
        if self.backend == "legacy_payload" and self.target_mode in {"expected_pnl", "expected_roi"}:
            assert self.legacy_payload is not None
            return predict_regression(self.legacy_payload, aligned)
        q_pred = self.predict_q(df_feat)
        return compute_trade_value_from_q(candidates, q_pred)

    def predict_outputs(self, candidates: pd.DataFrame, df_feat: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        aligned = self.align_features(df_feat)
        if self.backend == "legacy_payload" and self.target_mode in {"expected_pnl", "expected_roi"}:
            assert self.legacy_payload is not None
            trade_value_pred = predict_regression(self.legacy_payload, aligned)
            q_pred = infer_q_from_trade_value(candidates, trade_value_pred)
            return q_pred, trade_value_pred
        q_pred = self.predict_q(df_feat)
        trade_value_pred = compute_trade_value_from_q(candidates, q_pred)
        return q_pred, trade_value_pred


def build_legacy_adapter(payload: dict[str, Any], artifact_path: Path) -> ModelArtifactAdapter:
    feature_contract = FeatureContract(
        feature_columns=tuple(str(value) for value in payload.get("feature_columns", ())),
        numeric_columns=tuple(str(value) for value in payload.get("numeric_columns", ())),
        categorical_columns=tuple(str(value) for value in payload.get("categorical_columns", ())),
    )
    return ModelArtifactAdapter(
        backend="legacy_payload",
        target_mode=str(payload.get("target_mode", "q")),
        feature_contract=feature_contract,
        runtime_manifest={"artifact_version": 0, "model_family": "legacy_payload", "target_mode": payload.get("target_mode", "q")},
        artifact_path=artifact_path,
        legacy_payload=payload,
        calibrator=payload.get("calibrator"),
        calibrator_meta=payload.get("calibrator_meta") or {},
    )


def load_model_artifact(path: Path) -> ModelArtifactAdapter:
    resolved_path = Path(path)
    if is_runtime_bundle(resolved_path):
        bundle_dir = resolve_runtime_bundle_dir(resolved_path)
        bundle_paths = build_runtime_bundle_paths(bundle_dir)
        runtime_manifest = read_bundle_json(bundle_paths.runtime_manifest_path)
        feature_contract = load_feature_contract(bundle_paths.feature_contract_path)
        calibrator_meta = (
            read_bundle_json(bundle_paths.calibrator_meta_path)
            if bundle_paths.calibrator_meta_path.exists()
            else {}
        )
        calibrator = load_calibrator(bundle_paths.calibrator_path) if bundle_paths.calibrator_path.exists() else None
        predictor = _load_tabular_predictor(bundle_paths.predictor_dir)
        return ModelArtifactAdapter(
            backend="autogluon_q_bundle",
            target_mode=str(runtime_manifest.get("target_mode", "q")),
            feature_contract=feature_contract,
            runtime_manifest=runtime_manifest,
            artifact_path=bundle_dir,
            predictor=predictor,
            calibrator=calibrator,
            calibrator_meta=calibrator_meta,
        )

    payload = joblib.load(resolved_path)
    if not isinstance(payload, dict):
        raise TypeError(f"Expected dict model payload or runtime bundle at {resolved_path}, got {type(payload)!r}.")
    return build_legacy_adapter(payload, resolved_path)
