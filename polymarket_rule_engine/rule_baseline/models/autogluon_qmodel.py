from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from rule_baseline.models.runtime_bundle import (
    FeatureContract,
    build_runtime_bundle_paths,
    save_calibrator,
    save_feature_contract,
    write_bundle_json,
)
from rule_baseline.models.tree_ensembles import fit_grouped_calibrators, fit_probability_calibrator, infer_feature_types


DEFAULT_AUTOGUON_PRESETS = "medium_quality"
DEFAULT_CALIBRATION_MODE = "grouped_isotonic"
DEFAULT_GROUP_COLUMN = "horizon_hours"
DEFAULT_GROUP_MIN_ROWS = 20


def _load_tabular_predictor_class():
    try:
        from autogluon.tabular import TabularPredictor
    except ImportError as exc:
        raise ImportError(
            "AutoGluon training requires autogluon.tabular. Install it before training the q bundle."
        ) from exc
    return TabularPredictor


def _coerce_feature_frame(
    df: pd.DataFrame,
    feature_columns: list[str],
    numeric_columns: list[str],
    categorical_columns: list[str],
) -> pd.DataFrame:
    out = df.loc[:, feature_columns].copy()
    for column in numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce").fillna(0.0)
    for column in categorical_columns:
        out[column] = out[column].astype("string").fillna("UNKNOWN").astype(object)
    return out


def _extract_group_values(df: pd.DataFrame, group_column: str) -> pd.Series:
    if group_column not in df.columns:
        return pd.Series(["__missing__"] * len(df), index=df.index, dtype="object")
    if group_column == "horizon_hours":
        return pd.to_numeric(df[group_column], errors="coerce").round().astype("Int64").astype(str)
    return df[group_column].astype("string").fillna("__missing__").astype(str)


@dataclass
class AutoGluonQTrainingResult:
    bundle_dir: Path
    predictor: Any
    feature_contract: FeatureContract
    calibration_mode: str
    calibrator: object | None
    calibrator_meta: dict[str, Any]
    runtime_manifest: dict[str, Any]
    predictor_name: str | None
    fit_rows: int
    calibration_rows: int

    def predict(self, df_feat: pd.DataFrame) -> np.ndarray:
        aligned = df_feat.copy()
        categorical = set(self.feature_contract.categorical_columns)
        for column in self.feature_contract.feature_columns:
            if column not in aligned.columns:
                aligned[column] = "UNKNOWN" if column in categorical else 0.0
        aligned = aligned.loc[:, list(self.feature_contract.feature_columns)].copy()
        for column in self.feature_contract.numeric_columns:
            aligned[column] = pd.to_numeric(aligned[column], errors="coerce").fillna(0.0)
        for column in self.feature_contract.categorical_columns:
            aligned[column] = aligned[column].astype("string").fillna("UNKNOWN").astype(object)

        raw_prob = np.asarray(
            self.predictor.predict_proba(aligned, as_pandas=False, as_multiclass=False),
            dtype=float,
        )
        if self.calibrator is None:
            return np.clip(raw_prob, 0.0, 1.0)

        if (self.calibrator_meta or {}).get("type") == "grouped":
            from rule_baseline.models.tree_ensembles import apply_probability_calibrator

            group_column = str((self.calibrator_meta or {}).get("group_column") or DEFAULT_GROUP_COLUMN)
            groups = _extract_group_values(df_feat, group_column)
            prob = raw_prob.copy()
            for group_value, indices in groups.groupby(groups, sort=False).indices.items():
                index_array = np.asarray(indices, dtype=int)
                selected = None
                if isinstance(self.calibrator, dict):
                    selected = self.calibrator.get(str(group_value)) or self.calibrator.get("__global__")
                if selected is None:
                    continue
                prob[index_array] = apply_probability_calibrator(selected, raw_prob[index_array])
            return np.clip(prob, 0.0, 1.0)

        from rule_baseline.models.tree_ensembles import apply_probability_calibrator

        return np.clip(apply_probability_calibrator(self.calibrator, raw_prob), 0.0, 1.0)


def fit_autogluon_q_model(
    *,
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    feature_columns: list[str],
    bundle_dir: Path,
    artifact_mode: str,
    split_boundaries: dict[str, Any],
    calibration_mode: str = DEFAULT_CALIBRATION_MODE,
    predictor_presets: str = DEFAULT_AUTOGUON_PRESETS,
    time_limit: int | None = None,
    refit_full: bool = False,
    deploy_optimized: bool = False,
    predictor_hyperparameters: dict[str, Any] | None = None,
    label_column: str = "y",
    grouped_calibration_min_rows: int = DEFAULT_GROUP_MIN_ROWS,
    grouped_calibration_column: str = DEFAULT_GROUP_COLUMN,
) -> AutoGluonQTrainingResult:
    if df_train.empty:
        raise RuntimeError("AutoGluon q-model training requires non-empty training data.")

    numeric_columns, categorical_columns = infer_feature_types(df_train, feature_columns)
    feature_contract = FeatureContract(
        feature_columns=tuple(feature_columns),
        numeric_columns=tuple(numeric_columns),
        categorical_columns=tuple(categorical_columns),
    )

    train_features = _coerce_feature_frame(df_train, feature_columns, numeric_columns, categorical_columns)
    train_data = train_features.copy()
    train_data[label_column] = df_train[label_column].astype(int).values

    valid_data = None
    valid_features = pd.DataFrame()
    if not df_valid.empty:
        valid_features = _coerce_feature_frame(df_valid, feature_columns, numeric_columns, categorical_columns)
        valid_data = valid_features.copy()
        valid_data[label_column] = df_valid[label_column].astype(int).values

    bundle_paths = build_runtime_bundle_paths(bundle_dir)
    bundle_paths.ensure_dirs()

    TabularPredictor = _load_tabular_predictor_class()
    predictor = TabularPredictor(
        label=label_column,
        problem_type="binary",
        eval_metric="log_loss",
        path=str(bundle_paths.predictor_dir),
    )
    fit_kwargs: dict[str, Any] = {
        "train_data": train_data,
        "presets": predictor_presets,
        "refit_full": False,
        "set_best_to_refit_full": False,
    }
    if valid_data is not None and not valid_data.empty:
        fit_kwargs["tuning_data"] = valid_data
        # Bagging presets require explicit holdout usage when external tuning data is provided.
        fit_kwargs["use_bag_holdout"] = True
        # Disable DyStack here to keep fit behavior stable under explicit holdout tuning.
        fit_kwargs["dynamic_stacking"] = False
    if time_limit is not None:
        fit_kwargs["time_limit"] = time_limit
    if predictor_hyperparameters:
        fit_kwargs["hyperparameters"] = predictor_hyperparameters
    predictor.fit(**fit_kwargs)

    predictor_name = getattr(predictor, "model_best", None)
    if refit_full:
        predictor.refit_full()
        predictor_name = getattr(predictor, "model_best", predictor_name)

    calibrator = None
    calibrator_meta: dict[str, Any] = {
        "type": "none",
        "mode": calibration_mode,
        "group_column": grouped_calibration_column,
        "min_rows": int(grouped_calibration_min_rows),
        "fit_rows": int(len(df_train)),
        "calibration_rows": int(len(df_valid)),
        "grouped_fallback_to_global": False,
        "group_calibrator_count": 0,
    }
    if calibration_mode != "none" and valid_data is not None and not valid_data.empty and df_valid[label_column].nunique() > 1:
        raw_valid = np.asarray(
            predictor.predict_proba(valid_features, as_pandas=False, as_multiclass=False),
            dtype=float,
        )
        y_valid = df_valid[label_column].astype(int).values
        if calibration_mode == "global_isotonic":
            calibrator = fit_probability_calibrator(raw_valid, y_valid, "isotonic")
            calibrator_meta.update({"type": "global", "method": "isotonic"})
        elif calibration_mode == "grouped_isotonic":
            group_values = _extract_group_values(df_valid, grouped_calibration_column)
            grouped = fit_grouped_calibrators(
                raw_valid,
                y_valid,
                group_values,
                "isotonic",
                min_rows=grouped_calibration_min_rows,
            )
            global_calibrator = fit_probability_calibrator(raw_valid, y_valid, "isotonic")
            grouped["__global__"] = global_calibrator
            calibrator = grouped
            calibrator_meta.update(
                {
                    "type": "grouped",
                    "method": "isotonic",
                    "group_column": grouped_calibration_column,
                    "group_calibrator_count": int(max(len(grouped) - 1, 0)),
                    "grouped_fallback_to_global": True,
                }
            )
        else:
            raise ValueError(f"Unsupported AutoGluon calibration_mode: {calibration_mode}")

    runtime_manifest = {
        "artifact_version": 1,
        "model_family": "autogluon_tabular_q",
        "target_mode": "q",
        "label_column": label_column,
        "artifact_mode": artifact_mode,
        "predictor_path": str(bundle_paths.predictor_dir.relative_to(bundle_paths.root_dir)).replace("\\", "/"),
        "predictor_name": predictor_name,
        "refit_full_used": bool(refit_full),
        "deployment_optimized": bool(deploy_optimized),
        "split_boundaries": split_boundaries,
        "calibration_mode": calibration_mode,
        "grouped_calibration_column": grouped_calibration_column,
        "predictor_presets": predictor_presets,
        "fit_rows": int(len(df_train)),
        "calibration_rows": int(len(df_valid)),
    }

    save_feature_contract(bundle_paths.feature_contract_path, feature_contract)
    write_bundle_json(bundle_paths.runtime_manifest_path, runtime_manifest)
    write_bundle_json(bundle_paths.calibrator_meta_path, calibrator_meta)
    if calibrator is not None:
        save_calibrator(bundle_paths.calibrator_path, calibrator)

    return AutoGluonQTrainingResult(
        bundle_dir=bundle_dir,
        predictor=predictor,
        feature_contract=feature_contract,
        calibration_mode=calibration_mode,
        calibrator=calibrator,
        calibrator_meta=calibrator_meta,
        runtime_manifest=runtime_manifest,
        predictor_name=predictor_name,
        fit_rows=int(len(df_train)),
        calibration_rows=int(len(df_valid)),
    )
