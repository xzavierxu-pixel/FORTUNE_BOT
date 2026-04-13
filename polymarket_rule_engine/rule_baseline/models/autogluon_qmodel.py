from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from copy import deepcopy
import shutil
import time
import random
from typing import Any

import numpy as np
import pandas as pd

from rule_baseline.models.runtime_bundle import (
    FeatureContract,
    FULL_TRAINING_BUNDLE_DIRNAME,
    build_runtime_bundle_paths,
    save_calibrator,
    save_feature_contract,
    save_normalization_manifest,
    write_bundle_json,
)
from rule_baseline.models.tree_ensembles import (
    fit_grouped_calibrators,
    fit_probability_blend_calibrator,
    fit_probability_calibrator,
    infer_feature_types,
)


DEFAULT_AUTOGUON_PRESETS = "medium_quality"
DEFAULT_CALIBRATION_MODE = "global_isotonic"
DEFAULT_GROUP_COLUMN = "horizon_hours"
DEFAULT_GROUP_MIN_ROWS = 20
DEFAULT_RANDOM_SEED = 21
DEFAULT_TIME_LIMIT = 300
DEFAULT_PREDICTOR_HYPERPARAMETERS = {
    "GBM": {},
    "CAT": {},
    "XGB": {},
}
SUPPORTED_CALIBRATION_MODES = {
    "none",
    "global_isotonic",
    "grouped_isotonic",
    "global_sigmoid",
    "grouped_sigmoid",
    "beta_calibration",
    "blend_raw_global_isotonic_15",
    "blend_raw_global_isotonic_25",
    "blend_raw_global_isotonic_35",
    "blend_raw_beta_15",
    "blend_raw_beta_25",
    "blend_raw_beta_35",
}


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
        out[column] = (
            pd.to_numeric(out[column], errors="coerce")
            .replace([np.inf, -np.inf], np.nan)
            .fillna(0.0)
            .astype(np.float32)
        )
    for column in categorical_columns:
        out[column] = out[column].astype("string").fillna("UNKNOWN").astype(object)
    # Rebuild the frame once after column-wise coercion to avoid highly fragmented
    # pandas internals causing large temporary allocations inside AutoGluon.
    return pd.DataFrame(out, copy=True)


def _extract_group_values(df: pd.DataFrame, group_column: str) -> pd.Series:
    if group_column not in df.columns:
        return pd.Series(["__missing__"] * len(df), index=df.index, dtype="object")
    if group_column == "horizon_hours":
        return pd.to_numeric(df[group_column], errors="coerce").round().astype("Int64").astype(str)
    return df[group_column].astype("string").fillna("__missing__").astype(str)


@dataclass
class AutoGluonQTrainingResult:
    bundle_dir: Path
    full_bundle_dir: Path
    deploy_bundle_dir: Path
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
        aligned = _coerce_feature_frame(
            aligned,
            list(self.feature_contract.feature_columns),
            list(self.feature_contract.numeric_columns),
            list(self.feature_contract.categorical_columns),
        )

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


def _reset_bundle_dir(path: Path) -> None:
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


def _copy_runtime_bundle_metadata(
    *,
    source_dir: Path,
    target_dir: Path,
    feature_contract: FeatureContract,
    calibrator: object | None,
    calibrator_meta: dict[str, Any],
    runtime_manifest: dict[str, Any],
) -> None:
    bundle_paths = build_runtime_bundle_paths(target_dir)
    bundle_paths.ensure_dirs()
    save_feature_contract(bundle_paths.feature_contract_path, feature_contract)
    write_bundle_json(bundle_paths.runtime_manifest_path, runtime_manifest)
    save_normalization_manifest(
        bundle_paths.normalization_manifest_path,
        runtime_manifest.get("normalization_manifest", {}) or {},
    )
    write_bundle_json(bundle_paths.calibrator_meta_path, calibrator_meta)
    write_bundle_json(
        bundle_paths.deployment_summary_path,
        {
            **runtime_manifest.get("deployment_validation", {}),
            "source_bundle_dir": str(source_dir),
        },
    )
    if calibrator is not None:
        save_calibrator(bundle_paths.calibrator_path, calibrator)


def fit_autogluon_q_model(
    *,
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    feature_columns: list[str],
    required_critical_columns: list[str] | None = None,
    required_noncritical_columns: list[str] | None = None,
    feature_semantics_version: str | None = None,
    normalization_manifest: dict[str, Any] | None = None,
    bundle_dir: Path,
    full_bundle_dir: Path | None = None,
    artifact_mode: str,
    split_boundaries: dict[str, Any],
    calibration_mode: str = DEFAULT_CALIBRATION_MODE,
    predictor_presets: str = DEFAULT_AUTOGUON_PRESETS,
    time_limit: int | None = None,
    random_seed: int = DEFAULT_RANDOM_SEED,
    refit_full: bool = False,
    deploy_optimized: bool = False,
    predictor_hyperparameters: dict[str, Any] | None = None,
    label_column: str = "y",
    grouped_calibration_min_rows: int = DEFAULT_GROUP_MIN_ROWS,
    grouped_calibration_column: str = DEFAULT_GROUP_COLUMN,
    num_bag_folds: int | None = None,
    num_bag_sets: int | None = None,
    num_stack_levels: int | None = None,
    auto_stack: bool | None = None,
    calibration_holdout_policy: str = "explicit_valid_split",
) -> AutoGluonQTrainingResult:
    if df_train.empty:
        raise RuntimeError("AutoGluon q-model training requires non-empty training data.")

    numeric_columns, categorical_columns = infer_feature_types(df_train, feature_columns)
    feature_contract = FeatureContract(
        feature_columns=tuple(feature_columns),
        numeric_columns=tuple(numeric_columns),
        categorical_columns=tuple(categorical_columns),
        required_critical_columns=tuple(required_critical_columns or ()),
        required_noncritical_columns=tuple(required_noncritical_columns or feature_columns),
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

    deploy_bundle_dir = bundle_dir
    training_bundle_dir = full_bundle_dir or bundle_dir.parent / FULL_TRAINING_BUNDLE_DIRNAME
    deploy_bundle_paths = build_runtime_bundle_paths(deploy_bundle_dir)
    full_bundle_paths = build_runtime_bundle_paths(training_bundle_dir)
    _reset_bundle_dir(deploy_bundle_dir)
    if training_bundle_dir != deploy_bundle_dir:
        _reset_bundle_dir(training_bundle_dir)
    deploy_bundle_paths.ensure_dirs()
    full_bundle_paths.ensure_dirs()

    TabularPredictor = _load_tabular_predictor_class()
    predictor = TabularPredictor(
        label=label_column,
        problem_type="binary",
        eval_metric="log_loss",
        path=str(full_bundle_paths.predictor_dir),
    )
    fit_kwargs: dict[str, Any] = {
        "train_data": train_data,
        "presets": predictor_presets,
        "refit_full": False,
        "set_best_to_refit_full": False,
    }
    if num_bag_folds is not None:
        fit_kwargs["num_bag_folds"] = int(num_bag_folds)
    if num_bag_sets is not None:
        fit_kwargs["num_bag_sets"] = int(num_bag_sets)
    if num_stack_levels is not None:
        fit_kwargs["num_stack_levels"] = int(num_stack_levels)
    if auto_stack is not None:
        fit_kwargs["auto_stack"] = bool(auto_stack)
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
    else:
        fit_kwargs["hyperparameters"] = deepcopy(DEFAULT_PREDICTOR_HYPERPARAMETERS)
    random.seed(int(random_seed))
    np.random.seed(int(random_seed))
    fit_started = time.perf_counter()
    predictor.fit(**fit_kwargs)
    fit_duration_sec = time.perf_counter() - fit_started

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
        elif calibration_mode == "global_sigmoid":
            calibrator = fit_probability_calibrator(raw_valid, y_valid, "sigmoid")
            calibrator_meta.update({"type": "global", "method": "sigmoid"})
        elif calibration_mode == "beta_calibration":
            calibrator = fit_probability_calibrator(raw_valid, y_valid, "beta")
            calibrator_meta.update({"type": "global", "method": "beta"})
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
        elif calibration_mode == "grouped_sigmoid":
            group_values = _extract_group_values(df_valid, grouped_calibration_column)
            grouped = fit_grouped_calibrators(
                raw_valid,
                y_valid,
                group_values,
                "sigmoid",
                min_rows=grouped_calibration_min_rows,
            )
            global_calibrator = fit_probability_calibrator(raw_valid, y_valid, "sigmoid")
            grouped["__global__"] = global_calibrator
            calibrator = grouped
            calibrator_meta.update(
                {
                    "type": "grouped",
                    "method": "sigmoid",
                    "group_column": grouped_calibration_column,
                    "group_calibrator_count": int(max(len(grouped) - 1, 0)),
                    "grouped_fallback_to_global": True,
                }
            )
        elif calibration_mode.startswith("blend_raw_global_isotonic_"):
            alpha = float(calibration_mode.rsplit("_", 1)[-1]) / 100.0
            calibrator = fit_probability_blend_calibrator(
                raw_valid,
                y_valid,
                alpha=alpha,
                base_method="isotonic",
            )
            calibrator_meta.update({"type": "global", "method": "blend", "base_method": "isotonic", "blend_alpha": alpha})
        elif calibration_mode.startswith("blend_raw_beta_"):
            alpha = float(calibration_mode.rsplit("_", 1)[-1]) / 100.0
            calibrator = fit_probability_blend_calibrator(
                raw_valid,
                y_valid,
                alpha=alpha,
                base_method="beta",
            )
            calibrator_meta.update({"type": "global", "method": "blend", "base_method": "beta", "blend_alpha": alpha})
        else:
            raise ValueError(f"Unsupported AutoGluon calibration_mode: {calibration_mode}")

    clone_seconds = None
    clone_ok = False
    clone_error = ""
    if training_bundle_dir != deploy_bundle_dir:
        clone_started = time.perf_counter()
        try:
            predictor.clone_for_deployment(
                path=str(deploy_bundle_paths.predictor_dir),
                model="best",
                dirs_exist_ok=True,
            )
            clone_ok = True
        except Exception as exc:
            clone_error = str(exc)
            raise
        finally:
            clone_seconds = time.perf_counter() - clone_started
    else:
        clone_ok = True
        clone_seconds = 0.0

    deployment_predictor = predictor
    if training_bundle_dir != deploy_bundle_dir:
        deployment_predictor = TabularPredictor.load(str(deploy_bundle_paths.predictor_dir))

    persist_seconds = None
    persist_ok = False
    persist_error = ""
    if deploy_optimized:
        persist_started = time.perf_counter()
        try:
            deployment_predictor.persist()
            persist_ok = True
        except Exception as exc:
            persist_error = str(exc)
        persist_seconds = time.perf_counter() - persist_started

    runtime_manifest = {
        "artifact_version": 1,
        "model_family": "autogluon_tabular_q",
        "target_mode": "q",
        "label_column": label_column,
        "artifact_mode": artifact_mode,
        "predictor_path": str(deploy_bundle_paths.predictor_dir.relative_to(deploy_bundle_paths.root_dir)).replace("\\", "/"),
        "predictor_name": predictor_name,
        "refit_full_used": bool(refit_full),
        "deployment_optimized": bool(deploy_optimized),
        "bundle_role": "deployment",
        "full_bundle_dir": str(training_bundle_dir),
        "deploy_bundle_dir": str(deploy_bundle_dir),
        "split_boundaries": split_boundaries,
        "calibration_mode": calibration_mode,
        "grouped_calibration_column": grouped_calibration_column,
        "predictor_presets": predictor_presets,
        "random_seed": int(random_seed),
        "num_bag_folds": int(num_bag_folds) if num_bag_folds is not None else None,
        "num_bag_sets": int(num_bag_sets) if num_bag_sets is not None else None,
        "num_stack_levels": int(num_stack_levels) if num_stack_levels is not None else None,
        "auto_stack": bool(auto_stack) if auto_stack is not None else None,
        "use_bag_holdout": bool(fit_kwargs.get("use_bag_holdout", False)),
        "dynamic_stacking": fit_kwargs.get("dynamic_stacking"),
        "training_recipe": {
            "predictor_presets": predictor_presets,
            "time_limit": time_limit,
            "random_seed": int(random_seed),
            "label_column": label_column,
            "predictor_hyperparameters": fit_kwargs.get("hyperparameters"),
            "fit_rows": int(len(df_train)),
            "calibration_rows": int(len(df_valid)),
            "calibration_mode": calibration_mode,
            "calibration_holdout_policy": calibration_holdout_policy,
            "calibration_overlap_allowed": False,
        },
        "feature_semantics_version": feature_semantics_version,
        "normalization_manifest": normalization_manifest or {},
        "deployment_validation": {
            "fit_duration_sec": float(fit_duration_sec),
            "clone_for_deployment_ok": bool(clone_ok),
            "clone_for_deployment_duration_sec": float(clone_seconds) if clone_seconds is not None else None,
            "clone_for_deployment_error": clone_error,
            "persist_attempted": bool(deploy_optimized),
            "persist_ok": bool(persist_ok),
            "persist_duration_sec": float(persist_seconds) if persist_seconds is not None else None,
            "persist_error": persist_error,
        },
        "fit_rows": int(len(df_train)),
        "calibration_rows": int(len(df_valid)),
    }

    full_runtime_manifest = dict(runtime_manifest)
    full_runtime_manifest.update(
        {
            "bundle_role": "full_training",
            "predictor_path": str(full_bundle_paths.predictor_dir.relative_to(full_bundle_paths.root_dir)).replace("\\", "/"),
        }
    )

    _copy_runtime_bundle_metadata(
        source_dir=training_bundle_dir,
        target_dir=training_bundle_dir,
        feature_contract=feature_contract,
        calibrator=calibrator,
        calibrator_meta=calibrator_meta,
        runtime_manifest=full_runtime_manifest,
    )
    _copy_runtime_bundle_metadata(
        source_dir=training_bundle_dir,
        target_dir=deploy_bundle_dir,
        feature_contract=feature_contract,
        calibrator=calibrator,
        calibrator_meta=calibrator_meta,
        runtime_manifest=runtime_manifest,
    )

    return AutoGluonQTrainingResult(
        bundle_dir=deploy_bundle_dir,
        full_bundle_dir=training_bundle_dir,
        deploy_bundle_dir=deploy_bundle_dir,
        predictor=deployment_predictor,
        feature_contract=feature_contract,
        calibration_mode=calibration_mode,
        calibrator=calibrator,
        calibrator_meta=calibrator_meta,
        runtime_manifest=runtime_manifest,
        predictor_name=predictor_name,
        fit_rows=int(len(df_train)),
        calibration_rows=int(len(df_valid)),
    )
