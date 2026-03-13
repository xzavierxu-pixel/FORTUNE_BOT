from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    ExtraTreesClassifier,
    ExtraTreesRegressor,
    GradientBoostingClassifier,
    GradientBoostingRegressor,
    HistGradientBoostingClassifier,
    HistGradientBoostingRegressor,
    RandomForestClassifier,
    RandomForestRegressor,
    VotingClassifier,
    VotingRegressor,
)
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, RobustScaler


def build_preprocessor(numeric_columns: list[str], categorical_columns: list[str]) -> ColumnTransformer:
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0, keep_empty_features=True)),
            ("scaler", RobustScaler()),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN", keep_empty_features=True)),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    return ColumnTransformer(
        transformers=[
            ("num", numeric_pipeline, numeric_columns),
            ("cat", categorical_pipeline, categorical_columns),
        ],
        remainder="drop",
    )


def build_ensemble_classifier() -> VotingClassifier:
    gb = GradientBoostingClassifier(
        n_estimators=800,
        max_depth=5,
        learning_rate=0.02,
        min_samples_split=30,
        min_samples_leaf=15,
        subsample=0.8,
        max_features="sqrt",
        random_state=42,
    )
    rf = RandomForestClassifier(
        n_estimators=800,
        max_depth=12,
        min_samples_split=10,
        min_samples_leaf=5,
        max_features="sqrt",
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    et = ExtraTreesClassifier(
        n_estimators=800,
        max_depth=12,
        min_samples_split=10,
        min_samples_leaf=5,
        class_weight="balanced_subsample",
        random_state=42,
        n_jobs=-1,
    )
    hgb = HistGradientBoostingClassifier(
        max_iter=600,
        max_depth=7,
        learning_rate=0.025,
        min_samples_leaf=25,
        l2_regularization=0.15,
        random_state=42,
    )
    return VotingClassifier(
        estimators=[("gb", gb), ("rf", rf), ("et", et), ("hgb", hgb)],
        voting="soft",
        weights=[1.2, 1.0, 1.0, 1.1],
    )


def build_ensemble_regressor() -> VotingRegressor:
    gb = GradientBoostingRegressor(
        n_estimators=500,
        max_depth=4,
        learning_rate=0.03,
        min_samples_split=30,
        min_samples_leaf=15,
        subsample=0.8,
        max_features="sqrt",
        random_state=42,
    )
    rf = RandomForestRegressor(
        n_estimators=500,
        max_depth=12,
        min_samples_split=10,
        min_samples_leaf=5,
        max_features="sqrt",
        random_state=42,
        n_jobs=-1,
    )
    et = ExtraTreesRegressor(
        n_estimators=500,
        max_depth=12,
        min_samples_split=10,
        min_samples_leaf=5,
        random_state=42,
        n_jobs=-1,
    )
    hgb = HistGradientBoostingRegressor(
        max_iter=400,
        max_depth=7,
        learning_rate=0.03,
        min_samples_leaf=25,
        l2_regularization=0.15,
        random_state=42,
    )
    return VotingRegressor(
        estimators=[("gb", gb), ("rf", rf), ("et", et), ("hgb", hgb)],
        weights=[1.0, 1.0, 1.0, 1.0],
    )


def infer_feature_types(df: pd.DataFrame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
    numeric_columns: list[str] = []
    categorical_columns: list[str] = []
    for column in feature_columns:
        dtype = df[column].dtype
        if (
            pd.api.types.is_categorical_dtype(dtype)
            or pd.api.types.is_object_dtype(dtype)
            or pd.api.types.is_string_dtype(dtype)
            or pd.api.types.is_bool_dtype(dtype)
        ):
            categorical_columns.append(column)
        else:
            numeric_columns.append(column)
    return numeric_columns, categorical_columns


def fit_probability_calibrator(raw_valid: np.ndarray, y_valid: np.ndarray, method: str):
    if method == "isotonic":
        calibrator = IsotonicRegression(out_of_bounds="clip")
        calibrator.fit(raw_valid, y_valid)
        return calibrator
    if method == "sigmoid":
        calibrator = LogisticRegression(solver="lbfgs")
        calibrator.fit(raw_valid.reshape(-1, 1), y_valid)
        return calibrator
    raise ValueError(f"Unsupported calibration method: {method}")


def fit_grouped_calibrators(
    raw_valid: np.ndarray,
    y_valid: np.ndarray,
    group_values: pd.Series,
    method: str,
    min_rows: int = 50,
) -> dict[str, object]:
    calibrators: dict[str, object] = {}
    group_series = pd.Series(group_values).reset_index(drop=True).astype(str)
    for group_value, group_index in group_series.groupby(group_series, sort=False).indices.items():
        index_array = np.asarray(group_index, dtype=int)
        if len(index_array) < min_rows:
            continue
        group_probs = raw_valid[index_array]
        group_targets = y_valid[index_array]
        if len(np.unique(group_targets)) < 2:
            continue
        calibrators[str(group_value)] = fit_probability_calibrator(group_probs, group_targets, method)
    return calibrators


def apply_probability_calibrator(calibrator, raw_prob: np.ndarray) -> np.ndarray:
    if isinstance(calibrator, IsotonicRegression):
        return calibrator.transform(raw_prob)
    if isinstance(calibrator, LogisticRegression):
        return calibrator.predict_proba(raw_prob.reshape(-1, 1))[:, 1]
    raise ValueError(f"Unsupported calibrator type: {type(calibrator)!r}")


def fit_model_payload(
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    feature_columns: list[str],
    target_column: str = "y",
    calibration_mode: str = "cv_isotonic",
) -> dict:
    valid_modes = {
        "valid_isotonic",
        "valid_sigmoid",
        "domain_valid_isotonic",
        "horizon_valid_isotonic",
        "cv_isotonic",
        "cv_sigmoid",
        "none",
    }
    if calibration_mode not in valid_modes:
        raise ValueError(f"Unsupported calibration_mode: {calibration_mode}")

    numeric_columns, categorical_columns = infer_feature_types(df_train, feature_columns)
    preprocessor = build_preprocessor(numeric_columns, categorical_columns)

    X_train = preprocessor.fit_transform(df_train[feature_columns])
    y_train = df_train[target_column].astype(int).values

    calibrator = None
    calibrator_meta: dict[str, object] | None = None
    model_is_calibrated = False
    model = build_ensemble_classifier()

    if calibration_mode in {"cv_isotonic", "cv_sigmoid"}:
        method = "isotonic" if calibration_mode == "cv_isotonic" else "sigmoid"
        model = CalibratedClassifierCV(model, cv=3, method=method)
        model.fit(X_train, y_train)
        model_is_calibrated = True
    else:
        model.fit(X_train, y_train)

    if calibration_mode in {"valid_isotonic", "valid_sigmoid", "domain_valid_isotonic", "horizon_valid_isotonic"}:
        if not df_valid.empty and df_valid[target_column].nunique() > 1:
            X_valid = preprocessor.transform(df_valid[feature_columns])
            y_valid = df_valid[target_column].astype(int).values
            raw_valid = model.predict_proba(X_valid)[:, 1]
            if calibration_mode in {"valid_isotonic", "valid_sigmoid"}:
                method = "isotonic" if calibration_mode == "valid_isotonic" else "sigmoid"
                calibrator = fit_probability_calibrator(raw_valid, y_valid, method)
                calibrator_meta = {"type": "global", "method": method}
            elif calibration_mode == "domain_valid_isotonic":
                calibrator = fit_grouped_calibrators(raw_valid, y_valid, df_valid["domain"].astype(str), "isotonic")
                calibrator_meta = {"type": "grouped", "group_column": "domain", "method": "isotonic"}
            elif calibration_mode == "horizon_valid_isotonic":
                group_values = df_valid["horizon_hours"].round().astype("Int64").astype(str)
                calibrator = fit_grouped_calibrators(raw_valid, y_valid, group_values, "isotonic")
                calibrator_meta = {"type": "grouped", "group_column": "horizon_hours", "method": "isotonic"}

    return {
        "preprocessor": preprocessor,
        "model": model,
        "calibrator": calibrator,
        "calibrator_meta": calibrator_meta,
        "calibration_mode": calibration_mode,
        "model_is_calibrated": model_is_calibrated,
        "feature_columns": feature_columns,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
    }


def fit_regression_payload(
    df_train: pd.DataFrame,
    feature_columns: list[str],
    target_column: str,
) -> dict:
    numeric_columns, categorical_columns = infer_feature_types(df_train, feature_columns)
    preprocessor = build_preprocessor(numeric_columns, categorical_columns)

    X_train = preprocessor.fit_transform(df_train[feature_columns])
    y_train = df_train[target_column].astype(float).values

    model = build_ensemble_regressor()
    model.fit(X_train, y_train)

    return {
        "preprocessor": preprocessor,
        "model": model,
        "feature_columns": feature_columns,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
        "target_column": target_column,
    }


def predict_probabilities(payload: dict, df_feat: pd.DataFrame) -> np.ndarray:
    feature_columns = payload["feature_columns"]
    X = payload["preprocessor"].transform(df_feat[feature_columns])
    raw_prob = payload["model"].predict_proba(X)[:, 1]
    calibrator = payload.get("calibrator")
    calibrator_meta = payload.get("calibrator_meta") or {}
    if calibrator is not None and calibrator_meta.get("type") == "grouped":
        prob = raw_prob.copy()
        group_column = calibrator_meta.get("group_column")
        if group_column and group_column in df_feat.columns:
            groups = pd.Series(df_feat[group_column]).reset_index(drop=True).astype(str)
            for group_value, indices in groups.groupby(groups, sort=False).indices.items():
                group_calibrator = calibrator.get(str(group_value))
                if group_calibrator is None:
                    continue
                index_array = np.asarray(indices, dtype=int)
                prob[index_array] = apply_probability_calibrator(group_calibrator, raw_prob[index_array])
    elif calibrator is not None:
        prob = apply_probability_calibrator(calibrator, raw_prob)
    else:
        prob = raw_prob
    return np.clip(prob, 0.0, 1.0)


def predict_regression(payload: dict, df_feat: pd.DataFrame) -> np.ndarray:
    feature_columns = payload["feature_columns"]
    X = payload["preprocessor"].transform(df_feat[feature_columns])
    return payload["model"].predict(X)
