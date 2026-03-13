from __future__ import annotations

import warnings

import numpy as np
import pandas as pd
from catboost import CatBoostClassifier, CatBoostRegressor
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import VotingClassifier, VotingRegressor
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.linear_model import LogisticRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, RobustScaler
from xgboost import XGBClassifier, XGBRegressor

SEMANTIC_CATEGORICAL_FEATURES = {
    "domain",
    "category",
    "category_raw",
    "category_parsed",
    "category_override_flag",
    "market_type",
    "leaf_id",
    "direction",
    "group_key",
    "groupItemTitle_market",
    "gameId_market",
    "has_line",
    "has_number",
    "has_year",
    "has_dollar",
    "has_date",
    "starts_will",
    "starts_can",
    "has_by",
    "has_above_below",
    "has_or",
    "has_and",
    "is_player_prop",
    "is_team_total",
    "is_finance_threshold",
    "is_high_ambiguity",
    "cat_sports",
    "cat_crypto",
    "cat_politics",
    "cat_world",
    "cat_tech",
    "cat_entertainment",
    "dur_very_short",
    "dur_short",
    "dur_medium",
    "dur_long",
    "sub_domain_market",
    "source_url_market",
    "outcome_pattern_market",
}


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
    xgb = XGBClassifier(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        min_child_weight=5,
        objective="binary:logistic",
        eval_metric="logloss",
        random_state=42,
        n_jobs=-1,
    )
    lgbm = LGBMClassifier(
        n_estimators=500,
        learning_rate=0.03,
        num_leaves=63,
        max_depth=-1,
        min_child_samples=25,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        objective="binary",
        random_state=42,
        n_jobs=-1,
    )
    cat = CatBoostClassifier(
        iterations=500,
        depth=6,
        learning_rate=0.03,
        l2_leaf_reg=5.0,
        loss_function="Logloss",
        eval_metric="Logloss",
        random_state=42,
        verbose=False,
        allow_writing_files=False,
    )
    return VotingClassifier(
        estimators=[("xgb", xgb), ("lgbm", lgbm), ("cat", cat)],
        voting="soft",
        weights=[1.0, 1.1, 1.0],
    )


def build_ensemble_regressor() -> VotingRegressor:
    xgb = XGBRegressor(
        n_estimators=500,
        max_depth=6,
        learning_rate=0.03,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        min_child_weight=5,
        objective="reg:squarederror",
        random_state=42,
        n_jobs=-1,
    )
    lgbm = LGBMRegressor(
        n_estimators=500,
        learning_rate=0.03,
        num_leaves=63,
        max_depth=-1,
        min_child_samples=25,
        subsample=0.8,
        colsample_bytree=0.8,
        reg_lambda=2.0,
        objective="regression",
        random_state=42,
        n_jobs=-1,
    )
    cat = CatBoostRegressor(
        iterations=500,
        depth=6,
        learning_rate=0.03,
        l2_leaf_reg=5.0,
        loss_function="RMSE",
        random_state=42,
        verbose=False,
        allow_writing_files=False,
    )
    return VotingRegressor(
        estimators=[("xgb", xgb), ("lgbm", lgbm), ("cat", cat)],
        weights=[1.0, 1.1, 1.0],
    )


def infer_feature_types(df: pd.DataFrame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
    numeric_columns: list[str] = []
    categorical_columns: list[str] = []
    for column in feature_columns:
        if column in SEMANTIC_CATEGORICAL_FEATURES:
            categorical_columns.append(column)
            continue

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


def coerce_feature_frame(
    df: pd.DataFrame,
    numeric_columns: list[str],
    categorical_columns: list[str],
) -> pd.DataFrame:
    out = df.copy()
    for column in numeric_columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    for column in categorical_columns:
        out[column] = out[column].astype("string").fillna("UNKNOWN").astype(object)
    return out


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
    train_features = coerce_feature_frame(df_train[feature_columns], numeric_columns, categorical_columns)
    X_train = preprocessor.fit_transform(train_features)
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
            valid_features = coerce_feature_frame(df_valid[feature_columns], numeric_columns, categorical_columns)
            X_valid = preprocessor.transform(valid_features)
            y_valid = df_valid[target_column].astype(int).values
            with warnings.catch_warnings():
                warnings.filterwarnings(
                    "ignore",
                    message="X does not have valid feature names, but LGBMClassifier was fitted with feature names",
                    category=UserWarning,
                )
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
    train_features = coerce_feature_frame(df_train[feature_columns], numeric_columns, categorical_columns)
    X_train = preprocessor.fit_transform(train_features)
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
    feature_frame = coerce_feature_frame(df_feat[feature_columns], payload["numeric_columns"], payload["categorical_columns"])
    X = payload["preprocessor"].transform(feature_frame)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names, but LGBMClassifier was fitted with feature names",
            category=UserWarning,
        )
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
    feature_frame = coerce_feature_frame(df_feat[feature_columns], payload["numeric_columns"], payload["categorical_columns"])
    X = payload["preprocessor"].transform(feature_frame)
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="X does not have valid feature names, but LGBMRegressor was fitted with feature names",
            category=UserWarning,
        )
        return payload["model"].predict(X)
