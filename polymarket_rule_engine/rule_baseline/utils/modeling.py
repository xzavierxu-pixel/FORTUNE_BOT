from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.calibration import CalibratedClassifierCV
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import (
    ExtraTreesClassifier,
    GradientBoostingClassifier,
    HistGradientBoostingClassifier,
    RandomForestClassifier,
    VotingClassifier,
)
from sklearn.impute import SimpleImputer
from sklearn.isotonic import IsotonicRegression
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, RobustScaler


def build_preprocessor(numeric_columns: list[str], categorical_columns: list[str]) -> ColumnTransformer:
    num_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value=0.0, keep_empty_features=True)),
            ("scaler", RobustScaler()),
        ]
    )
    cat_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN", keep_empty_features=True)),
            ("onehot", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
        ]
    )
    return ColumnTransformer(
        transformers=[
            ("num", num_pipeline, numeric_columns),
            ("cat", cat_pipeline, categorical_columns),
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


def infer_feature_types(df: pd.DataFrame, feature_columns: list[str]) -> tuple[list[str], list[str]]:
    numeric_columns: list[str] = []
    categorical_columns: list[str] = []
    for column in feature_columns:
        dtype = df[column].dtype
        if pd.api.types.is_categorical_dtype(dtype) or pd.api.types.is_object_dtype(dtype) or pd.api.types.is_bool_dtype(dtype):
            categorical_columns.append(column)
        else:
            numeric_columns.append(column)
    return numeric_columns, categorical_columns


def fit_model_payload(
    df_train: pd.DataFrame,
    df_valid: pd.DataFrame,
    feature_columns: list[str],
    target_column: str = "y",
    calibration_mode: str = "cv_isotonic",
) -> dict:
    if calibration_mode not in {"valid_isotonic", "cv_isotonic", "none"}:
        raise ValueError(f"Unsupported calibration_mode: {calibration_mode}")

    numeric_columns, categorical_columns = infer_feature_types(df_train, feature_columns)
    preprocessor = build_preprocessor(numeric_columns, categorical_columns)

    X_train = preprocessor.fit_transform(df_train[feature_columns])
    y_train = df_train[target_column].astype(int).values

    calibrator = None
    model_is_calibrated = False
    model = build_ensemble_classifier()

    if calibration_mode == "cv_isotonic":
        model = CalibratedClassifierCV(model, cv=3, method="isotonic")
        model.fit(X_train, y_train)
        model_is_calibrated = True
    else:
        model.fit(X_train, y_train)

    if calibration_mode == "valid_isotonic":
        calibrator = None
        if not df_valid.empty and df_valid[target_column].nunique() > 1:
            X_valid = preprocessor.transform(df_valid[feature_columns])
            y_valid = df_valid[target_column].astype(int).values
            raw_valid = model.predict_proba(X_valid)[:, 1]
            calibrator = IsotonicRegression(out_of_bounds="clip")
            calibrator.fit(raw_valid, y_valid)

    return {
        "preprocessor": preprocessor,
        "model": model,
        "calibrator": calibrator,
        "calibration_mode": calibration_mode,
        "model_is_calibrated": model_is_calibrated,
        "feature_columns": feature_columns,
        "numeric_columns": numeric_columns,
        "categorical_columns": categorical_columns,
    }


def predict_probabilities(payload: dict, df_feat: pd.DataFrame) -> np.ndarray:
    feature_columns = payload["feature_columns"]
    X = payload["preprocessor"].transform(df_feat[feature_columns])
    raw_prob = payload["model"].predict_proba(X)[:, 1]
    calibrator = payload.get("calibrator")
    if calibrator is not None:
        prob = calibrator.transform(raw_prob)
    else:
        prob = raw_prob
    return np.clip(prob, 0.0, 1.0)
