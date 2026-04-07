from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import joblib


RUNTIME_BUNDLE_DIRNAME = "q_model_bundle_deploy"
FULL_TRAINING_BUNDLE_DIRNAME = "q_model_bundle_full"
LEGACY_RUNTIME_BUNDLE_DIRNAME = "q_model_bundle"
RUNTIME_MANIFEST_NAME = "runtime_manifest.json"
FEATURE_CONTRACT_NAME = "feature_contract.json"
NORMALIZATION_MANIFEST_NAME = "normalization_manifest.json"
CALIBRATOR_NAME = "calibrator.pkl"
CALIBRATOR_META_NAME = "calibrator_meta.json"


@dataclass(frozen=True)
class FeatureContract:
    feature_columns: tuple[str, ...]
    numeric_columns: tuple[str, ...]
    categorical_columns: tuple[str, ...]
    required_critical_columns: tuple[str, ...] = ()
    required_noncritical_columns: tuple[str, ...] = ()
    optional_debug_columns: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, list[str]]:
        return {
            "feature_columns": list(self.feature_columns),
            "numeric_columns": list(self.numeric_columns),
            "categorical_columns": list(self.categorical_columns),
            "required_critical_columns": list(self.required_critical_columns),
            "required_noncritical_columns": list(self.required_noncritical_columns),
            "optional_debug_columns": list(self.optional_debug_columns),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FeatureContract":
        feature_columns = tuple(str(value) for value in payload.get("feature_columns", []))
        required_critical = tuple(str(value) for value in payload.get("required_critical_columns", []))
        required_noncritical = tuple(str(value) for value in payload.get("required_noncritical_columns", []))
        return cls(
            feature_columns=feature_columns,
            numeric_columns=tuple(str(value) for value in payload.get("numeric_columns", [])),
            categorical_columns=tuple(str(value) for value in payload.get("categorical_columns", [])),
            required_critical_columns=required_critical,
            required_noncritical_columns=required_noncritical or feature_columns,
            optional_debug_columns=tuple(str(value) for value in payload.get("optional_debug_columns", [])),
        )


@dataclass(frozen=True)
class RuntimeBundlePaths:
    root_dir: Path
    predictor_dir: Path
    calibration_dir: Path
    metadata_dir: Path
    runtime_manifest_path: Path
    feature_contract_path: Path
    normalization_manifest_path: Path
    calibrator_path: Path
    calibrator_meta_path: Path
    deployment_summary_path: Path

    def ensure_dirs(self) -> None:
        for path in [self.root_dir, self.predictor_dir, self.calibration_dir, self.metadata_dir]:
            path.mkdir(parents=True, exist_ok=True)


def build_runtime_bundle_paths(bundle_dir: Path) -> RuntimeBundlePaths:
    return RuntimeBundlePaths(
        root_dir=bundle_dir,
        predictor_dir=bundle_dir / "predictor",
        calibration_dir=bundle_dir / "calibration",
        metadata_dir=bundle_dir / "metadata",
        runtime_manifest_path=bundle_dir / RUNTIME_MANIFEST_NAME,
        feature_contract_path=bundle_dir / FEATURE_CONTRACT_NAME,
        normalization_manifest_path=bundle_dir / NORMALIZATION_MANIFEST_NAME,
        calibrator_path=bundle_dir / "calibration" / CALIBRATOR_NAME,
        calibrator_meta_path=bundle_dir / "calibration" / CALIBRATOR_META_NAME,
        deployment_summary_path=bundle_dir / "metadata" / "deployment_summary.json",
    )


def is_runtime_bundle(path: Path) -> bool:
    if path.is_dir() and (path / RUNTIME_MANIFEST_NAME).exists():
        return True
    if path.is_file():
        for dirname in [RUNTIME_BUNDLE_DIRNAME, LEGACY_RUNTIME_BUNDLE_DIRNAME, FULL_TRAINING_BUNDLE_DIRNAME]:
            candidate = path.parent / dirname
            if candidate.is_dir() and (candidate / RUNTIME_MANIFEST_NAME).exists():
                return True
    return False


def resolve_runtime_bundle_dir(path: Path) -> Path:
    if path.is_dir() and (path / RUNTIME_MANIFEST_NAME).exists():
        return path
    for dirname in [RUNTIME_BUNDLE_DIRNAME, LEGACY_RUNTIME_BUNDLE_DIRNAME, FULL_TRAINING_BUNDLE_DIRNAME]:
        candidate = path.parent / dirname
        if candidate.is_dir() and (candidate / RUNTIME_MANIFEST_NAME).exists():
            return candidate
    raise FileNotFoundError(f"Runtime bundle not found for path: {path}")


def write_bundle_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, ensure_ascii=False, indent=2)


def read_bundle_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise TypeError(f"Expected JSON object at {path}, got {type(payload)!r}.")
    return payload


def save_feature_contract(path: Path, feature_contract: FeatureContract) -> None:
    write_bundle_json(path, feature_contract.to_dict())


def save_normalization_manifest(path: Path, payload: dict[str, Any]) -> None:
    write_bundle_json(path, payload)


def load_feature_contract(path: Path) -> FeatureContract:
    return FeatureContract.from_dict(read_bundle_json(path))


def save_calibrator(path: Path, calibrator: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(calibrator, path)


def load_calibrator(path: Path) -> object:
    return joblib.load(path)


def dataclass_to_dict(value: Any) -> dict[str, Any]:
    return asdict(value)
