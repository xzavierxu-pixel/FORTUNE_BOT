from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal

from rule_baseline.datasets.artifacts import build_artifact_paths
from rule_baseline.utils import config


@dataclass(frozen=True)
class DataScopeConfig:
    max_rows: int | None
    recent_days: int | None


@dataclass(frozen=True)
class SplitConfig:
    artifact_mode: Literal["offline", "online"]
    history_start: str | None
    split_reference_end: str | None
    train_start: str
    train_end: str
    valid_start: str | None
    valid_end: str | None
    test_start: str | None
    test_end: str | None
    allowed_splits: tuple[str, ...]


@dataclass(frozen=True)
class SamplingConfig:
    train_sample_rows: int | None
    train_sample_seed: int | None
    train_sample_scope: Literal["train_only"]


@dataclass(frozen=True)
class PublishConfig:
    prediction_publish_split: Literal["test", "valid", "train"]
    fail_if_publish_split_empty: bool


@dataclass(frozen=True)
class PipelineConfig:
    artifact_mode: Literal["offline", "online"]
    max_rows: int | None
    recent_days: int | None
    split: SplitConfig
    sampling: SamplingConfig
    publish: PublishConfig

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["split"]["allowed_splits"] = list(self.split.allowed_splits)
        return payload


def _datetime_to_iso(value) -> str | None:
    return value.isoformat() if value is not None else None


def default_pipeline_config_path(artifact_mode: str = "offline") -> Path:
    artifact_paths = build_artifact_paths(artifact_mode)
    return artifact_paths.audit_dir / "pipeline_runtime_config.json"


def resolve_pipeline_config(
    *,
    artifact_mode: str,
    max_rows: int | None,
    recent_days: int | None,
    history_start: str | None,
    split_reference_end: str | None,
    offline_validation_days: int = config.VALIDATION_DAYS,
    offline_test_days: int = config.TEST_DAYS,
    online_validation_days: int = config.ONLINE_VALIDATION_DAYS,
    train_sample_rows: int | None = None,
    train_sample_seed: int | None = 21,
    prediction_publish_split: str | None = None,
    fail_if_empty_split: bool = True,
) -> PipelineConfig:
    normalized_mode = artifact_mode.strip().lower()
    if normalized_mode not in {"offline", "online"}:
        raise ValueError(f"Unsupported artifact_mode: {artifact_mode}")

    reference_end_dt = config.parse_utc_datetime(split_reference_end) if split_reference_end is not None else config.current_utc()
    history_start_dt = config.resolve_history_start(history_start)

    if normalized_mode == "offline":
        (
            train_start,
            train_end,
            valid_start,
            valid_end,
            test_start,
            test_end,
        ) = config.compute_three_way_split_boundaries(
            reference_end_dt,
            validation_days=offline_validation_days,
            test_days=offline_test_days,
            history_start_override=history_start_dt,
        )
        allowed_splits: tuple[str, ...] = ("train", "valid", "test")
        resolved_publish_split = prediction_publish_split or "test"
    else:
        # Online uses the full resolved dataset as train. We keep the validation-days
        # parameter in the runtime artifact for traceability, but it does not produce
        # a separate split under the confirmed contract.
        _ = online_validation_days
        train_start = history_start_dt
        train_end = reference_end_dt
        valid_start = None
        valid_end = None
        test_start = None
        test_end = None
        allowed_splits = ("train",)
        resolved_publish_split = prediction_publish_split or "train"

    if train_sample_rows is None:
        sampling_seed = train_sample_seed if train_sample_seed is not None else 21
    else:
        sampling_seed = train_sample_seed if train_sample_seed is not None else 21

    return PipelineConfig(
        artifact_mode=normalized_mode,  # type: ignore[arg-type]
        max_rows=max_rows,
        recent_days=recent_days,
        split=SplitConfig(
            artifact_mode=normalized_mode,  # type: ignore[arg-type]
            history_start=_datetime_to_iso(history_start_dt),
            split_reference_end=_datetime_to_iso(reference_end_dt),
            train_start=_datetime_to_iso(train_start) or "",
            train_end=_datetime_to_iso(train_end) or "",
            valid_start=_datetime_to_iso(valid_start),
            valid_end=_datetime_to_iso(valid_end),
            test_start=_datetime_to_iso(test_start),
            test_end=_datetime_to_iso(test_end),
            allowed_splits=allowed_splits,
        ),
        sampling=SamplingConfig(
            train_sample_rows=train_sample_rows,
            train_sample_seed=sampling_seed,
            train_sample_scope="train_only",
        ),
        publish=PublishConfig(
            prediction_publish_split=resolved_publish_split,  # type: ignore[arg-type]
            fail_if_publish_split_empty=bool(fail_if_empty_split),
        ),
    )


def write_pipeline_runtime_config(
    pipeline_config: PipelineConfig,
    path: Path | None = None,
) -> Path:
    output_path = path or default_pipeline_config_path(pipeline_config.artifact_mode)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as file:
        json.dump(pipeline_config.to_dict(), file, ensure_ascii=False, indent=2)
    return output_path


def load_pipeline_runtime_config(
    path: str | Path | None = None,
    *,
    artifact_mode: str = "offline",
) -> PipelineConfig:
    input_path = Path(path) if path is not None else default_pipeline_config_path(artifact_mode)
    with input_path.open("r", encoding="utf-8") as file:
        payload = json.load(file)
    split_payload = dict(payload["split"])
    split_payload["allowed_splits"] = tuple(split_payload.get("allowed_splits") or [])
    return PipelineConfig(
        artifact_mode=payload["artifact_mode"],
        max_rows=payload.get("max_rows"),
        recent_days=payload.get("recent_days"),
        split=SplitConfig(**split_payload),
        sampling=SamplingConfig(**payload["sampling"]),
        publish=PublishConfig(**payload["publish"]),
    )


def required_export_splits(pipeline_config: PipelineConfig) -> tuple[str, ...]:
    if pipeline_config.artifact_mode == "offline":
        return ("train", "valid")
    return ("train",)
