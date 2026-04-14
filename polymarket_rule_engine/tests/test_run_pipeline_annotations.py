from __future__ import annotations

import os
import sys

sys.path.append(os.path.abspath("polymarket_rule_engine"))

from rule_baseline.workflow import run_pipeline


def test_run_pipeline_includes_market_annotations_by_default(monkeypatch) -> None:
    executed: list[tuple[str, list[str]]] = []

    def fake_run_step(label: str, command: list[str]) -> None:
        executed.append((label, command))

    monkeypatch.setattr(run_pipeline, "run_step", fake_run_step)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_pipeline.py", "--skip-fetch", "--skip-snapshots", "--skip-analysis", "--skip-backtest", "--skip-baselines"],
    )

    run_pipeline.main()

    assert any(label == "Build market annotations" for label, _ in executed)
    annotation_commands = [command for label, command in executed if label == "Build market annotations"]
    assert annotation_commands == [[sys.executable, "rule_baseline/domain_extractor/build_market_annotations.py"]]


def test_run_pipeline_can_skip_market_annotations(monkeypatch) -> None:
    executed: list[tuple[str, list[str]]] = []

    def fake_run_step(label: str, command: list[str]) -> None:
        executed.append((label, command))

    monkeypatch.setattr(run_pipeline, "run_step", fake_run_step)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "run_pipeline.py",
            "--skip-fetch",
            "--skip-annotations",
            "--skip-snapshots",
            "--skip-analysis",
            "--skip-backtest",
            "--skip-baselines",
        ],
    )

    run_pipeline.main()

    assert all(label != "Build market annotations" for label, _ in executed)


def test_run_pipeline_builds_groupkey_validation_reports_after_model(monkeypatch) -> None:
    executed: list[tuple[str, list[str]]] = []

    def fake_run_step(label: str, command: list[str]) -> None:
        executed.append((label, command))

    monkeypatch.setattr(run_pipeline, "run_step", fake_run_step)
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_pipeline.py", "--skip-fetch", "--skip-snapshots", "--skip-analysis", "--skip-backtest", "--skip-baselines"],
    )

    run_pipeline.main()

    labels = [label for label, _ in executed]
    assert "Train rules" in labels
    assert "Train model" in labels
    assert "Build GroupKey validation reports" in labels
    assert labels.index("Build GroupKey validation reports") > labels.index("Train model")
    report_commands = [command for label, command in executed if label == "Build GroupKey validation reports"]
    assert report_commands == [[sys.executable, "rule_baseline/training/build_groupkey_validation_reports.py", "--artifact-mode", "offline"]]
