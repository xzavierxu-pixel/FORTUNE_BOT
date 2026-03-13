"""Build PEG rule/LLM signals JSONL from default sources."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from execution_engine.core.config import load_config
from execution_engine.utils.io import append_jsonl
from execution_engine.connectors import rule_adapter, llm_adapter


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rule-input", default=None)
    parser.add_argument("--llm-snapshot-dir", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--rule-output", default=None)
    parser.add_argument("--llm-output", default=None)
    args = parser.parse_args()

    cfg = load_config()
    if args.run_id:
        import os

        os.environ["PEG_RUN_ID"] = args.run_id
        cfg = load_config()

    rule_input = Path(args.rule_input) if args.rule_input else cfg.rule_candidates_path
    rule_output = Path(args.rule_output) if args.rule_output else cfg.rule_signals_path

    if rule_input.exists():
        count = 0
        for row in rule_adapter._read_rows(rule_input):
            signal = rule_adapter._build_signal(row, cfg)
            append_jsonl(rule_output, signal)
            count += 1
        print(f"Wrote {count} rule signals to {rule_output}")
    else:
        print(f"Rule input missing: {rule_input}")

    snapshot_dir = Path(args.llm_snapshot_dir) if args.llm_snapshot_dir else None
    if snapshot_dir is None:
        snapshot_dir = Path(cfg.llm_snapshot_dir) / cfg.run_id

    results_path = snapshot_dir / "benchmark_results.jsonl"
    llm_output = Path(args.llm_output) if args.llm_output else cfg.llm_signals_path
    if results_path.exists():
        count = 0
        with results_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                row = json.loads(line)
                boxed = row.get("model_boxed_answer") or ""
                payload = llm_adapter._extract_json(str(boxed))
                if payload is None:
                    continue
                metadata = row.get("metadata", {}) or {}
                signal = llm_adapter._build_signal(payload, metadata, cfg)
                if signal is None:
                    continue
                append_jsonl(llm_output, signal)
                count += 1
        print(f"Wrote {count} llm signals to {llm_output}")
    else:
        print(f"LLM results missing: {results_path}")


if __name__ == "__main__":
    main()
