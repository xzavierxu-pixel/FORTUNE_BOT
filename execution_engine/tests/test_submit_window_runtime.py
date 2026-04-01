import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch
import sys

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from execution_engine.online.pipeline import submit_window
from execution_engine.shared.logger import log_structured
from execution_engine.runtime.run_state import acquire_submit_phase


def _base_cfg(root: Path) -> SimpleNamespace:
    return SimpleNamespace(
        run_id="RUN_001",
        run_mode="submit_window",
        run_date="2026-04-01",
        submit_phase_lock_path=root / "runtime" / "submit_phase.lock",
        run_submit_window_manifest_path=root / "submit_window" / "manifest.json",
        submit_window_run_monitor_after=True,
        submit_window_async_post_submit=True,
    )


class SubmitWindowRuntimeTest(unittest.TestCase):
    def test_run_submit_window_skips_when_previous_submit_phase_active(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = _base_cfg(root)
            with acquire_submit_phase(cfg.submit_phase_lock_path, run_id="RUN_PREV", run_mode="submit_window"):
                with patch.object(submit_window, "_publish_submit_window_summary", return_value=None):
                    result = submit_window.run_submit_window(cfg, max_pages=1)

            self.assertEqual(result.final_status, "skipped_previous_submit_phase_active")
            manifest = json.loads(cfg.run_submit_window_manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["submit_stage_status"], "skipped_previous_submit_phase_active")
            self.assertIn("blocking_submit_phase", manifest)

    def test_run_submit_window_schedules_post_submit_without_blocking_result(self) -> None:
        with TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            cfg = _base_cfg(root)

            def fake_replace(ns: SimpleNamespace, **changes: object) -> SimpleNamespace:
                data = dict(ns.__dict__)
                data.update(changes)
                return SimpleNamespace(**data)

            def fake_sync_impl(run_cfg: SimpleNamespace, *, max_pages: int | None = None) -> submit_window.SubmitWindowResult:
                manifest = {
                    "run_id": run_cfg.run_id,
                    "run_mode": run_cfg.run_mode,
                    "final_status": "completed",
                    "post_submit_monitor_enabled": False,
                    "post_submit_monitor_status": "skipped",
                    "post_submit_monitor_manifest_path": "",
                    "post_submit_latest_order_count": 0,
                    "post_submit_open_order_count": 0,
                    "post_submit_fill_count": 0,
                    "post_submit_open_position_count": 0,
                    "post_submit_exit_candidate_count": 0,
                    "post_submit_exit_submitted_count": 0,
                    "post_submit_settlement_close_count": 0,
                    "post_submit_canceled_exit_order_count": 0,
                    "metrics": {},
                    "pages": [],
                }
                run_cfg.run_submit_window_manifest_path.parent.mkdir(parents=True, exist_ok=True)
                run_cfg.run_submit_window_manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                return submit_window.SubmitWindowResult(
                    run_manifest_path=str(run_cfg.run_submit_window_manifest_path),
                    final_status="completed",
                    submit_phase_status="completed",
                    page_count=1,
                    expanded_market_count=2,
                    direct_candidate_count=3,
                    submitted_order_count=4,
                    submit_rejection_count=0,
                    underfilled_batch_count=0,
                    underfilled_batch_avg_size=0.0,
                    metrics={},
                    post_submit_monitor_status="skipped",
                    post_submit_monitor_manifest_path="",
                    post_submit_latest_order_count=0,
                    post_submit_open_order_count=0,
                    post_submit_fill_count=0,
                    post_submit_open_position_count=0,
                    post_submit_exit_candidate_count=0,
                    post_submit_exit_submitted_count=0,
                    post_submit_settlement_close_count=0,
                    post_submit_canceled_exit_order_count=0,
                    submit_phase_started_at_bj="",
                    submit_phase_finished_at_bj="",
                    post_submit_started_at_bj="",
                    post_submit_finished_at_bj="",
                    pages=[],
                )

            with (
                patch.object(submit_window, "replace", side_effect=fake_replace),
                patch.object(submit_window, "_run_submit_window_sync_impl", side_effect=fake_sync_impl),
                patch.object(submit_window, "_spawn_async_post_submit", return_value=True),
                patch.object(submit_window, "_publish_submit_window_summary", return_value=None),
            ):
                result = submit_window.run_submit_window(cfg, max_pages=1)

            self.assertEqual(result.final_status, "completed_post_submit_scheduled")
            self.assertEqual(result.post_submit_monitor_status, "scheduled")
            manifest = json.loads(cfg.run_submit_window_manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(manifest["post_submit_monitor_enabled"])
            self.assertEqual(manifest["post_submit_monitor_status"], "scheduled")
            self.assertIn("submit_phase_started_at_bj", manifest)
            self.assertIn("submit_phase_finished_at_bj", manifest)

    def test_log_structured_adds_beijing_timestamp_fields(self) -> None:
        with TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "logs.jsonl"
            log_structured(
                path,
                {
                    "type": "order_state",
                    "created_at_utc": "2026-04-01T00:00:00Z",
                    "updated_at_utc": "2026-04-01T00:01:00Z",
                },
            )
            row = json.loads(path.read_text(encoding="utf-8").strip())
            self.assertEqual(row["created_at_bj"], "2026-04-01T08:00:00+08:00")
            self.assertEqual(row["updated_at_bj"], "2026-04-01T08:01:00+08:00")
            self.assertIn("logged_at_bj", row)


if __name__ == "__main__":
    unittest.main()
