# Submit Window Online Troubleshooting

## Command

```powershell
$env:PYTHONPATH = "polymarket_rule_engine;."
$envFile = 'version3.server.env'
Get-Content $envFile | Where-Object { $_ -match '^[^#]' } | ForEach-Object {
    $parts = $_ -split '=', 2
    if ($parts.Length -eq 2) {
        $name = $parts[0].Trim()
        $value = $parts[1].Trim().Trim("'").Trim('"')
        Set-Content "env:$name" $value
    }
}
execution_engine\app\scripts\online\run_submit_window.ps1 -MaxPages 30
```

## What happened in the 2026-04-17 run

- The command did not fail immediately. It progressed through `market_stream` and `submit_window` and wrote run manifests under `execution_engine\data\runs\2026-04-17\SUBMIT_WINDOW\`.
- The apparent "hang" was a long quiet period rather than a confirmed deadlock. The wrapper invocation timed out, but the run artifacts show `submit_stage_status=completed` and `final_status=completed`.
- The real functional result was that all expanded markets were filtered out before live inference or order submission.

Key counters from `execution_engine\data\runs\2026-04-17\SUBMIT_WINDOW\submit_window\manifest.json`:

- `page_count = 10`
- `expanded_market_count = 116`
- `structural_reject_count = 116`
- `state_reject_count = 0`
- `direct_candidate_count = 0`
- `submitted_order_count = 0`

This means the pipeline never reached live inference for that run. The issue is upstream of pricing, selection, and submission.

## Warnings seen in logs

### `PerformanceWarning: DataFrame is highly fragmented`

Source:

- `polymarket_rule_engine\rule_baseline\features\tabular.py`

Meaning:

- This is a pandas performance warning caused by repeatedly inserting columns into the same frame.
- It is not the reason the online submit pipeline stopped producing candidates.

Mitigation applied:

- Missing object columns in `preprocess_features()` are now added in one batch instead of several sequential `frame["col"] = ...` inserts. This reduces fragmentation in the hot path that produced the warning.

### `Missing non-critical feature_contract columns defaulted in live inference`

Example:

- `h_max, h_min, horizon_hours_rule, price_max, price_min`

Meaning:

- Runtime feature alignment defaulted optional non-critical columns that were absent from the live batch.
- This is non-fatal by design. It indicates a contract alignment fallback, not a submission freeze.

Important note:

- In the specific 2026-04-17 run inspected here, live inference was not reached because all markets were removed by the structural coarse filter first. So this warning explains a different run path, not this exact `116/116` rejection outcome.

## Audit visibility in minimal artifact mode

Problem:

- `PEG_ARTIFACT_POLICY=minimal` previously retained only submission-boundary candidate events.
- When a run died earlier in the funnel, `audit/funnel_summary.json` could show counts from the submit manifest but lose reject reasons.

Mitigation applied:

- Minimal mode now retains the candidate states needed for funnel audit:
  - `STRUCTURAL_REJECT`
  - `STATE_REJECT`
  - `LIVE_PRICE_MISS`
  - `LIVE_SPREAD_TOO_WIDE`
  - `LIVE_STATE_MISSING`
  - `LIVE_STATE_STALE`
  - `INVALID_PRICE`
  - `SELECTED_FOR_SUBMISSION`
  - `SUBMISSION_REJECTED`
  - `SUBMITTED`

Impact:

- `audit/funnel_summary.json` can recover reason counts for early-stage filtering without switching the whole run to debug artifacts.
- This does increase `events.jsonl` volume slightly versus the old minimal behavior, but only for audit-relevant states.

## How to diagnose the next run quickly

Check these files first:

- `execution_engine\data\runs\<date>\SUBMIT_WINDOW\submit_window\manifest.json`
- `execution_engine\data\runs\<date>\SUBMIT_WINDOW\audit\funnel_summary.json`
- `execution_engine\data\runs\<date>\SUBMIT_WINDOW\market_stream\manifest.json`

Interpretation order:

1. If `direct_candidate_count = 0`, debug the structural coarse filter and rule-family coverage first.
2. If `direct_candidate_count > 0` but `live_eligible_count = 0`, inspect live state freshness, spread, and price gates.
3. If `selected_count > 0` but `submit_attempted_count = 0`, inspect selection gating and quote lookup.
4. If `submit_attempted_count > 0` but `submitted_order_count = 0`, inspect submission rejection statuses.
