# Online Execution Pipeline Design

## Status

This document describes the **target design** for the next iteration of the online execution pipeline.

It is **not** a description of the exact current implementation.

The goal is to provide a coding agent with a precise implementation blueprint for:

- market universe refresh
- token-level market data ingestion
- hourly snapshot generation
- rule/model scoring with `rule_baseline`
- passive limit-order execution
- order lifecycle management
- daily label sync and analysis

## Objectives

The online system must satisfy the following requirements:

1. Use a rolling **24-hour-to-expiry** universe of open Polymarket markets.
2. Only trade markets with exactly **two outcomes / two tokens**.
3. Never open both sides of the same market.
4. Evaluate markets at the **market level**, not the token level.
5. Use **hourly snapshots** on the hour.
6. Use **WebSocket market data** as the primary source for online snapshots.
7. Use `rule_baseline` logic for feature processing, rule matching, and model scoring to preserve offline/online consistency.
8. Use **passive limit orders** with a default price of `best_bid - 1 tick`.
9. Treat **partial fill as opened position**.
10. Stop future snapshot collection for a market only after it has actually opened a position.
11. If an order is not filled within **5 minutes**, cancel or let it expire and allow the market to re-enter the next hourly universe.
12. Persist all intermediate artifacts needed for replay, audit, debugging, and daily analysis.

## Non-Goals

This design intentionally does **not** optimize for:

- immediate execution
- aggressive crossing of the spread
- multi-outcome markets
- intrahour continuous re-scoring
- discretionary or continuously re-priced position management after entry

The strategy is passive, slow, and consistency-first, with a single fixed take-profit exit order after entry and settlement fallback if the exit order never fills.

## High-Level Architecture

The system should be split into four jobs.

### 1. `universe_refresh_6h`

Runs every 6 hours.

Responsibilities:

- fetch open markets
- filter to markets expiring within the next 24 hours
- filter to markets with exactly two tokens
- filter out markets that already have open positions
- build market-to-token mapping
- produce a canonical market universe artifact

### 2. `market_stream_manager`

Runs continuously.

Responsibilities:

- subscribe to Polymarket WebSocket `market` channel
- subscribe by `asset_id` / `token_id`, not `market_id`
- maintain latest token state in memory
- shard subscriptions across a small number of WebSocket connections if needed
- persist raw market events for replay and recovery

### 3. `snapshot_score_hourly`

Runs at the top of every hour.

Responsibilities:

- load the current universe
- exclude markets that already have an open position
- exclude markets currently in a pending order window
- sort markets by remaining time to expiry, nearest first
- process markets in batches of **20 markets**
- for each market, build one canonical hourly snapshot
- construct model input features using shared `rule_baseline` logic
- match rules, run model inference, allocate stake
- select at most one token per market
- re-check live order book before order submission
- submit passive limit orders

### 4. `label_analysis_daily`

Runs once per day after the last hourly cycle.

Responsibilities:

- sync labels for newly resolved markets
- attach labels to execution and opportunity records
- run post-trade analysis
- produce daily analysis outputs comparable to `rule_baseline/analysis` and `rule_baseline/backtesting`

## Canonical Entities

### Market

The decision unit.

Required fields:

- `market_id`
- `end_time_utc`
- `remaining_hours`
- `outcome_0_label`
- `outcome_1_label`
- `token_0_id`
- `token_1_id`
- `category`
- `domain`
- `market_type`

### Token

The market data subscription and order execution unit.

Required fields:

- `token_id`
- `market_id`
- `outcome_label`
- `side_index`

### Position

A market is considered opened once any submitted order on one side receives a non-zero fill.

Rules:

- one market can only have one opened side
- partial fill counts as opened
- once opened, the market is permanently excluded from future hourly snapshot collection

## Universe Refresh Design

### Input Source

- REST API: open markets endpoint

### Filtering Rules

Keep a market if all conditions are true:

1. market is open
2. `0 < end_time_utc - now <= 24 hours`
3. market has exactly two outcomes
4. market has exactly two token ids
5. market is not already in the opened position ledger

### Output Artifact

Suggested path:

- `execution_engine/data/shared/universe/current_universe.parquet`

Suggested columns:

- `universe_run_id`
- `market_id`
- `end_time_utc`
- `remaining_hours`
- `category`
- `domain`
- `market_type`
- `outcome_0_label`
- `outcome_1_label`
- `token_0_id`
- `token_1_id`
- `selected_reference_token_id`
- `selected_reference_outcome_label`
- `excluded_reason`

### Reference Token Selection

For hourly scoring, each market should expose one `selected_reference_token_id`.

This token is used only to build the **market-level snapshot**.

The design does not require both tokens to be continuously subscribed for scoring.

The target token for order placement is selected later, after scoring.

## WebSocket Market Data Design

### Primary Source

- `wss://ws-subscriptions-clob.polymarket.com/ws/market`

### Subscription Model

Subscribe by `asset_id` / `token_id`.

Example:

```json
{
  "type": "market",
  "assets_ids": ["TOKEN_ID_A", "TOKEN_ID_B", "TOKEN_ID_C"]
}
```

### Key Design Decision

Do **not** create one WebSocket per market or one WebSocket per token.

Use one or a small number of WebSocket connections, each carrying multiple token subscriptions.

### Sharding

Initial design:

- if active subscribed tokens <= 20: use 1 connection
- if active subscribed tokens between 21 and 80: use 2 to 4 connections
- above that, shard by token count and estimated activity

This design does not require domain-based sharding.
Token throughput is the main constraint.

### Raw Event Persistence

Persist every raw WebSocket event.

Suggested path layout:

- `execution_engine/data/shared/ws_raw/YYYY-MM-DD/HH/shard_<id>.jsonl`

Each record should contain:

- `received_at_utc`
- `shard_id`
- `token_id`
- raw payload

This is the recovery and replay source of truth.

## Hourly Snapshot Design

### Trigger

Run on the hour.

### Candidate Market Set

Start from current universe, then exclude:

1. markets already opened
2. markets with a pending live order that has not yet reached its 5-minute terminal state

### Processing Order

Sort by:

1. `remaining_hours` ascending
2. `end_time_utc` ascending

### Batch Size

Process **20 markets per batch**.

Rationale:

- scoring and feature construction take time
- smaller batches reduce stale-price risk
- each market only needs one reference token during scoring

### Snapshot Semantics

Each hourly run should generate one canonical market-level snapshot per processed market.

The snapshot must be aligned with `rule_baseline` conventions wherever possible.

Required fields:

- `market_id`
- `snapshot_time`
- `snapshot_date`
- `scheduled_end`
- `closedTime`
- `price`
- `horizon_hours`
- `category`
- `domain`
- `market_type`
- any additional fields required by `rule_baseline` feature preprocessing

### Important Constraint

The online snapshot builder should **not** become a second independent feature system.

Instead:

- move or expose reusable logic inside `polymarket_rule_engine/rule_baseline`
- call shared normalization and preprocessing functions from the online pipeline

The online system may provide online-specific raw inputs, but the transformation logic should be shared.

## Offline/Online Consistency Requirement

The target implementation must reuse or refactor shared logic from:

- [snapshots.py](/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/datasets/snapshots.py)
- [tabular.py](/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/features/tabular.py)
- [backtest_portfolio_qmodel.py](/Users/ROG/Desktop/fortune_bot/polymarket_rule_engine/rule_baseline/backtesting/backtest_portfolio_qmodel.py)

The following must remain consistent between offline and online:

- snapshot field naming
- horizon semantics
- rule matching
- feature preprocessing
- model input schema
- candidate scoring
- stake sizing

## Rule and Model Scoring Design

### Scoring Unit

The scoring unit is the **market**.

### Rule Hit Semantics

A market can only belong to one horizon bucket at one point in time because it only has one current `remaining_hours`.

A market may still match multiple rules inside that bucket and category/domain space.

### Required Artifacts Per Processed Market

Persist the following even if no order is submitted:

1. `raw_snapshot`
2. `normalized_snapshot`
3. `feature_input`
4. `rule_hits`
5. `model_outputs`
6. `selection_decision`

### `rule_hits` Must Include

- `rule_group_key`
- `rule_leaf_id`
- `price_min`
- `price_max`
- `h_min`
- `h_max`
- `rule_score`
- reason why the rule matched

### `model_outputs` Must Include

- `q_pred`
- `trade_value_pred`
- `growth_score`
- `f_exec`
- `direction_model`
- selected target outcome label
- selected target token id

### Selection Constraint

At most one token may be selected per market.

If both tokens or both directions appear feasible, keep only the higher-priority side according to deterministic ranking:

1. higher `growth_score`
2. higher expected edge
3. nearer expiry
4. stable tie-break by `market_id`

## Execution Design

### Pre-Submission Recheck

After scoring selects one token for a market, fetch or read the latest live order book for that specific target token immediately before submission.

Required live fields:

- `best_bid`
- `best_ask`
- `tick_size`
- timestamp

### Limit Order Philosophy

The model price is **not** the order price.

The model output defines whether the market is attractive.
The order book defines the actual passive limit price.

### Default Limit Price

For buy orders:

- default quote = `best_bid - 1 tick`

This is intentionally passive.

### Price Validity Constraints

The submitted price must satisfy all of the following:

1. positive and above minimum tick
2. not above the model-derived maximum acceptable price
3. not too stale relative to latest observed market state
4. still within configured risk checks

### Model-Derived Price Cap

The model-derived maximum acceptable buy price should be based on:

- predicted fair value
- expected fees
- a safety buffer

This cap is a **guardrail**, not the quote itself.

Use the more conservative of:

- passive price from order book
- model-derived price cap

### Order Type

- limit-only
- no market orders

### Time In Force

- 5 minutes maximum

Implementation may use explicit expiration or a cancel-after-TTL monitor.

### Exit Order Policy After Entry

Entry order handling stays unchanged during the initial 5-minute TTL window.

Rules:

- submit the buy limit order and allow it to work for up to 5 minutes
- do not cancel the remaining buy quantity early just because a partial fill occurred
- when the 5-minute TTL window ends, treat the actually filled quantity as the opened position
- cancel or let expire any still-unfilled buy quantity
- if filled quantity is greater than zero, immediately place a sell limit order for that filled quantity

Exit order rules:

- side = sell the same token that was filled
- quantity = only the actually filled share quantity from the entry order
- limit price = `0.99`
- submit immediately after the entry TTL window closes and opened quantity is known
- keep the exit order live until either it fills or the market resolves

If the sell limit order never fills, the remaining position is closed at market settlement using the final resolved payout.

The exit order is the default passive take-profit mechanism and should be persisted as part of the same order lifecycle.

## Order Lifecycle and Re-Entry Rules

### States

Minimum required states:

- `SCORED`
- `SUBMITTED`
- `PARTIALLY_FILLED`
- `FILLED`
- `EXPIRED`
- `CANCELED`
- `REJECTED`
- `OPENED_POSITION`

### State Semantics

#### If order is fully filled within 5 minutes

- mark market as opened
- stop future snapshot collection for that market
- stop future scoring for that market
- when the 5-minute entry TTL window closes, submit a sell limit order at `0.99` for the full filled share quantity

#### If order is partially filled within 5 minutes

- treat as opened position
- allow the remaining entry quantity to continue working until the 5-minute TTL window closes
- cancel or let expire the remaining unfilled quantity at TTL
- stop future snapshot collection for that market
- when the 5-minute entry TTL window closes, submit a sell limit order at `0.99` for the filled share quantity

#### If order receives zero fill and expires/cancels/rejects

- market is eligible to re-enter the next hourly universe
- no permanent lock

### Important Distinction

`submitted` is **not** the terminal exclusion point.

`opened position` is the terminal exclusion point.

## Position Ledger

Maintain a canonical opened-position ledger.

Suggested path:

- `execution_engine/data/shared/positions/open_positions.jsonl`

Required fields:

- `market_id`
- `token_id`
- `outcome_label`
- `entry_run_id`
- `entry_order_attempt_id`
- `entry_price`
- `filled_amount_usdc`
- `filled_shares`
- `opened_at_utc`
- `status`
- `exit_order_attempt_id`
- `exit_limit_price`
- `exit_submitted_at_utc`
- `exit_status`
- `exit_fill_price`
- `exit_filled_shares`
- `closed_at_utc`
- `close_reason`
- `realized_pnl_usdc`

Only markets present in this ledger are excluded from future universe refresh and snapshot collection.

## Artifact Layout

### Shared State

- `execution_engine/data/shared/universe/`
- `execution_engine/data/shared/ws_raw/`
- `execution_engine/data/shared/token_state/`
- `execution_engine/data/shared/positions/`
- `execution_engine/data/shared/orders_live/`
- `execution_engine/data/shared/labels/`

### Per-Run Output

- `execution_engine/data/runs/YYYY-MM-DD/<run_id>/`

Suggested per-run artifacts:

- `batch_manifest.json`
- `processed_markets.csv`
- `raw_snapshot_inputs.jsonl`
- `normalized_snapshots.parquet`
- `feature_inputs.parquet`
- `rule_hits.parquet`
- `model_outputs.parquet`
- `selection_decisions.parquet`
- `orders_submitted.jsonl`
- `fills.jsonl`
- `cancels.jsonl`
- `run_summary.json`

## Required Schemas

### `selection_decisions`

Must include at least:

- `run_id`
- `batch_id`
- `market_id`
- `selected_token_id`
- `selected_outcome_label`
- `selected_for_submission`
- `selection_reason`
- `stake_usdc`
- `growth_score`
- `f_exec`
- `q_pred`
- `trade_value_pred`

### `orders_submitted`

Must include at least:

- `run_id`
- `batch_id`
- `market_id`
- `token_id`
- `outcome_label`
- `order_attempt_id`
- `limit_price`
- `best_bid_at_submit`
- `best_ask_at_submit`
- `tick_size`
- `submitted_amount_usdc`
- `ttl_seconds`
- `submitted_at_utc`
- `order_status`

### `fills`

Must include at least:

- `run_id`
- `market_id`
- `token_id`
- `outcome_label`
- `order_attempt_id`
- `fill_id`
- `fill_price`
- `fill_amount_usdc`
- `fill_shares`
- `filled_at_utc`

## Realized PnL Semantics

Realized PnL must be computed using the actual close path of the filled position.

Rules:

- if the sell limit order fills, use the actual sell fill price to compute realized PnL
- if the sell limit order does not fill before market resolution, use the market settlement payout to compute realized PnL
- if the entry order receives zero fill, no position is opened and no realized PnL is recorded
- if the entry order is partially filled, only the filled portion becomes a position and is eligible for realized PnL
- the unfilled portion of the entry order is not a position and must not create separate position PnL

### Opportunity PnL for Unfilled Quantity

If you want to evaluate what the unfilled quantity would have earned at settlement, track it separately from realized PnL.

Rules:

- compute `opportunity_pnl` only for analysis, never for ledgered realized PnL
- the reference close for unfilled quantity may use market settlement payout
- keep `opportunity_pnl` in analysis outputs, not in the canonical position ledger

## Analysis Design

Daily analysis should produce two perspectives.

### 1. Executed Analysis

Focus on markets where orders were submitted.

Required outputs:

- submitted count
- filled count
- partial fill count
- fill rate
- cancel rate
- rejection rate
- average order lifetime
- average fill latency
- realized win rate after resolution
- performance by category
- performance by domain
- performance by horizon bucket
- performance by rule leaf

### 2. Opportunity Analysis

Focus on markets that were scored but not executed.

Required outputs:

- markets that matched rules but were not selected
- markets selected but not submitted
- markets submitted but not filled
- predicted edge vs realized label
- calibration by `q_pred`
- opportunity cost by rule leaf and horizon bucket

## Implementation Order

The recommended implementation order is:

1. Universe artifact and two-token filter
2. Shared market/token schema
3. WebSocket token ingestion and raw event persistence
4. Hourly market snapshot builder
5. Shared `rule_baseline` online preprocessing API
6. Rule hit persistence
7. Model output persistence
8. Per-market token selection
9. Passive order submission with `best_bid - 1 tick`
10. 5-minute TTL monitor and re-entry rules
11. Open-position ledger
12. Exit-order and settlement-close module
13. Daily label sync
14. Executed and opportunity analysis reports

## Code Organization Guidance

Keep the implementation simple and separate entry execution from exit and settlement handling.

Suggested package split:

- `execution_engine/online/execution/`: entry submission, entry TTL monitoring, and live order reconciliation
- `execution_engine/online/positions/`: opened-position ledger and position state transitions
- `execution_engine/online/exits/`: exit-order submission, exit-order monitoring, settlement close, and realized PnL

Minimum suggested files:

- `execution_engine/online/exits/submit_exit.py`
- `execution_engine/online/exits/monitor_exit.py`
- `execution_engine/online/exits/settlement.py`
- `execution_engine/online/exits/pnl.py`

This keeps buy-entry logic isolated from post-entry lifecycle logic and makes the code easier to test and maintain.

## Explicit Decisions Locked In

The following decisions are considered fixed for this design:

1. Universe is hard-coded to the next 24 hours to expiry.
2. Only two-token markets are eligible.
3. Markets are the scoring and trading unit.
4. Tokens are the streaming and execution unit.
5. Each market can open at most one side.
6. WebSocket subscriptions are multi-token, not one-token-per-connection.
7. Hourly processing is batched at 20 markets per batch.
8. Snapshot collection stops only after actual position opening, not after mere order submission.
9. Partial fill counts as an opened position.
10. Unfilled expired/canceled orders allow next-hour re-entry.
11. Default limit price is `best_bid - 1 tick`.
12. Intermediate artifacts must be fully persisted for replay and audit.

## Open Questions

The following are still implementation details, not strategic decisions:

1. exact reference-token selection rule during snapshot scoring
2. token-state retention window in memory
3. WebSocket reconnect and replay policy
4. whether per-batch execution should be strictly sequential or use bounded parallel scoring with serialized submission
5. exact file format choice for large artifacts: CSV vs Parquet

These do not change the core design above.
