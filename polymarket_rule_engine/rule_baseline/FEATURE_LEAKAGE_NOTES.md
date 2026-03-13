# Feature Leakage Notes

This document records the feature leakage investigation for `rule_baseline`, the confirmed issues, and the current handling rules.

## 1. Confirmed leakage source

The main confirmed leakage source was not the snapshot `price` field.

Snapshot `price` is built from `prices-history` and aligned to:
- `closedTime - 1h`
- `closedTime - 2h`
- `closedTime - 4h`
- `closedTime - 6h`
- `closedTime - 12h`
- `closedTime - 24h`

It is generated in:
- `rule_baseline/data_collection/build_snapshots.py`

The confirmed leakage came from market-level state fields merged from resolved raw markets through:
- `rule_baseline/features/tabular.py`
- `rule_baseline/features/market_feature_builders.py`

These fields were taken from the final merged raw market table, so in offline training they represent terminal market state rather than snapshot-time observable state.

## 2. Experiments and conclusions

### Experiment A: market-level dedup

Hypothesis:
- Multiple `horizon` rows from the same `market_id` might be causing overly optimistic metrics.

Result:
- Deduping to one row per market changed metrics very little.

Conclusion:
- Multi-horizon duplication is not the main root cause.

### Experiment B: remove rule-identity features

Tested features:
- `q_smooth`
- `rule_score`
- `group_key`
- `leaf_id`
- `direction`

Result:
- Metrics stayed near the prior level.

Conclusion:
- Rule features may amplify performance, but they were not the main leakage source.

### Experiment C: remove resolved market state features

Removing terminal market state features caused model quality to fall sharply from near-perfect to much more realistic levels.

Conclusion:
- The main leakage came from resolved-market terminal state fields, especially market price/microstructure style fields.

## 3. Features already removed from offline model input

The following classes of features are currently excluded in:
- `rule_baseline/training/train_snapshot_model.py`

### 3.1 Direct outcome leakage

- `winning_outcome_index`
- `winning_outcome_label`
- `primary_token_id`
- `secondary_token_id`
- `primary_outcome`
- `secondary_outcome`
- `selected_quote_side`
- `trade_value_true`
- `expected_pnl_target`
- `expected_roi_target`
- `residual_q_target`

### 3.2 Terminal market price / microstructure fields

- `bestBid`
- `bestAsk`
- `spread`
- `lastTradePrice`
- `best_bid`
- `best_ask`
- `mid_price`
- `quoted_spread`
- `quoted_spread_pct`
- `book_imbalance`

### 3.3 Duplicate features removed from model input

- `price_change_1h`
- `price_change_1d`
- `price_change_1w`
- `line_value`
- `volume24hrClob`
- `volume1wkClob`
- `domain_parsed`
- `domain_parsed_market`
- `source_host_market`
- `category_raw_market`
- `category_parsed_market`
- `category_override_flag_market`
- `is_date_based`
- `vol_x_sentiment`
- `cat_entertainment_str`

### 3.4 Time / identity-heavy fields removed from model input

- `startDate_market`
- `endDate_market`
- `closedTime_market`
- `description_market`
- `question_market`

These were removed because they are either explicit time identifiers or very high-cardinality market identity text fields.

### 3.5 Constant or no-information fields removed from model input

- `duration_is_negative_flag`
- `duration_below_min_horizon_flag`
- `price_in_range_flag`
- `liquidity`
- `negRisk`
- `liquidityAmm`
- `liquidityClob`
- `marketMakerAddress_market`
- `log_liq`
- `liq_ratio`
- `log_liquidity_clob`
- `log_liquidity_amm`
- `clob_share_liquidity`
- `has_percent`
- `has_million`
- `has_before`
- `has_after`
- `is_binary`
- `cap_ratio`
- `strong_pos`
- `cat_finance`
- `dur_very_long`

## 4. Features still under suspicion

These features are still in the offline model and remain suspicious because they are still sourced from merged raw market state:

- `oneHourPriceChange`
- `oneDayPriceChange`
- `oneWeekPriceChange`
- `volume`
- `volume24hr`
- `volume1wk`

Important:
- These features are not automatically invalid in principle.
- They may be valid online if captured at decision time.
- They remain suspicious offline because the current offline source is terminal merged market data, not a guaranteed snapshot-time replay.

## 5. Current working rule

For offline training and offline evaluation:
- Keep only features that are clearly available at `snapshot_time`, or features whose leakage risk is currently acceptable and still under explicit review.

For online/live training:
- Features such as bid/ask, spread, last trade, and price changes can be used only if they are captured at real decision time and stored with correct timestamps.

## 6. Recommended next steps

The next leakage checks should focus on isolated ablation tests for:
- `oneHourPriceChange`
- `oneDayPriceChange`
- `oneWeekPriceChange`
- `volume`
- `volume24hr`
- `volume1wk`

These should be tested in separate groups so the remaining optimism can be localized precisely.
