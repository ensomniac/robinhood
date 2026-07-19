# SIP Bar Aggregation Fidelity

Status: independently inspected development evidence, 2026-07-19  
Dataset: `dataset-sip-bar-aggregation-validation-2026-07-19-v1`  
Production strategy changed: no

## Why this slice exists

The unchanged ORB champion requires price above flat-to-rising session VWAP at
the actual trigger observation. A completed one-minute bar is insufficient when
the clean break occurs inside the minute. More importantly, multiplying a
published bar's WAP by its published volume does not reconstruct the provider's
WAP numerator: Alpaca's official aggregation rules include some trades in
reported volume while excluding them from high/low, and WAP includes only trades
that update both high/low and volume.

The prior readiness report therefore treated intraminute VWAP as unresolved. It
did not silently substitute the common but incorrect `sum(bar.wap * bar.volume)
/ sum(bar.volume)` approximation.

## Frozen rule contract

`sip_bar_aggregation.py` transcribes the complete minute-bar update matrix for
CTA tapes A/B and UTP tape C from Alpaca's official market-data FAQ:

<https://docs.alpaca.markets/us/docs/market-data-faq#how-are-bars-aggregated>

For every raw SIP trade, the rule produces separate open/close, high/low, and
volume eligibility states. When a trade has multiple conditions, the strictest
condition wins, matching the provider's documented rule. Unknown tapes,
missing conditions, and unknown or tape-inapplicable condition codes fail
closed and are counted rather than guessed.

The frozen manifest binds:

- the selected-candidate clean-trigger source and its SHA-256;
- all 325 crossing minutes selected before this aggregation test;
- `sip_bar_aggregation.py` and `sip_bar_validation.py` by SHA-256;
- exact OHLCV and trade-count matching;
- a one-microdollar absolute tolerance for floating-point WAP comparison;
- an explicit ban on target outcomes and strategy variants.

Manifest:
`historical_batches/sip_bar_validation/manifests/dataset-sip-bar-aggregation-validation-2026-07-19-v1-7456a6d3a6f817b99d5fc4fe7e06b2832203e4ce34629b7499d1fd744f753019.json`

## Source-oracle result

The frozen implementation rebuilt each corresponding Alpaca raw SIP one-minute
bar from the locally cached raw trades. An independent inspection repeated the
join and compared the private and public hashes and aggregates.

| Measure | Result |
|---|---:|
| Crossing minutes | 325 |
| Raw trades | 365,379 |
| Exact open matches | 325 |
| Exact high matches | 325 |
| Exact low matches | 325 |
| Exact close matches | 325 |
| Exact reported-volume matches | 325 |
| Exact eligible-trade-count matches | 325 |
| WAP matches within $0.000001 | 325 |
| Unsupported condition observations | 0 |
| Minutes where WAP denominator differs from reported volume | 325 |

The ratio of WAP-eligible volume to published volume ranged from approximately
0.4620 to 0.9862, with a median of approximately 0.8207. This is operationally
material: every tested trigger minute would use the wrong denominator if exact
VWAP were reconstructed from published WAP and volume alone.

## Correct replay use

For a trigger observed at time `t`:

1. Use complete prior-minute raw bars through the minute before `t` only when a
   condition-aware WAP numerator and denominator were preserved.
2. Load raw SIP trades from the start of the session through `t`, not through
   the end of the crossing minute.
3. Order trades by exchange/source timestamp and stable trade ID.
4. Apply `classify_minute_update` to every trade.
5. Add size to reported volume when volume is eligible.
6. Add `price * size` and size to the VWAP numerator and denominator only when
   both high/low and volume are eligible.
7. Fail the record closed if any trade condition is unsupported or if the raw
   prefix is incomplete.

`aggregate_prefix_vwap` implements the condition-aware prefix calculation. It
must not be used to infer that raw trades are complete; callers remain
responsible for source-coverage and information-cutoff checks.

## Claim boundary

This closes an input-semantics gap, not an alpha gap. The validation corpus is
the already-inspected 20-date scanner corpus, so it cannot be used for champion
returns, confirmation, parameter selection, or promotion. It earns permission
to compute exact historical intraminute VWAP on a future frozen corpus. It does
not earn a production rule change.

Still unresolved for faithful unchanged-v3 evaluation:

- a pre-registered, executable definition of structural invalidation and normal
  noise;
- a pre-registered point-in-time resistance definition;
- broader direct positive catalyst coverage;
- historical broker-specific tradability where obtainable;
- at least 100 previously uninspected dynamic scanner dates;
- outcome evaluation only after all contracts and dates are frozen.

## Reproduction

```sh
python3 sip_bar_validation.py validate \
  --manifest historical_batches/sip_bar_validation/manifests/dataset-sip-bar-aggregation-validation-2026-07-19-v1-7456a6d3a6f817b99d5fc4fe7e06b2832203e4ce34629b7499d1fd744f753019.json

python3 sip_bar_validation.py inspect \
  --manifest historical_batches/sip_bar_validation/manifests/dataset-sip-bar-aggregation-validation-2026-07-19-v1-7456a6d3a6f817b99d5fc4fe7e06b2832203e4ce34629b7499d1fd744f753019.json

python3 sip_bar_validation_inspection.py \
  --manifest historical_batches/sip_bar_validation/manifests/dataset-sip-bar-aggregation-validation-2026-07-19-v1-7456a6d3a6f817b99d5fc4fe7e06b2832203e4ce34629b7499d1fd744f753019.json
```

Raw rows and symbols remain only beneath `LOCAL_HISTORICAL_DATA_ROOT`. Public
artifacts contain aggregate evidence and hashes.
