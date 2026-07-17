# Historical Multi-Strategy Research Result

Run ID: `research-651f20ff135e-865889893aa8`

## Scope

- Requested dates: 100
- Available immutable bundles: 95
- Missing bundles retained as blockers: 5
- Dataset hash: `651f20ff135eecf850292066b5c75652cddb85914a2b30e707f12c3b84df9d3d`
- Worker processes: 4
- Wall time: 2.003 seconds
- Provider requests: 0
- Production ledger, archive, and maturity writes: 0

## Strategy Results

| Strategy | Covered | Trades | Win rate | Mean R | Total R | Profit factor | Max DD R |
|---|---:|---:|---:|---:|---:|---:|---:|
| orb-5m-research@1.0.0 | 95 | 59 | 40.7% | 0.070 | 4.119 | 1.113 | 5.512 |
| vwap-pullback@1.0.0 | 95 | 94 | 30.9% | -0.157 | -14.790 | 0.783 | 21.942 |
| hod-continuation@1.0.0 | 95 | 87 | 43.7% | 0.026 | 2.228 | 1.047 | 9.795 |
| opening-reversal@1.0.0 | 95 | 91 | 45.1% | 0.164 | 14.879 | 1.320 | 5.959 |

## Paired Comparison

Baseline: `orb-5m-research`. No-trade days count as 0R; only dates covered by both strategies enter each pair.

| Strategy | Common days | Mean delta R/day | Cumulative delta R | Better | Worse | Tied |
|---|---:|---:|---:|---:|---:|---:|
| hod-continuation | 95 | -0.0199 | -1.891 | 50 | 38 | 7 |
| opening-reversal | 95 | 0.1133 | 10.761 | 55 | 39 | 1 |
| vwap-pullback | 95 | -0.1990 | -18.909 | 28 | 67 | 0 |

## Interpretation Boundary

These are research-only counterfactuals over the already frozen, catalyst-selected candidate universe. They do not validate a strategy over the full market. The current bundles support complete one-minute bar strategies, but not arbitrary-time NBBO, full depth, tick-order, or independent benchmark-bar rules. Results do not update `SIGNALS.jsonl`, the public trade archive, or maturity.
