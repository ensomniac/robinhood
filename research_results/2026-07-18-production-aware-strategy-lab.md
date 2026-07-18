# Production-Aware Historical Strategy Lab

Run ID: `strategy-lab-651f20ff135e-b268f18ec755`

## Scope And Boundary

- Requested dates: 100
- Available immutable bundles: 95
- Missing bundles retained as blockers: 5
- Dataset hash: `651f20ff135eecf850292066b5c75652cddb85914a2b30e707f12c3b84df9d3d`
- Provider requests: 0
- Production strategy/configuration changes: 0
- Independent confirmation: no; every split is a retrospective stability check.
- Full tested family: all policy, cost, and target cells are retained in the JSON result.

## Declared Adversarial Gate

A policy is only `promising_for_independent_confirmation` when all of the following hold:

- Base 5 bps-per-side, 2R result: positive mean R, PF at least 1.20, maximum drawdown at most 6R, and one-sided 90% bootstrap lower mean R above zero.
- Positive total R in both retrospective chronological validation and holdout phases.
- At both 10 and 20 bps per side with a 2R target: positive total R, PF at least 1.20, and maximum drawdown at most 6R.
- Positive total R at 1R, 1.5R, 2R, and 3R targets under 5 bps-per-side costs.
- Passing is a freeze-and-confirm signal, never promotion evidence.

## Base Policy Results

| Policy | Trades | Mean R | Total R | PF | Max DD R | 90% lower mean R | Research status | Deployment |
|---|---:|---:|---:|---:|---:|---:|---|---|
| orb-production-stack | 0 | n/a | 0.000 | n/a | 0.000 | n/a | no_trade_evidence | no_trade_evidence |
| orb-all-strength | 59 | 0.070 | 4.119 | 1.113 | 5.512 | -0.162 | inconclusive | production_incompatible_stop_geometry |
| orb-early-strength | 36 | 0.234 | 8.415 | 1.410 | 4.247 | -0.075 | inconclusive | production_incompatible_stop_geometry |
| orb-early-score | 36 | 0.234 | 8.415 | 1.410 | 4.247 | -0.073 | inconclusive | production_incompatible_stop_geometry |
| orb-early-rvol | 36 | 0.234 | 8.415 | 1.410 | 4.247 | -0.076 | inconclusive | production_incompatible_stop_geometry |
| orb-early-earnings | 30 | 0.292 | 8.764 | 1.538 | 4.247 | -0.041 | inconclusive | production_incompatible_stop_geometry |
| orb-early-stop-0.8pct | 14 | -0.183 | -2.558 | 0.747 | 5.991 | -0.616 | reject_or_redesign | production_stop_geometry_enforced |
| reversal-all-strength | 91 | 0.164 | 14.879 | 1.320 | 5.959 | -0.014 | inconclusive | production_incompatible_stop_geometry |
| reversal-early-strength | 71 | 0.281 | 19.973 | 1.588 | 4.906 | 0.083 | promising_for_independent_confirmation | production_incompatible_stop_geometry |
| reversal-early-score | 71 | 0.307 | 21.763 | 1.625 | 4.906 | 0.102 | inconclusive | production_incompatible_stop_geometry |
| reversal-early-rvol | 71 | 0.214 | 15.225 | 1.422 | 4.906 | 0.012 | inconclusive | production_incompatible_stop_geometry |
| reversal-early-earnings | 67 | 0.315 | 21.110 | 1.683 | 4.528 | 0.107 | promising_for_independent_confirmation | production_incompatible_stop_geometry |
| reversal-early-stop-0.8pct | 6 | 0.252 | 1.511 | 1.466 | 1.087 | -0.577 | inconclusive | production_stop_geometry_enforced |
| vwap-pullback-all | 94 | -0.157 | -14.790 | 0.783 | 21.942 | -0.339 | reject_or_redesign | stop_geometry_needs_live_execution_confirmation |
| hod-continuation-all | 87 | 0.026 | 2.228 | 1.047 | 9.795 | -0.142 | inconclusive | production_incompatible_stop_geometry |

## Survivors Under Cost Stress

| Policy | 10 bps Total R | 10 bps PF | 20 bps Total R | 20 bps PF | 20 bps DD R | Median stop | <=0.8% stops | Implied median notional |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| reversal-early-strength | 16.928 | 1.483 | 8.327 | 1.219 | 5.899 | 2.1% | 5.6% | 11.6% |
| reversal-early-earnings | 18.190 | 1.570 | 9.813 | 1.282 | 5.899 | 2.1% | 6.0% | 11.4% |

The stop and implied-notional columns are the decisive deployment warning. A policy can have positive R expectancy while remaining incompatible with the current 0.8% production stop cap and 70-80% allocation objective.

## Production Gate And Timing Diagnostics

- Production candidate evaluations: 950
- Production-eligible candidates: 0
- Executable ORB counterfactual signals: 90
- Chase-rejected executable ORBs: 67 (74.4%)
- Median production evaluation delay from signal-bar start: 120.0 seconds
- Median production evaluation delay from research entry-bar start: 60.0 seconds

The delay uses one-minute bar-start timestamps and is not observed live latency. It demonstrates why the stored production adapter and the next-open research model are not interchangeable.

Gate attribution is descriptive, not causal. In particular, the fixed-slippage minute-bar model cannot disprove live spread, freshness, depth, chase, or protection controls. It can identify gates whose historical labels deserve a cleaner prospective test.

## Theory Decisions

1. **Freeze the simple early earnings reversal for independent confirmation.** It produced 67 trades, 21.110R, PF 1.683, and 4.528R maximum drawdown; development, retrospective validation, and retrospective holdout were all positive (10.576R, 4.688R, 5.845R). At 20 bps per side it retained 9.813R and PF 1.282.
2. **Keep the current production ORB frozen.** The complete production gate stack selected zero trades. Relaxed ORB variants did trade, but none cleared the statistical and severe-cost gate.
3. **Do not use production score or RVOL ranking as reversal selectors.** They were tested on the same corpus, add complexity, and failed the 20 bps PF/drawdown gate; simple signal strength is the safer comparator.
4. **Reject the 0.8% stop as a drop-in fit for these stored signals.** The tight-stop early ORB lost money, and only 4 of 67 preferred reversal trades fit the current cap. The preferred cohort's median structural stop was 2.1%, implying only 11.4% median notional at the current 0.25% risk budget plus reserve.
5. **Retire the current VWAP-pullback branch and deprioritize HOD continuation.** The former had negative expectancy; the latter was near flat with drawdown above the production maturity budget. More tuning on this inspected corpus would spend learning capacity on overfit risk.

## Selection Artifact Warning

The early ORB trade count fell from 47 at 0 bps to 36, 19, and 8 at 5, 10, and 20 bps. Modest added slippage can therefore appear to improve ORB returns by pushing entries past the chase cap and deleting trades. This is a selection artifact, not evidence that worse execution helps.

Promising for independent confirmation:

- `reversal-early-strength`
- `reversal-early-earnings`

Reject or redesign:

- `orb-early-stop-0.8pct`
- `vwap-pullback-all`

## Required Next Gate

Freeze new dates and symbols before viewing their target-session outcomes. Re-run only the exact simple early-earnings-reversal confirmation contract, retain every blocked date, collect full-universe and subminute quote/execution evidence where available, and do not edit production rules until both independent expectancy and deployable stop/protection geometry pass.
