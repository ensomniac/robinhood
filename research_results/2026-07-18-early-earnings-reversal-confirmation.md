# Early Earnings Reversal Independent-Confirmation Contract

Prepared: 2026-07-18 ET

Research ID: `2026-07-18-early-earnings-reversal-confirmation-v1`

Status: frozen design for a future independent historical sample; not a
production proposal, not promotion evidence, and not authorized for live use.

## Why This Contract Exists

The production-aware lab inspected the complete current 100-date corpus and
tested 15 policies across four symmetric entry/exit cost levels and four target
levels. The early Item 2.02 earnings reversal was the strongest simple policy
that survived every declared retrospective robustness gate:

- 67 trades, +21.110R, 0.315 mean R, PF 1.683, and 4.528R maximum drawdown at
  5 bps per side and a 2R target.
- Positive development, retrospective validation, and retrospective holdout:
  +10.576R, +4.688R, and +5.845R.
- One-sided 90% bootstrap lower bound for mean R: +0.107R from 20,000 samples.
- At 20 bps per side and 2R: +9.813R, PF 1.282, and 5.899R maximum drawdown.
- Positive total R at 1R, 1.5R, 2R, and 3R targets under 5 bps-per-side costs.

These observations selected this hypothesis. They cannot confirm it. The first
independent run must not alter the rules below after any target-session outcome
is viewed.

## Frozen Signal And Selection Rules

1. Freeze the requested dates and the complete ordered candidate universe using
   the existing point-in-time discovery and evidence-hash contract before
   requesting target-session prices.
2. Keep missing dates and provider-fidelity failures as explicit blockers. Do
   not replace a date or symbol after observing target-session data.
3. A candidate must have a verified SEC Item 2.02 earnings filing available by
   the replay boundary and satisfy the existing immutable common-stock,
   dilution, ADV, ATR, complete-session, and non-interpolated-bar contracts.
4. The 9:30-9:35 ET opening range must be bearish: its final close is below its
   first open.
5. From completed bars beginning at 9:35, take the first bullish close that
   crosses from at-or-below to above both the opening-range midpoint and the
   running session VWAP.
6. The completed signal bar must begin before 9:40 ET. A 9:40 or later signal is
   ineligible.
7. If several candidates signal on the same earliest minute, select the greatest
   frozen plugin strength, then symbol. Do not use production score or opening
   RVOL as a tie-break; those variants failed the severe-cost research gate.
8. Select at most one trade per requested date. Enter no earlier than the next
   one-minute bar open.
9. Set the structural stop to 0.05% below the minimum of the signal-bar low,
   prior-bar low, and opening-range low. Do not compress it to manufacture
   allocation.
10. Resolve same-minute stop/target ambiguity stop-first, use a 2R target for
    the primary result, and force flat by 15:50 ET.

The executable implementation is the versioned `opening-reversal` 1.0.0 plugin
plus the `reversal-early-earnings` 1.0.0 lab policy. The run identity must retain
the evidence, bundle, plugin, policy, implementation, execution-grid, production
version, and production-rules hashes.

## Independent Sample And Acceptance Gate

- Request at least 100 newly frozen trading dates not inspected by run
  `strategy-lab-651f20ff135e-b268f18ec755` and retain all of them in the result.
- Require at least 80 validation-grade dates and at least 50 executed signals.
  Failure to reach either minimum is insufficient evidence, not permission to
  substitute or weaken the contract.
- At 5 bps per side and 2R, require positive expectancy, PF at least 1.30,
  maximum drawdown at most 6R, and a positive one-sided 90% bootstrap lower
  bound for mean R.
- Require positive total R in each chronological half and after removing the
  best five trades.
- At 10 and 20 bps per side and 2R, require positive total R, PF at least 1.20,
  and maximum drawdown at most 6R.
- At 5 bps per side, require positive total R at every predeclared 1R, 1.5R, 2R,
  and 3R target. The primary 2R result may not be replaced by the best target.
- Publish all covered dates, blockers, policies, costs, targets, and exits. A
  failed gate rejects or redesigns the hypothesis; it does not trigger threshold
  tuning on the confirmation sample.

## Deployment Blocker And Required Fidelity

This contract is intentionally not production-compatible yet. In the inspected
sample, the median structural stop was 2.088%, only four of 67 trades fit the
current 0.8% production cap, and the UNVALIDATED 0.25% risk budget plus a 0.10%
reserve implied only 11.4% median notional. A 70-80% allocation target is
therefore not attainable without violating the loss cap.

Independent expectancy is necessary but not sufficient. Before any production
proposal, the setup must also demonstrate:

- risk-sized whole-share orders that accept the binding lower notional rather
  than tightening a real invalidation stop;
- fresh subminute NBBO and executable-depth evidence around the reclaim and
  next-order opportunity, because one-minute next-open fills are not live fill
  proofs;
- a predeclared marketable-limit chase cap and monitored stop-protection path;
- acceptable entry, exit, and stop slippage plus prompt protection in shadow or
  tightly bounded live-pilot evidence;
- the normal strategy-learning cadence and explicit user approval for any new
  production version.

Until those gates pass, `strategy_config.toml`, `AGENTS.md`, production maturity,
and the production ORB remain frozen.
