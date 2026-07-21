# Multi-Strategy Portfolio Validation Campaign

Campaign ID: `multi-strategy-portfolio-validation-v1`

Authorized: 2026-07-21

## Objective and completion contract

Discover at least three independently evidenced long-equity or ETF strategy
versions, start a controlled live pilot for each version as soon as it earns
`PILOT_READY`, and assemble a portfolio whose expected geometric growth remains
positive after realistic execution costs and risk. The campaign is complete only
when `python3 portfolio_maturity.py report` machine-earns
`THREE_PILOT_READY_LIVE_STARTED` and `python3 portfolio_validation.py audit`
passes every repository, privacy, capacity, Git, and broker-safety integrity gate.

`PILOT_READY` means a frozen strategy has passed historical development,
untouched confirmation, robustness, cost-stress, evidence, and prospective
shadow gates. It authorizes a tightly risk-capped live pilot; it is not a claim
of live profitability. `LIVE_VALIDATED` is awarded separately only after the
strategy earns the configured live execution, organic-stop, slippage,
protection, and violation evidence.

The campaign has no elapsed-time shortcut. Market closure, unavailable data,
provider access, a subscription decision, broker confirmation, or insufficient
new sessions is a resumable waiting state, never successful completion.

## Supersession and preserved evidence

The prior single-strategy campaign in `PRODUCTION_STRATEGY_VALIDATION.md` is
`SUPERSEDED_PAUSED` as the sole route to production. It is neither declared
successful nor failed, and its ignored event state, frozen evidence, public
ledger, catalyst semantics work, ORB v3 production rules, and challenger work
remain unmodified and auditable. The existing catalyst ORB lane remains one
candidate lane in this campaign. Its evidence may be reused only where the new
strategy's frozen contract proves semantic compatibility; evidence never gains
a new meaning merely because the campaign changed.

The old `strategy_config.toml` remains the numeric contract for frozen ORB v3.
`portfolio_config.toml` is the numeric source of truth for portfolio-level
research, pilot maturity, diversification, and risk limits. Each candidate has
its own version and rules hash. A material candidate-rule change creates a new
version and cannot inherit the changed strategy's maturity sample.

## Authorized portfolio envelope

- Long common equities and long ETFs only; no options, shorts, futures, crypto,
  or leveraged borrowing outside the broker's currently reviewed permissions.
- At most three concurrent positions, five new entries per trading day, and a
  five-trading-day holding period.
- Initial live pilots risk at most 0.50% of equity in planned loss per position,
  1.25% across open positions, 1.50% realized/planned daily loss, 4.00% weekly
  loss, and 8.00% peak-to-trough drawdown, with gross notional at or below 100%.
- Scaling is disabled until at least 20 clean portfolio live closes. Thereafter
  the hard ceiling is 1.00% planned loss per position, 2.00% aggregate planned
  open loss, 125% gross notional, and quarter-Kelly sizing subject to every
  lower risk, liquidity, buying-power, and broker constraint.
- Every held position must have accepted protection appropriate to its session
  and gap risk. Unknown orders, missing protection, unreconciled exposure,
  monitoring failure, or a hard risk breach immediately pauses new entries and
  prioritizes reconciliation and safe flattening.
- Broker/tool-required order review or human confirmation remains mandatory.
  The controller never contacts a provider or broker and never places an order.

## Initial strategy tournament

Stage 0 evaluates at most ten distinct mechanism families and twenty total
frozen variants. The initial slate is:

1. ETF opening-range momentum.
2. ETF VWAP mean reversion.
3. Equity gap continuation.
4. Equity gap recovery or mean reversion.
5. Cross-sectional relative-strength continuation.
6. Volatility-compression breakout.
7. Multi-session cross-sectional momentum.
8. Short-horizon oversold reversal.
9. Post-earnings announcement drift.
10. Preserved catalyst ORB retest.

Every variant freezes its universe, dates, feature timestamps, ranking,
selection rule, entry, exit, stops, maximum hold, execution model, parameter
family, cost grid, missing-data policy, falsification criteria, and rules hash
before outcome access. Stage 0 uses existing cache and evidence-bound bundles to
remove obviously weak or operationally infeasible mechanisms quickly. Such
specialized legacy data may screen a mechanism but cannot validate a broad
production universe.

At most one second wave of six new mechanism families is permitted. It opens
only after a written first-wave failure taxonomy identifies a genuinely missing
economic mechanism. A failed untouched confirmation permanently retires that
exact strategy version. Trial counts and rejected versions remain in the public
audit; selection never erases failures.

## Evidence phases

The persistent controller uses these phases:

1. `SUPERSESSION`: bind the new authority and preserved old-campaign artifacts.
2. `DATA_INVENTORY`: map existing bundles, provider/cache coverage, primary
   sources, storage reserve, timestamps, corporate actions, and outcome locks.
3. `TOURNAMENT`: preregister and run cheap, disjoint Stage 0 falsification tests.
4. `DEVELOPMENT`: collect representative frozen samples and build executable
   outcomes for surviving versions.
5. `CONFIRMATION`: evaluate chronologically separated untouched dates after an
   embargo at least as long as the five-day maximum hold.
6. `SHADOW_QUALIFICATION`: run the full prospective path without orders.
7. `LIVE_PILOT`: begin each strategy's controlled live pilot immediately after
   that strategy, independently, earns `PILOT_READY`.
8. `PORTFOLIO_AUDIT`: recompute strategy, diversification, operations, privacy,
   capacity, and publication gates.
9. `THREE_PILOT_READY_LIVE_STARTED`: terminal machine-earned milestone.

The initial bounded data inventory is published at
`research_results/2026-07-21-portfolio-data-inventory.json` and rebuilt with
`python3 portfolio_data_inventory.py inspect`. It identifies the legacy 95-date
candidate corpus as falsification-only, preserves the failed reversal
confirmation as previously inspected evidence, and routes the two ETF families
to a newly frozen disjoint development manifest. It authorizes no outcome
relabeling, provider request, broker action, or validation claim.

The outcome-locked first-wave slate is published at
`strategy_tournament/manifests/portfolio-stage0-slate-1c4f1cd20c490ea56e36fc0b81c30f1c1eb588d9a9dfb310501028de95b3c50f.json`
and inspected with:

```sh
python3 portfolio_tournament.py inspect \
  strategy_tournament/manifests/portfolio-stage0-slate-1c4f1cd20c490ea56e36fc0b81c30f1c1eb588d9a9dfb310501028de95b3c50f.json
```

It freezes one variant in each of the ten authorized mechanism families, counts
the 15 prior policy trials, and leaves ten first-wave variant slots unused. The
slate itself authorizes no outcome, provider, or broker access. Each variant
must still publish a separately inspected activation manifest binding exact
dates, symbols, input hashes, denominators, evaluator implementation, and the
execution/cost contract before its first Stage 0 outcome is evaluated.

The first activation was frozen at
`strategy_tournament/activations/etf-or-momentum-v1-fbea206058e0530a38f89b4b19ccdb71949329960fc6ebbef63ae56966961b03.json`.
It binds 164 exact common SPY/QQQ dates (328 symbol sessions and 127,920 expected
one-minute bars) to one complete IBKR feed and records 81 fidelity exclusions
without substitution. All dates were previously referenced by research, so the
activation is permanently falsification-only. Its input inspection is published at
`strategy_tournament/inspections/etf-or-momentum-v1-input-6736b37133fd8ba9d46c86e76ef3d7e21a2a09541f1ad631d4ec8b94ff78b1c8.json`:
it reconciles all 127,920 bars, authorizes only the frozen Stage 0 return
calculation, and computed no return itself.

The inspected result is
`research_results/2026-07-21-etf-or-momentum-stage0-fe9853712ae52f92b4a88403bdb9270f3f3a8add382a393a2b183529d9cc05d2.json`.
The exact variant is retired: 97 signals lost 34.213R at 5 bps per side, with
-0.353R expectancy, 0.465 profit factor, and 34.213R drawdown; at 20 bps per
side it lost 60.325R. It cannot be tuned on those dates or relabeled as maturity
evidence. The tournament advances to the next frozen mechanism.

The second activation is frozen, with returns still locked, at
`strategy_tournament/activations/etf-vwap-mean-reversion-v1-ecaff3785f2dc9fe3f14332be9e40e056e08aacf55f86487cb2f25df62688a47.json`.
It reuses the already-accounted 164-date ETF falsification corpus and binds a
simple five-change RSI washout, later bullish reversal, next-bar entry,
trigger-low stop, decision-time VWAP target, daily selection, force-flat, and
5/10/20 bps-per-side costs. It must pass its own committed input inspection
before the fixed return calculation can run.
Input inspection `deae004f10d53c1753435155385fc0557b6c39249f43896725bf1237dd32eaa0`
now reconciles the full graph with zero returns computed.

Resumable nonterminal statuses are `READY`, `WAITING_DATA`, `WAITING_MARKET`,
`WAITING_PROVIDER`, `WAITING_SUBSCRIPTION`, `WAITING_NEW_SESSIONS`,
`WAITING_USER_CONFIRMATION`, and `PAUSED_SAFETY`. The append-only ignored state
under `learning_runs/portfolio_validation/` binds every transition to the plan,
config, implementation and upstream hashes, current strategy identities,
phase, objective, blocker, next action, evidence hashes, and privacy-safe safety
state. `next` emits only one bounded handoff.

## Strategy promotion gates

A strategy version earns `PILOT_READY` only with all of the following:

- At least 50 eligible closed historical signals, including 20 untouched
  confirmation signals, plus five complete prospective shadow executions.
- Positive overall and confirmation expectancy, profit factor at least 1.30, a
  positive one-sided 90% bootstrap lower bound for mean R, maximum drawdown no
  worse than 6R, positive chronological halves, and positive total R after
  removing its five best outcomes.
- Positive total R, profit factor at least 1.20, and maximum drawdown no worse
  than 6R under both 10 and 20 bps per-side adverse cost stress.
- Complete session/candidate denominators, trial accounting, multiple-testing
  audit, executable fill/protection/exit modeling, and zero evidence, capture,
  or rule violations.

The portfolio milestone additionally requires three distinct mechanism
families, pairwise confirmation daily-return correlation below 0.70, combined
confirmation opportunity coverage of at least 60% on shared test dates, and at
least one controlled live execution started for every selected strategy.

`LIVE_VALIDATED` requires at least ten qualifying live executions and three
naturally occurring stop executions for the strategy, entry-slippage p95 at or
below 15 bps, unprotected-exposure p95 at or below ten seconds, stop slippage
inside its frozen reserve, and zero rule violations. A stop is never
manufactured and a marginal setup is never taken to satisfy a count.

## Outcome, data, and anti-lookahead rules

- Use point-in-time membership, split/corporate-action adjustment, timestamped
  data, provider fidelity, and explicit no-substitution manifests. Use local
  cache first, then approved providers without silently mixing a provider's
  missing fields with favorable replacements.
- Preserve at least the configured historical-store disk reserve. Capacity
  failure pauses collection and never deletes prior evidence automatically.
- Derive fills from executable bid/ask, tape ordering, marketable limits, gap
  behavior, and the frozen cost model. Same-interval stop/target ambiguity is
  resolved stop-first. Preserve misses, no-trades, partial availability,
  rejections, and delisted/failed symbols in denominators.
- Development and confirmation samples are disjoint. Confirmation dates are
  frozen before outcomes, chronologically separated, and embargoed by at least
  the maximum five-session holding period.
- Test portfolio overlap, capital contention, correlated drawdown, simultaneous
  gaps, liquidity, and position/risk budgets on combined event timelines—not by
  adding isolated backtest summaries.

## Commit, publication, and notification discipline

Commit and push each coherent controller, contract, frozen manifest, inspected
result, strategy disposition, production-version change, and live lifecycle
slice after proportionate tests and audits. Before every commit, review the
diff, run the relevant suite, confirm no secret or plaintext broker identifier,
and add durable progress when material. Final completion requires a clean
worktree and `HEAD` equal to its configured upstream.

Important phase changes, newly earned `PILOT_READY`, live starts, safety pauses,
irreducible user actions, and terminal completion may trigger privacy-safe email
updates under `settings.toml`. Email never delays position safety or substitutes
for broker/ledger truth.
