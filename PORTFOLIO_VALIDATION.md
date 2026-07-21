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

The persistent first-pilot execution goal is earned separately with
`python3 portfolio_validation.py audit --goal first-pilot-live-started`. It
records the nonterminal `FIRST_PILOT_READY_LIVE_STARTED` milestone only when one
exact `PILOT_READY` strategy has a complete closed live execution, the account
is freshly `FLAT_RECONCILED` with no position or open order, every repository
and privacy audit passes, and Git is clean and pushed. The audit is idempotent
and does not alter the three-strategy terminal milestone.

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

The inspected result is
`research_results/2026-07-21-etf-vwap-mean-reversion-stage0-6b3cb4374d858c293176a622e3dc23300d1e09e9a7207a2f487bae700281a5bd.json`.
This exact variant is also retired: 29 signals lost 8.388R at 5 bps per side,
with -0.289R expectancy, 0.601 profit factor, and 13.470R drawdown; at 20 bps
per side it lost 19.686R. The result cannot be tuned or promoted.

The third activation is frozen, with returns still locked, at
`strategy_tournament/activations/equity-gap-continuation-v1-3bc6f70c2a331e70b00a1dda076b258ce46f1175d062ecee0a641e4c4ab06f3f.json`.
It binds the exact 100-date legacy catalyst denominator: 95 available bundles,
five permanent-fidelity exclusions, 950 ordered candidate sessions, and 950
verified prior-session closes. It adds no dates or symbols and permits no
substitution, provider request, broker action, development claim, confirmation
claim, or return calculation before its separate committed input inspection.
The executable contract fixes the common-stock and price gates, 2-8% gap,
first-15-minute range, completed-bar VWAP and volume breakout, next-bar entry,
first-15-minute stop, 2R target, stop-first ambiguity, 15:50 force-flat, one
daily selection, and 5/10/20 bps-per-side costs.
Input inspection
`strategy_tournament/inspections/equity-gap-continuation-v1-input-fe842aebc174196ceaa34a529d6c3a8eb75ab4df5bc6db110221f7f3dea4c881.json`
independently rebuilt the activation, reconciled 370,500 real one-minute bars
and every prior close, made zero provider or broker requests, computed zero
returns, and authorizes only the frozen Stage 0 calculation.

The independently rebuilt result is
`research_results/2026-07-21-equity-gap-continuation-stage0-1f447fb4e066b041463e62e26d7e752a12e1b90da7c13b6881ea42e10dda9e94.json`,
with result inspection
`strategy_tournament/inspections/equity-gap-continuation-v1-result-66ede02688e090973e482ecc9c491349238d14e2af63fd02c336b99a8564c7be.json`.
The exact variant survives Stage 0 with 37 closed signals, +0.088R expectancy,
1.417 profit factor, 3.498R maximum drawdown, +0.926R total at 20 bps per
side, and zero violations. This result has no direct maturity effect and may
only advance the unchanged rule into a newly frozen representative development
sample.

The survivor's production version is now frozen, with returns still locked, at
`strategy_validation/equity_gap_continuation/manifests/equity-gap-continuation-v1-0145f77948ff8d7398c69ec7ae2229b72cfb7a07bab055c3dcfb15762d5cea43.json`.
Strategy `equity-gap-continuation` version `1.0.0` retains the Stage 0 signal,
fill, stop, target, ambiguity, force-flat, and cost semantics while replacing
the falsification-only catalyst shortlist with the representative production
universe: complete point-in-time active US common stocks at 09:35 ET. The
manifest binds 200 disjoint inspected 2025 sessions chronologically: the first
120 sessions and 4,833 eligible gap symbol-sessions are development, the next
five sessions are an excluded embargo, and the final 75 sessions and 3,192
eligible gap symbol-sessions are untouched confirmation. All identities use
only information available by 09:35. The private exact graph is hash-bound in
the canonical store, contains no target returns, permits no substitution or
parameter change, and forbids confirmation collection until a committed,
independently inspected development result passes every unchanged gate.
Independent freeze inspection
`strategy_validation/equity_gap_continuation/inspections/equity-gap-continuation-v1-freeze-1aa8fad408438bf19824ae1c01ede6ea35c446238d75113666e337e0fdecec67.json`
exactly rebuilds the replacement public and private selection graph, confirms
zero overlap, zero computed returns, zero provider requests, and zero broker
actions, and authorizes development collection only. Its inspected predecessor
was superseded before evaluation after the first collection request revealed
that the provider treated the 16:00 endpoint as inclusive. No return was
computed; the replacement changes only the request boundary to
15:59:59.999999 ET and preserves every date, candidate, rule, cost, and outcome
definition.

## Active discovery funnel

The controller maintains three concurrent lanes whenever survivors exist:

1. one exact variant in cheap Stage 0 falsification;
2. one Stage 0 survivor acquiring a representative development sample; and
3. one advanced survivor in untouched confirmation or prospective shadow.

`python3 portfolio_funnel.py status` is the public queue and lane authority.
It independently rebuilds every published Stage 0 disposition from the frozen
slate, result, and result inspection. It reports retired and surviving counts,
each candidate's validation phase and blockers, and `PILOT_READY`/live-start
progress toward three. `portfolio_validation.py status` embeds the same report.

After the two permanently retired ETF variants, the remaining first wave is
fixed in this order: equity gap continuation, equity gap recovery,
volatility-compression breakout, short-horizon oversold reversal,
cross-sectional momentum, post-earnings drift, relative-strength continuation,
and catalyst ORB retest. A disposition must be an ordered prefix; a later
variant cannot skip an unresolved earlier one.

Stage 0 requires at least 30 closed signals, positive expectancy, profit factor
at least 1.10, drawdown no worse than 8R, positive total R at 20 bps per side,
and zero violations. A failed or insufficient exact version is retired
immediately and cannot enter `PORTFOLIO_SIGNALS.jsonl`. A survivor must bind its
inspected Stage 0 result in its independent maturity inspection before any
representative development record is accepted.

If the first wave yields fewer than three survivors, a committed and inspected
failure taxonomy must precede the only permitted second wave. Its six frozen
families, in order, are sector ETF rotation, broad-ETF trend pullback,
close-to-open ETF momentum, two-to-three-day cross-sectional reversal,
five-day 52-week-high continuation, and turn-of-month ETF seasonality.

Send a concise privacy-safe campaign email after every three Stage 0
dispositions and immediately for a new survivor, `PILOT_READY` award, live
start, safety pause, or terminal milestone. The funnel exposes whether the
disposition cadence is due; notification remains secondary to evidence and
position safety.

Resumable nonterminal statuses are `READY`, `WAITING_DATA`, `WAITING_MARKET`,
`WAITING_PROVIDER`, `WAITING_SUBSCRIPTION`, `WAITING_NEW_SESSIONS`,
`WAITING_USER_CONFIRMATION`, and `PAUSED_SAFETY`. The append-only ignored state
under `learning_runs/portfolio_validation/` binds every transition to the plan,
config, implementation and upstream hashes, current strategy identities,
phase, objective, blocker, next action, evidence hashes, and privacy-safe safety
state. `next` emits only one bounded handoff.

## Strategy promotion gates

A strategy version earns `PILOT_READY` only with all of the following:

- At least 30 representative development signals. Development must have
  positive expectancy, profit factor at least 1.30, a positive one-sided 90%
  bootstrap lower expectancy, drawdown no worse than 6R, positive chronological
  halves, positive total R without the five best trades, and passing 10/20-bps
  stress.
- At least 20 chronologically separated untouched confirmation signals, frozen
  before outcome access after a five-trading-session embargo. Confirmation must
  independently pass the same robustness, cost, capture, and violation gates
  without any parameter change.
- At least 50 eligible closed historical signals across those two samples, plus
  five complete prospective shadow executions through discovery, evaluation,
  sizing, order construction, protection, monitoring, and journaling.
- Positive combined expectancy, profit factor at least 1.30, a positive
  one-sided 90% bootstrap lower bound for mean R, maximum drawdown no worse than
  6R, positive chronological halves, positive total R without the five best
  outcomes, and passing 10/20-bps stress.
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

Immediately before any controlled pilot entry, `portfolio_guard.py` must return
`ENTRY_READY` for the exact strategy ID, version, and rules hash using a broker
snapshot no more than 15 seconds old. The pure guard requires machine-earned
`PILOT_READY`, reconciled and fully protected existing exposure, a passed order
review, any broker-required human confirmation, tradability, a ready protective
order route, live monitoring, and safe post-entry position, entry-count, holding,
planned-loss, notional, daily, weekly, and drawdown limits. It has no broker
action surface and cannot satisfy a required human confirmation itself.

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
