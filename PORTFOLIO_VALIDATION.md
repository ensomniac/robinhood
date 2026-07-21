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

Development collection is `READY` in
`strategy_validation/equity_gap_continuation/collection-status.json`: all 4,833
frozen symbol-sessions returned 1,860,990 raw Alpaca SIP minute bars through 249
provider requests with zero retries and zero unresolved inputs. The canonical
data remains outside the repository. Collection computed zero target returns,
made zero broker actions, and did not access the locked confirmation phase.
Input inspection
`strategy_validation/equity_gap_continuation/inspections/equity-gap-continuation-v1-development-inputs-36ef7b4db655573630d0e407e35d3a1e9ed43a44c91da6bf9646a5a991066761.json`
reconciles all 1,860,990 bars and 4,833 candidates without computing a return.
It identifies 3,148 exact contiguous 390-minute symbol-sessions; the 1,685
provider-sparse sessions remain in the complete denominator as frozen
no-signals. The inspection authorizes the one unchanged development evaluation
after it is committed and pushed.

The inspected representative development result is
`research_results/2026-07-21-equity-gap-continuation-development-3add7a7bdbd3a47282663a0daa41e17d1b6d3e6e780f08d959cf81ca4f2dba92.json`,
with independent rebuild
`strategy_validation/equity_gap_continuation/inspections/equity-gap-continuation-v1-development-result-bf1050d3d88b2402890befd5c583f2b623b14fa1b33f2edc9f89692e130db8c6.json`.
This exact production version fails development: 111 closed signals produced
-0.069R expectancy, 0.807 profit factor, a negative 90% bootstrap lower bound,
15.946R maximum drawdown, a negative first chronological half, and -17.375R
after removing its five best trades. It loses at both 10 and 20 bps per side.
The version is retired without parameter repair; its 75-session confirmation
sample remains untouched and must never be opened for this version.
The inspected development evidence is now present in `PORTFOLIO_SIGNALS.jsonl`
as one final inspection, 120 complete session-denominator records, and 111
closed signal records. `portfolio_maturity.py` reports the exact version as
`RETIRED_DEVELOPMENT`; `portfolio_funnel.py` excludes it from the active
development lane and mechanically rejects any later confirmation, shadow, or
live record for that retired identity.

The fourth activation is frozen, with returns still locked, at
`strategy_tournament/activations/equity-gap-recovery-v1-550a812bdf0e39f272b9c055c1111bef9cffecf13ad7e69c8f5feb1ecca36bfb.json`.
It reuses the already inspected legacy denominator without adding or replacing
dates or symbols: 100 requested dates, 95 available sessions, five permanent
fidelity exclusions, 950 ordered symbol-sessions, and 950 verified prior
closes. The contract fixes a 2-8% gap down, a reclaim of both the session open
and completed-bar VWAP after ten completed minutes without a new low, next-bar
entry, the decision-time session low as stop, the nearer of prior close or raw
2R as target, stop-first ambiguity, 15:50 force-flat, one daily selection, and
5/10/20 bps-per-side costs. It authorizes no return access until its separate
input inspection is committed and pushed. Inspection
`6109b4a84007118f27bd6a5e474655b827e8d95515e709f23c5cfb0b95d33b91`
reconciles all 95 sessions, 950 symbol-sessions, 370,500 exact one-minute bars,
and 950 prior closes with zero return calculations, provider requests, or
broker actions.

The inspected recovery result is
`research_results/2026-07-21-equity-gap-recovery-stage0-f0bb51acd548819e91292210b5b59029fa7b4307a37770b084707e1530b5c1bc.json`,
with independent rebuild
`strategy_tournament/inspections/equity-gap-recovery-v1-result-c2deb307da4528c2f3b6b48d7330c5df8c965a2ca6f04763a5ca2d83fc9a6a12.json`.
The exact variant is retired: 65 signals lost 10.967R at 5 bps per side,
with -0.169R expectancy, 0.609 profit factor, and 14.281R drawdown. At
20 bps per side it lost 15.529R. It cannot be tuned on this corpus or enter
the maturity ledger; the tournament advances to volatility compression.

The fifth activation is frozen, with returns still locked, at
`strategy_tournament/activations/volatility-compression-breakout-v1-3376ff9d2b6af10247e9eee3704f4e3bf612283a42c37a2c0f1b8f680a76d4c0.json`.
It binds the unchanged 100/95/5-date and 950-symbol-session legacy denominator.
The executable contract fixes the first-30-minute range, the 20 completed bars
immediately preceding each signal as the compression window, a maximum 0.60
compression ratio, a 1.50 signal-volume multiple, a completed close strictly
above compression high and VWAP, next-bar entry, compression-low stop, raw 2R
target, stop-first ambiguity, 15:50 force-flat, one daily selection, and
5/10/20 bps-per-side costs. No return access is authorized before its separate
committed input inspection. Inspection
`600ae694412202299505870eb48a14a9819a00f6c88867c3370a27e89c7d8fce`
reconciles all 95 sessions, 950 symbol-sessions, and 370,500 exact one-minute
bars with zero return calculations, provider requests, or broker actions.

The inspected volatility-compression result is
`research_results/2026-07-21-volatility-compression-breakout-stage0-5959e922a6cdaf117521a2a70877984497077f3c79acd5aedf2ac87eea0bbdfe.json`,
with independent rebuild
`strategy_tournament/inspections/volatility-compression-breakout-v1-result-67792983095a62d5811046d8d15a095faa75da7566d7957492d6c52213824f53.json`.
The exact variant is retired: 95 signals lost 19.259R at 5 bps per side,
with -0.203R expectancy, 0.665 profit factor, and 22.495R drawdown. At
20 bps per side it lost 30.852R. It cannot be tuned on this corpus or enter
the maturity ledger; the tournament advances to short-horizon reversal.

The sixth activation is frozen, with returns still locked, at
`strategy_tournament/activations/short-horizon-oversold-reversal-v1-b65565475be90a9ad981e4e5dd1c598edbd21d5ee21ba9cad3333ca7da8d9d51.json`.
It binds the unchanged 100/95/5-date and 950-symbol-session legacy denominator.
The executable contract fixes a 30-completed-bar pre-trigger return of at most
-3%, simple RSI(5) no higher than 20 on the final pre-trigger bar, a bullish
completed close strictly above the prior high and session VWAP, next-bar entry,
decision-time session-low stop, raw 1.5R target, stop-first ambiguity, 15:50
force-flat, one daily selection, and 5/10/20 bps-per-side costs. No return
access is authorized before its separate committed input inspection. Inspection
`0e8796722e06b0643aeb9f6e3001e9e9320730d617619b50884293c006dd46dd`
reconciles all 95 sessions, 950 symbol-sessions, and 370,500 exact one-minute
bars with zero return calculations, provider requests, or broker actions.

The inspected short-horizon reversal result is
`research_results/2026-07-21-short-horizon-oversold-reversal-stage0-bb2152f3f18aef92bac5c7d5dcffc15bdbe2967525e63fa6678c6c553aeb75c6.json`,
with independent rebuild
`strategy_tournament/inspections/short-horizon-oversold-reversal-v1-result-be56bcf7bf8fe2fd16e223f510a8236135fbc39f1a27e7531af9a4649a706e25.json`.
The exact variant is retired for insufficient capacity: its ten signals earned
2.442R, +0.244R expectancy, 3.634 profit factor, and 0.749R drawdown at 5 bps
per side, and remained positive at 20 bps, but the frozen Stage 0 minimum is 30
signals. The favorable small sample cannot be tuned, expanded on the evaluated
corpus, promoted, or entered in the maturity ledger.

The seventh activation is frozen, with returns and provider collection still
locked, at
`strategy_tournament/activations/cross-sectional-momentum-v1-a8970ad2bfb0045f54404da985178686ec7cd0068c3a6a23f59165c7e415520a.json`.
It binds 24 non-overlapping 2025 ranking dates, 118,636 dated common-stock
membership rows, and 5,457 unique symbols from the two inspected point-in-time
scanner-replay security masters. The contract fixes a 15:45 completed-bar
price, prior-20-session return and $20 million average-dollar-volume gate,
prior-50-session SMA gate, top-decile ranking, three-name maximum, next-session
open, 1.5-ATR14 stop, fifth-session close, split handling, and 5/10/20
bps-per-side costs. The exact daily collection spans 2024-12-16 through
2025-12-22 and cannot begin until an independent committed activation
inspection authorizes it. Inspection
`a0f893cb02e31fb445febfacd4e5535a6e5ec602f18d630c06827efbdc84c31f`
exactly rebuilds all membership, calendar, split-source, implementation, and
rule bindings with zero returns, provider requests, or broker actions; its
minimum observed spacing is seven sessions and it authorizes only the frozen
daily input collection.

The daily collection is complete at
`strategy_tournament/cross_sectional_momentum/collection-status.json`: 5,457
frozen symbols produced 1,279,526 raw daily bars across the exact date window
in 131 provider requests. Four symbols returned no rows and remain explicit
missing inputs. The collection made zero broker actions and computed zero
strategy returns; the private payload is hash-bound and remains outside Git.

Input inspection
`198957534e2d6ec63519d9e6c96b598ff96f8490f2eece45af282ffc5b9358de`
reconciles the complete 118,636-member denominator. It finds 77,639 exact
daily-window plus 15:45-prefix inputs, 3,030 members missing a required daily
session, and 37,967 with an incomplete 15-minute ranking prefix. Every miss is
retained as a no-signal; the inspection makes no provider or broker call,
computes zero trade returns, and authorizes the one fixed Stage 0 evaluation.

The inspected cross-sectional momentum result is
`research_results/2026-07-21-cross-sectional-momentum-stage0-9a165ae149b2f9220a2d78c2251a34617ad63ed2a468ded8749fb5b0cc0a8869.json`,
with independent rebuild
`strategy_tournament/inspections/cross-sectional-momentum-v1-result-fa04429130158e31d221151b1d00d4dd3432ec43a8c88595dd0f8bc4a3c0421b.json`.
Its 72 signals produced +6.698R total, +0.093R expectancy, 1.214 profit
factor, and 13.636R maximum drawdown at 5 bps per side. The exact rule
remained positive at 20 bps with +5.516R total and 1.175 profit factor, but
the frozen 8R drawdown maximum is non-negotiable. The exact variant is retired,
cannot be repaired on this corpus, and contributes no maturity evidence. The
tournament advances to post-earnings drift.

The eighth activation is frozen, with all new provider and return access still
locked, at
`strategy_tournament/activations/post-earnings-drift-v1-d41dd9d9db8531ed90e8cfde0f40382b250ca67177b16f01146e2eab1594f6cf.json`.
It binds all 102 inspected source-verified positive-primary pairs across 52 new
2025 dates and 84 symbols, rather than reusing the failed earnings-reversal
confirmation sample. Fifteen prior policy trials and that failed confirmation
remain declared. Company-verified Robinhood earnings metadata must match the
frozen reaction session and show actual EPS strictly above estimated EPS. The
market rule fixes a 1%-8% opening gap, a 15:45 decision from completed minute
bars, close above open and VWAP, 15:46 entry, decision-time session-low stop,
fifth-date close, and 5/10/20 bps-per-side costs. The activation authorizes no
provider request until its zero-return inspection is committed and pushed.
Inspection
`fad0b77a77d8ae2d2d68197e00a1c25dd7bc189969c3b8db05a514784cd573e5`
independently rebuilds all 102 pairs, 52 dates, 84 symbols, source bindings,
prior-trial declarations, implementation, and rules. It computed zero returns,
made zero provider or broker calls, and authorizes only the exact earnings and
market input collection.

The first authorized metadata collection made all 84 read-only earnings calls,
but its non-interactive ingestion pipe closed before retaining any response.
No market outcome or strategy return was accessed. The original activation and
inspection remain preserved as the incident lineage. Superseding transport
activation
`strategy_tournament/activations/post-earnings-drift-v1-1302ec21b91847106830f51dd8e90ee2309290c93d3032f2696a84a55314d9a5.json`
keeps the exact strategy rules and 102-pair denominator unchanged, explicitly
counts the 84 discarded provider calls, and requires a new committed inspection
before one fully accounted metadata retry.
Inspection
`1f0bc5f5a0416ffe09d832be2f1dac2f72d03236ab775e85c4152dd13ebae58e`
rebuilds that superseding activation with zero additional provider calls and
zero returns. It verifies the unchanged 102-pair rule contract, the 84-call
incident accounting, and authorizes one fully retained metadata retry.

That retry also retained zero responses: canonical PTY line buffering rejected
the long JSON records before the collector received its terminator. No market
outcome or return was accessed. Second transport activation
`strategy_tournament/activations/post-earnings-drift-v1-dbedd0065e72bedbb2bb26f88dd9da5d7c0c42de57bbb8b0fab55dfa2a1697d4.json`
therefore counts 168 discarded calls across two failed ingestion attempts and
changes only the ingestion channel to raw no-echo streaming with an explicit
terminator. Strategy rules, dates, candidates, and all outcome gates are still
identical and locked pending another independent inspection.
Inspection
`e728d442e0e32c86ccb4447595f4245a195276fd2b9084c85b2e488fe24b3c0e`
rebuilds the corrected stream contract, the 168 prior-call count, and the
unchanged 102-pair rules with zero additional provider calls or returns. It
authorizes the next metadata retry through the corrected channel.

The corrected earnings collection is complete at
`strategy_tournament/post_earnings_drift/earnings-status.json`. All 84 frozen
symbols returned usable histories containing 648 normalized earnings rows. Its
252-provider-call total includes all 168 discarded calls plus the 84 retained
responses; the private payload is hash-bound, zero strategy returns were
computed, and no broker action occurred.

The first market collection failed closed after two provider calls because the
provider included its documented request-end boundary at 16:00 ET and the
regular-session normalizer rejected that extra row. One isolated read-only
diagnostic call confirmed that exact boundary behavior. No market payload or
strategy return was retained. Superseding activation
`strategy_tournament/activations/post-earnings-drift-v1-04dcf6470f6347ede51b6bed481c8ddd389744b7d649be14fbee646d235994ca.json`
truthfully counts all three discarded market calls and the already retained
earnings corpus, ignores same-date rows outside the frozen 09:30-15:59 regular
session, and preserves activation rules hash
`177a304fe28a4923b373975cb025a7b157fa5b8c476e0bca5a6d35c1b2dc2af9`.
It requires an independent committed inspection before market collection may
resume.
Inspection
`0159e738fe1e50c3248ea1496d560b264419f80077fc906b91d4cb6373c5d5f4`
rebuilds the superseding activation, its unchanged strategy-rules hash, the 102
candidate pairs, and all 255 provider calls made before the activation. It made
zero additional provider or broker calls, computed zero returns, and authorizes
the exact market retry.

The retry completed at
`strategy_tournament/post_earnings_drift/market-status.json`: all 84 symbols
have daily data and all 102 candidate pairs have minute data. The hash-bound
private payload contains 20,328 daily rows and 39,692 regular-session minute
rows. All 102 returned 16:00 boundary rows were excluded; 55 effective calls
plus the three discarded incident calls are reported. No return was computed
and no broker action occurred.
Input inspection
`dfbf41c267099c34fc2e97f88c801b9d6f2b4731c9598cba107c7faef5cbf9cf`
rebuilds all collection bindings and the complete 102-pair denominator without
provider access or return calculation. Eighty-seven pairs have a complete
390-bar regular session; 15 remain in the denominator as incomplete and cannot
signal. The inspection authorizes the single frozen Stage 0 evaluation.
The resulting six signals produced -0.4465R expectancy, 0.3303 profit factor,
-2.6788R total, and 2.7053R maximum drawdown at 5 bps per side. At 20 bps per
side the total is -2.8100R. The result fails signal count, expectancy, profit
factor, and stress gates and remains outside maturity; permanent retirement is
confirmed by result inspection
`a9efc2d2e2e9454673dd61b03f3037ff429613bde835ee5dd5d920b048d06d7a`.
The exact variant is permanently retired with no tuning on this corpus. The
first wave now has eight dispositions, seven Stage 0 retirements, and one Stage
0 survivor that later failed development. Relative-strength continuation is
next in the frozen queue.

Relative-strength continuation activation
`strategy_tournament/activations/relative-strength-continuation-v1-a22173e0b7ee21a9a5f37e4b7bc84c8863e9733d6c6d670313662a6e2b0a55cb.json`
freezes 80 new 2025 target dates, 395,316 dated common-stock memberships, and
5,552 unique symbols. Every cross-sectional-momentum Stage 0 date is excluded.
At 09:59 it ranks the full point-in-time universe by return minus SPY and retains
every name at least two percentage points stronger and above $5. From 10:00 to
14:30 it requires a close above prior high-of-day and completed-session VWAP on
at least 1.5 times the preceding 20-minute mean volume. The first executable
trigger by time, frozen relative-strength rank, and symbol enters at the next
minute open, uses the last-five-bar low, a 2R target, stop-first ambiguity, and a
15:50 force-flat. All fills and exits use 5/10/20 bps per side. Only 09:30-09:59
selection inputs may be collected after an independent activation inspection;
post-10:00 outcomes remain locked until the prefix is independently inspected.
Activation inspection
`3d386ed05e5b616f014169f6a6dfb0088ba146c82579140c66b71463a55586d2`
rebuilds all 80 dates, 395,316 memberships, 5,552 symbols, source hashes,
implementation, and rules with zero provider calls, returns, or broker actions.
It authorizes only the frozen 09:30-09:59 prefix collection.
The prefix collection is complete at
`strategy_tournament/relative_strength_continuation/prefix-status.json`. Across
all 80 dates it made 831 provider calls, retained 6,592,844 bars and 374,731
symbol-date payloads with at least one row, and ignored 236,485 rows outside the
half-open selection window. All 395,316 dated memberships remain in the
denominator. It computed zero returns and made no broker action; post-10:00
outcomes remain locked pending prefix inspection.

The first prefix inspection failed closed before ranking because the bound
common-stock daily corpus correctly omitted the SPY ETF and therefore could not
supply its prior close. The 831-call prefix remains valid and no target outcome
or stock rank was accessed. Superseding activation
`strategy_tournament/activations/relative-strength-continuation-v1-269dcb726208e7f364709a65ed51158c05ccefd02ecc005def0ffb460cc17c8a.json`
preserves rules hash
`0aa48c15a997b5182cf7223d0c2f6597e2fd9cbc5ce3bf9b2d97753cdf03e6f8`,
binds the existing prefix, and authorizes only one exact prior-session SPY daily
bar per target date before a new independent inspection.
Inspection
`7bfe5fddcbaaba26be35eba3cf35dea00d617a6654464998cc499f62f161f992`
rebuilds the successor activation, unchanged rules, full denominator, and 831
prior prefix calls. It makes zero additional calls or returns, authorizes only
the 80 exact SPY prior-close requests, and keeps target outcomes locked.

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
