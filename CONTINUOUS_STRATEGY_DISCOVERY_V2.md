# Continuous Strategy Discovery V2

Adopted: 2026-07-23

## Decision

The campaign's maximum of three **new mechanism families** per ISO week is not
a general research shutdown. After that budget is spent, discovery continues
through prospectively frozen exact versions inside already-authorized mechanism
families, provided each version uses disjoint evidence and every previous
attempt remains in the audit and selection history.

No failed exact version may be repaired on its evaluated corpus. A successor
may use the failure only as contaminated hypothesis-generating evidence. Its
development grid, deterministic selection rule, dates, confirmation reserve,
costs, falsifiers, implementation hashes, and production semantics must freeze
before any new outcome access.

## First completed lane

The first continuous lane is
`broad-etf-trend-pullback-v2-cost-floor`, implemented by the dense runtime under
family ID `liquid-etf-trend-pullback-cost-floor` and attributed to the existing
mechanism family `broad-etf-trend-pullback`.

Its predecessor is the retired
`broad-etf-trend-pullback-v1`. That version evaluated signals from 2023 through
2025 and failed positive total growth at 20 bps per side. None of those outcomes
can count toward v2 development, confirmation, or maturity.

The successor freezes:

- Four liquid ETFs: `SPY`, `QQQ`, `IWM`, and `DIA`.
- A 32-trial grid: SMA `{100,200}` × RSI(2) maximum `{5,10}` × three-session
  decline `{2%,3%}` × stop `{1.0,1.5}×ATR14` × hold `{3,5}` sessions.
- 200 warmup sessions, 1,000 rolling-origin development sessions, a
  five-session embargo, and 500 untouched confirmation sessions.
- Evidence ending before 2023, chronologically and outcome-disjoint from the
  retired predecessor's first 2023 signal.
- Alpaca raw SIP daily symbol ranges plus point-in-time Massive split actions,
  frozen before provider access and held constant across development and
  confirmation.
- All existing account-growth, 5/10/20-bps, stationary-bootstrap, DSR, Holm,
  PBO, neighbor-stability, power, profit-factor, drawdown, concentration, and
  execution gates.

The prior family attempt remains explicit in the contract. This successor does
not consume a new-mechanism-family slot.

## ETF successor disposition

The independently inspected development result
`650511bc77dc31fa3a9aeeab0029f9df7d6ffb99dfd165fbeb36b9908987665b`
is `REJECTED`. Every one of the 32 trials had negative 20-bps total log growth.
The best trial produced -0.006642 log growth, 0.8766 profit factor, and 6.799R
drawdown. No trial passed the DSR, Holm, rolling-fold, stressed PF/drawdown, or
neighbor-stability gates. Trial and rule accounting were complete.

This exact family version is retired without confirmation access. Its
development scope is now recorded in the global outcome-exposure index and
cannot be relabeled as untouched evidence.

## Latest immediate lane disposition

The latest successor was
`short-horizon-oversold-reversal-v3-gap-universe`, implemented under runtime
family ID `gap-universe-oversold-reversal`. It remains inside the already tested
`short-horizon-oversold-reversal` mechanism and consumes no new-family slot.

The preceding v2 search failed closed after the plugin computed the deterministic
development family but before a result artifact was written: it returned the
correct dataset manifest as an absolute path while the contract stored the same
path repository-relative. Failure artifact
`9f907241783e940b84e6fcdfa08529a495539ea05daaf2da6d86fe091a9820bc`
records that no metrics were surfaced, selection did not run, and confirmation
remained untouched. V3 changes only canonical path serialization, binds the v2
failure, preserves the exact grid, dates, rules, and costs, and must repeat the
full freeze/preflight/search chain before deterministic recovery.

The shortest defensible evidence path reuses the point-in-time input graph
previously frozen for representative equity-gap research:

- Development is the already outcome-exposed 120-session 2025 corpus containing
  4,833 common-stock symbol-sessions selected at 09:35 ET by a frozen opening
  price above $5 and 2-8% opening-gap rule. It is explicitly contaminated
  training and cannot itself become untouched evidence.
- The next five frozen sessions remain the embargo.
- The final 75 sessions, from 2025-08-14 through 2025-12-31, contain 3,192
  candidate symbol-sessions and currently have no overlap with the global
  outcome-exposure index. They remain unopened until one exact development
  winner is frozen.
- The complete 32-trial grid is lookback `{15,30}` minutes × selloff threshold
  `{-2%,-3%}` × simple RSI period `{3,5}` × RSI maximum `{15,20}` × target
  `{1.0,1.5}R`.
- Every trial uses a completed bullish close above the prior high and session
  VWAP, next-minute entry, the completed session low as stop, stop-first
  ambiguity, a 15:50 ET force-flat, the five-times-cost floor, one daily entry,
  chronological account compounding, and 5/10/20-bps costs.

Independent development inspection
`d8d9467606a1672a9fd516ab00affc514f7e496ce2bd101b3a9bcd8b2f9e0c0f`
retired v3. All 32 trials were complete, but zero passed Deflated Sharpe or
Holm, only two retained positive growth in every rolling fold, and no trial
survived the complete selection-aware gate set. The confirmation reserve
remained unopened and may not be used to repair the rejected version.

Continuous work now moves to the best existing-family successor with an
explicitly contaminated development partition and disjoint untouched
confirmation. This does not consume a new-family weekly slot and does not wait
for the next ISO week.

## Liquid-ETF cross-sectional successor disposition

The latest liquid-ETF successor was
`cross-sectional-momentum-v3-liquid-index-etf`, runtime family
`liquid-etf-cross-sectional-momentum`. The prior equity version had positive
5/10/20-bps results but failed only its Stage 0 drawdown gate; its exact version
remains retired. The successor preserves the cross-sectional-momentum mechanism
while replacing microcap concentration with the complete `DIA`, `IWM`, `QQQ`,
and `SPY` universe.

Its complete 32-trial grid is trailing return `{20,60}` sessions × SPY trend
SMA `{100,200}` × minimum return above the cross-sectional median `{1%,2%}` ×
stop `{1.5,2.0}×ATR14` × hold `{3,5}` sessions. Ranking uses only completed
closes, entry is the next session open, daily ambiguity is stop-first, and at
most one new family entry is allowed per session.

The already-inspected 2016-2020 daily partition is explicitly contaminated
development training and its source family outcome remains adverse history.
The five-session embargo and 500-session 2021-2022 confirmation reserve remain
disjoint and untouched. The implementation can load the shared development
dataset once with zero provider requests and uses the same exact ranking and
risk rules in production evaluation.

The v2 transition failed closed on its first development trial because
cost-sensitive account sizing allowed 5/10/20-bps paths to fill different
signals. Failure artifact
`f7ed6419da1bffc45044af4619ee4c558917e10b1e0c2758f071861f8f708c1b`
records zero returned trials, zero surfaced metrics, no result or selection,
and no confirmation access. V3 preserves the entire grid, dates, rules, and
costs; it changes only contention semantics. The 20-bps path freezes the
conservative filled-signal set and the 5/10/20-bps scenarios apply their exact
costs to that identical set. Capital-blocked candidates remain explicit in
trial accounting.

Development result
`8d3470d29b0138fac583e660bdf295ce9cc99c390e6ce2fb828c498a0a3d950c`
evaluated all 32 trials from one shared dataset load in 23.2 seconds with zero
provider requests. Independent inspection
`9ebc37a593b65c10697572cf23f6710c985e056861ec9624a44c69dd576155ef`
is `REJECTED`: all 32 trials had complete rule and trial accounting, but none
had positive 20-bps growth, stressed profit factor of at least 1.20, drawdown at
or below 6R, DSR probability of at least 0.90, Holm rejection, positive growth
in every rolling fold, or stable one-step neighbors. No winner or evidence
target was frozen, and the 500-session confirmation reserve remains untouched
and inaccessible.

The exact v3 successor is terminal and cannot be repaired on its evaluated
corpus.

## Equity-gap-continuation search disposition

Continuous work now returns to the existing equity-gap-continuation mechanism.
Its v1 production rule failed representative development, but its earlier
falsification lane showed enough directional signal to justify one
selection-aware development family rather than inventing a new mechanism or
waiting for a calendar reset.

The active successor must freeze:

- The already exposed 120-session, 4,833-candidate minute-bar corpus as
  contaminated development training only.
- The unchanged five-session embargo and untouched 75-session,
  3,192-candidate confirmation reserve from 2025-08-14 through 2025-12-31.
- A 32-trial grid: minimum opening gap `{2%,4%}` × opening range `{5,15}`
  minutes × breakout volume multiple `{1.5,2.5}` × signal cutoff
  `{10:30,11:30}` ET × target `{1.5,2.0}R`.
- The existing 8% maximum gap, next-minute entry, opening-range-low stop,
  stop-first same-interval ambiguity, 15:50 ET force-flat, one entry per day,
  complete no-signal accounting, 5/10/20-bps costs, and portfolio account
  simulation.
- The same deterministic daily ranking: earliest completed trigger, higher
  breakout volume multiple, larger opening gap, then canonical symbol.
- Selection-aware DSR, Holm, PBO, rolling-fold, neighbor-stability,
  concentration, profit-factor, drawdown, stationary-bootstrap, and power gates.

The v1 exact rule
`{2%, 15 minutes, 1.5x volume, 11:30 ET, 2.0R}` remains adverse training inside
the complete family. No confirmation minute bar may be collected unless an
independent development inspection selects one exact immutable winner and
freezes its rules hash and evidence counts first.

Before the next development evaluation, the discovery controller must store
row-level and repeated statistical evidence in the ignored content-addressed
historical store while Git retains a compact hash-bound result and inspection.
The already published large ETF development artifact remains auditable history;
it is not a precedent for placing another large row-level result in Git.

Freeze and commit the exact successor before generic preflight:

```sh
python3 equity_gap_continuation_discovery.py freeze \
  --created-at <actual-current-ISO8601-timestamp>
python3 strategy_discovery.py preflight \
  strategy_tournament/v2/continuous/equity-gap-continuation-v2-development-search/family-contract/contract-<sha256>.json
```

After committing preflight, use `strategy_discovery.py freeze-search`,
`evaluate-development`, and `inspect-development` in order. Development loads
the inspected local corpus once and makes zero provider requests. Confirmation
data collection remains fail-closed unless the independent development
inspection returns `WINNER_SELECTED`; it must then bind the exact winner rules
hash before any confirmation bar is requested.

Development result
`1d79a6e7d38cebf85087cad7ce1f2370fddc04ca816de75591ed83715ec41f3b`
evaluated all 32 trials in 29.7 seconds from one shared dataset load with zero
provider requests. Twenty-one trials had positive 20-bps growth, nine passed
stressed profit factor, 16 passed drawdown, and 24 passed neighbor stability.

Independent inspection
`270351f05caeae92cc6ccdec4f5c209c3129e4dbde8e333ab3f5b402d9676d00`
is `REJECTED`. No trial passed DSR or Holm, family PBO was 0.8286, and every
trial had at least one losing rolling-origin fold. The highest raw-growth
trial's DSR probability was only 0.3745. No winner or evidence target was
frozen, and the 75-session confirmation reserve remains untouched and
inaccessible.

The exact successor is terminal and cannot be repaired on its evaluated corpus.
Continuous work moves immediately to the strongest remaining existing-family
successor with contaminated development and disjoint untouched confirmation;
the weekly new-family reset is not a work pause.

## Transition chain

Run the lane from the repository root:

```sh
python3 continuous_strategy_discovery.py status
python3 continuous_strategy_discovery.py freeze-calendar \
  --created-at <actual-current-ISO8601-timestamp>
```

Commit the zero-price calendar contract, then independently inspect and commit
it:

```sh
python3 continuous_strategy_discovery_inspection.py inspect-contract \
  strategy_tournament/v2/continuous/broad-etf-trend-pullback-v2-cost-floor/calendar/contract/continuous-successor-calendar-contract-<sha256>.json \
  --inspected-at <actual-current-ISO8601-timestamp>
```

Only then may the single public-calendar request run. Commit its calendar,
source attestation, and collection status before independent data inspection:

```sh
python3 continuous_strategy_discovery.py collect-calendar \
  strategy_tournament/v2/continuous/broad-etf-trend-pullback-v2-cost-floor/calendar/contract/continuous-successor-calendar-contract-<sha256>.json \
  --collected-at <actual-current-ISO8601-timestamp>
python3 continuous_strategy_discovery_inspection.py inspect-calendar \
  strategy_tournament/v2/continuous/broad-etf-trend-pullback-v2-cost-floor/calendar/collection/continuous-successor-calendar-collection-<sha256>.json \
  --inspected-at <actual-current-ISO8601-timestamp>
```

After the inspected calendar is committed, freeze and commit the exact family
contract and capacity manifest:

```sh
python3 continuous_strategy_discovery.py freeze-successor \
  --created-at <actual-current-ISO8601-timestamp>
python3 strategy_discovery.py preflight \
  strategy_tournament/v2/continuous/broad-etf-trend-pullback-v2-cost-floor/family-contract/contract-<sha256>.json
```

The normal discovery chain then applies: freeze search, freeze and collect only
the development dataset, inspect it, evaluate all 32 trials, independently
select or retire, freeze one exact winner if earned, and only then collect the
preregistered confirmation reserve.

This lane has a separate committed pre-2023 calendar from the future W31 batch,
so pass it explicitly when freezing either collection plan:

```sh
python3 dense_data_collection.py \
  --calendar historical_batches/continuous_v2/session-calendar-2014-01-through-2022-12.json \
  freeze-development path/to/committed-successor-search.json
```

The daily fixed-ETF collection plane uses four frozen Alpaca symbol-range
requests plus split metadata, then loads the resulting dataset once for all
trials. It does not issue one provider request per date.

The first committed development collection plan used Alpaca SIP daily bars and
failed closed before trial evaluation because all four symbols began on
2016-01-04 instead of the frozen 2015-03-09 warmup boundary. That plan remains
adverse data-readiness history. The second exact plan preserved those dates and
bound Massive daily ranges, but the configured plan returned HTTP 403 on its
first price task after split metadata completed. It also remains adverse history.
Neither plan evaluated a trial or touched its confirmation reserve.

The active recovery first extends the independently inspected zero-price calendar
through 2022. Only after that inspection may a new exact contract select its
complete 200-warmup, 1,000-development, five-embargo, and 500-confirmation
partition from 2016-2022 and bind Alpaca raw SIP daily ranges. The global
outcome-exposure index currently has no 2016-2022 records and the predecessor's
first evaluated signal is in 2023. Every prior artifact remains preserved, but
none can count as promotion evidence for the new chain.

## Parallel lanes

The predeclared residual-equity and intraday-ETF families remain subject to the
new-family weekly budget. Their calendar gate is independent of this successor
lane and cannot block it.

The short-horizon oversold-reversal successor above is active now. Its retired
v1 ten-signal result is hypothesis evidence only, and the already exposed gap
corpus is contaminated development only. Neither may count toward confirmation
or maturity.

## Completion

This lane is research only until the normal evidence chain earns
`PILOT_READY`. The campaign remains incomplete until one exact strategy also
completes five clean prospective shadows, passes its production path, closes one
controlled live trade, reconciles flat with no residual orders, passes every
audit, and is committed and pushed.
