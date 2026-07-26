# Continuous Strategy Discovery V2

Adopted: 2026-07-23

## Decision

The campaign permits at most three **outcome-active mechanism families** at
once. After an independently inspected terminal disposition, its slot is
released immediately; discovery never waits for an ISO-week boundary. Every
previous attempt remains in the audit, global exposure index, and cumulative
selection history.

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
Continuous work moves immediately to
`two-to-three-day-cross-sectional-reversal-v2-liquid-index-etf`; the weekly
new-family reset is not a work pause.

The v1 reversal predecessor was capacity-falsified on 24 dates before any
market outcomes were accessed. V2 retains the same mechanism on the complete
four-index-ETF universe and declares all 32 combinations of two/three-session
return, SPY SMA100/SMA200 regime, 0.5%/1.0% cross-sectional lag, 1.0/1.5 ATR14
stop, and two/three-session hold. It uses the exposed 1,000-session 2016-2020
partition only as development training, retains the five-session embargo, and
keeps all 500 sessions in 2021-2022 inaccessible for confirmation unless a
selection-adjusted winner is first frozen. The implementation makes zero
provider or broker requests.

Independent inspection
`0abea388cc121ca801cd2c353fb2fa2da069f9397ba499b930a471d9cab29734`
is `REJECTED`. All 32 trials had negative 20-bps log growth, stressed profit
factor below 1.20, drawdown above 6R, DSR below 0.90, no Holm rejection, at
least one losing rolling fold, and no positive neighbor. The least-adverse
trial still had -0.057875 log growth, 0.6846 profit factor, 16.23R drawdown,
and only one positive fold out of five. No winner or evidence target was
frozen; all 500 confirmation sessions remain untouched. This exact version is
terminal, and continuous discovery advances another existing family.

The active successor is
`five-day-52-week-high-continuation-v2-liquid-index-etf`. Its v1 exact version
was capacity-falsified on the same 24-date ceiling without outcome access. V2
retains that mechanism on all four liquid index ETFs and prospectively declares
32 combinations of 2%/4% five-session return, 98%/100% proximity to the prior
252-session high, SPY SMA100/SMA200 regime, 1.0/1.5 ATR14 stop, and
three/five-session hold. Historical and production evaluators use the same
completed-bar ranking, next-open entry, stop-first ambiguity, and maximum
five-session exit. Development reuses only the contaminated 2016-2020
partition; the five-session embargo and all 500 confirmation sessions remain
locked.

Independent inspection
`711fc09695abe7fb7c8ed20a167e7f14d335f7ee9c15b0c3c7743689f2f05be4`
is `REJECTED`. Only two trials had positive stressed growth and acceptable
drawdown; none passed stressed profit factor, DSR, Holm, every rolling fold, or
neighbor stability. The best trial had only 29 fills, +0.001562 log growth,
1.0360 profit factor, 5.56R drawdown, and DSR 0.472. No winner or evidence
target was frozen, and all 500 confirmation sessions remain untouched.

The existing volatility-compression-breakout successor on the disjoint
120-session gap-universe minute corpus is now terminal. Independent inspection
`c429476cf46b2425dcd04a2019e7321c9bd92489c68f1411c475321ef9d8e253`
rejected all 32 trials: every stressed account path was negative, no stressed
profit factor reached 0.52, every drawdown exceeded 21R, DSR was zero, and PBO
was 1.0. No winner or evidence target was frozen, and all 75 confirmation
sessions remain untouched.

The turn-of-month successor is now terminal after independent inspection
`825e870d747bfed3d73fca0e27f6cbb2efea770f21517e66030c453c0f839ce8`.
Signal capacity was adequate at 60-206 fills per trial, but every 20-bps path
was negative, no stressed PF reached 0.81, every drawdown exceeded 14R, DSR was
zero, and PBO was 1.0. No winner or power target was frozen, and all 500
confirmation sessions remain untouched.

The sector-ETF-rotation successor is terminal after independent inspection
`93da8a6cdbdcaca98ac7b3839c71e41500011ec9c391fbf061cb026f907b22d4`.
All 32 stressed account paths were negative, no trial had positive growth in
every fold or a positive one-step neighbor, no Holm test rejected, and PBO was
0.7143. The least-negative path still had -0.00374 log growth, 0.958 profit
factor, 8.78R drawdown, and a negative stationary-bootstrap lower bound. The
120-session development scope is exposed; all 66 confirmation sessions remain
untouched and cannot repair this exact version.

Continuous discovery proceeds immediately to the existing close-to-open ETF
momentum mechanism. Its v1 exact rule on 2023-2025 data was close to flat at
primary cost but failed stress and drawdown. The successor must use only
disjoint 2022 `QQQ`, `IWM`, and `DIA` date-symbol pairs from the already
inspected local daily plus 15-minute source. `SPY` is excluded because the
sector successor exposed its 2022 development pairs. Before target-return
access the close-to-open successor must freeze the complete three-ETF universe,
warmup, development, five-session embargo, untouched confirmation, and all 32
combinations of 15:15/15:30 completed decision
bar, 0.5%/1.0% session-return floor, prior SMA20/SMA60 trend, 0.5/1.0 ATR14
stop, and next-open/next-09:45 close exit. Entry is the next observable
15-minute open, ambiguity is stop-first, the five-times-cost floor and one
daily entry remain mandatory, and no position may remain beyond the next
session. This existing mechanism consumes no new-family weekly slot and
requires no calendar wait.

Independent inspection
`491e629fe308683f25e45e6e37eb876a96b5fc20009ca67c40a44cc2ef33dd44`
is `REJECTED`. All 32 trials had complete rule and accounting records, but every
20-bps account path was negative, every trial had at least one non-positive
rolling fold, and none passed stressed profit factor, Deflated Sharpe, Holm, or
neighbor stability. The least-adverse trial still had -0.013056 log growth,
0.1978 profit factor, a negative stationary-bootstrap lower bound, and only nine
fills. No winner or evidence target was frozen, and all 66 confirmation sessions
remain untouched.

Continuous discovery now advances
`cross-sectional-momentum-v5-liquid-common-stock` inside the existing
cross-sectional-momentum mechanism. Its v1 equity predecessor
produced positive 5/10/20-bps returns across 72 signals but failed the drawdown
gate and concentrated gains in thin securities. The successor therefore tests
the same completed-close ranking and next-open continuation mechanism on a
prospectively frozen liquid common-stock universe: prior close at least $10,
prior 20-session median dollar volume at least $50 million, and the top 250 by
prior 60-session median dollar volume. The 32-trial grid covers 20/60-session
return, 50/100-session trend, 1%/2% cross-sectional excess, 1.5/2.0 ATR14 stop,
and three/five-session hold.

Development has 80 already-exposed decision dates inside a complete account
calendar that preserves no-trade days, overlapping positions, mark-to-market
drawdown, and compounding. Five embargo sessions begin only after the final
development position can settle. The later reserve contains 25 untouched
decision dates. `confirmation_signal_capacity` is separate from the account
calendar, so zero days cannot inflate the frozen power inventory. The complete
grid and exact production semantics must be committed before any additional row
from the existing content-addressed 2025 daily source is opened. This is an
existing-family successor, consumes no new-family weekly slot, and requires no
calendar wait.

V4 is terminal as an implementation-boundary failure. Its committed search
loaded the cached file, then shared normalization rejected an empty symbol
series before any candidate, return, account path, trial metric, or selection
was computed. V5 changes only the normalization boundary: empty row containers
are omitted from the bar map while their symbols remain in point-in-time
membership and therefore resolve as missing-data misses. The grid, dates,
costs, selection rule, and untouched confirmation reserve are unchanged.

## Transition chain

After the implementation commit is pushed, run:

```sh
python3 liquid_equity_momentum_discovery.py freeze \
  --created-at <actual-current-ISO8601-timestamp>
python3 strategy_discovery.py preflight \
  strategy_tournament/v2/continuous/cross-sectional-momentum-v5-liquid-common-stock/family-contract/contract-<sha256>.json
python3 strategy_discovery.py freeze-search \
  strategy_tournament/v2/continuous/cross-sectional-momentum-v5-liquid-common-stock/family-contract/contract-<sha256>.json
python3 strategy_discovery.py evaluate-development \
  strategy_tournament/v2/discovery/liquid-equity-cross-sectional-momentum/search/liquid-equity-cross-sectional-momentum-search-<sha256>.json
python3 strategy_discovery.py inspect-development \
  strategy_tournament/v2/discovery/liquid-equity-cross-sectional-momentum/development/liquid-equity-cross-sectional-momentum-development-<sha256>.json
```

Commit and push each transition before its successor. If and only if independent
development inspection selects a winner, freeze and commit that exact winner,
then bind the reserve to its rules hash before any confirmation outcome access:

```sh
python3 strategy_discovery.py freeze-winner \
  strategy_tournament/v2/discovery/liquid-equity-cross-sectional-momentum/development-inspection/liquid-equity-cross-sectional-momentum-development-inspection-<sha256>.json
python3 liquid_equity_momentum_discovery.py freeze-confirmation \
  strategy_tournament/v2/discovery/liquid-equity-cross-sectional-momentum/winner/liquid-equity-cross-sectional-momentum-winner-<sha256>.json \
  --created-at <actual-current-ISO8601-timestamp>
```

The cached daily file is loaded once per development or confirmation process.
No provider request is needed. Preflight opens only committed metadata; the
price file remains closed until the committed search authorizes development.

## Latest disposition and immediate successor

Independent inspection
`6a9bf3d06a3ecc3b37e37c6f7304c9945b317cb19a6995af453695f150f6fa29`
is `REJECTED`. V5 evaluated all 32 trials in 29.5 seconds from one cached load
with zero provider requests. Two trials had positive 20-bps growth, 1.246
stressed profit factor, and 5.633R drawdown, but their DSR probability was only
0.4288, the stationary-bootstrap lower bound was negative, two rolling folds
lost money and one was flat, and only one of five one-step neighbors remained
positive. No Holm-adjusted test rejected. No winner or evidence target was
frozen, and the 25 confirmation opportunities remain untouched.

Continuous work now proceeds with
`two-to-three-day-cross-sectional-reversal-v5-liquid-common-stock-residual-spy`.
The mechanism is the already evaluated short-horizon cross-sectional reversal,
not a new family. Its material successor change is the prospectively frozen
top-250 liquid common-stock denominator and market-residual standardization.
It uses the same complete account calendar, 80 explicitly contaminated
development decision dates, post-settlement five-session embargo, and 25
untouched confirmation opportunities. Before the cached daily file is reopened,
freeze all 48 combinations of prior return `{1,3}` sessions, downside residual
z `{-1.5,-2.0,-2.5}`, SPY trend `{SMA100,SMA200}`, stop
`{1.0,1.5}×ATR14`, and hold `{2,5}` sessions. It consumes no new-family slot
and does not wait for an ISO-week boundary.

V3 committed that exact search and opened the cached common-stock file once,
then failed before the first candidate because SPY was absent. Failure
`f5596e4e0aaca3208541fb3ab1ee4995e5b6bc9c7bfbd735637635622125a081`
records zero returned trials, zero surfaced metrics, no selection, no result,
no confirmation access, and no broker or provider action. V4 bound the complete
SPY reference and reached all 48 trials, but did so before its implementation
hash was frozen. Diagnostic `dfd84e2e...9544f` therefore makes its aggregate
results contaminated engineering evidence only. V5 freezes the same SPY
binding and semantics-preserving runtime caches before formal evaluation. No
grid, date, cost, selection, or confirmation-reserve change is permitted.

Formal V5 result `092e782b...0b2d6` evaluated all 48 trials from one cached
load with zero provider requests. Independent inspection
`c5fba1719ef3c26e2bd9fda0fe06fd4cbd1da58c3d604260adc53ebf95f9e138`
is `REJECTED`: twelve trials passed positive stressed growth and profit factor,
but zero passed stressed drawdown, DSR, Holm, or all-fold stability. The best
DSR was 0.0850 and the smallest drawdown among positive-growth trials was
6.337R. No winner or power target was frozen; all 25 confirmation decisions
remain untouched.

This closes the credible existing-family successor queue. Repeatedly changing
residual parameters after these outcomes would be selection leakage. The
continuous lane now activates the already predeclared dense batch under rolling
authorization `bedfb0ea...5e5585`. The three terminal predecessor families
released all three slots. The immediate transition is the single outcome-blind
calendar collection, now complete as `95b4cba1...213a05`, followed by a frozen
causal allocation contract and exact family-contract freeze. Family account
calendars remain disjoint and chronological. Development is contaminated
training evidence; only the 35 preregistered date/instrument pairs per family
are reserved as untouched confirmation signal opportunities. Intervening
account-calendar dates remain explicit zero-signal days, and lagged warmups may
overlap only as ineligible point-in-time feature input. No literal date wait
remains.

Run `python3 dense_batch_readiness.py status` for the exact machine handoff.
It verifies the current committed plan, calendar contract and inspection,
implementation hashes, rolling authority, credential availability, empty pre-collection output
paths, and global outcome-index hash. It also emits the exact ordered commands
for calendar collection, independent inspection, capacity allocation, contract
freeze, and development-data freeze. Its readiness state never grants target
outcome or broker access.

## Form 4 rolling replacement

The committed rolling-batch-1 status is terminal for its three exact family
IDs and must not be overwritten. Pair-aware allocation removes the artificial
calendar wait but does not revive a rejected version. A released rolling slot
therefore advances the genuinely distinct
`clustered-form4-open-market-purchase-continuation-v1` source-capacity lane.

The lane uses the SEC's official quarterly Insider Transactions Data Sets from
2018-Q1 through 2024-Q4. Before any download it freezes all 28 archive URLs,
the as-filed join keys, and strict event semantics: original Form 4 only,
non-derivative transaction code `P`, acquired common equity, positive shares
and per-share price, direct ownership, officer or director ownership, no
footnote on a core economic field, and at most four calendar days between the
transaction and filing. An affirmative `AFF10B5ONE` value is excluded when the
archive supplies that field. Filing date is the public timestamp surface; no
future strategy may enter before the next complete exchange-session open.

`insider_purchase_capacity.py` writes only compact contracts, inspections, and
telemetry to Git. The quarterly ZIPs and normalized row-level events remain in
the ignored content-addressed store. Capacity inspection accesses no market
price or forward return. At least 100 normalized issuer-filing events admits a
separately frozen development search; 50-99 preserves later single-rule
research; fewer than 50 retires the family for formal capacity. Development,
embargo, confirmation, costs, selection corrections, and exact production
semantics still freeze only after this outcome-blind capacity gate.

Capacity inspection `22b5ca89...ebd720` admitted 29,564 normalized events.
The separate `insider_purchase_discovery.py` transition implements the family
freeze without opening market data. It aggregates filings observable before
the same symbol's next open, applies a frozen $50,000 capacity floor, uses
2018-2022 as development, inserts a five-session year-boundary embargo, and
reserves 2023-2024 confirmation at exact symbol/session-pair granularity through
the maximum exit. Its 32 trials are purchase notional `{50000,250000}` by
distinct reporting owners `{1,2}` by maximum prior 20-session return
`{-5%,0%}` by stop `{1.5,2.0} ATR14` by hold `{3,5}` sessions. Only after the
contract, generic preflight, and search are committed may development prices be
collected; confirmation prices remain forbidden until an exact winner freezes.

## Parallel lanes

The residual-equity implementation is now used only for the existing
cross-sectional-reversal successor described above. The separately predeclared
intraday-ETF opening-reversal family occupies one of the three released rolling
slots only after its exact contract freezes.

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
