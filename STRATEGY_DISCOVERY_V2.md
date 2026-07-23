# Strategy Discovery V2

This prospective implementation surface extends the immutable authorization in
`PORTFOLIO_VALIDATION_V2.md`; it does not rewrite that hash-bound plan or any v1
or retired-v2 evidence.

## Schema 2 and historical truth

`portfolio_config.toml` schema 2 binds v2 as the active research campaign,
preserves v1 as immutable adverse history, sets the first-pilot target to one,
retains the portfolio target of three, and keeps the three-new-family weekly
ceiling. Schema-1 ledger records remain auditable but cannot satisfy a v2
maturity gate. `portfolio_config_v1.toml` is the byte-identical audit snapshot
used to reconstruct historical v1 artifacts.

For every v2 strategy, the chronological account path is the primary historical
truth surface. It includes every frozen session, including no-signal, rejected,
missed-fill, capital-blocked, and open-position days; sizes entries from the
configured pilot risk; enforces concurrent-position, aggregate-risk,
daily-entry, gross-notional, and cash contention; marks open positions daily;
and compounds equity only through that path. The 5-bps-per-side path is primary
and the 10/20-bps-per-side paths are mandatory stress cases. `R`, win rate, and
stop behavior remain diagnostics and retain their existing gates, but cannot
override failed account growth.

Development, untouched confirmation, and combined history must each have
positive compounded return and log growth. The primary path additionally needs
a positive one-sided 90% stationary-bootstrap lower bound for mean filled-trade
account return and net-dollar profit factor of at least 1.30. Both stress paths
need positive log growth, profit factor of at least 1.20, and account drawdown no
greater than the configured 6R budget. Both chronological halves and performance
without the five best trades must remain positive.

The independently inspected development winner freezes its evidence counts
before confirmation access. Required total signals are `max(50, power_target)`;
required confirmation signals are
`max(20, ceil(required_total_signals * 0.30))`. A five-session embargo remains
mandatory. Insufficient untouched inventory is terminal
`INSUFFICIENT_POWER_CAPACITY`, not permission to extend or substitute dates.

## Manifest-driven controller

`strategy_discovery.py` is the active controller. Its stable commands are:

- `status`
- `preflight <family-contract>`
- `freeze-search <family-contract>`
- `evaluate-development <contract>`
- `inspect-development <result>`
- `freeze-winner <inspected-result>`
- `evaluate-confirmation <winner>`
- `inspect-confirmation <result>`
- `queue-shadow <winner>`

Every transition verifies a content hash and, except for the initial read-only
status, requires its predecessor artifact to be committed and unchanged. The
controller is broker-inert. Preflight rejects outcome-like fields, development
requires every declared trial and explicit zero-return accounting, confirmation
accepts no parameter alternatives or date substitutions, and shadow queueing
requires a committed passing confirmation inspection.

For `selection_mode = "development_search"`, a family may contain at most 64
trials. A trial must first retain positive 20-bps log growth, stressed profit
factor of at least 1.20, drawdown no greater than 6R, positive rolling-fold
stability, complete rules, and complete accounting. It must then satisfy DSR at
0.90, Holm rejection at alpha 0.10, PBO no greater than 0.50, and positive
20-bps growth in at least half of its one-step parameter neighbors. Survivors
rank by 20-bps bootstrap lower mean account return, total log growth, drawdown,
then canonical trial ID. Confirmation never participates in selection.

A passing confirmation inspection emits a content-addressed
`historical-maturity-ledger` artifact. Its schema-2 inspection record binds the
exact strategy version and rules hash back to the committed discovery and
confirmation inspection; its session records preserve the chronological account
path; and its signal records retain net dollars, account-return fraction, `R`,
cost stress, and stop behavior. Only those emitted records may be appended to
`PORTFOLIO_SIGNALS.jsonl` after the artifact itself is committed.

Daily sessions and closed trades are deliberately separate. A filled entry day
marks an opportunity and may carry mark-to-market account return, but it creates
no maturity signal until the position closes. An exit day may close zero, one,
or several overlapping positions; each closed signal retains its original entry
date and exact 5/10/20-bps trade result while the session retains that day's one
portfolio account return.

`dense_strategy_runtime.py` implements all three predeclared families over one
already-frozen in-memory dataset. Daily candidates use completed close data and
enter only at the next session open. Intraday candidates use complete regular-
session SIP minute bars, exact condition-aware cumulative-VWAP numerator and
denominator inputs, completed reclaim bars, and the next minute-bar open. Gaps
through a stop fill at the opening observation, and a bar touching both stop and
target resolves stop-first. Missing next fills, invalid stops, or incomplete
holding bars are recorded without substitution. The shared account simulator
then applies whole-share sizing, overlapping positions, risk, gross-notional,
cash, and daily-entry contention at 5, 10, and 20 bps.

`dense_strategy_plugin.py` keeps row-level data outside Git under
`LOCAL_HISTORICAL_DATA_ROOT`. A committed public manifest binds the external
relative path, compressed-file SHA-256, canonical dataset SHA-256, exact family,
dates, lane, and formal capacity. Development data must also bind the committed
search hash. Confirmation data must be captured after and bind the exact winner
rules hash. Warm trial evaluation loads that frozen dataset once and makes zero
provider requests.

`portfolio_execution.py` is the broker-inert production adapter. It first
rebuilds the candidate through the same frozen strategy plugin and rejects any
implementation-file drift. It then rejects a
rules-hash or live-semantics mismatch, market data older than five seconds,
crossed or excessive spreads, stops at or above the observed bid, gross movement
below five times primary round-trip cost, incomplete before-open reconciliation,
missing GTC overnight protection, and any whole-share size outside risk,
notional, buying-power, depth, or recent-volume capacity. A passing result still
requires `portfolio_guard.py`, broker review, and any broker-required human
confirmation. Its unknown-submission reconciler permits a retry only after the
logical order is confirmed absent or terminal without exposure; active,
duplicate, or unknown broker state fails closed.

The next dense batch is predeclared in the content-addressed
`strategy_tournament/v2/next_batch/plans/` manifest. It preserves the exact
48-trial liquid-equity residual-reversal, 32-trial intraday index-ETF reversal,
and 32-trial liquid-ETF pullback grids. `next_week_discovery_batch.py status`
keeps all family-contract, provider, outcome, and broker permissions closed
until the 2026-W31 reset on 2026-07-27. After that reset it opens only the
disjoint development/embargo/confirmation evidence-freeze step; provider access
still requires committed exact family contracts and inspected predecessors.

`outcome_exposure.py` is the append-only global date/instrument contamination
authority. Its initial hash-bound baseline marks all 226 dates in the preserved
portfolio and legacy strategy ledgers with wildcard-symbol exposure, so none can
silently become v2 confirmation.
Every inspected development or confirmation result adds its exact frozen scope.
An absent, mutated, or stale baseline fails the default audit.

At or after the reset, `dense_capacity_inventory.py` builds the zero-outcome
inventory from the committed full-session calendar and current exposure index.
It requires one contiguous 480-session untouched run, takes the most recent
eligible run, and deterministically divides it into three disjoint blocks. Each
block contains 120 development sessions, five embargo sessions, and 35 reserved
confirmation sessions. It freezes a capacity manifest for every family without
provider, price, outcome, or broker access:

```sh
python3 dense_capacity_inventory.py \
  --as-of 2026-07-27 \
  --created-at 2026-07-27T08:00:00-04:00
```

The allocator fails before the reset, on a calendar or exposure-index error, or
when contamination leaves no complete contiguous run. Its inventory contains
the exact three family IDs, capacity-manifest paths, chronological development,
embargo, and confirmation dates, matching exposure scopes, the current
exposure-index SHA-256, and a self-hash. Pass that content-addressed inventory to:

```sh
python3 dense_family_contracts.py \
  strategy_tournament/v2/next_batch/capacity/w31-capacity-inventory-<sha256>.json \
  --as-of 2026-07-27
```

The command fails before the reset, below 100 formal observations, on any
cross-family pair overlap, on any prior confirmation exposure, or if the index
or inventory drifts. Success writes exactly three immutable family contracts
and updates the next-batch status while provider, outcome, and broker permissions
remain false. Each contract then follows the normal `preflight` and
`freeze-search` transitions before its separately hash-bound development data
may be collected or evaluated.
