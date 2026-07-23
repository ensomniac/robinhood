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
The legacy independent-trade bootstrap remains reportable diagnostics but is not
an active schema-2 promotion gate; its one-sided stationary account-return
replacement is the confidence authority. Schema-1 maturity semantics are
unchanged.

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
Inspection checks both the confirmation floor and whether observed development
fills plus every reserved confirmation session can reach the frozen total target.
The complete 5/10/20-bps daily path, closed-trade returns, and dollar P/L are
independently reconstructed from the per-date maturity rows, and the 20-bps values
must exactly match the selection arrays. The selected trial must also clear the
account-growth, concentration, profit-factor, drawdown, and stationary-bootstrap
gates before it can be frozen or access confirmation.

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
- `admit-historical <historical-maturity-ledger>`
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

Confirmation inspection independently requires one maturity row for every frozen
confirmation date and reconciles all 5/10/20-bps daily returns, filled-trade
returns, and dollar P/L arrays to those rows. A calendar, closed-signal denominator,
or cost-scenario mismatch invalidates the result instead of allowing a different
evidence surface into the maturity ledger.

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

The live adapter does not accept a caller's claimed rank. For equity reversal it
rebuilds the complete active-common-stock denominator, prior-close and 20/60-day
liquidity gates, unique listing identities, top 250, residual z-score, trend,
and ATR. For ETF pullback it requires every frozen symbol and complete calendar
history before rebuilding SMA, RSI2, decline, and ATR. For intraday reversal it
requires 60 complete 390-minute SIP histories plus the synchronized current
partial session, and accepts only a first reclaim on the latest completed bar.
The next-session/open or next-minute quote must match the rebuilt signal. Stop,
target, maximum hold, GTC/day protection, and stop-first exit semantics are then
derived from the exact winner rather than supplied as discretionary live facts.

`dense_strategy_plugin.py` keeps row-level data outside Git under
`LOCAL_HISTORICAL_DATA_ROOT`. A committed public manifest binds the external
relative path, compressed-file SHA-256, canonical dataset SHA-256, exact family,
dates, lane, and formal capacity. Development data must also bind the committed
search hash. Confirmation data must be captured after and bind the exact winner
rules hash. Warm trial evaluation loads that frozen dataset once and makes zero
provider requests. Parameter-invariant residual-return, rolling-z, and ATR
features are shared across the 48 equity trials; a real 250-symbol, 325-session
fixture enforces the local 60-second acceptance and checks cached/uncached parity.

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
The prior 2023-start calendar cannot supply disjoint 60/200-session warmups, so
the extended public-session calendar has its own zero-price collection contract.
That contract and its outcome-blind inspection may be frozen before the reset;
the single Alpaca calendar request remains closed until 2026-07-27:

```sh
python3 dense_session_calendar.py freeze \
  --created-at 2026-07-22T23:59:59-04:00
python3 dense_session_calendar.py inspect-contract \
  strategy_tournament/v2/calendar/contract/dense-session-calendar-contract-<sha256>.json \
  --inspected-at 2026-07-22T23:59:59-04:00
```

After the reset, collect and independently inspect that calendar before running
the allocator. The allocator requires one contiguous 940-session untouched run:
200 equity warmup plus 160 evidence sessions, 60 intraday warmup plus 160
evidence sessions, and 200 ETF-pullback warmup plus 160 evidence sessions. Each
evidence block contains 120 development sessions, five embargo sessions, and 35
reserved confirmation sessions. It freezes a capacity manifest for every family
without price, return, or broker access:

```sh
python3 dense_session_calendar.py collect \
  strategy_tournament/v2/calendar/contract/dense-session-calendar-contract-<sha256>.json \
  --as-of 2026-07-27 \
  --collected-at 2026-07-27T08:00:00-04:00
# Commit the collection status, then independently inspect its ignored rows.
python3 dense_session_calendar_inspection.py \
  strategy_tournament/v2/calendar/collection/dense-session-calendar-collection-<sha256>.json \
  --inspected-at 2026-07-27T08:05:00-04:00
# Commit the inspection before allocating any evidence dates.
python3 dense_capacity_inventory.py \
  --as-of 2026-07-27 \
  --created-at 2026-07-27T08:10:00-04:00
# Commit the inventory and its three capacity manifests before contract freeze.
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

`dense_data_collection.py` derives the exact request plan only from a committed
frozen search (or, later, a committed exact winner). Daily families use raw
Massive grouped SIP bars plus split actions frozen through the dataset end;
equity membership adds dated active-common-stock reference snapshots. The
intraday family uses complete Alpaca SIP minute bars. Task checkpoints and
cumulative telemetry live outside Git under the configured historical store;
completed plans are idempotent and make zero repeated provider requests.
Retryable transport, 429, and 5xx failures receive at most five attempts for the
same exact task with bounded exponential pacing and any larger numeric
`Retry-After` delay. Every failed attempt and wait is persisted before another
request; permanent fidelity and permission failures still stop immediately.

```sh
python3 dense_data_collection.py --as-of 2026-07-27 \
  freeze-development path/to/committed-search.json
# Commit the generated collection plan before the next command.
python3 dense_data_collection.py --as-of 2026-07-27 \
  collect path/to/committed-collection-plan.json
# Commit the collection status before independent inspection.
python3 dense_data_collection_inspection.py \
  path/to/committed-collection-status.json \
  --inspected-at 2026-07-27T12:00:00-04:00
```

Independent inspection rebuilds the runtime dataset from every checkpoint,
rehashes the ignored gzip payload, revalidates full point-in-time scope, and
only then freezes the Git-visible development or confirmation dataset manifest.
Commit that manifest before evaluation. The complete development transition for
each family is then:

```sh
python3 strategy_discovery.py preflight path/to/committed-family-contract.json
# Commit after every successful transition below.
python3 strategy_discovery.py freeze-search path/to/committed-family-contract.json
python3 dense_data_collection.py --as-of 2026-07-27 \
  freeze-development path/to/committed-search.json
python3 dense_data_collection.py --as-of 2026-07-27 \
  collect path/to/committed-development-collection-plan.json
python3 dense_data_collection_inspection.py \
  path/to/committed-development-collection-status.json \
  --inspected-at 2026-07-27T12:00:00-04:00
python3 strategy_discovery.py evaluate-development path/to/committed-search.json
python3 strategy_discovery.py inspect-development path/to/committed-development-result.json
```

If inspection selects a winner, freeze and commit it before any confirmation
request. Confirmation planning copies the winner preregistration timestamp and
rules hash; independent inspection attests that capture occurred afterward:

```sh
python3 strategy_discovery.py freeze-winner path/to/committed-development-inspection.json
python3 dense_data_collection.py --as-of 2026-07-27 \
  freeze-confirmation path/to/committed-winner.json
python3 dense_data_collection.py --as-of 2026-07-27 \
  collect path/to/committed-confirmation-collection-plan.json
python3 dense_data_collection_inspection.py \
  path/to/committed-confirmation-collection-status.json \
  --inspected-at 2026-07-27T16:00:00-04:00
python3 strategy_discovery.py evaluate-confirmation path/to/committed-winner.json
python3 strategy_discovery.py inspect-confirmation path/to/committed-confirmation-result.json
# Commit the passing inspection and emitted historical-maturity-ledger first.
python3 strategy_discovery.py admit-historical path/to/committed-historical-maturity-ledger.json
# Commit PORTFOLIO_SIGNALS.jsonl before queueing the exact winner.
python3 strategy_discovery.py queue-shadow path/to/committed-winner.json
```

After an exact winner passes committed untouched confirmation,
`admit-historical` validates the content-addressed ledger and passing confirmation
binding, then admits its complete record set to `PORTFOLIO_SIGNALS.jsonl` in one
atomic idempotent transaction. Any existing identity with different evidence
rejects the whole batch. Shadow queueing requires those exact admitted records and
their ledger commit, so a strategy cannot collect qualifying shadows while its
historical evidence exists only in a side artifact.

After admission,
`strategy_discovery.py queue-shadow` freezes its five-shadow queue and exact
winner binding. Each prospective signal then uses a committed admission chain:

```sh
python3 portfolio_shadow.py start path/to/committed-shadow-queue.json \
  path/to/privacy-safe-fresh-setup.json
# Commit the entry artifact. After the simulated position closes or expires:
python3 portfolio_shadow.py close path/to/committed-shadow-entry.json \
  path/to/privacy-safe-closure.json
# Commit the final, replay it independently, commit that inspection, then admit:
python3 portfolio_shadow_inspection.py inspect \
  path/to/committed-shadow-final.json
python3 portfolio_shadow_inspection.py admit \
  path/to/committed-shadow-inspection.json
```

The runner imports no broker connector and hard-codes zero broker actions.
Entry and exit prices come from fresh ask and bid observations respectively;
partial fills, missed limits, protection timing, 5/10/20-bps costs, monitoring,
and journaling are explicit. Missed limits remain nonqualifying observations.
Any capture or rule violation resets the clean consecutive-shadow streak for
that exact version; five new clean closed fills are required afterward.

Once `portfolio_maturity.py report` awards the exact version `PILOT_READY`, the
controlled live path is explicit and fail closed:

```sh
python3 portfolio_live.py prepare path/to/committed-winner.json \
  path/to/fresh-live-setup.json
python3 portfolio_live.py record-entry path/to/live-preparation.json \
  path/to/encrypted-entry-observation.json
# On any unknown transport outcome, query orders and reconcile before retrying:
python3 portfolio_live.py reconcile-unknown-entry path/to/live-entry.json \
  path/to/privacy-safe-order-reconciliation.json
python3 portfolio_live.py record-protection path/to/reconciled-exposure.json \
  path/to/encrypted-protection-observation.json
# If protection submission is unknown, query/reconcile orders before recording
# its definitive protected or flatten-required state; never retry first.
python3 portfolio_live.py close path/to/protection-result.json \
  path/to/encrypted-flat-close.json
python3 portfolio_live_inspection.py inspect path/to/live-final.json
python3 portfolio_live_inspection.py admit path/to/live-inspection.json
```

Preparation repeats encryption, lifecycle, strategy-ledger, portfolio-ledger,
clean-worktree, exact-production, broker-review, confirmation, flat-account, and
`ENTRY_READY` checks. Later transitions authenticate encrypted field-bound IDs,
never retry an unknown or active logical order, require a partial-entry remainder
to become terminal, and force flattening when protection fails. Only an
independently replayed close with a flat reconciled account, terminal residual
orders, complete monitoring/journal capture, and recorded realized dollars,
account return, R, slippage, protection timing, and notification status may enter
`PORTFOLIO_SIGNALS.jsonl`. Protection-failure safety closes remain auditable but
cannot earn the live-started milestone. Terminal reconciliation must include the
authenticated partial-entry remainder and any separate protective order; when a
protective stop executes, that same authenticated order must be the recorded exit.

The final lifecycle and every bound preparation, exposure, and protection artifact
must be committed before independent live inspection. Admission then requires the
inspection and existing portfolio ledger to be committed and rechecks that the
exact strategy ID, version, and rules hash remain `PILOT_READY` immediately before
the reconciled close is appended. Caller-supplied maturity reports are rejected
outside the explicit no-commit, no-repository-check test path.
