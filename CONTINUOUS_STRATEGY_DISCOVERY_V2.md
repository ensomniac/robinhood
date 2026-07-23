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

## Immediate lane

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
- Evidence ending before 2022, chronologically and outcome-disjoint from the
  retired predecessor.
- All existing account-growth, 5/10/20-bps, stationary-bootstrap, DSR, Holm,
  PBO, neighbor-stability, power, profit-factor, drawdown, concentration, and
  execution gates.

The prior family attempt remains explicit in the contract. This successor does
not consume a new-mechanism-family slot.

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

The daily fixed-ETF collection plane uses four frozen symbol-range requests plus
split metadata, then loads the resulting dataset once for all trials. It does
not issue one provider request per date.

## Parallel lanes

The predeclared residual-equity and intraday-ETF families remain subject to the
new-family weekly budget. Their calendar gate is independent of this successor
lane and cannot block it.

The short-horizon oversold-reversal family remains the next outcome-blind
capacity lane because its retired v1 produced only ten signals despite positive
5/10/20-bps diagnostics. It cannot access new target returns until a separate
capacity expansion and exact successor contract are frozen and inspected.

## Completion

This lane is research only until the normal evidence chain earns
`PILOT_READY`. The campaign remains incomplete until one exact strategy also
completes five clean prospective shadows, passes its production path, closes one
controlled live trade, reconciles flat with no residual orders, passes every
audit, and is committed and pushed.
