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
