# Public Trade Ledger

This file is the public running ledger for the Robinhood Codex experiment. It
tracks visible trade decisions, outcomes, and account balance snapshots over
time. Detailed context belongs in `trades/active/` while a session or trade is
live, then moves to `trades/archived/` after it is complete.

This ledger is not investment advice. It is a public audit trail for an
automated trading experiment.

## Ledger Rules

- Record every visible trading decision: entries, exits, stop changes, rejected
  setups, no-trade decisions, daily stops, and force-flat actions.
- Pair every ledger update with a detailed context update under `trades/`.
- Use Eastern Time for timestamps.
- Omit secrets, full account numbers, login details, and MFA material.
- Record account balance snapshots when available.
- Record strategy version, maturity, live/shadow mode, net P/L after fees, and
  net R when applicable.
- Treat this ledger and confirmed broker state as authoritative. Email is a
  secondary notification channel and never replaces a ledger entry.
- Keep all exact broker identifiers out of this ledger. Encrypted exact values
  belong only in the matching detailed context under `trades/`.
- After any `trades/` file changes, commit and push the root project.

## Strategy State

| Version | Maturity | Closed Frozen-Rule Signals | Live Allocation | Planned Risk Cap | Last Review |
| --- | --- | ---: | --- | --- | --- |
| `2026-07-15-orb-v3` | UNVALIDATED | 0 | 70% target; 80% cap; safely sized shortfall allowed | 0.25% of equity including stop-slippage reserve | 2026-07-15 |
| `2026-07-15-orb-v2` | RETIRED before use | 0 | 70-80% of buying power | 0.50% of equity including stop-slippage reserve | 2026-07-15 |

## Account Balance Timeline

| Date (ET) | Time (ET) | Event | Account Balance | Notes |
| --- | --- | --- | ---: | --- |
| 2026-07-15 | 17:20 | Ledger created | TBD | Initial public ledger setup. |

## Trade And Decision Log

| Date (ET) | Time (ET) | Symbol | Version / Mode | Decision / Action | Setup | Entry | Exit / Stop / Milestone | Net Result | Net R | Account Balance | Context |
| --- | --- | --- | --- | --- | --- | ---: | --- | ---: | ---: | ---: | --- |
| 2026-07-15 | 17:20 | - | Initial / setup | Ledger created | Project setup | - | - | - | - | TBD | - |
| 2026-03-12 | 10:30 replay | - | `2026-07-15-orb-v3` / historical shadow | No trade; 0/10 candidates eligible | Five-minute ORB | - | - | $0 simulated | - | Synthetic only | [`2026-03-12-session`](trades/archived/2026_03_12/2026-03-12-session.md) |
| 2026-04-23 | 10:30 replay | - | `2026-07-15-orb-v3` / historical shadow | No trade; 0/10 candidates eligible | Five-minute ORB | - | - | $0 simulated | - | Synthetic only | [`2026-04-23-session`](trades/archived/2026_04_23/2026-04-23-session.md) |

## Open Trades

No open trades are recorded in this ledger yet.

## Closed Trades

No closed trades are recorded in this ledger yet.

## Historical Replay Coverage

The seed-frozen ten-date batch completed two validation-grade no-trade sessions
and retained eight provider-fidelity blockers without substituting dates or
symbols. See [`historical_batches/2026-07-15-ten-days.json`](historical_batches/2026-07-15-ten-days.json)
for the machine-readable collection and replay status.
