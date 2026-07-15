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
- After any `trades/` file changes, commit and push the root project.

## Account Balance Timeline

| Date (ET) | Time (ET) | Event | Account Balance | Notes |
| --- | --- | --- | ---: | --- |
| 2026-07-15 | 17:20 | Ledger created | TBD | Initial public ledger setup. |

## Trade And Decision Log

| Date (ET) | Time (ET) | Symbol | Decision / Action | Setup | Entry | Exit / Stop / Target | Result | Account Balance | Context |
| --- | --- | --- | --- | --- | ---: | --- | ---: | ---: | --- |
| 2026-07-15 | 17:20 | - | Ledger created | Project setup | - | - | - | TBD | - |

## Open Trades

No open trades are recorded in this ledger yet.

## Closed Trades

No closed trades are recorded in this ledger yet.
