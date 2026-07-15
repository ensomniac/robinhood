# Robinhood Codex

A public experiment in agentic stock trading.

This repository tracks a real-time experiment where Codex researches, plans,
executes, monitors, and journals aggressive intraday equity trades through a
Robinhood agentic trading workflow. The goal is to make the process visible:
the strategy, decision rules, trade context, and outcomes are all kept in this
repo so anyone can follow along.

This is not investment advice, a recommendation to trade, or a claim that the
strategy will be profitable. Intraday trading is high risk, margin can amplify
losses, and automated execution can fail in ways that matter financially.

## What This Is

- A public record of an automated day-trading workflow.
- A strategy notebook for refining an agentic trading playbook from evidence.
- A trade journal that records both visible trades and no-trade decisions.
- A transparency layer around what the agent was allowed to do, what it did,
  and why.

## Current Strategy

The active strategy is defined in [AGENTS.md](AGENTS.md). At a high level:

- Long equities only.
- One open trade maximum.
- Regular market hours only.
- No new entries before 9:35 AM ET or after 3:15 PM ET.
- Flat by 3:55 PM ET.
- Target a 2% gross gain on qualified setups.
- Cap planned loss at 0.8% from average fill.
- Use 70-100% of available buying power only when the setup qualifies.
- Prefer catalyst-backed stocks with strong relative volume, tight spreads,
  enough book depth, and clear technical structure.

The current playbook focuses on:

- Five-minute opening range breakouts.
- VWAP pullback continuations.
- High-of-day continuations.

## Public Trade Ledger

The running public ledger lives in [TRADES.md](TRADES.md).

That file is the first place to look for:

- Visible trade decisions.
- Open and closed trade outcomes.
- No-trade decisions when the agent rejects a setup.
- Running account balance snapshots over time.
- Links to detailed context files under `trades/`.

Detailed per-session and per-trade context lives under:

- `trades/active/` for active research, open trades, and in-progress sessions.
- `trades/archived/` for completed sessions and closed trades.

## Publishing Discipline

This project is intended to be auditable. When a trade decision is made, the
agent updates both:

- [TRADES.md](TRADES.md), for the public running ledger.
- A matching context file under `trades/`, for detailed reasoning and state.

Whenever any file under `trades/` changes, the full root project must be
committed and pushed back to GitHub with a meaningful commit message.

Canonical workflow:

```sh
git status --short
git add .
git commit -m "Updated trades: <plain-English summary>"
git push
```

Commit messages should describe what changed, for example:

- `Updated trades: logged no-trade decision for weak VWAP setup`
- `Updated trades: opened AAPL VWAP continuation context`
- `Updated trades: closed TSLA breakout trade with final balance`
- `Updated strategy: tightened liquidity gates`

## Repository Map

```text
.
├── AGENTS.md          # Operating contract for Codex and the trading strategy
├── README.md          # Public project overview
├── TRADES.md          # Running public trade ledger and balance timeline
├── trades/
│   ├── active/        # Active trade/session context
│   └── archived/      # Completed trade/session context
└── LICENSE
```

## Safety And Privacy

This repository is public. Do not commit secrets, access tokens, MFA material,
full account numbers, private personal data, or broker credentials. Account
balances may be published as part of the experiment, but account identifiers
should remain redacted or omitted.

Broker review alerts, margin warnings, halts, liquidity failures, connector
errors, and unresolved login/MFA requirements are treated as blockers unless
Codex can resolve them automatically inside the strategy rules.
