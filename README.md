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

The active operating contract is defined in [AGENTS.md](AGENTS.md), with the
evidence, limitations, and worked risk example in
[STRATEGY_REVIEW.md](STRATEGY_REVIEW.md). Strategy version
`2026-07-15-orb-v2` is currently `UNVALIDATED` because this repository has no
completed local trade sample.

At a high level:

- Long equities only.
- One filled trade per day and one open trade maximum.
- Regular market hours only.
- Production entries only from 9:35-10:30 AM ET; flat by 3:50 PM ET.
- One production setup: a long five-minute opening range breakout in a
  catalyst-backed Stock in Play.
- Compute opening relative volume from the current 9:30-9:35 bar versus the same
  interval over the prior 14 sessions.
- Size from account risk, stop distance, a stop-slippage reserve, buying power,
  and executable liquidity. The resulting notional must still be 70-100% to
  qualify.
- Use +2% as a milestone with a structural runner rule, not a daily quota or an
  unconditional fixed take-profit.
- Pause or demote the strategy when local expectancy, drawdown, slippage, data,
  or broker-tool health fails the documented gates.

VWAP pullback and high-of-day continuation setups are research-only until they
earn separate positive out-of-sample evidence. The cited ORB research studied a
diversified long-short portfolio; its reported returns are not expected returns
for this concentrated, long-only implementation.

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

## Email Notifications

The project can send operational notifications through the existing server mail
sender. Delivery is routed to `ryan@ensomniac.com`. The human-editable policy is
in [settings.toml](settings.toml):

- `off` sends no operational email.
- `trades` sends only placed, materially modified, and completed trade events.
- `verbose` also sends likely-setup, critical safety, and session-summary events.

`trades` is the default. The sender reads the settings file on every CLI call, so
editing the value affects the next notification without a restart.

The project CLIs require Python 3.11 or newer and the declared dependencies:

```sh
python3 -m pip install -r requirements.txt
```

Validate configuration without sending:

```sh
python3 email_sender.py check
```

Send a policy-filtered notification:

```sh
python3 email_sender.py notify trade_completed \
  --subject "AAPL trade completed" \
  --body "Position confirmed flat" \
  --data-json '{"net_r":1.4,"exit_reason":"structural target"}'
```

Run an explicit delivery test:

```sh
python3 email_sender.py test
```

The test command deliberately bypasses verbosity and is only for diagnostics.
Operational mail is best effort and never takes priority over protective orders,
position reconciliation, or ledger updates. The broker and repository remain the
authoritative state if email is delayed or fails.

## Encrypted Trade Identifiers

Exact order IDs, client reference UUIDs, and confirmation/cancellation/replacement
IDs are stored directly in detailed trade context as authenticated
`enc:fernet:vN:...` ciphertext. There is no separate private trade-state store.
The only private file is the ignored `.env` key ring.

Setup and verify:

```sh
python3 -m pip install -r requirements.txt
python3 sensitive_data.py init-key  # first setup only
python3 sensitive_data.py check
python3 sensitive_data.py audit
```

The current machine already has its initial mode-0600 key. Do not run `init-key`
again or replace `.env`; losing that key makes historical ciphertext
unrecoverable. See [IDENTIFIER_ENCRYPTION.md](IDENTIFIER_ENCRYPTION.md) for
encryption, decryption, rotation, auditing, recovery, and performance instructions.

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

- `Updated trades: logged no-trade decision for weak ORB setup`
- `Updated trades: opened AAPL Stock-in-Play ORB context`
- `Updated trades: closed TSLA ORB trade with final balance`
- `Updated strategy: tightened liquidity gates`

## Repository Map

```text
.
├── .env.example       # Key variable names only; the real .env is ignored
├── AGENTS.md          # Operating contract for Codex and the trading strategy
├── email_sender.py    # Settings-aware notification CLI and server client
├── IDENTIFIER_ENCRYPTION.md # Inline identifier encryption and recovery guide
├── README.md          # Public project overview
├── requirements.txt  # Python runtime dependency declaration
├── sensitive_data.py # Fast encrypt/decrypt/audit/benchmark CLI
├── settings.toml      # Human-editable operational settings
├── STRATEGY_REVIEW.md # Evidence, limitations, sizing example, and rationale
├── tests/             # Isolated notification and encryption unit tests
├── TRADES.md          # Running public trade ledger and balance timeline
├── trades/
│   ├── CONTEXT_TEMPLATE.md # Required live/shadow session record
│   ├── active/        # Active trade/session context
│   └── archived/      # Completed trade/session context
└── LICENSE
```

## Safety And Privacy

This repository is public. Do not commit secrets, access tokens, MFA material,
plaintext account numbers, private personal data, broker credentials, or
plaintext broker identifiers. Account balances may be published as part of the
experiment, but account identifiers should remain redacted or omitted. Encrypted
order/ref/confirmation tokens are permitted only in detailed trade context.

Broker review alerts, margin warnings, halts, liquidity failures, connector
errors, and unresolved login/MFA requirements are treated as blockers unless
Codex can resolve them automatically inside the strategy rules.
