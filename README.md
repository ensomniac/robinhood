# Robinhood Codex

A public experiment in agentic stock trading.

This repository tracks a real-time experiment where Codex researches, plans,
executes, monitors, and journals aggressive intraday equity trades through a
Robinhood agentic trading workflow. The goal is to make the process visible:
the strategy, decision rules, trade context, and outcomes are all kept in this
repo so anyone can follow along.

Codex owns this workflow's in-scope outcomes rather than acting as a passive
advisor. Its primary objective is rapid compounding growth, pursued through the
repository's hard risk, evidence, execution, and protection constraints. As a
normal completion step, validated repository changes are committed and pushed
without waiting for routine approval.

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
[STRATEGY_REVIEW.md](STRATEGY_REVIEW.md). The v3 failure analysis and repair map
is in [STRATEGY_AUDIT_2026-07-15.md](STRATEGY_AUDIT_2026-07-15.md). Strategy version
`2026-07-15-orb-v3` is currently `UNVALIDATED` because this repository has no
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
  and executable liquidity. Seventy percent is an aggressive allocation target,
  not a reason to exceed risk or discard a safely sized positive-expectancy setup.
- Start live pilots at a 0.25% planned-risk cap and a 90-point minimum score;
  evidence can promote the cap to 0.50% and then 0.65%.
- Use +2% as a milestone with a structural runner rule, not a daily quota or an
  unconditional fixed take-profit.
- Pause or demote the strategy when local expectancy, drawdown, slippage, data,
  or broker-tool health fails the documented gates.

VWAP pullback and high-of-day continuation setups are research-only until they
earn separate positive out-of-sample evidence. The cited ORB research studied a
diversified long-short portfolio; its reported returns are not expected returns
for this concentrated, long-only implementation.

The numeric rules live in [strategy_config.toml](strategy_config.toml).
`strategy_engine.py` computes every score, hard gate, and sizing result;
`session_guard.py` enforces the live entry/protection interlock; and
`strategy_ledger.py` provides append-only signal records, reproducible metrics,
paired exit comparison, and evidence-earned maturity.

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
- `trades/archived/YYYY_MM_DD/` for terminal ideas, sessions, and trades. Every
  archived context contains a readable outcome review and embedded JSON learning
  record.

After the first recorded session, `SIGNALS.jsonl` is the machine-readable
companion. See [SIGNAL_LEDGER.md](SIGNAL_LEDGER.md) for its privacy-safe schema
and append/audit/report commands.

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

## Session Modes And Executable Controls

Start a new agentic workflow with the numbered mode picker:

```sh
python3 session_mode.py
```

The modes are live trading, current-day shadow trading, historical learning,
strategy review, and the bounded edit/learning loop. Selection itself is
non-mutating. Only live mode permits broker actions, and every normal safety
gate still applies.

Run the engineering learning loop against measured artifacts before collecting
more data:

```sh
python3 session_mode.py --mode learning
python3 learning_loop.py inspect
python3 learning_loop.py review-plan learning_runs/plan.json
```

The versioned [LEARNING_LOOP.md](LEARNING_LOOP.md) prompt inventories existing
changes, identifies ten working controls, proves the bottleneck, filters at most
five improvements, applies one coherent slice, validates it, records the lesson,
and stops. It cannot access the broker or activate production strategy changes.
Local inventories and plans remain under ignored `learning_runs/`.

The early Item 2.02 reversal research has a separate prospective evidence plane.
It cannot reuse the inspected corpus as confirmation and it cannot activate a
strategy:

```sh
# Before requesting any target-session prices for the new 100+ date sample.
python3 historical_strategy_lab.py freeze-confirmation \
  --evidence historical_batches/evidence-<new-independent-sample>.json

# After post-preregistration bundles exist under ignored historical_data/.
python3 historical_strategy_lab.py run-confirmation \
  historical_batches/confirmation_manifests/confirmation-<sha256>.json

# For each prospective current-day no-order observation.
python3 shadow_reversal.py record /path/to/privacy-safe-shadow-capture.json
```

The confirmation runner executes only the frozen early Item 2.02 policy and
reports both R expectancy and structural-stop, risk-sized account geometry. The
shadow recorder recomputes the signal, NBBO/depth, chase cap, protection timing,
and exit lifecycle without exposing an order-action path. See
[HISTORICAL_RESEARCH.md](HISTORICAL_RESEARCH.md) for the exact freeze, collection,
qualification, and stop-without-tuning gates.

The active workflow uses local decision surfaces around authoritative
broker and market tool responses:

```sh
# Evaluate a fully populated candidate payload.
python3 strategy_engine.py /path/to/candidate.json

# Confirm that structured history is valid and compute earned maturity.
python3 strategy_ledger.py audit
python3 strategy_ledger.py report

# Confirm active context is current and every archive has an outcome.
python3 trade_lifecycle.py audit

# Interlock the final broker/session snapshot before entry and after state changes.
python3 session_guard.py /path/to/broker-snapshot.json
```

Only `strategy_engine.py` output with `eligible=true` under the current rules
hash can proceed to broker review. Only `session_guard.py` status `ENTRY_READY`
can proceed to a new live entry. The scripts calculate and guard decisions; they
do not fetch broker data or place orders themselves.

Historical learning uses point-in-time replay bundles and never touches broker
orders:

```sh
python3 historical_learning.py validate /path/to/YYYY-MM-DD.json
python3 historical_learning.py run --selection selection.json --ready-only
```

See [HISTORICAL_LEARNING.md](HISTORICAL_LEARNING.md) for the strict data-fidelity
contract. Once bundles are local, the research-only matrix runner can evaluate
several versioned bar-based strategies in parallel without provider calls or
production-ledger writes:

```sh
python3 historical_research.py run \
  --evidence historical_batches/evidence-2026-07-16-one-hundred-days.json \
  --workers 4
```

It progressively reveals immutable bars, enters only on the next minute, uses a
shared conservative execution model, stores isolated per-strategy shards under
ignored `research_runs/`, and can publish a compact comparison. See
[HISTORICAL_RESEARCH.md](HISTORICAL_RESEARCH.md) for supported strategies,
limitations, plugin requirements, and reproducibility rules. Evidence review is
similarly non-executing:

```sh
python3 strategy_learning.py report
python3 strategy_learning.py propose  # only after both cadence gates pass
```

The read-only Interactive Brokers adapter is the default required market-data
collector for replay candidates through a locally logged-in Trader Workstation:

```sh
python3 ibkr_historical.py check
python3 ibkr_historical.py probe AAPL --date 2026-05-12
python3 ibkr_historical.py candidate AAPL \
  --date 2026-05-12 \
  --evaluation-time 09:40:00 \
  --output historical_data/ibkr/2026-05-12-AAPL.json
```

Use the collector for every candidate in the preselected historical universe.
The adapter has no account, portfolio, or order surface. It collects regular-
session minute bars, the 14-session time-matched opening-volume lookback, daily
bars, and historical top-of-book bid/ask ticks with sizes. TWS socket access uses
the authenticated desktop session and needs no API private key. Historical
scanner-universe capture and point-in-time catalysts still require independent
sources, but those sources are not expected to provide bars or quote/depth data.

Before the final candidate universe is frozen, put a ranked buffer under
`candidate_pool_by_date` and resolve its US-stock contract plus required prior
opening/daily history without observing target-session prices:

```sh
python3 historical_discovery.py ingest-earnings \
  --output historical_data/discovery/earnings.json
python3 historical_discovery.py build selection.json trading-days.json \
  historical_data/discovery/earnings.json \
  --output historical_data/manifests/draft.json \
  --buffer-limit 80 --workers 4
```

The discovery helper joins a frozen market-wide earnings capture to SEC filing
metadata and primary documents, caches source artifacts locally, and screens
strong dilution language. It does not request target-session market prices.

```sh
python3 historical_universe.py \
  historical_data/manifests/draft-YYYY-MM-DD.json \
  --output historical_data/manifests/evidence-YYYY-MM-DD.json \
  --workers 4 --continue-on-exhausted
```

This skips known non-common-stock or dilution-conflicted rows before provider
work, then rejects retired, unresolvable, low-ADV, low-ATR, or history-incomplete
symbols from the draft pool while alternatives remain. Daily gates run before
the more expensive 28-day opening-history request, which fits in one five-minute
provider chunk. Provider-wide permissions and connection failures still stop the
batch, and no candidate may be replaced after the output universe is frozen.
Each examined symbol/date is atomically cached under ignored
`historical_data/preflight/` storage, so an interrupted or repeated preflight
resumes rather than restarting. Cache reuse requires the same strategy/rules
qualification plus matching pre-session content hashes. Accepted opening and
daily bars are reused by bundle collection without observing target-session
prices. Repeated contract proof is separately cached under ignored
`historical_data/contracts/` with request identity and a content hash. Resolved
metadata expires after 30 days by default, error-200 unresolvable results after
one day, and provider-wide failures are never cached. Same-symbol probes
single-flight across workers; `--fresh-contracts` forces a provider refresh.
Bounded workers overlap provider latency but consume results in rank
order; a provider-wide failure prevents the next batch from launching. In large
batches, `--continue-on-exhausted` records a sparse date as blocked and proceeds
without substituting a date or weakening the ten-candidate requirement.

For a pre-frozen multi-day evidence manifest, the resumable bundle builder keeps
successful raw responses under the ignored data directory and validates a full
day before writing a replay bundle:

```sh
python3 historical_bundle_builder.py \
  historical_data/manifests/evidence-YYYY-MM-DD.json \
  --workers 4
```

It reports market-data permissions, missing boundary ticks, and unresolved
historical symbols as fidelity blockers instead of substituting data or changing
the frozen candidate universe. It stops a disconnected request stream
immediately, reconnects once by default, resumes from the raw cache, and writes
an atomic public status under `historical_batches/`. A matching preflight cache
eliminates the bundle collector's duplicate opening-volume and prior-daily
requests; missing or incompatible cache entries safely fall back to normal IBKR
collection. The result reports wall time, provider request telemetry, cache
hits, and preflight reuse. See
[HISTORICAL_THROUGHPUT_PLAN.md](HISTORICAL_THROUGHPUT_PLAN.md) for measured
bottlenecks and the large-batch plan.

New schema-2 replay bundles wait for the first opening-range crossing bar to
complete, reserve the following minute for quote snapshots, and evaluate at that
window's closing boundary. This keeps quote snapshots point-in-time; legacy
schema-1 bundles remain readable without changing their recorded semantics.

IBKR remains primary. If the ignored `.env` contains `MASSIVE_API_KEY`, permanent
IBKR bar/quote gaps can fall back to adjusted Massive SIP aggregates and
historical NBBO quotes. Transport outages never switch providers. Candidate and
benchmark provenance is retained in the final bundle, missing intervals are not
interpolated, and the frozen symbol/date set never changes.

`historical_learning.py run --ready-only` replays every valid date in the
original selection, reports the rest as blocked, skips already archived dates,
and never substitutes. The batch engineering gate is at least 80% validation-
grade yield across 20 random dates with zero substitutions and cascade errors.

In TWS, enable `API > Settings > Enable ActiveX and Socket Clients`, keep
`Read-Only API` and `Allow connections from localhost only` enabled, and make
the socket port match `IBKR_PORT`. The ignored local `.env` on this machine also
enables best-effort TWS startup; an interactive login may still be required.

Proposals never apply themselves. See [STRATEGY_LEARNING.md](STRATEGY_LEARNING.md).

## Progress History

Reusable engineering and operational discoveries live in
[`progress/HISTORY.jsonl`](progress/HISTORY.jsonl), with the contribution
contract in [`progress/README.md`](progress/README.md). Validate and install the
tracked contribution hook with:

```sh
python3 progress_history.py audit
python3 progress_history.py install-hook
```

The hook prompts substantive commits to preserve their breakthrough, failure,
provider constraint, or architecture lesson instead of letting it disappear in
conversation history.

## Publishing Discipline

This project is intended to be auditable. When a trade decision is made, the
agent updates all applicable public surfaces:

- [TRADES.md](TRADES.md), for the public running ledger.
- A matching context file under `trades/`, for detailed reasoning and state.
- `SIGNALS.jsonl`, through `strategy_ledger.py record`, for append-only metrics.

Whenever `TRADES.md`, `SIGNALS.jsonl`, or any file under `trades/` changes, the
full root project must be committed and pushed back to GitHub with a meaningful
commit message.

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
├── LEARNING_LOOP.md   # Versioned bounded edit/learning prompt
├── email_sender.py    # Settings-aware notification CLI and server client
├── historical_learning.py # Point-in-time, no-broker replay engine
├── HISTORICAL_LEARNING.md # Replay data and operator contract
├── historical_research.py # Process-parallel research-only strategy matrix
├── historical_research_strategies.py # Versioned immutable-bar plugins
├── historical_strategy_lab.py # Frozen-policy lab and confirmation manifests
├── learning_loop.py  # Read-only latency inventory and change-plan guard
├── HISTORICAL_RESEARCH.md # Research isolation and plugin contract
├── ibkr_historical.py # Optional read-only TWS historical-data adapter
├── IDENTIFIER_ENCRYPTION.md # Inline identifier encryption and recovery guide
├── README.md          # Public project overview
├── requirements.txt  # Python runtime dependency declaration
├── session_guard.py  # Entry, protection, heartbeat, and flatten interlock
├── session_mode.py   # Explicit live/shadow/historical/review selector
├── shadow_reversal.py # No-order prospective reversal qualification recorder
├── SIGNAL_LEDGER.md  # Structured public signal-record schema and workflow
├── sensitive_data.py # Fast encrypt/decrypt/audit/benchmark CLI
├── settings.toml      # Human-editable operational settings
├── strategy_config.toml # Numeric rules, maturity risk, and promotion gates
├── strategy_engine.py # Deterministic OR_RVOL, gate, score, and sizing engine
├── strategy_ledger.py # Append/audit/report CLI for session and signal data
├── strategy_learning.py # Evidence report and non-applying proposals
├── strategy_maturity.py # Reproducible metrics and evidence-earned maturity
├── STRATEGY_LEARNING.md # Outcome and review-cadence workflow
├── STRATEGY_AUDIT_2026-07-15.md # Ranked failure analysis and v3 repairs
├── STRATEGY_REVIEW.md # Evidence, limitations, sizing example, and rationale
├── tests/             # Strategy, safety, notification, and encryption tests
├── TRADES.md          # Running public trade ledger and balance timeline
├── trade_lifecycle.py # Terminal outcome embedding and daily archiving
├── trades/
│   ├── CONTEXT_TEMPLATE.md # Required live/shadow session record
│   ├── active/        # Active trade/session context
│   └── archived/      # YYYY_MM_DD terminal context and outcome data
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
