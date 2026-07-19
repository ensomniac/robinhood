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
Configuration loading fails closed when session times, spread limits, circuit
breakers, maturity risk/allocation caps, or promotion sample ordering are
internally inconsistent.
The evaluator also distinguishes the 0.08% A+ median-spread limit from the
broader 0.10% operating limit: an `UNVALIDATED` 90-point pilot is rejected when
it cannot actually earn A+ classification.
The same evaluator hard-rejects a planned stop that is not below the observed
bid. Drawdown and consecutive-loss breakers now live in the numeric config and
are consumed by `session_guard.py`, rather than being duplicated as guard
literals.

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
python3 learning_registry.py audit
python3 learning_loop.py init
python3 learning_loop.py inspect
python3 learning_loop.py review-plan learning_runs/plan.json
python3 learning_loop.py start --objective experiment-<registered-id>
python3 learning_loop.py start --objective dataset-<registered-id>
python3 learning_loop.py run --run-id learning-<id> --max-steps 20
```

The versioned [LEARNING_LOOP.md](LEARNING_LOOP.md) prompt governs each bounded
slice. [LEARNING_PROGRAM.md](LEARNING_PROGRAM.md) and the append-only `learning/`
registries persist datasets, experiment families, failures, and three-axis
strategy evidence across invocations. The controller performs finite,
resumable experiment or dataset state transitions and stops when Codex judgment or new evidence is
required. It cannot access the broker or activate production strategy changes.
Operational state remains under ignored `learning_runs/`.

Research contracts must materialize every requested trading day, including
missed, rejected, and no-signal days as explicit zero account returns. Evaluate
account compounding and the complete registered search family with:

```sh
python3 learning_statistics.py learning_runs/<run-id>/statistics-input.json \
  --output learning_runs/<run-id>/statistics-result.json
```

The report includes log growth, compounded return, account drawdown,
stationary-block bootstrap uncertainty, Deflated Sharpe, PBO, Holm family
decisions, and a power-derived sample target. It does not silently remove a
signal when adverse cost assumptions turn its fill into a miss.

Audit point-in-time security identity and dataset claims with:

```sh
python3 learning_data.py audit
python3 learning_data.py resolve OLD_SYMBOL --as-of 2021-06-01
python3 learning_data.py freeze-dataset learning_runs/<run-id>/dataset-input.json
```

A `production_scanner_replay` must reconstruct the dynamic complete universe at
09:35 ET from information available by then and bind the exact scanner rules and
security-master hashes. The existing catalyst-selected bundles are registered
as `catalyst_falsification`; they cannot support a full production-policy claim.
The security master is intentionally empty until sourced records are added—an
unresolved historical instrument remains a blocker instead of being replaced
with a symbol that happens to resolve today.

Invent and preregister strategies through a bounded contract:

```sh
cp learning/HYPOTHESIS_TEMPLATE.json learning_runs/<run-id>/hypothesis.json
python3 learning_experiment.py freeze learning_runs/<run-id>/hypothesis.json
python3 learning_experiment.py rolling-plan learning_runs/<run-id>/dates.json
python3 learning_experiment.py validate-result \
  learning/hypotheses/experiment-<id>-<sha256>.json \
  learning_runs/<run-id>/evaluation.json
```

The engine allows at most three new mechanisms per ISO week and at most 256
predeclared trials per family. Every trial must appear in the result. A locked
primary trial must pass geometric growth, stationary-bootstrap, profit-factor,
drawdown, Deflated-Sharpe, PBO, Holm-family, and rolling-origin gates before
development can queue independent confirmation. A failed confirmation can only
close; it cannot return to threshold tuning.

Inspect strategy readiness and compare challengers on paired requested days:

```sh
python3 learning_strategy.py audit
python3 learning_strategy.py report
python3 learning_strategy.py compare learning_runs/<run-id>/paired-days.json
python3 learning_strategy.py health learning_runs/<run-id>/health.json
```

Alpha (`UNTESTED` through `RETIRED`), execution (`UNVERIFIED` through
`LIVE_CALIBRATED`), and operations (`READY`, `PAUSED`, or `KILL_SWITCH`) advance
independently. Paired comparisons retain zero-return days and use time-uniform
confidence bounds over geometric log-return differences. Integrity, rule, or
protection failures pause immediately; statistical degradation is reported but
never changes production rules automatically. `strategy_ledger.py report`
remains authoritative for production maturity.

Run the finite persistent cadence after the close:

```sh
python3 learning_cadence.py status
python3 learning_cadence.py run --max-tasks 5
```

The cadence completes safe deterministic audits and returns `needs_agent` for
provider collection or research judgment. See
[LEARNING_CADENCE.md](LEARNING_CADENCE.md). The repository does not install an
external scheduler automatically.

The early Item 2.02 reversal research reached a terminal negative result. Its
frozen confirmation machinery remains for audit and reproducibility, but the
hypothesis-specific prospective shadow path was removed and must not be run or
retuned:

```sh
# Historical reproduction only; do not freeze a replacement sample to tune the failure.
python3 historical_strategy_lab.py freeze-confirmation \
  --evidence historical_batches/evidence-<new-independent-sample>.json

# After post-preregistration bundles exist under ignored historical_data/.
python3 historical_strategy_lab.py run-confirmation \
  historical_batches/confirmation_manifests/confirmation-<sha256>.json
```

The confirmation runner executes only the frozen early Item 2.02 policy and
reports both R expectancy and structural-stop, risk-sized account geometry. Its
22 primary trades lost 10.056R with -0.457R mean expectancy, 0.370 profit factor,
and 10.492R maximum drawdown; every target and cost-stress cell was negative.
The result is retained as falsification evidence, not a workflow to continue. See
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

The first full-universe 09:35 scanner replay uses a separate, pre-price frozen
pipeline. Its sourced security master is populated, and v4 is now independently
inspected `READY`: 118 Alpaca raw SIP sessions produced all 20 dynamic ranks and
389 exact selected date-symbol pairs without S3. Provider observations live in
the external symbol-first canonical store; only a private hash-attested full-
universe index is derived for fast replay. See
[SCANNER_REPLAY.md](SCANNER_REPLAY.md) for the exact immutable manifest, claim
boundary, failed-contract lineage, current status, and reproduction commands.
The fidelity lock is satisfied. The selected-candidate join is inspected
`READY` for development plumbing: all 389 selected
pairs have candidate bars and news contexts, all 40 benchmark sessions are
present, and 325 crossing windows have bounded raw trade/quote tape. Execution
attrition is substantial. A follow-on independently inspected dataset mapped
all pairs to dated CIKs, retained 100 SEC primary documents, and froze clean SIP
condition semantics. It rejected 197 first raw crosses and reduced chase-cap
passes from 177 to 155. The next inspected layer classified the 100 SEC complete
submissions with exact prior-close recency: only 12 of 389 pairs have a verified
material recent primary catalyst, only one has verified positive direction, and
six are conflict rejects. It also joined all 325 clean-trigger windows to 966
official Nasdaq historical halt rows with no overlaps. This closed historical
halt state, but did not yet close broker-specific tradability, resistance,
structural-stop, benchmark-strength, or conservative-liquidity inputs. V3
remained unchanged. An outcome-blind readiness join subsequently found zero
resolved hard-gate survivors: the only verified positive-catalyst pair failed
spread and chase. It also corrected the engine-compatible chase count to 101
and found zero pairs passing every measured non-catalyst proxy. Returns remain
unread and v3 remains frozen. A final input-semantics layer rebuilt all 325
crossing-minute provider bars from 365,379 raw trades: every OHLCV/count field
matched exactly, every WAP matched within one microdollar, and zero trade
conditions were unsupported. Because the WAP-eligible denominator differed
from reported volume in all 325 minutes, exact trigger-time VWAP now uses the
condition-aware raw-trade prefix rather than a completed-bar approximation. See
[SELECTED_CANDIDATE_JOIN.md](SELECTED_CANDIDATE_JOIN.md) and
[SELECTED_CANDIDATE_FIDELITY.md](SELECTED_CANDIDATE_FIDELITY.md), then
[CHAMPION_INPUT_FIDELITY.md](CHAMPION_INPUT_FIDELITY.md) and
[CHAMPION_INPUT_READINESS.md](CHAMPION_INPUT_READINESS.md), and
[SIP_BAR_AGGREGATION.md](SIP_BAR_AGGREGATION.md).
The next frozen input layer in [PREENTRY_STRUCTURE.md](PREENTRY_STRUCTURE.md)
defines point-in-time stop noise and split-aware resistance without reading
outcomes. It independently reconstructed all 325 terminal trigger records; 153
of 255 derivable stops fit the unchanged 0.8% cap and 89 pass both unchanged
stop and resistance geometry. Seventy records remain explicitly unresolved.
This closes an input-definition gap, not alpha validation, and v3 remains
unchanged.

The next campaign is now executable and in progress. It freezes 100 previously
uninspected H1-2026 scanner dates with zero overlap against v4 and no date
substitution. Its 133 required sessions reuse 113 compatible, hash-attested v4
indexes plus exact symbol deltas and collect 20 new full-universe sessions. The
campaign remains scanner-only until all rankings are independently inspected;
direct catalyst, trigger, and outcome contracts are frozen afterward, and v3 is
evaluated unchanged before any single revision can be proposed. See
[SCANNER_EXPANSION.md](SCANNER_EXPANSION.md).

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

Canonical per-symbol/day observations now live outside the repository at
`LOCAL_HISTORICAL_DATA_ROOT`; see
[HISTORICAL_DATA_STORE.md](HISTORICAL_DATA_STORE.md) for the schema, migration
ledger, fidelity rules, and writer contract. Check the store and use the
cache-first three-provider collector with:

```sh
python3 historical_data_cli.py check
python3 historical_data_cli.py check-providers --symbol AAPL --date 2026-07-17
python3 historical_data_cli.py fetch AAPL --date 2026-07-17
```

The read-only Interactive Brokers adapter remains the first live market-data
provider for replay candidates through a locally logged-in Trader Workstation:

```sh
python3 ibkr_historical.py check
python3 ibkr_historical.py probe AAPL --date 2026-05-12
python3 ibkr_historical.py candidate AAPL \
  --date 2026-05-12 \
  --evaluation-time 09:40:00 \
  --output historical_data/ibkr/2026-05-12-AAPL.json
```

These low-level diagnostics also record every successful bar/quote response in
`LOCAL_HISTORICAL_DATA_ROOT`. The optional `--output` file is resumable candidate
evidence, not the canonical observation copy.

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

The command-line builder checks the canonical cache and then uses IBKR, Massive,
and Alpaca in that order. Timeouts, rate limits, permission failures, and
permanent fidelity gaps may advance to the next configured provider, but a
candidate is recollected as a whole; incompatible feeds are never spliced.
Candidate and benchmark provenance is retained in the final bundle, missing
intervals are not interpolated, and the frozen symbol/date set never changes.
Successful provider responses are written to the canonical external store while
ignored raw evidence shards and assembled replay bundles remain available for
exact workflow resume.

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
├── historical_data_cli.py # Cache-first canonical history operator CLI
├── historical_service.py # Local/IBKR/Massive/Alpaca fallback and recording
├── historical_store.py # External symbol/year/day canonical store
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
├── scanner_replay_alpaca.py # Frozen non-S3 full-universe source collector
├── scanner_replay_inspection.py # Independent scanner/canonical-data verifier
├── SCANNER_REPLAY.md # Dynamic 09:35 selection contract and runbook
├── CHAMPION_INPUT_FIDELITY.md # SEC catalyst and official halt contract
├── CHAMPION_INPUT_READINESS.md # Outcome-blind unchanged-v3 readiness
├── selected_candidate_join.py # Frozen selected-pair bar/news/tape join
├── SELECTED_CANDIDATE_JOIN.md # Join contract, findings, blockers, and runbook
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
