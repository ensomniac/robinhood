# Robinhood Codex

A public experiment in agentic stock trading.

This repository tracks a real-time experiment where Codex researches, plans,
executes, monitors, and journals aggressive long-equity and ETF strategies
through a Robinhood agentic trading workflow. The goal is to make the process
visible: strategy rules, selection failures, trade context, and outcomes are all
kept in this repo so anyone can follow along.

Codex owns this workflow's in-scope outcomes rather than acting as a passive
advisor. Its primary objective is rapid compounding growth, pursued through the
repository's hard risk, evidence, execution, and protection constraints. As a
normal completion step, validated repository changes are committed and pushed
without waiting for routine approval.

This is not investment advice, a recommendation to trade, or a claim that any
strategy will be profitable. Active trading is high risk, margin can amplify
losses, and automated execution can fail in ways that matter financially.

## What This Is

- A public record of an automated active-trading workflow.
- A strategy notebook for refining an agentic trading playbook from evidence.
- A trade journal that records both visible trades and no-trade decisions.
- A transparency layer around what the agent was allowed to do, what it did,
  and why.

## Current Portfolio Campaign

The active operating contract is defined in [AGENTS.md](AGENTS.md), with the
multi-strategy objective, evidence gates, risk envelope, and anti-cycle rules in
[PORTFOLIO_VALIDATION.md](PORTFOLIO_VALIDATION.md). The active superseding
research scope and first-pilot milestone are defined in
[PORTFOLIO_VALIDATION_V2.md](PORTFOLIO_VALIDATION_V2.md); the evidence-derived
forward mechanism thesis is kept separately in
[PORTFOLIO_THESIS_V2.md](PORTFOLIO_THESIS_V2.md) so the content-addressed
authorization plan remains immutable. The campaign targets at least three
independent `PILOT_READY` long-equity/ETF strategy versions and starts a
controlled live pilot for each as soon as it qualifies.

Campaign v2 currently has zero `PILOT_READY` strategies. Its first exact variant
was retired at Stage 0, and Schedule 13D activist continuation v1 was retired in
representative development after its positive mean failed bootstrap,
outlier-dependence, and stressed-profit-factor gates. The research thesis now
requires event-driven candidates to identify a continuing post-announcement
demand mechanism; positive disclosure alone is insufficient. The next bounded
family is accelerated-share-repurchase continuation, restricted first to an
outcome-blind capacity test of executed, committed ASR agreements rather than
ordinary repurchase authorizations.

At a high level:

- Long common equities and ETFs only.
- At most three concurrent positions, five new entries per day, and five trading
  days per hold.
- Initial pilots risk at most 0.50% of equity in planned loss per position and
  1.25% across open positions, with daily, weekly, drawdown, notional, liquidity,
  protection, and broker limits layered on top.
- Each version needs 30 robust representative development signals, 20
  separately robust untouched confirmation signals, five clean prospective
  shadows, complete trial accounting, and zero violations.
- Three selected mechanisms must also be distinct, have confirmation daily-return
  correlation below 0.70, and cover at least 60% of shared confirmation dates.
- `PILOT_READY` authorizes a controlled pilot; it is not `LIVE_VALIDATED` and is
  not a profitability guarantee.

The numeric portfolio rules live in
[portfolio_config.toml](portfolio_config.toml). `portfolio_maturity.py` audits
the append-only evidence and computes strategy and portfolio maturity.
`portfolio_validation.py` maintains resumable ignored state and emits one
bounded handoff without contacting providers or brokers. `portfolio_funnel.py`
rebuilds the ordered Stage 0 dispositions and exposes the falsification,
development, and confirmation/shadow lanes. `portfolio_guard.py` is the pure
fail-closed pre-entry gate for an exact `PILOT_READY` version and a fresh
privacy-safe post-entry risk reconciliation.

The first reproducible inventory is in
[`research_results/2026-07-21-portfolio-data-inventory.json`](research_results/2026-07-21-portfolio-data-inventory.json).
Rebuild it with `python3 portfolio_data_inventory.py inspect`. It keeps the
existing 95-date catalyst corpus in its honest falsification-only scope and
shows where new representative or untouched data is still required.

The first ten mechanism variants are preregistered—without outcome, provider,
or broker authorization—in
[`strategy_tournament/manifests/portfolio-stage0-slate-1c4f1cd20c490ea56e36fc0b81c30f1c1eb588d9a9dfb310501028de95b3c50f.json`](strategy_tournament/manifests/portfolio-stage0-slate-1c4f1cd20c490ea56e36fc0b81c30f1c1eb588d9a9dfb310501028de95b3c50f.json).
`portfolio_tournament.py` deterministically rebuilds and inspects that lock
before any individual Stage 0 activation can access outcomes.

The first frozen activation binds 164 exact SPY/QQQ sessions in
[`strategy_tournament/activations/etf-or-momentum-v1-fbea206058e0530a38f89b4b19ccdb71949329960fc6ebbef63ae56966961b03.json`](strategy_tournament/activations/etf-or-momentum-v1-fbea206058e0530a38f89b4b19ccdb71949329960fc6ebbef63ae56966961b03.json).
Its inspected falsification result rejected the exact variant after 97 signals:
expectancy was -0.353R, profit factor 0.465, and total return -34.213R at 5 bps
per side. It cannot contribute development, confirmation, maturity, or live
evidence and will not be repaired on those dates.

The second frozen trial tested a separately preregistered ETF VWAP-reversion
mechanism on the same falsification-only denominator after its exact no-return
input inspection was committed and pushed. The inspected trial failed with 29
signals, -8.388R total, -0.289R expectancy, and 0.601 profit factor, so the
exact rule is retired without tuning. The active queue now begins with equity
gap continuation; `python3 portfolio_funnel.py status` prints all eight
remaining first-wave families in their frozen order.

### Preserved ORB v3 lane

Strategy version `2026-07-15-orb-v3` remains `UNVALIDATED` and available as one
candidate lane. Its evidence, limitations, and worked risk example are in
[STRATEGY_REVIEW.md](STRATEGY_REVIEW.md), and its failure analysis is in
[STRATEGY_AUDIT_2026-07-15.md](STRATEGY_AUDIT_2026-07-15.md). Its frozen
one-position, one-entry, 9:35-10:30 ET catalyst opening-range-breakout contract
has not been weakened or reinterpreted.

VWAP pullback and high-of-day continuation setups are research-only until they
earn separate positive out-of-sample evidence. The cited ORB research studied a
diversified long-short portfolio; its reported returns are not expected returns
for this concentrated, long-only implementation.

The ORB numeric rules live in [strategy_config.toml](strategy_config.toml).
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
The $5 universe gate reads the validated opening-bar open directly; callers do
not supply a second authoritative `opening_price` value.

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

`SIGNALS.jsonl` remains the ORB v3 machine-readable companion. Portfolio
inspection, session, shadow, and live evidence is appended to
`PORTFOLIO_SIGNALS.jsonl` once the first inspected record exists. Both are
privacy-safe and machine-audited; see [SIGNAL_LEDGER.md](SIGNAL_LEDGER.md) for
the legacy signal schema and `portfolio_maturity.py` for the portfolio schema.

The first Stage 0 survivor, equity gap continuation `1.0.0`, now has a frozen
representative 2025 validation contract. It reserves 120 chronological sessions
for development, five for the embargo, and 75 untouched sessions for
confirmation. `equity_gap_continuation_validation.py` enforces the outcome
access order; the freeze itself contributes no maturity evidence.

## Email Notifications

The project can send operational notifications through the existing server mail
sender. Delivery is routed to `ryan@ensomniac.com`. The human-editable policy is
in [settings.toml](settings.toml):

- `off` sends no operational email.
- `trades` sends only placed, materially modified, and completed trade events.
- `verbose` also sends likely-setup, critical safety, and session-summary events.

`trades` is the default. The sender reads the settings file on every CLI call, so
editing the value affects the next notification without a restart.

The project CLIs require Python 3.11 or newer. Install the PyPI-resolvable core
dependencies:

```sh
python3 -m pip install -r requirements.txt
```

IBKR collectors additionally require the official TWS API Python client. The
project currently expects `ibapi` 10.37.2, which is distributed in the TWS API
bundle rather than as that version on PyPI. From the matching extracted bundle,
install `source/pythonclient` into the same interpreter or virtual environment:

```sh
cd /path/to/IBJts/source/pythonclient
python3 -m pip install .
python3 -m pip show ibapi
```

See IBKR's [TWS API installation documentation](https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/)
for the official bundle and platform instructions. Keeping `ibapi` out of
`requirements.txt` is intentional: a nonexistent PyPI pin made every core and
test dependency installation unsatisfiable.

For repository validation, install the official client as above, then the
development dependencies and invoke pytest through the selected interpreter:

```sh
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
python3 -m ruff check .
```

Use `python3 -m pytest`, not a machine-global `pytest` launcher; the module form
cannot silently select a different or stale Python installation.

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

The multi-strategy portfolio campaign persists separately from each finite
learning slice. It coordinates preserved evidence, data inventory, tournament,
development, confirmation, shadow, live-pilot, and portfolio-audit gates without
contacting a provider or broker itself:

```sh
python3 portfolio_validation.py init
python3 portfolio_validation.py status
python3 portfolio_validation.py next
python3 portfolio_validation.py audit

python3 portfolio_funnel.py status
python3 portfolio_funnel.py audit

python3 portfolio_maturity.py audit
python3 portfolio_maturity.py report

# Live workflow only, after writing a fresh privacy-safe snapshot:
python3 portfolio_guard.py /path/to/entry-snapshot.json
```

Its hash-chained private state lives under ignored
`learning_runs/portfolio_validation/`. Waiting for data, market hours, provider
access, required confirmation, subscription, or additional organic sessions is
nonterminal. Only a clean audit can record
`THREE_PILOT_READY_LIVE_STARTED`; see
[PORTFOLIO_VALIDATION.md](PORTFOLIO_VALIDATION.md). The prior single-ORB
campaign and its ignored state remain preserved as `SUPERSEDED_PAUSED`, not
declared successful or failed.

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

The disjoint 100-date expansion is now independently inspected `READY`. It
evaluated 526,587 point-in-time security/date rows, retained 6,024 eligible
rows, and selected 1,987 exact pairs without changing v3. The frozen downstream
join found 1,460 raw SIP trigger tapes; clean-trigger inspection retained 1,267
basic fresh/uncrossed windows and 726 chase-cap passes. Only 14 pairs had a
verified positive recent SEC catalyst, and zero passed every known unchanged-v3
hard gate, so returns remain unread. See
[SCANNER_EXPANSION.md](SCANNER_EXPANSION.md) and
[SCANNER_SELECTED_PAIRS.md](SCANNER_SELECTED_PAIRS.md).

Direct-source recovery then enriched 4,205 secondary article bodies, routed 134
potential primary URLs, captured 121 hashed responses, and measured a current
structural ceiling of 33 selected pairs across HTML and PDF timestamp-shaped
sources. The frozen source-semantics pass independently reconciled all 38 joins
but verified only 3 positive pairs, below its 20-pair capacity gate. Outcomes
remain locked while the campaign follows same-source recovery and, if still
needed, disjoint acquisition under
[CATALYST_SOURCE_SEMANTICS_PLAN.md](CATALYST_SOURCE_SEMANTICS_PLAN.md); no new
strategy variant or outcome access is permitted before sufficient unchanged-v3
capacity is proven. The ordered recovery sequence in
[CATALYST_SOURCE_RECOVERY.md](CATALYST_SOURCE_RECOVERY.md) is now exhausted:
exact cross-pass deduplication still leaves only three positive pairs. The next
outcome-blind acquisition boundary is the exact disjoint 100-session selection
in [DEVELOPMENT_TRANCHE.md](DEVELOPMENT_TRANCHE.md). Its implementation must be
committed before selection freeze, and the resulting manifest must be committed
before any provider access.

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
python3 strategy_learning.py propose  # only after cadence and research-lock gates pass
```

Canonical per-symbol/day observations now live outside the repository at
`LOCAL_HISTORICAL_DATA_ROOT`; see
[HISTORICAL_DATA_STORE.md](HISTORICAL_DATA_STORE.md) for the schema, migration
ledger, fidelity rules, and writer contract. The writer leaves at least
`LOCAL_HISTORICAL_MIN_FREE_GIB` free (20 GiB by default) before every atomic
day-file replacement. Check the store and use the cache-first three-provider
collector with:

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
