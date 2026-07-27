# Strategy Lab

Strategy Lab is the repository's primary strategy-discovery and observability
path. It turns the existing immutable local historical store into a compact
feature mart, tests 50–500 new declarative configurations after each market
close, and promotes very few candidates through frozen development, locked
holdout, prospective paper, and controlled pilot stages.

It is a research system, not a promise of profitable strategies. A day with
zero survivors is a successful run when every weak strategy is rejected
truthfully.

## Start here

```sh
python3 strategy_lab.py init
python3 strategy_lab.py data sync
python3 strategy_lab.py run daily
python3 strategy_lab.py status
python3 strategy_lab.py scheduler install
```

The default batch is 250 configurations. Explicit targets must remain between
50 and 500:

```sh
python3 strategy_lab.py run daily --target 500
```

Exact prior batches can be rebuilt:

```sh
python3 strategy_lab.py run reproduce <run-id>
```

## Compact architecture

The primary path is intentionally small:

| Boundary | Responsibility |
| --- | --- |
| `strategy_lab/data.py` | Incremental local gzip catalog and Parquet feature mart |
| `strategy_lab/contracts.py` | Versioned declarative strategy DSL and semantic hashes |
| `strategy_lab/generator.py` | Causal idea generation and deterministic configuration expansion |
| `strategy_lab/engine.py` | Point-in-time signals, next-open/intraday fills, stops, targets, costs, and portfolio limits |
| `strategy_lab/statistics.py` | Bootstrap, Wilson bound, DSR, PBO, Holm, folds, and neighbor stability |
| `strategy_lab/validation.py` | Frozen development/holdout/paper promotion gates |
| `strategy_lab/runner.py` | Daily batch, exact deduplication, artifacts, and reproduction |
| `strategy_lab/snapshot.py` | Redacted dashboard snapshot |
| `strategy_lab/bridge.py` | Signed SmartSioux transport and typed command worker |
| `strategy_lab/live.py` | Exact `PILOT_READY` Codex/Robinhood MCP boundary |

Persistent state lives outside Git under
`/Users/ensomniac/trade/historical_data/_strategy_lab/`:

- `strategy_lab.duckdb` for runs, results, candidates, commands, and audit events;
- `marts/daily_features.parquet` for the content-hashed feature mart;
- `snapshots/current.json` for the redacted local dashboard state;
- logs and the mode-0600 bridge secret.

The raw local archive remains immutable. Sync records each content hash,
rejects malformed quote-only or incomplete files explicitly, and performs no
provider or broker request.

## Strategy contract

Strategies are data, not executable Python. Each exact version contains:

- a bounded signal expression using allowlisted features and operators;
- an optional point-in-time cross-sectional rank;
- `next_open` or bounded intraday entry;
- stop, target, maximum five-session hold, and cost assumptions;
- causal thesis, falsifier, family, and at most four tunable values.

The AST is limited to depth four and twelve nodes. Semantic hashing prevents
renamed copies from entering the trial count. The OpenAI idea step can suggest
mechanisms through a structured schema, but it cannot execute code or access
outcomes. It uses `gpt-5.6-terra`, `store=false`, a daily $5 ceiling, and
deterministic built-in ideas when no API key is available.

## Evidence partitions

Every feature-mart version freezes:

1. chronological development;
2. a five-session embargo;
3. locked forward holdout.

Development requires at least 50 trades, positive stressed growth, stressed
profit factor of at least 1.20, a positive one-sided 90% bootstrap lower
expectancy, win-rate lower bound above 50%, drawdown at most 6R, five positive
walk-forward folds, DSR at least 0.90, PBO at most 0.50, Holm rejection, and
neighbor stability. Only one strategy may survive per family.

Holdout can be opened once per family and at most three times per week. It
requires at least 20 trades and the same economic direction without modifying
the frozen rules. A historically validated candidate then needs five
consecutive clean prospective paper signals.

`PILOT_READY` is earned only after those gates. It is not `LIVE_VALIDATED` and
does not guarantee future profitability.

## Execution realism

- Signals use only features observable before entry.
- Daily trades enter at the next eligible open.
- Intraday trades use post-10:00 bars and never same-bar hindsight.
- Same-interval stop/target ambiguity resolves stop-first.
- Gaps through stops fill at the open, not the desired stop.
- Primary and stressed costs are both reported.
- Sizing obeys the existing 0.50% planned-loss, 1.25% aggregate planned-loss,
  1.5% daily-loss, 4% weekly-loss, 8% drawdown, 100% gross-notional,
  three-position, and five-new-entry limits.

## SmartSioux

The native **Strategy Lab** tab at
[smartsioux.com](https://smartsioux.com/) shows:

- feature-mart health and frozen partitions;
- latest run progress and throughput;
- validation funnel and candidate states;
- leaderboard metrics including rejected strategies;
- audit events and command history;
- read-only state for all signed-in users;
- typed controls only for `ryan@ensomniac.com`.

The browser and server cannot contact Robinhood. SmartSioux stores only a
redacted HMAC-signed snapshot and typed command queue. The local bridge rejects
expired, replayed, unknown, non-Ryan, or free-form commands.

Live actions remain disabled by default. When separately enabled, only the
exact armed `PILOT_READY` rules hash can invoke the fixed Codex workflow through
the configured Robinhood MCP. Repository, maturity, account, risk, broker, and
platform confirmation gates still apply.

## Schedule and operations

`python3 strategy_lab.py scheduler install` installs two user LaunchAgents:

- weekday research at 4:30 PM Eastern;
- a signed dashboard bridge once per minute; long research runs publish their
  own signed progress heartbeat at most once every ten seconds.

Inspect them with:

```sh
python3 strategy_lab.py scheduler status
tail -f /Users/ensomniac/trade/historical_data/_strategy_lab/logs/daily.log
tail -f /Users/ensomniac/trade/historical_data/_strategy_lab/logs/bridge.log
```

Pause and resume can be queued from SmartSioux. Research never calls the
broker, and the live bridge remains off unless
`STRATEGY_LAB_LIVE_ENABLED=1` is deliberately configured.

## Verification

```sh
python3 -m pytest -q tests/test_strategy_lab_*.py
python3 -m ruff check strategy_lab strategy_lab.py tests/test_strategy_lab_*.py
python3 strategy_lab.py run reproduce <run-id>
```

The pre-revamp repository is preserved at the annotated tag
`legacy-pre-strategy-lab-2026-07-26`. Its evidence remains valid adverse and
historical context; it is no longer the operator's primary navigation path.
