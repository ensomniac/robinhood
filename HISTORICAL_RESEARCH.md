# Historical Multi-Strategy Research

Updated: 2026-07-18

## Purpose And Boundary

`historical_research.py` reuses complete, ignored `historical_data/YYYY-MM-DD.json`
bundles to test several research strategies in one local batch. It is separate
from production replay:

The provider observations underlying newly built bundles are retained under
`LOCAL_HISTORICAL_DATA_ROOT` using the contract in
`HISTORICAL_DATA_STORE.md`. Research still consumes frozen bundles so a provider
cache extension cannot silently change an already-bound experiment.

- it makes no IBKR, Robinhood, web, or other provider requests;
- it never writes `SIGNALS.jsonl`, `TRADES.md`, `trades/`, strategy configuration,
  or maturity state;
- generated per-date shards live under ignored `research_runs/`;
- only a compact aggregate JSON/Markdown result is intended for publication;
- every missing or unsupported input remains an explicit coverage blocker.

The current bundles represent the already frozen, catalyst-selected candidate
universe. Results answer “how would these rules have behaved on these frozen
candidates?” They do not establish full-market expectancy.

This limitation is machine-enforced by `learning_data.py`. The public dataset
registry classifies the corpus as `catalyst_falsification` with
`FALSIFICATION_ONLY` claim scope. A production ORB dataset must instead freeze
the scanner rules and point-in-time security-master identity, then reconstruct
the dynamic complete 09:35 ET universe using only information available by that
time. Current contract resolution may not replace a renamed, retired, or
delisted historical instrument.

The first scanner-faithful pilot is now frozen separately. Its sourced security
master, exact 20 dates, split-adjustment input, 118-file collection requirement,
and current credentials blocker are documented in `SCANNER_REPLAY.md`. Until
that pilot is READY, this catalyst corpus remains falsification-only and must
not generate additional tuned variants. `learning/RESEARCH_LOCK.json` enforces
that rule in both hypothesis freezing and the weekly cadence.

## Architecture

One date is the unit of parallel work. A worker process loads a date bundle
once, freezes each candidate's 390 one-minute bars into immutable dataclasses,
and evaluates all requested strategy plugins. This is faster and more memory
efficient than launching one process per strategy, because parsing and disk I/O
are shared within a date. The parent process is the only writer: it sorts all
results by date and strategy, writes isolated shards atomically, and constructs
the comparison report deterministically.

```text
public evidence manifest
        |
        v
date + bundle hash inventory
        |
        v
bounded process pool (one task per date)
        |
        +-- load/freeze bundle once
        +-- reveal bars progressively to strategy A
        +-- reveal bars progressively to strategy B
        +-- reveal bars progressively to strategy N
        |
        v
single deterministic writer
        +-- research_runs/<run-id>/shards/<strategy>/<date>.json
        +-- research_runs/<run-id>/result.json
        +-- research_runs/<run-id>/report.md
        +-- optional compact public JSON and Markdown
```

The run ID is derived from the dataset and configuration hashes. The dataset
hash covers the public evidence manifest plus the content hash or missing status
of every requested daily bundle. The configuration hash covers plugin identity,
version, description, requirements, plugin and runner implementation content
hashes, the common execution model, and baseline.
Worker count and timing are runtime telemetry; they do not change the stable
result.

## Point-In-Time And Execution Semantics

The runner enforces the time boundary rather than trusting plugin authors. It
calls each plugin on progressively revealed immutable prefixes and accepts a
decision only for the final completed bar in the prefix. A plugin that backdates
a decision is blocked. No strategy sees the next bar when it forms a signal.

All strategies use one shared execution model:

1. Form the signal from a fully completed one-minute bar before 10:30 ET.
2. Enter at the following minute's open plus five basis points of adverse
   slippage. Strategy-specific entry caps are checked only after that open is
   observable.
3. Use the plugin's technical stop and a target at 2R planned risk.
4. Apply five basis points of adverse exit slippage.
5. When a minute contains both stop and target, resolve the unknown event order
   to the stop.
6. If neither boundary trades, exit at the 15:50 bar's open.
7. Rank simultaneous candidate signals by earliest completed signal, then
   strength, then symbol; take at most one trade per strategy and date.

The default slippage and target are deliberately parameterized for sensitivity
testing. They are not a claim about actual execution quality. Minute bars cannot
reconstruct tick order.

## Plugin Contract

Built-ins are listed with:

```sh
python3 historical_research.py list-strategies
```

The initial research plugins are:

- `orb-5m-research@1.0.0`: bullish five-minute opening range, completed close
  over its high and VWAP, plus a 0.15% next-open chase cap;
- `vwap-pullback@1.0.0`: recent above-VWAP trade followed by a bullish touch,
  reclaim, and prior-high confirmation;
- `hod-continuation@1.0.0`: bullish close over prior high of day and VWAP with at
  least 1.5 times the prior five-minute average volume;
- `opening-reversal@1.0.0`: red opening range followed by a bullish reclaim of
  opening midpoint and running VWAP.

Each plugin exposes `strategy_id`, semantic `version`, `description`,
`DataRequirements`, and `find_signal(CandidateContext)`. Add a plugin to the
built-in registry or load a public external object with `module:attribute`.
Requirements are fail-closed. For example, a plugin declaring historical full
depth, benchmark bars, or subminute trades is reported as unsupported by the
current bundle adapter instead of being silently approximated.

Changing signal semantics requires a version change. Changing only the number
of worker processes does not.

## Operator Commands

Run all built-ins over the frozen 100-date evidence set with four worker
processes:

```sh
python3 historical_research.py run \
  --evidence historical_batches/evidence-2026-07-16-one-hundred-days.json \
  --workers 4
```

Run selected strategies and publish the compact aggregate:

```sh
python3 historical_research.py run \
  --evidence historical_batches/evidence-2026-07-16-one-hundred-days.json \
  --strategy orb-5m-research \
  --strategy hod-continuation \
  --baseline orb-5m-research \
  --workers 4 \
  --publish-prefix research_results/2026-07-16-multi-strategy
```

Test a less optimistic execution model without reacquiring data:

```sh
python3 historical_research.py run \
  --evidence historical_batches/evidence-2026-07-16-one-hundred-days.json \
  --entry-slippage-bps 10 \
  --exit-slippage-bps 10 \
  --target-r 1.5 \
  --workers 4
```

Every aggregate reports covered/blocked/trade days, signal counts, win rate,
mean/median/total R, profit factor, maximum drawdown, exit reasons, common
rejections, and a paired date-level comparison against the selected baseline.
No-trade days are 0R in the paired comparison, and only dates covered by both
strategies enter a pair.

## Production-Aware Strategy Lab

`historical_strategy_lab.py` is the higher-level learning controller for testing
why a production rule stack does or does not trade. It reuses the same frozen
bundles, progressive plugin contract, next-bar entry, stop-first ambiguity, and
force-flat semantics, then adds:

- exact reconstruction and verification of each bundle's frozen scanner and
  ordered-candidate evidence hash;
- current `strategy_engine.py` evaluation for every frozen candidate, with
  normalized production rejection categories and pass/fail outcome cohorts;
- versioned one-trade-per-day policies for production eligibility, early timing,
  Item 2.02 catalysts, stop compatibility, and simultaneous-signal selection;
- a complete slippage/target matrix, chronological 60/20/20 stability phases,
  deterministic bootstrap uncertainty, trade concentration, stop distance, and
  risk-implied notional;
- before/after hashes proving that production configuration, ledgers, contexts,
  and maturity artifacts did not change.

Run the complete declared lab family locally:

```sh
python3 historical_strategy_lab.py run \
  --evidence historical_batches/evidence-2026-07-16-one-hundred-days.json \
  --bootstrap-samples 20000 \
  --publish-prefix research_results/2026-07-18-production-aware-strategy-lab
```

The default grid is 0, 5, 10, and 20 bps of adverse entry and exit slippage
crossed with 1R, 1.5R, 2R, and 3R targets. The lab publishes every cell. Its
`promising_for_independent_confirmation` label requires positive retrospective
phases, a positive one-sided 90% bootstrap lower bound, robust PF/drawdown at 10
and 20 bps, and positive results at all four target levels. That label never
changes live rules; newly frozen dates and deployable execution geometry remain
mandatory.

The first production-aware result and its frozen follow-up contract are:

- `research_results/2026-07-18-production-aware-strategy-lab.{json,md}`
- `research_results/2026-07-18-early-earnings-reversal-confirmation.md`

## Independent Confirmation Workflow

The lab also owns the prospective confirmation boundary for the sole frozen
`reversal-early-earnings@1.0.0` policy. This is a two-step workflow; an old
bundle cannot be relabeled after its outcome is known.

First, generate a new point-in-time evidence manifest with at least 100 dates
and complete ordered Item 2.02 candidate universes. Its scanner attestation must
state `target_session_prices_observed=false`. The dates must not overlap any date
inspected by `strategy-lab-651f20ff135e-b268f18ec755`. Freeze it before target-
session collection:

```sh
python3 historical_strategy_lab.py freeze-confirmation \
  --evidence historical_batches/evidence-<new-independent-sample>.json
```

The command writes
`historical_batches/confirmation_manifests/confirmation-<sha256>.json`. The hash
covers every date, ordered candidate and catalyst row, excluded dataset identity,
the one policy/plugin, execution grid, acceptance thresholds, deployment model,
production baseline, and implementation hashes. Validate the frozen identity at
any time with:

```sh
python3 historical_strategy_lab.py validate-confirmation \
  historical_batches/confirmation_manifests/confirmation-<sha256>.json
```

Only after that file exists may the normal read-only historical collector request
the target sessions. Every resulting bundle must use `sample_phase=confirmation`,
carry the exact manifest hash and registration timestamp, retain the exact date
and symbol order, and have `source.captured_at` strictly after preregistration.
Then run the fixed confirmation:

```sh
python3 historical_strategy_lab.py run-confirmation \
  historical_batches/confirmation_manifests/confirmation-<sha256>.json \
  --bootstrap-samples 20000 \
  --publish-prefix research_results/<independent-confirmation-name>
```

The result publishes every requested date, blocker, primary trade/exit, and all
12 frozen cost/target cells. It applies the predeclared 80-date/50-signal,
expectancy, PF, drawdown, bootstrap, chronological-half, best-five-removal,
cost-stress, and target-stress gates. Failure returns
`stop_without_threshold_tuning`; callers cannot supply another policy or grid.

Two deployment views remain separate from strategy R. The structural arm keeps
the real stop, reserves 10 bps for stop slippage, and sizes whole shares to the
UNVALIDATED 0.25% account-risk budget with an 80% allocation cap. It reports
account compounding, log growth, account drawdown, allocation/shortfall, stop
geometry, stop slippage, binding caps, and performance without the largest five
gains. The production-compatible cohort contains only stops that naturally
start at or below 0.8%; wider stops are never tightened into it, and fewer than
20 cohort trades is explicitly insufficient for inference.

## Independent 100-Date Confirmation Result

The preregistered 2026-07-18 run froze the exact 100-date seed
`1003109952991718283` with zero overlap against
`strategy-lab-651f20ff135e-b268f18ec755`. Two dates exhausted their ordered
candidate buffers before target collection and remained explicit blockers.
After preregistration, 31 validation-grade bundles were collected and 19 more
dates hit immutable target-data blockers. Collection stopped when the resulting
21 missing dates made the required 80 validation-grade dates mathematically
impossible; the remaining 48 dates were not requested or substituted.

The observed subset also rejected the hypothesis on performance. At the frozen
5 bps-per-side and 2R primary cell, 22 trades returned -10.056R, -0.457R mean
expectancy, 0.370 profit factor, and 10.492R maximum drawdown. Every frozen
target and cost-stress cell was negative. Risk-sized structural deployment
compounded -2.298% with no stop compression or risk-cap violation, while only
six naturally occurring stops were at or below 0.8%.

The decision is `stop_without_threshold_tuning`. This hypothesis must not
advance to shadow qualification or a production proposal. The immutable
manifest and complete result are published under
`historical_batches/confirmation_manifests/` and
`research_results/2026-07-18-independent-early-earnings-reversal-confirmation.{json,md}`.

## Retired Shadow Qualification Path

The reversal-specific prospective shadow runner was removed after independent
confirmation rejected the hypothesis. No shadow observation had been started,
and retaining an executable qualification path contradicted the terminal
`stop_without_threshold_tuning` decision. The implementation and its tests
remain available in Git history; the frozen manifest and published negative
result remain the durable audit artifacts. Future execution qualification must
belong to an evidence-qualified strategy version, not be inherited from this
retired reversal.

## First 100-Date Matrix

The evidence-bound run on 2026-07-16 retained all 100 requested dates, covered
the 95 complete bundles, and kept the five historical bid/ask-boundary gaps as
blockers. Four worker processes completed 400 date-strategy evaluations in
2.003 seconds with zero provider calls. The same matrix took 7.332 seconds with
one worker, a 3.66 times wall-clock speedup, and produced the same stable result.

The initial identity audit caught four valid-looking date files whose symbols
belonged to older frozen universes. The bundle builder had reused files by date
and schema validity without binding them to their evidence rows. The repaired
builder now stores and verifies `source.frozen_evidence_sha256`; the research
runner independently requires exact ordered symbol equality with the public
evidence manifest. Rebuilding all 95 evidence-bound bundles reused 926 candidate
raws, collected 36 missing candidates with 92 IBKR requests, and took 201.790
seconds. No date or symbol was substituted.

The first-pass results were:

- `orb-5m-research`: 59 trades, +4.119R total, 0.070R mean, 1.113 profit factor;
- `vwap-pullback`: 94 trades, -14.790R total, -0.157R mean, 0.783 profit factor;
- `hod-continuation`: 87 trades, +2.228R total, 0.026R mean, 1.047 profit factor;
- `opening-reversal`: 91 trades, +14.879R total, 0.164R mean, 1.320 profit factor.

These are exploratory in-sample comparisons of four newly specified plugins,
not promotion evidence. The opening-reversal result is a hypothesis to freeze
and test on independent dates, while the current VWAP-pullback definition is a
clear reject or redesign candidate. The exact aggregate is published under
`research_results/2026-07-16-one-hundred-days-multi-strategy.{json,md}`.

## What The Stored Data Can And Cannot Test

The complete one-minute candidate bars support long bar-close entries, VWAP and
volume conditions, opening-range variants, bar-based stop/target rules, force
flat rules, parameter sweeps, and multiple isolated strategies in one run.

The published bundles do not currently provide complete benchmark minute bars,
arbitrary-time historical NBBO for every possible signal, full order-book depth,
or tick ordering across the whole session. Strategies that need those fields
must declare them and remain blocked until the collection contract is expanded.
The evidence manifest's frozen universe must not be widened or replaced using
target-session performance.

## Validation

`tests/test_historical_research.py` and `tests/test_historical_strategy_lab.py`
prove immutable inputs, progressive
no-lookahead disclosure, next-bar entry, adverse same-minute ambiguity,
requirements blocking, malformed-bar rejection, exact preregistration and
excluded-date identity, manifest mutation/order/capture-time refusal,
deterministic one-policy results, structural-stop sizing, identical stable
output across worker counts, and unchanged production ledger files.
Before publishing a result, run the full repository tests and audits in the
normal project workflow.
