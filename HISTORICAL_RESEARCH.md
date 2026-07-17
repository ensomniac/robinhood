# Historical Multi-Strategy Research

Updated: 2026-07-16

## Purpose And Boundary

`historical_research.py` reuses complete, ignored `historical_data/YYYY-MM-DD.json`
bundles to test several research strategies in one local batch. It is separate
from production replay:

- it makes no IBKR, Robinhood, web, or other provider requests;
- it never writes `SIGNALS.jsonl`, `TRADES.md`, `trades/`, strategy configuration,
  or maturity state;
- generated per-date shards live under ignored `research_runs/`;
- only a compact aggregate JSON/Markdown result is intended for publication;
- every missing or unsupported input remains an explicit coverage blocker.

The current bundles represent the already frozen, catalyst-selected candidate
universe. Results answer “how would these rules have behaved on these frozen
candidates?” They do not establish full-market expectancy.

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

`tests/test_historical_research.py` proves immutable inputs, progressive
no-lookahead disclosure, next-bar entry, adverse same-minute ambiguity,
requirements blocking, malformed-bar rejection, built-in patterns, identical
stable output across one and multiple workers, and unchanged production ledger
files. Before publishing a result, run the full repository tests and audits in
the normal project workflow.
