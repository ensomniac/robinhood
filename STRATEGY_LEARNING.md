# Strategy Learning And Context Lifecycle

The learning system has two separate responsibilities:

1. `trade_lifecycle.py` makes every terminal idea, trade, and session complete,
   date-partitioned, privacy-safe evidence.
2. `strategy_learning.py` reads that evidence and the structured signal ledger
   to create diagnostics and cadence-gated proposals. It cannot apply changes.

This separation prevents one trade—or a generated narrative—from silently
changing the real-money strategy.

## Closing Context

Only current-day in-progress context belongs directly under `trades/active/`.
Create a public outcome JSON object when an idea is rejected/stale, a trade is
flat, or a session ends. Required fields are:

```json
{
  "context_id": "2026-07-16-XYZ-1",
  "date": "2026-07-16",
  "context_kind": "trade",
  "mode": "live",
  "result": "success",
  "summary": "The catalyst ORB reached its structural exit.",
  "primary_reason": "two_percent_milestone",
  "thesis_result": "held",
  "what_worked": ["Opening volume and VWAP structure held."],
  "what_failed": [],
  "lessons": ["Retain as one frozen-rule observation."],
  "next_time": ["Continue complete scanner capture."],
  "metrics": {"net_r": 1.8},
  "strategy_version": "2026-07-15-orb-v3",
  "rules_hash": "current strategy_engine.py rules hash"
}
```

Allowed context kinds are `session`, `trade_idea`, and `trade`. Results are
`success`, `failure`, `flat`, `rejected`, `stale`, `no_trade`, or `completed`.
The lifecycle validator rejects UUIDs, broker/account identifier fields,
non-finite numbers, mismatched dates, stale rules for new outcomes, and unsafe
kind/result combinations.

Close and audit with:

```sh
python3 trade_lifecycle.py close trades/active/2026-07-16-XYZ-1.md outcome.json
python3 trade_lifecycle.py audit
```

The close operation appends a readable `Terminal Outcome Summary` and an embedded
JSON learning record, then moves the single context to
`trades/archived/2026_07_16/`. Historical replay is the only caller allowed to
archive a non-current date, through the explicit `--historical-replay` path.

The audit fails on stale/misnamed active files, terminal context left active,
wrong archive folder shape, date mismatches, duplicate context IDs, malformed or
missing outcome data, and privacy/schema violations. Old strategy-version
outcomes remain structurally auditable after a new version is activated.

## Evidence Report

Run the read-only report at any time:

```sh
python3 strategy_learning.py report
```

It first requires a clean `strategy_ledger.py audit`, then combines:

- Canonical maturity, expectancy, drawdown, execution, no-trade, and paired-exit
  results from `SIGNALS.jsonl`.
- Terminal result and primary-reason counts from archived context.
- Coverage and bounded cohort diagnostics for OR_RVOL, score, spread, stop
  fraction, resistance room, and reward/risk.

The report does not treat rejected or daily-limit-missed ideas as realized
returns. It includes only records with the current strategy version and rules
hash in current-rule conclusions.

## Review Cadence And Proposals

A written strategy review requires both:

- At least 20 new closed, triggered, frozen-rule signals since the last review.
- At least 30 calendar days since the current version began or the last review.

When both pass:

```sh
python3 strategy_learning.py propose
```

The command writes a timestamped JSON proposal under `strategy_proposals/`.
Predeclared diagnostics can flag exit-overlay underperformance, stop reserve
shortfall, or material cohort differences at OR_RVOL 3 and score 95. These are
research hypotheses, not optimized live thresholds. Every proposal explicitly
states that automatic application is false and a separate production-version
workflow is required.

There is intentionally no `apply` command. Codex makes the evidence-backed
application decision under the delegated authority in `AGENTS.md`. An accepted
proposal still requires a documented source/evidence review, a separate
production-change workflow, a new strategy version and rules hash, preserved
prior sample, updated tests/docs, and preregistered confirmation evidence. A
proposal with no supported hypothesis freezes the current rules and asks for
more complete data.

## Publishing

Archived context, `SIGNALS.jsonl`, `TRADES.md`, and strategy proposals are public
evidence. After changes to trade context or ledgers, run the identifier and
lifecycle audits, then commit and push the full root project under the canonical
publishing workflow in `AGENTS.md`. Raw provider replay bundles are ignored and
must not be used as a substitute for the public derived evidence.
