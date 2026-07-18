# Persistent Learning Cadence

The cadence is a finite scheduler target, not a trading daemon. It performs safe
deterministic audits, records completion under ignored `learning_runs/`, and
returns `needs_agent` when research judgment, provider collection, or a new
frozen contract is required. It cannot access Robinhood, place orders, collect
provider data, change production configuration, or activate a strategy.

## Operator Command

Run after the close or from a user-controlled scheduler:

```sh
cd /Users/ensomniac/trade/robinhood_codex
python3 session_mode.py --mode learning
python3 learning_cadence.py run --max-tasks 5
```

The command is idempotent inside each cadence interval. Installing `cron`,
`launchd`, GitHub Actions, or another external scheduler is intentionally a
separate operational action; this repository does not silently install one.

## Task Contracts

- Daily: audit public registries, data claims, hypothesis contracts, strategy
  axes, production ledger, lifecycle, privacy, and durable progress.
- Nightly: resume only a dataset already registered as `FROZEN` or `COLLECTING`.
  The cadence surfaces the exact IDs but performs no provider calls itself.
  The current scanner pilot is registered `COLLECTING`, so this task keeps
  surfacing its exact ID until its 118 minute files and 20 daily universes are
  collected and inspected; use `SCANNER_REPLAY.md` as its runbook.
- Weekly: allow no more than three distinct new mechanisms. An active
  `learning/RESEARCH_LOCK.json` makes catalyst-corpus invention an automatic
  `research_fidelity_lock` no-op until the named dataset is registered at its
  required status. Codex may also record a no-op when available data cannot
  falsify a useful idea.
- Monthly: surface only experiments that already earned
  `CONFIRMATION_QUEUED`; otherwise complete as a no-op.
- Quarterly: recompute production maturity and all strategy readiness axes.
- Immediate: a live safety workflow remains governed by `session_guard.py` and
  takes priority over this cadence. Integrity, rule, or protection failure
  produces a pause signal through `learning_strategy.py health`.

Mark a judgment-bound task only after its evidence is durable:

```sh
python3 learning_cadence.py complete \
  --task weekly_hypothesis_review \
  --outcome no_op \
  --evidence-path LEARNING_PROGRAM.md
```

Blocked tasks remain due at the next invocation. A task outcome never changes a
production strategy or creates broker/provider authority.
