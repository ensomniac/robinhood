# Repository Learning Loop

Prompt version: `2026-07-19-v4`

This is the public operating prompt for `learning` mode. It is subordinate to
`AGENTS.md`, the active user request, tool requirements, privacy rules, and every
live-exposure safety obligation. Its purpose is to shorten the path from durable
evidence to one validated improvement. It grants no broker authority and never
activates a production strategy change.

The loop is deliberately bounded. One invocation may apply at most one coherent
engineering slice and may make at most one repair attempt after validation. A
failure that survives that repair is recorded and returned as a blocker; it does
not trigger another self-edit cycle. `LEARNING_PROGRAM.md` and the append-only
`learning/` registries persist the larger program across explicit invocations;
ignored `learning_runs/` state makes each finite objective resumable.

## Phase 0 - Safety Gate

1. Select `learning` with `python3 session_mode.py --mode learning`.
2. Confirm the mode reports `broker_actions_allowed=false`.
3. Do not start while live exposure, an unresolved order, or another
   safety-critical workflow needs attention. An active trade context is a local
   warning; broker truth remains authoritative in a live workflow.
4. Inventory the dirty worktree before editing. Existing changes belong to the
   user unless proven otherwise and must be preserved.
5. Run `python3 learning_loop.py inspect` with the relevant batch, evidence, and
   research artifacts. Do not assume collection is slow, evaluation is slow, or
   symbol resolution is slow without telemetry.
6. Run `python3 learning_loop.py audit` and register the objective in
   `learning/EXPERIMENTS.jsonl` or `learning/DATASETS.jsonl` before starting a
   resumable run. A frozen collection is a dataset objective, not a synthetic
   experiment.

## Phase 1 - Evidence And Strengths

State the actual outcome surface, then identify ten concrete things that are
working. Prefer executable controls and measured facts: immutable evidence
identity, no-lookahead guards, cache hits, provider request telemetry, replay
yield, local research speed, test coverage, audits, isolation boundaries,
resume behavior, and publication discipline.

This phase prevents a broad refactor from deleting protections simply because
they add visible structure.

## Phase 2 - Bottleneck Proof

Partition latency into discovery, symbol metadata, historical bars, historical
quotes, derivation, evaluation, interpretation, validation, and publishing.
Measure request counts, summed request seconds, pacing waits, cache hits, wall
time, and local evaluations per second. Name one primary bottleneck and any
secondary costs.

Use an already frozen local corpus before collecting more dates. New collection
is justified only when the current data cannot answer the next decision, an
independent confirmation sample is required, or coverage is demonstrably too
small.

For replay fidelity, a bar high locates a possible trigger window but never
proves a clean or executable break. Preserve raw trade conditions and
provider-ordering timestamps, form required quote snapshots only after the
condition-valid continuous regular-sale cross, and treat secondary news,
unclassified primary filings, top-of-book-only size, current
tradability, and missing halt/depth facts as blockers rather than truthy
defaults. Minute-high eligibility and clean-cross eligibility are separate
decisions: special prints may satisfy the former without satisfying the latter.
Keep exact licensed rows outside Git and publish hashes and aggregate coverage
only.

## Phase 3 - Improvement Candidates

Generate no more than five candidate improvements. For each, record expected
learning-time reduction, fidelity risk, implementation size, reversibility,
required evidence, and acceptance test. Include simplification and deletion
options, not only new components.

When a candidate is a new trading mechanism, freeze it through
`learning_experiment.py` and count it against the three-per-ISO-week hypothesis
budget. Its complete parameter grid, locked primary parameters, falsification
criteria, contamination risks, and production-compatibility risks must be
registered before evaluation.

Production-rule edits, automatic strategy activation, broker actions, account
access, credential handling, and external application writes are out of scope.
Repository commit/push after validation remains governed by `AGENTS.md`; it is
not authority carried inside the change plan. A strategy hypothesis may only
become an isolated proposal or versioned research plugin.

## Phase 4 - Adversarial Filter

Try to disprove every candidate. Check for target-session leakage, stale cache
reuse, hidden provider switching, outcome-driven date or symbol substitution,
overfitting, dirty-worktree damage, duplicated infrastructure, unbounded loops,
and telemetry that measures the wrong layer.

Discard candidates whose expected benefit is unsupported or whose safety cost
is larger than the measured delay. Rank the survivors and select one coherent
slice. A no-op is valid when none survives.

## Phase 5 - Apply Once

Write a machine-readable change plan and validate it with
`python3 learning_loop.py review-plan <plan.json>`. Apply only the selected
engineering, test, documentation, or proposal slice. Preserve existing user
changes and keep raw historical inputs, generated research shards, secrets, and
private configuration out of Git.

Do not edit `strategy_config.toml` in this mode. Do not reinterpret a faster data
pipeline as strategy evidence.

## Phase 6 - Validate And Benchmark

Run focused tests first, then the full relevant suite and repository audits.
Benchmark the same boundary used in Phase 2 and compare request counts as well as
wall time. A warm-cache speedup is not evidence of a cold-path improvement, and
removing a provider request is more durable than tuning concurrency blindly.

After installing the official IBKR client and `requirements-dev.txt` into the
same environment, the portable full-suite gate is:

```sh
python3 -m pytest -q
python3 -m ruff check .
```

Do not rely on a bare machine-global `pytest` executable; invoke the module from
the explicitly selected interpreter so a stale launcher cannot invalidate a
persistent run.

If validation fails, diagnose and attempt one bounded repair. If it still fails,
stop, retain useful evidence without committing broken work, and report the
blocker.

## Phase 7 - Record And Stop

Review the final diff for privacy and scope. Add one append-only
`progress/HISTORY.jsonl` entry for a reusable finding or architecture change.
Run lifecycle, ledger, progress, and sensitive-data audits as applicable. Commit
and push validated changes under the normal repository mandate.

End with the measured before/after result, the remaining dominant bottleneck,
and the next experiment. Update the public registries with every terminal
failure as well as every success. Do not recursively launch another learning loop.
A future run must be a new explicit invocation or a separately
implemented, tested, safety-gated scheduler invocation.

The persistent controller follows the objective's own registry. Experiment
events use the hypothesis lifecycle. Dataset events map `COLLECTING` to a frozen
collection handoff, uninspected `READY` to an independent-inspection handoff,
inspected `READY` to successful closure, and `FAILED` or `RETIRED` to rejection
and closure. This keeps data-engineering work resumable without pretending that
collection is a strategy experiment or consuming the hypothesis budget.

The implemented scheduler target is `learning_cadence.py`. It may run
deterministic audits and record ignored local cadence state, but provider
collection and hypothesis invention remain explicit agent handoffs. See
`LEARNING_CADENCE.md` for the finite operator contract.
