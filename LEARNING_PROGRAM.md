# Persistent Strategy Learning Program

Program version: `2026-07-18-v1`

Production champion: `2026-07-15-orb-v3`

Production maturity: `UNVALIDATED`

## Objective

Continuously improve durable geometric account growth after spreads, fees,
slippage, missed fills, drawdowns, and operational failure risk. The program is
persistent across bounded Codex invocations; it is not an infinite process and
does not activate production rules.

## Truth Surfaces

- `learning/DATASETS.jsonl` records every evidence corpus and what claims it can
  support.
- `learning/EXPERIMENTS.jsonl` records every hypothesis family, trial result,
  failure, and disposition.
- `learning/STRATEGIES.jsonl` records immutable strategy identities and their
  alpha, execution, operations, and production-role states.
- `learning/RESEARCH_LOCK.json` blocks new catalyst-corpus hypothesis contracts
  until its required scanner-replay dataset reaches the registered `READY`
  state.
- Ignored `learning_runs/<run-id>/state.json` holds resumable operational state.
- `progress/HISTORY.jsonl` remains the durable architecture and findings log.
- `strategy_ledger.py report` remains authoritative for earned production
  maturity.

Registry corrections append a superseding event. Existing events are never
rewritten or deleted.

## Program State

1. **Persistent controller and registries:** implemented.
2. **Daily account simulator and selection-aware statistics:** implemented.
3. **Point-in-time security master:** populated from 20 dated common-stock
   snapshots and bound to an immutable hash-addressed snapshot.
4. **Scanner-faithful dataset:** frozen and collecting; it does not qualify as
   a production scanner replay until 118 market-wide minute files are present,
   the dynamic results are inspected, and a READY registry event supersedes its
   current COLLECTING event.
5. **Registered hypothesis and experiment lifecycle:** implemented.
6. **Three-axis champion/challenger evidence:** implemented.
7. **Operating cadence and monitoring:** implemented.

## Current Evidence Decisions

- Keep `2026-07-15-orb-v3` frozen and `UNVALIDATED`.
- Treat the existing 95-bundle catalyst-derived corpus as development and
  falsification evidence, not a faithful replay of the live 09:35 scanner.
- Retire the exact early Item 2.02 reversal contract after its independent
  confirmation failed. Do not tune its thresholds against that sample.
- Do not add flexible machine learning until point-in-time universe fidelity,
  experiment-family accounting, and nested chronological validation exist.

## Current Next Objective

Resume the frozen `dataset-production-scanner-replay-2026-07-18-v1` collection
after the existing Massive Dashboard-issued S3 credentials are present in the
ignored `.env`. Download the exact 118 market-wide minute files, reconstruct and
inspect all 20 dynamic 09:35 ET universes, and only then register the dataset as
READY. Until that evidence exists, the weekly hypothesis review must record a
no-op rather than inventing more variants on the already inspected catalyst
corpus. This boundary is enforced by `learning_experiment.py` and
`learning_cadence.py`, not only documented. See `SCANNER_REPLAY.md`.

## Bounded Iteration

Each run inventories evidence, registers or resumes one objective, freezes its
contract, verifies data, evaluates it, performs adversarial review, records a
disposition, and closes. A run performs at most 20 deterministic transitions.
Codex supplies research judgment at explicit `next_action` boundaries. Broker
actions, external application writes, and automatic strategy activation are
forbidden.

## Promotion Boundary

Research may produce a versioned production proposal only after independent
alpha evidence, shadow or live-calibrated execution evidence, and operational
readiness all pass their frozen contracts. Applying the proposal is a separate
production-version workflow under `AGENTS.md`; it is never an action exposed by
the learning controller.
