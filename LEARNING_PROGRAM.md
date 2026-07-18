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
- Ignored `learning_runs/<run-id>/state.json` holds resumable operational state.
- `progress/HISTORY.jsonl` remains the durable architecture and findings log.
- `strategy_ledger.py report` remains authoritative for earned production
  maturity.

Registry corrections append a superseding event. Existing events are never
rewritten or deleted.

## Program State

1. **Persistent controller and registries:** implemented.
2. **Daily account simulator and selection-aware statistics:** implemented.
3. **Point-in-time security master and scanner-faithful datasets:** implemented;
   no current dataset qualifies as a production scanner replay.
4. **Registered hypothesis and experiment lifecycle:** queued.
5. **Three-axis champion/challenger evidence:** queued.
6. **Operating cadence and monitoring:** queued.

## Current Evidence Decisions

- Keep `2026-07-15-orb-v3` frozen and `UNVALIDATED`.
- Treat the existing 95-bundle catalyst-derived corpus as development and
  falsification evidence, not a faithful replay of the live 09:35 scanner.
- Retire the exact early Item 2.02 reversal contract after its independent
  confirmation failed. Do not tune its thresholds against that sample.
- Do not add flexible machine learning until point-in-time universe fidelity,
  experiment-family accounting, and nested chronological validation exist.

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
