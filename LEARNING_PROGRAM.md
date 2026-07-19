# Persistent Strategy Learning Program

Program version: `2026-07-19-v3`

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

1. **Persistent controller and registries:** implemented. Frozen datasets are
   first-class resumable objectives and close successfully only from an
   independently inspected `READY` event; collection does not masquerade as a
   strategy experiment or consume the hypothesis budget.
2. **Daily account simulator and selection-aware statistics:** implemented.
3. **Point-in-time security master:** populated from 20 dated common-stock
   snapshots and bound to an immutable hash-addressed snapshot.
4. **Scanner-faithful dataset:** v4 is inspected `READY`. All 118 Alpaca raw SIP
   session indexes, 20 dynamic rankings, 105,261 point-in-time evaluations, 389
   selected pairs, and 608,386 canonical documents passed independent source,
   metric, rank, and persistence verification.
5. **Registered hypothesis and experiment lifecycle:** implemented.
6. **Three-axis champion/challenger evidence:** implemented.
7. **Operating cadence and monitoring:** implemented.

## Current Evidence Decisions

- Keep `2026-07-15-orb-v3` frozen and `UNVALIDATED`.
- Treat the existing 95-bundle catalyst-derived corpus as development and
  falsification evidence, not a faithful replay of the live 09:35 scanner.
- Retire the exact early Item 2.02 reversal contract after its independent
  confirmation failed. Do not tune its thresholds against that sample; its
  unused hypothesis-specific shadow runner has been removed.
- Do not add flexible machine learning until point-in-time universe fidelity,
  experiment-family accounting, and nested chronological validation exist.

## Current Next Objective

The scanner fidelity objective is complete and its resumable dataset run closed
successfully. The research lock condition is satisfied, but the next objective
is not a new strategy variant. Freeze a selected-candidate join over the replay's exact
date-symbol pairs, then collect target-session one-minute trades, time-valid
catalysts, historical quotes when available, benchmark context, and outcome
inputs into the canonical external store. Evaluate the unchanged champion and a
paired paper-aligned exit baseline before estimating any production gate's
marginal value. The 20-date dataset proves pipeline fidelity; a subsequent
preregistered sample of at least 100 previously uninspected scanner dates is
required for meaningful alpha inference. The ordered stages and decision gates
are maintained in `STRATEGY_LEARNING_EXECUTION_PLAN.md`. See
`SCANNER_REPLAY.md` for the completed source and inspection counts.

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
