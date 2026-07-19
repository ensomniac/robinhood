# Persistent Strategy Learning Program

Program version: `2026-07-19-v5`

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
5. **Selected-candidate join:** inspected `READY` for pipeline development. All
   389 exact pairs and 40 benchmark sessions have one-minute bars; 325 crossing
   windows have bounded raw trade/quote tape, and all pairs have time-bounded
   news-discovery contexts. Primary catalyst, clean-condition, depth,
   tradability, halt, resistance, and sector contracts remain explicit blockers.
6. **Primary-source and clean-trigger fidelity:** independently inspected
   `READY` for pipeline development. All 389 pairs have a dated CIK; 100 unique
   SEC primary documents produced 81 filing-covered pairs and eight dilution
   conflicts. Frozen SIP semantics rejected 197 of 325 first raw crosses; only
   155 condition-valid clean-cross windows remained inside the chase cap.
7. **Registered hypothesis and experiment lifecycle:** implemented.
8. **Three-axis champion/challenger evidence:** implemented.
9. **Operating cadence and monitoring:** implemented.

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

The scanner, selected-candidate join, point-in-time CIK, SEC primary-source
candidate, and clean-condition mechanics are complete. The next objective is
still not a strategy variant: classify the 80 material primary-source candidates
without converting filing presence into positive direction, and close or
explicitly reject the point-in-time halt/tradability, resistance, sector, and
depth inputs. Then evaluate the unchanged champion. The existing 20 dates prove
pipeline fidelity and gate attrition only. A subsequent preregistered sample of
at least 100 previously uninspected scanner dates is required for meaningful
alpha inference. See `SCANNER_REPLAY.md`, `SELECTED_CANDIDATE_JOIN.md`,
`SELECTED_CANDIDATE_FIDELITY.md`, and
`STRATEGY_LEARNING_EXECUTION_PLAN.md`.

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
