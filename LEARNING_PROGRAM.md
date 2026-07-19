# Persistent Strategy Learning Program

Program version: `2026-07-19-v10`

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
- `learning/RESEARCH_LOCK.json` blocks both new catalyst-corpus hypothesis
  contracts and `strategy_learning.py` proposals until its required
  scanner-replay dataset reaches the registered, independently inspected
  `READY` state in the exact required lane.
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
7. **Catalyst classification and official halt history:** independently
   inspected `READY` for pipeline development. Prior-close recency reduced 106
   filing observations to 50 recent observations; only 12 of 389 pairs had a
   verified material primary catalyst, only one had verified positive direction,
   and six were conflict rejects. All 325 clean-trigger windows were joined to
   966 official Nasdaq halt rows with zero overlaps. Broker-specific historical
   tradability remains unreconstructable and is explicitly prospective.
8. **Unchanged-champion input readiness:** independently inspected `READY`.
   Zero pairs survive the resolved hard-gate cascade without outcomes. Only one
   pair has a verified positive primary catalyst, and it fails spread and chase.
   Fifty-three pass non-catalyst execution geometry, but zero pass all measured
   market, stop, and resistance proxies. This is readiness and falsification
   evidence, not alpha.
9. **Exact SIP bar and prefix-VWAP semantics:** independently inspected
   `READY`. A frozen condition-aware implementation rebuilt open, high, low,
   close, reported volume, eligible trade count, and WAP against the provider
   oracle on all 325 crossing minutes and 365,379 raw trades with zero
   unsupported conditions. In every minute, WAP-eligible volume differed from
   reported volume, proving that exact intraminute/session VWAP requires the raw
   condition-aware trade prefix rather than `bar.wap * bar.volume`.
10. **Registered hypothesis and experiment lifecycle:** implemented.
11. **Exact pre-entry structure semantics:** independently inspected `READY`.
    All 249 long-history requests and 325 premarket windows are terminal; 255
    trigger records derive without lookahead, 153 fit the unchanged 0.8% stop
    cap, and 89 pass both stop and resistance geometry. Missing quote/history
    inputs remain unresolved rather than favorable.
12. **Three-axis champion/challenger evidence:** implemented.
13. **Operating cadence and monitoring:** implemented.
14. **Disjoint scanner expansion:** in progress. A seeded 100-date H1-2026
    sample excludes every v4 target and permits no substitution. Its 133-session
    acquisition graph can reuse 113 independently attested v4 inputs while
    collecting the exact new-master symbol delta; 20 sessions require a fresh
    full-universe pull. Reference identity collection precedes market freeze,
    and no trigger, catalyst outcome, or strategy variant is part of this stage.

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

The first outcome-blind unchanged-champion readiness join, exact intraminute
VWAP semantics, and deterministic stop/noise and resistance contracts are now
independently inspected. The structure layer replaces misleading proxies but
does not change v3 or expose outcomes: 89 of 255 derivable trigger records pass
both unchanged geometry gates, while 70 fail closed before derivation. The next
objective is to acquire at least 100 previously uninspected dynamic scanner
dates with direct catalyst sources and apply the exact input contracts there.
Broker-specific tradability remains a prospective execution-qualification
requirement. Evaluate unchanged v3 first on that new sample; earn at most one
preregistered revision only if its deployment capacity or net expectancy fails.
See `SCANNER_EXPANSION.md`, `SCANNER_REPLAY.md`,
`SELECTED_CANDIDATE_JOIN.md`, `SELECTED_CANDIDATE_FIDELITY.md`,
`CHAMPION_INPUT_FIDELITY.md`, `CHAMPION_INPUT_READINESS.md`,
`SIP_BAR_AGGREGATION.md`, `PREENTRY_STRUCTURE.md`, and
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
