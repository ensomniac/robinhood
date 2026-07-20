# Persistent Production-Strategy Validation Campaign

Campaign version: `2026-07-19-v1`

Production champion: `2026-07-15-orb-v3`

Current maturity authority: `strategy_ledger.py report`

## Completion Contract

This campaign remains active across bounded Codex invocations until the current
production champion machine-earns `VALIDATED`. A market-hours wait, provider
failure, subscription decision, required broker confirmation, safety pause, or
need for more naturally occurring sessions is a resumable state and never a
successful terminal result.

Terminal completion requires all of these facts at the same time:

- the ledger reports `earned_maturity="VALIDATED"` for the current rules hash;
- the registered champion is `CONFIRMED`, `LIVE_CALIBRATED`, operationally
  `READY`, and its computed readiness is `VALIDATED`;
- at least 50 eligible closed signals include 20 untouched confirmation
  signals, ten live executions, and five naturally occurring live stops;
- overall and confirmation expectancy are positive, profit factor is at least
  1.30, the one-sided 90% bootstrap lower mean R is positive, and maximum
  drawdown is at most 6R;
- entry-slippage p95 is at most 15 bps, unprotected-exposure p95 is at most ten
  seconds, stop slippage is inside its reserve, rule violations are zero, and
  capture and execution records are complete;
- registries, ledger, lifecycle, learning data, security master, progress,
  privacy, historical-store capacity, and controller lineage pass audit;
- a fresh private broker safety snapshot reports reconciled flat state with no
  position, open order, or unknown order; and
- the public worktree is clean and `HEAD` equals its configured upstream.

Validation is evidence that the frozen production contract passed the
repository's alpha, execution, operations, and integrity thresholds. It is not
a guarantee of future profitability.

## Persistent Controller

`strategy_validation.py` owns only campaign coordination. Its append-only,
hash-chained operational events and current-state projection live under the
ignored `learning_runs/production_validation/` directory. Every event binds the
champion ID, strategy version, rules hash, this plan's hash, core upstream
artifact hashes, phase, objective, blocker, next action, evidence hashes, and
the latest privacy-safe broker safety snapshot.

The controller composes the existing strategy registry, ledger maturity report,
learning-data and registry audits, lifecycle state, privacy audit, progress
history, historical-store reserve, Git state, and private run state. It does not
replace any of them. It cannot contact a data provider, access Robinhood, submit
an order, edit a production rule, or promote a strategy axis.

Use it from the repository root:

```sh
python3 strategy_validation.py init
python3 strategy_validation.py status
python3 strategy_validation.py next
python3 strategy_validation.py record \
  --phase SOURCE_RECOVERY \
  --status READY \
  --objective recover-primary-source-failures \
  --next-action "Freeze the accession-bound SEC recovery contract." \
  --evidence research_results/<inspected-result>.json
python3 strategy_validation.py audit
```

`record` cannot accept phase `VALIDATED`. Only `audit` can append that terminal
event, and only after recomputing every completion condition. A champion,
strategy-version, or rules-hash change automatically restarts the active sample
at `DEVELOPMENT_ACQUISITION`; prior public evidence remains intact but cannot
promote the new rules.

The phases are:

1. `SOURCE_SEMANTICS`
2. `SOURCE_RECOVERY`
3. `DEVELOPMENT_ACQUISITION`
4. `DEVELOPMENT_OUTCOMES`
5. `CHALLENGER_REVIEW`
6. `CONFIRMATION`
7. `SHADOW_QUALIFICATION`
8. `LIVE_PILOT`
9. `PROMOTION_AUDIT`
10. terminal `VALIDATED`

Nonterminal statuses are `READY`, `WAITING_MARKET`,
`WAITING_USER_CONFIRMATION`, `WAITING_PROVIDER`, `WAITING_SUBSCRIPTION`,
`WAITING_NEW_SESSIONS`, and `PAUSED_SAFETY`. `next` emits one bounded handoff
for the active agent under `AGENTS.md`; it never performs the handoff itself.

## Source Semantics And Capacity Gate

The first active objective is the frozen 33-pair, 33-document, 38-join contract
in `CATALYST_SOURCE_SEMANTICS_PLAN.md`. `catalyst_source_semantics.py` must
freeze exact inputs and implementation hashes, derive private one-terminal-row
evidence without outcome access, support explicit human/agent review fields,
and independently rebuild its public aggregate.

Terminal disposition precedence is transport or HTTP failure; ownership
unresolved; issuer binding unresolved; irrelevant source; timestamp missing;
timestamp conflict; same-day time unresolved; published after 09:35;
semantics unresolved; nonmaterial; verified conflict; verified negative; and
verified positive. Financing or dilution conflicts run before positive
classification. Metadata, headers, capture time, URL dates, and PDF creation
dates never establish causal publication alone, and same-day date-only evidence
fails.

If fewer than 20 verified-positive catalysts remain, close outcome-blind and
enter `SOURCE_RECOVERY`. If catalyst capacity reaches 20 but fewer than 20 rows
survive the unchanged-v3 non-return gates, enter `DEVELOPMENT_ACQUISITION`. Only
20 or more complete survivors authorize a separately frozen outcome contract.

Recovery order is accession-bound SEC-operated endpoints with a compliant user
agent, frozen retries of transport failures, then canonical issuer-host document
chains. Secondary news is never substituted for a primary source. If existing
sources cannot produce capacity, collect exact disjoint 100-session tranches
with frozen dates, point-in-time universe, security master, splits, provider
queries, source rules, and zero substitution. Use the local store first, retain
whole-provider fidelity, collect full-universe coarse inputs plus selected-name
detail, and preserve the configured 20-GiB reserve. A demonstrated need for a
paid archive produces a vendor/tier memo and `WAITING_SUBSCRIPTION`; the agent
does not purchase access or fabricate credentials.

For the second disjoint tranche, all 557 primary-document requests succeeded,
but the frozen review retained 19 verified-positive pairs and 377 causal,
exact-CIK-bound pairs whose filing document was semantically unresolved. The
complete unresolved surface contains 436 joins to 417 unique accessions, mostly
8-K/6-K Item 9.01 stubs. `development_sec_accession_chain_recovery.py`
therefore freezes each exact SEC complete-submission endpoint before access and
applies the unchanged v3 rules only to issuer-filed `EX-99` exhibits. This is a
primary accession document-chain recovery, not a new mechanism or favorable
symbol selection. It preserves capture failures, missing exhibits, financing
conflicts, the original no-source denominator, aggregate privacy, and every
outcome lock. Provider access requires the implementation and then its exact
manifest to be committed and pushed in separate slices.

That accession-chain collection is now independently inspected: 417/417 exact
requests succeeded with no substitution, 577 `EX-99` source rows rebuilt, and
83 additional pairs reached verified positive under the frozen rules. Exact
deduplication with the prior 19 produces 102 combined positive pairs. This
passes the source-capacity gate only. Outcomes remain locked while a separately
frozen `DEVELOPMENT_ACQUISITION` contract applies every unchanged-v3 non-return
gate and proves at least 20 complete survivors.

`development_non_return_v3.py` implements that next network-free boundary for
the exact 102 combined positives. It privately deduplicates the 19 prior and 83
recovered positive hashes, rejoins them to the full 1,871-pair scanner surface,
rechecks coarse unchanged-v3 gates, and freezes only causal pre-entry request
classes through the final +10-second decision snapshot. The resulting graph
covers 102 pairs on 52 dates and retains zero target artifacts. Manifest
`7d5be928...db829` independently rebuilds that frozen graph, its private
selection, strategy and rules hashes, implementation bindings, aggregate
privacy boundary, and outcome lock. It authorizes only a separately frozen
provider collector, which must be committed, inspected, and pushed before
collection.

`development_non_return_collection_v3.py` supplies that isolated provider
boundary without editing the completed 21-pair collector. Its scoped adapter
binds both collector implementations, the v3 source contract, exact 102-pair
lineage, private namespace, tranche-v3 outputs, raw Alpaca SIP fidelity, full
calendar and split basis, 20-GiB reserve, aggregate privacy, and the closed
outcome lock. Read-only preflight finds zero target artifacts; the adapter must
be committed and pushed before its own manifest can be frozen. Collector
manifest `cded09e9...e48b6` now independently rebuilds that exact boundary as
`FROZEN_READY`; provider access remains false until the manifest is committed
and pushed.

Provider collection and independent inspection are now complete for that
manifest. All 102 pairs reached one terminal pre-entry disposition through
109,217 chronological one-second windows: 75 have complete raw pre-entry
inputs and 27 have no clean cross before 10:30. The inspector rebuilt every
pair hash, chronological cursor, final-decision boundary, implementation and
manifest binding, privacy boundary, and outcome lock. No post-decision row or
target outcome was accessed. These 75 records are input-ready only; a separate
frozen network-free qualification must still prove at least 20 complete
unchanged-v3 survivors before any outcome contract is legal.

That qualification boundary is now implemented in
`development_non_return_qualification_v3.py` with a separate per-pair
reconstruction in `development_non_return_qualification_v3_inspection.py`. It
binds the inspected raw collection, exact SIP VWAP, unchanged quote/spread/chase
and market gates, frozen stop/noise/resistance structure, the production
evaluator, terminal precedence, aggregate privacy, and the outcome lock. Its
normalized shadow reference session isolates candidate evaluation without
claiming broker facts; broker-specific tradability remains a prospective gate.
The implementation must be committed and pushed before its contract can be
frozen, and only the independent inspector may authorize a later outcome
contract after at least 20 survivors.

That qualification contract is now independently frozen in manifest
`558001a2...214e2`. It fixes the exact upstream collection hashes, 102-pair
denominator, 75 input-ready records, unchanged v3 rules and implementations,
ordered attrition, 20-survivor threshold, private gate-record boundary, and
closed outcome lock. No qualification result or target artifact existed at
freeze. Evaluation remains prohibited until this manifest and its public
aggregate status are committed and pushed.

The v1 builder result failed independent inspection because its persisted JSON
arrays were compared directly with semantically identical in-memory evaluator
tuples. No gate value or terminal reason was accepted from that failed result,
and outcomes remain locked. The v1 artifact is retained as failed evidence; a
new v2 qualification dataset will freeze the canonical JSON comparison repair
before reevaluation.

V2 isolates its manifest, private result, aggregate status, and inspection
artifact from v1. Its only intended semantic change is canonical persisted-JSON
comparison during independent inspection; all v3 gates, upstream inputs,
terminal precedence, privacy boundaries, and outcome locks remain unchanged.

V2 manifest `0140ac92...7c4f0` now independently rebuilds as
`FROZEN_READY` with zero v2 target artifacts. It is the only active
qualification contract; evaluation is prohibited until this exact manifest and
its aggregate contract status are committed and pushed.

Independent v2 qualification rebuilds every one of the 102 pair records and
confirms zero unchanged-v3 survivors without post-entry access. The terminal
counts are 27 no-cross, 23 unresolved-input, 38 quote, four A+-spread, one
chase, and nine resistance failures. The result fails the minimum 20-survivor
gate, keeps outcomes locked, and falsifies unchanged v3 deployability on this
source-verified corpus. V3 is not tuned on the failed evidence; the next phase
is `CHALLENGER_REVIEW` under the frozen weekly mechanism-family limits.

The sole active challenger is now the preregistered catalyst ORB retest family
in `CHALLENGER_ORB_RETEST.md`, contract `b766000b...6d105`. It contains one
trial and changes only the causal entry mechanism: observe the first clean ORB,
then require a completed retest hold and rebreak. Champion v3 remains unchanged;
all portfolio, risk, stop, liquidity, protection, session, cost-stress, and
outcome-lock constraints remain in force. Every previously inspected target
date is excluded from its future development and confirmation samples.

The first corpus has completed that ordered recovery with only three exact
deduplicated positive pairs, so its outcomes remain locked. The active
`DEVELOPMENT_ACQUISITION` handoff is implemented by
`development_tranche.py` and documented in `DEVELOPMENT_TRANCHE.md`. The
selection implementation is committed before freeze, its manifest is committed
before reference access, and the later provider-bound scanner manifest is
committed before market collection.

The first disjoint tranche later produced 21 source-verified positives, but its
independently inspected causal pre-entry collection retained only 19 complete
inputs; two pairs had no clean cross before 10:30 across 13,219 exact
one-second windows. No target outcome was accessed. Because the 20-survivor
gate is impossible for that frozen corpus, the active handoff remains
`DEVELOPMENT_ACQUISITION` for another exact disjoint 100-session tranche.

## Development, Challenger, And Confirmation Gates

Outcome access starts only after an immutable contract freezes executable entry,
missed-fill, partial-data, bid/ask and tape ordering, 5-bps-per-side primary
cost, 10- and 20-bps stress, stop-first ambiguity, gap-stop fills, stop reserve,
+2% milestone, runner rules, and 15:50 force-flat behavior. No-trade sessions,
misses, unavailable rows, and rejects stay in denominators. Inspected records
publish only through the atomic ledger and lifecycle paths.

Development must have positive expectancy, profit factor at least 1.30, a
positive one-sided 90% bootstrap lower mean R, drawdown at most 6R, positive
chronological halves, positive performance without the five best trades,
positive total R with profit factor at least 1.20 and drawdown at most 6R under
both stress grids, and zero rule, capture, or evidence violations.

If the champion fails, record its exact falsification without tuning it on the
failed corpus. Keep every hard portfolio and protection constraint. Permit one
active challenger and at most three new mechanism families per ISO week. Freeze
one causal change, its parameter family, cost grid, falsification criteria,
compatibility risks, and contamination risks before evaluation. A failed
confirmation retires that exact challenger. A passing challenger becomes a new
production version with atomic evaluator, config, tests, docs, and registry
changes, then restarts maturity evidence under its new rules hash.

Untouched confirmation freezes chronologically separated dates before outcome
access, applies an embargo of at least one session, changes no champion rule,
and requires at least 20 eligible signals with the same alpha, robustness,
stress, drawdown, capture, and integrity gates. Confirmation ledger rows use
`sample_phase="confirmation"` and `mode="shadow"`.

## Shadow, Live, And Promotion

Before live pilots, collect at least five complete eligible prospective shadow
executions through scanner, catalyst, trigger, quote/book, evaluator, guard,
order construction, protection, monitoring, and journal paths with zero
violations or unresolved failures. Only then may the registry execution axis
become `SHADOW_VERIFIED`.

Eligible later market sessions may enter `live` mode under `AGENTS.md`. Broker
and platform confirmations that require Ryan remain mandatory and produce
`WAITING_USER_CONFIRMATION`. While unvalidated, live pilots retain score 90,
median spread 0.08%, 0.25%-equity planned-risk, 80%-allocation, and every other
hard gate. Stops must occur naturally; no marginal entry or manufactured stop
may satisfy a count.

Unknown orders, missing protection, violations, breakers, integrity mismatches,
and monitoring failures pause entries, prioritize reconciliation or flattening,
retain the adverse observation, and require root-cause repair. Execution-budget
failure extends collection after repair. Alpha degradation returns to
`CHALLENGER_REVIEW`. `LIVE_CALIBRATED` is earned only after complete execution
and protection evidence stays within budget.

Every manifest is committed before collection. Every independently inspected
result, registry disposition, reusable finding, production-version update, and
live lifecycle record is committed and pushed after proportionate tests and
privacy audits. Waiting and safety pauses remain active campaign states; only
the completion contract closes the campaign.
