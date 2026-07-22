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

Its exact development selection is now frozen and independently rebuilt:
100 disjoint sessions from 2023-01-26 through 2024-12-24, 474 required
target/lookback sessions, selected-date hash `8712bb4d...84d187`, and no
substitution or outcome access. The active controller phase is
`DEVELOPMENT_ACQUISITION`. `challenger_orb_retest_acquisition.py` supplies the
next outer freeze: it binds the unchanged full-universe scanner engine to the
retest trigger, primary-source semantic rules, selected-symbol detail limits,
capacity reserve, and outcome lock. Its independent inspector requires zero
target market artifacts. The inner scanner manifest and outer acquisition
manifest must both be committed and pushed before target Alpaca collection.
All challenger provider operations now pass through one repository-native
single-writer lock, including the long full-universe Alpaca scanner collection.
A separate partial-cache inspector independently rebuilds raw and logical
snapshot hashes and rejects unexpected or temporary files before those
identities can become a security master.

That identity stage has completed and independently reconstructs all 100 exact
snapshots, a 6,797-record point-in-time master covering 6,561 instruments, and
2,499 split events. The combined audit reports zero target-market artifacts,
keeps outcomes locked, and preserves the storage reserve. The master and input
attestations remain a pre-scanner evidence slice that must be published before
the inner scanner request graph is frozen.

The inner Alpaca scanner graph is now frozen as manifest `8d1f67bb...c30e41`:
474 required sessions, 6,525 point-in-time symbols, and zero provider requests,
derived rows, or canonical merges. It remains a zero-state contract only and
must be published before the outer challenger trigger/source-rules freeze.

Outer acquisition manifest `b4d49ce7...c9102` now independently rebuilds the
published scanner-lock implementation, inner scanner graph, one-trial retest
trigger, primary-source semantic rules, selected-symbol-only detail boundary,
provider priority, storage reserve, and outcome lock. Its zero-state inspection
covers all 100 dates and 474 sessions without substitution or target-market
artifacts. The scanner dataset is publicly registered as `COLLECTING`; target
Alpaca access begins only after this outer slice is committed and pushed.

Scanner acquisition v1 subsequently reached 466/474 independently valid
sessions with 56,776 provider requests, zero retries, zero invalid sessions,
and no outcome access. The first reusable-source session failed closed because
eight attested inherited rows were absent from the immediate source-master
coverage set and were therefore also classified as provider deltas. The
duplicate guard prevented a mixed or duplicated file. A v2 pre-outcome recovery
lineage preserves the v1 artifacts publicly, binds the adapter repair, selects
only complete hash-attested v1 sessions that exist before freeze, and leaves
every missing session provider-bound. Dates, strategy rules, providers,
substitution policy, source semantics, and outcome locks remain unchanged; the
repair code must be committed and pushed before the v2 inner and outer
manifests are frozen.

The v2 inner scanner contract is now frozen as manifest
`77dc80b5...b96640`. Its empty target root, 474-session graph, 6,525-symbol
target union, exactly 466 pre-freeze attested v1 sessions, eight missing
provider-bound sessions, zero substitutions, and closed outcome lock rebuild
from the manifest and external zero-state status. It must be committed and
pushed before the v2 outer acquisition contract is frozen.

The v2 outer acquisition contract is now independently frozen as manifest
`b580483b...9d8e9` and registered `COLLECTING`. Independent inspection
rebuilds the 100 exact target dates, all 474 sessions, the 466/8 reusable versus
provider-bound partition, every implementation and upstream hash, primary
source semantics, shared scanner lock, storage reserve, zero substitutions,
zero v2 target-market artifacts, and the closed outcome lock. This slice must
be committed and pushed before coarse scanner collection resumes.

The v2 coarse scanner dataset is now independently `READY`. Collection
materialized all 474 exact sessions, reused 466 frozen attested sessions, made
969 Alpaca SIP requests for the remaining eight with zero retries, and retained
zero invalid session. Independent inspection recomputes 536,967 point-in-time
universe evaluations, 4,019 coarse-eligible rows, 1,826 dynamic shortlist
selections across all 100 dates, every rank and shortlist hash, all source
attestations and split adjustments, and 2,492,864 canonical documents. The
result remains outcome-blind and authorizes only a separately frozen
selected-symbol causal-detail and primary-source acquisition boundary.

The challenger-specific selected-pair freezer and its independent inspector now
implement the next privacy and contamination boundary. The freezer binds the
exact v2 scanner/inspection lineage and one-trial hypothesis, stores all 1,826
date-security rows outside Git, exposes only counts and hashes, enforces the
configured reserve and zero downstream artifacts, and keeps outcomes closed.
The inspector separately reconstructs every row and daily shortlist directly
from the private scanner detail, verifies all source, implementation,
publication, mechanism, access, capacity, privacy, and outcome contracts, and
refuses tampered private state. These implementations must be committed and
pushed before freezing the exact selected-pair manifest; that inspected
manifest must itself be committed and pushed before any selected-symbol causal
or primary-source request graph is frozen or collected.

That selected-pair boundary is now independently `FROZEN_READY` as manifest
`24ef6d29...965de4`. Independent reconstruction matches all 1,826 rows across
100 dates, private graph hash `3ed15493...183c2`, daily-shortlist set hash
`887d1f12...ab5a5`, and source-detail hash `5e9dcb08...d21b`, while finding
zero downstream artifact, substitution, public selected identity, or outcome
access. The next stage remains a separately implemented, frozen, inspected,
committed, and pushed exact request graph for selected-symbol causal inputs and
primary sources; this selected-pair manifest alone authorizes no provider
request.

The generic primary-source semantics contract builder now composes both the
legacy development selection schema and the challenger's modern selected-pair
outcome lock. The modern path is accepted only when point-in-time primary
evidence is restricted to selected symbols and substitution, post-entry rows,
returns, and outcomes are all forbidden; tests also prove that weakening any
required lock fails closed. This implementation must be committed and pushed
before freezing the challenger's real source-semantics manifest, which must in
turn be independently inspected and published before an exact SEC/issuer
request graph is allowed.

The exact challenger source-semantics contract is now frozen as manifest
`60a7d000...299ce9` for all 1,826 selected pairs across 100 dates. It binds the
private selection and daily-shortlist hashes, unchanged source rules, current
implementation and dependency versions, scanner and identity attestations,
recovery order, privacy boundary, 20-GiB reserve, zero target artifacts, and
the separate-outcome lock. Independent inspection now marks the contract
`FROZEN_READY` after rebuilding every private pair and daily hash and matching
the complete source, implementation, capacity, privacy, and outcome boundary.
It confirms zero target sources, selected-symbol detail, substitution, or
outcomes. This status must be committed and pushed before the exact SEC/issuer
request graph is frozen; no provider request or source claim is authorized yet.

Deterministic extraction and independent review of the later 508-document
contract now classify the complete 1,826-pair denominator. Ten pairs are
verified positive, 10 are verified conflicts, five are verified negative, 60
are non-material, 1,369 have no selected SEC source, and 372 remain
`DOCUMENT_SEMANTICS_UNRESOLVED` across 424 pair/document joins. Because the
positive-capacity minimum is 20, the outcome contract remains closed and the
frozen recovery precedence advances to exact SEC accession-chain recovery.
No return, target-session outcome, alpha, maturity, production, or broker claim
is authorized by this source-only result.

The challenger recovery adapter now gives that source-only result an isolated
dataset namespace while requiring the unchanged v3 accession-chain engine hash
`e8800c0c...0112ac`. It fixes the recovery surface at 372 pairs, 433 complete
pair/accession joins, and 417 unique accessions, and records the adapter itself
as the effective implementation. A separate inspector reuses the proven
zero-response reconstruction and additionally verifies these challenger
counts. Both implementations must be committed and pushed before freeze; no
provider request is permitted until the resulting manifest independently earns
`FROZEN_READY` and is itself committed and pushed.

Recovery manifest `562266f3...1b3515` now freezes the complete 372-pair,
433-join, 417-accession graph before provider access. It binds the published
source-semantics result, private review and selection hashes, exact SEC request
semantics, effective adapter and upstream classifier hashes, unchanged terminal
precedence, 20-GiB reserve, privacy, zero substitution, and closed outcomes.
The target namespace was empty at freeze. This uninspected manifest must be
committed and pushed before independent zero-response reconstruction.

Independent recovery inspection now marks manifest `562266f3...1b3515`
`FROZEN_READY`. It reconstructs every one of the 372 pair identities, 433
joins, and 417 exact requests; verifies the upstream lineage, adapter and rule
hashes, privacy, capacity, and outcome locks; and confirms zero response,
collection-index, or reviewed-result artifacts. The READY status must be
committed and pushed before collection.

The network-free challenger SEC identity graph is now frozen as manifest
`ddf298d1...227860`. It exactly maps all 1,826 selected pairs, preserves eight
missing-CIK rows, and privately binds 529 SEC-operated submissions requests,
precise acceptance-time rules, forms, pacing, retry, cache provenance, failure
isolation, capacity, privacy, zero substitution, and the outcome lock. Its
target response namespace is empty. The `COLLECTING` manifest was committed
and pushed before independent inspection. That inspection now rebuilds the
complete graph as `FROZEN_READY` with the same 1,826/1,818/8/529 counts and
zero submissions, primary documents, substitutions, or outcomes. Its READY
status must be committed and pushed before a separately bound collector may
access the 529 SEC submissions responses.

The committed collector has now completed all 529 requests with 451 cache
hits, 78 SEC downloads, zero failures, zero pending, and zero substitutions.
Independent inspection reparses and rehashes all 88,806,127 source bytes and
rebuilds 498 candidate filings, 516 pair/filing joins, 453 source-bearing
pairs, and 715 historical supplemental descriptors. These are discovery
counts, not verified catalysts. Supplemental files and primary documents stay
locked behind separate exact manifests, and no causal input or outcome was
accessed.

The supplemental descriptor reduction is now frozen as manifest
`c6d329b3...7f68af`: all 715 descriptor decisions retain 20 exact window
overlaps and exclude 695 non-overlaps, with malformed ranges included
conservatively. The graph binds private decisions, exact requests, source
lineage, implementation, capacity, privacy, zero substitution, and the outcome
lock with zero supplemental responses. The earlier uninspected `COLLECTING`
manifest was committed and pushed before inspection. Independent reconstruction
now marks it `FROZEN_READY` with all 715 decisions, 20 selected requests, 695
exclusions, source and implementation bindings, capacity, privacy, zero
substitution, and outcome locks intact. Its READY status must be committed and
pushed before the bounded supplemental collector may run.

That collector now completes and independently rebuilds all 20 requests with
20 SEC downloads, zero failures, zero pending, and zero substitutions. It
rehashed 6,346,357 source bytes and contributes 10 additional time-window
candidate documents and 10 joins across four pairs. These are discovery
metadata only. The union with the 498 main-submissions candidates must be
frozen before any primary document access; semantic and outcome locks remain
closed.

The combined primary-document graph is now frozen as manifest
`4eb3f9c4...85e5cc`: 498 main plus 10 supplemental candidates become 508
exact accession-bound SEC requests with all 526 pair/document joins preserved
across 457 pairs. Conflicting duplicates fail closed, source identities remain
private, and the target document namespace is empty. The earlier `COLLECTING`
manifest was committed and pushed before inspection. Independent reconstruction
now marks it `FROZEN_READY` with every request, join, lineage binding, capacity
gate, privacy lock, and zero-response invariant intact. Its READY status must
be committed and pushed before the bounded primary-document collector may run.

The published collector has now completed all 508 requests with 508 downloads,
zero failures, zero pending requests, and zero substitutions. Independent
inspection rebuilt every terminal wrapper, rehashed 21,161,872 raw source
bytes, and confirmed private collection hash
`05a64282f41af040e7224a4c27a8c325fa87837cb1db9f10d1ed1694239260b3`.
No source semantic, verified catalyst, causal market input, return, or outcome
was derived. A separately frozen and inspected semantics contract is required
before classification.

Semantics manifest `b3c4d0ca...6ed93` now binds deterministic classification of
the 508 documents, 526 pair/source joins, and 1,369 explicit no-source pairs.
The exact rules, implementation hashes, private selection identity, terminal
precedence, 20-pair capacity gates, privacy boundary, and outcome lock are
frozen. It must be independently inspected and pushed before extraction or
classification.

Independent zero-result inspection now marks the contract `FROZEN_READY`, with
all 1,826 pair identities, 508 document identities, 526 joins, private hashes,
implementation bindings, and outcome locks intact. Extraction remains blocked
until that READY status is committed and pushed.

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
