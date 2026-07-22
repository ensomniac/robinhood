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

The exact collector completed 417/417 accession requests with zero failures or
pending work and retained 949,569,801 source bytes privately. A dedicated
collection-only inspector now rebuilds the index, rehashes every terminal
wrapper and raw accession, revalidates transport integrity and the full request
denominator, and requires zero semantic-review artifacts. Its implementation
and the uninspected completion status must be committed and pushed before this
inspection; semantic review remains closed until the inspection is published.

Independent collection inspection now rebuilds all 417 terminal wrappers and
the complete private index, rehashes and transport-validates all 949,569,801
source bytes, matches collection hash `6d89d180...ae393` and wrapper-set hash
`d36c664c...875f`, and confirms zero semantic-review artifacts. Only after this
result is committed and pushed may deterministic issuer-filed EX-99 review run;
returns and outcomes remain forbidden.

Independent EX-99 review now reconstructs 554 recovered source rows over the
complete 372-pair recovery denominator. It finds 85 recovered verified-positive
pairs, 91 conflicts, 19 verified negatives, and 177 still unresolved pairs;
together with the 10 prior positives, the deduplicated source capacity is 95.
That passes the frozen minimum of 20 and advances to
`DEVELOPMENT_ACQUISITION`. Private result hash `fa9e196a...e524f` binds the
review. This source result must be committed and pushed before any selected
causal detail or outcome access; it is not alpha or maturity evidence by
itself.

The challenger pre-entry implementation now joins only the 95 independently
verified-positive pairs to the frozen scanner identities. Those pairs span 47
dates and imply an exact 284-request causal graph: 95 candidate minute-bar
windows, 95 raw SIP-trade windows, and SPY/QQQ minute bars on all 47 dates,
ending at 10:30 ET. The bound retest engine truncates each retained record at
its first failed touch, final decision, or cutoff. Quotes, fills, post-entry
rows, returns, and outcomes remain forbidden. A separate inspector rebuilds
the zero state and trigger results. Both implementations must be committed and
pushed before the graph is frozen.

Pre-entry manifest `e9bc5a8a...846b77` now binds exactly 95 verified-positive
pairs across 47 dates and 284 requests, with private selection hash
`358fcb85...9da5d`, pair hash `44a203a6...f4061`, and request graph
`c2af71c0...1fc9a`. It freezes source and implementation hashes, causal
retention boundaries, one daily entry, the 20-GiB reserve, zero substitution,
and closed quote, fill, post-entry, return, and outcome access. The manifest is
uninspected and must be committed and pushed before zero-state reconstruction.

The pre-entry inspector now marks `e9bc5a8a...846b77` `FROZEN_READY` after
rebuilding all 95 pair identities, 47 dates, 284 requests, source and
implementation hashes, capacity, privacy, and outcome locks. The target state
contains one frozen selection and zero terminal wrappers, collection index, or
trigger index. This READY status must be committed and pushed before collection.

Collection from pushed commit `67101db` now has 284 successful terminal
requests, zero failures or pending requests, and 3,195,784 causal rows. The
public status contains aggregate counts and private hashes only. A separate
network-free inspector will rebuild the collection index, rehash every wrapper
and canonical row, verify the outcome lock and zero-trigger boundary, and must
itself be committed and pushed before inspection or trigger derivation.

The initial inspection failed closed on the generic cache reader's complete-RTH
requirement. The collected bars are deliberately bounded regular-session
windows, so a separate local reader now admits only exact provenance-bound
09:30/09:35-10:30 Alpaca SIP windows. This is a transport adapter only; the
frozen collector and trigger rules remain unchanged, and the adapter must be
committed and pushed before inspection retries.

The independent collection inspection now passes all 284 requests and
3,195,784 canonical rows. It binds private collection hash
`f3a5708f...d3a`, wrapper set `9747be6a...7753`, canonical-row set
`fe752cfa...a36a`, exact implementation hashes, the outcome lock, and zero
trigger artifacts. This inspection must be committed and pushed before causal
trigger derivation.

A trigger compatibility runner and separate inspector now bind the unchanged
trigger rules to the exact-window reader and inspected collection. Their sole
runtime substitution is the local reader class; the engine, causal boundary,
daily cap, and outcome lock are unchanged. The implementation authorizes no
derivation until a new runner manifest is frozen and independently inspected.

Runner manifest `6dc393ae...ed9cd` now freezes the exact compatibility path,
six implementation hashes, all inspected collection bindings, 95 pairs across
47 dates, unchanged causal trigger rules, and the outcome lock. The private
trigger namespace is empty. This uninspected manifest must be committed and
pushed before independent zero-state reconstruction.

Independent reconstruction now marks `6dc393ae...ed9cd` `FROZEN_READY`. Every
source, collection, implementation, compatibility, causal-rule, and outcome
binding matches, the full 95-pair/47-date/284-request denominator rebuilds,
and no trigger artifact exists. This status must be committed and pushed before
derivation.

The pushed runner has produced an uninspected causal trigger index: 29 trigger
pairs on 22 dates, with 23 no-initial-break, two no-retest, and 41 failed-hold
terminations across the complete 95-pair denominator. Private hash
`f8cf5c89...f7b4` binds the index. These counts support no capacity conclusion
until committed and independently rebuilt, and no post-entry outcome was read.

The first result inspection failed closed because the frozen base derivation
also writes to the collection status path. That file is restored to its
published `COLLECTION_INSPECTED` content while the full uninspected trigger state
remains in `trigger-status.json`; private data and evidence hashes are unchanged.

Independent reconstruction now marks the causal trigger result `READY`: all 95
decisions, 284 source requests, terminal counts, and private trigger hash match,
and no post-entry target was read. Its maximum daily capacity is 22 signals,
below the frozen 50-signal development requirement. The exact challenger is not
retired because outcomes remain locked; it continues in
`DEVELOPMENT_ACQUISITION` with another disjoint preregistered session tranche.

The second challenger tranche selector and independent inspector are now
implemented in a new namespace. They preserve the exact mechanism, bind seed
`2026072106`, explicitly include the first challenger selection in the frozen
prior set, and currently identify 345 eligible dates after all public and
private exclusions. No selection may be frozen until these implementations are
committed and pushed.

Second-tranche selection manifest `cfa2e9fe...0d55a7` now binds exactly 100
dates, 527 exclusions, and 530 required sessions from the fixed 345-date pool.
The selected and required identities are privately hash-bound as
`1be9c996...56fe1e` and `fa7c4664...ee2973`. This uninspected selection must be
committed and pushed before independent reconstruction or provider access.

Independent reconstruction now marks second-tranche selection
`cfa2e9fe...0d55a7` `FROZEN_READY`. The exact selection, exclusions, required
session graph, implementation bindings, private hashes, disjointness, and
outcome lock all match. Publication is required before dated reference access.

The second-tranche acquisition adapter and independent inspector now isolate
all provider, master, split, scanner, status, and lock paths without modifying
the frozen first corpus. The exact split window covers the complete required
session graph from `2023-01-03` through `2026-07-17`; the prior complete v2
scanner manifest is the sole reusable lineage. Independent zero-state reference
inspection reports 0/100 ready with no unexpected artifact. Provider access is
blocked until these implementations are committed and pushed.

The exact split boundary is `2023-01-03` through `2026-07-17`. All 100 reference snapshots
now independently rebuild with raw/logical hashes `bb6e5053...f31a52` and
`03c2ffc6...c5533`. Their 7,995-record, 7,521-instrument point-in-time master has
hash `92500496...4ddc2`; 4,933 split events have ignored-artifact hash
`d2b7ea6e...bdbadb`. The combined inspection finds zero target-market artifact,
no outcome access, and a valid 20-GiB reserve. Publication is required before
scanner freeze.

Inner scanner manifest `5805ca2d...2da99` freezes the 530-session,
7,522-symbol zero-state graph with no target-market artifact or substitution.
Its first status reconstruction failed closed because the adapter inherited the
first corpus's default manifest-root argument. The explicit second-tranche root
repair changes no frozen scanner input and no provider request occurred; it must
be published before outer freeze.

Outer manifest `3dbdef70...d0289c` now binds the published zero-state scanner,
470 candidate reusable sessions, 60 provider-bound sessions, the unchanged
retest mechanism, source-rules hash `99843372...b4dc`, selected-symbol limits,
capacity, the shared lock, no substitution, and the closed outcome boundary.
This uninspected artifact must be committed and pushed before reconstruction or
market collection.

The independent outer inspection now reports `FROZEN_READY` from pushed commit
`d89a40e`. It revalidated every implementation and upstream hash, all 100
reference snapshots, 7,995 master rows, 4,933 split events, 530 required
sessions, the 20-GiB reserve, zero target-market artifacts, no substitution,
and no outcome access. This inspection must be published before the exact
scanner collection starts.

The complete second-tranche scanner and its independent result are now
`READY`. The source graph contains all 530 required sessions, 470 from the
frozen reuse set, 10,588 exact Alpaca requests, zero retries, and no invalid
session. The independent implementation recomputed 538,210 universe rows,
3,574 coarse-eligible rows, 1,826 dynamic selections, all source and shortlist
hashes, and 2,799,071 canonical documents. No selected-symbol detail, catalyst,
return, outcome, alpha, confirmation, maturity, production, or broker claim is
earned; the exact selected-pair boundary must be frozen next.

The isolated second-tranche selected-pair freezer and independent-inspector
adapter are now implemented. Their read-only pre-freeze reconstruction matches
all 1,826 unique rows and 100 dates with private selection hash
`173991d5...5b0137`. The new contract explicitly binds both adapters and both
unchanged base implementations; it keeps exact identities outside Git and
allows no downstream causal artifact or outcome. The implementation must be
published before the graph is frozen and inspected.

That graph now independently earns `FROZEN_READY` as manifest
`9cdb9fd0...5a511`. The inspector rebuilds all 1,826 private rows across 100
dates, graph hash `173991d5...5b0137`, daily-shortlist set hash
`d22db8ad...8fbf4`, and source-detail hash `380567f1...5fd57`; exact selected
identities remain outside Git. Zero downstream artifact, substitution, causal
input, or outcome exists. A separate source/causal contract must be frozen and
published next.

The second-tranche source-contract adapter and inspector now bind the complete
1,826-pair private denominator, 100 daily partitions, unchanged primary-source
rules hash `99843372...b4dc`, and an isolated zero-artifact namespace. The
implementation contract includes both adapters, the immutable generic builder,
and the source-semantics parser. The implementation must be committed and
pushed before manifest freeze; source bodies, classifications, causal inputs,
and outcomes remain inaccessible.

Source-semantics manifest `e8463fc8...79f9f` now independently earns
`FROZEN_READY` with all 1,826 selected pairs, 100 daily partitions, unchanged
rules hash `99843372...b4dc`, implementation hash `4b0a203d...2ed4`, and zero
target source artifacts. It authorizes only a separately frozen and published
SEC/issuer identity/request graph; source bodies, semantic classifications,
causal inputs, and outcomes remain locked.

SEC identity manifest `83e21d3e...e1f638` now independently earns
`FROZEN_READY`. It exactly maps all 1,826 private pairs to the attested
point-in-time master, retains two missing-CIK rows and four listing-scoped
joins, and freezes 537 SEC-operated submissions requests. Private identity
hash `041d8a4d...ba7f5`, request-graph hash `6ad3c618...40991`, and daily
aggregate hash `7af5b4f6...d8f7c` bind the zero-response graph. No submission,
primary document, source classification, causal input, substitution, or
outcome was accessed. This boundary must be published before collection.

The published bounded collector then completed and independently rebuilt all
537 frozen submissions requests: 494 shared-cache hits, 43 SEC downloads,
zero failures, zero pending requests, and zero substitutions. It rehashed
91,467,903 source bytes and derived 513 time-window filing candidates, 525
pair/filing joins across 452 pairs, and 730 historical supplemental
descriptors. These are discovery identities, not verified catalyst semantics.
Supplemental files, primary documents, causal inputs, and outcomes remain
locked behind later exact contracts.

Second-tranche supplemental manifest `693a1487...7b7e0` now independently
freezes all 730 descriptor decisions before provider access. Exactly 28
descriptors overlap a frozen pair window and 702 are excluded as outside those
windows. Private contract hash `ff66c238...ce90d`, request-graph hash
`662275e8...1f0be`, and decision hash `25fdc404...27cde` bind the exact graph,
while the target namespace contains zero supplemental response artifacts.
Supplemental files, primary documents, semantic classifications, causal
inputs, substitutions, and outcomes remain locked until this boundary is
committed and pushed and the bounded collector is run.

The published collector then completed and independently rebuilt all 28 exact
supplemental requests: 13 shared-cache hits, 15 SEC downloads, zero failures,
zero pending requests, and zero substitutions. It rehashed 9,231,148 source
bytes and contributes 27 time-window candidate documents and 27 joins across
15 pairs. These are discovery metadata only. The union with the 513 main
submissions candidates must be frozen before any primary-document access;
semantic and outcome locks remain closed.

Second-tranche primary-document manifest `6698ac65...b24190` now
independently freezes the complete union: 513 main plus 27 supplemental
observations become 540 exact accession-bound SEC requests, with all 552
source-specific pair/document joins preserved across 467 pairs. There are zero
duplicate observations, private contract hash `c93f2b16...14017`, request hash
`d7258c06...a8891`, join hash `aefe937f...d240c`, and zero target document
artifacts. No document body, semantic classification, causal input,
substitution, or outcome was accessed; collection requires this boundary to be
committed and pushed first.

The published primary-document collector then completed all 540 frozen
requests: 27 shared-cache hits, 513 SEC downloads, zero failures, zero pending
requests, and zero substitutions. Independent inspection rebuilt every
terminal wrapper, rehashed 28,374,135 raw source bytes, and confirmed private
collection hash `5b923be6...f269c`. Raw availability is not semantic evidence;
ownership, issuer binding, causal timing, direction, materiality, conflicts,
selected-market inputs, and outcomes remain unclassified and locked.

Second-tranche semantics manifest `cd0e30db...f784e` now independently
freezes deterministic review of the complete 1,826-pair denominator: 540
documents, 552 pair/source joins across 467 source-bearing pairs, and 1,359
explicit no-source pairs. Pair identity hash `39bb6704...36994`, source hash
`19bb1dbb...3bdb`, join hash `e6265d42...e79c6`, no-source hash
`24204926...bb393`, and private selection hash `bf8505c6...3f443` bind the
zero-derived-artifact state. Source text, classifications, causal market
inputs, substitutions, and outcomes remain locked until this manifest is
committed and pushed.

Independent semantic inspection then rebuilt all 540 documents and 552 joins
over the full 1,826-pair denominator. Pair dispositions are 18 verified
positive, 17 conflict, nine negative, 58 non-material, 365 document-semantics
unresolved, and 1,359 without a selected SEC source. The 18 positives miss the
frozen minimum of 20, so the only permitted next step is exact accession-chain
recovery over the 365 unresolved pairs. Private result hash
`3a69fee7...e63d2` binds the review; outcomes remain unobserved and forbidden.

An isolated second-tranche accession-chain adapter now binds those exact 365
unresolved pairs, 434 prior joins, and 425 unique accession requests to the
unchanged base engine hash `e8800c0c...12ac`. Its preflight selection rebuilds
pair hash `19a7a69b...e197`, request hash `f114ba25...7671`, and join hash
`2199bf53...a31e` with the 18 prior positives and no outcomes. Separate
zero-response and collection inspectors are included. The implementation must
be committed and pushed before a recovery manifest may be frozen.

Accession-chain manifest `62acdbbc...cd0fe` now independently earns
`FROZEN_READY`. It preserves the same 365-pair, 434-join, 425-accession surface,
all upstream and implementation hashes, the exact EX-99 review rules, the
20-GiB reserve, aggregate-only privacy, and zero collection or review
artifacts. Publication authorizes only the 425 exact SEC requests; source
semantics, returns, outcomes, alpha, maturity, production, and broker claims
remain blocked.

The published recovery collector completed all 425 requests with 21 shared
cache hits, 404 SEC downloads, zero failures, zero pending requests, and zero
substitutions. Independent transport inspection rebuilt the index, rehashed
all terminal wrappers and 967,855,132 raw source bytes, and confirmed private
collection hash `c484921c...a79ac` and wrapper-set hash
`5b880029...0c7c3`. Zero review artifact exists. Deterministic EX-99 review may
run only after this collection inspection is committed and pushed; outcomes
remain locked.

Independent EX-99 inspection then rebuilt 575 recovered source rows across all
365 recovery pairs. It classifies 95 pairs verified positive, 106 conflict, 11
negative, and 153 unresolved. The 95 recovered positives plus 18 prior
positives produce 113 deduplicated verified-positive pairs, passing the frozen
minimum of 20 and moving this tranche to `DEVELOPMENT_ACQUISITION`. Private
result hash `01a88158...479c8` binds the review; returns, target-session
outcomes, alpha, maturity, production, and broker actions remain forbidden.

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

The disjoint second challenger tranche has now cleared its source-evidence
capacity gate with 113 independently rebuilt positive pairs across 55 sessions.
Its causal pre-entry acquisition is implemented as a hash-pinned adapter over
the unchanged first-corpus engine, with a separate dataset identity, private
artifact path, public status, and independent contract and transport
inspectors. Network-free preflight produces exactly 336 requests: 113 candidate
bar windows, 113 candidate trade windows, and 110 SPY/QQQ benchmark windows.
The outcome lock remains closed; this implementation alone grants no market
access, trigger, return, alpha, maturity, production, or broker claim.

Pre-entry manifest `5a6ac82d...ec9108` now freezes those 113 pairs, 55
sessions, and 336 requests before selected-market access. Private selection
hash `f5f4801c...3385fe`, pair hash `ba33b6f0...a2ae44`, and request hash
`3342764e...f4e66e` bind the complete graph. The manifest is uninspected and
must be committed and pushed before independent zero-response reconstruction;
collection and trigger derivation remain blocked.

Independent reconstruction now marks the second-tranche graph
`FROZEN_READY`: all 113 pairs, 55 dates, 336 requests, source and
implementation hashes, and outcome locks match. The private target contains
one frozen selection and zero wrappers, collection indexes, or trigger indexes.
This READY state must be committed and pushed before the exact bounded
collection may begin.

The published collector has completed all 336 frozen requests with 336 Alpaca
downloads, zero failures, zero pending requests, and zero substitutions,
retaining 4,325,297 causal rows through 10:30 ET. Private collection hash
`c8471a11...d65e6` binds the uninspected transport result. Independent
network-free reconstruction of every wrapper and canonical row is required
before trigger derivation; outcomes remain locked.

Independent transport inspection has now rebuilt the complete index, all 336
wrappers, and all 4,325,297 canonical rows. Wrapper-set hash
`19f744a1...64c67` and canonical-row-set hash `f84d8f98...23d39` bind the
result, while the trigger namespace remains empty. Publication of this
inspection permits only a separately frozen compatibility-bound trigger run;
fill and outcome access remain forbidden.

That trigger run is now implemented as a second-tranche adapter over the
hash-pinned first runner. It binds the exact-window reader, the unchanged
`catalyst-orb-retest-v1` engine, the inspected 336-request collection, and the
second-tranche result inspector. Network-free preflight confirms 113 pairs, 55
dates, 4,325,297 causal rows, and zero trigger artifact. This implementation
must be committed and pushed before its compatibility manifest is frozen.

Trigger manifest `99ab24f7...5760fe` now freezes that exact compatibility
boundary, including the inspected collection digests and all runner, reader,
pre-entry, trigger, and result-inspector hashes. It permits no trigger-rule
change and retains the complete outcome lock. The manifest is uninspected and
must be committed and pushed before zero-artifact reconstruction.

Independent reconstruction now marks trigger manifest `99ab24f7...5760fe`
`FROZEN_READY`. The complete source and implementation bindings match, the
only runtime substitution is the exact-window reader, and the private trigger
namespace remains empty. This READY state must be committed and pushed before
causal derivation.

The published runner has produced an uninspected causal denominator: 23
triggering pairs on 22 distinct dates from all 113 positive pairs, with private
trigger hash `c6303e81...0cb4c3`. Terminal reasons retain every pair. No
post-entry data or target outcome was accessed. Independent reconstruction is
required before this count can contribute to cumulative development capacity.

Independent result reconstruction now confirms all 113 decisions, all terminal
reasons, and private trigger hash `c6303e81...0cb4c3`. The second tranche
contributes 22 distinct daily signals. Combined with the first disjoint
tranche's 22 inspected daily signals, the unchanged challenger has 44 causal
development-capacity signals, six below the immutable minimum of 50. Outcomes
remain locked and a third exact disjoint acquisition tranche is required; no
parameter repair is permitted.

The third acquisition selector is now implemented as a hash-pinned adapter
over the proven second-tranche selector. It adds both prior frozen selections
to the complete repository exclusion snapshot, then deterministically samples
100 more sessions under a new seed. The adapter has its own dataset identity,
private exclusion namespace, public artifacts, and independent inspector. Its
network-free preflight must pass before any manifest is frozen.

Third-tranche selection manifest `90dde41a...152bfa` now freezes 100 dates
from a 245-date eligible pool after 627 exclusions, plus the exact 502-session
prior-data graph. Selected-date hash `abb3bd4a...40314` and required-session
hash `91f2d9a8...8817e` bind the graph with no substitution or outcome access.
The uninspected manifest must be committed and pushed before independent
reconstruction.

The third acquisition boundary is now implemented with isolated reference,
security-master, split-action, scanner-cache, lock, manifest, and public-status
paths. It reuses only the previously inspected scanner lineage and hash-pinned
acquisition engines. Independent preflight must prove 100 expected and zero
present reference snapshots, zero target-market artifacts, sufficient reserve,
and zero outcome access before collection.

The complete third-tranche raw reference download then exposed a fail-closed
provider-identity defect before master construction: Massive's documented
case-sensitive symbols can include a lowercase when-issued suffix, and two
snapshot dates each contain two distinct issuers whose symbols collapse to the
same uppercase execution symbol. The isolated third-tranche adapter now
preserves every raw response, derives a separately hashed canonical cache, and
excludes every member of any normalization-collision group under one generic
outcome-blind rule. It never selects a preferred issuer, substitutes a date, or
changes strategy rules. The independent inspector reconstructs the raw-to-
canonical transform without calling the production helper. This repair must be
committed and pushed before canonicalization or master construction.

That published transform now independently rebuilds all 100 raw and canonical
snapshots. It retains 537,918 of 537,922 raw identity rows, normalizes 53 unique
when-issued symbols, and excludes all four rows in the two collision groups.
The point-in-time master contains 7,879 records across 7,428 instruments with
hash `1c85b7c0...21f89`; the complete 2023-01-10 through 2026-07-13 split graph
contains 4,883 events with ignored-artifact hash `b03f97d9...47660`.
Independent combined reconstruction confirms the master, split range, raw and
canonical hashes, zero target-market artifacts, no substitutions, and no
outcomes. These point-in-time inputs may be published before scanner freeze.

Inner scanner manifest `34029d3c...60a96` now freezes all 502 exact required
sessions, the 7,456-symbol point-in-time union, 463 candidate reusable
sessions, and 39 provider-bound sessions at zero target state. It binds the
published canonical master and normalization attestation, complete split
graph, unchanged scanner engine and rules, compatible first-corpus lineage,
raw Alpaca SIP semantics, zero substitution, and the outcome lock. The
manifest and zero-state status must be committed and pushed before the outer
mechanism/source contract is frozen.

Outer acquisition manifest `898e423c...1c003e` now binds that exact inner
scanner graph, all upstream selection and calendar artifacts, the unchanged
one-trial retest trigger, primary-source rules hash `99843372...b4dc`,
selected-symbol-only detail boundary, provider priority, capacity reserve,
implementation hashes, privacy boundary, zero substitution, and the complete
outcome lock. It is uninspected and must be committed and pushed before the
separate inspector may reconstruct it.

Independent outer reconstruction now marks manifest `898e423c...1c003e`
`FROZEN_READY`. It reopens all 100 raw and canonical identity snapshots, the
7,879-record master, 4,883 split actions, and all 502 scanner sessions while
rehashing every upstream, mechanism, source, implementation, capacity,
privacy, zero-state, and outcome-lock binding. The scanner dataset remains
uncollected and `COLLECTING`; this inspection must be committed and pushed
before reusable rows may materialize or Alpaca may be contacted.

The exact third-tranche scanner corpus is now independently `READY`.
Reconstruction verifies all 502 frozen source sessions, 2,648,865 canonical
documents, 537,918 point-in-time universe evaluations, 3,062 coarse-eligible
rows, and 1,781 dynamic selections across the unchanged 100 dates. The run
made 6,803 Alpaca SIP requests with zero retries and reused 463 compatible
sessions. Summary hash `6068140c...ba28`, private-detail hash
`9ed5608a...7b9`, and inspection hash `946018b7...8b95` bind the complete
result. This scanner evidence authorizes only an exact selected-pair freeze;
causal details, outcomes, alpha, maturity, production, and broker actions
remain blocked.

The third-tranche selected-pair boundary is now implemented as an isolated,
hash-pinned adapter over the unchanged independently tested selection
primitive. It binds scanner manifest `34029d3c...60a96`, outer manifest
`898e423c...1c003e`, the inspected 1,781-pair result, the original hypothesis
and one-trial trigger, its own private namespace, and a separate independent
inspector. No pair detail or downstream artifact is accessed by this
implementation slice. The adapter and tests must be committed and pushed
before the default freezer may materialize or hash the private exact-pair
contract.

Selected-pair manifest `2714c94f...9b159` now freezes all 1,781 exact ordered
third-tranche selections across the 100 inspected scanner dates. Private
selection hash `76e8e4d1...29fb7` binds the omitted date-security graph; the
public manifest also binds every daily shortlist, published scanner input,
implementation file, hypothesis, one-trial trigger, capacity reserve,
zero-substitution rule, empty downstream namespace, and outcome lock. It is
`FROZEN_AWAITING_INSPECTION` and must be committed and pushed before the
separate inspector may reopen the private graph. No causal detail, source,
trigger, post-entry data, or outcome has been accessed.

The v1 selected-pair inspection then failed closed before accepting any
evidence because the inherited publication checker required all pre-freeze
input commits to equal the later manifest-publication HEAD. That condition is
incompatible with the required implementation-commit, freeze-commit,
inspection sequence. Manifest `2714c94f...9b159` is preserved as
`INSPECTION_BLOCKED`. A v2 pair contract keeps the same scanner, selection
primitive, 1,781-pair denominator, hypothesis, trigger, privacy, and outcome
rules, but its isolated checker requires each pinned input commit to be a clean
pushed ancestor of inspection HEAD and verifies the exact historical blob at
that commit. The v2 implementation must be committed and pushed before a new
manifest is frozen; no selection repair, causal access, or outcome access is
permitted.

Repaired selected-pair manifest `f50fe206...f225a` now freezes the same 1,781
ordered selections under dataset v2, sourced only from the unchanged inspected
scanner-v1 corpus. Private hash `9d94c294...2106b` reflects the new dataset
identity while the date-security rows remain unchanged. The contract pins
pushed repair commit `2559801`, every upstream byte and mechanism binding, a
zero-artifact v2 preentry namespace, capacity, privacy, zero substitution, and
the full outcome lock. It is `FROZEN_AWAITING_INSPECTION` and must be committed
and pushed before the repaired inspector runs.

Independent v2 reconstruction now marks selected-pair manifest
`f50fe206...f225a` `FROZEN_READY`. It matches all 1,781 ordered private rows,
all 100 date partitions, private graph hash `9d94c294...2106b`, and daily-
shortlist set hash `038f35c9...a7a65`. Every current binding and exact Git blob
at pushed pre-freeze commit `2559801` matches, the v2 downstream namespace is
still empty, identifiers remain private, substitutions are forbidden, and no
outcome is permitted. Publication authorizes only an independently frozen
no-outcome causal/source request graph.

The isolated third-tranche catalyst/source contract is now implemented for
the `FROZEN_READY` pair-v2 input and unchanged scanner-v1 corpus. It preserves
the generic primary-source rules and parser while binding its own private
source namespace, adapter, independent inspector, point-in-time master source,
strategy attestation, and public documentation surface. Its publication proof
uses the repaired pushed-ancestor and exact-historical-blob semantics from the
outset. Four focused tests cover identity routing, scoped base configuration,
closed-source freeze/inspection, and historical publication verification. The
implementation must be committed and pushed before its zero-artifact manifest
is frozen; selected-symbol source access and outcomes remain forbidden.

Source manifest `2d6803ec...a58da` now freezes the complete outcome-blind
primary-source boundary for all 1,781 selected pairs. It binds pair-v2 manifest
`f50fe206...f225a`, daily-shortlist hash `038f35c9...a7a65`, unchanged source-
rules hash `99843372...b4dc`, implementation hash `7279ff97...2aa3`, scanner,
identity and strategy sources, pushed commit `a9d860c`, dependencies, private
namespace, capacity, zero target artifacts, no substitution, and the outcome
lock. The uninspected manifest must be committed and pushed before independent
zero-state reconstruction; source requests remain forbidden.

Source-contract v1 inspection then failed closed on its mutable documentation
binding: this campaign document necessarily changed when manifest
`2d6803ec...a58da` was published, so current bytes no longer matched the
pre-freeze `selection_doc` hash. No source or outcome was accessed, and v1 is
preserved as `INSPECTION_BLOCKED`. Source-contract v2 keeps the exact pair-v2
denominator, scanner-v1 source, rules, parser, and outcome locks, while binding
a dedicated immutable `source-contract-boundary-v2.md` and isolated private
namespace/status. Its implementation and immutable note must be committed and
pushed before v2 freeze; neither pair selection nor source rules may change.

Source-contract v2 manifest `3c39f19e...768dfc` now freezes that repaired
zero-artifact boundary for all 1,781 pairs. It binds immutable-note hash
`a65da186...e6c74`, pair-v2 and scanner-v1, unchanged source-rules hash
`99843372...b4dc`, implementation hash `bb84ceaa...9ab64`, every source and
dependency, pushed commit `f5774ed`, isolated private state, capacity,
substitution prohibitions, and the outcome lock. It must be committed and
pushed uninspected before the v2 inspector runs; selected-symbol requests and
outcomes remain blocked.

Independent reconstruction now marks source-contract v2
`3c39f19e...768dfc` `FROZEN_READY`. All 1,781 private pair identities and 100
daily partitions rebuild, every current input and historical blob at pushed
commit `f5774ed` matches, source-rules hash `99843372...b4dc` and
implementation hash `bb84ceaa...9ab64` are unchanged, and the immutable note
remains exact. Capacity is ready, the target namespace is empty, substitutions
are forbidden, and no selected-symbol detail or outcome has been accessed.
Only a separately frozen source identity/request graph may proceed.

SEC identity manifest `85ba7b49...8697e` now freezes the exact third-tranche
primary-source denominator before provider access. It resolves all 1,781
selected pairs against the attested point-in-time master, retains 1,778
CIK-present pairs and three explicit missing-CIK pairs, preserves nine
listing-scoped joins, and binds 538 unique SEC submissions requests. Private
identity hash `fe168d1d...1ef8b`, request-graph hash `e5ff36e6...52589`, and
daily aggregate hash `9c05864d...fbbd` bind the omitted identities and request
rows. The dataset-specific response namespace contains zero artifacts;
inspection and a published `FROZEN_READY` status remain mandatory before any
SEC access, and documents, triggers, outcomes, alpha, maturity, production, and
broker actions remain blocked.

Independent zero-state reconstruction now marks SEC manifest
`85ba7b49...8697e` `FROZEN_READY`. It reproduces all 1,781 pair/master joins,
1,778 CIK-present rows, three retained missing-CIK rows, nine listing-scoped
joins, the 538-request private graph, daily aggregates, lineage, capacity, and
every request and outcome lock. The SEC graph covers 99 dates with at least one
selected pair inside the frozen 100-date corpus; this corrects the initial
registry event's `requested_dates` aggregate without changing any identity or
request. The response namespace remains empty. Only the separately published
bounded submissions collector may proceed; supplemental files, primary
documents, semantics, triggers, and outcomes remain locked.

The bounded third-tranche submissions collection is independently inspected.
All 538 frozen requests are terminal and successful: 513 were exact shared-
cache hits, 25 were SEC downloads, and none failed or substituted. Rebuilt
metadata yields 463 unique time-valid candidate filings, 478 pair/filing joins
across 414 pairs, and 729 historical supplemental descriptors. Private
collection hash `2ec27da1...27839`, candidate-document graph
`9a3c9338...7f037`, and supplemental descriptor graph
`a8b3db39...97fb3` match. Filing presence is not positive-catalyst evidence;
all supplemental requests and primary documents remain locked behind separate
published manifests, and no outcome has been accessed.

Supplemental manifest `9f36dd76...0cc55` is independently `FROZEN_READY`.
The network-free reducer rebuilt all 729 descriptors, selected exactly 27 with
target-window overlap, excluded 702 outside every frozen pair window, and found
zero pre-freeze supplemental responses. Private contract hash
`dc619c38...21b5e`, request graph `18e58911...3fe1b`, and decision graph
`ab0c1414...983ac` bind every private decision. Publication authorizes only
those 27 SEC-operated supplemental requests; primary documents, semantic
classification, triggers, outcomes, alpha, maturity, production, and broker
actions remain locked.

All 27 frozen supplemental requests are now independently inspected: 16 exact
cache hits, 11 SEC downloads, zero failures, zero pending requests, and zero
substitutions. Precise acceptance-time filtering adds 19 unique candidate
documents and 20 pair/document joins across 18 pairs. Private collection hash
`d7ceabd9...63e5e` and candidate graph `cf162bc0...6b959` reproduce exactly.
These remain discovery identities only; no document body, source semantics,
trigger, or outcome has been observed. The exact union document graph must be
frozen and published before primary-document access.

Primary-document manifest `c8c5e820...7c8af` is independently
`FROZEN_READY`. It unions the inspected main and supplemental indexes into 482
unique accession-bound SEC-operated requests, preserves 498 pair/document joins
across 431 pairs, finds zero conflicting duplicate observations, and confirms
zero pre-freeze document responses. Private graph hash
`a43ba6ef...8e7de`, request graph `a68c266a...7c6c3`, and join graph
`ef6aff71...e1ec1` bind every omitted identity and relationship. Only these
482 document bodies may now be collected after publication; semantics,
triggers, outcomes, alpha, maturity, production, and broker actions remain
locked.

The complete third-tranche primary-document corpus is independently inspected.
All 482 frozen requests succeeded: 56 exact cache hits, 426 SEC downloads,
zero failures, zero pending requests, and zero substitutions. Inspection
rehashed all 19,689,633 retained bytes and rebuilt private collection hash
`78e95eab...01f26` plus every terminal wrapper. Raw availability is transport
evidence only; ownership, issuer binding, causal timing, financing conflicts,
direction, materiality, and terminal source semantics remain unclassified.
Outcomes and selected-market detail remain inaccessible.

Semantics manifest `26fe3b93...3dc40` is independently `FROZEN_READY` before
text extraction. It freezes all 1,781 pair identities, 482 documents, 498
pair/source joins, 431 source-bearing pairs, and 1,350 explicit no-source pairs
under the unchanged primary-source rules and deterministic classifier. Pair
hash `bcaee0e0...af65e`, source hash `4747c875...b4755`, join hash
`437bb561...7f4ba`, no-source hash `9ee0d608...da524`, and private hash
`4015c0a9...11229` bind the zero-artifact state. Publication permits only
source-text extraction and deterministic review; outcomes remain forbidden
regardless of the eventual positive-source count.

Independent semantics reconstruction verifies 23 positive primary-source pairs,
so the frozen 20-pair capacity gate passes and the next phase is
`DEVELOPMENT_ACQUISITION`. The complete 1,781-pair disposition also retains 16
verified conflicts, seven verified negatives, 47 non-material/context-only
pairs, 338 document-semantics unresolved pairs, and 1,350 pairs without a
selected SEC source. Private result hash `d168c2d4...fcdb2` binds the review.
Because positive capacity already passes, accession-chain recovery is not the
preregistered next step; exact pre-entry market-data acquisition may proceed
after publication. No trigger, post-entry value, return, or outcome is known.

The isolated third-tranche pre-entry adapter is implemented and focused tests
pass. It hash-pins unchanged base implementation `5543beb0...d5ad`, consumes
only the repaired pair-v2 private surface and inspected 23-positive semantics
result, records that source capacity passed without accession recovery, and
selects 23 pairs across 20 sessions. The planned graph contains exactly 86
causal requests: 23 candidate bar windows, 23 candidate trade windows, and 40
SPY/QQQ benchmark windows. Dedicated zero-state, collection, and result
inspectors remain isolated to tranche3-v2. The implementation must be committed
and pushed before a manifest is frozen; market access and outcomes remain
forbidden.

Pre-entry manifest `45e9441b...52500` now freezes all 23 verified-positive
pairs across 20 third-tranche sessions before selected-market access. The graph
contains exactly 86 requests: 23 candidate bar windows, 23 candidate trade
windows, and 40 SPY/QQQ benchmark windows. Private selection hash
`d1f8a974...a013`, pair hash `6dee8e2b...48123`, and request hash
`a3082eba...6e4be` bind the omitted denominator. The target contains one
private frozen selection and no response, collection, trigger, post-entry, or
outcome artifact. It is uninspected and must be committed and pushed before
independent zero-state reconstruction; provider access remains forbidden.

Independent reconstruction now marks pre-entry manifest
`45e9441b...52500` `FROZEN_READY`. All 23 verified-positive pairs, 20 sessions,
86 requests, private selection, source and implementation hashes, capacity,
privacy, and outcome locks match. The target still contains exactly one frozen
selection and zero terminal wrappers, collection indexes, or trigger indexes.
Publication authorizes only the frozen causal collection; fills, post-entry
rows, returns, outcomes, alpha, maturity, production, and broker actions remain
blocked.

The published collector completed all 86 exact frozen causal requests with 86
Alpaca SIP downloads, zero failures, zero pending requests, and zero
substitutions. The bounded 09:30-10:30 ET graph retains 739,435 causal rows and
no post-entry data. Private collection hash `8fbdf0c3...44d5b` binds the
transport result. This collection remains uninspected; no trigger, fill,
return, outcome, alpha, maturity, production, or broker claim is permitted
until every wrapper and canonical row independently rebuilds.

Independent transport inspection rebuilt the complete 86-request denominator,
rehashed every terminal wrapper, and reloaded all 739,435 canonical causal
rows. Wrapper-set hash `1807429f...1133`, canonical-row-set hash
`6eee957b...b7b`, and private collection-content hash `9570ee27...2c1e` bind
the result. The trigger namespace remains empty and post-entry data remains
locked; causal trigger derivation may begin only after this inspection is
committed and pushed.

Independent reconstruction now marks the third-tranche selection
`FROZEN_READY`: the live exclusion snapshot, both prior selections, all 100
selected dates, and all 502 required sessions match, with zero overlap,
substitution, or outcome access. This inspection must be committed and pushed
before implementing provider acquisition.

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
