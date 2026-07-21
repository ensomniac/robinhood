# Catalyst ORB Retest Challenger

Status: `PREREGISTERED` research-only active challenger

Contract: `b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105`

Primary trial: `trial-8a93c0ab15c85248`

Champion: `2026-07-15-orb-v3`, preserved unchanged and `UNVALIDATED`

## Mechanism

The challenger does not buy the first opening-range breakout. It observes a
condition-valid first break, then requires the first fully completed one-minute
retest bar to touch the opening-range high and close at or above it. Entry is
the first condition-valid rebreak of that bar high before 10:30 ET. The causal
hypothesis is that a completed hold and rebreak measures accepted price
discovery rather than the transient impulse of the first break.

This is one frozen trial, not a parameter search. The exact rule, primary
parameters, execution assumptions, cost stresses, falsification gates,
contamination disclosures, and compatibility risks live in:

`learning/hypotheses/experiment-catalyst-orb-retest-v1-b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105.json`

## Constraints That Do Not Change

- Long common-stock equities only, one position and one filled entry per day.
- Regular hours only, no entry after 10:30 ET, force flat by 15:50 ET.
- Source-verified positive catalyst, financing and dilution conflicts rejected
  first, bullish opening range, exact opening relative volume of at least 1.0.
- Three fresh quote/book snapshots, unchanged A+ spread, depth, liquidity,
  halt, benchmark, and tradability requirements.
- Structural stop below retest invalidation and ordinary noise, at least 10% of
  prior-completed-session ATR(14), never compressed beyond the 0.8% cap.
- At least 2.5R and 2.2% room before resistance, +2% milestone, unchanged
  runner contract, whole-share risk sizing, protection, circuit breakers, and
  no overnight exposure.

## Evidence And Falsification

The challenger may not use any previously inspected target date. The v3
qualification corpus supplied only pre-outcome deployability evidence; its
post-entry data remains inaccessible. Earlier scanner, multi-strategy,
production-aware, and reversal dates and outcomes are disclosed ancestors and
must also be excluded.

Development requires at least 50 eligible closed signals, positive geometric
growth and bootstrap lower mean R, profit factor at least 1.30, drawdown at most
6R, positive chronological halves, positive performance without the five best
trades, and positive 10- and 20-bps stress results with profit factor at least
1.20 and drawdown at most 6R. Rule, capture, or evidence violations must be zero.
Every no-trade, missed fill, unavailable row, and reject stays in denominators.

`challenger_orb_retest.py` is the outcome-blind trigger implementation. It
requires complete causal trade and completed-bar windows, excludes the initial
break minute from retest eligibility, treats the first later touch as the only
hold test, requires condition-valid continuous-sale prints for both breaks, and
requires the +10-second decision snapshot to finish by 10:30. Interpolation or
an incomplete window fails closed.

Failure retires this exact challenger without repair on the sample. Passing
development only earns an untouched confirmation contract; it does not change
production rules or maturity.

## Next Acquisition

Freeze another exact disjoint 100-session selection before provider or outcome
access. Reuse the point-in-time security-master, primary-source catalyst, SIP
tape/quote, split, halt, and capacity controls already proven by the v3 pipeline,
but bind the new retest evaluator and exclude every inspected target date.

The current 2025-2026 calendar has only two eligible disjoint sessions after
the v3 selection. `challenger_orb_retest_calendar.py` therefore freezes an exact
Alpaca calendar request for 2023-01-01 through the last completed session on
2026-07-17 before provider access. Its separate inspector independently rebuilds
the provider query, session ordering, uniqueness, hours, hashes, and outcome
lock. Calendar output is public reference data, not target market or outcome
data. The implementation and zero-output manifest must each be committed and
pushed before collection.

```sh
python3 challenger_orb_retest_calendar.py freeze
python3 challenger_orb_retest_calendar_inspection.py inspect-contract \
  historical_batches/challenger_orb_retest_v1/calendar_manifests/<manifest>.json
# Commit and push the inspected zero-output manifest before the commands below.
python3 challenger_orb_retest_calendar.py collect \
  historical_batches/challenger_orb_retest_v1/calendar_manifests/<manifest>.json
python3 challenger_orb_retest_calendar_inspection.py inspect \
  historical_batches/challenger_orb_retest_v1/calendar_manifests/<manifest>.json
```

Calendar manifest `d0991e14...710c60` is now independently `FROZEN_READY`.
It binds both published implementations, the exact Alpaca endpoint and
2023-01-01/2026-07-17 query bounds, validation rules, output paths, and zero
pre-freeze output artifacts. It must be committed and pushed before the single
read-only calendar request.

The request is now independently `COLLECTION_INSPECTED`: 887 sessions from
2023-01-03 through 2026-07-17, calendar hash
`0ef45909...938ebb`. Every row, query field, hour, ordering constraint, count,
and hash rebuilds. No target session has yet been selected and no target market
or outcome data was accessed.

`challenger_orb_retest_tranche.py` and its separate inspector now adapt the
proven disjoint selector without modifying its frozen base implementation. They
use seed `2026072005`, exclude all four earlier selections plus every signal,
archived context, and currently inspected evidence date, and bind the extended
calendar, v3 falsification, hypothesis, trigger, capacity projection, and
private exclusion snapshot. Preflight finds 445 eligible dates; the exact
100-date selection needs 474 sessions including lookbacks and projects about
1.88 GB of private storage. This is capacity only: the implementation must be
committed and pushed before `freeze`, and the resulting selection and manifest
must be independently inspected and published before any dated reference or
market request.

```sh
python3 challenger_orb_retest_tranche.py freeze
python3 challenger_orb_retest_tranche_inspection.py \
  historical_batches/challenger_orb_retest_v1/selection_manifests/<manifest>.json
```

Selection manifest `c1c51924...ec7dae` is now independently `FROZEN_READY`.
It binds seed `2026072005`, 427 exclusions, a 445-date eligible pool, exactly
100 disjoint selected sessions, and 474 required sessions including lookbacks.
The selected-date hash is `8712bb4d...84d187`; the required-session hash is
`31afb1ff...bc54a`. Substitution and outcomes remain forbidden. This exact
selection and manifest must be committed and pushed before dated reference
identity access.

`challenger_orb_retest_acquisition.py` now implements the next two-layer market
boundary without modifying the hash-bound scanner engine. The inner generic
manifest freezes the point-in-time security master, split history, exact Alpaca
SIP full-universe queries, compatible-cache reuse, and zero market state. The
outer challenger manifest additionally binds the one-trial hypothesis, causal
retest trigger, primary-source ownership/timestamp/conflict rules, selected-name
detail limits, provider priority, capacity reserve, and outcome lock. Its
separate inspector independently rebuilds every binding and refuses a nonzero
pre-freeze market state. It also reopens all 100 private identity snapshots,
the public master, and the ignored split artifact to rebuild counts and hashes
without trusting the controller's aggregate. Both manifests and their inspected
status must be committed and pushed before `collect-scanner` can contact
Alpaca.

The dated reference collector is resumable under the frozen Massive query and
pacing contract. Use only the challenger controller entrypoint: its
repository-native single-writer lock serializes dated-reference collection,
master construction, split retrieval, and full-universe scanner collection.
The independent reference inspection
rehashes both the raw gzip containers and their parsed logical rows, rejects
unexpected or temporary files, and therefore distinguishes harmless gzip
metadata changes from content drift. After all 100 exact snapshots complete,
build the challenger security master and split attestation, freeze the inner
scanner manifest, then freeze and independently inspect the outer contract:

The exact identity stage is now complete. Independent reconstruction reopens
all 100 dated snapshots with raw set hash `af30f011...377a8e` and logical set
hash `ee7774f4...2e4ac`, with no temporary or unexpected artifact. The resulting
point-in-time master contains 6,797 records for 6,561 instruments and has hash
`b9dac192...93158`. The exact frozen split query contains 2,499 events with
hash `58b9ada2...0c020`. The combined input audit confirms the 20-GiB reserve,
zero target-market artifacts, no substitution, and no outcome access. These
public inputs must be committed and pushed before `freeze-scanner`.

Inner scanner manifest `8d1f67bb...c30e41` is now frozen over 474 exact
target/lookback sessions and a point-in-time union of 6,525 symbols. Its
zero-state audit reports no ready session, provider request, retry, derived
row, or canonical merge. Substitution and outcome access remain forbidden.
This manifest and its public status must be committed and pushed before the
outer challenger mechanism/source contract is frozen.

Outer acquisition manifest `b4d49ce7...c9102` is now independently
`FROZEN_READY`. It binds inner manifest `8d1f67bb...c30e41`, the published shared
scanner lock, one-trial retest mechanism, source-semantics hash
`99843372...b4dc`, selected-symbol-only detail, provider priority, the 20-GiB
reserve, and the no-outcome lock. Inspection rebuilds all 100 dates and 474
sessions with zero target-market artifact or substitution. The scanner dataset
is registered as `COLLECTING`, not `READY`. This outer slice must be committed
and pushed before `collect-scanner` may contact Alpaca.

The v1 scanner collection later reached 466/474 independently valid sessions
with 56,776 provider requests, zero retries, zero invalid sessions, and no
outcome access. It then failed closed on the first of eight reusable-source
sessions: chained reuse had carried eight attested rows that were absent from
the immediate reusable security-master union, so the adapter misclassified
those already-present symbols as provider deltas and its duplicate guard
correctly stopped the merge. V1 remains immutable failed operational evidence.

The v2 recovery changes only reusable-input accounting and lineage. Delta
coverage is the union of the source contract's requested universe and the
actual symbols in each hash-attested reusable file. A new manifest may reuse
only complete source/sidecar pairs that exist and revalidate before its freeze;
missing or partial pairs cannot enter that contract. The v2 dataset IDs point
to the 466 complete v1 sessions, preserve the exact 100 target dates,
474-session graph, point-in-time master, rules, provider, zero-substitution
policy, and outcome lock, and leave the remaining eight sessions
provider-bound. The repair implementation and regression tests must be
committed and pushed before either new v2 manifest is frozen.

V2 inner scanner manifest `77dc80b5...b96640` is now `FROZEN_READY` with an
empty target root. It binds all 474 required sessions and 6,525 target symbols,
exactly 466 complete v1 source/sidecar pairs that revalidated before freeze,
and eight provider-bound missing sessions. Its zero-state status contains no
ready target session, request, retry, derived row, or canonical merge. This
inner manifest and status must be committed and pushed before the v2 outer
mechanism/source contract is frozen.

V2 outer acquisition manifest `b580483b...9d8e9` is now independently
`FROZEN_READY` and registered as `COLLECTING`. Inspection rebuilds the exact
100 selected dates, 474-session graph, 6,525-symbol scanner contract, 466
attested reusable sessions, eight provider-bound sessions, source-semantics
rules, shared lock, 20-GiB reserve, and zero-substitution and outcome locks.
It finds zero v2 target-market artifact and authorizes coarse scanner
collection only after this outer contract, status, registry event, and durable
finding are committed and pushed.

That v2 scanner collection and result are now independently `READY`. All 474
sessions validate: 466 came from the frozen attested v1 reuse set, the remaining
eight used 969 exact Alpaca SIP requests with zero retries, and no invalid
session exists. The scanner build evaluates 536,967 point-in-time universe
rows, retains 4,019 coarse-eligible rows, and selects 1,826 dynamic shortlist
rows across all 100 dates. The independent inspector rehashes all source
sessions, recomputes every threshold disposition, rank, shortlist hash, split
adjustment, and 2,492,864 canonical documents, and reports `valid=true`.
This is scanner evidence only: selected-symbol causal detail, catalyst sources,
quotes, breaks, returns, and outcomes still require a separately frozen
post-scanner contract.

`challenger_orb_retest_selected_pairs.py` now implements the first
post-scanner boundary. It deterministically extracts all 1,826 exact ordered
date-security selections into the ignored historical store, publishes only
counts, daily hashes, upstream hashes, and permitted input classes, and refuses
any pre-existing downstream causal/source artifact. It also binds the one-trial
retest mechanism, the v2 inner and outer manifests, the independently inspected
scanner result, the 20-GiB reserve, zero substitution, and the closed outcome
lock. `challenger_orb_retest_selected_pairs_inspection.py` independently
reopens the 100-date private scanner detail and reconstructs every identity,
rank, field, daily shortlist, and aggregate hash without calling the freezer or
the shared extraction primitive. The implementation and tests must be committed
and pushed before the exact contract is frozen.

```sh
python3 challenger_orb_retest_selected_pairs.py
python3 challenger_orb_retest_selected_pairs_inspection.py \
  historical_batches/challenger_orb_retest_v1/selected_pair_manifests/<manifest>.json
```

The resulting manifest and aggregate-only inspection status must then be
committed and pushed before a separate exact provider/query graph may authorize
selected-symbol causal tape, completed retest bars, quote/book snapshots,
pre-entry structure, or primary-source acquisition. Neither command authorizes
post-entry rows, returns, outcomes, alpha, a strategy change, maturity, or broker
activity.

Selected-pair manifest `24ef6d29...965de4` is now independently
`FROZEN_READY`. The separate reconstruction matches all 1,826 private rows and
100 daily partitions, private graph hash `3ed15493...183c2`, daily-shortlist set
hash `887d1f12...ab5a5`, and source-detail hash `5e9dcb08...d21b`. It finds
zero downstream pre-entry artifacts, zero substitution, no public selected
identity, and no outcome access. This manifest, status, registry event, and
durable finding must be committed and pushed before the exact causal/source
request graph is frozen.

```sh
python3 challenger_orb_retest_acquisition.py collect-reference
python3 challenger_orb_retest_acquisition.py reference-status
python3 challenger_orb_retest_acquisition_inspection.py --reference-only
python3 challenger_orb_retest_acquisition.py build-master
python3 challenger_orb_retest_acquisition.py collect-splits
python3 challenger_orb_retest_acquisition.py audit-inputs

# Commit and push the master, source attestations, input status, and durable
# finding before freezing the target market request graph.
python3 challenger_orb_retest_acquisition.py freeze-scanner

# Commit and push the inner scanner manifest and zero-state status before the
# outer mechanism/source-rules contract is frozen.
python3 challenger_orb_retest_acquisition.py freeze
python3 challenger_orb_retest_acquisition_inspection.py \
  historical_batches/challenger_orb_retest_v1/acquisition_manifests/<outer-manifest>.json \
  historical_batches/challenger_orb_retest_v1/scanner_manifests/<scanner-manifest>.json
```

This stage authorizes coarse scanner collection only after publication. It does
not authorize selected-symbol detail, catalyst requests, returns, outcomes,
production changes, maturity credit, or broker actions. The exact selected-pair
and causal detail request graph remains a separate post-scanner freeze.
