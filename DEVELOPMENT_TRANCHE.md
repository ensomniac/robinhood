# Disjoint Production-Development Tranche

Planned dataset: `dataset-development-tranche-2026-07-19-v2`

Status: exact selection manifest `6bc3d685...ad4f82` is independently inspected
`FROZEN_READY`, and scanner manifest `e500cf2a...85a3643` is independently
inspected `READY`; no target outcome has been accessed or derived

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Why Another Tranche Is Required

The completed source-recovery sequence leaves only three exact, deduplicated
verified-positive catalyst pairs in the prior 100-date expansion. That is below
the 20-pair source-capacity gate, so post-entry outcomes for that corpus remain
locked. The next development input must therefore be disjoint rather than a
revision selected after seeing the existing sample.

`development_tranche.py` froze the next exact 100 target sessions before any
new reference or market collection. Its outcome-blind freeze found
185 eligible sessions in the attested 2025 calendar after excluding 227 dates
from the signal ledger, archived contexts, prior scanner selections, and every
JSON artifact cited by all 27 currently inspected dataset registrations. The
seeded selection needs 243 target/lookback sessions. These are frozen input
counts, not an alpha result.

The committed freeze selected exactly 100 dates with seed `2026071902`, remains
disjoint from all 227 frozen exclusions, and requires 243 target/lookback
sessions. Independent inspection rebuilt the selection and all current exclusion
surfaces. The public dates are in
`historical_batches/development_tranche_v2/selection-2026-07-19-100-days.json`;
exact combined exclusion rows remain private.

## Frozen Selection Contract

The controller binds:

- the exact calendar and its Alpaca calendar-source attestation;
- every signal-ledger date and every dated archived context, including hashes;
- both earlier public scanner selections;
- the current set of inspected dataset registry events and every JSON evidence
  artifact they cite, extracting only explicitly labeled requested, selected,
  frozen, or candidate-by-date collections;
- the private exact exclusion set, public selected-date file, implementation,
  source-semantics exit result, seed, eligible pool, required lookback sessions,
  and zero-substitution rule; and
- an observed capacity projection plus the configured 20-GiB historical-store
  reserve.

Exact archived paths and the combined exclusion rows remain in the ignored
historical store. Public artifacts contain aggregate counts, hashes, and the
eventually frozen market-session selection. Selection does not read or derive
target returns.

The implementation rebuilds the selected dates deterministically and refuses
inspection if the signal ledger, archived contexts, prior selections, inspected
dataset set, cited evidence artifacts, exclusion set, implementation, private
snapshot, or public selection has changed.

## Acquisition Boundary

This selection manifest is only the first preregistration layer. It must be
committed before dated reference access. A separate scanner manifest must then
freeze the point-in-time security master, split actions, exact provider queries,
primary-source rules, cache contract, zero substitution, and reserve check and
must itself be committed before market collection.

Full-universe coarse scanner inputs use exact compatible local cache first and
whole-session Alpaca SIP collection second. Only selected names receive minute,
tape, quote, or other detailed inputs, with provider priority local canonical
cache, IBKR, Massive, then Alpaca. A symbol/session may not splice providers.
Missing data remains missing, and full-universe detail collection is forbidden.

The conservative preflight projection is 34,118 provider requests and about
0.90 GiB of private artifacts for 243 required sessions, against more than 230
GiB of free space at the implementation check. These are planning bounds, not a
promise of collection duration or a substitute for the freeze-time reserve
check.

## Runbook

The freeze and inspection commands are:

```sh
python3 development_tranche.py freeze

python3 development_tranche.py inspect \
  --manifest historical_batches/development_tranche_v2/manifests/<manifest>.json
```

Commit and push the public selection and hash-addressed manifest before running
`scanner_replay.py collect-reference`. Then build the dated security master,
collect the exact split range, freeze and commit the scanner market manifest,
and only then begin provider-bound market collection.

Any overlap, source drift, implementation drift, date substitution, mixed
provider row, missing provenance, reserve breach, or target-outcome access before
the separately frozen outcome contract stops the tranche. A partial collection
is resumable and cannot become `READY` until independent inspection passes.

## Reference Identity Finding

All 100 dated Massive identity snapshots completed without retry, failure,
substitution, target-price access, or target-outcome access. The first master
build then failed closed because 31 composite FIGIs appeared under two
simultaneous ticker listings across 129 date groups. There were zero
ticker/exchange listing collisions.

The master builder now scopes only a composite FIGI proven to have simultaneous
aliases by the observed ticker and exchange. Ordinary composite identities keep
their stable ID, including non-overlapping ticker changes. Share-class and
fallback collisions remain fatal because those identifiers cannot prove a
listing. The derived private master records the provider FIGIs and the public
attestation reports only collision and listing-scoped record counts.

The repaired master is now locally validated at 6,008 records and 5,889
instruments, including 63 listing-scoped records and 1,269 fallback-identity
records, with content hash `5afd5776...1793c5f`. The fallback records are unique
under their frozen ticker/exchange/CIK/name basis; ambiguity still fails the
cross-record audit. The exact December 10, 2024 through November 28, 2025 split
query retained 1,351 events with hash `2a1f930b...2d70b7f`.

The scanner adapter now accepts the attested object-row market calendar and
binds its repository path and file hash into new market manifests. It likewise
binds the public selection file, seed, dates, hash, and no-substitution flag.
Legacy manifests remain readable, while every newly frozen contract gets the
stronger lineage checks.

Market manifest `e500cf2a...85a3643` now freezes 100 targets, all 243 required
sessions, a 5,797-symbol point-in-time union, raw Alpaca SIP 15-minute regular
bars and exact 09:30-09:34 minute bars, the calendar, selection, master, splits,
unchanged v3 rules, canonical store, provider fidelity, and zero substitution.
Its pre-freeze status independently reports zero target artifacts, requests,
retries, derived rows, or canonical merges. The manifest was committed and
pushed before its first market request.

## Inspected Scanner Result

The frozen collection completed all 243 required sessions with 30,075 Alpaca
SIP requests, zero retries, zero reused sessions, 5,012,355 derived rows, and
1,245,715 canonical day-document merges. The independent inspector rebuilt all
100 rankings and verified 516,335 point-in-time common-stock evaluations, 4,355
eligible rows, and 1,906 selected ranks.

It also re-read 1,245,715 canonical documents, including 1,148,154 exact
opening-minute datasets, and matched every ranking, rejection disposition,
split adjustment, source hash, strategy rule, and shortlist hash. The calendar
attestation binds the exact 394-session file committed before collection and a
separate Alpaca comparison that matched every date, open, close, and early close.

This closes scanner selection fidelity for this disjoint tranche only. Catalyst
ownership, issuer binding, causal timestamp, direction, conflict, quote/book,
tradability, clean-trigger, execution, and outcome evidence remain absent or
locked. Selected-pair manifest `a1362e19...9c834f` now freezes the exact 1,906
date/symbol pairs outside Git with zero substitution and no downstream access.
The next acquisition layer must commit their primary-source semantics before
requesting any target source, selected-symbol detail, or outcome.
