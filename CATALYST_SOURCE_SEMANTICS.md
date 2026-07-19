# Catalyst Source Semantics

Status: implementation validated; the formal manifest must be committed before
private extraction or review

## Purpose

`catalyst_source_semantics.py` closes the outcome-blind primary-source gate for
the exact structural surface established by the prior readiness and PDF
profiles. It deterministically selects 33 security-date pairs, 33 captured
documents, and 38 pair/source joins, binds each pair to the point-in-time
instrument, exchange, CIK, and issuer name, then freezes the private selection
before any semantic judgment.

The pipeline has no provider or broker client and no target-price or return
input. Exact symbols, dates, URLs, source text, identities, timestamps, review
decisions, and row dispositions remain in the external historical store or the
ignored review workspace. Git receives only frozen contracts and aggregate
counts, hashes, claim boundaries, and inspection results.

The point-in-time CIK dependency is the immutable 1,987-pair identity-map
selection inside the broader selected-candidate-fidelity manifest. That exact
selection and its attestation are complete and hash-bound even though the
broader dataset correctly remains `COLLECTING` for an unrelated later trigger
stage. Source semantics requires the identity-map contract, not a false `READY`
claim for the broader dataset.

## Workflow

Freeze first and commit the returned hash-addressed manifest before extraction:

```sh
python3 catalyst_source_semantics.py freeze
```

Then use that exact manifest for every later command:

```sh
python3 catalyst_source_semantics.py extract --manifest <manifest>
python3 catalyst_source_semantics.py review --manifest <manifest>
```

The first `review` call writes an ignored private template at
`learning_runs/production_validation/source-semantics-review.json`. It includes
the 38 exact row IDs, point-in-time identity, source routing, extracted title and
text excerpt, timestamp candidates, and issuer/financing diagnostics. It does
not include outcomes. A reviewer must fill every decision, set
`review_completed=true`, and preserve
`target_outcomes_observed_or_derived=false` before applying it:

```sh
python3 catalyst_source_semantics.py review \
  --manifest <manifest> \
  --review-input learning_runs/production_validation/source-semantics-review.json
python3 catalyst_source_semantics.py inspect --manifest <manifest>
```

Inspection independently rehashes every upstream input, rebuilds the exact
joins and source extraction, replays timestamp normalization and terminal
precedence from the private decisions, reconciles both 38 join dispositions and
33 pair dispositions, and verifies the reviewed private hash. It cannot accept
a changed implementation, dependency, manifest, private selection, upstream
artifact, or open outcome lock.

## Review Contract

Ownership and issuer identity are separate decisions. SEC evidence needs an
HTTPS SEC-operated source plus exact security-master-CIK-to-document-CIK
binding. An authority host proves authority ownership only. Exchange evidence
needs exact listing identity. A potential issuer host needs at least two frozen
binding methods; investor-relations prefixes and generic infrastructure hosts
are not ownership by themselves.

Accepted timestamp evidence follows the frozen order: explicit
source-controlled publication datetime; consistent JSON-LD plus visible
datetime; consistent publication metadata plus visible datetime; contextual
`time` datetime; or a PDF dateline corroborated by the issuer/authority page for
the same hashed document. Accepted datetimes are explicit UTC. A prior-date-only
source is conservatively normalized to source-zone end of day. A same-day
date-only source fails. Metadata, HTTP headers, capture time, URL dates, and PDF
creation/modification fields remain diagnostics and never become accepted
timestamps automatically.

Each join receives exactly one terminal disposition in frozen precedence:

1. capture transport or HTTP failure;
2. source ownership unresolved;
3. issuer binding unresolved;
4. irrelevant primary source;
5. timestamp missing, conflicting, same-day unresolved, or after cutoff;
6. document semantics unresolved or nonmaterial;
7. verified financing/conflict;
8. verified negative; or
9. verified positive.

Financing and dilution review runs before a positive classification. Pair-level
aggregation rejects a conflicting or negative source before accepting another
positive source for the same pair.

## Capacity Decision

Fewer than 20 verified-positive pairs closes this run without outcomes and
routes the persistent campaign to `SOURCE_RECOVERY`. At least 20 positives only
permits the unchanged-v3 non-return survivor gate; it does not unlock outcomes
or production changes by itself. Outcomes require a later separately frozen
contract and at least 20 complete non-return survivors.
