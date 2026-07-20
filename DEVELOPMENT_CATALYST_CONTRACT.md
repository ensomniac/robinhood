# Disjoint Primary-Source Semantics Contract

Planned dataset:
`dataset-primary-source-semantics-contract-2026-07-19-development-v2`

Status: implementation validated; manifest not yet frozen; no target source,
selected-symbol detail, or outcome access has occurred

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Purpose

`development_catalyst_contract.py` freezes the exact 1,906-pair selected surface
against the existing catalyst-semantics definitions before any target source is
requested. It independently rebuilds every private daily shortlist, verifies
the private content hash, binds scanner, inspection, security-master, strategy,
implementation, parser, and dependency hashes, and records zero target-source
artifacts at freeze time.

The contract imports the established timestamp precedence, event taxonomy,
issuer-binding methods, and one-terminal-reason precedence from
`catalyst_source_semantics.py`. It does not create a parallel interpretation of
those rules.

## Locked Source Rules

- Primary issuer, SEC, or exchange evidence is required. Secondary content may
  route discovery but can never verify a catalyst or replace a failed primary
  source.
- Ownership and issuer binding must be proven. Same-day date-only evidence
  fails, and metadata, HTTP headers, capture time, URL dates, and PDF creation
  dates cannot independently establish causal publication.
- Financing and dilution conflicts are classified before any positive event.
- Every pair/source join receives exactly one disposition under the frozen
  terminal precedence.
- Exact pairs, URLs, source rows, text, timestamps, identities, diagnostics, and
  review decisions stay outside Git. Public artifacts contain aggregates and
  hashes only.
- Recovery order is accession-bound SEC endpoints with a compliant user agent,
  frozen-pacing transport retries, then canonical issuer pages and document
  chains. No source or symbol substitution is allowed.

## Outcome Lock

Post-entry fields and returns remain inaccessible. Fewer than 20 verified
positive pairs returns the campaign to `SOURCE_RECOVERY`; at least 20 positives
but fewer than 20 complete unchanged-v3 non-return survivors remains
`DEVELOPMENT_ACQUISITION`. Outcomes require a separately frozen contract only
after at least 20 complete survivors pass independent inspection.

## Runbook

Commit and push the implementation before freezing its first manifest:

```sh
python3 development_catalyst_contract.py freeze
```

Then independently rebuild the contract before any source request:

```sh
python3 development_catalyst_contract.py inspect \
  historical_batches/development_tranche_v2/catalyst_manifests/<manifest>.json
```

The inspected manifest and public zero-state must be committed and pushed before
target-source acquisition begins.
