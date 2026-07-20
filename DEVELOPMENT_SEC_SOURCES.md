# Disjoint SEC Primary-Source Acquisition

Planned dataset:
`dataset-development-sec-primary-sources-2026-07-19-v2`

Status: identity/query freezer and independent inspector implemented; no SEC
target response has been requested or read by this dataset

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Purpose

`development_sec_sources.py` is the network-free first acquisition stage for
the exact 1,906 pairs bound by the disjoint primary-source semantics contract.
It resolves every pair by instrument, symbol, exchange, and point-in-time
coverage against the attested security master. An absent CIK is retained as an
explicit unresolved row; an absent or ambiguous security-master match fails the
contract.

Exact symbols, identities, CIKs, windows, and request URLs remain under
`LOCAL_HISTORICAL_DATA_ROOT`. Public artifacts contain only counts, hashes,
lineage, pacing rules, privacy boundaries, and claim limits.

## Frozen Request Semantics

- Stage one is restricted to SEC EDGAR submissions JSON at an exact private
  request graph derived from the point-in-time CIK map.
- Relevant forms are `8-K`, `8-K/A`, `6-K`, and `6-K/A`. The window begins four
  calendar days before each target at 00:00 ET and ends at 09:35 ET inclusive.
  A precise `acceptanceDateTime` is required; filing date alone cannot prove a
  causal same-day timestamp.
- The compliant user agent, four-worker client, global 0.15-second minimum
  spacing, 30-second timeout, four attempts, exponential retry schedule, cache
  namespaces, and per-CIK failure behavior are hash-bound before collection.
- Existing shared SEC cache is read first but does not count as a dataset
  response until target-specific provenance is recorded. A failed or empty
  response is never treated as proof of no filing, and one failed CIK cannot
  abort unrelated requests.
- Supplementary submissions files and accession-bound primary documents may be
  discovered by stage one but cannot be requested until their own exact
  manifests are frozen, inspected, committed, and pushed.
- No issuer, symbol, source, provider, or date substitution is allowed. Target
  outcomes and post-entry fields remain locked.

## Runbook

Commit and push the implementation and tests before freezing the request graph:

```sh
python3 development_sec_sources.py freeze
```

Then independently rebuild the exact point-in-time mapping and zero-response
state:

```sh
python3 development_sec_sources.py inspect \
  historical_batches/development_tranche_v2/sec_manifests/<manifest>.json
```

Commit and push the inspected manifest and public status before contacting SEC.
The later collector must consume only that committed manifest, record a terminal
result for every unique CIK, and keep all response bodies outside Git.

An inspected identity/request contract adds no verified catalyst, alpha,
confirmation, maturity, promotion, or production readiness by itself.
