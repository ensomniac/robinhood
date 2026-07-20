# Second-Tranche SEC Primary-Source Acquisition

Planned dataset: `dataset-development-sec-primary-sources-2026-07-20-v3`

Status: identity/request-graph implementation ready; no v3 CIK graph is frozen
and no SEC target response has been requested or read

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Purpose

`development_sec_sources.py` is the network-free first acquisition stage for
the exact 1,871 pairs bound by the second-tranche source-semantics contract. It
resolves every pair by instrument, symbol, exchange, and point-in-time coverage
against the attested security master. Missing CIKs remain explicit unresolved
rows; missing or ambiguous security-master matches fail the contract.

The adapter now accepts an explicit dataset identity, source-semantics manifest,
selected-pair manifest, security master and attestation, strategy attestation,
source-contract document, manifest root, and inspection-status path. The dataset
identity scopes both the private identity/query map and target response
namespace, preventing cross-tranche reads or writes.

## Frozen Semantics

- Stage one is restricted to SEC-operated submissions JSON for the exact private
  CIK request graph.
- Forms are `8-K`, `8-K/A`, `6-K`, and `6-K/A`; the window begins four calendar
  days before the target and ends at 09:35 ET inclusive.
- Precise `acceptanceDateTime` is required. Filing date alone cannot prove a
  causal same-day timestamp.
- The compliant private user agent, four workers, global 0.15-second spacing,
  30-second timeout, four attempts, retry schedule, cache policy, and per-CIK
  terminal behavior are frozen before collection.
- Shared cache is allowed only with target-specific provenance. Failed or empty
  responses are not proof of no filing, and one failed CIK cannot abort other
  requests.
- Supplementary files and accession-bound primary documents require their own
  frozen and pushed manifests before access.
- Exact identities and requests remain outside Git. Provider/date/source
  substitution, selected-symbol detail, and outcomes remain forbidden.

## Runbook

Commit and push the implementation before freezing the identity graph:

```sh
python3 development_sec_sources.py \
  --dataset-id dataset-development-sec-primary-sources-2026-07-20-v3 \
  --source-contract historical_batches/development_tranche_v3/catalyst_manifests/dataset-primary-source-semantics-contract-2026-07-20-development-v3-412efc72a74d337d161c8f672c6692cbc1a86fa7befecacf31e3e605a75043b8.json \
  --selected-manifest historical_batches/development_tranche_v3/selected_pair_manifests/dataset-selected-candidate-contract-2026-07-20-development-v3-f00e8393391d8daee026e326d98643d8f07e2f38f847e4b257c6caa1b73c8897.json \
  --security-master historical_batches/development_tranche_v3/security-master.jsonl \
  --security-master-source historical_batches/development_tranche_v3/security-master-source.json \
  --strategy-source historical_batches/scanner_expansion/production-strategy-source.json \
  --source-contract-doc DEVELOPMENT_CATALYST_CONTRACT_V3.md \
  --output-root historical_batches/development_tranche_v3/sec_manifests \
  freeze
```

The frozen manifest must be committed and pushed before its separate
`inspect <manifest> --status
historical_batches/development_tranche_v3/sec-contract-status.json` pass. The
inspected zero-response state must then be committed and pushed before any SEC
request. This stage cannot establish a verified catalyst or alpha result.
