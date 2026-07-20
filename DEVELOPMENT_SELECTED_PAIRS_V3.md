# Second Disjoint Development Selected-Pair Contract

Dataset: `dataset-selected-candidate-contract-2026-07-20-development-v3`

Status: exact 1,871-pair selection frozen and independently rebuilt; no
catalyst, selected-symbol detail, post-selection path, or outcome access has
begun

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Frozen Selection

Manifest `f00e8393...c8897` consumes the independently inspected v3 scanner
summary and revalidates every daily shortlist count, ordered rank, eligibility
disposition, point-in-time instrument identity, scanner field, and shortlist
hash. It freezes 1,871 exact security-date pairs across all 100 second-tranche
dates. Fourteen dates legitimately contain fewer than 20 eligible names; the
minimum retained shortlist is three. No ineligible substitute is added.

The exact symbols, identities, ranks, and scanner measurements remain under the
configured external historical root. Git contains only daily counts and hashes,
source artifact hashes, the private content hash, and the downstream information
contract. A separate already-tested reconstruction path independently rebuilt
the private content hash, all date partitions, ranks, shortlist counts, and
shortlist hashes before this status was published.

## Information Boundary

The freeze reads only the inspected scanner detail needed to preserve the exact
selection. It does not read or derive catalyst semantics, source availability,
clean trade crosses, NBBO, depth, tradability, selected-symbol post-09:35 paths,
fills, exits, returns, alpha, confirmation, or maturity evidence.

The next contract must bind these exact pairs to primary-only source rules,
ownership and issuer-binding requirements, causal timestamp precedence,
financing-first direction, one-terminal-reason precedence, recovery order,
private row storage, implementation hashes, and the same outcome lock before any
source request. Selected-symbol market detail and outcomes remain forbidden
until separately frozen contracts are earned.

## Runbook

```sh
python3 scanner_selected_pairs.py \
  --dataset-id dataset-selected-candidate-contract-2026-07-20-development-v3 \
  --summary research_results/2026-07-20-development-tranche-v3-scanner.json \
  --output-root historical_batches/development_tranche_v3/selected_pair_manifests
```

Rerunning the freezer is idempotent and revalidates the private content. Any
divergent private content or second public manifest fails closed. This evidence
cannot support alpha, a strategy change, maturity, or production use.
