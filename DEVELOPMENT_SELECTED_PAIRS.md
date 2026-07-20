# Disjoint Development Selected-Pair Contract

Dataset: `dataset-selected-candidate-contract-2026-07-19-development-v2`

Status: exact 1,906-pair selection frozen; no catalyst, selected-symbol detail,
post-selection path, or outcome access has begun

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Frozen Selection

Manifest `a1362e19...9c834f` consumes the independently inspected scanner
summary and revalidates every daily shortlist count, ordered rank, eligibility
disposition, point-in-time instrument identity, scanner field, and shortlist
hash. It freezes 1,906 exact security-date pairs across all 100 disjoint target
dates. Twelve dates legitimately contain fewer than 20 eligible names; the
minimum retained shortlist is six. No ineligible substitute is added.

The exact symbols, identities, ranks, and scanner measurements are stored under
the configured external historical root. Git contains only daily counts and
hashes, source artifact hashes, the private content hash, and the downstream
information contract. Rerunning the freezer against the same dataset is
idempotent and revalidates the private content hash; divergent content or a
second manifest fails closed.

## Information Boundary

The freeze reads only the already-inspected scanner detail needed to preserve
the exact selection. It does not read or derive:

- catalyst presence, ownership, issuer binding, timestamp, direction, or
  dilution conflict;
- clean trade crosses, prefix VWAP, NBBO, depth, chase price, resistance,
  structural stop, market alignment, or tradability;
- post-selection price paths, fills, exits, returns, alpha, confirmation, or
  maturity evidence.

The next contract must bind these exact pairs to point-in-time primary-source
rules, timestamp and terminal-reason precedence, source-recovery order, private
row storage, and the same outcome lock before any source request. Selected-symbol
market detail and outcomes remain forbidden until their own separately committed
contracts are earned.

## Runbook

```sh
python3 scanner_selected_pairs.py \
  --dataset-id dataset-selected-candidate-contract-2026-07-19-development-v2 \
  --summary research_results/2026-07-19-development-tranche-scanner.json \
  --output-root historical_batches/development_tranche_v2/selected_pair_manifests
```

The committed manifest is the immutable input to the next primary-source
catalyst contract. It is selection evidence only and cannot support an alpha,
strategy-change, maturity, or production claim.
