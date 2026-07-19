# Expansion Selected-Pair Contract

Dataset: `dataset-selected-candidate-contract-2026-07-19-expansion-v1`

Status: exact selection frozen; no downstream catalyst, trigger, quote, path, or
outcome collection has begun

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## What Is Frozen

The contract consumes the independently inspected 100-date dynamic scanner
result and revalidates every daily shortlist count, ordered rank, eligibility
disposition, identity, and public shortlist hash. It freezes all 1,987 selected
security-date pairs under manifest
`369efda1967ad83096e3c26672e349b44bb8d0afd694f391490042a9e22c140b`.
No date or symbol substitution is permitted.

Exact symbols and scanner fields remain in the external historical root at the
manifest-declared `LOCAL_HISTORICAL_DATA_ROOT/_derived/scanner_selected_pairs/`
path. The repository contains only the 100 daily counts/hashes, source artifact
hashes, private-selection content hash, and downstream rules. Rerunning the
freezer returns the same manifest; it refuses divergent private content or more
than one manifest for the dataset.

## Information Boundary

Freezing this selection did not read or derive:

- catalyst presence, source, direction, or dilution conflict;
- a clean SIP trade cross, prefix VWAP, NBBO, depth, or chase price;
- resistance, structural stop, market alignment, or tradability disposition;
- any post-selection price path, exit, return, alpha, or maturity metric.

The next collector may acquire only point-in-time primary catalyst evidence,
clean-trigger/NBBO inputs, and unchanged-v3 post-trigger outcomes for these exact
pairs. Gate attrition must be published before returns. A strategy variant or
production edit remains forbidden until unchanged v3 is evaluated and a
specific failure earns one separately preregistered mechanism proposal.

## Why This Is The Next Growth Step

The scanner gap is closed, but scanner eligibility is not trade eligibility.
Freezing the 1,987 candidates before richer evidence prevents catalysts and
outcomes from influencing which names survive. Applying the already-defined v3
gates unchanged will measure whether the strategy produces enough deployable
signals and positive geometric growth after costs. A zero-signal or negative
result remains useful evidence; it cannot authorize threshold loosening.

## Runbook

```sh
python3 scanner_selected_pairs.py
```

The next implementation slice is a manifest-aware downstream collector that
reuses compatible canonical observations, records missing fields explicitly,
keeps licensed rows outside Git, and independently inspects every derived gate
before target returns are summarized.
