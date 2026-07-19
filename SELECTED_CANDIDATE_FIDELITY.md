# Selected-Candidate Catalyst And Clean-Trigger Fidelity

Updated: 2026-07-19 ET

Dataset: `dataset-selected-candidate-fidelity-2026-07-19-v1`

Manifest SHA-256:
`38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd`

Status: independently inspected `READY` for development-only pipeline
fidelity. It is not alpha, confirmation, promotion, or a production-rule
change.

## Purpose And Frozen Boundary

This dataset closes two specific gaps in the exact 389-pair dynamic-scanner
join without selecting a new symbol, changing a date, observing an outcome, or
inventing a strategy variant:

1. bind every selected security-date to a sourced point-in-time issuer CIK and
   collect time-valid SEC primary-source candidates; and
2. replace “first raw price above the opening high” with frozen, tested SIP
   trade-condition semantics for a clean continuous-market cross.

The 20 dates were already inspected. These results can harden data contracts
and expose gate attrition, but they cannot estimate expectancy. Exact symbols,
CIKs, filings, documents, raw trades, and quote windows remain outside Git under
`LOCAL_HISTORICAL_DATA_ROOT`. Public artifacts contain hashes, aggregate counts,
and explicit claim boundaries only.

## Point-In-Time Issuer And Catalyst Evidence

All 389 pairs map to 283 unique CIKs in the dated Massive reference snapshots.
The mapper joined 349 pairs by ticker plus share-class FIGI. Forty source rows
lacked a usable FIGI and were accepted only when the ticker occurred exactly
once in that dated snapshot. No pair used a current ticker table or an
outcome-driven substitute.

The collector downloaded and retained 283 SEC submissions documents and 100
unique primary filing documents in the external `_sources/sec/` cache. For each
pair it admitted only forms `8-K`, `8-K/A`, `6-K`, and `6-K/A` accepted from
four calendar days before the target through 09:35 ET. Independent inspection
rebuilt every cutoff and primary-document content hash.

Coverage is deliberately modest:

- 81 of 389 pairs had at least one time-valid primary filing;
- 80 had a material 8-K item or a 6-K primary-source candidate;
- 106 filings were retained: 76 8-K, three 8-K/A, and 27 6-K;
- 18 carried Item 2.02 earnings-result metadata; and
- eight contained Item 3.02 or strong primary-document dilution/financing
  language.

Every retained filing remains `classification_required=true` and
`verified_positive_catalyst=false`. A primary filing proves source and timing;
it does not, by itself, prove positive direction, materiality to the long thesis,
or absence of a conflicting disclosure. The remaining 308 pairs are not
silently treated as having no catalyst—the bounded SEC lane simply did not find
one, and another permitted primary/direct source would still be required.

## Frozen Clean-Cross Semantics

Alpaca documents that minute-bar high/low eligibility depends on tape and trade
conditions, and that the strictest rule wins when a trade has multiple
conditions. `sip_trade_conditions.py` transcribes that table as
`alpaca-sip-minute-v1` and fails closed on missing, unknown, OTC, or
tape-incompatible values. See Alpaca's
[bar-aggregation rules](https://docs.alpaca.markets/us/docs/market-data-faq#how-are-bars-aggregated),
which cite the CTA and UTP SIP specifications.

The separate `continuous-regular-cross-v1` contract is intentionally narrower.
A clean ORB cross must:

- carry the tape's regular-sale condition (` ` on tapes A/B, `@` on tape C);
- contain only continuous-execution modifiers (`E` automatic execution where
  valid and `F` intermarket sweep); and
- update the minute high/low under the strictest-condition rule.

Odd lots, auctions, acquisitions/distributions, special settlements, extended-
hours markers, cross trades, late or out-of-sequence reports, corrected prints,
and every other special print cannot establish the clean continuous cross even
when a provider would permit some of them to update a bar high. This is a replay
fidelity definition of the existing “clean trade” requirement, not a new entry
mechanism.

## Inspected Trigger Findings

Independent inspection reconstructed all 325 crossing windows:

- only 128 first raw price crosses were clean continuous regular-sale crosses;
- 197 first raw crosses were rejected by the frozen condition contract;
- every window contained a later clean cross, but 195 clean timestamps moved
  later than the first raw price cross, with a maximum shift of 42.934563
  seconds;
- 309 clean-cross windows yielded all three 0/5/10-second quote snapshots;
- 297 had three positive, uncrossed snapshots no more than five seconds old;
- only 155 remained inside the 0.15% chase cap at the final snapshot; and
- 939 usable quote observations had a median spread of 0.13001%, still above
  the production 0.10% operating limit.

The previous unresolved join reported 177 chase-cap passes from the first raw
price cross. Using a condition-valid clean cross reduces that to 155. This is
not a measured return effect; it is proof that condition-blind trigger timing
overstates executable availability.

## Runbook

The exact frozen commands are:

```sh
python3 selected_candidate_fidelity.py freeze

python3 selected_candidate_fidelity.py collect-sec \
  --manifest historical_batches/selected_candidate_fidelity/manifests/dataset-selected-candidate-fidelity-2026-07-19-v1-38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd.json

python3 selected_candidate_fidelity.py collect-clean-triggers \
  --manifest historical_batches/selected_candidate_fidelity/manifests/dataset-selected-candidate-fidelity-2026-07-19-v1-38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd.json

python3 selected_candidate_fidelity.py inspect \
  --manifest historical_batches/selected_candidate_fidelity/manifests/dataset-selected-candidate-fidelity-2026-07-19-v1-38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd.json

python3 selected_candidate_fidelity_inspection.py \
  --manifest historical_batches/selected_candidate_fidelity/manifests/dataset-selected-candidate-fidelity-2026-07-19-v1-38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd.json
```

The public collection summary is
`historical_batches/selected_candidate_fidelity/collection-status.json`.
Aggregate results are in
`research_results/2026-07-19-selected-candidate-fidelity.json` and the
independent reconstruction is in
`research_results/2026-07-19-selected-candidate-fidelity-inspection.json`.

## Decision And Next Evidence

Keep `2026-07-15-orb-v3` frozen and `UNVALIDATED`. No strategy rule was earned.
The follow-on `CHAMPION_INPUT_FIDELITY.md` layer now classifies complete SEC
submissions with exact prior-close recency and joins official historical halts.
It finds only 12 verified material catalyst pairs, one with verified positive
direction, and zero halt overlaps. Broker-specific historical tradability is
explicitly not reconstructable. Resistance, structural stop/noise,
benchmark-relative strength, and conservative executable liquidity remain
blockers. Only after an outcome-blind unchanged-champion readiness contract is
complete should the system expose returns or freeze at least 100 previously
uninspected scanner dates for meaningful alpha inference.
