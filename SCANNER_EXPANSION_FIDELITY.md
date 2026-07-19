# Expansion Catalyst And Clean-Trigger Fidelity

Dataset: `dataset-selected-candidate-fidelity-2026-07-19-expansion-v1`

Status: SEC candidate collection complete; clean-trigger batch exposed a
recoverable missing-quote control-flow defect and remains incomplete

Manifest: `ee9ba6f3a2d48f3337ee654c545ea3c02287e23f4a4db5a3fb86adf1b0c780ce`

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Frozen Evidence Contract

The contract consumes the inspected 1,987-pair base join and its 1,460 raw
crossing windows. Every pair is mapped to one CIK through the exact dated
Massive instrument ID, ticker, and primary exchange; all 100 reference
snapshots and the resulting 656 unique CIKs are hash-bound. This exact listing
match preserves the composite-FIGI distinction between parallel listings.

The unchanged collector may now acquire only:

- SEC EDGAR submissions and primary 8-K/6-K documents accepted from four
  calendar days before the session through 09:35 ET;
- dilution-conflict and material-item discovery, without assuming positive
  direction;
- the first raw SIP print above the frozen opening high that satisfies the
  already validated minute-bar and continuous-regular-cross condition rules;
- causal top-of-book snapshots around that clean cross.

Exact identities, CIKs, filings, and raw rows remain outside Git. The adapter,
legacy collector, scanner identity builder, dated references, source join, and
source trigger index are all hash-bound. No substitution, return access, alpha
claim, maturity claim, strategy variant, or production change is permitted.

## Runbook

```sh
python3 selected_candidate_fidelity_expansion.py freeze
python3 selected_candidate_fidelity_expansion.py collect-sec <manifest>
python3 selected_candidate_fidelity_expansion.py collect-triggers <manifest>
python3 selected_candidate_fidelity_expansion.py inspect <manifest>
```

Primary-document presence remains discovery evidence. A later frozen
classification stage must establish material positive direction and conflicting
dilution before unchanged-v3 gate attrition can be published. Returns remain
off-limits until all non-return gates are independently inspected.

SEC collection mapped all 1,987 pairs, downloaded or reused submissions for 656
CIKs, and found 696 time-valid primary filings across 591 pairs. It identified
581 pairs with a material primary candidate and 40 dilution conflicts, while
correctly leaving verified-positive count at zero pending classification.

The first condition-aware trigger pass stopped after an explicit Alpaca
no-quote observation left no canonical quote dataset for one clean-cross
window. The legacy collector raised on the second cache lookup instead of
recording missing NBBO and continuing. No quote was fabricated and no provider
was substituted. A separate resumable, quote-gap-aware trigger contract is now
required; this manifest remains `COLLECTING`.
