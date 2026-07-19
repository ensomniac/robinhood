# Champion Catalyst And Halt Input Fidelity

Dataset: `dataset-champion-input-fidelity-2026-07-19-v2`

Source dataset: `dataset-selected-candidate-fidelity-2026-07-19-v1`

Status: independently inspected `READY` for `DEVELOPMENT_ONLY`

Production strategy: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Purpose And Boundary

This layer answers two input-fidelity questions for the exact 389 already-frozen
scanner-selected security-date pairs:

1. Which SEC filing candidates were accepted after the prior completed session
   closed and before the target-session 09:35 ET cutoff, and what can their
   issuer documents support without inferring favorable direction?
2. Was the symbol in an official Nasdaq historical halt interval from the exact
   clean trigger through the ten-second observation window?

It does not inspect target-session returns, evaluate alpha, invent a strategy,
qualify broker execution, or change a production rule. The 20 dates were already
used to develop and inspect the pipeline. They can prove mechanics and attrition,
but cannot serve as independent strategy evidence.

## Frozen Evidence

The v2 manifest freezes:

- the exact 20 dates and 389 selected pairs;
- the parent scanner, CIK, SEC-candidate, and clean-trigger artifact hashes;
- each target date's prior completed-session close;
- the private selection hash;
- the complete-submission collector, classifier, and Nasdaq-halt parser hashes;
- the rule that target outcomes are never read.

Complete SEC submissions and official Nasdaq response bodies are retained under
`LOCAL_HISTORICAL_DATA_ROOT/_sources/`. Pair classifications and halt joins are
retained under `_derived/champion_input_fidelity/` and merged as typed contexts
into each canonical symbol/year/day document. Licensed or identifying rows stay
outside the public repository; public artifacts contain aggregate counts and
content hashes only.

The first frozen contract, v1, stopped when Nasdaq returned fractional-second
halt timestamps that its whole-second parser rejected. It saw no target outcomes
and made no substitution or rule change. The failure is preserved in
`historical_batches/champion_input_fidelity/contract-lineage-2026-07-19.json`;
v2 explicitly supersedes it with a parser that accepts both official timestamp
shapes.

## Catalyst Contract

The classifier parses EDGAR complete-submission SGML and selects the primary
filing plus issuer communication exhibits such as `EX-99` press releases. A
filing is recent only when its SEC acceptance timestamp is strictly after the
prior completed session's 16:00 ET close and no later than the target 09:35 ET
cutoff.

The contract is intentionally conservative:

- filing presence is never positive evidence by itself;
- Items 1.01, 2.01, and 2.02 establish direct materiality, not bullish direction;
- positive direction requires a high-precision issuer phrase for a guidance
  raise, regulatory approval, positive clinical result, contract award, or
  record result;
- dilution, financing, guidance reduction, going-concern, delisting, and
  workforce-reduction language overrides positive matches and rejects the pair;
- unrecognized recent primary documents remain unresolved rather than eligible;
- non-SEC events and analyst actions remain unresolved until a direct source or
  directly attributed, independently corroborated report is captured.

This is a high-precision triage contract, not a complete natural-language
understanding system. It is designed to avoid false favorable defaults. It must
not be loosened on these already-inspected dates to manufacture more signals.

## Halt And Tradability Contract

`nasdaq_halts.py` downloads each date from Nasdaq Trader's official historical
halt service, retains the raw response, parses every published interval, and
joins the exact symbol to the clean cross through ten seconds later. Every one
of the 325 clean-trigger windows had an official historical record set available;
none overlapped a halt interval.

An active point-in-time common-stock listing plus no official halt is a useful
historical exchange-state proxy. It is not broker-specific long tradability.
Robinhood's target-date account, restriction, and review state cannot be
reconstructed retrospectively. Historical alpha evaluation may report the proxy
explicitly, but execution qualification must still verify live broker
tradability prospectively and may not relabel the proxy as a broker fact.

## Inspected Result

- 389 exact selected pairs reconciled.
- 106 filing observations classified from 100 unique complete submissions.
- 56 filings were stale because they were accepted before the prior close.
- 30 recent filings remained unresolved.
- 12 filings were material but directionally unresolved.
- One filing had a verified positive issuer direction.
- Seven filings carried a negative or financing conflict.
- At pair level, 351 had no recent SEC primary source, 20 had unresolved recent
  primary evidence, 11 were material but directionally unresolved, one was
  verified positive, and six were conflict rejects.
- 966 official halt rows across all 20 dates were reparsed.
- 325 clean-trigger windows were evaluated and zero had an overlapping halt.

An independent implementation re-read the frozen parent SEC index, re-hashed all
100 complete submissions, rebuilt recency and classification, reparsed all 20
official halt responses, rebuilt interval decisions, and verified the canonical
contexts. Its aggregate result matches the collector exactly.

## Decision

No strategy rule was earned. The catalyst gate is more selective than the prior
filing-presence proxy, and the historical halt input is now closed without
claiming broker-specific tradability. The unchanged champion still lacks a
complete, outcome-blind readiness join for resistance room, structural
invalidation/noise, benchmark-relative strength, and conservative executable
liquidity. Those inputs must be quantified before returns are exposed.

## Runbook

The exact frozen commands are:

```sh
python3 champion_input_fidelity.py freeze

python3 champion_input_fidelity.py collect \
  --manifest historical_batches/champion_input_fidelity/manifests/dataset-champion-input-fidelity-2026-07-19-v2-3098b32480cc59ce0a51624a84db2c0f194a518341f12824e0e615583809b3a5.json

python3 champion_input_fidelity.py inspect \
  --manifest historical_batches/champion_input_fidelity/manifests/dataset-champion-input-fidelity-2026-07-19-v2-3098b32480cc59ce0a51624a84db2c0f194a518341f12824e0e615583809b3a5.json

python3 champion_input_fidelity_inspection.py \
  --manifest historical_batches/champion_input_fidelity/manifests/dataset-champion-input-fidelity-2026-07-19-v2-3098b32480cc59ce0a51624a84db2c0f194a518341f12824e0e615583809b3a5.json
```

The public summaries are
`research_results/2026-07-19-champion-input-fidelity.json` and
`research_results/2026-07-19-champion-input-fidelity-inspection.json`.
