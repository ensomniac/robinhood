# Expansion Candidate Join

Dataset: `dataset-selected-candidate-join-2026-07-19-expansion-v1`

Status: frozen before network collection

Manifest: `15d8baefcd4bedb3cf473cad4b98638a0c101ae2a1bfc64552d80c72dede2f6b`

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Contract

The join consumes exactly the 1,987 private security-date pairs in the
selection-only expansion contract. It permits no date or symbol substitution.
The adapter verifies the source selection hash, writes a dataset-specific
private copy outside Git, and binds both itself and the unchanged 389-pair
collector by SHA-256. This reuses tested acquisition code without editing the
completed legacy evidence dependency or creating a duplicate implementation.

The staged collector acquires:

- raw-adjustment Alpaca SIP one-minute candidate and SPY/QQQ bars;
- Alpaca/Benzinga news strictly through the target 09:35 ET cutoff, as
  secondary discovery only;
- the first aggregate minute whose high crosses the frozen opening-range high;
- raw SIP trades in that minute and top-of-book quotes at the first observed
  cross and roughly five and ten seconds later.

The contract does not verify a primary catalyst, classify trade conditions,
read target returns, derive alpha, change v3, or support maturity or promotion.
Those claims remain blocked. A 10 GiB free-space reserve and a pilot are
mandatory before bulk collection; the freeze observed about 81.7 GiB free.

## Why This Is Safe Reuse

`selected_candidate_join.py` is retained byte-for-byte because its hash is part
of the completed 389-pair manifest. `selected_candidate_join_expansion.py`
activates it only after verifying the new manifest's adapter and collector
hashes and assigning the new dataset namespace in the isolated process. Raw
identities, rows, checkpoints, and errors remain under
`LOCAL_HISTORICAL_DATA_ROOT`; public artifacts contain aggregate counts and
hashes only.

## Runbook

```sh
python3 selected_candidate_join_expansion.py freeze
python3 selected_candidate_join_expansion.py pilot <manifest>
python3 selected_candidate_join_expansion.py collect <manifest>
python3 selected_candidate_join_expansion.py derive <manifest>
python3 selected_candidate_join_expansion.py collect-tape <manifest>
python3 selected_candidate_join_expansion.py inspect <manifest>
```

After this base join is independently inspected, primary issuer/SEC/exchange
catalyst classification and condition-aware clean-trigger aggregation must be
frozen and completed before unchanged-v3 gate attrition or outcomes are read.
