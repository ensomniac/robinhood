# Expansion Champion Input Fidelity

Dataset: `dataset-champion-input-fidelity-2026-07-19-expansion-v1`

Status: frozen before complete-submission and halt collection

Manifest: `043f21e4290bb6019413382e314c13aa284550a03160f7cc5ffed32149a7b46a`

This contract joins the complete 1,987-pair SEC candidate index to the
independently inspected 1,460-window clean-trigger corpus. It freezes the prior
regular-session close for every pair before classification and reuses the
hash-bound conservative SEC classifier to distinguish positive direction,
material-but-unresolved filings, and dilution or negative conflicts.

It also freezes the Nasdaq Trader historical halt source and evaluates official
halts only from each condition-valid clean cross through its final +10-second
quote target. Missing broker-specific historical tradability remains an
explicit prospective-only blocker.

Exact identities, CIKs, filings, classifier evidence, and halt rows remain
outside Git. The source SEC and trigger artifacts, calendar, adapter, legacy
collector, classifier, halt client, and 10 GiB reserve are hash-bound. No target
outcome, alpha, maturity, strategy variant, or production change is permitted.

```sh
python3 champion_input_fidelity_expansion.py freeze
python3 champion_input_fidelity_expansion.py collect <manifest>
python3 champion_input_fidelity_expansion.py inspect <manifest>
```
