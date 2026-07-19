# Resumable Expansion Clean-Trigger Fidelity

Dataset: `dataset-clean-trigger-fidelity-2026-07-19-expansion-v1`

Status: independently inspected `READY` for clean-trigger and quote-input
fidelity only

Manifest: `44225398ce0fde7acf19ddb6e87a0ace0195e19fed482d3258530f479e4d6ad4`

The contract classifies the exact 1,460 ordered raw crossing windows already
frozen by the inspected expansion join. It uses the unchanged validated Alpaca
SIP bar-update and continuous-regular-cross condition rules, then selects
top-of-book observations causally at zero, five, and ten seconds after the clean
cross.

An incomplete local quote window receives one bounded Alpaca SIP refresh. If
the provider again returns no observation, the record becomes
`MISSING_TRIGGER_NBBO`; no quote is fabricated, no provider is substituted, and
the rest of the corpus continues. Checkpoints are written outside Git every 25
new windows. Completed rows resume in frozen order, while true
`COLLECTION_ERROR` rows remain retryable.

The manifest binds the source join/result/trigger index, the complete SEC index,
all 1,460 ordered private identities, condition versions, missing-observation
policy, collector hash, and 10 GiB disk reserve. It reads no return or outcome
and cannot establish trade eligibility, alpha, maturity, a strategy variant, or
a production change.

Collection classified all 1,460 windows with zero collection errors. All had a
clean continuous cross, but only 569 first raw price crosses were themselves
clean; 886 shifted later, with a 54.03-second maximum. There were 1,340 complete
three-snapshot windows, 1,267 preliminary fresh/uncrossed windows, 726 chase-cap
passes, and four explicit `MISSING_TRIGGER_NBBO` blockers. These are input-gate
counts, not eligible signals or returns.

```sh
python3 clean_trigger_fidelity_expansion.py freeze
python3 clean_trigger_fidelity_expansion.py collect <manifest>
python3 clean_trigger_fidelity_expansion.py inspect <manifest>
```
