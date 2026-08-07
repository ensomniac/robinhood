# Preserved History

The repository was frozen before its 2026-08-07 clean-slate reset. The complete
pre-reset tracked tree is recoverable from the annotated Git tag
`legacy-pre-clean-slate-2026-08-07`; it is historical evidence, not an active
strategy or an authorization to trade.

Inspect the old tree without altering the current branch:

```sh
git show --stat legacy-pre-clean-slate-2026-08-07
git worktree add ../robinhood_codex-legacy legacy-pre-clean-slate-2026-08-07
```

Remove that inspection worktree with normal Git worktree commands when it is
no longer needed. Do not copy an old strategy back into the clean branch merely
to reuse its results; its contamination and adverse evidence remain binding.

## Durable private data

Historical market data remains outside Git under the configured
`LOCAL_HISTORICAL_DATA_ROOT`. No canonical market document, provider cache,
private account identifier, or secret was copied into this public history
directory.

The private preservation inventory is stored at:

```text
/Users/ensomniac/trade/historical_data/_archive/pre-clean-slate-2026-08-07/
```

Its summary and compressed catalogs bind the canonical file catalog and all
retained noncanonical artifacts. Secret contents are deliberately absent from
that inventory. `PRE_RESET_MANIFEST.json` records the public hashes needed to
verify the boundary.

## Compact ledgers

- `OUTCOME_EXPOSURE_INDEX.jsonl` is the unchanged global contamination ledger.
- `legacy/SIGNALS.jsonl` and `legacy/PORTFOLIO_SIGNALS.jsonl` are exact compact
  source-ledger copies required to audit its baseline records.
- `ACCOUNT_HISTORY.md` is the sanitized account/trading summary retained on the
  clean branch.
- `progress/HISTORY.jsonl` remains the append-only engineering and research
  history.

Run `python3 outcome_exposure.py audit` after any history operation. Existing
records are immutable; new research that accesses outcomes must append a new
record rather than editing or deleting prior exposure.
