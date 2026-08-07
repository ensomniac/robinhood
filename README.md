# Robinhood Codex: Clean Slate

This branch is a deliberately small foundation for historical-data operations,
sanitized account context, immutable research history, and neutral engineering
learning. It contains no active trading strategy, no strategy scheduler, and no
broker execution path. Live trading is explicitly disabled.

The complete pre-reset repository is preserved at the annotated tag
`legacy-pre-clean-slate-2026-08-07`. Private historical data remains in place
outside Git; the preservation boundary and hashes are documented in
[`history/PRE_RESET_MANIFEST.json`](history/PRE_RESET_MANIFEST.json).

## Supported surface

- Canonical per-symbol/per-day historical market-data storage
- Cache-first historical fetches from IBKR, Massive, and Alpaca
- Resumable manifest-based data refresh
- Legacy data migration into the canonical store
- Read-only, runtime-discovered account context with no persisted identifier
- Append-only outcome-exposure and engineering history
- A bounded strategy-neutral learning and audit loop

There is intentionally no signal engine, portfolio allocator, live/shadow
mode, order workflow, or production strategy configuration.

## Setup

```sh
python3 -m pip install -r requirements.txt -r requirements-dev.txt
cp .env.example .env
chmod 600 .env
```

Set `LOCAL_HISTORICAL_DATA_ROOT` in `.env` to the existing absolute private
store. Add only the historical-provider credentials you use. Interactive
Brokers requires its official Python API and an already authenticated TWS or IB
Gateway session; the adapter exposes historical market data only.

Validate the local privacy and history boundaries:

```sh
python3 sensitive_data.py check
python3 sensitive_data.py audit history
python3 outcome_exposure.py audit
python3 progress_history.py audit
python3 learning_loop.py audit
```

## Historical data operations

Audit every canonical document (expensive on the current multi-million-file
store):

```sh
python3 historical_data_cli.py check
```

Fetch one regular session, serving a fidelity-qualified local copy first and
then trying configured providers in deterministic order:

```sh
python3 historical_data_cli.py fetch AAPL --date 2026-07-01
```

Refresh a neutral manifest of symbol/date pairs:

```json
{
  "schema_version": 1,
  "requests": [
    {"date": "2026-07-01", "symbol": "AAPL"},
    {"date": "2026-07-01", "symbol": "MSFT"}
  ]
}
```

```sh
python3 historical_data_cli.py refresh --manifest requests.json
```

Completed requests checkpoint under the external store at
`_operations/refresh/<manifest-sha256>.jsonl`. Rerunning the same manifest skips
hash-valid completed requests. Failures remain retryable and are reported
without changing the manifest or substituting another symbol/date.

Provider connectivity can be checked independently:

```sh
python3 historical_data_cli.py check-providers --symbol AAPL --date 2026-07-01
```

See [`HISTORICAL_DATA_STORE.md`](HISTORICAL_DATA_STORE.md) for the storage,
provider, refresh, and migration contracts.

## Account context

[`history/ACCOUNT_HISTORY.md`](history/ACCOUNT_HISTORY.md) is the only compact
account/trading summary retained on this branch. Account inspection is
read-only: discover the authorized Agentic account at runtime, require an
unambiguous `agentic_allowed=true` response, and never persist or guess its
number. No broker mutation is authorized.

The available session modes make this boundary explicit:

```sh
python3 session_mode.py --list
```

All modes report `broker_actions_allowed: false`.

## Preserved history

The current branch retains:

- the unchanged global outcome-exposure index;
- its exact legacy signal-ledger sources;
- the sanitized account summary;
- the append-only progress history; and
- compact reset manifests and audits.

Detailed legacy code, results, trade contexts, and tracked artifacts remain in
the archive tag. Instructions for inspecting them without disturbing this
branch are in [`history/README.md`](history/README.md).

## Neutral learning

The learning loop operates on engineering and data quality only:

```sh
python3 learning_loop.py inspect
python3 learning_loop.py validate-prompt LEARNING_LOOP.md
python3 learning_loop.py review-plan --objective "Improve refresh diagnostics" \
  --scope historical_data_cli.py --scope tests/test_historical_data_cli.py
python3 learning_loop.py audit
```

It cannot activate a strategy or broker workflow. See
[`LEARNING_LOOP.md`](LEARNING_LOOP.md) for the bounded prompt contract.

## Development checks

```sh
python3 -m ruff check .
python3 -m pytest -q
```

Substantive commits must append one structured entry to
`progress/HISTORY.jsonl`; the tracked pre-commit hook enforces that rule.
