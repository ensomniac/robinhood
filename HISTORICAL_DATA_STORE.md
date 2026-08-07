# Historical Data Store

The canonical historical market-data store is private, append-by-content, and
external to this public repository. Configure its absolute path with
`LOCAL_HISTORICAL_DATA_ROOT`.

## Layout and identity

Each day document lives at:

```text
<root>/<symbol-lower>/<year>/<YYYY-MM-DD>.json.gz
```

`_store.json` fixes the store schema and layout. A day document identifies one
canonical symbol/date and contains zero or more immutable datasets and context
objects. Dataset identity hashes its provider, channel, timeframe, feed,
adjustment, session, scope, quality, limitations, and rows. Context identity
hashes its kind, provider, observation time, and payload.

`HistoricalDayStore.merge()` adds content by identity, validates the complete
document, writes a deterministic gzip payload to a temporary file, fsyncs via
the filesystem boundary, and atomically replaces the destination while holding
the symbol/day lock. It refuses writes that would breach the configured free
disk reserve.

The store can contain more documents than a downstream catalog accepts. The
2026-08-07 preservation audit validated 4,664,104 current day documents, while
the frozen Strategy Lab catalog covered a 4,069,520-row subset. Do not conflate
a catalog count with the store count.

## Provider order

Regular-session bar requests use this deterministic sequence:

1. Local canonical IBKR data
2. Local canonical Massive data
3. Local canonical Alpaca data
4. Live IBKR historical data
5. Live Massive historical data
6. Live Alpaca historical data

The local lane must meet the requested timeframe, feed, adjustment, and
completeness constraints. A cache miss advances to the next lane. Successful
live responses are normalized and merged into the canonical store with request
provenance. No adapter exposes an account, portfolio, or order operation.

Provider fallback is not symbol or date substitution. A request for one exact
pair either returns that pair or fails.

## Commands

### Full audit

```sh
python3 historical_data_cli.py check
```

This decompresses and validates every matching day document. On the current
store it is intentionally expensive. The result reports file, symbol, date,
dataset, context, provider, and validation-error counts.

### One pair

```sh
python3 historical_data_cli.py fetch SYMBOL --date YYYY-MM-DD
```

The request covers 09:30-16:00 America/New_York and requires at least one bar.
The JSON result records the selected provider, row count, canonical path, and
every cache/live attempt category.

### Provider check

```sh
python3 historical_data_cli.py check-providers --symbol SYMBOL --date YYYY-MM-DD
```

This asks each configured live provider for a five-minute historical window and
reports each result separately. It is a network/provider diagnostic, not an
account or strategy check.

### Resumable refresh

`refresh` accepts a JSON object with `schema_version: 1` and a nonempty
`requests` array. Every item must contain exactly a canonicalizable equity
`symbol` and ISO `date`. Duplicate pairs and unknown fields fail closed. The
normalized requests are sorted by date and symbol before the manifest hash is
computed.

```sh
python3 historical_data_cli.py refresh --manifest requests.json
```

For each pair, refresh first seeks a fidelity-qualified local full session. On
a miss it uses the live provider order above. A success is already durably
merged by the recording adapter before an append-only checkpoint is fsynced.
Checkpoint records bind the manifest hash, request hash, provider, row count,
canonical path, completion time, and their own record hash.

Checkpoints live outside Git:

```text
<root>/_operations/refresh/<manifest-sha256>.jsonl
```

On resume, every checkpoint and referenced canonical document is revalidated.
Only exact completed requests are skipped. Malformed, duplicate, wrong-manifest,
or drifted checkpoints fail closed. Failed requests are reported but are not
checkpointed, so the same frozen manifest may retry them later. Refresh never
ranks securities, reads account data, evaluates returns, or places orders.

## Migration

`historical_migration.py` preserves the legacy import path for ignored replay
bundles, provider evidence, and context caches. Migration is content-addressed
and resumable; source hashes and dispositions belong in its external migration
ledger. Keep the original sources after migration. Legacy field names such as
`candidate` describe the old input schema and do not make migration an active
strategy surface.

Inspect the available migration arguments before a run:

```sh
python3 historical_migration.py --help
```

Use a dry run or bounded source selection first when available, then validate
the resulting canonical documents. Never point cleanup tools at the store.

## Retained namespaces

The clean-slate reset deliberately left these private namespaces untouched:

- `_sources/`
- `_derived/`
- `_migrations/`
- `_strategy_lab/` as frozen legacy state
- `_archive/pre-clean-slate-2026-08-07/`
- ignored repository-local historical and learning input roots

The archive inventory hashes retained artifacts but omits secret contents. See
`history/PRE_RESET_MANIFEST.json` for the verification boundary.
