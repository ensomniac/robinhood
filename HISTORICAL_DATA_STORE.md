# Canonical Local Historical Data Store

Per-symbol, per-day market observations belong in the private directory named by
`LOCAL_HISTORICAL_DATA_ROOT`. They do not belong in Git, S3, or an application
database. The repository still owns code, frozen manifests, replay bundles,
test results, migration summaries, and other compact evidence.

## Why this is the storage boundary

Historical research repeatedly asks for the same expensive immutable facts.
Keeping one verified local copy makes the normal path cache-first, removes
provider latency from repeated strategy tests, avoids repository growth and
licensing mistakes, and lets different workflows reuse identical observations.
The symbol-first layout is easy to browse and copy selectively. Deterministic
gzip-compressed JSON keeps the content inspectable with ordinary tools while
substantially reducing the multi-year footprint.

The store is not a database of mutable truth. A provider series is immutable by
content. A second provider, feed, adjustment basis, interval, or corrected row
becomes another dataset in the same day document; it never silently overwrites
or blends the existing series. Callers select a compatible whole-provider
series in explicit priority order.

## Required layout

The environment setting must be an absolute directory outside this public
repository:

```text
LOCAL_HISTORICAL_DATA_ROOT/
  _store.json
  _locks/
  _migrations/
    robinhood-codex-canonical-day-v1/
      summary.json
      source-files.jsonl.gz
  aapl/
    2025/
      2025-06-02.json.gz
    2026/
      2026-03-03.json.gz
  spy/
    2026/
      2026-03-03.json.gz
```

Symbols are lowercase only in directory names. The document repeats the
uppercase normalized symbol and ISO session date. One file represents one US
equity symbol and one Eastern-time session date, but may hold several distinct
provider datasets and dated contexts.

## Day document contract

The logical JSON shape is:

```json
{
  "schema_version": 1,
  "kind": "us_equity_daily_history",
  "symbol": "AAPL",
  "date": "2026-03-03",
  "timezone": "America/New_York",
  "datasets": [
    {
      "id": "bars:alpaca:trades:1m:<content-prefix>",
      "kind": "bars",
      "provider": "alpaca",
      "channel": "trades",
      "timeframe": "1m",
      "feed": "sip",
      "adjustment": "raw",
      "session": "regular",
      "scope": "full_session",
      "content_sha256": "<sha256>",
      "quality": {"row_count": 390, "complete": true},
      "limitations": [],
      "rows": [
        {
          "t": "2026-03-03T09:30:00-05:00",
          "o": 241.5,
          "h": 241.8,
          "l": 241.4,
          "c": 241.7,
          "v": 128430,
          "n": 912,
          "vw": 241.63,
          "i": false
        }
      ],
      "provenance": {
        "source_count": 1,
        "source_fingerprints": ["<sha256>"],
        "samples": [{"source_type": "live_provider_collection"}]
      }
    }
  ],
  "contexts": [
    {
      "id": "context:ibkr:security_contract:<content-prefix>",
      "kind": "security_contract",
      "provider": "ibkr",
      "observed_at": "2026-03-03T14:40:00+00:00",
      "payload": {},
      "content_sha256": "<sha256>",
      "provenance": {}
    }
  ]
}
```

Compact bar keys are `t`, `o`, `h`, `l`, `c`, `v`, `n`, `vw`, and `i` for the
Eastern ISO timestamp, OHLC, volume, trade count, volume-weighted price, and
interpolation flag. Compact quote keys are `t`, `bp`, `ap`, `bs`, `as`, `bx`,
`ax`, `cnd`, and `tape`. Provider fields not covered by the canonical keys are
retained under `x`; they must not be discarded merely because another provider
does not emit them.

Every dataset identity includes its provider, channel, timeframe, feed,
adjustment, session, scope, quality, limitations, and rows. Every context
identity includes its kind, provider, observation time, and payload. Store
audits recompute both hashes and IDs. Writes use per-day file locks, a temporary
file, and atomic replacement, so concurrent collectors cannot expose a partial
document.

## What belongs here

Store immutable observations or dated symbol context:

- regular and extended-session OHLCV bars, with their actual timeframe;
- historical top-of-book quotes and sizes, including provider timestamps,
  exchange codes, conditions, tape, feed, and adjustment basis;
- point-in-time contract/security metadata when its observation time is known;
- point-in-time candidate, preflight, earnings, and replay context tied to one
  symbol and day;
- partial or otherwise incompatible series when they are clearly limited and
  excluded from `require_complete` reads.

Do not put these in the canonical day store:

- API keys, credentials, account identifiers, broker order identifiers, or live
  account state;
- frozen multi-symbol manifests, SEC source documents, full replay bundles,
  research outputs, test results, or public batch summaries;
- an inferred or interpolated row presented as a provider observation;
- current contract metadata without a captured-at time presented as historical
  point-in-time truth.

Those non-daily artifacts can remain under ignored repository paths when their
existing workflows need them. The migration ledger hashes and accounts for them
without pretending they are canonical daily observations.

## Provider and cache policy

The collection order is:

1. a compatible complete local dataset for IBKR, Massive, then Alpaca;
2. live read-only IBKR historical data;
3. Massive historical SIP data;
4. Alpaca historical market data.

A failed provider attempt may advance on timeout, rate limit, permission, or
fidelity failure. A single candidate is recollected as a whole from the next
provider. Bars, daily history, opening history, and quote evidence from
incompatible feeds are never spliced together to make a candidate appear
complete. Provider attempts and the provider that supplied the accepted data
remain visible in collection status and replay provenance.

IBKR data is labeled with its SMART feed and provider adjustment basis because
the API does not expose the same explicit adjustment selector as the HTTP
providers. Massive aggregate bars are retained as split-adjusted SIP data, and
its historical quote adjustments and limitations remain explicit. Alpaca
defaults to `feed=sip` and `adjustment=raw`; do not silently substitute IEX for
SIP or raw for split-adjusted data. Feed and adjustment compatibility must be a
deliberate caller requirement.

The canonical collector never requires Massive S3 credentials. Flat files may
remain useful for a separate market-wide scanner-replay bulk ingestion, but the
per-symbol/day cache uses read-only APIs and local storage.

## Operator commands

Validate the store, including every document's content hashes:

```sh
python3 historical_data_cli.py check
```

Test each configured live provider independently on a completed session. Every
successful response is retained:

```sh
python3 historical_data_cli.py check-providers \
  --symbol AAPL --date 2026-07-17
```

Fetch a complete day cache-first, then IBKR, Massive, and Alpaca:

```sh
python3 historical_data_cli.py fetch AAPL --date 2026-07-17
```

The generic `fetch` command currently requires the standard 390 one-minute
regular-session rows. It fails closed on a newly collected 210-row early-close
session because the store does not yet carry an authoritative session-schedule
table that can distinguish a real half day from a provider truncation. Legacy
early closes with explicit source completeness are preserved, but extending the
one-command collector requires a sourced calendar with close times; do not
weaken the row gate to guess.

The replay bundle and preflight CLIs also open the canonical store. Their
ignored raw evidence shards remain for exact workflow resume and bundle audit,
but successful market-data calls are recorded canonically at collection time.
Use `historical_data_cli.py fetch` for a future bulk-history mode rather than
calling low-level provider diagnostics directly.

## Migration and no-loss audit

Run the non-destructive migration from the repository root:

```sh
python3 historical_migration.py
```

It translates the old `/Users/ensomniac/trade/data/intraday/` tree and the
per-day portions of ignored repository `historical_data/`. Legacy bar metadata,
provider-specific fields, candidate/preflight context, quote context, and dated
contract/earnings context are retained. Partial and incompatible records are
stored with explicit limitations, not promoted to preferred complete series.

Every source file is hashed into
`_migrations/robinhood-codex-canonical-day-v1/source-files.jsonl.gz` with one of
three outcomes:

- `migrated`: daily observations or dated context were translated;
- `retained`: the file is a non-daily artifact and remains unchanged at source;
- `omitted_non_data`: filesystem metadata such as `.DS_Store` only.

The migration never deletes or edits a source. This gives the audit a literal
file-by-file proof of accounting and makes rollback possible. Remove or archive
an old source tree only in a separately authorized operation after comparing
the ledger, canonical audit, and backups. Re-running the migration is safe:
content IDs deduplicate identical observations while provenance fingerprints
record distinct sources.

## Rules for future writers

All new historical collection features must use `HistoricalDayStore` and either
`RecordingHistoricalClient` or an equivalent tested writer. They must:

1. require `LOCAL_HISTORICAL_DATA_ROOT` and reject a repository-local path;
2. retain provider, feed, adjustment, session, scope, and limitations;
3. partition rows by Eastern session date before writing;
4. preserve provider-only fields under `x`;
5. use atomic store merges and never hand-edit a gzip day file;
6. serve a compatible complete local series before making a provider call;
7. keep whole-provider candidate fidelity and never fill missing rows;
8. add tests for normalization, retry classification, pagination, idempotence,
   corruption detection, and fallback order;
9. keep raw credentials and bulky daily observations out of Git;
10. run `historical_data_cli.py check` before treating a collection as ready.
