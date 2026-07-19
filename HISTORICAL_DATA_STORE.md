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

Compact raw-trade keys are `t`, `p`, `s`, `x`, `cnd`, `id`, `tape`, and
`source_t` for Eastern observation time, price, size, exchange, conditions,
provider trade ID, tape, and the exact provider timestamp. Nanosecond ordering
uses `source_t`; the integer epoch is only a compatibility field. Additional
trade fields are retained under `xtra`. Catalyst-discovery articles are dated
contexts, not bar datasets, and must state whether they are secondary discovery
or primary verified evidence.

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
- bounded raw-trade tape needed to reconstruct a historical trigger, preserving
  conditions and exact provider ordering rather than inferring from a bar high;
- point-in-time contract/security metadata when its observation time is known;
- point-in-time candidate, preflight, earnings, and replay context tied to one
  symbol and day;
- partial or otherwise incompatible series when they are clearly limited and
  excluded from `require_complete` reads.

Do not put these in a symbol/year/day document:

- API keys, credentials, account identifiers, broker order identifiers, or live
  account state;
- frozen multi-symbol manifests, SEC source documents, full replay bundles,
  research outputs, test results, or public batch summaries;
- an inferred or interpolated row presented as a provider observation;
- current contract metadata without a captured-at time presented as historical
  point-in-time truth.

Raw non-daily provider sources belong under
`LOCAL_HISTORICAL_DATA_ROOT/_sources/<source>/`; private multi-symbol indexes and
exact frozen-pair derivatives belong under
`LOCAL_HISTORICAL_DATA_ROOT/_derived/<workflow>/<dataset-id>/`. For example,
SEC submissions and primary filing documents are cached in `_sources/sec/`,
while exact CIK mappings and catalyst/trigger indexes live under `_derived/`.
The dated symbol document may retain a hash-bound context that points to the
source evidence, but it must not duplicate a large filing or market-wide index.
Ignored legacy workflow artifacts may remain at their original paths for
no-loss migration verification; new collection must use the external source and
derived namespaces.

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

A bounded extended-hours bar request is reusable only when one stored
provenance sample covers the entire requested UTC interval with the same bar
size, provider, feed, adjustment, and `use_rth=false`. The dataset remains
`session=all`, `scope=observed_window`, and `quality.complete=false`; its
separate `requested_window_complete=true` means only that the provider paginated
that exact request to exhaustion. Local reads fail closed outside the attested
window and never substitute a regular-session series. This distinction lets a
future premarket-resistance collector reuse 04:00-09:30 ET data without calling
an observed window a complete trading day.

IBKR data is labeled with its SMART feed and provider adjustment basis because
the API does not expose the same explicit adjustment selector as the HTTP
providers. Massive aggregate bars are retained as split-adjusted SIP data, and
its historical quote adjustments and limitations remain explicit. Alpaca
defaults to `feed=sip` and `adjustment=raw`; do not silently substitute IEX for
SIP or raw for split-adjusted data. Feed and adjustment compatibility must be a
deliberate caller requirement. `ALPACA_MINIMUM_INTERVAL_SECONDS` defaults to
0.35 seconds and paces every REST page, including pagination. Reduce it only
when the purchased plan explicitly permits the resulting request rate.

The canonical collector never requires Massive S3 credentials. The dynamic
scanner collector also has a non-S3 path: `scanner_replay_alpaca.py` batches raw
historical Alpaca SIP bars, writes every returned 15-minute regular-session and
one-minute opening-window series through `HistoricalDayStore`, and uses a
private derived index only to accelerate full-universe replay. The opening
window is always `quality.complete=false` with
`opening_window_complete=true|false`; a normal complete one-minute cache read
can therefore never mistake five rows for a full session. A minute-derived
one-day aggregate is stored as `kind=derived`, not as a provider daily bar.
The completed v4 run independently reconciled 608,386 canonical documents,
including 557,520 opening-minute datasets, to all 118 source indexes; its
aggregate evidence is public while licensed symbol rows remain outside Git.

Provider `1Day` bars are not interchangeable with minute-derived session bars.
Alpaca applies type-specific trade-condition rules to daily bars, and the v2
scanner pilot proved that its daily open/volume can differ from regular-session
minute aggregation. Such rows may be retained with a non-preferred
`provider_trade_date` scope and an explicit limitation, but they cannot satisfy
scanner ADV/ATR input. See `SCANNER_REPLAY.md` for the frozen v4 source contract
and failure lineage.

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

Resume the separate full-universe scanner collection without S3:

```sh
python3 scanner_replay_alpaca.py collect \
  historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-19-v4-645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json
```

The generic `fetch` command treats a successful, exhausted request covering the
full 09:30-16:00 ET envelope as complete. It does not require 390 populated
minute buckets: trade-bar APIs can omit intervals without qualifying trades,
and scheduled early closes naturally return fewer rows. The canonical quality
record therefore carries `requested_window_complete=true` and
`sparse_intervals_allowed=true`. Partial-window and empty responses remain
incomplete and cannot satisfy a `require_complete` cache read. Workflows with a
sourced exchange calendar should additionally retain the actual scheduled close
in their point-in-time source contract.

The same request-exhaustion flag applies to non-RTH observed windows, but those
windows are selected by exact provenance coverage rather than by
`require_complete`. A 2026-07-19 live Alpaca SIP pilot fetched 322 AAPL bars for
the 2026-07-17 04:00-09:30 ET window and reproduced the same 322 epochs from the
local store on the immediate cache read. This is a cache-path integration test,
not strategy evidence.

Native Alpaca `15Min` bars are canonicalized as `timeframe=15m`. A complete
local one-minute regular-session series may also be aggregated deterministically
to 15 minutes. A 252-session live pilot returned and cached 6,552 AAPL bars with
exact OHLCV replay across all 252 day files. The pre-entry structure collector
uses this coarse history plus exact selected-symbol premarket windows instead of
mirroring unnecessary full-session one-minute history; see
`PREENTRY_STRUCTURE.md`.

The replay bundle and preflight CLIs also open the canonical store. Their
ignored raw evidence shards remain for exact workflow resume and bundle audit,
but successful market-data calls are recorded canonically at collection time.
Use `historical_data_cli.py fetch` for a future bulk-history mode rather than
calling low-level provider diagnostics directly.

Before a new bulk manifest is frozen, measure the pilot's canonical bytes per
symbol-session and provider requests per symbol-session, project both over the
exact target set, and inspect free space on the volume containing
`LOCAL_HISTORICAL_DATA_ROOT`. Preserve additional capacity for atomic temporary
files and a full validation pass. Prefer selected-symbol one-minute collection
over a full-universe one-minute mirror; broad daily or coarse intraday coverage
and narrow trade-capable detail provide the useful fidelity at far lower storage
and request cost.

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
