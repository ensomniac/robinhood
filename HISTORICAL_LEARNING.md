# Historical Learning

Historical mode replays the current frozen strategy against a randomly selected,
previously unarchived trading day. It is shadow research only: the workflow must
not call broker review, placement, or cancellation tools and cannot change live
strategy rules.

## Operator Workflow

Start with the session picker and select historical mode:

```sh
python3 session_mode.py
```

The agent asks how many days to simulate. For each requested day:

1. Obtain an authoritative exchange-calendar JSON array of `YYYY-MM-DD` trading
   dates, then select unarchived dates and retain the reported random seed:

   ```sh
   python3 historical_learning.py select trading-days.json --days 3 > selection.json
   ```

2. Use point-in-time scanner and news sources to create a ranked draft pool with
   at least 20 distinct candidates and preferably 30-50 when the source supports
   it. Select the date before collecting its candidate facts. Do not choose a day
   because its result is already known. Before freezing the final ten, run a
   strategy-aware
   IBKR pre-session-history preflight:

   ```sh
   python3 historical_universe.py \
     historical_data/manifests/draft-YYYY-MM-DD.json \
     --output historical_data/manifests/evidence-YYYY-MM-DD.json \
     --workers 4
   ```

   The draft uses `candidate_pool_by_date`; the output uses
   `candidates_by_date` and records accepted, skipped, and unused buffered
   symbols. Preflight never requests target-session prices. It first rejects a
   draft record that is not explicitly a U.S.-listed common stock or already has
   a known dilution conflict. It requests prior daily bars first and enforces the
   configured 14-session average-volume and ATR gates before paying for explicit
   contract resolution and opening history. An IBKR error 200 from that STK/USD
   request remains an unresolvable-symbol skip. A surviving symbol still needs 14 prior
   9:30 five-minute bars with positive volume and at least 15 prior daily
   sessions. A symbol that cannot satisfy those immutable evaluator inputs may
   be skipped for the next ranked buffered name. Provider-wide permission,
   connection, and pacing failures stop the batch. If the buffer is exhausted,
   collection remains blocked rather than freezing fewer than ten names.

   Preflight checkpoints every examined `symbol + replay date` atomically under
   ignored `historical_data/preflight/` storage. Rerunning the same draft reuses
   matching strategy/rules qualification and streams whether each symbol was
   ready, skipped, or cached. Only candidates that pass the daily gates request
   a 28-calendar-day five-minute opening-history window, which fits in one
   provider chunk. A symbol-scoped IBKR HMDS `query returned no data` response is
   an input-incomplete buffered skip, while pacing, permission, connection, and
   timeout failures remain batch
   blockers. Accepted cache entries retain only pre-session opening/daily bars,
   explicitly attest that no target-session price was observed, carry a content
   hash copied into the frozen manifest, and are reused by bundle collection
   only after the rule fingerprint and both hashes verify.

   The default four-worker scheduler overlaps provider response latency in
   bounded rank-order batches. Completion order never changes accepted order.
   At most the remainder of the active batch can finish beyond the tenth
   accepted rank; those pre-session-only results are cached and listed as
   speculative, but remain outside the frozen universe. A provider-wide failure
   prevents the next batch from launching. Use a smaller `--workers` value when
   telemetry shows IBKR soft throttling.

   On the 2026-07-16 ten-date pass, the old preflight aborted after roughly 24
   minutes without a checkpoint. The optimized pass examined 102 buffered names
   and completed in 736.5 seconds; an exact cache-resume rerun completed in 1.13
   seconds. These measurements are machine/provider observations, not strategy
   evidence. The daily-first and one-chunk changes landed after that benchmark;
   measure their cold-run impact on the next new batch rather than treating the
   projected request reduction as a measured speedup.
3. Run `python3 ibkr_historical.py check`, then use the IBKR collector for every
   frozen candidate's regular-session minute bars, opening-volume lookback,
   prior daily bars, and historical top-of-book snapshots. Web sources are not
   expected to supply those market-data fields. Store the assembled replay
   bundle under the ignored `historical_data/` directory. A frozen evidence
   manifest can be collected and assembled reproducibly with:

   ```sh
   python3 historical_bundle_builder.py \
     historical_data/manifests/evidence-YYYY-MM-DD.json \
     --workers 4
   ```

   The builder caches each successful raw provider response, derives the first
   completed ORB crossing bar, reserves the following minute for quote snapshots,
   and evaluates at that quote window's closing boundary. It then derives ATR,
   opening RVOL rank, VWAP state, resistance, and benchmark alignment. It writes
   a day bundle only after all frozen symbols and both benchmarks are complete.
   When the evidence manifest links a valid
   preflight cache, the builder reuses its 14 opening-volume bars and prior daily
   history, then requests only target-session minute bars and quote evidence for
   those fields. A retryable transport failure stops the
   current request stream immediately, opens one fresh connection by default,
   and resumes from cached files. A permanent failure stops the affected date
   after its first blocker instead of producing one false failure per remaining
   symbol. The atomic public status defaults to
   `historical_batches/<manifest-name>.json`.

   Benchmarks and frozen candidates use the same bounded deterministic worker
   model on one read-only TWS connection. Successful raw files are written by
   their workers before ordered result handling, so an interruption can reuse
   already-completed work. The command result includes wall time, request
   telemetry, cache-hit counters, and preflight-reuse counts. See
   `HISTORICAL_THROUGHPUT_PLAN.md` for the measured bottleneck, tuning rules, and
   large-batch acceptance protocol.

   IBKR is primary. If `MASSIVE_API_KEY` is configured, a permanent IBKR
   bar/quote fidelity gap may be recollected from Massive's adjusted SIP
   aggregates and historical NBBO quotes. A connection outage never triggers a
   provider switch. Every recovered candidate and benchmark records its actual
   provider; no candidate is replaced and no missing bar or quote is fabricated.
4. Validate every bundle before replay:

   ```sh
   python3 historical_learning.py validate historical_data/2025-06-02.json
   ```

5. Replay every validation-grade date as soon as it is ready while preserving
   the original frozen selection:

   ```sh
   python3 historical_learning.py run \
     --selection selection.json \
     --ready-only
   ```

   `--ready-only` skips already archived dates, replays each valid selected date,
   and records missing/invalid dates as blocked. It never substitutes another
   date. The default status path under `historical_batches/` is updated before
   replay and after every completed date, so rerunning the same command is
   idempotent and resumable. Omit `--ready-only` only when an all-or-nothing
   atomic batch is explicitly required. `historical_learning.py interactive`
   remains available for a pre-collected unbiased bundle pool.
6. Verify `python3 trade_lifecycle.py audit`,
   `python3 strategy_ledger.py audit`, the archived context, and `TRADES.md`.
   Commit and push the public evidence under the repository publishing rules.

If the active context directory is not empty or a selected date already has an
archive folder, strict replay stops; ready-only replay records it as already
completed. An existing live position or unresolved broker order also blocks
historical mode at the agent-workflow layer; live safety has priority over
offline research.

The historical engineering gate is at least 16 validation-grade completions
from 20 newly randomized dates (80% yield), zero date/symbol substitutions, and
zero cascade errors. Batch status computes this gate; it is an infrastructure
acceptance test, not strategy evidence and not permission to weaken fidelity.

## Bundle Contract

New bundles use `schema_version=2`; the validator continues reading immutable
legacy schema-1 bundles. Each bundle contains the historical date,
`sample_phase`, complete-capture state, point-in-time source attestations, and at
least ten candidate objects:

```json
{
  "schema_version": 2,
  "date": "2025-06-02",
  "sample_phase": "pilot",
  "session_capture_complete": true,
  "simulation_account_equity": 25000,
  "simulation_buying_power": 25000,
  "source": {
    "provider": "named point-in-time provider",
    "captured_at": "2026-07-15T12:00:00+00:00",
    "point_in_time": true,
    "regular_hours_only": true,
    "split_adjusted": true,
    "historical_quotes_and_depth": true,
    "catalysts_point_in_time": true,
    "universe_capture_complete": true
  },
  "candidates": []
}
```

`source.provider` must identify the actual source. The boolean attestations are
claims that the collected data satisfies the contract, not values to set merely
to pass validation. If historical top-of-book depth, point-in-time catalysts, or
full scanner capture is unavailable, the day is not validation-grade and must
not be simulated into `SIGNALS.jsonl`.

The simulation equity and buying power are explicit synthetic inputs used only
for deterministic sizing. They must be positive, buying power cannot be below
equity, and the runner overrides any account values inside candidate payloads.
Do not collect or insert the live account balance for historical replay.

A confirmation-phase bundle additionally requires:

```json
{
  "preregistration": {
    "registered_at": "2026-07-15T11:00:00+00:00",
    "manifest_hash": "public hash of the frozen collection plan"
  }
}
```

The preregistration must precede `source.captured_at`. Pilot observations cannot
be relabeled as confirmation after their outcomes are seen.

Each candidate requires:

- A public `signal_id` in `YYYY-MM-DD-SYMBOL-N` form and matching symbol.
- An evaluation time from 9:35 through 10:30 ET.
- For schema 2, `evaluation_basis=next_minute_after_completed_breakout_bar`.
- A catalyst URL, timezone-aware publication time no later than evaluation, and
  point-in-time attestation.
- A complete input object accepted by `strategy_engine.py`. Its three quote/book
  snapshots also include `observed_at_et` no later than evaluation.
- Exactly 390 ordered, non-interpolated regular-session one-minute bars from
  9:30 through 15:59 ET. Each bar contains `time_et`, OHLC, volume, and
  `interpolated=false`.
- Optional runner fields only when the historical record supports every runner
  gate: `runner_eligible=true`, `above_rising_vwap=true`,
  `market_supportive=true`, `catalyst_intact=true`, `spread_fraction` no greater
  than 0.001, and a `structural_stop` below the bar close.

The evaluator payload remains the numeric source for OR_RVOL, score, stop,
sizing, spread, depth, VWAP, catalyst, market, and resistance gates. The replay
runner replaces its maturity with the evidence-earned state and its mode with
`shadow`; it does not relax any gate.

## Replay Semantics

Schema-2 candidates are evaluated after the first completed one-minute bar whose
high crossed the opening range and a subsequent full minute reserved for the
three-snapshot quote window. This prevents quotes taken before the crossing bar
completed from being treated as though the later intraminute high were already
known. Legacy schema-1 bundles retain their original trigger-minute meaning and
are never silently rewritten.

The selected simulated trade is the earliest eligible trigger; simultaneous
triggers rank by score, then OR_RVOL, then symbol. At most one is selected. Later
eligible triggers are logged as `missed` with an open outcome and are excluded
from strategy-return
metrics. For every schema-2 candidate claiming a clean break, validation also
proves at one-minute resolution that the bar two minutes before evaluation is the
first post-9:35 bar to trade above the supplied opening-range high and that no
quote snapshot precedes its completion.

The selected trade fills only if its evaluation-minute high reaches the evaluated
entry limit. Minute data cannot reveal event order inside a bar, so a bar that
contains both stop and target resolves to the stop. Stop fills conservatively
include the planned reserve. The project exit follows the protective stop,
qualified +2% runner behavior, and 3:50 PM force-flat. A paired baseline holds
to the original stop or the last regular-session bar. MFE and MAE use only bars
observed through the project exit.

Every candidate and the session receive temporary context under `trades/active/`.
At completion, the runner embeds a terminal outcome and moves all context to
`trades/archived/YYYY_MM_DD/`. It atomically appends the day's validated session
and signal records as one ledger batch.

## Historical Market-Data Providers

`ibkr_historical.py` is the default primary market-data source for historical
replays. It is an independent, read-only client for a locally logged-in Trader
Workstation or IB Gateway. It does not import IABApp and exposes no account,
portfolio, order, or execution methods. Configure its socket through the ignored
`.env`, then verify the handshake before collecting any candidate:

In TWS, `API > Settings` must have `Enable ActiveX and Socket Clients`,
`Read-Only API`, and `Allow connections from localhost only` enabled. Its socket
port must match `IBKR_PORT`. No IBKR API key or account identifier is required.

The local `.env` may set `IBKR_AUTO_START_TWS=true` and point
`IBKR_TWS_APP_PATH` at the installed application. When `check` finds no socket,
the adapter then attempts to launch TWS and waits for the configured timeout. It
cannot complete an interactive login: if the socket remains unavailable, start
or log in to TWS and rerun `check`. Do not disable read-only or localhost-only
mode to work around a connection failure.

```sh
python3 ibkr_historical.py check
```

Collect every frozen candidate's raw market-data evidence with:

```sh
python3 ibkr_historical.py candidate AAPL \
  --date 2026-05-12 \
  --evaluation-time 09:40:00 \
  --output historical_data/ibkr/2026-05-12-AAPL.json
```

The output contains the full regular-session one-minute trade bars, 14 prior
time-matched opening-volume bars, prior daily bars, historical bid/ask ticks with
top-of-book sizes, and three strategy-shaped quote snapshots. `bars` and
`quotes` subcommands support narrower ad hoc collection. IBKR historical volume
is filtered, and historical bid/ask ticks provide top-of-book size rather than a
full depth ladder. Those top-of-book sizes satisfy the replay bundle's historical
depth evidence requirement. The adapter therefore supplies the required market
data but cannot by itself attest full historical scanner capture or point-in-time
catalyst fidelity; obtain only those two evidence classes from independent
point-in-time sources.

When the narrow quote window is empty, the collector makes one regular-session
lookback request and preserves the last observable quote with its actual age.
The evaluator still hard-rejects a stale snapshot; the fallback records the
liquidity failure rather than making the quote appear fresh. A symbol with no
same-session quote evidence remains a collection blocker.

Determine each candidate's evaluation time from its IBKR session bars: find the
first opening-range-high crossing bar from 9:35 through 10:28 ET, wait for that
bar to complete, use the following minute for the three quote snapshots, and
evaluate at the quote window's closing boundary. Use 10:30 with
`clean_break=false` when no sufficiently early break occurred. Do not declare a
market-data blocker merely because the scanner or
news archive lacks bars, quotes, or depth; try the IBKR collection first and
report the exact failed IBKR fact only if the adapter cannot return it.

### Optional Massive SIP fallback

Set `MASSIVE_API_KEY` only in the ignored `.env` to enable the read-only
fallback. Optional settings are `MASSIVE_BASE_URL` and
`MASSIVE_TIMEOUT_SECONDS`. The key is never written to a bundle, status, error,
or public configuration output.

`historical_providers.py` normalizes adjusted Massive minute aggregates and
historical NBBO quotes to the same read-only client contract used by the bundle
builder. Five-minute opening bars and prior daily bars are resampled only from
regular-session minute aggregates. Massive does not emit an aggregate for a
minute with no eligible trade; the adapter preserves that absence and the 390-
minute bundle validator fails closed rather than filling or interpolating it.
Because historical NBBO records have no aggregate-style `adjusted` parameter,
the adapter queries Massive's split factors and adjusts quote prices and sizes
to the same current-share basis as adjusted aggregates; an invalid factor fails
closed.
Pagination is restricted to the configured HTTPS origin before the API key is
forwarded. Provider contracts: [adjusted stock aggregates](https://massive.com/docs/rest/stocks/aggregates/custom-bars)
[historical stock NBBO quotes](https://massive.com/docs/rest/stocks/trades-quotes/quotes),
and [split adjustment factors](https://massive.com/docs/rest/stocks/corporate-actions/splits).

Fallback is limited to permanent market-data fidelity gaps after IBKR has been
attempted. Retryable disconnects, timeouts, pacing failures, and server errors
are retried or surfaced; they do not silently select a different provider.
Separate point-in-time scanner and catalyst evidence is still mandatory.

The live OR_RVOL rule remains unchanged: all 14 prior opening volumes must be
positive. A provider's zero-volume representation is a preflight/fallback issue,
not permission to reinterpret or silently version the production strategy.

## Limitations

The simulator is deterministic, not a claim of fill certainty. One-minute bars
cannot reproduce queue position, hidden liquidity, sub-minute halts, latency, or
full tape sequencing. The stop-first ambiguity rule and spread/reserve charges
reduce optimistic bias but do not eliminate it. Historical evidence earns only
the maturity permitted by the existing ledger rules and never proves future
profitability.
