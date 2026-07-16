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

2. Use point-in-time scanner and news sources to freeze at least ten distinct
   candidates and their catalyst provenance for each selected date. Select the
   date before collecting its candidate facts. Do not choose a day because its
   result is already known.
3. Run `python3 ibkr_historical.py check`, then use the IBKR collector for every
   frozen candidate's regular-session minute bars, opening-volume lookback,
   prior daily bars, and historical top-of-book snapshots. Web sources are not
   expected to supply those market-data fields. Store the assembled replay
   bundle under the ignored `historical_data/` directory. A frozen evidence
   manifest can be collected and assembled reproducibly with:

   ```sh
   python3 historical_bundle_builder.py \
     historical_data/manifests/evidence-YYYY-MM-DD.json
   ```

   The builder caches each successful raw IBKR response, derives the first ORB
   evaluation time, ATR, opening RVOL rank, VWAP state, resistance, and
   benchmark alignment, then writes a day bundle only after all frozen symbols
   and both benchmarks are complete. Provider permission, sparse-tick, and
   retired-symbol failures remain explicit blockers; the builder never replaces
   a frozen candidate after observing market data.
4. Validate every bundle before replay:

   ```sh
   python3 historical_learning.py validate historical_data/2025-06-02.json
   ```

5. After enough validated bundles exist, run the requested random batch:

   ```sh
   python3 historical_learning.py run --selection selection.json
   ```

   This guarantees that the dates randomized before collection are exactly the
   dates replayed. `python3 historical_learning.py interactive` provides the CLI
   prompts for a pre-collected unbiased bundle pool.
6. Verify `python3 trade_lifecycle.py audit`,
   `python3 strategy_ledger.py audit`, the archived context, and `TRADES.md`.
   Commit and push the public evidence under the repository publishing rules.

If the active context directory is not empty or a selected date already has an
archive folder, replay stops. An existing live position or unresolved broker
order also blocks historical mode at the agent-workflow layer; live safety has
priority over offline research.

## Bundle Contract

Each bundle is a JSON object with `schema_version=1`, the historical date,
`sample_phase`, complete-capture state, point-in-time source attestations, and at
least ten candidate objects:

```json
{
  "schema_version": 1,
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

Candidates are evaluated at their recorded trigger time. The selected simulated
trade is the earliest eligible trigger; simultaneous triggers rank by score,
then OR_RVOL, then symbol. At most one is selected. Later eligible triggers are
logged as `missed` with an open outcome and are excluded from strategy-return
metrics. For every candidate claiming a clean break, validation also proves at
one-minute resolution that its evaluation bar is the first post-9:35 bar to
trade above the supplied opening-range high.

The selected trade fills only if its trigger-minute high reaches the evaluated
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

## Required Interactive Brokers Market-Data Collection

`ibkr_historical.py` is the default required market-data source for historical
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

Determine each candidate's evaluation time from its IBKR session bars: use the
first opening-range-high break from 9:35 through 10:30 ET, or 10:30 with
`clean_break=false` when no break occurred. Then run the full `candidate`
collection at that timestamp so its three quote snapshots are aligned with the
evaluation. Do not declare a market-data blocker merely because the scanner or
news archive lacks bars, quotes, or depth; try the IBKR collection first and
report the exact failed IBKR fact only if the adapter cannot return it.

## Limitations

The simulator is deterministic, not a claim of fill certainty. One-minute bars
cannot reproduce queue position, hidden liquidity, sub-minute halts, latency, or
full tape sequencing. The stop-first ambiguity rule and spread/reserve charges
reduce optimistic bias but do not eliminate it. Historical evidence earns only
the maturity permitted by the existing ledger rules and never proves future
profitability.
