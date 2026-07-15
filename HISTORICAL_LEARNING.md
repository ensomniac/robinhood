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

2. Use point-in-time market/news sources to collect one replay bundle per date
   under the ignored `historical_data/` directory. Select the date before
   collecting its candidate facts. Do not choose a day because its result is
   already known.
3. Validate every bundle before replay:

   ```sh
   python3 historical_learning.py validate historical_data/2025-06-02.json
   ```

4. After enough validated bundles exist, run the requested random batch:

   ```sh
   python3 historical_learning.py run --selection selection.json
   ```

   This guarantees that the dates randomized before collection are exactly the
   dates replayed. `python3 historical_learning.py interactive` provides the CLI
   prompts for a pre-collected unbiased bundle pool.
5. Verify `python3 trade_lifecycle.py audit`,
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

## Limitations

The simulator is deterministic, not a claim of fill certainty. One-minute bars
cannot reproduce queue position, hidden liquidity, sub-minute halts, latency, or
full tape sequencing. The stop-first ambiguity rule and spread/reserve charges
reduce optimistic bias but do not eliminate it. Historical evidence earns only
the maturity permitted by the existing ledger rules and never proves future
profitability.
