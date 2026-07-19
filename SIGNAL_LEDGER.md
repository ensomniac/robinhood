# Structured Signal Ledger

`SIGNALS.jsonl` is created on the first call to `strategy_ledger.py record`. It
is the machine-readable companion to `TRADES.md`, not a replacement for the
public narrative or detailed context. The file is append-only: corrections are
new records or explicit versioned migrations, never silent history rewrites.

Two record types are supported:

- `session` proves that a trading day was closed and whether scanner capture was
  complete, a trade was taken, and a no-trade outcome occurred.
- `signal` records every evaluated candidate. A selected eligible trigger must
  be closed in live or shadow mode. An otherwise eligible later trigger blocked
  by the one-entry daily limit uses `decision=missed`, `closed=false`, and a
  `missed_reason`; it remains selection evidence but is not a strategy return.
  Selected winners must not be the only observations retained.

The CLI adds the current strategy version, rules fingerprint, and schema version
to new records. Public aliases use `YYYY-MM-DD-session[-N]` and
`YYYY-MM-DD-SYMBOL-N`; UUIDs and broker/account identifier fields are rejected.
Exact broker identifiers continue to belong only in encrypted detailed context.

## Commands

Append one prepared session or signal object:

```sh
python3 strategy_ledger.py record /path/to/public-record.json
```

Validate JSON, uniqueness, privacy fields, required paired exits, execution
fields, and the full session/signal relationship:

```sh
python3 strategy_ledger.py audit
```

The relationship audit requires exactly one session record for every signal
group; reconciles candidate and triggered counts; permits at most one executed
decision; checks `trade_taken`; and binds every signal to the session's date,
mode, sample phase, rules hash, and capture-completeness claim. Prefer atomic
session-plus-signal batches for generated history. A transient single-record CLI
append is not considered complete until the matching group has been written and
the audit passes.

Calculate expectancy, profit factor, win rate, drawdown, execution percentiles,
no-trade frequency, paired project-versus-EOD exits, and earned maturity:

```sh
python3 strategy_ledger.py report
```

## Required Session Fields

```json
{
  "record_type": "session",
  "session_id": "2026-07-16-session",
  "date": "2026-07-16",
  "mode": "shadow",
  "sample_phase": "pilot",
  "closed": true,
  "session_capture_complete": true,
  "trade_taken": false,
  "candidate_count": 4,
  "triggered_signal_count": 0,
  "no_trade_reason": "No candidate passed every hard gate",
  "rule_violations": []
}
```

## Required Signal Fields

Closed triggered signals require `net_r`, `net_pnl_dollars`, the actual project
exit in R, and the paper-aligned end-of-day shadow exit in R. Closed live signals
also require entry slippage and unprotected-exposure time. A stop execution
requires actual stop slippage and its planned reserve.

Signals may also include a numeric `features` object for the bounded learning
fields `opening_relative_volume`, `score`, `median_spread_bps`, `stop_fraction`,
`resistance_room_fraction`, and `reward_risk`. Unknown or non-finite features are
rejected. `strategy_learning.py` uses these fields for cohort diagnostics; it
does not mutate the strategy.

```json
{
  "record_type": "signal",
  "signal_id": "2026-07-16-XYZ-1",
  "session_id": "2026-07-16-session",
  "date": "2026-07-16",
  "symbol": "XYZ",
  "mode": "shadow",
  "sample_phase": "pilot",
  "session_capture_complete": true,
  "triggered": true,
  "eligible": true,
  "decision": "shadow",
  "closed": true,
  "net_r": 1.2,
  "net_pnl_dollars": 120.0,
  "project_exit_net_r": 1.2,
  "paper_baseline_eligible": true,
  "paper_eod_shadow_net_r": 1.8,
  "entry_slippage_bps": null,
  "unprotected_seconds": null,
  "stop_executed": false,
  "stop_slippage_bps": null,
  "stop_reserve_bps": null,
  "rule_violations": [],
  "rejection_reasons": []
}
```
