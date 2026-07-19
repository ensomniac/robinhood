# Unchanged-Champion Input Readiness

Dataset: `dataset-champion-input-readiness-2026-07-19-v1`

Source dataset: `dataset-champion-input-fidelity-2026-07-19-v2`

Status: independently inspected `READY` for `DEVELOPMENT_ONLY`

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Question Answered

Before exposing any target-session return, can the exact frozen 389-pair corpus
reproduce enough of the unchanged production input contract to evaluate v3
faithfully?

The answer is no. Zero pairs survive even the already-resolved hard-gate
cascade, and zero pairs satisfy every measured non-catalyst diagnostic proxy.
This is useful negative evidence about readiness and strategy geometry. It is
not alpha evidence and does not earn a looser rule.

## Frozen Boundary

The manifest binds:

- the exact 20 dates and 389 scanner-selected security-date pairs;
- the inspected primary-catalyst, official-halt, and clean-trigger sources;
- the active `strategy_config.toml` bytes, strategy version, and canonical rules
  hash;
- the session calendar and target-date split actions;
- the readiness implementation and every private source hash;
- Alpaca's November 3, 2025 quote-size-in-shares transition;
- a contract that forbids target outcomes, favorable missing values, and new
  variants.

The private join remains under `LOCAL_HISTORICAL_DATA_ROOT`. Public artifacts
contain only counts and hashes. No post-entry return, target, stop outcome,
end-of-day price, or winner/loser label enters this dataset.

## Exact Gates Rebuilt

For each pair the join independently reconstructs:

- verified positive primary-catalyst status and negative/financing conflicts;
- the clean continuous regular-sale cross;
- three quote snapshots at zero, five, and ten seconds;
- quote freshness, uncrossed state, median and maximum spread;
- the engine-compatible chase interval, including the lower bound that the
  final ask must still be at or above the opening high;
- official target-date halt state;
- conservative visible quantity from the minimum best-ask size and last fully
  completed real one-minute volume, each capped at the frozen 5%;
- candidate and SPY/QQQ state using only bars fully completed by the final quote
  snapshot.

The lower chase bound matters. The earlier clean-trigger report counted 155
asks no higher than the 0.15% cap, including asks that had already fallen below
the opening high. The production evaluator requires a nonnegative chase
fraction. Only 101 pairs satisfy both sides. This is a fidelity correction, not
a threshold change.

## Conservative Diagnostics, Not Production Facts

Three remaining fields are deliberately labeled proxies:

- Resistance proxy: the nearest split-adjusted prior-15-session daily high
  above the final ask. A prior high is observable, but is not automatically a
  confirmed technical resistance level. No overhead high does not default to
  unlimited room.
- Stop proxy: the opening-range low as structural invalidation, with the frozen
  0.10 daily-ATR floor. It exposes the v3 geometry but does not prove that the
  level is the best real-time invalidation or outside ordinary noise.
- VWAP/market proxy: provider bars whose full one-minute interval ended by the
  final snapshot. It avoids lookahead but cannot reproduce intraminute VWAP when
  the observation occurs inside a minute.

Broker-specific target-date tradability cannot be reconstructed. The dated
common-stock identity and official no-halt join remain a historical exchange
proxy; live execution qualification must verify the broker fact prospectively.
Sector-specific relative strength also remains absent. Candidate performance
relative to both SPY and QQQ is retained separately and never silently renamed
as sector evidence.

## Independently Inspected Result

Across all 389 selected pairs:

- 325 have a clean continuous cross.
- 309 have all three snapshots.
- 297 have fresh, positive, uncrossed snapshots.
- 108 pass the frozen median and single-snapshot spread limits.
- 101 pass the full engine-compatible chase interval.
- 325 evaluated trigger windows are officially halt-clear.
- 308 have a positive conservative visible-liquidity quantity.
- 309 are above completed-bar VWAP and 284 have flat-to-rising completed-bar
  VWAP.
- 275 have a supportive completed-bar SPY/QQQ state.
- 308 outperform both benchmarks from the open to the final ask.
- Only one has a verified positive recent primary catalyst.

The ordered production cascade is decisive:

1. 389 selected pairs.
2. One verified positive primary catalyst.
3. That one has a clean cross and fresh quotes.
4. It fails the frozen spread limit.
5. It also fails the frozen chase rule.
6. Zero survive the already-resolved hard gates.

The independent diagnostic surface also shows:

- 53 pairs pass all measured non-catalyst execution-geometry gates.
- 44 of those also pass completed-bar market diagnostics.
- Only 11 of 309 proxy-evaluable pairs fit the opening-low/0.10-ATR stop inside
  the frozen 0.8% cap.
- 196 have a known prior daily high above entry; only 50 have at least 2.2% room
  to that conservative proxy. Another 113 have no overhead high in the lookback
  and remain unresolved rather than favorable.
- Zero pass execution geometry, completed-bar market diagnostics, the stop
  proxy, and the known-resistance-room proxy together, even before catalyst.

The independent inspector rejoined every source row, recomputed all quote,
spread, chase, completed-bar, benchmark, visible-liquidity, split-adjusted
resistance, and stop calculations, and reproduced the aggregate counts and
cascade exactly.

## Strategy Decision

Do not expose returns for this corpus and do not change v3. A return analysis
would be dominated by knowingly incomplete catalyst coverage and unresolved
production inputs; it could only encourage post-hoc gate relaxation.

The current rule stack has a genuine deployability problem: the 0.8% stop cap
and 2.2% resistance-room requirement appear jointly difficult for the observed
opening-range geometry. But the diagnostic levels are not yet sufficiently
faithful to say which production rule is wrong. The next evidence must separate:

1. a reproducible real-time structural invalidation/noise definition;
2. a reproducible resistance definition available before entry;
3. a larger, previously uninspected scanner sample with direct catalyst sources;
4. prospective broker tradability and shadow execution calibration.

Only after those contracts are frozen should the system test whether the
unchanged v3 rules select any trades. If they still produce near-zero capacity,
one preregistered rule revision may be tested on new data. It must not be tuned
against these 20 development dates.

## Runbook

```sh
python3 champion_input_readiness.py freeze

python3 champion_input_readiness.py build \
  --manifest historical_batches/champion_input_readiness/manifests/dataset-champion-input-readiness-2026-07-19-v1-00f25fa34ad8b99803e3c5faad6480c3a8394258e5660b9de4a60363b75b955a.json

python3 champion_input_readiness.py inspect \
  --manifest historical_batches/champion_input_readiness/manifests/dataset-champion-input-readiness-2026-07-19-v1-00f25fa34ad8b99803e3c5faad6480c3a8394258e5660b9de4a60363b75b955a.json

python3 champion_input_readiness_inspection.py \
  --manifest historical_batches/champion_input_readiness/manifests/dataset-champion-input-readiness-2026-07-19-v1-00f25fa34ad8b99803e3c5faad6480c3a8394258e5660b9de4a60363b75b955a.json
```

Public results are in
`research_results/2026-07-19-champion-input-readiness.json` and
`research_results/2026-07-19-champion-input-readiness-inspection.json`.
