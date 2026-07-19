# Dynamic 09:35 Scanner Replay

Status: v4 is `READY` and independently inspected. All 118 source sessions and
20 target-date rankings passed the source, point-in-time input, metric, ranking,
and canonical-persistence gates on 2026-07-19.

This workflow closes the survivorship and shortlist-selection gap in the
catalyst-derived historical corpus. It reconstructs the common-stock universe
that could have existed at 09:35 ET on each frozen date. It does not establish a
catalyst, tradability, spread, depth, breakout, order, or outcome decision.

## Active frozen contract

The exact 20 dates selected with seed `20260718` remain unchanged. The original
random order remains in
`historical_batches/scanner_replay/selection-2026-07-18-20-days.json`; no date or
symbol may be substituted. Their union with each 15-session lookback requires
118 distinct sessions from 2025-12-10 through 2026-06-23.

The active pre-collection contract is:

```text
historical_batches/scanner_replay/manifests/
  dataset-production-scanner-replay-2026-07-19-v4-
  645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json
```

`scanner-rules-v2.json` preserves every v1 threshold, ranking rule, lookback,
date, and rejection policy. It only separates the provider-neutral scanner math
from the provider-specific collection contract. The manifest binds both that
rules hash and the exact collector implementation hash.

`production-strategy-source.json` binds those scanner fields to the canonical
rules hash and exact Git object for `2026-07-15-orb-v3` that existed before all
scanner target collection. Inspection verifies the four universe thresholds,
opening-RVOL lookback, and 09:35 start against that source. This is an input-
fidelity rule, not a strategy revision.

## Executed result and claim boundary

The completed run made 15,087 Alpaca SIP requests with zero retries. It retained
2,481,269 derived source rows and performed 608,386 canonical day merges. The
independent inspector then verified all 608,386 represented day documents,
their raw 15-minute and derived daily datasets, and 557,520 opening-minute
datasets against the source indexes.

Across the exact 20 target dates, the point-in-time common-stock denominators
contained 105,261 security-date evaluations. The frozen gates admitted 1,228
eligible rows: 9 to 148 per date, with a median of 59. Nineteen dates supplied a
full top 20; 2026-03-27 supplied only nine, so the immutable result contains 389
selected date-symbol pairs rather than substituting eleven weaker or later-known
names. Thirty-nine evaluations required a target-date split adjustment.

The largest terminal exclusions were 68,458 incomplete target opening bars and
24,239 incomplete prior opening histories. Those are not silently filled data
gaps: the full point-in-time master is the denominator, while a security without
five real opening trade minutes or fourteen real time-matched prior openings is
ineligible under the frozen scanner contract. The result therefore establishes
which names this data source and policy could faithfully rank; it does not imply
that every listed common stock traded continuously.

Public evidence is in
`research_results/2026-07-19-scanner-replay.json` and
`research_results/2026-07-19-scanner-replay-inspection.json`. It contains dates,
counts, hashes, rejection totals, and invariants but no licensed symbol rows.
The private selected identities are now eligible to be frozen into a separate
selected-candidate data-join manifest. They are not yet trades, setups, or alpha
observations.

## Point-in-time security identity

The security master was built from the exact 20 dated Massive
`/v3/reference/tickers` snapshots using `market=stocks`, `locale=us`, `type=CS`,
and `active=true`. It contains 5,668 sourced records for 5,593 instruments and
resolves a listing only on dates where that source actually observed it. It does
not infer continuity between sparse sample dates.

The v4 manifest binds the immutable compressed snapshot with canonical content
hash `8a6912f2ccefe84a9dd8a2e81c78de38cb0b1d713c18b95528f2d0363d9de99a`,
not the mutable current-master path. FIGI-backed rows retain cross-symbol
identity; rows without FIGI are explicitly listing-key fallbacks and are never
silently linked across renames. Licensed identities remain private, while
`security-master-source.json` publishes the source contract, counts, and hash.

## Source and sufficient-statistic contract

v4 uses the configured Alpaca historical stock bars endpoint with explicit
`feed=sip`, `adjustment=raw`, and `asof=-`:

- `1Min` bars from 09:30:00 through 09:34:59.999999 ET provide the five real
  opening minutes.
- `15Min` bars from 09:30:00 through 15:59:59.999999 ET provide the completed
  regular-session OHLCV needed for prior close, ADV(14), and ATR(14).
- Missing symbols or intervals stay missing. `asof=-` prevents current-name
  remapping from hiding a symbol discontinuity.
- Massive split actions remain the source for target-date normalization. Future
  corporate actions are forbidden.

`split-actions-source.json` binds the local 891-event action file to SHA-256
`0be5859007884dc8ec1cbf1e1245293443da7da0fafa03c6b3bb3ce84c30cd2b`.
It also identifies the earlier immutable commit that publicly recorded that same
hash before v4 target-price collection. Inspection verifies both the local file
and the historical Git evidence, closing the post-outcome corporate-action drift
path that a provider name alone would leave open.

`session-calendar-source.json` similarly binds the exact 145-session calendar
to its pre-collection Git object. A read-only Alpaca market-calendar query over
the same range returned the identical 145 dates and identified 2025-12-24 as the
only 13:00 ET close. No target or lookback date changed after this verification.

Alpaca documents that non-minute intraday bars are aggregated from minute bars
by taking first open, maximum high, minimum low, last close, summed volume/count,
and volume-weighted VWAP. Aggregating the returned 15-minute regular-session bars
therefore supplies the exact sufficient statistics consumed by the scanner
without downloading hundreds of millions of one-minute rows. The frozen
scanner never uses intraday detail after 09:35 on the target date.

The uniform source query does retain full-session 15-minute aggregates on target
dates. Neither the build nor the independent recomputation consumes those
post-09:35 target values: target rows contribute only their five opening
minutes. Their local availability nevertheless means this 20-date corpus is a
pipeline and selection-fidelity dataset, not clean independent alpha
confirmation. Any later production claim must be frozen before collection on
previously uninspected dates.

The first post-freeze session gate retained 5,163 symbols, including 1,523 with
all five real opening minutes, in 127 provider requests with zero retries. For
AAPL on 2025-12-10, the Alpaca 15-minute aggregate and the derived replay index
matched exactly on OHLCV. A Massive one-minute SIP cross-check matched high,
low, and close, differed by $0.09 on the opening print, and differed by 206
shares out of 21.3 million. That small vendor trade-condition difference is
retained as source provenance; providers are never blended within a scanner
session.

Official references:

- <https://docs.alpaca.markets/reference/stockbars>
- <https://docs.alpaca.markets/docs/market-data-faq>
- <https://docs.alpaca.markets/docs/about-market-data-api>

## Canonical and derived storage

Every returned provider observation is written through `HistoricalDayStore` to
the private symbol-first location:

```text
LOCAL_HISTORICAL_DATA_ROOT/<symbol>/<year>/<date>.json.gz
```

Each day document keeps the raw Alpaca SIP 15-minute regular-session series, the
raw one-minute opening window, and a one-day derived aggregate with explicit
provider/feed/adjustment/scope/quality/provenance. The five-minute window is
never marked as a complete one-minute session, so ordinary cache reads cannot
mistake it for 390 bars.

For build speed, the collector also creates a private deterministic index under:

```text
LOCAL_HISTORICAL_DATA_ROOT/_derived/scanner_replay/
  dataset-production-scanner-replay-2026-07-19-v4/
```

Each indexed symbol contains its real opening rows plus one synthetic 15:59 row
whose residual volume/count and daily OHLC reconstruct the 15-minute aggregate.
That row is explicitly a derived calculation index, never a provider minute
observation. Every source file has a sidecar attestation and SHA-256. A partial
file never becomes ready.

Raw provider rows and detailed symbol evaluations remain private. Public
evidence retains request contracts, counts, hashes, rejection totals, and hashed
daily top-20 identities.

## Contract lineage and failed gates

- v1 froze Massive market-wide minute flat files. It collected zero minute
  artifacts and was superseded because separate flat-file credentials were not
  configured and local non-S3 collection is preferred.
- v2 tested Alpaca `1Day` bars. The first session failed because provider daily
  trade-condition semantics did not reproduce the 09:30 minute open or
  minute-derived regular-session volume. No derived source file became ready.
  Twelve already-written provider rows were preserved but corrected to a
  non-preferred `provider_trade_date` scope. The external no-loss repair ledger
  is `_migrations/scanner-replay-alpaca-v2-scope-repair/summary.json`.
- v3 proved the 15-minute shape for three sessions but inherited a rules file
  whose source label still named Massive. It was stopped and superseded before
  evaluation; its data did not change any date, symbol, threshold, or rank.
- v4 froze the identical strategy contract against provider-neutral rules and
  recollects its own hash-attested session artifacts. Earlier inspected rows are
  disclosed in the manifest as source-fidelity tests only.

Failed and superseded manifests remain immutable. They are evidence about what
was rejected, not clutter to erase.

## Scanner semantics

For every point-in-time active common stock on XNAS, XNYS, XASE, ARCX, or BATS:

1. Load only raw SIP prior-session bars and target-date 09:30-09:34 bars.
2. Apply splits effective by the target date. Prior prices use
   `split_from / split_to`; volume uses the reciprocal.
3. Require five real target opening minutes, 14 complete prior opening windows,
   and 15 prior daily sessions for true-range construction.
4. Compute opening price, prior-14 ADV, prior-14 ATR, opening RVOL, bullish
   opening candle, and return from prior close.
5. Apply the unchanged production-universe thresholds.
6. Rank eligible names by opening RVOL descending, opening return descending,
   then symbol ascending; retain the top 20 and all rejection counts.

Missing history, a symbol discontinuity, or a malformed source row is an
explicit rejection. It never triggers current-symbol lookup, date substitution,
end-of-day shortlist replacement, catalyst-only filtering, or provider mixing.

The exact 389 selected pairs now feed the separately frozen development join in
`SELECTED_CANDIDATE_JOIN.md`. That join adds selected-session and benchmark
minute bars, time-bounded news discovery, raw crossing trades, and post-cross
quotes without changing scanner v4. It does not expand the scanner's claim:
primary catalysts, clean trade-condition semantics, full depth, point-in-time
tradability/halt state, resistance, sector evidence, and independent outcomes
remain outside this selection dataset.

## Runbook

```sh
python3 session_mode.py --mode learning

python3 scanner_replay_alpaca.py status \
  historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-19-v4-645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json

python3 scanner_replay_alpaca.py collect \
  historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-19-v4-645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json

python3 scanner_replay_alpaca.py build \
  historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-19-v4-645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json

python3 scanner_replay_inspection.py \
  historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-19-v4-645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json
```

The inspection command re-hashes all external sessions, independently parses
the derived source indexes, rebuilds the target-date security-master population,
and recomputes every ADV, ATR, opening-RVOL denominator, point-in-time split
factor, threshold disposition, rejection distribution, rank, and top-20 hash.
It also opens every represented canonical symbol/day document and reconciles its
raw 15-minute series, partial opening-minute series, quality flags, and derived
daily aggregate back to the source index. This intentionally does not trust the
build engine or a bulk sidecar to certify its own output or persistence. Only a
valid report may support a `READY` dataset event with `inspected=true`. That
event now exists. The research lock's fidelity condition is satisfied, but the
next objective remains the frozen selected-candidate join and unchanged-
champion evaluation; READY does not authorize automatic variant invention or a
production rule change.
