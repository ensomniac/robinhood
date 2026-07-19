# Dynamic 09:35 Scanner Replay

Status: frozen and collecting; market-wide minute files are not yet present.

The external canonical day store introduced on 2026-07-18 preserves existing
and future per-symbol observations without Git or S3. It does not close this
dataset's distinct market-wide capture gap: issuing REST requests for every
active listing across 118 sessions would be a different, expensive acquisition
plan and must not be mistaken for the frozen flat-file contract. No new catalyst
variant is authorized while this scanner dataset remains incomplete.

The frozen contract is registered `COLLECTING` in `learning/DATASETS.jsonl`, so
the persistent nightly cadence cannot lose the unfinished work. That registry
state is not evidence suitability and does not satisfy the research lock.

This workflow closes the survivorship and shortlist-selection gap in the
catalyst-derived historical corpus. It reconstructs the scanner universe that
could have existed at 09:35 ET on each frozen date. It does not establish a
catalyst, tradability, spread, depth, breakout, order, or outcome decision.

## Frozen pilot

The exact 20 dates were selected by the existing historical selector with seed
`20260718`. The original randomized order is retained in
`historical_batches/scanner_replay/selection-2026-07-18-20-days.json`; dates
may not be added, removed, or substituted. The hash-addressed pre-price contract
is:

```text
historical_batches/scanner_replay/manifests/
  dataset-production-scanner-replay-2026-07-18-v1-
  01f6fd204c02b29cb4aa9c24542273332bddcdc9140ac5eeb159a2635a3d53f0.json
```

The contract requires 118 distinct market sessions: each target plus the union
of its 15-session lookback. It recorded zero target minute artifacts at freeze.

## Security identity

`learning/SECURITY_MASTER.jsonl` was derived from 20 dated Massive
`/v3/reference/tickers` snapshots using `market=stocks`, `locale=us`,
`type=CS`, and `active=true`. The local populated master contains 5,668 records for
5,593 instruments and resolves only on dates actually observed by the source.
It therefore cannot invent continuity between random sample dates.

The dataset contract binds the immutable compressed snapshot under
`learning/security_masters/`, not the mutable current-master path. A future
master extension cannot change this dataset's identity hash. FIGI-backed rows
retain cross-symbol identity. Source rows without FIGI are marked as
listing-key fallbacks and are never silently linked across renames.

Both master files are Git-ignored because Massive's market-data terms prohibit
public redistribution. The public repository retains their content hashes,
source contract, coverage counts, and fallback counts instead of licensed rows.

## Scanner semantics

For every point-in-time active common stock on XNAS, XNYS, XASE, ARCX, or BATS:

1. Load only SIP bars from prior sessions and 09:30 through 09:34:59 ET on the
   target date.
2. Apply splits and share changes effective by the target date. Prior prices
   are multiplied by `split_from / split_to`; volumes use the reciprocal.
   Future corporate actions are forbidden.
3. Require five real opening minutes, 14 complete prior opening windows, and 15
   prior daily sessions for true-range construction.
4. Compute open price, prior-14 ADV, prior-14 ATR, first-five-minute opening
   RVOL, bullish opening candle, and return from the prior close.
5. Apply the frozen production-universe thresholds from
   `strategy_config.toml`.
6. Rank every eligible name by opening RVOL descending, opening return
   descending, then symbol ascending; retain the top 20 and complete rejection
   counts.

Missing history, a symbol discontinuity, or a malformed source row is an
explicit rejection. It never triggers a current-symbol lookup, another-date
substitution, end-of-day shortlist, catalyst-only shortlist, or provider switch.

## Private raw data and public evidence

Provider files and detailed per-symbol evaluations live under ignored
`learning_runs/scanner_replay/`. Public evidence retains source URLs, request
contracts, counts, hashes, hashed daily top-20 identities, and rejection totals.
This avoids publishing licensed raw SIP data while retaining an auditable chain
to every local artifact.

Massive documents the dated ticker reference at
<https://massive.com/docs/rest/stocks/tickers/all-tickers>, unadjusted market-wide
minute files at <https://massive.com/docs/flat-files/stocks/overview>, S3 setup at
<https://massive.com/docs/flat-files/quickstart>, and split adjustment at
<https://massive.com/docs/rest/stocks/corporate-actions/splits>.

## Resume runbook

The current account's REST key is configured, but Massive's separate
Dashboard-issued S3 access and secret keys are not. Put the existing values in
the ignored mode-0600 `.env` without pasting them into commands, chat, logs, or
Git:

```dotenv
MASSIVE_S3_ACCESS_KEY_ID=
MASSIVE_S3_SECRET_ACCESS_KEY=
```

Then run:

```sh
python3 session_mode.py --mode historical

python3 scanner_replay.py status \
  historical_batches/scanner_replay/selection-2026-07-18-20-days.json \
  historical_batches/scanner_replay/session-calendar-2025-12-through-2026-06.json

python3 scanner_replay.py collect-minutes \
  historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-18-v1-01f6fd204c02b29cb4aa9c24542273332bddcdc9140ac5eeb159a2635a3d53f0.json

python3 scanner_replay.py build \
  historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-18-v1-01f6fd204c02b29cb4aa9c24542273332bddcdc9140ac5eeb159a2635a3d53f0.json \
  historical_batches/scanner_replay/session-calendar-2025-12-through-2026-06.json
```

The downloader is resumable and hashes each complete gzip file. Partial files
never become ready. After build, inspect every daily denominator, rejection
distribution, split-adjusted candidate, and top-20 list. Only then may a
`READY` production-scanner dataset event be appended to
`learning/DATASETS.jsonl`. Until that happens, no further strategy variant on
the inspected catalyst corpus is evidence-qualified. The active
`learning/RESEARCH_LOCK.json` makes that boundary executable: hypothesis
freezing rejects the catalyst-falsification lane, and the weekly cadence records
a no-op until this exact dataset ID is `READY`.
