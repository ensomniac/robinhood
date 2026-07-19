# Selected-Candidate Replay Join

Updated: 2026-07-19 ET

Dataset: `dataset-selected-candidate-join-2026-07-19-v1`

Manifest SHA-256: `a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4`

Status: inspected `READY` for pipeline development only. It is not alpha,
confirmation, promotion, or trade-eligibility evidence.

## Purpose And Frozen Boundary

This dataset closes the next layer after the dynamic 09:35 scanner replay. It
uses exactly the scanner's 389 selected security-date pairs across the same 20
dates. It does not rerank, replace, pad, or invent symbols. The exact pair list,
raw provider rows, and symbol-level derived records remain outside Git under
`LOCAL_HISTORICAL_DATA_ROOT`; the public manifest contains their content hashes,
counts, dates, and per-date shortlist hashes.

The source dates were already inspected while scanner v4 was built. This join
therefore validates data plumbing and measures gate attrition, but cannot answer
whether the strategy has alpha. A meaningful return claim still requires a new
preregistered sample of previously uninspected scanner dates.

## Collected Data

The capacity pilot measured one selected session and SPY/QQQ, projected about
16.3 MB for the bar/news layer, observed about 90.2 GB free, and enforced a
10 GiB reserve before bulk collection.

The completed external store contains:

- 389/389 selected candidate regular-session raw Alpaca SIP one-minute bar
  datasets, totaling 151,158 populated trade minutes;
- 40/40 SPY/QQQ date datasets, totaling 15,600 one-minute bars;
- 389/389 bounded catalyst-discovery contexts from the Alpaca historical news
  endpoint, queried from four calendar days before each target through 09:35 ET;
- 325 exact crossing-minute tape joins for the 325 candidates whose one-minute
  high first exceeded the opening-range high before 10:30 ET;
- 365,379 raw SIP trades and 97,949 top-of-book quotes retained only in those
  bounded crossing windows rather than wastefully mirroring full-session ticks.

Alpaca documents historical SIP bars, trades, and quotes on its stock market-
data endpoints, including pagination and `asof` symbol mapping behavior. This
collector sets `asof=-` because the sourced local security master—not current
provider entity mapping—is authoritative. Alpaca's historical news goes back to
2015 and is supplied by Benzinga, so it is discovery evidence only. See the
[historical bars](https://docs.alpaca.markets/us/v1.4.2/reference/stockbars),
[historical trades](https://docs.alpaca.markets/us/reference/stocktrades-1),
[historical quotes](https://docs.alpaca.markets/us/reference/stockquotes-1), and
[historical news](https://docs.alpaca.markets/us/docs/historical-news-data)
documentation.

## Inspected Findings

Of 389 scanner-selected pairs:

- 325 crossed the opening-range high before the 10:30 cutoff; 64 did not;
- 303 crossing windows supplied all three post-cross quote snapshots;
- 292 supplied three snapshots that were positive, uncrossed, and at most five
  seconds old at their observation targets;
- only 177 still had a final ask within 0.15% of the opening-range high after
  the required observation interval;
- 260 had at least one time-valid secondary news candidate and 129 had none;
- the 933 usable snapshot observations had a median spread of about 0.1318%,
  above the production operating limit of 0.10%.

These are gate-availability measurements, not returns. They demonstrate why a
bar whose high crosses the opening range cannot be counted as an executable ORB:
quotes may be absent, stale, crossed, wide, or already beyond the chase cap by
the time the required observation is complete.

## Rules Hardened By This Dataset

The numeric production strategy remains `2026-07-15-orb-v3`. The following
evidence rules now govern replay construction:

1. A one-minute high may locate a possible crossing window only. It is not an
   exact trigger or a clean break.
2. Exact replay must retain raw SIP trades with provider timestamp, trade ID,
   exchange, tape, size, and condition metadata. The first observed price above
   the range remains `UNRESOLVED_CONDITION_SEMANTICS` until the SIP condition
   rules used by the production clean-break definition are frozen and tested.
3. Quote snapshots must be formed after the observed cross at zero, five, and
   ten seconds using only the latest quote available at each target. A missing,
   nonpositive, crossed, or more-than-five-second-old snapshot is a blocker.
4. Chase is evaluated from the final post-observation ask. A lower bar close or
   an earlier intraminute print cannot override a failed 0.15% chase cap.
5. Benzinga-derived news never sets `verified_catalyst=true`. A time-valid
   issuer, SEC, exchange, or otherwise permitted primary/directly attributed
   source is still required under `AGENTS.md`.
6. Historical top-of-book sizes are not a full-depth ladder. They can falsify
   spread/freshness availability but cannot silently prove the production depth
   gate.
7. Point-in-time tradability, halt state, resistance, and sector-relative
   strength remain explicit missing fields. Current facts may not backfill them.
8. No outcome or strategy variant may be evaluated from this corpus until the
   unchanged champion's required fields are either sourced or rejected as
   unavailable. Missing evidence produces no trade, never a favorable default.

## Runbook

The workflow is deterministic and cache-resumable:

```sh
python3 selected_candidate_join.py freeze

python3 selected_candidate_join.py pilot \
  historical_batches/selected_candidate_join/manifests/dataset-selected-candidate-join-2026-07-19-v1-a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4.json

python3 selected_candidate_join.py collect \
  historical_batches/selected_candidate_join/manifests/dataset-selected-candidate-join-2026-07-19-v1-a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4.json

python3 selected_candidate_join.py derive \
  historical_batches/selected_candidate_join/manifests/dataset-selected-candidate-join-2026-07-19-v1-a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4.json

python3 selected_candidate_join.py collect-tape \
  historical_batches/selected_candidate_join/manifests/dataset-selected-candidate-join-2026-07-19-v1-a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4.json

python3 selected_candidate_join.py inspect \
  historical_batches/selected_candidate_join/manifests/dataset-selected-candidate-join-2026-07-19-v1-a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4.json
```

Public status is in
`historical_batches/selected_candidate_join/collection-status.json`; the
aggregate inspection is in
`research_results/2026-07-19-selected-candidate-join.json`. Repeated collection
reads exact compatible canonical shards before calling Alpaca. A null provider
observation collection is retained as an honest empty result, not treated as
malformed data or filled synthetically.

## Next Evidence Work

Close the primary-catalyst and clean-trade-condition contracts before computing
champion returns. Then freeze at least 100 new scanner dates, collect the same
layers without inspecting target outcomes before registration, and evaluate the
unchanged champion first. Only that independent sample can earn one
preregistered production-rule proposal.
