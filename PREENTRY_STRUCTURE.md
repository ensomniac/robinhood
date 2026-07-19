# Point-In-Time Pre-Entry Structure

Dataset: `dataset-preentry-structure-fidelity-2026-07-19-v2`

Status: independently inspected `READY` for `DEVELOPMENT_ONLY`

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Question Answered

Can the existing real-time stop/noise and resistance requirements be made
deterministic, point-in-time, split-aware, and fail-closed on the 325 frozen
clean-trigger windows before any target-session outcome is exposed?

Yes. The dataset supplies one reproducible interpretation of those already
required inputs. It does not establish that the interpretation has alpha, does
not optimize a threshold, and does not change the production strategy.

## Why These Definitions

Support and resistance do not have a universal mathematical definition. The
source literature treats recent extrema as observable approximations and uses
average absolute price increments as a way to give a level a noise width. The
52-week-high literature also supports treating an established long-horizon high
as a behaviorally salient reference point. The opening-range-breakout source
motivates the opening range itself as the immediate structural level.

The frozen contract therefore uses only information available at the final
three-snapshot observation:

- opening support is the exact high of the five completed 09:30-09:35 ET bars;
- ordinary noise is the greater of median observed spread dollars and the mean
  absolute adjacent one-minute close increment over as many as 60 completed
  target-session bars;
- structural invalidation is the opening high minus that observed noise;
- stop distance remains the greater of the existing `0.10 * ATR(14)` floor and
  the distance from entry to structural invalidation;
- the existing 0.8% maximum stop fraction remains unchanged;
- overhead levels are the complete target-date 04:00-09:30 ET premarket high,
  prior-session high, prior-14-session high, and prior-252-session high;
- every daily high is reconstructed from raw SIP 15-minute regular-session bars
  and normalized to the target-date share basis using only splits effective no
  later than the target date;
- the existing 2.2% minimum room remains unchanged.

Sources bound by the frozen manifest:

- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4729284
- https://arxiv.org/abs/2101.07410
- https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1572269

These sources motivate a deterministic measurement contract; they do not prove
that the chosen implementation is profitable.

## Missingness And Lookahead Rules

The contract requires a contiguous completed regular-minute prefix beginning at
09:30 ET. A bar whose closing boundary is after the final quote observation is
future information and is rejected. Exactly three point-in-time quote snapshots
are required to define the observation, final ask, and median spread.

Alpaca requests use raw SIP data with `asof=-`. A successfully exhausted request
is terminal even when a symbol has no observations before an IPO or across a
symbol discontinuity. Collection completeness and 252-session coverage are
separate facts:

- all 249 identity requests and all 325 premarket windows completed;
- 234 identities have the full 252 same-symbol sessions;
- 15 have explicit history gaps;
- missing daily history cannot establish price discovery;
- a split-adjusted high that is present in partial history may supply adverse
  overhead evidence, but can never supply favorable clear-sky evidence;
- a complete zero-row premarket response is an observed absence, not a cache
  failure;
- broker-specific historical tradability remains unreconstructable and is not
  inferred.

The provider observations live under `LOCAL_HISTORICAL_DATA_ROOT`. Public
artifacts contain aggregate counts and hashes only.

## Result

Of 325 clean-trigger records:

- 255 have enough point-in-time quote and trigger geometry to derive structure;
- 16 are unresolved because they lack exactly three snapshots;
- 54 are unresolved because the final ask is already below the opening high;
- 153 of 255 derivable stops fit the unchanged 0.8% maximum;
- median derivable stop distance is 0.6946% of entry;
- 89 are blocked by known overhead inside the unchanged 2.2% room;
- 131 resolve with overhead beyond 2.2%;
- 17 resolve as 252-session price discovery;
- 18 retain unresolved resistance inputs;
- 89 pass both the stop and resistance geometry.

The earlier opening-range-low proxy admitted only 11 stops. The new contract
shows that the apparent stop-cap bottleneck was mostly an input-definition
problem: a breakout-failure level just below the opening high is materially
closer than the opening low while still sitting outside the measured noise
zone. This is deployability evidence, not outcome evidence. The inspected dates
still have zero resolved catalyst-led production candidates, so changing the
0.8% cap, the 2.2% room, or another alpha rule would be post-hoc and remains
forbidden.

## Independent Inspection

`preentry_structure_inspection.py` does not import the structure implementation.
It independently reloads canonical bars and bounded premarket windows,
reconstructs the exact regular-minute prefixes, reparses split events,
recalculates every stop and resistance field, reproduces all 325 terminal
records and aggregate counts, checks the private result hash, and rejects public
symbol/row leakage.

The v1 manifest is retained as a failed contract. Its collection completed, but
the builder attempted to index a final quote before checking for exactly three
snapshots. V2 changes only that fail-closed control. It does not change dates,
identities, thresholds, structure semantics, or outcome access.

## Reproduction

```sh
python3 preentry_structure_dataset.py collect \
  --manifest historical_batches/preentry_structure/manifests/dataset-preentry-structure-fidelity-2026-07-19-v2-73390adee260fc80fe0faa514eac5ba474d94b1eb28ab443d6557331be36cdb3.json

python3 preentry_structure_dataset.py build \
  --manifest historical_batches/preentry_structure/manifests/dataset-preentry-structure-fidelity-2026-07-19-v2-73390adee260fc80fe0faa514eac5ba474d94b1eb28ab443d6557331be36cdb3.json

python3 preentry_structure_dataset.py inspect \
  --manifest historical_batches/preentry_structure/manifests/dataset-preentry-structure-fidelity-2026-07-19-v2-73390adee260fc80fe0faa514eac5ba474d94b1eb28ab443d6557331be36cdb3.json

python3 preentry_structure_inspection.py \
  --manifest historical_batches/preentry_structure/manifests/dataset-preentry-structure-fidelity-2026-07-19-v2-73390adee260fc80fe0faa514eac5ba474d94b1eb28ab443d6557331be36cdb3.json
```

Public evidence is in
`historical_batches/preentry_structure/acquisition-status.json`,
`research_results/2026-07-19-preentry-structure.json` and
`research_results/2026-07-19-preentry-structure-inspection.json`.

## Decision

Keep v3 frozen. Use this exact structure contract prospectively in the next
previously uninspected 100-plus-date dynamic scanner corpus. Evaluate unchanged
v3 first. Only an independently inspected new sample with adequate catalyst,
execution, and outcome coverage may earn one preregistered revision.
