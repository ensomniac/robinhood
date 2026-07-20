# Development Non-Return Qualification

Planned dataset:
`dataset-development-non-return-qualification-2026-07-20-v5`

Status: collector manifest `b3645a88...fb8438` independently rebuilt
`COLLECTION_INSPECTED`; 19 of 21 pairs have complete pre-entry inputs, two
have no clean cross before 10:30, and no target outcome has been accessed

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

Second-tranche dataset:
`dataset-development-non-return-qualification-2026-07-20-tranche-v3-v1`

Second-tranche status: the exact 102 combined source-positive pairs across 52
dates rebuild outcome-blind with zero target artifacts; the v3 adapter is ready
to commit before freeze

## Purpose

`development_non_return.py` is the network-free boundary between the inspected
source-semantics result and detailed market acquisition. It independently
rejoins the private pair-disposition hashes to the exact 1,906-pair scanner
selection, retains only the 21 `VERIFIED_POSITIVE_PRIMARY` pairs, rechecks their
coarse scanner gates, and freezes every causal request class before provider
access.

Exact positive symbols, dates, instrument IDs, scanner rows, request rows, and
later observations remain under `LOCAL_HISTORICAL_DATA_ROOT`. The public
manifest retains the complete 100-date source surface plus only counts and
identity hashes for the positive subset. Post-entry prices and returns remain
inaccessible.

## Second-Tranche V3 Boundary

`development_non_return_v3.py` isolates the second tranche without changing
the completed 21-pair v5 evidence or its hash-bound implementation. It verifies
the independently inspected 19-positive primary-document review and 83-positive
accession recovery, requires every recovery decision to replace only a formerly
unresolved pair, and rejoins the exact 102 disjoint positive hashes to all 1,871
scanner-selected rows. A live read-only rebuild produces 102 positive pairs on
52 dates, positive identity hash `af5c68f7...795478`, date hash
`2c28e055...271cf2`, and request graph `d1f13baf...12f00`, with zero target
artifacts.

The adapter reuses the previously validated causal request shape while extending
the frozen calendar and split-action window through 2025-12-31. It freezes 102
opening prefixes, 102 premarket prefixes, 102 prior-history prefixes, 306
conditional fully completed candidate/SPY/QQQ prefixes, 52 official halt dates,
one split query, chronological one-second clean-cross discovery, a condition-
eligible trade prefix, and the exact 0/5/10-second quote window. The current
strategy remains `2026-07-15-orb-v3` at rules hash
`00c3aa83...b4b5867`. Post-decision provider rows, missing-input optimism,
substitution, and target outcomes remain forbidden. Commit and push the adapter,
tests, documentation, and progress finding before freezing its private graph.
The first freeze attempt correctly failed before writing because the shorter
`...-v3` identifier already belongs to a superseded first-tranche contract.
The explicit `...-tranche-v3-v1` identity prevents that cross-tranche alias and
leaves the older private evidence unchanged.

## Frozen Acquisition Boundary

The private graph permits only:

- raw Alpaca SIP candidate one-minute bars only for the complete 09:30-09:35
  opening range;
- candidate premarket bars through 09:30 ET and prior-session history ending
  before the target open for the frozen structure contract;
- one exact Massive split-action query spanning the complete frozen calendar
  range so every raw prior-session high can be put on the target-date basis;
- official Nasdaq halt records for the privately selected dates;
- every chronological one-second raw-trade window from 09:35, stopping at the
  first condition-valid continuous regular-sale cross or the 10:30 cutoff;
- candidate and SPY/QQQ one-minute bars ending before the minute containing the
  final decision snapshot, so only fully completed bars are visible;
- a condition-aware raw-trade prefix ending exactly at the final +10-second
  decision snapshot for session VWAP; and
- one bounded quote window from one second before the clean cross through the
  +10-second decision snapshot, producing the frozen 0/5/10-second snapshots.

The exact compatible local Alpaca cache is checked first. The network provider
remains Alpaca because the validated SIP trade-condition and quote semantics are
provider-specific; IBKR or Massive rows cannot be spliced into the same pair.
Missing inputs fail closed. Full-universe detailed collection, provider
switching, symbol/date substitution, target-return access, and any request after
the final decision snapshot are forbidden.

Broker-specific historical tradability cannot be recreated and remains a
prospective live/shadow gate. The historical contract must still prove every
reconstructable unchanged-v3 gate: clean trigger, condition-aware VWAP, fresh
uncrossed quotes, spread, chase, visible capacity, official halt state, benchmark
alignment, structural stop/noise, resistance room, reward/risk, and evaluator
eligibility.

## Runbook

Commit and push this implementation and its tests before freezing:

```sh
python3 development_non_return.py freeze
```

For the second-tranche combined positive set, use the isolated adapter instead:

```sh
python3 development_non_return_v3.py freeze
python3 development_non_return_v3.py inspect \
  historical_batches/development_tranche_v3/non_return_manifests/<manifest>.json
```

Then independently rebuild the private selection, source lineage, strategy
rules, request graph, privacy boundary, and zero-target state:

```sh
python3 development_non_return.py inspect \
  historical_batches/development_tranche_v2/non_return_manifests/<manifest>.json
```

Commit and push the inspected manifest and aggregate status before implementing
or running provider collection. At least 20 complete unchanged-v3 non-return
survivors are required before a separately frozen outcome contract is legal.
Fewer survivors returns the campaign to disjoint `DEVELOPMENT_ACQUISITION`; it
does not permit a gate change or outcome access.

## Manifest-Bound Collector

`development_non_return_collection.py` is the separately frozen provider
boundary for v5. Its source must be committed and pushed before `freeze`; the
resulting collector manifest must then be inspected, committed, and pushed
before `collect` can contact a provider.

Collector status: manifest `b3645a88...fb8438` independently rebuilt
`COLLECTION_INSPECTED`. All 21 pairs reached one terminal disposition through
13,219 chronological one-second windows: 19 `PREENTRY_INPUTS_COLLECTED` and two
`NO_CLEAN_CROSS_BEFORE_CUTOFF`. Inspection rebuilt every pair hash, cursor,
final-decision boundary, implementation/manifest identity, privacy boundary,
and outcome lock. No provider row after a final decision and no target outcome
was observed or derived.

The exact tranche therefore cannot meet the minimum 20-survivor gate even
before later non-return rejects are evaluated. Its outcomes remain locked. The
campaign stays in `DEVELOPMENT_ACQUISITION` and must freeze another disjoint
100-session tranche; neither missing capacity nor the two no-cross paths permit
a rule change, substitution, or favorable outcome access.

The collector checks the local exact cache first, preserves whole-provider raw
Alpaca SIP fidelity, checkpoints every one-second search window atomically, and
stops before requesting the second after the first clean cross. Candidate and
benchmark bars end before the final-decision minute; the decision tape is
end-exclusive at +10 seconds; the quote window is inclusive only through the
+10-second snapshot. Calendar, full-range splits, and official halt evidence
are separately hash-bound. Pair rows and requests remain private; public status
contains only counts, hashes, terminal classes, and claim boundaries.

```sh
python3 development_non_return_collection.py freeze
python3 development_non_return_collection.py inspect-contract \
  historical_batches/development_tranche_v2/non_return_collection_manifests/<manifest>.json
# Commit and push the inspected collector manifest before either command below.
python3 development_non_return_collection.py collect \
  historical_batches/development_tranche_v2/non_return_collection_manifests/<manifest>.json
python3 development_non_return_collection.py inspect \
  historical_batches/development_tranche_v2/non_return_collection_manifests/<manifest>.json
```

## Frozen V5 Result

Manifest `bd4425a0...f7571e` independently rejoins all 1,906 source pair hashes
to the scanner selection and retains exactly 21 verified-positive pairs on 21
distinct dates. The private positive-selection hash is
`f7f2ba05...11783`, pair-identity hash is `4de0c32b...bc2b09`,
positive-date hash is `304134d8...b5cf24`, and request-graph hash is
`133765c4...77c5f`.

Inspection rebuilt the strategy version, rules hash, implementation hashes,
source selection, coarse scanner gates, request graph, privacy boundary, and
zero-target state. The graph includes 21 opening-range, 21 premarket, 21
prior-history, 21 halt, 63 conditional fully-completed bar prefixes, and one
full-range split-action query. Trigger discovery is raw one-second SIP tape in
chronological order and stops at the first clean cross. It authorizes no
provider access by itself and still forbids outcomes.

## Superseded V4 Result

Manifest `1a3f4abd...97147` independently rejoins all 1,906 source pair hashes
to the scanner selection and retains exactly 21 verified-positive pairs on 21
distinct dates. The private positive-selection hash is
`0a4679fd...bad80`, pair-identity hash is `4de0c32b...bc2b09`,
positive-date hash is `304134d8...b5cf24`, and request-graph hash is
`651a0bd6...e2e5a`.

Inspection rebuilt the strategy version, rules hash, implementation hashes,
source selection, coarse scanner gates, request graph, privacy boundary, and
zero-target state. The graph freezes 21 opening-range bar prefixes, 21
premarket prefixes, 21 prior-history prefixes, 21 official halt dates, and 63
conditional fully-completed candidate/benchmark prefixes. Trigger discovery is
raw one-second SIP tape in chronological order and stops at the first clean
cross. It authorized no provider access by itself and still forbids outcomes.
Inspection then found that its inherited split-action artifact began too late
to adjust the complete 252-session history. V5 adds one exact full-range
Massive split query and requires a complete split basis before resistance can
be resolved.

## Superseded V3 Result

Manifest `9b1fefa...81c5b` independently rejoins all 1,906 source pair hashes to
the scanner selection and retains exactly 21 verified-positive pairs on 21
distinct dates. The private positive-selection hash is
`1b81562b...bcb575`, pair-identity hash is `4de0c32b...bc2b09`,
positive-date hash is `304134d8...b5cf24`, and request-graph hash is
`913ae6f6...2810c`.

Inspection rebuilt the strategy version, rules hash, implementation hashes,
source selection, coarse scanner gates, request graph, privacy boundary, and
zero-target state. The graph freezes 21 candidate bar prefixes, 42 benchmark
prefixes, 21 premarket prefixes, 21 prior-history prefixes, and 21 official
halt dates plus only the causal conditional tape and quote derivations. It
authorized no provider access by itself and still forbids outcomes. A second
pre-provider review found that its full 09:30-10:30 aggregate bar prefixes could
expose later bars and the remainder of an early crossing minute. V4 removes
aggregate bars from trigger discovery, searches every raw second in order, and
requests only fully completed candidate and benchmark bars once the final
decision time is known.

## Superseded V2 Result

Manifest `6b5b24cb...8210c` independently rejoins all 1,906 source pair hashes to
the scanner selection and retains exactly 21 verified-positive pairs on 21
distinct dates. It rechecks the unchanged coarse scanner gates, strategy
version, rules hash, implementation hashes, private/public boundary, and zero
target artifacts.

The private positive-selection hash is `7e42d22d...75a0c0`, pair-identity hash
is `4de0c32b...bc2b09`, positive-date hash is `304134d8...b5cf24`, and request
graph hash is `4b4dd86d...db816b`. The graph freezes 21 candidate bar prefixes,
42 benchmark prefixes, 21 premarket prefixes, 21 prior-history prefixes, and 21
official halt dates plus the conditional trade and quote derivations. It
authorized no provider access by itself and still forbids outcomes. A
pre-provider adversarial review found that its single crossing-minute tape
request could expose trades later than the final decision snapshot and its
quote request extended one second beyond +10. No provider request had occurred,
so v2 is retired without contaminated data. V3 changes only the causal request
boundary: it searches one-second tape windows chronologically, stops at the
first clean cross, and ends both VWAP and quote evidence at +10 seconds.
