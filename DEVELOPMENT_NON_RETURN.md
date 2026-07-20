# Development Non-Return Qualification

Planned dataset:
`dataset-development-non-return-qualification-2026-07-20-v3`

Status: v3 implementation ready; superseded v2 manifest `6b5b24cb...8210c`
made zero provider requests and is retained as a failed lookahead contract

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

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

## Frozen Acquisition Boundary

The private graph permits only:

- raw Alpaca SIP candidate and SPY/QQQ one-minute bars from 09:30 through the
  10:30 ET entry cutoff, end-exclusive;
- candidate premarket bars through 09:30 ET and prior-session history ending
  before the target open for the frozen structure contract;
- official Nasdaq halt records for the privately selected dates;
- chronological one-second raw-trade windows only inside aggregate crossing
  minutes, stopping at the first condition-valid continuous regular-sale cross
  or the 10:30 cutoff;
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
