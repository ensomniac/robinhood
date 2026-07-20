# Catalyst ORB Retest Challenger

Status: `PREREGISTERED` research-only active challenger

Contract: `b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105`

Primary trial: `trial-8a93c0ab15c85248`

Champion: `2026-07-15-orb-v3`, preserved unchanged and `UNVALIDATED`

## Mechanism

The challenger does not buy the first opening-range breakout. It observes a
condition-valid first break, then requires the first fully completed one-minute
retest bar to touch the opening-range high and close at or above it. Entry is
the first condition-valid rebreak of that bar high before 10:30 ET. The causal
hypothesis is that a completed hold and rebreak measures accepted price
discovery rather than the transient impulse of the first break.

This is one frozen trial, not a parameter search. The exact rule, primary
parameters, execution assumptions, cost stresses, falsification gates,
contamination disclosures, and compatibility risks live in:

`learning/hypotheses/experiment-catalyst-orb-retest-v1-b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105.json`

## Constraints That Do Not Change

- Long common-stock equities only, one position and one filled entry per day.
- Regular hours only, no entry after 10:30 ET, force flat by 15:50 ET.
- Source-verified positive catalyst, financing and dilution conflicts rejected
  first, bullish opening range, exact opening relative volume of at least 1.0.
- Three fresh quote/book snapshots, unchanged A+ spread, depth, liquidity,
  halt, benchmark, and tradability requirements.
- Structural stop below retest invalidation and ordinary noise, at least 10% of
  prior-completed-session ATR(14), never compressed beyond the 0.8% cap.
- At least 2.5R and 2.2% room before resistance, +2% milestone, unchanged
  runner contract, whole-share risk sizing, protection, circuit breakers, and
  no overnight exposure.

## Evidence And Falsification

The challenger may not use any previously inspected target date. The v3
qualification corpus supplied only pre-outcome deployability evidence; its
post-entry data remains inaccessible. Earlier scanner, multi-strategy,
production-aware, and reversal dates and outcomes are disclosed ancestors and
must also be excluded.

Development requires at least 50 eligible closed signals, positive geometric
growth and bootstrap lower mean R, profit factor at least 1.30, drawdown at most
6R, positive chronological halves, positive performance without the five best
trades, and positive 10- and 20-bps stress results with profit factor at least
1.20 and drawdown at most 6R. Rule, capture, or evidence violations must be zero.
Every no-trade, missed fill, unavailable row, and reject stays in denominators.

`challenger_orb_retest.py` is the outcome-blind trigger implementation. It
requires complete causal trade and completed-bar windows, excludes the initial
break minute from retest eligibility, treats the first later touch as the only
hold test, requires condition-valid continuous-sale prints for both breaks, and
requires the +10-second decision snapshot to finish by 10:30. Interpolation or
an incomplete window fails closed.

Failure retires this exact challenger without repair on the sample. Passing
development only earns an untouched confirmation contract; it does not change
production rules or maturity.

## Next Acquisition

Freeze another exact disjoint 100-session selection before provider or outcome
access. Reuse the point-in-time security-master, primary-source catalyst, SIP
tape/quote, split, halt, and capacity controls already proven by the v3 pipeline,
but bind the new retest evaluator and exclude every inspected target date.

The current 2025-2026 calendar has only two eligible disjoint sessions after
the v3 selection. `challenger_orb_retest_calendar.py` therefore freezes an exact
Alpaca calendar request for 2023-01-01 through the last completed session on
2026-07-17 before provider access. Its separate inspector independently rebuilds
the provider query, session ordering, uniqueness, hours, hashes, and outcome
lock. Calendar output is public reference data, not target market or outcome
data. The implementation and zero-output manifest must each be committed and
pushed before collection.

```sh
python3 challenger_orb_retest_calendar.py freeze
python3 challenger_orb_retest_calendar_inspection.py inspect-contract \
  historical_batches/challenger_orb_retest_v1/calendar_manifests/<manifest>.json
# Commit and push the inspected zero-output manifest before the commands below.
python3 challenger_orb_retest_calendar.py collect \
  historical_batches/challenger_orb_retest_v1/calendar_manifests/<manifest>.json
python3 challenger_orb_retest_calendar_inspection.py inspect \
  historical_batches/challenger_orb_retest_v1/calendar_manifests/<manifest>.json
```

Calendar manifest `d0991e14...710c60` is now independently `FROZEN_READY`.
It binds both published implementations, the exact Alpaca endpoint and
2023-01-01/2026-07-17 query bounds, validation rules, output paths, and zero
pre-freeze output artifacts. It must be committed and pushed before the single
read-only calendar request.

The request is now independently `COLLECTION_INSPECTED`: 887 sessions from
2023-01-03 through 2026-07-17, calendar hash
`0ef45909...938ebb`. Every row, query field, hour, ordering constraint, count,
and hash rebuilds. No target session has yet been selected and no target market
or outcome data was accessed.

`challenger_orb_retest_tranche.py` and its separate inspector now adapt the
proven disjoint selector without modifying its frozen base implementation. They
use seed `2026072005`, exclude all four earlier selections plus every signal,
archived context, and currently inspected evidence date, and bind the extended
calendar, v3 falsification, hypothesis, trigger, capacity projection, and
private exclusion snapshot. Preflight finds 445 eligible dates; the exact
100-date selection needs 474 sessions including lookbacks and projects about
1.88 GB of private storage. This is capacity only: the implementation must be
committed and pushed before `freeze`, and the resulting selection and manifest
must be independently inspected and published before any dated reference or
market request.

```sh
python3 challenger_orb_retest_tranche.py freeze
python3 challenger_orb_retest_tranche_inspection.py \
  historical_batches/challenger_orb_retest_v1/selection_manifests/<manifest>.json
```

Selection manifest `c1c51924...ec7dae` is now independently `FROZEN_READY`.
It binds seed `2026072005`, 427 exclusions, a 445-date eligible pool, exactly
100 disjoint selected sessions, and 474 required sessions including lookbacks.
The selected-date hash is `8712bb4d...84d187`; the required-session hash is
`31afb1ff...bc54a`. Substitution and outcomes remain forbidden. This exact
selection and manifest must be committed and pushed before dated reference
identity access.
