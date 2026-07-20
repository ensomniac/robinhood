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

Failure retires this exact challenger without repair on the sample. Passing
development only earns an untouched confirmation contract; it does not change
production rules or maturity.

## Next Acquisition

Freeze another exact disjoint 100-session selection before provider or outcome
access. Reuse the point-in-time security-master, primary-source catalyst, SIP
tape/quote, split, halt, and capacity controls already proven by the v3 pipeline,
but bind the new retest evaluator and exclude every inspected target date.
