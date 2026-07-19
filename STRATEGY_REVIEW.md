# 2026 Agentic Intraday Strategy Review

Reviewed: 2026-07-19 (ET)

Active strategy: `2026-07-15-orb-v3`

This document records the evidence and engineering rationale behind the active
rules in `AGENTS.md`. It is not a promise of profit. The goal is rapid compounded
growth, but growth is only durable when the strategy preserves capital, measures
its edge after costs, and refuses trades that do not match the tested setup.

## Executive Determination

Keep these parts of the original strategy:

- Long equities only, regular hours only, and flat before the close.
- One position at a time.
- Catalyst-backed, liquid stocks with fresh quotes and tight spreads.
- A broker-held protective stop and active monitoring.
- A public ledger that includes rejected setups and no-trade days.

Change these parts:

1. Make the five-minute opening range breakout (ORB) on a verified Stock in Play
   the only production setup. Treat VWAP pullbacks and high-of-day continuations
   as research-only until this repository has separate positive evidence for them.
2. Size first by account loss risk, then cap by buying power and executable
   liquidity. Notional allocation is an output of the calculation, not the risk
   control.
3. Replace the hard fixed take-profit with a +2% milestone and a structural
   trailing exit. The trade must still have a credible path to +2%, but the agent
   must not hold a failed trend merely to seek that number or automatically cap a
   strong trend at exactly +2%.
4. Use the paper's time-matched opening relative volume definition, a volatility
   stop anchored to 10% of daily ATR, and an explicit slippage reserve.
5. Add executable stale-data checks, order timeouts, duplicate-order controls,
   drawdown circuit breakers, deterministic sizing, an append-only signal ledger,
   and an evidence-based strategy maturity state.
6. Obey the current MCP tool contract. Repository authorization cannot replace
   an explicit confirmation that a broker tool or platform requires.

### 2026-07-16 frozen-rule checkpoint

The first ten archived historical sessions produced 100 candidate signals: 60
had a recorded opening-range crossing, but every candidate was rejected and no
performance-bearing signal closed. Sixty-one candidates failed at least one fact
known before the replay session began. Of those, 60 failed a daily metric: 51
were below the one-million-share 14-session average-volume gate and 16 were below
the $0.50 daily-ATR gate, with seven failing both. Those candidates should never
have consumed target-session bars and quote requests. Historical preflight now
applies the already-frozen universe gates before target-session collection; this
is a throughput and sample-
construction fix, not a relaxed strategy rule.

Ninety candidates also carried a chase rejection. Review showed that schema-1
bundle construction took quote snapshots at the start of the one-minute bar
whose later high established the breakout. The data therefore used information
not yet available at the quote timestamp. Schema 2 evaluates at the next minute
boundary after a full post-break quote-observation minute and records the basis
explicitly; legacy schema-1 artifacts remain readable and unchanged.

This sample contains zero eligible or closed performance signals, so it cannot
support a threshold, setup, sizing, or exit change. The learning cadence is also
blocked at zero new closed signals and one elapsed day. Determination: keep
`2026-07-15-orb-v3` frozen and `UNVALIDATED`, improve evidence fidelity and
collection throughput, then collect a new unbiased sample. The paper's ADV and
ATR filters remain the current primary-source rationale, and the larger
five-minute request is within IBKR's current duration/bar-size table. [University
of St. Gallen ORB paper](https://www.alexandria.unisg.ch/server/api/core/bitstreams/3c2989c4-688d-4d78-8a71-f02690990d51/content),
[IBKR TWS API historical-data documentation](https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/)

### 2026-07-19 evidence and architecture checkpoint

The accumulated public ledger now contains 106 closed no-trade sessions and
1,060 rejected candidate signals. Six hundred six candidates recorded an ORB
crossing, but zero were eligible and zero produced a closed performance label.
The current maturity report therefore remains `UNVALIDATED`: expectancy,
profit factor, win rate, payoff ratio, bootstrap confidence, entry slippage,
stop slippage, and protection latency are all still unmeasured for the active
strategy. A large count of overlapping rejections is not evidence that any one
threshold is wrong. In particular, 975 chase rejects, 798 reward/risk rejects,
773 resistance-room rejects, 753 median-spread rejects, and 612 stop-noise
rejects came from the earlier catalyst-derived replay surface, which did not
reconstruct the production 09:35 cross-section.

The strongest mined challenger also failed its independent test. The frozen
early Item 2.02 reversal produced 22 primary trades over 31 usable dates and
lost 10.056R: -0.457R mean expectancy, 0.370 profit factor, and 10.492R maximum
drawdown. Every target and cost-stress cell was negative. The correct response
is deletion and focus, not another parameter search: the reversal is `RETIRED`,
its unused prospective shadow runner has been removed, and its immutable
manifest and result remain as falsification evidence.

The source-attested dynamic 09:35 scanner replay described in
`SCANNER_REPLAY.md` is now inspected `READY`. It froze the point-in-time security
master, exact dates, scanner rules, calendar, split actions, pre-collection
production configuration, provider request contract, and independent rebuild
before any target-session strategy outcome was joined. All 118 source sessions
passed; the 20 dates contained 105,261 point-in-time security evaluations, 1,228
eligible rows, and 389 exact selected pairs. Nineteen dates supplied a top 20;
one supplied only nine and was not padded. The inspector reconciled 608,386
canonical documents to the source and independently reproduced every metric,
disposition, rank, and shortlist hash.

This is a strategy rule
about evidence eligibility, not an alpha threshold: no result from the old
catalyst-derived universe justified a new mechanism or production rule before
this fidelity gate closed. Its completion releases the mechanical research lock,
but the first 20 dates validate selection machinery only; they cannot promote or
revise the active strategy because catalyst, quote, depth, breakout, execution,
and outcome fields remain absent.

Determination: preserve every numeric v3 production rule and the
`2026-07-15-orb-v3` sample identity. Collect detailed data only for the frozen
selected pairs, evaluate the unchanged champion first,
and earn at most one preregistered revision from a subsequently frozen sample.
This sequence maximizes the chance that additional complexity improves net
geometric growth instead of fitting a biased shortlist.

The separately frozen selected-candidate join then measured the next fidelity
layer without inventing a variant. All 389 pairs and 40 benchmark sessions have
one-minute SIP bars; 325 had a pre-cutoff crossing window with raw trade/quote
tape. Only 303 supplied three post-cross snapshots, 292 passed basic
fresh/uncrossed checks, and 177 remained within the 0.15% chase cap. Median
usable spread was 0.1318%, above the 0.10% operating gate. These measurements
show that bar-only ORB counts materially overstate executable opportunities.
They do not estimate returns because primary catalyst, clean-condition, depth,
tradability/halt, resistance, and sector evidence remain incomplete and the
source dates were already inspected. The production rules therefore remain
unchanged; `SELECTED_CANDIDATE_JOIN.md` defines the hardened replay boundary.

The next frozen fidelity layer mapped every selected pair to a dated issuer CIK,
retained 100 SEC primary documents, and independently reconstructed exact SIP
condition decisions. Only 128 of 325 first raw price crosses were clean
continuous regular-sale prints; 195 valid trigger times moved later, and only
155 remained inside the chase cap. SEC coverage produced 81 filing-covered
pairs and eight dilution conflicts, but no filing was allowed to become a
positive catalyst without directional classification. Median usable spread
remained 0.13001%. This closes CIK and clean-condition mechanics while making
the remaining catalyst and execution blockers more precise; it still earns no
production-rule change. `SELECTED_CANDIDATE_FIDELITY.md` is authoritative for
this layer.

The next source-fidelity layer enforced exact prior-close recency against SEC
complete submissions and issuer exhibits. Of 389 pairs, only 12 had a verified
material recent primary catalyst; 11 were directionally unresolved, one had a
verified positive issuer phrase, and six were conflict rejects. The same frozen
layer reparsed 966 official Nasdaq historical halt rows and found no overlap in
the 325 clean-trigger observation windows. An independent implementation
reproduced every source hash, classification, halt interval, and canonical
context. This closes historical halt state, not broker-specific tradability.
`CHAMPION_INPUT_FIDELITY.md` is authoritative for this layer.

This source layer argued for further fidelity, not looser entry rules. The
unchanged champion was subsequently assembled without target outcomes for
resistance room, structural invalidation/noise, benchmark-relative strength,
and conservative visible liquidity. A missing value stays blocking. The 20
development dates cannot earn a numeric threshold or production change.

The outcome-blind unchanged-champion readiness join then tested whether the
resolved inputs could support an evaluation at all. They cannot. Only one of
389 pairs has a verified positive primary catalyst, and that pair fails both
the production spread and chase gates. Across the whole corpus, 53 pairs pass
non-catalyst execution geometry, 44 also pass completed-bar market diagnostics,
only 11 opening-low/0.10-ATR stop proxies fit inside 0.8%, and zero pass every
measured non-catalyst proxy. The faithful engine chase interval also reduces
the earlier 155 upper-cap-only count to 101 because a final ask below the
opening high is no longer counted as an entry-ready breakout.

This is not evidence that the stop or resistance thresholds should be loosened.
The diagnostic invalidation and resistance levels are conservative proxies, and
the dates are already contaminated for strategy inference. It is evidence that
deployability must be measured before alpha: define those inputs reproducibly,
freeze new scanner dates with direct catalysts, and evaluate v3 unchanged before
testing one preregistered revision. `CHAMPION_INPUT_READINESS.md` is authoritative.

The frozen follow-on in `PREENTRY_STRUCTURE.md` now supplies those exact
stop/noise and resistance definitions. It independently reconstructed all 325
terminal records: 153 of 255 derivable stops fit the unchanged 0.8% cap and 89
pass both unchanged geometry gates, while 70 records remain unresolved. This
corrects the deployability proxy but does not read returns, validate alpha, or
earn a production change. The next evidence must come from at least 100
previously uninspected dynamic scanner dates with direct catalyst sources.

The remaining intraminute VWAP ambiguity was then closed without examining
returns. A hash-frozen implementation of Alpaca's published tape-specific
minute aggregation rules rebuilt all 325 crossing-minute provider bars from
365,379 raw trades: every OHLCV and eligible-trade-count field matched exactly,
every WAP matched within one microdollar, and no condition was unsupported. The
WAP-eligible denominator differed from published volume in every tested minute,
with a median eligible/reported-volume ratio of about 0.8207. Exact trigger-time
VWAP must therefore use the condition-aware raw trade prefix; weighting bar WAP
by published bar volume is not faithful. This closes a replay input definition,
not an alpha question, and earns no production change. See
`SIP_BAR_AGGREGATION.md`.

The next evidence campaign is now fixed rather than aspirational. A seeded
100-date H1-2026 selection excludes every v4 target, permits no substitutions,
and requires 133 source sessions. One hundred thirteen may reuse compatible
hash-attested v4 inputs only while collecting the exact new-master symbol delta;
20 require new full-universe collection. The campaign will freeze direct
catalyst, trigger, and outcome contracts only after the scanner rankings pass
independent inspection. If fewer than 20 unchanged-v3 closed signals survive
all gates, the result is inadequate deployment capacity, not permission to tune
the observed dates. `SCANNER_EXPANSION.md` is authoritative for this sequence.

An implementation audit also found that the evaluator applied the 0.10%
operating median-spread limit but classified every 90-point setup as A+, even
though the frozen rule requires 0.08% for A+. The engine now enforces the
existing A+ spread condition explicitly: a wider but otherwise clean setup can
remain `qualified` after promotion, but it cannot pass an `UNVALIDATED` A+ pilot.
Configuration loading also fails closed on inconsistent time, spread, risk,
maturity, and promotion relationships. This is rule fidelity and operational
hardening, not a new alpha threshold, so v3 remains unchanged. The rules hash
does change to bind a normalized config: the unchanged three-loss, 2% rolling
five-session, and 4% strategy drawdown breakers now live in the numeric source
of truth instead of guard literals. The engine also hard-rejects a planned stop
at or above an observed bid, enforcing the existing outside-spread requirement.

## What The Repository Had Right

The original strategy already addressed several common failure modes: it banned
overnight exposure, limited simultaneous positions, required a real catalyst,
rejected wide spreads and unprotectable stops, preferred marketable limits, and
required detailed journaling. Those controls should remain.

Its principal weakness was not a bad indicator. It was a lack of a measured,
reproducible edge and any executable control that could reproduce the prose. At
the initial review, `TRADES.md` had no completed trades and `trades/` had no
completed session contexts. The current archive has historical no-trade sessions
but still no closed performance-bearing signal. The strategy therefore has no
local win rate, expectancy,
drawdown, slippage distribution, or setup-specific sample. Version v3 adds
deterministic evaluation, structured signal data, computed maturity, and a live
session interlock; those controls make future evidence measurable but do not
manufacture an edge from an empty sample.

## Evidence And Its Limits

### Five-minute ORB evidence

*A Profitable Day Trading Strategy for the U.S. Equity Market* was written in
February 2024 and last revised on SSRN in April 2025. It studied more than 7,000
U.S. stocks from 2016-2023 without survivorship bias. Its base five-minute ORB
returned only 29% in total, with a 41.4% hit rate.
Filtering for the top 20 opening-relative-volume Stocks in Play raised the
reported total return to 1,637%, annualized return to 41.6%, and Sharpe ratio to
2.81, net of the modeled per-share commission. The study's qualifying universe
used price above $5, 14-day average volume of at least one million shares, daily
ATR above $0.50, and first-five-minute relative volume of at least 1.0. It sized
by loss risk, used a stop 10% of daily ATR from entry, and normally held to the
close. [University of St. Gallen paper](https://www.alexandria.unisg.ch/server/api/core/bitstreams/3c2989c4-688d-4d78-8a71-f02690990d51/content)

The paper's exact opening relative volume is:

```text
OR_RVOL = today's 9:30-9:35 volume
          / mean(9:30-9:35 volume for the prior 14 sessions)
```

This is not ordinary full-day relative volume and it is not the current
one-minute volume divided by the previous 14 one-minute bars.

The evidence implementation differs materially from this project:

| Dimension | Paper portfolio | Project v3 |
| --- | --- | --- |
| Selection | Top 20 OR_RVOL names | At most one scored name |
| Direction | Long bullish ranges and short bearish ranges | Long only |
| Entry | Range trigger after the first five minutes | Trigger only through 10:30 ET |
| Exit | Stop or end of day | Structural failure or +2% runner overlay; flat by 3:50 |
| Filters | Price, volume, ATR, OR_RVOL rank | Adds catalyst, VWAP, market, resistance, spread, and depth gates |
| Modeled costs | Per-share commission | Spread, slippage reserve, liquidity cap, and observed execution |

These differences can help or hurt. They mean the paper does not validate the
project's single-name selection or exit overlay. Every closed project trigger
must therefore retain a paired paper-aligned end-of-day shadow result.

The result does not validate this repository's implementation. The paper traded
a diversified long-short portfolio, while this project trades one concentrated,
long-only name. The paper modeled commissions but did not establish this
project's Robinhood spread, market impact, stop slippage, tool latency, catalyst
filter, VWAP overlay, +2% milestone, or single-candidate selection. Its reported
return must be treated as motivating evidence, not an expected result.

### Why local validation is mandatory

In a large study of day traders, less than 1% showed predictably positive
abnormal returns after fees. That result came from a different market and era,
so it does not estimate this project's odds, but it is strong evidence against
assuming that activity or confidence equals skill. [Barber, Lee, Liu, and
Odean](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=529063)

Backtest selection also inflates apparent Sharpe ratios. The Deflated Sharpe
Ratio was developed to adjust for multiple testing and non-normal returns, and a
study of 888 algorithms found that ordinary backtest Sharpe had little power to
predict later out-of-sample performance. [Deflated Sharpe Ratio
paper](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551), [backtest
versus out-of-sample study](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2745220)

The practical conclusion is simple: freeze the rules for a meaningful sample,
log every eligible signal, include all execution costs, and promote the strategy
only from out-of-sample results.

### 2026 broker and regulatory facts

- FINRA's intraday margin standards became effective on June 4, 2026, with a
  member phase-in through October 20, 2027. The rule replaced PDT counting and
  the $25,000 PDT minimum, but it did not remove maintenance margin or intraday
  deficit risk. The live broker review remains authoritative. [FINRA Regulatory
  Notice 26-10](https://www.finra.org/rules-guidance/notices/26-10)
- Robinhood states that its Agentic account can be traded by a connected agent
  and warns that agents can act on incomplete or stale information. [Robinhood
  Agentic Trading overview](https://robinhood.com/us/en/support/articles/agentic-trading-overview/)
- Robinhood supports equity market, limit, stop, stop-limit, and trailing-stop
  orders but not bracket orders; the current MCP also exposes no OCO operation.
  A target order and protective stop therefore cannot be assumed to cancel one
  another. [Robinhood order
  types](https://robinhood.com/us/en/support/articles/360001213963/)
- A stop-market order prioritizes execution but may fill materially below its
  trigger; a stop-limit controls price but can leave the position open. This
  strategy uses a stop-market only after rejecting names where that execution
  risk is unacceptable. [FINRA stop-order guidance](https://www.finra.org/investors/insights/stop-orders-factors-consider-during-volatile-markets)
- Automated errors can compound quickly, which supports price/size limits,
  duplicate-order protection, and a kill switch. [SEC market-access risk-control
  FAQ](https://www.sec.gov/rules-regulations/staff-guidance/trading-markets-frequently-asked-questions/divisionsmarketregfaq-0)

## Revised Production Setup

### 1. Strategy maturity

The current state is `UNVALIDATED` because there are no completed
performance-bearing signals or live trades.

| State | Minimum evidence | Permitted live exposure |
| --- | --- | --- |
| `UNVALIDATED` | Fewer than 20 frozen-rule closed signals | A+ only; at most 0.25% equity planned risk and 80% allocation cap |
| `PROVISIONAL` | At least 20 closed signals, five live executions, complete capture, expectancy above 0R, profit factor at least 1.20, max drawdown at most 6R, zero violations | At most 0.50% equity planned risk and 85% allocation cap |
| `VALIDATED` | At least 50 closed signals including 20 confirmation signals, ten live and five stop executions, positive overall/confirmation expectancy and 90% bootstrap lower bound, profit factor at least 1.30, max drawdown at most 6R, zero violations, execution in budget | At most 0.65% equity planned risk and 100% allocation cap |

`Closed signals` includes live and properly recorded shadow trades. Five live
executions are required before provisional status because a shadow fill cannot
validate the broker path. The last 20 or more preregistered confirmation signals
support the out-of-sample gate. Do not mix rule hashes within a sample. A
material rule change starts a new strategy version and new sample. Fifty
observations are still small; promotion allows the aggressive tier but does not
establish certainty. `strategy_ledger.py report` is the authority for earned
maturity.

### 2. Candidate universe at 9:35 ET

Require all of the following:

- U.S.-listed common stock, long tradability confirmed.
- Opening price above $5; prefer above $10.
- Prior 14-session average daily volume at least 1,000,000 shares.
- Daily ATR over the prior 14 completed sessions at least $0.50.
- Current first-five-minute OR_RVOL at least 1.0; prefer at least 3.0.
- A bullish 9:30-9:35 candle (`close > open`); a doji or red candle is ineligible.
- A verified, non-rumor catalyst with a timestamp and direct source URL.
- No current or recent halt concern, broken/crossed quote, unstable listing,
  recent reverse split without stable liquidity, or obvious dilution conflict.

The scanner is only the coarse universe builder. At 9:35, shortlist candidates
using the saved relative-volume/mover scan, then call intraday historicals in
batches and compute exact OR_RVOL from each session's 9:30-9:35 bar. Record
whether the rank is exact across the universe or approximate within the scanner
results. Never label an approximate rank as the paper's exact top-20 rank.

### 3. Entry window and trigger

- Production entries are allowed from 9:35 through 10:30 ET only.
- Buy only on a trade through the first-five-minute high, in the direction of the
  bullish opening candle.
- Price must be above current session VWAP, VWAP must be flat-to-rising, and the
  stock must show relative strength versus SPY and QQQ.
- Do not chase if the current ask is more than 0.15% above the range high or if
  the new price invalidates the stop, allocation, liquidity, or reward/risk math.
- There must be at least 2.2% unobstructed room to a defensible resistance level
  and at least 2.5R to that level after estimated spread and slippage.
- If no candidate qualifies by 10:30, record a no-trade decision.

VWAP, catalyst, resistance, and market alignment are project-specific overlays,
not claims from the ORB paper. Their contribution must be logged so later data
can show whether they help.

### 4. Quote and liquidity gate

Immediately before review, take three fresh quote/book snapshots over roughly
10 seconds:

- Every quote and book timestamp must be no more than five seconds old when read.
- Bid and ask must be positive and uncrossed.
- Median spread must be at or below 0.10%; no snapshot may exceed 0.15%.
- A+ classification requires a median spread at or below 0.08%.
- Planned quantity may not exceed 5% of visible ask depth executable at the
  entry limit or 5% of recent real one-minute volume. Ignore interpolated bars.
- Reject sudden depth withdrawal, repeated spread expansion, or a price book
  that cannot support the marketable limit and a credible stop exit.

### 5. Stop and position sizing

For a long candidate:

```text
atr_stop_distance = 0.10 * daily_ATR_14
technical_distance = entry_limit - technical_invalidation
stop_distance = max(atr_stop_distance, technical_distance)
planned_stop = entry_limit - stop_distance

stop_slippage_reserve_per_share = max(current_spread, 0.001 * entry_limit)
risk_per_share = stop_distance + stop_slippage_reserve_per_share
risk_budget = account_equity * permitted_risk_fraction

q_risk = floor(risk_budget / risk_per_share)
q_allocation = floor(allocation_cap * available_buying_power / entry_limit)
q_liquidity = floor(liquidity_cap_shares)
quantity = min(q_risk, q_allocation, q_liquidity)
```

Reject the trade when:

- `stop_distance / entry_limit > 0.8%`;
- the stop is inside the spread or ordinary opening noise;
- expected loss including the stop-slippage reserve exceeds the maturity-state
  risk cap;
- all risk, allocation, or liquidity caps produce a zero-share order; or
- buying-power, margin, tradability, review, or liquidity checks fail.

Seventy percent remains the aggressive allocation target, but not a hard gate.
The target is mathematically incompatible with part of the allowed stop range:
at 0.50% account risk, a 0.70% stop plus a 0.10% reserve can deploy only about
62.5% of buying power. Rejecting that trade solely for safe sizing discards the
same planned R economics. Version v3 accepts an otherwise qualified trade at the
risk- and liquidity-capped size and records the binding cap and shortfall. It
never increases loss risk merely to reach the target.

#### Worked sizing example

Assume $25,000 account equity and buying power, a $50.00 entry, a $49.70 valid
stop, and $0.05 per-share stop slippage reserve.

```text
A risk budget             = $25,000 * 0.50% = $125.00
risk per share            = $0.30 + $0.05 = $0.35
q_risk                    = floor($125 / $0.35) = 357 shares
80% allocation cap       = floor($20,000 / $50) = 400 shares
quantity before liquidity = min(357, 400) = 357 shares
notional                  = $17,850 = 71.4% of buying power
planned loss reserve      = 357 * $0.35 = $124.95
+2% gross milestone       = $51.00, or $357 gross = 2.86R
```

The same setup at a validated A+ risk budget of 0.65% would permit 464 shares,
or 92.8% notional, before the liquidity cap. The example shows why allocation
must follow stop risk rather than precede it.

### 6. Order lifecycle

- Use a marketable buy limit with the smallest cushion supported by the current
  ask and book. Never chase beyond the precomputed maximum entry.
- Use one UUID for one logical order and reuse it only for a transport retry.
- Treat unknown transport outcome as unresolved; query orders before retrying.
- If an entry is not fully filled within 10 seconds or the trigger fails, cancel
  the unfilled remainder. Immediately protect any filled quantity.
- Recompute milestone, stop, risk, and allocation from the actual average fill.
- Use a broker-held, regular-hours, GFD stop-market for the filled whole shares
  after review/confirmation requirements are satisfied.
- Robinhood has no bracket/OCO order. Do not place an independent target sell
  while the full-size stop remains live.
- Tool instructions override repository authorization. If review requires user
  confirmation, show the required review and obtain it. Before entering, ensure
  the workflow can also satisfy any confirmation needed for prompt stop
  protection; otherwise do not enter. If the tool accepts a review bypass only
  after an explicit user request, ask for that session-specific stop/emergency
  exit authorization in the entry-review message rather than inferring it from
  repository standing authorization.
- Without native replace support, review and confirm a tighter stop while the old
  stop is live; then cancel/confirm the old stop and immediately place the new
  stop. If replacement fails, restore protection or flatten.
- For a manual exit, pre-review and confirm while the stop is live; then
  cancel/confirm the stop and immediately submit the exit. If submission fails,
  restore protection or flatten through the safest available route.

### 7. Monitoring and exit

- Poll quotes/books about every 5-10 seconds while an active trigger or open
  position needs attention; poll order state after every action.
- The broker-held stop remains primary protection. Monitoring is not a substitute
  for the resting stop.
- Never widen a stop.
- At +1R, tighten only if a confirmed higher low or other valid structure leaves
  the stop outside normal noise.
- At +2R, trail below the nearest confirmed higher low or VWAP structure.
- At +2.0% from fill, this is a milestone, not an automatic ceiling. Continue
  only while price is above rising VWAP, spread is at or below 0.10%, the market
  and catalyst remain supportive, and a stop can lock at least +1R outside noise.
  Otherwise exit immediately.
- Exit on planned milestone/runner or trend failure, catalyst contradiction,
  VWAP loss, liquidity or spread failure, halt risk, market reversal, monitoring
  failure, or by 3:50 ET.
  The earlier force-flat time gives five minutes to resolve cancels or partials.
- Once flat, confirm the position is zero, cancel the remaining stop, and confirm
  the cancellation. Never infer flatness from an order submission alone.

### 8. Circuit breakers

- At most one filled entry per trading day.
- Stop for the day after any stopped-out or thesis-invalidated trade.
- Hard planned-risk cap: 0.25% while unvalidated, 0.50% while provisional, and
  0.65% when validated; 0.75% is an absolute realized planning ceiling, never a
  target.
- Pause new live entries after three consecutive losing trades, a 2% rolling
  five-session equity drawdown, or a 4% strategy peak-to-trough drawdown.
- Resume only after a written audit of signal quality, spread, slippage, tool
  health, and rule adherence. Shadow trading may continue during the pause.
- Any stale data, duplicate/unknown order, broker outage, missing stop, or
  inability to monitor triggers the kill switch: no new order, and flatten an
  existing position as soon as the broker surface safely permits.

These drawdown thresholds are risk-governance choices for this concentrated
implementation; they are not estimates from the cited ORB backtest.

## Measurement Protocol

Every eligible signal, including rejected and shadow signals, must record:

- Strategy version and maturity state.
- Exact OR_RVOL inputs and ranking scope.
- Catalyst class, source, and timestamp.
- Opening range OHLCV, ATR, VWAP, SPY/QQQ context, spread snapshots, and depth.
- Planned and actual entry, stop, slippage reserve, quantity, notional, and risk.
- Fill latency, entry slippage, stop placement latency, and stop slippage.
- Maximum favorable excursion (MFE), maximum adverse excursion (MAE), realized
  P/L after fees in dollars, percent, and R.
- Rule adherence and reason for every override or rejection.
- Current rules hash, public session/signal aliases, sample phase, complete
  scanner-capture status, engine decision, guard decision, and sizing binding cap.
- Project-exit R and the paired paper-aligned end-of-day shadow R.

Core metrics by strategy version and setup:

```text
net_R = net_realized_pnl / planned_risk_dollars
expectancy_R = mean(net_R)
profit_factor = sum(positive net P/L) / abs(sum(negative net P/L))
win_rate = winning_trades / closed_trades
payoff_ratio = average_win_R / abs(average_loss_R)
```

Also report median and 95th-percentile entry slippage, stop slippage, maximum
drawdown in R and dollars, rule-violation count, and no-trade frequency. Compare
results with a shadow version that exits at the close so the +2% milestone and
trailing overlay can be evaluated rather than assumed.

`strategy_ledger.py record` creates the append-only public observations;
`strategy_ledger.py audit` checks schema, uniqueness, privacy, and required
paired outcomes; and `strategy_ledger.py report` calculates these metrics and
the promotion result. `strategy_engine.py` is the canonical candidate math and
`session_guard.py` is the pre-entry/open-position interlock. The scripts do not
replace authoritative broker or market data; they prevent inconsistent use of it.

Do not optimize thresholds after every loss. Review on a fixed cadence of 20
closed signals or monthly, whichever is later. Preserve the old version's sample
when changing a rule.

## Bottom Line

The fastest credible route to growth is not maximizing notional on every mover.
It is concentrating only when a reproducible Stock-in-Play ORB signal, a valid
volatility/technical stop, executable liquidity, and current evidence all agree.
Until the local ledger proves positive net expectancy, the strategy is an
experiment and must be sized as one.
