# 2026 Agentic Intraday Strategy Review

Reviewed: 2026-07-15 (ET)

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
5. Add stale-data checks, order timeouts, duplicate-order controls, drawdown
   circuit breakers, and an evidence-based strategy maturity state.
6. Obey the current MCP tool contract. Repository authorization cannot replace
   an explicit confirmation that a broker tool or platform requires.

## What The Repository Had Right

The original strategy already addressed several common failure modes: it banned
overnight exposure, limited simultaneous positions, required a real catalyst,
rejected wide spreads and unprotectable stops, preferred marketable limits, and
required detailed journaling. Those controls should remain.

Its principal weakness was not a bad indicator. It was a lack of a measured,
reproducible edge. As of this review, `TRADES.md` has no completed trades and
`trades/` has no completed session contexts. The strategy therefore has no local
win rate, expectancy, drawdown, slippage distribution, or setup-specific sample.

## Evidence And Its Limits

### Five-minute ORB evidence

The 2025 revision of *A Profitable Day Trading Strategy for the U.S. Equity
Market* studied more than 7,000 U.S. stocks from 2016-2023 without survivorship
bias. Its base five-minute ORB returned only 29% in total, with a 41.4% hit rate.
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

The current state is `UNVALIDATED` because there are no completed local trades.

| State | Minimum evidence | Permitted live exposure |
| --- | --- | --- |
| `UNVALIDATED` | Fewer than 20 frozen-rule closed signals | 70-80% notional, at most 0.50% equity planned risk |
| `PROVISIONAL` | At least 20 closed signals, net expectancy above 0R, profit factor above 1.20, execution within budget | 70-85% notional, at most 0.50% equity planned risk |
| `VALIDATED` | At least 50 closed signals, positive out-of-sample expectancy, profit factor above 1.30, max drawdown at or below 6R, acceptable stop slippage | A+ trades may use 90-100% notional and at most 0.65% equity planned risk |

`Closed signals` includes live and properly recorded shadow trades. Do not mix
rules within a sample. A material rule change starts a new strategy version and
new sample. Fifty observations are still a small sample; promotion allows the
aggressive tier but does not establish certainty.

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
- the resulting notional is below 70% of buying power; or
- buying-power, margin, tradability, review, or liquidity checks fail.

This makes 70-100% allocation conditional on risk. It never permits increasing
loss risk merely to reach an allocation band.

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
- Hard planned-risk cap: 0.50% of equity until validated; 0.65% for validated A+
  setups; 0.75% is an absolute realized planning ceiling, never a target.
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

Do not optimize thresholds after every loss. Review on a fixed cadence of 20
closed signals or monthly, whichever is later. Preserve the old version's sample
when changing a rule.

## Bottom Line

The fastest credible route to growth is not maximizing notional on every mover.
It is concentrating only when a reproducible Stock-in-Play ORB signal, a valid
volatility/technical stop, executable liquidity, and current evidence all agree.
Until the local ledger proves positive net expectancy, the strategy is an
experiment and must be sized as one.
