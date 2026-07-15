# Session And Trade Context Template

Use this structure in `trades/active/YYYY-MM-DD-session.md` when a live or shadow
workflow starts. Keep one canonical session file and link symbol-specific
files only when the session becomes too large. Move completed context to
`trades/archived/`.

Do not publish full account numbers, order IDs, UUIDs, authentication material,
or MFA details.

## Session

- Date (ET):
- Start time (ET):
- End time (ET):
- Mode: live / shadow
- Strategy version:
- Maturity: UNVALIDATED / PROVISIONAL / VALIDATED
- Account: Agentic account (identifier omitted)
- Starting equity:
- Available buying power:
- Permitted risk fraction:
- Permitted allocation band:
- Rolling five-session drawdown:
- Consecutive losses:
- Circuit breaker state: clear / paused, with reason
- Existing positions and orders reconciled:
- Tool and monitoring health:

## Market Context

| Time ET | SPY vs VWAP | QQQ vs VWAP | Five-minute direction | Breadth/sector note | Source timestamps |
| --- | --- | --- | --- | --- | --- |

## Opening Candidate Screen

Record the scanner name, filters, result limit, and whether ranking is exact or
only approximate within returned results.

| Symbol | Catalyst URL and time | Open | Avg vol 14d | ATR 14d | OR vol today | Avg OR vol 14d | OR_RVOL | Rank scope | 9:30 candle | Eligible |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- | --- |

For every rejected candidate, record the first hard reject and any additional
material risks. Do not silently omit a candidate after research begins.

## Selected Candidate

- Symbol:
- Catalyst summary:
- Primary source URL:
- Source publication time:
- Catalyst conflict/dilution check:
- Setup: five-minute Stock-in-Play ORB
- Opening range: O / H / L / C / volume
- OR_RVOL calculation:
- Ranking scope:
- Current VWAP and slope:
- SPY/QQQ relative strength:
- Resistance and +2% room evidence:

### Score

| Component | Points | Evidence |
| --- | ---: | --- |
| Catalyst quality | /25 | |
| Opening relative volume | /20 | |
| ORB structure and timing | /25 | |
| Liquidity, spread, depth, tradability | /15 | |
| Market and sector alignment | /10 | |
| Exit-plan quality | /5 | |
| **Total** | **/100** | |

### Hard Gates

- [ ] Verified non-rumor catalyst
- [ ] Bullish first-five-minute candle
- [ ] Exact OR_RVOL at least 1.0
- [ ] Price above flat-to-rising session VWAP
- [ ] Fresh, positive, uncrossed quotes and books
- [ ] Median three-snapshot spread at or below 0.10%
- [ ] No snapshot spread above 0.15%
- [ ] Quantity at or below liquidity cap
- [ ] At least 2.2% room and at least 2.5R before resistance
- [ ] Stop outside normal noise and no wider than 0.8%
- [ ] Position notional at least 70% after risk sizing
- [ ] No position, unresolved order, circuit breaker, margin issue, or halt risk
- [ ] Entry time between 9:35 and 10:30 ET
- [ ] Monitoring and prompt protective-stop workflow available

## Pre-Order Snapshots

| Snapshot | Time ET | Data age | Bid | Ask | Spread % | Ask depth at/inside limit | Recent 1m real volume | Notes |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |

## Trade Plan And Math

- Entry trigger:
- Maximum entry limit:
- Technical invalidation:
- ATR stop distance (`0.10 * ATR14`):
- Final stop distance (`max(ATR stop, technical distance)`):
- Planned stop:
- Stop slippage reserve per share:
- Risk per share:
- Account risk budget:
- `q_risk`:
- Allocation cap and `q_allocation`:
- Liquidity cap and `q_liquidity`:
- Final quantity:
- Planned notional and buying-power percentage:
- +2% milestone:
- Resistance distance and reward/risk:
- Planned force-flat time:

## Broker Review And Confirmation

- Review time (ET):
- Review type and quantity:
- Required market-data disclosure: record verbatim when the tool requires it
- Order checks/alerts:
- User confirmation required by tool: yes / no
- Confirmation received (ET):
- Protective-stop confirmation path verified before entry:

## Order Lifecycle

Do not record full order IDs or ref UUIDs in this public file.

| Time ET | Event | State | Quantity | Price | Fees | Data/order latency | Action |
| --- | --- | --- | ---: | ---: | ---: | --- | --- |

- Unknown transport outcome reconciled before retry:
- Partial-fill remainder canceled:
- Actual average fill:
- Actual risk recomputed:
- Protective stop type: stop-market, regular-hours, GFD
- Protective stop quantity and trigger:
- Stop accepted at (ET):
- Unprotected exposure duration:

## Monitoring

| Time ET | Last | Bid/ask | VWAP | MFE | MAE | Stop | Structure/thesis | Action |
| --- | ---: | --- | ---: | ---: | ---: | ---: | --- | --- |

Record every stop replacement and confirm the previous stop is canceled or
replaced without leaving duplicate sell quantity.

## Exit And Reconciliation

- Exit trigger and reason:
- Exit review/confirmation:
- Exit average price and quantity:
- Exit fees:
- Gross P/L:
- Net P/L:
- Net return on notional:
- Net R:
- MFE in R / percent:
- MAE in R / percent:
- Entry slippage:
- Stop/exit slippage:
- Position confirmed zero at (ET):
- Remaining stop canceled and confirmed at (ET):
- Ending equity/buying power:

## No-Trade Decision

When no trade occurs, record the cutoff time, candidates evaluated, first hard
reject for each, and whether the decision was caused by signal quality, market
context, execution quality, risk limits, or tool health.

## Post-Session Review

- Original thesis held: yes / no / no trade
- All rules followed:
- Any override and who requested it:
- What the data supports changing:
- What must not be changed from one observation:
- Ledger entry added:
- Context moved to `trades/archived/`:
- Commit and push completed:
