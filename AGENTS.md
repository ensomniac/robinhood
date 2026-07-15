# Robinhood Codex Trading Context

Research refreshed: 2026-07-15

This file governs Codex work in this repository for Robinhood market research,
trade planning, and broker-tool use. It is written for aggressive intraday
equity trading research and automated execution on Ryan Martin's behalf.

The repo this file exists in is meant to store any information codex deems
important, along with individual per-trade context files (for research and state),
stored in /trades/active/ & /trades/archived

Update this file with ongoing changes to the strategy

## Authority Boundary

- The user's (Ryan) trading preference is aggressive day trading: seek explosive
  intraday growth, target a 2% gross gain, exit all trades before the end of the
  same trading day, use 70-100% of available buying power when a trade qualifies,
  based on the trade's conviction, and never hold more than one open trade at a time.
- This file is a standing authorization for Codex to place or cancel real-money
  trades without explicit user approval in the active trading workflow.
- Once Ryan starts a live trading workflow, do not pause for Ryan's approval
  before scanner changes, order reviews, entries, protective stops, stop
  replacements, target exits, flattening actions, canceling stale or redundant
  orders, or journal updates, provided the action stays within this file.
- If Robinhood MCP tool instructions include an order-review or approval step
  that Codex can complete, complete that step and execute on Ryan's behalf. Treat
  this file plus Ryan's live-session start command as the standing authorization.
- Ask Ryan only when blocked by login, MFA, connector outage, a broker/tool
  requirement that cannot be completed by Codex, or a requested action outside
  this file's strategy such as options, short sales, overnight holds, multiple
  simultaneous trades, or a different risk profile.

## How The Robinhood MCP Works

MCP means Model Context Protocol. Robinhood's MCP server exposes broker and
market actions as typed tools that Codex can call. In this environment, the
important tool families are:

- Account state: `get_accounts`, `get_portfolio`
- Market discovery: `get_scans`, `create_scan`, `update_scan_filters`,
  `update_scan_config`, `run_scan`
- Market data: `get_equity_quotes`, `get_equity_price_book`,
  `get_equity_fundamentals`, `search`
- Positions and orders: `get_equity_positions`, `get_equity_orders`
- Equity trade readiness: `get_equity_tradability`, `review_equity_order`
- Live equity actions: `place_equity_order`, `cancel_equity_order`
- Options tooling also exists, but this strategy is equity-only unless the user
  explicitly requests options in a separate active workflow.

Operational MCP rules:

- Never guess an account number. Ryan only has 1 Agentic trading account with
  Robinhood and that is the account that is to be used.
- Only call live order tools for accounts where `agentic_allowed=true`.
- Before a live equity order, call `review_equity_order` by default, evaluate
  buying-power, tradability, halt, margin, and order-check alerts, and proceed
  automatically when no blocking alert exists.
- `place_equity_order` places real-money orders. In this Codex environment, remember
  that speed is critical and you have Ryan's permission to execute as quickly as possible.
- `cancel_equity_order` cancels a real order.
- For immediate fills, prefer a marketable limit order near the current ask for
  buys and near the current bid for sells, rather than an unbounded market order.
- Outside regular hours, Robinhood supports limit orders only for immediate
  execution. Market and stop orders are regular-hours-only. Do not enter this
  day-trade strategy outside regular hours because a protective stop cannot
  execute there.
- Use a fresh UUID `ref_id` for each logical order and reuse it only for retries
  of the same order after transient transport failure. Store each trade's research,
  plan, state, etc. in individual context files in the /trades/ folder.

## Regulatory And Broker Context

- FINRA replaced the old Pattern Day Trader framework with intraday margin
  standards effective 2026-06-04, with broker implementation phase-in allowed
  through 2027-10-20.
- Robinhood says margin accounts are monitored in real time for intraday margin
  deficits under the new standards. Always let the live account and review tools
  determine current buying power, restrictions, and margin alerts.
- Margin day trading can still lose some or all capital, and margin can create
  losses beyond the original investment. Treat every trade idea as unproven
  until the live order review and exit plan are validated.
- Stop orders reduce risk but do not guarantee the stop price. Stop-market
  orders can slip; stop-limit orders can fail to fill. Reject thin, halted,
  unstable, or wide-spread stocks where the stop cannot be trusted.

## Strategy Objective

Primary objective: find one high-conviction long equity day trade with a clean
technical and catalyst-based path to a 2% gross gain before the close.

Hard constraints:

- One open trade maximum across all equity positions and open equity orders.
- Long equities only. No short sales or futures.
- Regular market hours only: 9:30 AM-4:00 PM ET.
- No new entries before 9:35 AM ET; use the first 5 minutes to establish opening
  range and liquidity.
- No new entries after 3:15 PM ET.
- Force flat by 3:55 PM ET, or earlier if liquidity deteriorates.
- Default allocation is 80% of available buying power, bounded by 70-100%, only
  when risk controls are satisfied. Use 90-100% only for A+ setups with unusually
  strong catalyst quality, spread at or below 0.10%, deep book support, clean
  market alignment, and no broker review concerns. Use 75-85% for normal A
  setups. Reject anything below A quality instead of reducing below 70%.
- Gross profit target is +2.0% from average fill, but with high conviction, closely
  monitor a trade if higher gains might be possible. Move the stop as needed.
- Maximum planned loss is 40% of the gain target. For a 2.0% target, the stop
  must be no worse than -0.8% from average fill, before expected slippage.
- If the technically valid stop is wider than 0.8%, reject the trade rather than
  widening the stop or reducing discipline.
- If the stop would sit inside normal noise, reject the trade rather than using
  an arbitrary tight stop. The planned stop must be outside the current spread,
  outside ordinary candle noise for the setup, and far enough from the trigger to
  represent real technical invalidation while still staying within the 0.8% cap.
- Use whole-share sizing unless fractional shares can be protected by a valid
  stop order in the current account and session.

## Market Discovery Loop

During an active user-invoked trading session, scan continuously until either a
valid trade setup appears, the user stops the session, or the entry cutoff passes.

1. Establish the account and session state.
   - Call `get_accounts`; require a clear account number and `agentic_allowed`
     status before any trade-ticket work.
   - Call `get_portfolio` for buying power.
   - Call `get_equity_positions` and today's `get_equity_orders` to verify
     there is no open position, open stop, open target, queued order, partial
     fill, or unresolved rejection. If anything is open, manage or resolve it
     before considering a new entry.

2. Build the live candidate universe.
   - Start with saved scanners from `get_scans` and run relevant gainers,
     movers, volume, or catalyst scans with `run_scan`.
   - If a purpose-built scan is missing and scanner writes are appropriate in
     the active workflow, create or update scans for high-volume daily gainers,
     unusual volume, and earnings/news movers.
   - Prefer candidates with price above $10, hard minimum price above $5,
     regular-session volume above 1,000,000 shares, regular-session dollar
     volume above $25,000,000, relative volume above 3x and preferably above 5x,
     clear positive catalyst, and tradability allowed by Robinhood.
   - Exclude securities with halts, pending halt risk, broken quotes, spreads
     above 0.20%, inadequate book depth, recent reverse splits without stable
     liquidity, or social-media-only catalysts.

3. Validate each candidate.
   - Use `get_equity_quotes` for real-time bid, ask, last, and prior close.
   - Use `get_equity_price_book` to verify spread and visible depth.
   - Use `get_equity_fundamentals` for current OHLCV, average volume, market cap,
     float, and profile context.
   - Use live web/news research when the catalyst is not already clear. Prefer
     issuer releases, SEC filings, exchange notices, and reputable market news.
   - Compare the candidate against SPY and QQQ as market context. Avoid long
     entries when the broad market is sharply reversing unless the candidate has
     independent relative strength.
   - Reject candidates when the planned order would be too large for displayed
     depth or recent tape volume. As a default, require the planned quantity to
     be no more than 5% of visible near-book depth or recent one-minute volume
     unless book refreshes and prints clearly show enough executable liquidity.

4. Score the setup before preparing an order.
   - Catalyst quality: 25 points
   - Relative volume and abnormal participation: 20 points
   - Technical structure and timing: 25 points
   - Liquidity, spread, depth, and tradability: 15 points
   - Market/sector alignment: 10 points
   - Exit-plan quality: 5 points
   - Minimum score: 85/100, with no hard reject conditions.
   - Minimum score: 90/100 for 90-100% allocation. Scores from 85-89 may still
     qualify only for 75-85% allocation and only if every hard gate is clean.
   - Hard gates before any order ticket: verified non-rumor catalyst, price
     above VWAP for long setups, spread at or below 0.20% and preferably 0.10%,
     visible room of at least 2.2% before obvious resistance, reward/risk at or
     above 2.5:1 after spread and expected slippage, and a stop that is both
     technically valid and no wider than 0.8%.

5. Build a premarket shortlist when the workflow starts before the open.
   - From 9:00-9:30 AM ET, prepare but do not enter. Use earnings, issuer press
     releases, SEC filings, analyst actions, FDA or contract news, exchange
     notices, and abnormal premarket volume to build a focused watchlist.
   - At 9:30-9:35 AM ET, observe the opening range, spread, book behavior, and
     whether the catalyst names hold relative strength. Do not enter before
     9:35 AM ET.

## Qualified Long Setups

Use only one of these setup families unless Ryan explicitly instructs a new
playbook.

1. Five-minute opening range breakout.
   - Wait until the first 5-minute candle closes.
   - Candidate must be a stock in play: unusually high opening volume and a real
     catalyst.
   - Opening candle should show directional strength, ideally close above open.
   - Entry trigger is a break above the 5-minute opening range high with volume
     expansion, price above VWAP, and marketable liquidity.
   - Stop goes below the breakout failure level, VWAP, or nearest clean swing
     low, but never more than 0.8% below entry.

2. VWAP pullback continuation.
   - Candidate is already trending above VWAP with rising or stable VWAP.
   - Pullback into VWAP or a short moving average holds on reduced selling
     pressure.
   - Entry trigger is reclaim/continuation with bid support and tightening
     spread.
   - Stop goes below VWAP/swing invalidation, capped at 0.8%.

3. High-of-day continuation.
   - Candidate consolidates tightly below high of day after a catalyst move.
   - Breakout must have enough room to reach +2% before major visible resistance.
   - Avoid if the stock is already extended far above VWAP and the stop would be
     arbitrary or wider than 0.8%.

## Order Construction

Before any live order:

- Verify no existing equity position or unresolved equity order.
- Verify current account buying power supports a 70-100% allocation without
  creating a margin or intraday margin deficit alert.
- Verify `get_equity_tradability` allows the planned symbol and session.
- Compute:
  - `entry_limit`: marketable buy limit near current ask, with a tight slippage
    cap justified by spread and depth.
  - `target_price = average_fill * 1.02`
  - `max_stop_price = average_fill * 0.992`
  - `planned_stop`: the tighter of technical invalidation and `max_stop_price`
    for risk control.
  - `allocation = 80%` of available buying power by default, bounded 70-100%
    according to the conviction tiers in Hard constraints.
  - `quantity = floor(allocation / entry_limit)` unless whole-share sizing makes
    the account allocation impossible. Do not use a quantity that prevents a
    valid protective stop.
- Call `review_equity_order`, evaluate and log the trade ticket, thesis, target,
  stop, invalidation, allocation, expected account risk, and all alerts.
- If review returns no blocking alert and the trade still satisfies this file,
  immediately call `place_equity_order` without waiting for Ryan.

After a reviewed and filled entry:

- Poll `get_equity_orders` until the entry state is filled, rejected, cancelled,
  or stale.
- If the entry partially fills, size the exit plan to the actual filled quantity.
- Immediately prepare and review a protective sell stop for the filled quantity.
- If the protective stop review returns no blocking alert, immediately place the
  protective stop without waiting for Ryan.
- Do not place a simultaneous target order unless an OCO/bracket mechanism is
  available and verified through the active broker/tool surface. If no OCO
  exists, actively monitor the position and close manually with a marketable
  limit sell when target or invalidation occurs.

## Live Monitoring And Exit Management

Once in a trade, monitoring is mandatory. If Codex cannot monitor, do not enter.

Polling loop while position is open:

- Check quote and price book every 15-30 seconds, or faster during sharp moves.
- Check order state after every order action.
- Keep a running log of thesis, price, spread, volume behavior, stop, target,
  and any catalyst updates.
- Never widen a stop. Stops may only stay fixed or tighten.
- Move stop toward breakeven only if price advances enough that normal noise is
  unlikely to trigger it immediately.
- At +2.0%, exit immediately unless the trade qualifies for a structured runner.
  A runner is allowed only when price remains above VWAP, spread is at or below
  0.15% and preferably 0.10%, the broad market remains supportive, the catalyst
  is still intact, and the stop can be tightened to breakeven or better without
  sitting inside normal noise.
- For a structured runner, trail against the nearest clean higher low, VWAP
  hold, or tight consolidation failure. Never let a winning trade turn into a
  planned loss after the +2.0% target has been reached.
- Exit immediately if:
  - price reaches target,
  - price hits planned stop or technical invalidation,
  - catalyst is contradicted,
  - VWAP or opening range structure fails,
  - spread widens beyond 0.20%,
  - liquidity disappears,
  - market-wide reversal invalidates the long thesis,
  - the symbol is halted or halt risk becomes obvious,
  - order monitoring fails,
  - it is 3:55 PM ET.

For target exits, use a marketable limit sell near the bid with enough price
protection to avoid a poor fill. After the position is flat, cancel any remaining
protective stop automatically when the tool permits Codex to complete the action.
If one trade is stopped out, stop looking for new entries for the day unless Ryan
explicitly starts a new live workflow after that loss.

## Trade Journal And Continuous Improvement

Record every researched candidate and every trade decision in a local journal
file if one exists, or create `trading-journal.md` when the user asks to track
results. Minimum fields:

- Date and ET timestamps
- Account used, without exposing secrets
- Candidate source and scan
- Catalyst source links
- Entry setup family
- Score and disqualifying risks considered
- Entry, target, stop, quantity, allocation, and expected account risk
- Review alerts
- Fill details
- Exit details
- P/L in dollars and percent
- Whether the original thesis held
- What to change next time

Revise the plan only from evidence. Prefer tightening filters that reduce false
positives: better catalyst quality, tighter spread limits, higher relative
volume, stronger VWAP behavior, cleaner 2:1+ reward/risk, and more reliable
market alignment.

## Hard Reject Conditions

Reject a trade immediately if any of these are true:

- No explicit account number.
- Account is not `agentic_allowed=true` for live order tools.
- A current position or unresolved order already exists.
- Stop cannot be placed, monitored, or respected.
- Target is less than 2% or stop would be more than 0.8%.
- Stop is so tight that it sits inside the spread or ordinary setup noise rather
  than at real technical invalidation.
- Reward/risk is worse than 2.5:1 before slippage.
- Spread is above 0.20% or book depth is inadequate for the planned allocation.
- Planned order size is too large for near-book depth or recent one-minute tape
  volume.
- The move is based only on rumor, social media, or unexplained scanner activity.
- The entry is outside regular market hours.
- The entry is before 9:35 AM ET or after 3:15 PM ET.
- The trade cannot be exited by 3:55 PM ET.
- Any order review, broker alert, connector error, login/MFA requirement, or
  external blocker that Codex cannot resolve automatically is unresolved.

## Research Sources

- Robinhood Agentic Trading: https://robinhood.com/us/en/agentic-trading/
- Robinhood Day Trading: https://robinhood.com/us/en/support/articles/day-trading/
- Robinhood Extended-Hours Trading: https://robinhood.com/us/en/support/articles/extendedhours-trading/
- FINRA Regulatory Notice 26-10: https://www.finra.org/rules-guidance/notices/26-10
- FINRA frequent intraday trading overview: https://www.finra.org/investors/insights/frequent-intraday-trading
- FINRA new intraday margin requirements: https://www.finra.org/investors/insights/intraday-margin-requirements
- FINRA order types: https://www.finra.org/investors/investing/investment-products/stocks/order-types
- Investor.gov order types: https://www.investor.gov/introduction-investing/investing-basics/how-stock-markets-work/types-orders
- QuantConnect opening range breakout research summary: https://www.quantconnect.com/research/18444/opening-range-breakout-for-stocks-in-play/
- SSRN opening range breakout paper: https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4729284
- Charles Schwab VWAP overview: https://www.schwab.com/learn/story/how-to-use-volume-weighted-indicators-trading
