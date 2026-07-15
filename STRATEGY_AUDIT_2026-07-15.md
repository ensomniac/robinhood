# Aggressive Growth Strategy Audit

Date: 2026-07-15 (ET)

Resulting strategy version: `2026-07-15-orb-v3`

## Determination

The repository had a thoughtful safety-oriented playbook but not an executable
trading system or a locally demonstrated edge. Aggressive compounding depends on
positive net expectancy, repeatable sizing, low execution leakage, and avoiding
ruin. The five highest-impact gaps all prevented one of those properties from
being measured or enforced.

The research anchor is relevant but not directly transferable. The ORB paper was
written in February 2024 and last revised on SSRN in April 2025. Its strongest
reported result was a diversified top-20 OR_RVOL portfolio with both longs and
shorts, a stop or end-of-day exit, and modeled per-share commission. This project
selects at most one long, stops entries at 10:30, adds discretionary-style
filters, and uses a different exit overlay. Those differences are an unvalidated
new strategy, not a small implementation detail. See the [SSRN record](https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4729284)
and [paper copy](https://www.alexandria.unisg.ch/server/api/core/bitstreams/3c2989c4-688d-4d78-8a71-f02690990d51/content).

## Top Five Findings And Repairs

### 1. Prose could not reproduce a trade decision

Failure mode: OR_RVOL, score, spread, stop, reserve, liquidity caps, and
reward/risk were all instructions for an agent to calculate. There was no single
numeric source of truth and no test that two runs would reach the same result.
Fast execution amplifies even small drift in percentage units or stale inputs.

Repair: `strategy_config.toml` now freezes all numeric rules and schema versions.
`strategy_engine.py` deterministically computes the candidate score, time-matched
14-session OR_RVOL, quote gate, stop, slippage reserve, risk/allocation/liquidity
caps, resistance room, reward/risk, allocation result, and hard rejects. Its
rules hash must follow the decision into the session record.

### 2. Full pilot risk preceded evidence

Failure mode: an `UNVALIDATED` strategy with zero local observations could risk
0.50% of equity with 70-80% notional. Promotion required positive metrics but did
not define a confirmation sample, confidence bound, minimum live execution
evidence, or exact execution budget. A few favorable or selectively retained
outcomes could therefore increase risk prematurely.

Repair: unvalidated live pilots are A+-only, capped at 0.25% planned account risk
and 80% allocation. Provisional risk of 0.50% must be earned from 20 complete,
fixed-rule closed signals including five live executions. Validated risk of 0.65%
requires 50 signals, 20 preregistered confirmation signals, ten live executions,
five stop executions, positive overall and confirmation expectancy, profit factor
at least 1.30, a positive 90% bootstrap lower bound for mean R, drawdown no worse
than 6R, zero violations, and execution within budget. `strategy_maturity.py`
calculates the state instead of trusting a label.

### 3. The project could not measure its edge or selection bias

Failure mode: Markdown requested metrics but supplied no structured observations
or calculator. Logging only researched or traded winners would invisibly bias the
sample. The required shadow end-of-day comparison for the project-specific exit
overlay was not operational.

Repair: `strategy_ledger.py` creates an append-only `SIGNALS.jsonl` ledger with
separate session and signal records. It requires complete scanner-capture status,
fixed version/hash, pilot versus confirmation phase, execution fields, and paired
project/EOD results. It rejects duplicate public aliases, UUIDs, and broker or
account identifier fields. Its audit and report commands reproduce expectancy,
profit factor, drawdown, execution percentiles, no-trade rate, paired exit value,
and earned maturity. `SIGNAL_LEDGER.md` defines the public schema.

### 4. The 70% minimum rejected safely sized positive-R trades

Failure mode: the minimum notional rule contradicted risk-first sizing. At a
0.50% risk budget, a 0.70% stop plus the required 0.10% reserve can deploy only
about 62.5% of buying power. A setup with at least 2.5R before resistance could
therefore pass every edge and execution gate yet be discarded solely because the
system refused its own safe position size.

Repair: 70% remains an aggressive target, not a hard gate. The engine never
increases quantity to reach it. It accepts an otherwise qualified nonzero order
at its safe size and records whether risk, allocation, or liquidity bound the
quantity. This preserves candidate expectancy without increasing planned loss.

### 5. Live safety depended on attention instead of state

Failure mode: one-trade limits, unknown-order reconciliation, entry timeouts,
stop coverage, monitoring freshness, drawdown pauses, and force-flat behavior
were prose. The most dangerous interval—an entry fill before a confirmed stop—had
no executable timer or state decision.

Repair: `session_guard.py` consumes a fresh authoritative broker/session snapshot
and the ledger-earned maturity. It returns explicit states for entry readiness,
active-entry management, immediate protection, protected-position management,
flat-order reconciliation, unknown-order reconciliation, and kill-switch
flattening. It blocks stale rule hashes and maturity inflation. An unprotected
fill gets only the configured short transition window; stale monitoring, stale
data, over-covering stops, unknown orders, unavailable broker tooling, or 3:50 PM
exposure escalate to flatten and reconcile.

## What This Does Not Prove

Version v3 still has zero closed signals. The repairs make a growth claim
testable and operationally safer; they do not make the strategy profitable.
Remaining material risks include:

- The single-name long-only selection, catalyst/VWAP score, 10:30 cutoff, and
  +2% runner overlay all diverge from the paper and need their own data.
- The evaluator and guard depend on complete, fresh broker and market facts from
  the active tool workflow. They cannot protect a position if they are skipped.
- Robinhood does not support equity bracket orders, so entry-to-stop and
  stop-to-manual-exit transitions retain irreducible execution risk.
- Stop markets can slip beyond the reserve in a halt, gap, or fast market. The
  observed p95 reserve feedback reduces ordinary leakage but cannot cap tail loss.
- Fifty signals remain a small sample. Validation is permission to use the
  configured aggressive tier, not proof of durable future alpha.

No broker, account, scanner, email, or live-order action was taken during this
repository audit.
