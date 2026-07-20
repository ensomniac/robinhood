# Persistent Production-Strategy Validation Campaign

Campaign version: `2026-07-19-v1`

Production champion: `2026-07-15-orb-v3`

Current maturity authority: `strategy_ledger.py report`

## Completion Contract

This campaign remains active across bounded Codex invocations until the current
production champion machine-earns `VALIDATED`. A market-hours wait, provider
failure, subscription decision, required broker confirmation, safety pause, or
need for more naturally occurring sessions is a resumable state and never a
successful terminal result.

Terminal completion requires all of these facts at the same time:

- the ledger reports `earned_maturity="VALIDATED"` for the current rules hash;
- the registered champion is `CONFIRMED`, `LIVE_CALIBRATED`, operationally
  `READY`, and its computed readiness is `VALIDATED`;
- at least 50 eligible closed signals include 20 untouched confirmation
  signals, ten live executions, and five naturally occurring live stops;
- overall and confirmation expectancy are positive, profit factor is at least
  1.30, the one-sided 90% bootstrap lower mean R is positive, and maximum
  drawdown is at most 6R;
- entry-slippage p95 is at most 15 bps, unprotected-exposure p95 is at most ten
  seconds, stop slippage is inside its reserve, rule violations are zero, and
  capture and execution records are complete;
- registries, ledger, lifecycle, learning data, security master, progress,
  privacy, historical-store capacity, and controller lineage pass audit;
- a fresh private broker safety snapshot reports reconciled flat state with no
  position, open order, or unknown order; and
- the public worktree is clean and `HEAD` equals its configured upstream.

Validation is evidence that the frozen production contract passed the
repository's alpha, execution, operations, and integrity thresholds. It is not
a guarantee of future profitability.

## Persistent Controller

`strategy_validation.py` owns only campaign coordination. Its append-only,
hash-chained operational events and current-state projection live under the
ignored `learning_runs/production_validation/` directory. Every event binds the
champion ID, strategy version, rules hash, this plan's hash, core upstream
artifact hashes, phase, objective, blocker, next action, evidence hashes, and
the latest privacy-safe broker safety snapshot.

The controller composes the existing strategy registry, ledger maturity report,
learning-data and registry audits, lifecycle state, privacy audit, progress
history, historical-store reserve, Git state, and private run state. It does not
replace any of them. It cannot contact a data provider, access Robinhood, submit
an order, edit a production rule, or promote a strategy axis.

Use it from the repository root:

```sh
python3 strategy_validation.py init
python3 strategy_validation.py status
python3 strategy_validation.py next
python3 strategy_validation.py record \
  --phase SOURCE_RECOVERY \
  --status READY \
  --objective recover-primary-source-failures \
  --next-action "Freeze the accession-bound SEC recovery contract." \
  --evidence research_results/<inspected-result>.json
python3 strategy_validation.py audit
```

`record` cannot accept phase `VALIDATED`. Only `audit` can append that terminal
event, and only after recomputing every completion condition. A champion,
strategy-version, or rules-hash change automatically restarts the active sample
at `DEVELOPMENT_ACQUISITION`; prior public evidence remains intact but cannot
promote the new rules.

The phases are:

1. `SOURCE_SEMANTICS`
2. `SOURCE_RECOVERY`
3. `DEVELOPMENT_ACQUISITION`
4. `DEVELOPMENT_OUTCOMES`
5. `CHALLENGER_REVIEW`
6. `CONFIRMATION`
7. `SHADOW_QUALIFICATION`
8. `LIVE_PILOT`
9. `PROMOTION_AUDIT`
10. terminal `VALIDATED`

Nonterminal statuses are `READY`, `WAITING_MARKET`,
`WAITING_USER_CONFIRMATION`, `WAITING_PROVIDER`, `WAITING_SUBSCRIPTION`,
`WAITING_NEW_SESSIONS`, and `PAUSED_SAFETY`. `next` emits one bounded handoff
for the active agent under `AGENTS.md`; it never performs the handoff itself.

## Source Semantics And Capacity Gate

The first active objective is the frozen 33-pair, 33-document, 38-join contract
in `CATALYST_SOURCE_SEMANTICS_PLAN.md`. `catalyst_source_semantics.py` must
freeze exact inputs and implementation hashes, derive private one-terminal-row
evidence without outcome access, support explicit human/agent review fields,
and independently rebuild its public aggregate.

Terminal disposition precedence is transport or HTTP failure; ownership
unresolved; issuer binding unresolved; irrelevant source; timestamp missing;
timestamp conflict; same-day time unresolved; published after 09:35;
semantics unresolved; nonmaterial; verified conflict; verified negative; and
verified positive. Financing or dilution conflicts run before positive
classification. Metadata, headers, capture time, URL dates, and PDF creation
dates never establish causal publication alone, and same-day date-only evidence
fails.

If fewer than 20 verified-positive catalysts remain, close outcome-blind and
enter `SOURCE_RECOVERY`. If catalyst capacity reaches 20 but fewer than 20 rows
survive the unchanged-v3 non-return gates, enter `DEVELOPMENT_ACQUISITION`. Only
20 or more complete survivors authorize a separately frozen outcome contract.

Recovery order is accession-bound SEC-operated endpoints with a compliant user
agent, frozen retries of transport failures, then canonical issuer-host document
chains. Secondary news is never substituted for a primary source. If existing
sources cannot produce capacity, collect exact disjoint 100-session tranches
with frozen dates, point-in-time universe, security master, splits, provider
queries, source rules, and zero substitution. Use the local store first, retain
whole-provider fidelity, collect full-universe coarse inputs plus selected-name
detail, and preserve the configured 20-GiB reserve. A demonstrated need for a
paid archive produces a vendor/tier memo and `WAITING_SUBSCRIPTION`; the agent
does not purchase access or fabricate credentials.

The first corpus has completed that ordered recovery with only three exact
deduplicated positive pairs, so its outcomes remain locked. The active
`DEVELOPMENT_ACQUISITION` handoff is implemented by
`development_tranche.py` and documented in `DEVELOPMENT_TRANCHE.md`. The
selection implementation is committed before freeze, its manifest is committed
before reference access, and the later provider-bound scanner manifest is
committed before market collection.

The first disjoint tranche later produced 21 source-verified positives, but its
independently inspected causal pre-entry collection retained only 19 complete
inputs; two pairs had no clean cross before 10:30 across 13,219 exact
one-second windows. No target outcome was accessed. Because the 20-survivor
gate is impossible for that frozen corpus, the active handoff remains
`DEVELOPMENT_ACQUISITION` for another exact disjoint 100-session tranche.

## Development, Challenger, And Confirmation Gates

Outcome access starts only after an immutable contract freezes executable entry,
missed-fill, partial-data, bid/ask and tape ordering, 5-bps-per-side primary
cost, 10- and 20-bps stress, stop-first ambiguity, gap-stop fills, stop reserve,
+2% milestone, runner rules, and 15:50 force-flat behavior. No-trade sessions,
misses, unavailable rows, and rejects stay in denominators. Inspected records
publish only through the atomic ledger and lifecycle paths.

Development must have positive expectancy, profit factor at least 1.30, a
positive one-sided 90% bootstrap lower mean R, drawdown at most 6R, positive
chronological halves, positive performance without the five best trades,
positive total R with profit factor at least 1.20 and drawdown at most 6R under
both stress grids, and zero rule, capture, or evidence violations.

If the champion fails, record its exact falsification without tuning it on the
failed corpus. Keep every hard portfolio and protection constraint. Permit one
active challenger and at most three new mechanism families per ISO week. Freeze
one causal change, its parameter family, cost grid, falsification criteria,
compatibility risks, and contamination risks before evaluation. A failed
confirmation retires that exact challenger. A passing challenger becomes a new
production version with atomic evaluator, config, tests, docs, and registry
changes, then restarts maturity evidence under its new rules hash.

Untouched confirmation freezes chronologically separated dates before outcome
access, applies an embargo of at least one session, changes no champion rule,
and requires at least 20 eligible signals with the same alpha, robustness,
stress, drawdown, capture, and integrity gates. Confirmation ledger rows use
`sample_phase="confirmation"` and `mode="shadow"`.

## Shadow, Live, And Promotion

Before live pilots, collect at least five complete eligible prospective shadow
executions through scanner, catalyst, trigger, quote/book, evaluator, guard,
order construction, protection, monitoring, and journal paths with zero
violations or unresolved failures. Only then may the registry execution axis
become `SHADOW_VERIFIED`.

Eligible later market sessions may enter `live` mode under `AGENTS.md`. Broker
and platform confirmations that require Ryan remain mandatory and produce
`WAITING_USER_CONFIRMATION`. While unvalidated, live pilots retain score 90,
median spread 0.08%, 0.25%-equity planned-risk, 80%-allocation, and every other
hard gate. Stops must occur naturally; no marginal entry or manufactured stop
may satisfy a count.

Unknown orders, missing protection, violations, breakers, integrity mismatches,
and monitoring failures pause entries, prioritize reconciliation or flattening,
retain the adverse observation, and require root-cause repair. Execution-budget
failure extends collection after repair. Alpha degradation returns to
`CHALLENGER_REVIEW`. `LIVE_CALIBRATED` is earned only after complete execution
and protection evidence stays within budget.

Every manifest is committed before collection. Every independently inspected
result, registry disposition, reusable finding, production-version update, and
live lifecycle record is committed and pushed after proportionate tests and
privacy audits. Waiting and safety pauses remain active campaign states; only
the completion contract closes the campaign.
