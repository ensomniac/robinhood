# Production-Aware Strategy Learning Execution Plan

Prepared: 2026-07-18 ET

Mode: `learning`

Production strategy: `2026-07-15-orb-v3`, frozen

## Objective And Decision Boundary

Use the immutable historical corpus to identify trade-capable, risk-compatible
strategy hypotheses and improve the speed and quality of the learning loop. This
work may add research tooling, tests, compact results, and an isolated strategy
proposal. It may not edit `strategy_config.toml`, activate a live rule, access a
broker, place an order, or count retrospective results as independent promotion
evidence.

The governing principle is that a strategy which never trades cannot be
successful or learn. Live safety gates remain hard. Unvalidated alpha gates must
instead expose counterfactual outcomes so their marginal value can be measured.

## Baseline Outcome Surface

- The evidence manifest requests 100 dates; 95 immutable bundles are available
  and five remain explicit blockers.
- The complete corpus contains 950 candidates and the public production ledger
  contains 1,060 rejected signals across 106 no-trade sessions.
- Production has zero closed performance-bearing signals.
- The local four-strategy matrix evaluates 400 date-strategy pairs in about two
  seconds with zero provider requests.
- Cold acquisition, not local evaluation, remains the learning-time bottleneck.

## Ten Controls To Preserve

1. Frozen date and symbol manifests with no post-selection substitution.
2. Exact ordered-symbol and evidence-bound bundle identity.
3. Progressive completed-bar disclosure with no plugin lookahead.
4. Next-bar execution and adverse stop-first resolution for ambiguous minutes.
5. One selected trade per strategy per day.
6. Explicit missing-date and unsupported-requirement blockers.
7. Shared execution assumptions across compared policies.
8. Research isolation from `SIGNALS.jsonl`, `TRADES.md`, `trades/`, maturity,
   broker state, and production configuration.
9. Deterministic configuration, dataset, implementation, and result hashes.
10. Full tests, sensitive-data, lifecycle, ledger, progress, diff, commit, and
    push gates before completion.

## Five Improvement Hypotheses

### H1 - Trade-capable counterfactual learning

Compute an outcome for every bar-executable signal, even when the production
alpha stack would reject it. Retain every production rejection reason and report
pass-versus-fail outcome cohorts for each gate.

- Expected benefit: replaces zero production labels with gate-attribution data.
- Primary risk: retrospective counterfactuals could be mistaken for live-ready
  trades.
- Acceptance: results are research-only, evidence-bound, one-trade policies are
  separate from all-signal diagnostics, and production artifacts remain byte
  identical.

### H2 - Early-window execution

Test signals before 09:40 ET against the existing 10:30 window under 0, 5, 10,
and 20 basis points of adverse entry and exit slippage and 1R, 1.5R, 2R, and 3R
targets. Measure the gap between bar signal time and delayed production
evaluation time.

- Expected benefit: determine whether speed is part of the edge and quantify
  the historical adapter's chase bias.
- Primary risk: the cutoff was discovered on the same sample.
- Acceptance: chronological thirds, monthly stability, bootstrap uncertainty,
  concentration, and an explicit contaminated-holdout warning are published.

### H3 - Stop, allocation, and risk compatibility

Test unrestricted structural stops and production-compatible 0.8% stop cohorts.
Report stop-distance distributions and implied notional at the current 0.25%
account-risk budget with a 0.10% reserve.

- Expected benefit: prevents a positive R-model from being presented as
  deployable when it cannot meet production geometry.
- Primary risk: filtering after observing outcomes overfits the stop threshold.
- Acceptance: the 0.8% value comes only from frozen production configuration;
  no threshold search may redefine it.

### H4 - Early opening reversal and weak-strategy deletion

Test the existing opening-reversal signal before 09:40 and confirm that current
VWAP-pullback and high-of-day variants do not merit more near-term work.

- Expected benefit: directs learning capacity toward the strongest stored
  hypothesis while pruning weak branches.
- Primary risk: the reversal was invented and evaluated on this corpus.
- Acceptance: it may become only an independent-confirmation proposal. It may
  not be called validated or activated.

### H5 - Cross-sectional selection quality

Compare earliest-signal selection using signal strength, production score, and
opening-relative-volume rank. Separately test earnings-item 2.02 filtering.

- Expected benefit: one-trade-per-day concentration makes ranking decisive.
- Primary risk: approximate ten-name scanner ranks are not full-market ranks,
  and repeated policy comparisons inflate apparent performance.
- Acceptance: all policies are versioned, the complete comparison family is
  disclosed, and no winning selector is promoted from this sample.

## Adversarial Checks

- Recalculate dataset identity from the public evidence manifest and every
  requested bundle hash.
- Reject bundles with missing attestations, wrong dates, incomplete sessions,
  or ordered-symbol mismatches.
- Form signals only from progressively completed one-minute prefixes and enter
  no earlier than the following bar.
- Keep missing dates in every denominator and comparison.
- Publish all tested policy and cost combinations, not only winners.
- Label chronological splits as retrospective stability checks because the full
  corpus was already inspected.
- Keep fixed production thresholds distinct from exploratory parameters.
- Hash the lab, runner, strategy-plugin, policy, execution-grid, and evidence
  inputs into the run identity.
- Verify production artifacts before and after the run.
- Refuse output paths targeting production ledgers, archives, configuration, or
  progress history.

## Implementation Slice

1. Add `historical_strategy_lab.py` as a research-only controller over the
   existing immutable strategy and execution contracts.
2. Enumerate every candidate signal and cost-grid outcome once, then apply
   versioned one-trade-per-day policies without provider calls.
3. Produce gate attribution, stop geometry, timing delay, chronological split,
   monthly, bootstrap, drawdown, concentration, and cost-sensitivity reports.
4. Add focused tests for fidelity, policy filtering, deterministic output,
   isolation, and unsafe publication paths.
5. Run the complete corpus, publish compact JSON and Markdown, and create an
   isolated proposal only for hypotheses that survive the declared gates.

## Success And Stop Conditions

The slice succeeds when the tooling is deterministic, all tests and audits pass,
the result fully discloses the tested family, production files remain unchanged,
and the report clearly distinguishes reject, inconclusive, and independent-
confirmation candidates.

The loop stops after this one implementation and validation round. Any true
confirmation run must use newly frozen dates that were not inspected during this
work.

## Executed Result

Completed: 2026-07-18 ET

- Implemented `historical_strategy_lab.py` with exact frozen-evidence hash
  verification, point-in-time production-gate attribution, versioned one-trade
  policies, chronological phases, deterministic bootstraps, concentration,
  stop/notional geometry, and a 240-cell cost/target matrix.
- Evaluated 3,800 candidate-strategy observations over the exact 100 requested
  dates: 95 immutable bundles were available and five remained blockers. The
  final run made zero provider requests and completed in 12.055 seconds after
  immutable production configuration and strategy plugins were loaded once per
  run instead of repeatedly. That is a 13.9% reduction from the first complete
  14.003-second lab pass.
- The current production gate stack selected zero trades. Counterfactuals proved
  this is a learning-label failure, not an absence of market setups: the lab
  found 90 executable ORB signals and trade-capable reversal policies.
- Only `reversal-early-strength` and `reversal-early-earnings` passed the
  tightened research gate, including positive retrospective validation and
  holdout, a positive one-sided 90% bootstrap lower bound, and PF at least 1.20
  with drawdown at most 6R at both 10 and 20 bps per side.
- `reversal-early-earnings` is the preferred independent-confirmation contract:
  67 trades, +21.110R, PF 1.683, 4.528R maximum drawdown, and +9.813R/PF 1.282
  at 20 bps per side. This is not independent evidence.
- Production compatibility failed: its median structural stop was 2.088%, only
  four of 67 stops fit 0.8%, and the current 0.25% risk budget plus reserve
  implies 11.4% median notional. No production strategy rule changed.
- The score and RVOL selectors failed severe-cost robustness, the tight-stop
  early ORB lost money, VWAP pullback was rejected, and HOD continuation was
  deprioritized. This prunes three unproductive learning branches.
- Published the complete machine result and human review under
  `research_results/2026-07-18-production-aware-strategy-lab.*`. A separate
  confirmation contract records the next evidence gate without creating a
  cadence-bypassing production strategy proposal.
