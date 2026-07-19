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

## Post-Execution Correction

Updated: 2026-07-19 ET

The independent confirmation described above is no longer pending. Its frozen
100-date contract stopped when the required 80 validation-grade dates became
unreachable. The 31 usable dates produced 22 trades; at the primary 5 bps per
side and 2R setting they lost 10.056R, with -0.457R mean expectancy, 0.370
profit factor, and 10.492R maximum drawdown. Every target and cost-stress cell
was negative. The early Item 2.02 reversal is therefore `RETIRED`; it must not be
sent to shadow qualification or repaired by tuning the failed sample.

This result also changes the interpretation of the 95-bundle development
corpus. It remains valuable for falsification, implementation tests, and
counterfactual gate diagnostics, but its catalyst-derived ten-name universes
cannot answer whether the production scanner would have selected the same names
at 09:35. New mechanism invention on that corpus remains locked until the
dynamic point-in-time scanner replay is complete and independently inspected.

## Revised Evidence Sequence

The shortest credible path toward explosive compounding is now the following
ordered sequence. Skipping a stage would create faster backtests but weaker
evidence and a greater chance of compounding a false edge.

### Stage 1 - Close selection fidelity

Status: completed and independently inspected on 2026-07-19.

The frozen 20-date dynamic 09:35 scanner replay reached `READY` with all
118 hash-attested source sessions, the exact point-in-time common-stock master,
unchanged rules and dates, all 20 rankings, and an independent reconstruction of
the master population, ADV, ATR, opening RVOL, split factors, dispositions, and
top-20 identities. It retained 105,261 security-date evaluations, 1,228 eligible
rows, and 389 selected pairs; the one nine-name day was not padded. All 608,386
canonical documents were reconciled to source. This stage measures universe
selection only; it did not earn an alpha-rule change.

### Stage 2 - Build a trade-capable selected-candidate join

Status: pipeline join plus primary-source/clean-trigger fidelity completed and
independently inspected on 2026-07-19; production-input qualification remains
blocked.

The hash-frozen development manifest retained the exact 389 pairs. All candidate
sessions and 40 SPY/QQQ sessions have raw Alpaca SIP one-minute bars, all pairs
have bounded news-discovery contexts, and all 325 pre-cutoff crossing windows
have raw SIP trade tape plus bounded quote collection. The tape contains 365,379
trades and 97,949 quotes. Only 303 crossings had three snapshots, 292 had basic
fresh/uncrossed snapshots, and 177 remained inside the 0.15% chase cap after the
observation interval. Median usable spread was 0.1318%, above the 0.10%
production operating limit.

This completes the reusable acquisition mechanics but not a production replay.
Alpaca/Benzinga news remains secondary evidence. The follow-on frozen fidelity
dataset mapped all 389 pairs to dated CIKs, retained 100 SEC primary documents,
and separated minute-high eligibility from a clean continuous regular-sale
cross. Of 325 first raw crosses, 197 failed the clean contract; condition-valid
timing reduced chase-cap passes from 177 to 155. Eighty-one pairs had SEC filing
candidates and eight had dilution conflicts.

The next frozen layer read the SEC complete submissions and issuer exhibits,
enforced acceptance after the prior completed-session close, and joined official
Nasdaq halt history. Only 12 pairs had a verified material recent primary
catalyst; 11 remained directionally unresolved, one had verified positive
direction, and six were conflict rejects. None of the 325 clean-trigger windows
overlapped one of the 966 official halt records. This closes historical halt
state but not broker-specific historical tradability, which cannot be recreated
and must remain a prospective execution-qualification check. Top-of-book is not
a full ladder. Resistance, structural invalidation/noise, benchmark-relative
strength, and conservative executable-liquidity contracts remain incomplete.
Missing fields block rather than default. See `SELECTED_CANDIDATE_JOIN.md`,
`SELECTED_CANDIDATE_FIDELITY.md`, and `CHAMPION_INPUT_FIDELITY.md`.

The storage-capacity preflight measured bytes per selected pair from a
representative pilot, published projected incremental bytes and request count,
and required enough free space for the projection plus an atomic-write/audit
reserve. The current volume is already 96% allocated even
though the canonical history store is only about 2.2 GB. A full-universe
one-minute pull is therefore wasteful and unsafe. Stage 2 collected only the
frozen selected pairs, benchmarks, and bounded trigger tape, while enforcing a
10 GiB reserve and exact cache resume.

### Stage 3 - Qualify unchanged-champion inputs before outcomes

Status: completed and independently inspected on 2026-07-19; no target-session
return was read and unchanged-champion outcome evaluation remains blocked.

Build one deterministic readiness matrix over the exact 389 frozen pairs. It
must consume only source data available at the clean trigger, publish aggregate
attrition, preserve an unresolved value instead of a favorable default, and
bind every derived-input implementation hash. At minimum it must determine:

- recent verified catalyst disposition from the frozen primary-source layer;
- clean trigger, three bounded snapshots, spread, and final chase distance;
- official historical halt proxy with broker tradability kept prospective;
- conservative quantity capacity from visible best-ask size and the prior
  completed real one-minute volume, without calling top-of-book a full ladder;
- candidate strength relative to SPY and QQQ using only completed bars;
- known overhead resistance from pre-session split-adjusted history;
- whether a structural invalidation and stop-noise contract can be reproduced
  without inventing new thresholds.

The inspected matrix found 53 non-catalyst execution-geometry passes, 44 that
also pass completed-bar market diagnostics, only 11 opening-low/0.10-ATR stop
proxies inside the 0.8% cap, and zero pairs passing every measured non-catalyst
proxy. The single verified positive catalyst pair fails both spread and chase,
so zero pairs survive the resolved hard-gate cascade. The chase reconstruction
also corrected the earlier 155 upper-cap-only count to 101 engine-compatible
passes by requiring the final ask to remain at or above the opening high.

The matrix passed independent pipeline inspection. A subsequent frozen
source-oracle validation closed intraminute VWAP semantics: 325 of 325 raw-trade
reconstructions matched the provider's OHLCV/count and WAP, with zero
unsupported conditions. Because WAP-eligible volume differed from reported
volume in every crossing minute, future trigger-time VWAP must use the
condition-aware raw trade prefix described in `SIP_BAR_AGGREGATION.md`.

The separately frozen structure layer in `PREENTRY_STRUCTURE.md` then closed
the stop/noise and resistance definition gap without outcomes. It completed all
249 identity-history requests and 325 exact premarket windows, retained 15
same-symbol history gaps as unresolved, and independently reconstructed all 325
terminal records. Of 255 derivable records, 153 fit the unchanged 0.8% stop cap
and 89 pass both unchanged stop and resistance geometry. This replaces the
opening-low diagnostic proxy; it does not validate the definition's alpha or
earn a production change. Sector evidence and broker-specific tradability remain
unresolved or prospective. The exact 20 development dates still may not support
an alpha or promotion claim. See `CHAMPION_INPUT_READINESS.md` and
`PREENTRY_STRUCTURE.md`.

On at least 100 previously uninspected dynamic scanner dates, retain every
selected candidate, every trigger, every production rejection reason, and a
paired paper-aligned end-of-day outcome. Apply the frozen VWAP and structure
contracts without editing them after selection. Report both the one-trade daily
portfolio and all-signal gate attribution. It may still conclude that zero or
too few pairs are evaluable. That would be an evidence result, not permission to
loosen gates. The first 20 scanner dates validate the pipeline and expose gross
selection behavior; they are not enough for production promotion.

The expansion campaign in `SCANNER_EXPANSION.md` completed and independently
inspected exactly those 100 new H1-2026 dates with seed `20260719` after
excluding every v4 target. It has zero date overlap and no substitutions. The
133-session graph reused only 113 compatible, hash-attested inputs, requested
2,147 exact new-universe symbol deltas, and collected 20 fresh full-universe
sessions. Independent reconstruction verified 526,587 evaluations and selected
1,987 eligible security-date pairs. Reuse remains an acquisition optimization,
not additional alpha evidence.

Step 1 below is now complete: `SCANNER_SELECTED_PAIRS.md` and manifest
`369efda1...22c140b` freeze those exact 1,987 private pairs before any richer
field or target outcome is read. The research lock now requires independently
inspected unchanged-v3 evaluation of that contract before strategy invention.
The base acquisition contract in `SCANNER_EXPANSION_JOIN.md` is also frozen as
manifest `15d8baef...e2f6b`. It reuses the byte-identical, hash-bound 389-pair
collector through a small dataset adapter instead of duplicating or modifying
completed evidence code. Its independently inspected collection completed all
1,987 candidate sessions and 1,460 raw trigger tapes with zero errors. Only
1,331 trigger windows had all three causal quote snapshots, and the observed
median snapshot spread was about 0.153%. Primary-catalyst verification,
condition-aware trigger inspection, gate attrition, and every return field
remain downstream and frozen off. `SCANNER_EXPANSION_FIDELITY.md` now freezes
the next stage as manifest `ee9ba6f3...c780ce`: exact dated listing-to-CIK
mapping for 656 unique CIKs, bounded SEC primary documents, and the unchanged
condition-aware clean-cross contract, still with no outcome access.
An explicit no-quote response exposed a batch-fatal assumption in that frozen
collector. `CLEAN_TRIGGER_FIDELITY_EXPANSION.md` now isolates the repair under
manifest `44225398...e4d6ad4`: every ordered trigger is checkpointed, missing
NBBO is retained as gate attrition, and only true collection errors are retried.
That corpus is now independently `READY`: all 1,460 windows classified with
zero collection errors, four missing-NBBO blockers, 1,267 preliminary
fresh/uncrossed windows, and 726 chase-cap passes. Only 569 first raw price
crosses were clean, confirming that condition-aware timing materially changes
the executable trigger without earning a rule revision.
`CHAMPION_INPUT_FIDELITY_EXPANSION.md` now freezes the next manifest
`043f21e4...a7b46a`, combining all 1,987 SEC candidate records with the 1,460
clean-cross windows for conservative primary-direction classification and
official Nasdaq halt state, still before any return field.

After all 100 rankings pass independent reconstruction, follow this decision
protocol without changing the order:

1. Freeze the exact selected pairs and staged acquisition contracts before
   trigger or return fields are read. The selection and base join are frozen;
   direct primary-catalyst and condition-aware trigger contracts remain next.
2. Apply the existing clean-trigger, condition-aware prefix VWAP, stop/noise,
   resistance, benchmark, spread, chase, halt, and conservative-capacity
   contracts unchanged. Missing inputs block and must not abort unrelated rows.
3. Publish gate attrition before returns. If fewer than 20 closed unchanged-v3
   signals survive, report inadequate deployment capacity; do not loosen a gate
   to manufacture a backtest. A later, disjoint preregistered acquisition may
   enlarge the sample.
4. If at least 20 signals survive, evaluate the one-trade-per-day portfolio and
   all-signal attribution after spread/slippage costs. Keep chronological folds,
   daily clustering, log-equity growth, drawdown, profit factor, and bootstrap
   uncertainty visible together.
5. Preserve v3 when evidence is positive or inconclusive. Only a clearly
   diagnosed failure may register one mechanism-level proposal, which must then
   face a new disjoint confirmation contract before production consideration.

### Stage 4 - Earn at most one rule revision

A rule may be tightened, removed, or otherwise revised only when the frozen
champion sample contains enough closed outcomes to estimate its marginal effect,
the hypothesis is preregistered before a new confirmation slice, all tested
alternatives are disclosed, and the result survives chronological, cost,
concentration, drawdown, and bootstrap checks. One accepted mechanism creates a
new strategy version and resets its evidence sample. A no-op is the correct
decision when the evidence does not distinguish a change from noise.

### Stage 5 - Qualify execution separately

Positive historical alpha is insufficient. The unchanged candidate and order
logic must collect prospective shadow evidence for observation delay, executable
spread/depth, chase-cap fills, protection latency, stop slippage, and monitoring
continuity. Live pilots remain limited by the maturity rules in `AGENTS.md` and
`strategy_config.toml`; alpha evidence cannot stand in for broker-path evidence.

## Why This Sequence Supports Geometric Growth

With one concentrated trade per day, a biased shortlist corrupts every
downstream expectancy estimate. Closing that bias first makes later work slower
once but reusable indefinitely through the canonical local cache. Measuring
paired net-R and log-equity outcomes then directs effort toward rules that improve
the entire distribution, not merely win rate or one attractive backtest. Frozen
families, explicit negative results, and independent confirmation reduce the
multiple-testing penalty; spread, slippage, capacity, drawdown, and protection
gates prevent a paper edge from being mistaken for deployable compounding.
