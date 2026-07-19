# Feature Backlog And Implementation Record

Sparse feature ideas are expanded here into explicit acceptance criteria before
implementation. Risk-bearing behavior must remain inside `AGENTS.md`; a backlog
item never grants live-trading authority or permission to weaken safety gates.

## Completed 2026-07-19

### 16. Faithful Dynamic 09:35 Scanner Replay

Status: `dataset-production-scanner-replay-2026-07-19-v4` is independently
inspected `READY`; production strategy rules remain frozen.

- Collected all 118 frozen Alpaca raw SIP sessions without S3, substitution, or
  provider retries and retained every observation in the external canonical
  symbol/year/day store.
- Reconstructed 105,261 point-in-time common-stock evaluations across the exact
  20 dates, admitted 1,228 under the unchanged v3 universe gates, and retained
  389 selected pairs. The nine-name short day was not padded.
- Independently rehashed source artifacts and recomputed security-master
  populations, split factors, ADV, ATR, opening RVOL, dispositions, ranks, and
  shortlist hashes; reconciled 608,386 canonical documents back to source.
- Kept public evidence privacy-safe and aggregate-only. This closes universe
  selection fidelity, not catalyst, quote, execution, outcome, or alpha
  validation.
- Made frozen datasets first-class resumable learning objectives, corrected
  sparse/early-close cache reuse, and removed the terminally rejected reversal's
  unused 1,135-line shadow implementation and tests.
- Follow-on: the exact selected-candidate join and clean-trigger fidelity layers
  are now complete in items 17 and 18. Unchanged-champion evaluation remains
  blocked on the fields listed there.

### 17. Selected-Candidate Bar, News, And Trigger-Tape Join

Status: `dataset-selected-candidate-join-2026-07-19-v1` is inspected `READY`
for development-only pipeline fidelity; production strategy rules remain
frozen.

- Froze the exact 389 scanner-selected security-date pairs privately and
  published only hashes, dates, and aggregate counts.
- Passed a storage pilot with a 10 GiB reserve, then collected 389 candidate
  and 40 benchmark one-minute SIP sessions plus bounded news contexts.
- Located 325 crossing windows and retained 365,379 raw trades and 97,949
  quotes only around those windows; no full-session tick mirror was created.
- Measured 303 three-snapshot windows, 292 basic fresh/uncrossed windows, 177
  post-observation chase-cap passes, and a 0.1318% median usable spread.
- Hardened replay rules so bar highs, secondary news, top-of-book sizes, and
  current metadata cannot impersonate exact triggers, verified catalysts, full
  depth, or point-in-time truth.
- Follow-on: item 18 now closes dated CIK, bounded SEC primary-source candidate,
  and clean-condition mechanics. Directional catalyst classification and the
  remaining champion inputs are still required before 100+ new dates.

### 18. Point-In-Time Primary Sources And Clean SIP Triggers

Status: `dataset-selected-candidate-fidelity-2026-07-19-v1` is independently
inspected `READY` for development-only pipeline fidelity; production rules
remain frozen.

- Mapped all 389 frozen pairs to 283 dated issuer CIKs without current-identity
  backfill; 349 used ticker plus share-class FIGI and 40 used a unique dated
  ticker because the source row had no usable FIGI.
- Cached 283 SEC submissions documents and 100 unique primary filing documents
  externally. Eighty-one pairs had a time-valid filing candidate, including 18
  Item 2.02 candidates, and eight dilution conflicts were detected.
- Kept every filing unclassified and `verified_positive_catalyst=false`; primary
  source presence is not a favorable catalyst default.
- Transcribed Alpaca's tape-specific strictest-condition minute-bar rules and
  froze a narrower continuous-regular-cross contract with exhaustive tests.
- Replayed all 325 crossing windows: 197 first raw crosses were invalid, 195
  clean triggers moved later, only 155 passed the final chase cap, and median
  usable spread remained 0.13001%.
- Next: source-grounded directional classification for the 80 material primary
  candidates and explicit resolution of halt/tradability, resistance, sector,
  and depth inputs before champion returns or new-date alpha collection.

### 19. Champion Catalyst Classification And Official Halt History

Status: `dataset-champion-input-fidelity-2026-07-19-v2` is independently
inspected `READY` for development-only pipeline fidelity; production rules
remain frozen.

- Re-read 100 SEC complete submissions and enforced exact prior-session-close
  recency for 106 filing observations without observing target outcomes.
- Reduced the 389-pair corpus to 12 verified material catalyst pairs: 11 remain
  directionally unresolved and one has a high-precision positive issuer phrase.
  Six pairs are hard conflict rejects; 351 have no recent SEC primary source.
- Retained unrecognized filings and non-SEC event types as unresolved rather
  than using filing presence, document materiality, or secondary news as a
  favorable default.
- Collected and reparsed 966 official Nasdaq historical halt rows for all 20
  dates. None overlapped the 325 clean-trigger observation windows.
- Explicitly separated the historical active-listing/no-halt proxy from
  broker-specific tradability, which requires prospective broker qualification.
- Preserved the failed v1 source-shape contract and superseded it without date,
  pair, rule, or outcome substitution.
- Next: build an outcome-blind unchanged-champion readiness matrix for
  resistance, structural stop/noise, benchmark-relative strength, and
  conservative executable liquidity before exposing returns.

### 20. Outcome-Blind Unchanged-Champion Readiness

Status: `dataset-champion-input-readiness-2026-07-19-v1` is independently
inspected `READY` for development-only readiness evidence; production rules
remain frozen and no target outcomes were read.

- Rejoined all 389 exact pairs against catalyst, clean-trigger, quote, spread,
  chase, official halt, visible liquidity, completed-bar VWAP, SPY/QQQ, prior
  split-adjusted high, and opening-range stop inputs.
- Corrected the historical chase fidelity count from 155 upper-cap-only passes
  to 101 engine-compatible passes that also keep the final ask at or above the
  opening high.
- Found 53 non-catalyst execution-geometry passes and 44 that also pass the
  completed-bar market diagnostics.
- Found only 11 opening-low/0.10-ATR stop proxies inside the 0.8% cap and only
  50 known-overhead resistance proxies with at least 2.2% room; zero pairs pass
  every measured non-catalyst proxy together.
- Confirmed that the single verified positive primary-catalyst pair fails both
  spread and chase, leaving zero resolved hard-gate survivors before outcomes.
- Next: specify real-time structural invalidation/noise and technical resistance
  without tuning these dates, then freeze 100+ new scanner dates with direct
  catalyst evidence and evaluate unchanged v3 first.

## Completed 2026-07-18

### 12. Bounded Edit And Learning Loop

Status: implemented by `LEARNING_PROGRAM.md`, `LEARNING_LOOP.md`, the public
`learning/` registries, `learning_loop.py`, `learning_cadence.py`, and the
evidence/data/statistics modules on 2026-07-18. External scheduler installation
remains an explicit operator action.

- Publishes a versioned eight-phase prompt subordinate to `AGENTS.md`: safety,
  ten working controls, bottleneck proof, at most five candidates, adversarial
  filtering, one apply slice, validation/benchmarking, and durable recording.
- Adds an explicit `learning` session mode with no broker authority and no
  automatic strategy activation.
- Inventories and preserves the dirty worktree, reads batch/evidence/research
  telemetry, and distinguishes cold data acquisition from fast local research.
- Validates machine-readable change plans, supports a deliberate no-op, refuses
  external/broker actions and `strategy_config.toml` edits, requires proposals
  to remain isolated, and caps the apply loop to one round.
- Keeps generated run and cadence state under ignored `learning_runs/`, while
  datasets, complete experiment families, negative results, and three-axis
  strategy evidence remain public and append-only.
- Adds selection-aware daily account statistics, point-in-time security and
  data-claim contracts, bounded hypothesis invention, paired champion/challenger
  evidence, degradation monitoring, and a finite daily-to-quarterly cadence.
- Tests cover prompt and authority boundaries, resume and lock behavior,
  evidence identity, no-lookahead/selection controls, strategy-transition
  refusal, no-op behavior, privacy, progress, and non-recursion.

### 13. Cross-Date Contract Resolution Cache

Status: implemented in `ibkr_historical.py` and `historical_universe.py` on
2026-07-18.

- Caches IBKR contract detail results by exact STK request identity with an
  integrity hash, atomic writes, public metadata classification, and telemetry.
- Resolved results expire after 30 days by default; error-200 unresolvable
  results expire after one day; transport, permission, pacing, and timeout
  failures are never cached.
- Concurrent same-symbol probes single-flight, process memory avoids repeated
  disk reads, and `--fresh-contracts` forces a current provider refresh.
- The 100-day evidence showed contract lookup is secondary, not primary: 29.161
  of 14,049.332 summed preflight request-seconds. The durable learning priority
  is local corpus reuse, then request elimination, then new collection only for
  missing coverage or confirmation.

### 14. Production-Aware Counterfactual Strategy Lab

Status: implemented by `historical_strategy_lab.py` and documented in
`HISTORICAL_RESEARCH.md` on 2026-07-18. Production rules remain frozen.

- Converts every executable stored signal into a research outcome while
  retaining the current production evaluator's exact rejection reasons. This
  restores labels and marginal gate evidence even when the complete production
  stack never trades.
- Verifies the public evidence manifest, exact scanner/candidate frozen hash,
  ordered symbols, full session/source attestations, bundle hashes, and
  production artifact isolation before publishing a result.
- Tests 15 predeclared one-trade policies across 0/5/10/20 bps per-side costs
  and 1/1.5/2/3R targets, with chronological stability phases, 20,000-sample
  base bootstraps, drawdown, concentration, stop geometry, and implied notional.
- On the 95-date usable corpus, only simple early reversal strength and the
  Item 2.02 earnings subset survived the severe-cost research gate. The
  earnings subset retained +9.813R and PF 1.282 at 20 bps per side but remained
  production-incompatible because its median structural stop was 2.088% and
  implied median notional was 11.4% at the current risk budget.
- Rejects tight-stop early ORB and the current VWAP-pullback definition,
  deprioritizes HOD continuation, and refuses to treat score/RVOL selection
  variants as winners after their 20 bps robustness failure.
- Freezes a public independent-confirmation contract without bypassing the
  production strategy-review cadence, editing `strategy_config.toml`, or
  making broker/provider calls.

### 15. Independent Reversal Confirmation And Retired Shadow Path

Status: historical confirmation completed and failed on 2026-07-18. The
challenger is `RETIRED`; the unused prospective shadow implementation was
removed, its Git history and evidence remain auditable, and production rules
remain frozen.

- Freezes a public hash-addressed manifest before target-session collection.
  It embeds at least 100 new dates, complete ordered Item 2.02 evidence,
  previously inspected run/date identities, the sole policy and plugin,
  execution grid, acceptance thresholds, deployment assumptions, production
  baseline, preregistration time, and implementation hashes.
- Rejects inspected-date overlap, altered manifests or implementations,
  pre-preregistration captures, phase relabeling, candidate reordering, date or
  symbol substitution, and any attempt to supply another policy or grid.
- Evaluates the independent sample at 5/10/20 bps and 1/1.5/2/3R, publishes
  every requested date/blocker and primary trade/exit, and applies the frozen
  date/signal, expectancy, PF, drawdown, bootstrap, halves, best-five, cost, and
  target gates. Failure stops without retuning.
- Adds structural-stop whole-share deployment at 0.25% account risk plus a 10
  bps reserve, with account compounding/log growth/drawdown, allocation and
  shortfall, stop slippage, binding cap, and largest-five-removal metrics.
  A separate cohort admits only naturally <=0.8% stops and needs 20 trades for
  inference; no stop is tightened and no allocation floor increases risk.
- The frozen 100-date confirmation stopped when the required 80 usable dates
  became mathematically unreachable. Its 22 primary trades returned -10.056R,
  -0.457R mean expectancy, 0.370 profit factor, and 10.492R maximum drawdown;
  every target and cost-stress cell was negative. No shadow sample was started,
  the hypothesis-specific shadow runner and its tests were deleted, and the
  failed contract may not be retuned.

## Completed 2026-07-16

### 11. Parallel Multi-Strategy Historical Research

Status: implemented by `historical_research.py`,
`historical_research_strategies.py`, and `HISTORICAL_RESEARCH.md` on 2026-07-16.

- Reuses complete local daily bundles with zero provider calls and hashes the
  public evidence manifest plus every available/missing bundle into a stable
  dataset identity.
- Binds every builder-produced date bundle to its exact frozen candidate and
  scanner evidence hash; the runner separately rejects ordered symbol mismatch.
- Evaluates versioned research plugins across dates in a bounded process pool.
  Each worker parses one date once for all strategies; one parent writer merges
  deterministic, isolated per-strategy/date shards.
- Enforces no-lookahead by exposing progressively revealed immutable bar
  prefixes and rejecting backdated decisions. All strategies enter on the next
  bar and share slippage, target, stop-first ambiguity, and force-flat rules.
- Makes unsupported requirements explicit instead of approximating absent
  depth, benchmark bars, or subminute data.
- Keeps generated shards outside Git and experimental signals outside the
  production ledger, trade archive, configuration, and maturity calculations.
- Produces a compact auditable result with per-strategy metrics and paired
  date-level comparisons. Tests prove worker-count determinism and production
  artifact isolation.

## Completed 2026-07-15

### 1. Daily Active Context And Date-Partitioned Archives

Status: implemented by `trade_lifecycle.py` and the lifecycle rules in
`AGENTS.md`.

- `trades/active/` contains only visible Markdown context for in-progress work on
  the current ET day. Historical mode may temporarily use a past date only while
  its validated replay is running and must leave no active files afterward.
- Rejected/stale ideas, flat trades, no-trade sessions, and completed sessions
  are terminal and move immediately to `trades/archived/YYYY_MM_DD/`.
- The lifecycle audit rejects stale or misnamed active context, terminal context
  left active, malformed archive folders, date mismatches, duplicate public
  context IDs, and archives without valid outcomes.
- Archive collisions fail closed; an existing day is never overwritten or used
  for a second historical simulation.

### 2. Terminal Outcome Dataset

Status: implemented by the embedded outcome schema in `trade_lifecycle.py`.

- Closing requires a concise result, primary reason, thesis result, what worked,
  what failed, at least one lesson, next-time actions, and relevant public
  metrics.
- The close operation adds both a readable Markdown review and the canonical JSON
  outcome to the same context before moving it. This keeps narrative and learning
  data together instead of maintaining a drift-prone side index.
- New outcomes must match the current strategy version/rules hash and cannot
  contain UUIDs, plaintext broker/account identifiers, secrets, or non-finite
  values. Historical outcomes remain auditable after later strategy versions.
- `STRATEGY_LEARNING.md` documents the schema, close command, audit, correction,
  and publishing workflow.

### 3. Evidence-Based Strategy Learning

Status: implemented safely by `strategy_learning.py`.

- Reports combine canonical ledger/maturity metrics, paired project-versus-EOD
  exits, archived outcome reasons, and bounded feature-cohort diagnostics.
- A review proposal is allowed only after both 20 new closed frozen-rule signals
  and 30 calendar days since version start or the last review.
- Generated changes are hypotheses for an evidence-backed delegated decision.
  The tool has no apply command and never edits `strategy_config.toml` or
  `AGENTS.md`.
- Any accepted change requires a separate production-change workflow, a new
  strategy version and rules hash, preserved prior sample, and preregistered
  confirmation evidence.
  This resolves the risk in automatic self-modification without discarding the
  requested continuous-learning capability.

### 4. Historical Learning Mode

Status: implemented by `historical_learning.py` and documented in
`HISTORICAL_LEARNING.md`.

- The workflow asks for a day count, randomly selects completed exchange trading
  days not already represented by an archive folder, retains the seed, and asks
  for an additional count after the requested batch.
- Each point-in-time replay bundle requires at least ten candidates; complete,
  non-interpolated regular-session minute bars; split adjustment; time-valid
  catalysts; full-universe capture; and historical quote/depth snapshots. Missing
  fidelity is a blocker rather than permission to invent data.
- Every candidate is evaluated with the frozen production engine. At most one
  trade is soft-executed, using earliest trigger and deterministic tie breaks.
  Later eligible signals are retained as daily-limit misses and excluded from
  return metrics.
- Replay is conservative and no-lookahead: same-bar stop/target ambiguity resolves
  stop-first, +2% runner and 3:50 force-flat rules are respected, and paired EOD
  shadow results are recorded.
- Candidate/session context is created in `trades/active/` during replay, then all
  records are outcome-completed, archived, and atomically appended to the signal
  ledger as a validated batch. Historical mode never calls broker order tools.
- Normal batches use ready-only replay: every valid date from the original
  selection runs immediately, missing dates remain explicit blockers, archived
  dates are idempotently skipped, and no substitute date is introduced.
- Atomic public batch status tracks selected, ready, replayed, blocked, and
  already completed dates. The engineering target is at least 80% validation-
  grade yield across 20 random dates with zero substitutions and cascade errors.

### 5. Agentic Session Mode Selector

Status: implemented by `session_mode.py` and the startup rules in `AGENTS.md`.

- Every new agentic trading workflow has five explicit choices: live,
  current-day shadow, historical replay, strategy review, or bounded repository
  learning. When one mode is clear, Codex selects it and continues without a
  redundant prompt.
- The numbered CLI also accepts stable named modes for automation and returns a
  machine-readable selection plus the next required safety step.
- Selection is declarative. Only live mode permits broker actions; no mode choice
  bypasses account, review, confirmation, evaluator, guard, lifecycle, or privacy
  rules. Shadow, historical, review, and learning modes cannot place/cancel
  orders or silently apply strategy changes.

### 6. Optional Interactive Brokers Historical Data Adapter

Status: implemented by `ibkr_historical.py` and documented in
`HISTORICAL_LEARNING.md`.

- Connects only to an authenticated local TWS/IB Gateway socket and exposes no
  account, portfolio, order, or execution methods.
- Collects regular-session historical bars, time-matched opening-volume
  lookbacks, daily bars, and historical top-of-book bid/ask ticks with sizes.
- Produces three timestamped, strategy-shaped quote snapshots for a recorded
  evaluation time and preserves explicit IBKR feed limitations.
- Uses ignored `.env` connection settings and requires no API private key or
  account identifier.
- Fails with an actionable TWS startup/login/API-socket message when the local
  service is unavailable.
- Classifies retryable transport/provider failures separately from permanent
  fidelity gaps, aborts a disconnected request stream immediately, reconnects
  once by default, and resumes from atomic per-provider caches.
- Optional Massive SIP and Alpaca adapters follow IBKR in the canonical
  provider chain. Retryable and permanent failures may advance, but each
  candidate is recollected from one provider; field/feed/adjustment provenance
  remains explicit and missing trade intervals are never filled.

### 9. Pre-Freeze Historical Symbol Viability

Status: implemented by `historical_universe.py` and the `probe` surface in
`ibkr_historical.py`.

- Historical discovery now produces a ranked draft pool with spare candidates
  before the final universe is frozen.
- `historical_discovery.py` automates the high-market-cap earnings/SEC lane:
  connector results are normalized, filings are restricted by point-in-time
  acceptance, strong primary-document dilution language is screened, and all
  source artifacts are resumably cached outside Git. Large runs retain an
  80-name reserve because a 40-name reserve was empirically too shallow for the
  immutable ADV/ATR gates.
- SEC registrants are deduplicated to one deterministic representative ticker
  before preflight. This removes preferred/depositary siblings from the ranked
  pool; IBKR `stockType=COMMON` remains the authoritative final proof.
- Before provider calls, the preflight rejects draft rows that are not explicitly
  common stock or already carry a dilution conflict. It then resolves IBKR stock
  contracts and evaluates prior daily ADV/ATR gates before verifying 14 positive
  prior opening-volume sessions, all without requesting target-session prices.
  A retired, unresolvable, ineligible, or input-incomplete symbol can be skipped
  in favor of the next ranked buffered name without using future performance to
  select the sample.
- Only symbol-scoped failures are skippable. Permissions, pacing, connection,
  and provider-wide failures still stop the batch instead of silently shrinking
  or distorting the universe.
- `--continue-on-exhausted` records one sparse date as blocked and continues
  later frozen dates without substitution. Strict fail-fast remains the default.
- Contract proof now requires IBKR `stockType=COMMON`; the pre-session cache
  contract was bumped so older entries without this proof cannot be reused.
- The frozen evidence manifest records accepted, skipped, and unused buffered
  symbols plus draft, qualification, and accepted-history hashes. After this
  point, the builder never replaces a candidate based on observed market data.
- Preflight now checkpoints every examined symbol/date under ignored storage,
  resumes exact reruns from a versioned cache, and streams progress. Only names
  that pass daily gates request opening history; the fixed 28-day window fits in
  one five-minute HMDS chunk while still proving all 14 required prior sessions.
- Accepted pre-session opening and daily bars are shared with bundle collection,
  eliminating duplicate provider requests. Cache identity and the no-target-
  price attestation are validated before reuse; cache misses use normal IBKR
  collection.
- A 2026-07-16 ten-date measurement completed 102 buffered preflight checks in
  736.5 seconds and repeated the exact cached pass in 1.13 seconds. Keep tracking
  cold-run, resume, bundle-collection, and blocked-date timing on larger batches.
  The daily-first/one-chunk optimization landed after this baseline and still
  needs a new cold-run benchmark.
- The 2026-07-16 throughput pass added bounded rank-ordered workers, global
  pacing reservation, provider telemetry, and daily-gate-first request
  elimination. On the March 30 graph, explicit contract lookups now apply only
  to the ten daily-gate survivors, reducing planned cold calls from 118 to 74.
  Initial concurrent trials ran inside an IBKR soft-throttle window and are not
  clean speedup evidence; retain worker count and request telemetry on the timed
  100-day run and its exact cached rerun.

### 10. Durable Progress History And Contribution Hook

Status: implemented by `progress_history.py`, `progress/HISTORY.jsonl`, and the
tracked `.githooks/pre-commit` hook.

- Meaningful breakthroughs, diagnosed failures, provider constraints,
  architecture decisions, and reusable workflow improvements have a structured,
  append-only home outside transient conversation history.
- The history audit validates IDs, timestamps, required findings/impact, unique
  entries, and repository-relative file references.
- Substantive staged changes require a new history contribution. Mechanical,
  generated-ledger, and test-only commits are exempt; an explicit bypass exists
  only for truly lesson-free maintenance.
- The first record captures the five-day replay pass: A/B/C market-data
  entitlements, the required TWS restart, resumable caching, the retired `SEMR`
  blocker, and the legitimate flat `LOB` opening bar.

## Outstanding

Items below are design-ready backlogs, not authority to weaken live-trading or
historical-fidelity rules. New ideas should include scope, safety boundaries,
data contracts, acceptance tests, and publishing behavior before implementation.

### 7. Valuable Data Cache

Status: core per-symbol/day market-data scope implemented on 2026-07-18 by
`historical_store.py`, `historical_service.py`, `historical_data_cli.py`, and
`historical_migration.py`. See `HISTORICAL_DATA_STORE.md`. Point-in-time web/SEC
cache lifecycle and optional pruning/reporting remain separate enhancements.

The implemented content-addressed cache for expensive, slow, or rate-limited
market-data inputs lets future work reuse verified evidence without confusing
provider feeds. Future web/SEC extensions should follow the same principles and
add equivalent lifecycle handling without confusing old data with current
truth.

Acceptance criteria:

- Every entry records source/provider, request identity, as-of time, retrieval
  time, schema version, producer version, content hash, privacy class, and
  freshness/immutability policy.
- Point-in-time historical artifacts are immutable. Current/live responses have
  explicit expiry and can never be described as current after expiry.
- Writes are atomic and integrity-audited; corrupt, partial, schema-incompatible,
  or provenance-free entries fail closed and are recollected when safe.
- Cache hits explain exactly what was reused and why it is still valid. A caller
  can demand fresh collection for drift-prone or safety-critical facts.
- Provide `audit`, `inspect`, and `prune` commands plus bounded retention and
  size reporting. Raw private inputs remain ignored and public summaries remain
  safe to commit.
- Never cache credentials, MFA material, session cookies, plaintext account or
  broker identifiers, or mutable live broker state.
- Extend the implemented historical market-data store to authoritative calendars
  and time-valid catalyst evidence, with deterministic tests for expiry and
  schema migration in those mutable/current data classes.
