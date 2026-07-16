# Feature Backlog And Implementation Record

Sparse feature ideas are expanded here into explicit acceptance criteria before
implementation. Risk-bearing behavior must remain inside `AGENTS.md`; a backlog
item never grants live-trading authority or permission to weaken safety gates.

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
- Generated changes are hypotheses for explicit human review. The tool has no
  apply command and never edits `strategy_config.toml` or `AGENTS.md`.
- Any accepted change requires Ryan's explicit approval, a new strategy version
  and rules hash, preserved prior sample, and preregistered confirmation evidence.
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

- Every new agentic trading workflow presents four explicit choices: live,
  current-day shadow, historical learning, or strategy review.
- The numbered CLI also accepts stable named modes for automation and returns a
  machine-readable selection plus the next required safety step.
- Selection is declarative. Only live mode permits broker actions; no mode choice
  bypasses account, review, confirmation, evaluator, guard, lifecycle, or privacy
  rules. Historical and review modes cannot place/cancel orders or silently apply
  strategy changes.

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
- An optional Massive SIP adapter supplies adjusted regular-session aggregates
  and historical NBBO evidence only for permanent primary-provider gaps. It
  retains field/provider provenance and never fills missing trade intervals.

### 9. Pre-Freeze Historical Symbol Viability

Status: implemented by `historical_universe.py` and the `probe` surface in
`ibkr_historical.py`.

- Historical discovery now produces a ranked draft pool with spare candidates
  before the final universe is frozen.
- The preflight resolves IBKR stock contracts and verifies 14 positive prior
  opening-volume sessions plus 15 prior daily sessions without requesting
  target-session prices. A retired, unresolvable, or input-incomplete symbol can
  be skipped in favor of the next ranked buffered name without using future
  performance to select the sample.
- Only symbol-scoped failures are skippable. Permissions, pacing, connection,
  and provider-wide failures still stop the batch instead of silently shrinking
  or distorting the universe.
- The frozen evidence manifest records accepted, skipped, and unused buffered
  symbols plus a hash of the draft. After this point, the builder never replaces
  a candidate based on observed market data.

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

Status: partially satisfied by resumable atomic IBKR and Massive namespaces in
`historical_bundle_builder.py`; content addressing, lifecycle tooling, and
cross-workflow cache policy remain outstanding.

Build a content-addressed cache for expensive, slow, or rate-limited public and
market-data inputs so future work can reuse verified evidence without confusing
old data with current truth.

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
- Integrate the historical IBKR raw cache first, then authoritative calendars
  and time-valid catalyst evidence, with deterministic tests for hits, expiry,
  corruption, schema migration, and concurrent writers.

### 8. Code Review, Cleanup, And Improve

Status: planned. The progress-history checkpoint now supplies its durable input,
but the review mode itself is not implemented.

Create a repository-review mode driven by a versioned top-level prompt that
turns recent diffs, tests, audits, TODO items, and progress findings into a
repeatable inspect-propose-apply-validate loop.

Acceptance criteria:

- The top-level review prompt is public, versioned, testable, and subordinate to
  `AGENTS.md`, tool requirements, privacy rules, and the active user request. It
  cannot broaden broker or external-write authority.
- The mode starts read-only, inventories the current diff and validation state,
  and emits a scoped review plan before mutation. It never runs while exposure
  or another safety-critical workflow needs attention.
- Safe repository cleanup and test improvements may use normal implementation
  authority. Production strategy changes remain proposals requiring Ryan's
  explicit approval, a new strategy version, and the existing evidence gates.
- End-of-run invocation is conditional on meaningful repository work and a clean
  safety state; it is skipped for urgent live management, simple read-only
  answers, generated-only updates, and explicit user opt-out.
- Every applied improvement passes relevant tests/audits, updates the progress
  history when it yields a reusable lesson, and is committed/pushed under the
  normal publishing rules.
- Tests cover prompt loading, instruction precedence, no-op runs, dirty-worktree
  preservation, strategy-change refusal, progress contribution, and failure
  recovery without an infinite self-edit loop.
