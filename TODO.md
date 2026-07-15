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

## Outstanding

No feature from the current backlog remains unimplemented. New ideas should be
added here with scope, safety boundaries, data contract, acceptance tests, and
publishing behavior before implementation.
