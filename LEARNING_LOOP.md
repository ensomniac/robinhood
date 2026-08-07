# Neutral Learning Loop

Contract version: `2026-08-07-v1`

The clean-slate learning loop improves repository engineering and historical
data quality. It does not discover, choose, tune, activate, or trade a strategy.

## Objective

State one bounded engineering or data-quality outcome. The objective must be
testable in the repository and must not require a broker mutation, external
message, deployment, or strategy decision.

## Scope

List the exact repository files or private data interfaces in scope. Preserve
unrelated user changes. Historical data may be read and, when explicitly
authorized, appended through the canonical store; it must not be deleted,
relocated, or silently rewritten.

## Evidence

Name the current truth surface and the observations needed before editing. Use
code, tests, manifests, canonical documents, provider responses, or frozen
hashes as appropriate. Distinguish a metadata check from a full data audit.

## Acceptance checks

List concrete commands or assertions that prove the objective. Include focused
tests during development and the retained full suite before handoff. A change
that modifies durable behavior also needs an append-only progress entry.

## Safety boundaries

- No active strategy or strategy proposal
- No return-based rule choice or contamination relabeling
- No account identifier persistence
- No broker order review, placement, cancellation, replacement, or mutation
- No scheduler, deployment, email, or other external write
- No deletion or relocation of preserved historical data

## Commands

Inspect the current repository and data boundary:

```sh
python3 learning_loop.py inspect
```

Validate this prompt or another document implementing the same required
sections:

```sh
python3 learning_loop.py validate-prompt LEARNING_LOOP.md
```

Review a bounded plan before implementation:

```sh
python3 learning_loop.py review-plan \
  --objective "Improve refresh diagnostics" \
  --scope historical_data_cli.py \
  --scope tests/test_historical_data_cli.py
```

Audit the clean-slate invariants:

```sh
python3 learning_loop.py audit
```

The tool is deliberately non-orchestrating: it does not edit files, execute a
plan, call a provider, or start a background job. Implementation remains an
explicit repository task with normal review and verification.
