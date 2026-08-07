# Robinhood Codex Clean-Slate Contract

Effective: 2026-08-07

This repository has no active trading strategy. Its supported surface is
limited to historical-data stewardship, sanitized account context, immutable
history, and strategy-neutral engineering/data learning. The complete prior
tree is frozen at `legacy-pre-clean-slate-2026-08-07`.

## Hard broker boundary

- Live trading is disabled.
- Do not place, cancel, replace, review for submission, or otherwise mutate a
  broker order. Do not start an order workflow merely because a connector is
  available.
- Read-only account inspection is allowed only when it is directly requested or
  needed for an authorized account-maintenance task. Discover accounts at
  runtime, require exactly one unambiguous Robinhood account with
  `agentic_allowed=true`, and stop if the result is absent or ambiguous.
- Never guess, hard-code, log, or commit an account number. Do not persist
  credentials, tokens, MFA material, cookies, or private connector payloads.
- The legacy statement that one Agentic account exists is context, not a stored
  identifier and not authority to trade.

A future strategy requires explicit user authorization, a new prospective
contract, tests, and contamination-aware evidence. Adding research code does
not restore broker authority. Live operation needs a separate explicit
authorization after all required safeguards are rebuilt and verified.

## Historical data

The canonical store is private and external to Git at
`LOCAL_HISTORICAL_DATA_ROOT`. Preserve it in place.

- Never delete, truncate, relocate, or bulk-rewrite the store or its `_sources`,
  `_derived`, `_migrations`, `_archive`, or other retained namespaces.
- Use `historical_store.py` for canonical document semantics and
  `historical_service.py` for cache-first provider fallback.
- Provider adapters are historical-data-only. Their presence never authorizes
  an account or order call.
- `fetch` and `refresh` may access configured historical providers only when the
  requested data operation is in scope. Keep the provider order deterministic:
  local canonical cache, IBKR historical data, Massive, then Alpaca.
- Migration is append-by-content. Verify source hashes and the resulting store;
  never discard a source merely because migration completed.
- A full `historical_data_cli.py check` reads millions of documents and can be
  expensive. Do not represent a metadata check as a full audit.

## History and contamination

- `history/OUTCOME_EXPOSURE_INDEX.jsonl` is append-only. Existing records,
  source hashes, dates, and symbols are immutable.
- `history/legacy/*.jsonl` are exact baseline sources for that index. Do not
  edit them.
- `progress/HISTORY.jsonl` is append-only. Record each substantive repository
  change with `progress_history.py add`.
- Legacy implementations and detailed evidence stay at the archive tag. They
  may be inspected, but rejected or exposed evidence must not be relabeled as
  untouched or used to tune a successor.
- Never rewrite history to make a new idea appear independent.

## Account and identifier privacy

The current public account summary is `history/ACCOUNT_HISTORY.md`. Keep it
sanitized. If an exact operational identifier ever must be stored in Git,
encrypt it with `sensitive_data.py` and audit the context before committing.
Passwords, authentication secrets, and session material must never be stored,
even encrypted.

## Neutral learning loop

`learning_loop.py` may inspect the repository, validate a bounded prompt,
review a proposed engineering/data plan, and audit the clean-slate contract. It
must not invent or activate a strategy, access outcomes to choose a rule, call
a broker, deploy a scheduler, or send external messages.

Make bounded changes with explicit evidence and acceptance checks. Preserve
pre-existing user changes. Diagnose before editing when the request is an
audit, review, or explanation rather than an implementation request.

## Validation and Git

Before committing a substantive change:

```sh
python3 outcome_exposure.py audit
python3 progress_history.py audit
python3 sensitive_data.py audit history
python3 learning_loop.py audit
python3 -m ruff check .
python3 -m pytest -q
```

Run narrower checks during development, then the complete retained suite before
handoff. Inspect the staged diff, confirm secrets are absent, commit
intentionally, and push the current branch when the requested implementation is
complete. Never use destructive Git recovery commands on user work.
