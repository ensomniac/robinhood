# Progress History

`HISTORY.jsonl` is the repository's append-only record of meaningful technical,
research, workflow, and operational discoveries. It captures why the system
changed, what evidence produced the insight, what improved, and what remains to
be tested. Trade outcomes remain in the signal ledger and archived trade
contexts; this history is for reusable engineering and process knowledge.

## Contribution Contract

Add an entry whenever work yields a meaningful outcome: a diagnosed failure, a
new provider constraint, a reusable architecture decision, a safety improvement,
a performance win, or an observation that changes how future work should run.
Interesting failed approaches belong here when they prevent future repetition.
Small wording-only or generated-ledger commits do not need an entry.

Each JSON Lines record has:

- a unique date-prefixed `id`, timestamp, category, title, and summary;
- one or more concrete `findings` and resulting `impact` statements;
- optional `follow_ups` and repository-relative `related_files`.

Append with:

```sh
python3 progress_history.py add \
  --id 2026-07-15-example-finding \
  --recorded-at 2026-07-15T23:00:00-04:00 \
  --category reliability \
  --title "Example finding" \
  --summary "What happened and why it matters." \
  --finding "Concrete evidence." \
  --impact "How future passes improve." \
  --related-file path/to/file.py
```

Validate it with `python3 progress_history.py audit`.

## Commit Hook

The tracked pre-commit hook requires at least one newly staged history record
when substantive source, strategy, or operating documentation changes are
staged. Install it once per checkout:

```sh
python3 progress_history.py install-hook
```

`PROGRESS_SKIP=1 git commit ...` is reserved for genuinely mechanical or
generated commits with no reusable lesson. It is not appropriate after a bug,
provider discovery, workflow repair, safety decision, or architecture change.
