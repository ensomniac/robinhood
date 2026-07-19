# Twelve-Hour Learning and Strategy Hardening Report

Window: 2026-07-19 00:23:38-12:23:38 ET

Mode: `learning`; broker actions forbidden

Strategy: `2026-07-15-orb-v3`

End maturity: `UNVALIDATED`

Rules hash: `00c3aa83754f880ec67d29b7446e43ffccb23c2fb0c939bc4927909f8b4b5867`

Status: complete; end-of-window audits passed

## Executive finding

The repository is materially safer and the dynamic 09:35 scanner-replay gap is
closed, but the strategy has not earned an alpha claim or maturity promotion.
The 100-date expansion produced a faithful scanner-selected universe and then
failed closed when unchanged-v3 catalyst and execution gates yielded zero fully
ready signals. That is a positive learning-loop outcome: the system now exposes
why capacity disappears instead of manufacturing a profitable backtest from
post hoc candidates, ambiguous catalysts, or weakened spread/chase rules.

No return was read on the expansion campaign and no strategy variant was
invented. Existing risk and circuit-breaker controls were centralized and
validated, but no alpha threshold was loosened and the strategy version did not
change. `SIGNALS.jsonl`, `TRADES.md`, and `trades/` remained unchanged.

## Repository scope and delivery

- Commit range: `0895c33` through the final report commit (50 validated commits,
  including this report).
- Base: `0ae197b`; last implementation head before this report: `3f23883`.
- Diff including this report: 122 files, 20,180 insertions, and 187 deletions.
- Changed surface: 24 code/config files, 22 test files, 26 other Markdown files,
  15 compact research results, 29 historical-batch artifacts, and six other
  files.
- Branch: `1.0.0`; local head and `origin/1.0.0` matched after every coherent
  slice.

## Strategy and safety hardening

The session tightened implementation fidelity around the existing v3 policy:

- Enforced the existing 0.08% A+ median-spread threshold separately from the
  0.10% operating threshold. A nominal 90-point setup no longer becomes an
  unvalidated live pilot with a merely qualified spread.
- Moved the existing three-loss, 2% rolling-five-session, and 4% strategy
  drawdown controls into `strategy_config.toml`, made them part of the rules
  hash, and removed duplicated code-level authority.
- Added fail-closed configuration relationships for strategy identity, schema,
  ordered market/session times, maturity progression, risk/allocation ladders,
  and promotion thresholds.
- Removed a duplicate opening-price input so the validated opening bar is the
  sole price authority.
- Required return records used for promotion to come from eligible signals and
  reconciled ledger session evidence against archived lifecycle context.
- Scoped historical rules-hash blockers to performance-bearing records rather
  than allowing nonperformance research rows to create false maturity drift.
- Prevented a partially filled position from receiving a second protective stop
  when an undersized stop already exists; the guard now replaces it.
- Made duplicate entry and exit orders fail safe, with exposure taking priority
  over research or journaling.
- Enforced and rechecked the research lock immediately before proposal writes,
  preventing a stale earlier check from racing a new strategy proposal.
- Distinguished simultaneous exchange listings in scanner identity resolution
  rather than collapsing a symbol to an arbitrary listing.

These changes reduce operational ruin paths and false promotion. They do not
claim higher expectancy; they make any future expectancy harder to fake and
safer to execute.

## Waste and maintainability

- Simplified scanner replay build output and removed redundant status plumbing.
- Removed the stale S3 acquisition blocker because the project now uses the
  local historical store and provider fallback path.
- Removed duplicate numeric authorities and narrowed maturity hash checks to the
  records they govern.
- A 90%-confidence dead-code scan found no actual deletion target; every hit was
  a required context-manager protocol parameter. No code was deleted merely to
  satisfy a static heuristic.
- Measured coverage across the evaluator, guard, lifecycle, learning controls,
  and store was 76% overall: historical store 83%, strategy engine 82%, session
  guard 77%, learning data 74%, trade lifecycle 71%, and learning loop 65%.
  The final full behavioral suite is the authoritative gate.
- Complexity review still identifies older replay orchestration and the central
  session guard as hotspots. Refactoring them without a frozen behavioral
  contract would be riskier than leaving them intact during this campaign.
- Removed the unsatisfiable `ibapi==10.37.2` PyPI pin from core requirements,
  documented installation from IBKR's official TWS API bundle, added
  `requirements-dev.txt`, and bound validation to `python3 -m pytest` rather
  than a stale machine-global launcher.
- Added a configurable 20-GiB default free-space reserve to the canonical
  historical writer. Changed day documents and alias repairs now fail before
  an atomic temporary file could consume that reserve; idempotent cache reads
  and merges remain available without new write capacity.

## Dynamic 09:35 scanner replay

Dataset:
`dataset-production-scanner-replay-2026-07-19-expansion-v1`

Manifest:
`03a6eff46748cb396b0a5f532d884a9532766c6e546c92afda6c2947a9a9e22b`

The exact 100-date campaign was frozen before collection. Independent
inspection verified:

- 100 completed dates and unchanged frozen selection.
- 526,587 point-in-time security/date evaluations.
- 6,024 production-policy-eligible rows.
- 1,987 selected candidates.
- 133 source sessions and 176 split-adjusted evaluations.
- 686,079 canonical day documents with matching derived daily and 15-minute
  datasets; 628,899 also carry opening one-minute data.
- Source artifacts, session calendar, split actions, production rules,
  denominators, ranks, metrics, and shortlist hashes all reverified.

The point-in-time security master ends with 5,668 records for 5,593 instruments
and hash
`8a6912f2ccefe84a9dd8a2e81c78de38cb0b1d713c18b95528f2d0363d9de99a`.

## Unchanged-v3 non-return attrition

The exact 1,987 private pairs were frozen before downstream joins. The base
candidate join found:

- Complete candidate bars and catalyst-discovery contexts for all 1,987 pairs.
- 1,460 raw SIP trigger tapes.
- 1,331 three-snapshot windows; 1,279 basic fresh/uncrossed windows.
- 1,416 pairs with any news candidate, which is discovery coverage only.

The separately frozen clean-trigger pass classified all 1,460 crossings:

- 1,460 continuous clean crosses.
- 1,340 three-snapshot windows.
- 1,267 basic fresh/uncrossed windows.
- 726 final asks inside the existing chase cap.
- Four explicit missing-NBBO blockers and zero collection errors.

Point-in-time SEC and official halt fidelity then classified 696 filings and
5,649 halt records:

- 48 financing/negative conflicts.
- 361 filings stale before the prior close.
- 87 unresolved primary sources.
- 186 materially relevant filings with unresolved direction.
- 14 verified positive primary filings.
- Zero official halt overlaps in 1,460 trigger windows.

The unchanged-v3 readiness cascade on those 14 positive pairs found nine clean
triggers, eight fresh three-snapshot windows, two spread passes, one chase pass,
and zero rows passing both known hard gates. It read no outcomes and earned no
rule change. The opening-low stop proxy also fit the 0.8% cap on none of the
eight measurable rows, although a later technical invalidation model would
still be required for a real signal.

## Catalyst fidelity pipeline

Because the narrow SEC surface could not supply a development sample, the
session built a bounded direct-source pipeline without treating secondary news
as primary evidence:

1. Alpaca enrichment returned all 7,391 frozen articles across 100 dates and
   supplied 4,205 bodies totaling 27,772,306 bytes. Content covers 1,183 pairs;
   804 remain without content.
2. Source-lead extraction parsed all 4,205 bodies, normalized 13,811 URLs, and
   rejected 42,836 article-level platform/source links. It found primary-routing
   leads for 123 pairs and corroboration-routing leads for 227.
3. The exact 134 authority, exchange, and potential issuer-host URLs were frozen
   before network capture. All reached terminal dispositions: 121 hashed
   responses totaling 23,793,420 bytes and 13 transport errors; HTTP outcomes
   were 84 success, 35 forbidden, and two not found.
4. Offline HTML profiling found 71 successful HTML documents, 24 canonical-link
   candidates, nine recognized publication-meta candidates, 14 JSON-LD
   `datePublished` candidates, and 16 `<time datetime>` candidates. Zero were
   accepted.
5. Pair-level readiness showed that 1,864 of 1,987 pairs have no primary route;
   123 have a route, 84 reach HTTP 200, 70 reach successful HTML, 15 reach
   successful PDF, and 18 expose a standard HTML timestamp candidate.
6. Every one of the 13 PDFs was rendered and visually inspected under the PDF
   workflow. They span 459 pages and mix issuer, SEC, court, government,
   reference, release, and presentation formats. All have extractable
   first-three-page text and a month-name date candidate; zero metadata or text
   dates were accepted.
7. The 15 PDF pairs do not overlap the 18 HTML timestamp-candidate pairs. The
   current structural ceiling is therefore 33 pairs, 33 documents, and 38 exact
   pair/source joins. Five pairs have two candidate sources.

The new `CATALYST_SOURCE_SEMANTICS_PLAN.md` specifies the next executable gate:
source ownership and point-in-time issuer binding, source-type-specific
publication semantics, same-day date-only rejection, conflict taxonomy,
terminal reason reconciliation, and a hard 20-row capacity gate before any
return can be read.

## Historical store and provider posture

The final full audit validated:

- 844,904 canonical day files.
- 2,334,718 datasets and 124,537 contexts.
- 6,605 symbols over 733 dates.
- Provider datasets: Alpaca 2,138,737; IBKR 195,960; replay bundle 20; Massive
  one.
- Zero schema, hash, or document errors.
- 84,843,290,624 bytes free after audit, roughly 79 GiB; the volume is 96% used.

The cache is large and internally valid, but provider diversity is not yet what
the configured fallback order suggests. Massive contributes security-reference
collection and only one canonical dataset in this store. That is not a blocker
for the completed Alpaca-SIP replay, but it is a resilience and provenance
concern for future expansion. The canonical writer now enforces its configured
disk reserve; avoid another broad campaign until retention/capacity policy is
explicit.

## Final integrity state

- Test suite: 424 passed plus 49 subtests.
- Ruff: all checks passed.
- Signal ledger: 1,166 valid records, 106 sessions, 1,060 signals.
- Lifecycle: 1,166 archived contexts, zero active contexts, zero violations.
- Maturity: `UNVALIDATED`; zero closed, confirmation, live, or stop-execution
  signals; zero rule violations and zero mismatched performance-rule records.
- Learning registry: 27 datasets across the registered lanes; valid.
- Security master: 5,668 records, 5,593 instruments; valid.
- Progress history: valid.
- Sensitive-data audit: 1,168 files checked, zero plaintext/private-identifier
  violations.
- Learning runs: completed bounded runs are closed; the two pre-existing exact
  selected-pair/fidelity contracts remain intentionally `PREREGISTERED` while
  the research lock is active.
- Broker actions: none.

## Strategic conclusion and next order of work

The highest-value decision is to keep v3 unchanged. The scanner fidelity gap is
closed, but the evidence now shows insufficient fully executable capacity, not
a parameter that should be optimized. Loosening spread, chase, catalyst, or stop
rules to create trades would directly increase false-edge, slippage, and
drawdown risk—the opposite of durable geometric growth.

Next work, in order:

1. Freeze the exact 33-pair, 33-document, 38-join source-semantics dataset under
   `CATALYST_SOURCE_SEMANTICS_PLAN.md`.
2. Verify source ownership, point-in-time issuer identity, and pre-09:35 causal
   publication. Date-only same-day rows fail.
3. Resolve material direction and financing conflicts under a frozen taxonomy.
4. If fewer than 20 verified positive rows survive, close without outcomes and
   acquire a disjoint frozen sample or same-source recovery. Do not loosen v3.
5. If at least 20 survive, reapply every already frozen v3 non-return gate. Only
   then freeze an outcome, cost, stop/exit, and return contract.
6. Keep production `UNVALIDATED` until the public ledger independently earns
   promotion.

This sequencing is likely to support explosive compounding because it improves
the reliability of the edge estimate and execution safety before increasing
capital exposure. It deliberately prefers no trade over a backtest assembled
from look-ahead, ambiguous causality, or unexecutable microstructure.
