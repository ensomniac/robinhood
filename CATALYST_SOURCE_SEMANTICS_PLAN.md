# Catalyst Source Semantics Plan

Status: required next bounded learning slice; no evaluation or return access is
authorized by this plan

## Decision

Run one source-semantics pass over the exact 33 structurally reachable selected
pairs before any outcome join. Do not expand to the other 1,954 pairs, invent a
strategy variant, loosen the catalyst gate, or infer causality from a successful
HTTP response, metadata timestamp, or date-shaped string.

The objective is to answer one question: for how many of the 33 pairs can a
primary source be bound to the point-in-time issuer and shown to have published
a material, directional event before the 09:35 ET decision boundary? Only a
later frozen contract may evaluate unchanged-v3 non-return gates, and only if at
least 20 rows survive this source stage may an outcome contract be proposed.

## Measured starting surface

- Exact scanner-selected denominator: 1,987 pairs over 100 dates.
- Any primary-routing lead: 123 pairs; no primary route: 1,864.
- HTTP 200: 84 pairs; successful HTML: 70; successful PDF: 15.
- Standard HTML timestamp candidate: 18 pairs from 20 documents.
- First-three-page PDF date candidate: 15 non-overlapping pairs from 13
  documents.
- Combined structural ceiling: 33 pairs, 33 source documents, and 38 exact
  pair/source joins; five pairs have two candidate sources.
- Candidate-pair routing categories: 19 authority, 13 potential issuer host,
  and one exchange. These are routing labels, not verified ownership.
- Accepted timestamps, verified catalysts, outcomes, and production-rule
  changes: zero.

This surface is just above the 20-signal provisional minimum. It is therefore
large enough to justify careful verification but too small to tolerate broad
false positives. The expected result may legitimately be fewer than 20 rows.

## Freeze sequence

The next dataset contract must bind, before formal derivation:

1. The exact 33 pairs, 33 source documents, 38 private pair/source joins, and all
   upstream manifest/result hashes.
2. Source-type routing rules and public-suffix-normalized host checks.
3. The point-in-time security-master hash and each pair's resolved instrument,
   exchange, symbol, and CIK where available.
4. Exact ownership and issuer-binding fields, timestamp candidates and
   precedence, timezone rules, same-day cutoff rules, conflict states, and
   rejection reason codes.
5. Parser implementation and dependency versions.
6. A private row schema plus an aggregate-only public result boundary.
7. `target_outcomes_observed_or_derived=false` and
   `automatic_strategy_application=false`.

Formal derivation must produce one terminal disposition for every pair/source
join. Independent inspection must rebuild all aggregates from the frozen inputs
and compare private result hashes before the dataset can become `READY`.

## Source ownership and issuer binding

### SEC sources

- Require a final or canonical host of `sec.gov` under an HTTPS public path.
- Bind the document's registrant CIK to the pair's security-master CIK. A ticker
  mention alone is insufficient because tickers are reused and can change.
- Require filing accession/document identity where the source is a filing or
  exhibit. A general SEC page, search page, unrelated registrant, historical
  filing, or annual report linked only as background is not a catalyst.
- The current 26 SEC responses with HTTP 403 remain unresolved. A future
  recovery must freeze an SEC-operated same-source endpoint or accession-based
  retrieval; it may not silently substitute secondary news.

### Other authority sources

- A `.gov` host establishes government control of the page, not issuer binding
  or materiality.
- Require the source's operative event section to name the resolved issuer or a
  uniquely bound subsidiary/product and describe a direct action such as an
  approval, enforcement action, award, recall, court ruling, or contract.
- Reject background statistics, weather/reference tables, political letters,
  generic policy pages, unrelated court matters, and links included only as
  article context.
- For court documents, bind parties and decision date; a filing date, submitted
  date, historical opinion, or case reference is not automatically causal.

### Exchange sources

- Require an exchange-controlled host and exact symbol/listing identity for the
  point-in-time instrument.
- Accept only a direct listing, halt/resumption, compliance, or issuer-specific
  exchange notice with causal publication before the cutoff.
- A generic issuer profile, quote page, or exchange marketing article fails.

### Issuer-host sources

- A host beginning with `ir.`, `investor.`, or `investors.` is only a routing
  candidate. Bind it to the issuer using at least two of: legal company name,
  point-in-time ticker, security-master CIK, issuer-owned canonical domain, or
  an issuer-controlled footer/identity statement.
- Generic infrastructure such as `q4inc.com` and `q4cdn.com` cannot establish
  issuer ownership by hostname. Require the document itself and its referring
  issuer page or canonical chain to bind the exact company.
- A release, earnings deck, or results PDF must name the issuer and relevant
  reporting period. A calendar/event page, replay, stale deck, unrelated
  subsidiary, or generic investor landing page fails.

## Publication-time semantics

Every accepted source needs an aware timestamp and a causal relation to the
pair's 09:35 ET boundary. Candidate fields are considered in this order, but
precedence never overrides a conflict:

1. An explicit source-controlled publication timestamp attached to the release
   or notice, including its timezone.
2. Consistent JSON-LD `datePublished` and page-visible release timestamp.
3. Consistent `article:published_time`-style metadata and page-visible release
   timestamp.
4. A `<time datetime>` value whose DOM context is demonstrably the document's
   publication time, not an event/calendar/update time.
5. For PDFs, a clearly labeled release/issued date in the document's first-page
   issuer header or dateline, corroborated by the issuer/authority page that
   linked the same hashed document.

The following cannot independently establish causal publication:

- HTTP `Date`, `Last-Modified`, ETag, capture time, file creation/modification
  metadata, URL path dates, fiscal-period labels, filing periods, conference
  dates, signatures, decision/submission dates, or dates found in quoted text.
- A date with no time on the same trading day. Because it cannot prove
  availability before 09:35 ET, it is `SAME_DAY_TIME_UNRESOLVED` and fails this
  replay. A date strictly before the trading date may pass the cutoff only after
  ownership and document semantics pass.
- A later update date when original publication time is unknown.

Normalize accepted timestamps to UTC and retain the source timezone and raw
field privately. If credible fields disagree on calendar date, ordering around
09:35 ET, or document identity, disposition is `TIMESTAMP_CONFLICT`; never pick
the most convenient field.

## Document semantics and direction

Timestamp and issuer verification must complete before event classification.
Then apply a separately frozen taxonomy:

- `DIRECT_POSITIVE`: a primary source describes a material event that is
  directionally supportive for the long thesis.
- `DIRECT_NEGATIVE`: the primary event is materially adverse.
- `FINANCING_OR_DILUTION_CONFLICT`: an offering, shelf, convert, ATM, liquidity
  warning, or other financing conflict undermines the long catalyst.
- `MIXED_OR_CONTRADICTORY`: material positive and negative facts coexist without
  a predeclared resolution rule.
- `NON_MATERIAL_OR_CONTEXT_ONLY`: source is real and issuer-bound but does not
  establish a trade-worthy event.
- `UNRESOLVED`: ownership, issuer, timestamp, semantics, or direction is not
  deterministically supportable.

Positive keyword matches are never enough. Financing/conflict detection runs
before positive disposition. Analyst actions remain outside this direct-source
pass and still require a directly attributed reputable report plus independent
corroboration under the existing v3 rules.

## Terminal reason codes

At minimum, publish aggregate counts for:

- `VERIFIED_POSITIVE_PRIMARY`
- `VERIFIED_NEGATIVE_PRIMARY`
- `VERIFIED_CONFLICT`
- `SOURCE_OWNERSHIP_UNRESOLVED`
- `ISSUER_BINDING_UNRESOLVED`
- `PRIMARY_SOURCE_IRRELEVANT`
- `TIMESTAMP_MISSING`
- `TIMESTAMP_CONFLICT`
- `SAME_DAY_TIME_UNRESOLVED`
- `PUBLISHED_AFTER_0935`
- `DOCUMENT_SEMANTICS_UNRESOLVED`
- `NON_MATERIAL_OR_CONTEXT_ONLY`
- `CAPTURE_FORBIDDEN`, `CAPTURE_NOT_FOUND`, and `CAPTURE_TRANSPORT_ERROR`

One row may retain multiple diagnostic failures privately, but the public
attrition cascade needs one predeclared terminal reason so counts reconcile to
the exact denominator.

## Capacity and outcome gates

Publish the cascade in this order:

1. 33 structural candidates.
2. Source ownership verified.
3. Point-in-time issuer binding verified.
4. Causal pre-09:35 timestamp verified.
5. Document semantics resolved.
6. Material direction/conflict resolved.
7. Verified positive primary catalysts.
8. Unchanged-v3 catalyst plus already frozen trigger/execution survivors.

If fewer than 20 rows remain after step 7 or after unchanged-v3 non-return
gates, close the run as inadequate capacity. Acquire a new disjoint frozen date
set or improve same-source recovery under a separate contract. Do not read
returns, relax a gate, reclassify unresolved evidence, or substitute dates.

If at least 20 survive, freeze the exact rows, exit/stop simulation semantics,
cost model, and return windows before reading a price outcome. Production still
remains `UNVALIDATED`; maturity is earned only through the ledger rules.

## Why this supports compounding growth

Explosive compounding depends more on avoiding false edge and large drawdowns
than on maximizing the number of backtest trades. This plan attacks the largest
remaining source of look-ahead and selection bias: treating evidence captured
months later, ambiguous dates, and unrelated primary pages as if they were known
before 09:35. The small, reconcilable cascade makes failure informative, keeps
the strategy from learning on manufactured catalysts, and concentrates future
data spend on rows that can actually reach an executable v3 signal.
