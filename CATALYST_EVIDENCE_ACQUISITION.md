# Point-in-Time Catalyst Evidence Acquisition

Status: executable acquisition plan; no new strategy variant

Scope: complete verified-catalyst inputs for the frozen 1,987-pair scanner
expansion without reading target outcomes or weakening `2026-07-15-orb-v3`.

## Measured Starting Point

The 100-date expansion already contains an Alpaca/Benzinga discovery context for
every selected pair. An outcome-blind metadata audit found:

- 1,987 pair contexts and 9,648 pair-attributed articles;
- 1,416 pairs with at least one article and 571 with no article;
- 7,391 unique articles, all sourced from `benzinga.com`;
- 7,391 URLs and authors, 4,205 summaries, and zero retained full-content bodies;
- nonexclusive keyword leads: 2,089 earnings/guidance, 1,878 analyst action,
  258 regulatory/clinical, 230 contract/award, 115 merger/acquisition, 105
  capital return, and 94 financing/dilution.

These are triage counts only. A headline, summary, category keyword, Benzinga
article, or article count never satisfies the verified catalyst gate. The SEC
layer independently verified only 14 positive primary events and identified 48
negative or financing conflicts. The remaining capacity gap is source
acquisition and corroboration, not scanner yield.

## Evidence Contract

Every accepted catalyst record must retain these point-in-time fields outside
Git under `LOCAL_HISTORICAL_DATA_ROOT`:

- security-date identity and sourced instrument/issuer identifiers;
- discovery article ID, source, author, created/updated timestamps, URL, and
  content hash;
- information cutoff and proof that every relied-on source was available no
  later than 09:35 ET on the replay date;
- source class: `SEC`, `ISSUER_RELEASE`, `EXCHANGE_NOTICE`,
  `REGULATOR_OR_GOVERNMENT`, `ATTRIBUTED_ANALYST_REPORT`, or `REPUTABLE_WIRE`;
- canonical source URL, publisher/owner, publication timestamp, capture
  timestamp, response status, content type, raw bytes hash, and parsed-text hash;
- issuer/entity binding evidence and whether the domain/source was controlled by
  the issuer or named authority at that time;
- deterministic materiality, direction, and financing/negative-conflict
  dispositions with matched evidence locations;
- for analyst actions, named firm, named analyst when available, action, rating,
  price target and prior target, plus a separately identified independent
  corroborating source;
- explicit unresolved reason when any required field or source is absent.

Raw bodies, symbols, URLs tied to private rows, and pair-level dispositions stay
external. Git receives only frozen manifests, code, aggregate counts, hashes,
inspection reports, and claim boundaries.

## Acquisition Waterfall

For each frozen pair, stop at the first complete qualifying evidence package;
never reinterpret a lower-quality lead as a higher-quality source.

1. Reuse the hash-bound SEC complete submission and issuer exhibits. Preserve
   all conflict rejects even if a separate article is positive.
2. Requery the exact frozen Alpaca news window with `include_content=true` under
   a new dataset ID. This enriches discovery and attribution only. It does not
   verify the event.
3. Extract outbound/canonical links and named sources from the full article.
   Reject tracking, syndication, social-media, and URL-shortener destinations.
4. Fetch direct issuer newsroom releases, exchange notices, FDA/regulator
   notices, or government award pages when the source timestamp is causal.
5. For an analyst action, require a directly attributed reputable report and a
   second independent corroborating report or a direct analyst-firm source.
   Two URLs that syndicate the same story count as one source.
6. If the primary page is unavailable, timestamp is uncertain, issuer binding
   fails, content changed after cutoff, or corroboration is absent, retain the
   pair as unresolved. Do not fill the field from a headline or later archive.

## Bounded Execution Stages

### Stage A - Freeze discovery enrichment

- Freeze the exact 1,987 pairs, 100 dates, article metadata hashes, provider
  parameters, 09:35 cutoff, collector bytes, and disk reserve.
- Use the existing Alpaca key and the original per-date symbol sets. Same-source
  retries are allowed; provider substitution and pair/date substitution are not.
- Checkpoint each date. A missing article body is a row-level disposition, not a
  batch-fatal error.

Frozen as manifest `154e5831...4fff2e`: 1,987 pairs, 100 dates, and 7,391
unique discovery articles are bound before provider collection.

### Stage B - Enrich secondary content

- Pull `include_content=true` once per frozen date/symbol batch.
- Join by article ID and created timestamp; do not join by mutable headline.
- Publish content availability, attribution completeness, source-link counts,
  and errors before fetching any primary page.

Complete and inspected: all 7,391 unique articles returned, 4,205 contained
27,772,306 bytes of full content, 1,183 pairs have at least one content-complete
lead, and 804 do not. No record was promoted to primary evidence.

### Stage C - Acquire direct sources

- Freeze the exact lead set and canonical-link parser before network collection.
- Use source-specific clients with identifiable user agents, bounded pacing,
  conditional cache reuse, raw-byte retention, and permanent error categories.
- Store immutable responses externally and deduplicate by content hash without
  losing pair/article attribution.

The prerequisite offline routing contract is frozen as manifest
`14107372...de495a` in `CATALYST_SOURCE_LEADS.md`. It cannot contact a source or
promote a host category to verified evidence. Derivation is now `READY`: 123
pairs have authority/exchange/potential-issuer routing leads and 227 have
wire/secondary corroboration leads, after rejecting 42,836 article-level
platform links.

The exact 134 unique primary-routing URLs are frozen for bounded response
capture as manifest `409a807f...b41ef4` in
`CATALYST_PRIMARY_SOURCE_CAPTURE.md`.

Capture is complete: 121 responses totaling 23,793,420 bytes passed hash
inspection and 13 targets retained terminal transport errors. Parsing and
source verification remain separately frozen work.

Offline format/timestamp-candidate profiling is frozen as manifest
`411be9d0...cb3151f` in `CATALYST_SOURCE_PROFILE.md`.
Inspection is complete: only 9 of 71 successful HTML responses expose a
recognized publication-meta candidate, 14 expose JSON-LD `datePublished`, and
16 expose a `<time datetime>` candidate. All remain unaccepted. The next
contract must define format-specific timestamp precedence, issuer-domain
binding, source ownership, and failure handling before any row can become
causal primary evidence.

Pair-level readiness is separately frozen under manifest
`7ace3b39...568bbd0b` in `CATALYST_SOURCE_PAIR_READINESS.md`. It rejoins the
exact 1,987 pairs to response and format flags without accepting a timestamp,
source, catalyst, or outcome.

Inspection is `READY`: 1,864 pairs have no primary-routing lead; 123 have a
lead, 84 reach HTTP 200, 70 reach successful HTML, 15 reach successful PDF,
and only 18 expose any standard HTML timestamp candidate. Counts overlap. This
18-pair surface is below the 20-signal provisional minimum even before source
verification or unchanged-v3 attrition, so returns remain locked.

### Stage D - Classify and independently inspect

- Freeze classifier rules before classification. Positive phrases never
  override a financing or negative conflict.
- An independent implementation must rejoin every pair, source timestamp,
  content hash, issuer binding, direction, conflict, and corroboration result.
- Publish the cascade: selected pairs, discovery leads, content-complete leads,
  direct sources, causal sources, verified material, verified positive, and
  conflicts.

### Stage E - Re-run unchanged-v3 non-return gates

- Apply the already frozen trigger/execution gates only after catalyst fidelity
  is independently `READY`.
- Publish attrition before any return. If fewer than 20 signals survive, report
  inadequate capacity and acquire a new disjoint date set; do not loosen a gate.
- Only a later frozen outcome contract may compute returns, costs, expectancy,
  drawdown, or geometric growth.

## Prioritization

Start with analyst-action leads because they are numerous and absent from the
SEC classifier, but keep their two-source requirement. Then process
regulatory/clinical and contract/award leads, where authoritative government or
issuer pages are often available. Earnings/guidance leads overlap heavily with
SEC exhibits and should first be reconciled against the completed SEC corpus to
avoid redundant downloads. Financing leads are conflict-first.

The 571 no-article pairs remain unresolved until a separate direct-source index
exists. They must not be labeled `NO_CATALYST`; absence from one news feed is not
evidence of absence.

## Resource and Stop Controls

- The full canonical store audit on 2026-07-19 found 844,904 files, 2,334,718
  datasets, 124,537 contexts, 6,605 symbols, 733 dates, and zero errors.
- The volume had 84,951,482,368 free bytes after SEC collection. Preserve the
  existing minimum reserve and freeze an estimated-byte budget before content
  enrichment.
- Stop on source-hash drift, date/pair drift, cutoff ambiguity, rate-limit state
  that cannot checkpoint safely, disk-reserve breach, or any accidental outcome
  access.
- Keep the research lock active. This plan improves evidence coverage; it does
  not authorize a variant, production rule change, maturity change, or broker
  action.
