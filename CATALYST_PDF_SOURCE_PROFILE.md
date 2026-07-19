# Expansion Catalyst PDF Source Profile

Dataset: `dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1`

Status: independently inspected `READY` for development-only structure claims

Manifest: `b1f2bff3bbc816151676a940e6547e9526a1539398c2a4afae48664896162e4c`

This development-only profile binds all 13 successful captured PDF responses,
their hashes, `pypdf` 6.14.2, the first-three-page extraction limit, metadata
presence fields, three date-pattern families, and five structural marker
families. Exact document text, metadata, date candidates, and URL joins remain
in the external historical store.

Visual reconnaissance rendered and inspected the first page of every document.
The corpus mixes SEC reports, issuer earnings releases and presentations,
government forms and letters, a court opinion, and a weather table. That
heterogeneity motivated narrow structural profiling; it does not classify a
catalyst. The final parser and all input/derivation hashes are frozen before
formal derivation. No target return or price outcome was accessed.

PDF creation/modification metadata is never publication proof. A date in the
first three pages may describe a filing period, historical event, meeting,
signature, update, decision, or release. Document markers are routing aids only.
No date, source, issuer, event, direction, conflict, or catalyst can be accepted
by this profile.

## Inspected result

- All 13 PDFs parsed, all expose extractable text in the first three pages, and
  the complete corpus spans 459 pages with a range of one to 145 pages.
- Twelve expose creation metadata, 12 modification metadata, and eight title
  metadata. Zero metadata dates were accepted.
- All 13 contain at least one month-name date candidate in the first three
  pages; one also contains a numeric month/day/year candidate and none contains
  an ISO date candidate. Zero text dates were accepted.
- Structural marker counts overlap: eight documents hit an earnings/results
  marker, six an annual-report marker, six a government marker, one a court
  marker, and one the literal phrase `press release`. Marker hits are not
  document or catalyst classifications.
- The 13 PDFs touch 15 selected pairs. Those 15 do not overlap the 18 pairs with
  an HTML timestamp candidate, yielding 33 pairs with some date-shaped source
  structure. This is a structural ceiling only, not verified evidence.

The 33-pair candidate surface is large enough to justify one narrow
source-semantics pass, but any source/issuer/date rejection will reduce it before
unchanged-v3 trigger and execution attrition. Returns therefore remain locked.
The next contract must bind document ownership and issuer identity, distinguish
release dates from fiscal/report/event dates, and require causal availability
before accepting a row.

```sh
python3 catalyst_pdf_source_profile.py freeze
python3 catalyst_pdf_source_profile.py derive --manifest \
  historical_batches/catalyst_pdf_source_profile/manifests/dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1-b1f2bff3bbc816151676a940e6547e9526a1539398c2a4afae48664896162e4c.json
python3 catalyst_pdf_source_profile.py inspect --manifest \
  historical_batches/catalyst_pdf_source_profile/manifests/dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1-b1f2bff3bbc816151676a940e6547e9526a1539398c2a4afae48664896162e4c.json
```
