# Expansion Catalyst PDF Source Profile

Dataset: `dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1`

Status: frozen before formal derivation and independent inspection

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

```sh
python3 catalyst_pdf_source_profile.py freeze
python3 catalyst_pdf_source_profile.py derive --manifest \
  historical_batches/catalyst_pdf_source_profile/manifests/dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1-b1f2bff3bbc816151676a940e6547e9526a1539398c2a4afae48664896162e4c.json
python3 catalyst_pdf_source_profile.py inspect --manifest \
  historical_batches/catalyst_pdf_source_profile/manifests/dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1-b1f2bff3bbc816151676a940e6547e9526a1539398c2a4afae48664896162e4c.json
```
