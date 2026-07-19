# Expansion Catalyst Source Profile

Dataset: `dataset-catalyst-source-profile-2026-07-19-expansion-v1`

Status: inspected `READY` for development-only format profiling

Manifest: `411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f`

This outcome-blind contract profiles the 121 hash-verified captured responses.
It freezes HTML title, canonical-link, publication-meta, time-element, JSON-LD
date-field, redirect, header, HTTP, and content-type inspection before parsing.

Extracted timestamps and canonical links remain unaccepted candidates. They do
not establish historical availability, source ownership, issuer binding,
materiality, direction, conflict, or a catalyst. PDF and XML bodies are counted
but not interpreted by the HTML profiler. Exact response facts remain external.

## Inspected result

- All 134 terminal URL records were rejoined: 121 captured responses and 13
  bounded transport failures.
- Captures contain 107 HTML, 13 PDF, and one XML response. HTTP results include
  84 successful responses, 35 forbidden responses, and two not-found responses.
- Of the 71 successful HTML responses, 71 expose a title, 24 a canonical link,
  nine a recognized publication-meta candidate, 14 a JSON-LD `datePublished`
  candidate, and 16 a `<time datetime>` candidate.
- Fifty-eight captured responses expose `Last-Modified`; 116 did not redirect,
  five redirected once, and two ended on a different host.
- Zero timestamp candidates were accepted. Source ownership, issuer binding,
  causal availability, primary-catalyst verification, and production-rule
  authority all remain false.

The next evidence contract must be format-specific. It must bind issuer domains
and source ownership, define timestamp precedence and conflict handling, and
independently verify point-in-time availability before classifying direction.
The 35 forbidden, two not-found, and 13 transport-failure targets remain
unresolved rather than being silently replaced by secondary evidence.

```sh
python3 catalyst_source_profile.py freeze
python3 catalyst_source_profile.py derive --manifest \
  historical_batches/catalyst_source_profile/manifests/dataset-catalyst-source-profile-2026-07-19-expansion-v1-411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f.json
python3 catalyst_source_profile.py inspect --manifest \
  historical_batches/catalyst_source_profile/manifests/dataset-catalyst-source-profile-2026-07-19-expansion-v1-411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f.json
```
