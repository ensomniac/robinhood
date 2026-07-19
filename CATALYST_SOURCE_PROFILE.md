# Expansion Catalyst Source Profile

Dataset: `dataset-catalyst-source-profile-2026-07-19-expansion-v1`

Status: frozen before offline response profiling

Manifest: `411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f`

This outcome-blind contract profiles the 121 hash-verified captured responses.
It freezes HTML title, canonical-link, publication-meta, time-element, JSON-LD
date-field, redirect, header, HTTP, and content-type inspection before parsing.

Extracted timestamps and canonical links remain unaccepted candidates. They do
not establish historical availability, source ownership, issuer binding,
materiality, direction, conflict, or a catalyst. PDF and XML bodies are counted
but not interpreted by the HTML profiler. Exact response facts remain external.

```sh
python3 catalyst_source_profile.py freeze
python3 catalyst_source_profile.py derive --manifest \
  historical_batches/catalyst_source_profile/manifests/dataset-catalyst-source-profile-2026-07-19-expansion-v1-411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f.json
python3 catalyst_source_profile.py inspect --manifest \
  historical_batches/catalyst_source_profile/manifests/dataset-catalyst-source-profile-2026-07-19-expansion-v1-411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f.json
```
