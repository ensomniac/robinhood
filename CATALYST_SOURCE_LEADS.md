# Expansion Catalyst Source Leads

Dataset: `dataset-catalyst-source-leads-2026-07-19-expansion-v1`

Status: frozen before offline derivation

Manifest: `14107372085ed528dd001e74814024a9093d308ac4ba7bbf50fab7617cde495a`

This contract parses outbound links from the exact 4,205 content-complete
secondary articles without network access. It freezes URL normalization,
platform exclusions, and routing categories for authority, exchange, potential
issuer-host, wire, secondary-corroboration, and other external leads.

A host or path category is a routing hint only. It does not prove source
ownership, issuer binding, point-in-time availability, event materiality,
direction, conflict status, analyst attribution, or independent corroboration.
Symbols, articles, URLs, and pair rows remain outside Git. Outcomes, variants,
and production changes are inaccessible.

```sh
python3 catalyst_source_leads.py freeze
python3 catalyst_source_leads.py derive --manifest \
  historical_batches/catalyst_source_leads/manifests/dataset-catalyst-source-leads-2026-07-19-expansion-v1-14107372085ed528dd001e74814024a9093d308ac4ba7bbf50fab7617cde495a.json
python3 catalyst_source_leads.py inspect --manifest \
  historical_batches/catalyst_source_leads/manifests/dataset-catalyst-source-leads-2026-07-19-expansion-v1-14107372085ed528dd001e74814024a9093d308ac4ba7bbf50fab7617cde495a.json
```
