# Expansion Catalyst Source Pair Readiness

Dataset: `dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1`

Status: independently inspected `READY` for development-only readiness claims

Manifest: `7ace3b399e8e6320325c755001cb3b31217d77fb534f2ccd030d9ecd568bbd0b`

This development-only dataset rejoins the exact 1,987 scanner-selected pairs to
the 134 frozen primary-routing URLs through immutable article keys and URL
hashes. It measures whether each pair has a routed URL, terminal response,
HTTP-200 response, successful HTML/PDF/XML body, canonical-link candidate, or
standard HTML timestamp candidate.

A read-only aggregate reconnaissance motivated this contract. The final
implementation, four input hashes, exact joins, flag definitions, private-row
boundary, and zero-evidence rules are frozen before the formal derivation and
inspection. This is infrastructure characterization, not a blinded alpha test.
No return, event direction, materiality, source ownership, issuer binding,
causal timestamp, or production rule is observed or inferred.

Success only means the pair has a source artifact that could be examined under
a later format-specific contract. A 200 response, canonical link, or
timestamp-shaped field cannot verify a catalyst. Exact pair, article, URL, and
source-response joins remain in the external historical store.

## Inspected result

- 1,864 of 1,987 pairs have no primary-routing lead; 123 have at least one.
- 117 pairs have a captured response and six have at least one terminal capture
  error. Eighty-four pairs connect to an HTTP-200 response.
- Seventy pairs connect to successful HTML, 15 to successful PDF, and none to
  successful XML. These pair counts overlap and must not be summed.
- Twenty-two pairs have a canonical-link candidate and only 18 have any
  recognized HTML publication-meta, JSON-LD `datePublished`, or `<time
  datetime>` candidate.
- Routed categories cover 59 authority-candidate pairs, 69 potential
  issuer-host pairs, and one exchange-candidate pair, with overlaps.
- Zero timestamps were accepted. Source ownership, issuer binding, causal
  availability, materiality, direction, conflict, catalyst verification,
  outcomes, and production-rule authority remain false.

The 18-pair timestamp-candidate surface is already below the 20-signal
`PROVISIONAL` minimum before issuer verification, v3 trigger/execution gates,
or closed outcomes. PDF-specific parsing could expand the evidence surface, but
must be frozen separately and cannot treat document metadata as publication
proof. The next efficient work is source-type-specific verification, not a new
strategy variant or return calculation.

```sh
python3 catalyst_source_pair_readiness.py freeze
python3 catalyst_source_pair_readiness.py derive --manifest \
  historical_batches/catalyst_source_pair_readiness/manifests/dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1-7ace3b399e8e6320325c755001cb3b31217d77fb534f2ccd030d9ecd568bbd0b.json
python3 catalyst_source_pair_readiness.py inspect --manifest \
  historical_batches/catalyst_source_pair_readiness/manifests/dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1-7ace3b399e8e6320325c755001cb3b31217d77fb534f2ccd030d9ecd568bbd0b.json
```
