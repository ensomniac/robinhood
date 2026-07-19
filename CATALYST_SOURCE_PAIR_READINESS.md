# Expansion Catalyst Source Pair Readiness

Dataset: `dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1`

Status: frozen before formal derivation and independent inspection

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

```sh
python3 catalyst_source_pair_readiness.py freeze
python3 catalyst_source_pair_readiness.py derive --manifest \
  historical_batches/catalyst_source_pair_readiness/manifests/dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1-7ace3b399e8e6320325c755001cb3b31217d77fb534f2ccd030d9ecd568bbd0b.json
python3 catalyst_source_pair_readiness.py inspect --manifest \
  historical_batches/catalyst_source_pair_readiness/manifests/dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1-7ace3b399e8e6320325c755001cb3b31217d77fb534f2ccd030d9ecd568bbd0b.json
```
