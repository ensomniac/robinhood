# Expansion Primary-Source Capture

Dataset: `dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1`

Status: frozen before network collection

Manifest: `409a807f1cae100ced8b9018aeb99612a2efc9d5f496e5981c26654762b41ef4`

This contract freezes the exact 134 unique authority, exchange, and potential
issuer-host routing URLs before any request. The collector blocks private and
non-global addresses, revalidates every redirect, permits five redirects, caps
responses at 10 MiB, checkpoints every URL, and retains terminal failures.

Raw URLs, article mappings, addresses, response metadata, and response bytes
stay under `LOCAL_HISTORICAL_DATA_ROOT`. Public artifacts contain only counts
and hashes. A successful response does not verify source ownership, issuer
binding, historical availability by 09:35 ET, event direction, conflicts, or a
primary catalyst. Outcomes and production changes remain inaccessible.

```sh
python3 catalyst_primary_source_capture.py freeze
python3 catalyst_primary_source_capture.py collect --manifest \
  historical_batches/catalyst_primary_source_capture/manifests/dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1-409a807f1cae100ced8b9018aeb99612a2efc9d5f496e5981c26654762b41ef4.json
python3 catalyst_primary_source_capture.py inspect --manifest \
  historical_batches/catalyst_primary_source_capture/manifests/dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1-409a807f1cae100ced8b9018aeb99612a2efc9d5f496e5981c26654762b41ef4.json
```
