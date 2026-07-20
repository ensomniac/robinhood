# Second-Tranche Primary-Source Semantics Contract

Planned dataset:
`dataset-primary-source-semantics-contract-2026-07-20-development-v3`

Status: implementation ready; the v3 contract is not yet frozen and no target
source, selected-symbol detail, or outcome access is authorized

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Implementation Boundary

`development_catalyst_contract.py` now accepts an explicit dataset identity and
exact paths for the selected-pair manifest, scanner manifest, scanner summary,
independent scanner inspection, point-in-time security-master attestation,
production-strategy attestation, selection documentation, public manifest root,
and public inspection status. The dataset identity also scopes the ignored
private source namespace, so one tranche cannot read or overwrite another
tranche's source rows.

The implementation does not change the source rules. It still requires primary
evidence, source ownership, issuer binding, causal timestamp precedence,
same-day date-only rejection, financing or dilution checks before positive
classification, one terminal disposition under the frozen precedence, whole-
source fidelity, zero substitution, private exact rows, and a separately frozen
outcome contract after both 20-pair gates pass.

Freeze fails if any target source artifact already exists for the new dataset.
Inspection independently rebuilds the private selected-pair surface, upstream
hashes, implementation and dependency versions, source rules, recovery order,
capacity contract, privacy boundary, and outcome lock.

## Runbook

The implementation, tests, this document, and durable progress record must be
committed and pushed before running `freeze`:

```sh
python3 development_catalyst_contract.py \
  --dataset-id dataset-primary-source-semantics-contract-2026-07-20-development-v3 \
  --source-manifest historical_batches/development_tranche_v3/selected_pair_manifests/dataset-selected-candidate-contract-2026-07-20-development-v3-f00e8393391d8daee026e326d98643d8f07e2f38f847e4b257c6caa1b73c8897.json \
  --scanner-manifest historical_batches/development_tranche_v3/scanner_manifests/dataset-production-scanner-replay-2026-07-20-development-v3-1a37bc3d141dff3741bc965303e490eaa59d8f76071b9e5d1bd2b301eb878926.json \
  --scanner-summary research_results/2026-07-20-development-tranche-v3-scanner.json \
  --scanner-inspection research_results/2026-07-20-development-tranche-v3-scanner-inspection.json \
  --security-master-source historical_batches/development_tranche_v3/security-master-source.json \
  --strategy-source historical_batches/scanner_expansion/production-strategy-source.json \
  --selection-doc DEVELOPMENT_SELECTED_PAIRS_V3.md \
  --output-root historical_batches/development_tranche_v3/catalyst_manifests \
  freeze
```

After that manifest is committed and pushed, rerun the same common arguments
with `inspect <manifest> --status
historical_batches/development_tranche_v3/catalyst-contract-status.json`. Source
acquisition remains forbidden until the independently inspected zero-state is
also committed and pushed.
