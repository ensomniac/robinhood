# Historical Batch Status

This directory contains privacy-safe, machine-readable status for frozen
historical selections. Raw replay inputs remain under the Git-ignored
`historical_data/` directory.

`historical_bundle_builder.py` writes collection state here by default. It
records ready and blocked dates, typed failures, fallback recoveries, zero
substitutions, and the fixed engineering acceptance gate. It never records API
keys, account data, broker identifiers, or raw provider payloads.

`historical_learning.py run --selection ... --ready-only` updates the batch
status before replay and after every completed date. A rerun recognizes archive
folders as already completed. Missing or invalid selected dates remain blocked;
the runner never replaces them.

The infrastructure acceptance target is at least 16 validation-grade dates from
20 newly randomized dates, zero date/symbol substitutions, and zero cascade
errors. Passing it does not promote the strategy or relax the replay fidelity
contract.
