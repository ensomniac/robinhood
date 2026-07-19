# 100-Date Dynamic Scanner Expansion

Planned dataset: `dataset-production-scanner-replay-2026-07-19-expansion-v1`

Status: point-in-time reference collection in progress; market-data contract not
yet frozen and no scanner result exists

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Frozen Date Selection

`historical_batches/scanner_expansion/selection-2026-07-19-100-days.json`
contains exactly 100 dates selected with seed `20260719` from the 123 H1-2026
sessions after excluding all 20 dates in the inspected scanner-v4 dataset. The
selection has zero overlap with those prior targets and permits no substitution.
Three otherwise eligible H1 dates remain unused rather than being silently
added after results exist.

The exact selection requires 133 target/lookback sessions. Of those, 113 have
hash-attested scanner-v4 inputs and 20 require new full-universe collection.

## Why Reuse Is Required But Not Free Evidence

Recollecting every overlapping full-universe session would repeat thousands of
provider requests and millions of canonical merges without adding information.
The generalized Alpaca collector can therefore import a prior session only when
the frozen source manifest has the identical endpoint, feed, raw-adjustment,
`asof=-`, regular-session, opening-window, and derived-index contracts. It binds
the source manifest hash and each source artifact hash.

The old campaign's symbol union is not assumed complete for the new targets.
For every reused session, the collector derives the old union from the old
hash-bound security master, computes the new master union, requests the exact
delta symbols, and merges those observations with the attested source index.
New sessions request the complete new union. This removes redundant work
without introducing survivorship omission.

Some reused source rows existed before this campaign was frozen. The manifest
must disclose that fact and state that no target outcomes were observed or
derived. This campaign can become selection-fidelity and development evidence;
preexisting input rows do not by themselves make it clean independent alpha
confirmation. Any later outcome contract must be frozen separately before its
selected-candidate targets are read.

## Ordered Acquisition

1. Collect Massive dated common-stock reference snapshots for all 100 selected
   dates. These contain identity and listing metadata, not target prices.
2. Build a new ignored point-in-time security master and publish only its source
   contract, counts, hashes, and requested dates.
3. Freeze the scanner manifest against the exact selection, calendar, unchanged
   scanner rules, immutable security-master snapshot, current collector bytes,
   v4 reuse manifest, 113 reusable dates, 20 new dates, and zero substitutions.
4. For each required session, import only attested compatible source rows,
   collect the new-union delta, or collect the complete union when no source
   exists. Persist every provider observation under
   `LOCAL_HISTORICAL_DATA_ROOT`.
5. Rebuild all 100 dynamic 09:35 rankings and run an independent inspection
   before registering `READY`.
6. Freeze the exact selected pairs into a separate catalyst/trigger/outcome
   contract. Acquire direct primary catalyst sources and evaluate unchanged v3
   before inventing or revising any strategy rule.

## Stop Conditions

- Any date overlap, manifest mutation, source-contract mismatch, missing reused
  attestation, source hash mismatch, target/master symbol omission, provider
  substitution, or date substitution stops the campaign.
- Missing bars remain explicit scanner exclusions; no interval is interpolated.
- Licensed symbol rows and full-universe indexes remain outside Git.
- A partial collection remains resumable and cannot be called `READY`.
- Strategy outcomes, proposals, and production configuration remain out of
  scope until the independent scanner and selected-candidate gates pass.

## Runbook

```sh
python3 scanner_replay.py --run-root learning_runs/scanner_expansion \
  collect-reference \
  historical_batches/scanner_expansion/selection-2026-07-19-100-days.json

python3 scanner_replay.py --run-root learning_runs/scanner_expansion \
  build-master \
  historical_batches/scanner_expansion/selection-2026-07-19-100-days.json \
  --output learning/security_masters/scanner-expansion-master.jsonl \
  --source-manifest historical_batches/scanner_expansion/security-master-source.json

python3 scanner_replay_alpaca.py freeze \
  --dataset-id dataset-production-scanner-replay-2026-07-19-expansion-v1 \
  --selection historical_batches/scanner_expansion/selection-2026-07-19-100-days.json \
  --calendar historical_batches/scanner_replay/session-calendar-2025-12-through-2026-06.json \
  --security-master learning/security_masters/scanner-expansion-master.jsonl \
  --security-source historical_batches/scanner_expansion/security-master-source.json \
  --reuse-manifest historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-19-v4-645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json \
  --output-root historical_batches/scanner_expansion/manifests
```
