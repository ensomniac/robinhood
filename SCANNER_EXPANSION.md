# 100-Date Dynamic Scanner Expansion

Planned dataset: `dataset-production-scanner-replay-2026-07-19-expansion-v1`

Status: all 100 point-in-time reference snapshots collected; security-master
identity hardening validated; market-data contract not yet frozen and no scanner
result exists

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

## Reference Identity Finding

All 100 Massive snapshots completed without substitutions and contain 5,225 to
5,307 active common-stock rows apiece. They contain listing metadata only; no
target price, scanner rank, trigger, or outcome was collected or inspected.

The larger sample exposed one real identity collision on April 6: Massive
listed both `ANAB` and temporary when-issued `ANABV` as active Nasdaq common
stocks. They shared a share-class FIGI but had distinct composite FIGIs. This is
valid source behavior: OpenFIGI's allocation rules state that a share-class FIGI
can link multiple traded-venue instruments and therefore does not identify one
listing. The builder now uses composite FIGI first and share-class FIGI only as
a fallback, preserving separately tradable listings without weakening the
master's overlap rejection. See
<https://www.openfigi.com/assets/local/figi-allocation-rules.pdf>.

The builder also validates a temporary master before atomically publishing it.
An identity collision can no longer leave an invalid output that appears ready.

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

The shared scanner engine now writes the campaign dataset identity, private
detail, and provider-bound public summary in one pass. The Alpaca adapter no
longer writes a misleading legacy-ID intermediate summary and then replaces it;
this removes an unused artifact and makes detail/summary identity disagreement
an inspection failure.

Some reused source rows existed before this campaign was frozen. The manifest
must disclose that fact and state that no target outcomes were observed or
derived. This campaign can become selection-fidelity and development evidence;
preexisting input rows do not by themselves make it clean independent alpha
confirmation. Any later outcome contract must be frozen separately before its
selected-candidate targets are read.

## Ordered Acquisition

1. Collect Massive dated common-stock reference snapshots for all 100 selected
   dates. These contain identity and listing metadata, not target prices.
   **Complete: 100/100.**
2. Build a new ignored point-in-time security master and publish only its source
   contract, counts, hashes, and requested dates.
3. Collect a complete Massive split-action range through June 30 and publish its
   count, query range, local path, and content hash without target prices.
4. Freeze the scanner manifest against the exact selection, calendar, unchanged
   scanner rules, immutable security-master and split snapshots, current
   collector bytes, v4 reuse manifest, 113 reusable dates, 20 new
   dates, and zero substitutions. Commit that manifest before market collection.
5. For each required session, import only attested compatible source rows,
   collect the new-union delta, or collect the complete union when no source
   exists. Persist every provider observation under
   `LOCAL_HISTORICAL_DATA_ROOT`.
6. Rebuild all 100 dynamic 09:35 rankings and run an independent inspection
   before registering `READY`.
7. Freeze the exact selected pairs into a separate catalyst/trigger/outcome
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
  status \
  historical_batches/scanner_expansion/selection-2026-07-19-100-days.json \
  historical_batches/scanner_replay/session-calendar-2025-12-through-2026-06.json \
  --security-master learning/security_masters/scanner-expansion-master.jsonl \
  --splits learning_runs/scanner_expansion/splits.json.gz

python3 scanner_replay.py --run-root learning_runs/scanner_expansion \
  collect-reference \
  historical_batches/scanner_expansion/selection-2026-07-19-100-days.json

python3 scanner_replay.py --run-root learning_runs/scanner_expansion \
  build-master \
  historical_batches/scanner_expansion/selection-2026-07-19-100-days.json \
  --output learning/security_masters/scanner-expansion-master.jsonl \
  --source-manifest historical_batches/scanner_expansion/security-master-source.json

python3 scanner_replay_alpaca.py \
  --run-root learning_runs/scanner_expansion collect-splits \
  --start 2025-12-10 --end 2026-06-30

python3 scanner_replay_alpaca.py freeze \
  --dataset-id dataset-production-scanner-replay-2026-07-19-expansion-v1 \
  --selection historical_batches/scanner_expansion/selection-2026-07-19-100-days.json \
  --calendar historical_batches/scanner_replay/session-calendar-2025-12-through-2026-06.json \
  --security-master learning/security_masters/scanner-expansion-master.jsonl \
  --security-source historical_batches/scanner_expansion/security-master-source.json \
  --splits learning_runs/scanner_expansion/splits.json.gz \
  --split-source historical_batches/scanner_expansion/split-actions-source.json \
  --strategy-source historical_batches/scanner_expansion/production-strategy-source.json \
  --reuse-manifest historical_batches/scanner_replay/manifests/dataset-production-scanner-replay-2026-07-19-v4-645f727fe0b596ee591a6ff32515b50883634b0e3294e83333ff6b83949b04b4.json \
  --output-root historical_batches/scanner_expansion/manifests
```

The pre-freeze status command reports reference, security-master, and split
readiness without treating legacy Massive S3 credentials or flat files as
requirements. Market rows must be collected only after freeze through the
non-S3 Alpaca adapter and its manifest-aware status command. The old flat-file
collector remains available for older contracts, but its credentials are not a
blocker for this campaign.
