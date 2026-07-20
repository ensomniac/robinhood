# Second-Tranche SEC Primary-Source Acquisition

Planned dataset: `dataset-development-sec-primary-sources-2026-07-20-v3`

Status: all 611 frozen SEC submissions requests are independently inspected;
supplemental files, primary documents, semantics, and outcomes remain locked

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Purpose

`development_sec_sources.py` is the network-free first acquisition stage for
the exact 1,871 pairs bound by the second-tranche source-semantics contract. It
resolves every pair by instrument, symbol, exchange, and point-in-time coverage
against the attested security master. Missing CIKs remain explicit unresolved
rows; missing or ambiguous security-master matches fail the contract.

The adapter now accepts an explicit dataset identity, source-semantics manifest,
selected-pair manifest, security master and attestation, strategy attestation,
source-contract document, manifest root, and inspection-status path. The dataset
identity scopes both the private identity/query map and target response
namespace, preventing cross-tranche reads or writes.

## Frozen Semantics

- Stage one is restricted to SEC-operated submissions JSON for the exact private
  CIK request graph.
- Forms are `8-K`, `8-K/A`, `6-K`, and `6-K/A`; the window begins four calendar
  days before the target and ends at 09:35 ET inclusive.
- Precise `acceptanceDateTime` is required. Filing date alone cannot prove a
  causal same-day timestamp.
- The compliant private user agent, four workers, global 0.15-second spacing,
  30-second timeout, four attempts, retry schedule, cache policy, and per-CIK
  terminal behavior are frozen before collection.
- Shared cache is allowed only with target-specific provenance. Failed or empty
  responses are not proof of no filing, and one failed CIK cannot abort other
  requests.
- Supplementary files and accession-bound primary documents require their own
  frozen and pushed manifests before access.
- Exact identities and requests remain outside Git. Provider/date/source
  substitution, selected-symbol detail, and outcomes remain forbidden.

## Frozen Identity Graph

Manifest `8dc5996b...464628` resolves all 1,871 pairs exactly. It retains 1,862
CIK-present pairs and nine explicit missing-CIK rows, producing 611 unique SEC
submissions requests. Private identity hash `896d2830...0c45b0`, request-graph
hash `1df8d19c...59dd90`, daily aggregate hash
`9ef85ec3...75f74b`, and implementation hash
`f27d3761...6615d2c` bind the result. The dataset-specific target response
namespace contained zero artifacts at freeze.

Independent inspection rebuilt all point-in-time joins, the exact private
identity and request graph, daily aggregates, strategy and source lineage,
pacing and cache rules, private namespace, capacity, and outcome lock. It
confirmed the same 1,871/1,862/9/611 counts and zero responses, substitutions,
or outcomes. The aggregate status is
`historical_batches/development_tranche_v3/sec-contract-status.json`.

This authorizes only a separately committed and pushed manifest-bound SEC
submissions collector. Supplemental submissions files and primary documents
remain inaccessible until their own later contracts are frozen and pushed.

`development_sec_submissions.py` accepts the explicit second-tranche dataset
identity and scopes every terminal wrapper and collection index beneath that
dataset's frozen private response namespace. It verifies that the manifest
dataset and target namespace agree, requires the collector and manifest to be
committed and pushed before provider access, uses the shared SEC cache first,
and preserves a terminal success or failure row for every one of the 611 exact
requests. Its inspector independently reparses and rehashes every successful
raw response and rebuilds aggregate request, filing, join, and failure counts.
It cannot fetch supplemental files or primary documents, publish private source
identity, classify catalyst semantics, or access outcomes.

The pushed collector completed all 611 frozen requests with 552 shared-cache
hits, 59 SEC downloads, zero failures, zero pending requests, and zero
substitutions. Independent inspection rehashed and reparsed 92,605,384 source
bytes and rebuilt 546 candidate-document requests, 572 pair/filing joins, and
490 pairs with at least one time-window candidate. The payloads expose 677
historical supplemental-file descriptors; they are discovery inputs, not
authorized requests. No supplemental file, primary document, source semantic,
selected-symbol detail, or outcome was accessed or asserted.

The next stage must independently reduce those 677 descriptors to exact frozen
pair-window overlap and commit that request graph before any supplemental SEC
access. Candidate documents remain unclassified until their exact union is
separately frozen, collected, and reviewed under the source-semantics contract.

`development_sec_supplemental.py` now accepts explicit source and supplemental
dataset identities plus exact published source status and inspection paths. It
independently rebuilds the v3 submissions index, scopes its private decision
graph and empty-response preflight to the v3 source namespace, and retains
invalid descriptor ranges conservatively. A read-only preflight selects nine
exact window-overlap requests and excludes 668 descriptors whose filing ranges
provably cannot intersect any frozen pair window. Commit and push this
implementation before writing the private graph or manifest; the nine-request
manifest must then be independently inspected and pushed before provider access.

Manifest `78827454...39bc9d` now freezes those nine requests and all 677 exact
descriptor decisions from the pushed implementation. It binds private contract
hash `89eceaf1...5377d4`, request-graph hash `896f6b8d...64c3e4`, decision hash
`cdb5647a...e89a1`, unchanged source and strategy lineage, the 20-GiB reserve,
and zero target supplemental responses. It must be committed and pushed before
independent inspection; inspection still does not authorize provider access.

Independent inspection now rebuilds manifest `78827454...39bc9d` as
`FROZEN_READY`: all 677 decisions, nine selected requests, 668 exclusions,
private hashes, source lineage, implementation hashes, capacity, privacy, and
outcome locks match, and the response namespace remains empty. The aggregate
status is `historical_batches/development_tranche_v3/sec-supplemental-contract-status.json`.
Provider access still requires a separately committed and pushed collector.

`development_sec_supplemental_collection.py` now accepts the explicit v3
supplemental and source dataset identities, verifies their manifest relationship
and private namespace, and scopes every response wrapper and index beneath the
v3 source response root. It consumes only the nine selected requests, uses the
shared SEC cache first, isolates provider and parser failures, resumes from
revalidated atomic wrappers, and independently reparses each raw response.
Primary documents, semantic classification, private identities, and outcomes
remain inaccessible. Commit and push the collector and tests before provider
access.

The pushed collector completed all nine requests with three shared-cache hits,
six SEC downloads, zero failures, zero pending requests, and zero substitutions.
Independent inspection rehashed 3,090,677 raw bytes and retained 11 additional
time-window document candidates and 11 joins across nine pairs. These candidates
remain unclassified; no primary document, source semantic, selected-symbol
detail, or outcome was accessed. The next network-free stage must union these
with the 546 main-submissions candidates, preserve every pair join, and freeze
the exact accession-bound document graph before any document request.

`development_sec_documents.py` now accepts explicit document, source, and
supplemental dataset identities plus all six exact published manifest/status/
inspection inputs. It independently rebuilds both private collection indexes,
validates every CIK/accession/primary-document tuple against its SEC-operated
URL, fails closed on conflicting duplicates, preserves every source-specific
pair join, and scopes its private graph and empty response preflight to v3.
A read-only preflight produces 557 unique document requests from 557 candidate
observations, 583 source-specific joins, and 499 affected pairs, with zero
target responses. Commit and push this implementation before writing or
freezing that graph.

## Runbook

Commit and push the implementation before freezing the identity graph:

```sh
python3 development_sec_sources.py \
  --dataset-id dataset-development-sec-primary-sources-2026-07-20-v3 \
  --source-contract historical_batches/development_tranche_v3/catalyst_manifests/dataset-primary-source-semantics-contract-2026-07-20-development-v3-412efc72a74d337d161c8f672c6692cbc1a86fa7befecacf31e3e605a75043b8.json \
  --selected-manifest historical_batches/development_tranche_v3/selected_pair_manifests/dataset-selected-candidate-contract-2026-07-20-development-v3-f00e8393391d8daee026e326d98643d8f07e2f38f847e4b257c6caa1b73c8897.json \
  --security-master historical_batches/development_tranche_v3/security-master.jsonl \
  --security-master-source historical_batches/development_tranche_v3/security-master-source.json \
  --strategy-source historical_batches/scanner_expansion/production-strategy-source.json \
  --source-contract-doc DEVELOPMENT_CATALYST_CONTRACT_V3.md \
  --output-root historical_batches/development_tranche_v3/sec_manifests \
  freeze
```

The frozen manifest must be committed and pushed before its separate
`inspect <manifest> --status
historical_batches/development_tranche_v3/sec-contract-status.json` pass. The
inspected zero-response state must then be committed and pushed before any SEC
request. This stage cannot establish a verified catalyst or alpha result.

After the collector implementation is committed and pushed, run the exact
submissions-only graph with:

```sh
python3 development_sec_submissions.py \
  --dataset-id dataset-development-sec-primary-sources-2026-07-20-v3 \
  --manifest historical_batches/development_tranche_v3/sec_manifests/dataset-development-sec-primary-sources-2026-07-20-v3-8dc5996b6b88d52fc8e08efc9932c6a1db30a3ec61a2ef09d7f22346bb464628.json \
  collect \
  --status historical_batches/development_tranche_v3/sec-submissions-status.json

python3 development_sec_submissions.py \
  --dataset-id dataset-development-sec-primary-sources-2026-07-20-v3 \
  --manifest historical_batches/development_tranche_v3/sec_manifests/dataset-development-sec-primary-sources-2026-07-20-v3-8dc5996b6b88d52fc8e08efc9932c6a1db30a3ec61a2ef09d7f22346bb464628.json \
  inspect \
  --status historical_batches/development_tranche_v3/sec-submissions-status.json \
  --output research_results/2026-07-20-development-sec-submissions-inspection.json
```

After committing and pushing the supplemental freezer, freeze its exact graph:

```sh
python3 development_sec_supplemental.py \
  --dataset-id dataset-development-sec-supplemental-2026-07-20-v3 \
  --source-dataset-id dataset-development-sec-primary-sources-2026-07-20-v3 \
  --source-manifest historical_batches/development_tranche_v3/sec_manifests/dataset-development-sec-primary-sources-2026-07-20-v3-8dc5996b6b88d52fc8e08efc9932c6a1db30a3ec61a2ef09d7f22346bb464628.json \
  --source-status historical_batches/development_tranche_v3/sec-submissions-status.json \
  --source-inspection research_results/2026-07-20-development-sec-submissions-inspection.json \
  --source-contract-doc DEVELOPMENT_SEC_SOURCES_V3.md \
  --output-root historical_batches/development_tranche_v3/sec_supplemental_manifests \
  freeze
```

Commit and push the resulting manifest before running `inspect` with the same
arguments and `--status
historical_batches/development_tranche_v3/sec-supplemental-contract-status.json`.
Inspection is still network free and does not authorize a supplemental request
until its aggregate zero-response state is separately committed and pushed.

After committing and pushing the collector, run and inspect only the frozen
nine-request graph:

```sh
python3 development_sec_supplemental_collection.py \
  --dataset-id dataset-development-sec-supplemental-2026-07-20-v3 \
  --source-dataset-id dataset-development-sec-primary-sources-2026-07-20-v3 \
  --manifest historical_batches/development_tranche_v3/sec_supplemental_manifests/dataset-development-sec-supplemental-2026-07-20-v3-7882745425690f2436ceaada8efdd937f137caeedb784c2c0bf3c6ded839bc9d.json \
  collect \
  --status historical_batches/development_tranche_v3/sec-supplemental-status.json

python3 development_sec_supplemental_collection.py \
  --dataset-id dataset-development-sec-supplemental-2026-07-20-v3 \
  --source-dataset-id dataset-development-sec-primary-sources-2026-07-20-v3 \
  --manifest historical_batches/development_tranche_v3/sec_supplemental_manifests/dataset-development-sec-supplemental-2026-07-20-v3-7882745425690f2436ceaada8efdd937f137caeedb784c2c0bf3c6ded839bc9d.json \
  inspect \
  --status historical_batches/development_tranche_v3/sec-supplemental-status.json \
  --output research_results/2026-07-20-development-sec-supplemental-inspection.json
```
