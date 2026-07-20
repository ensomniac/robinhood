# Disjoint SEC Primary-Source Acquisition

Planned dataset:
`dataset-development-sec-primary-sources-2026-07-19-v2`

Status: manifest `ecabc373...83d264` independently rebuilt `FROZEN_READY`; no
SEC target response has been requested or read by this dataset

Production champion: `2026-07-15-orb-v3`, unchanged and `UNVALIDATED`

## Purpose

`development_sec_sources.py` is the network-free first acquisition stage for
the exact 1,906 pairs bound by the disjoint primary-source semantics contract.
It resolves every pair by instrument, symbol, exchange, and point-in-time
coverage against the attested security master. An absent CIK is retained as an
explicit unresolved row; an absent or ambiguous security-master match fails the
contract.

Exact symbols, identities, CIKs, windows, and request URLs remain under
`LOCAL_HISTORICAL_DATA_ROOT`. Public artifacts contain only counts, hashes,
lineage, pacing rules, privacy boundaries, and claim limits.

## Frozen Request Semantics

- Stage one is restricted to SEC EDGAR submissions JSON at an exact private
  request graph derived from the point-in-time CIK map.
- Relevant forms are `8-K`, `8-K/A`, `6-K`, and `6-K/A`. The window begins four
  calendar days before each target at 00:00 ET and ends at 09:35 ET inclusive.
  A precise `acceptanceDateTime` is required; filing date alone cannot prove a
  causal same-day timestamp.
- The compliant user agent, four-worker client, global 0.15-second minimum
  spacing, 30-second timeout, four attempts, exponential retry schedule, cache
  namespaces, and per-CIK failure behavior are hash-bound before collection.
- Existing shared SEC cache is read first but does not count as a dataset
  response until target-specific provenance is recorded. A failed or empty
  response is never treated as proof of no filing, and one failed CIK cannot
  abort unrelated requests.
- Supplementary submissions files and accession-bound primary documents may be
  discovered by stage one but cannot be requested until their own exact
  manifests are frozen, inspected, committed, and pushed.
- No issuer, symbol, source, provider, or date substitution is allowed. Target
  outcomes and post-entry fields remain locked.

## Runbook

Commit and push the implementation and tests before freezing the request graph:

```sh
python3 development_sec_sources.py freeze
```

Then independently rebuild the exact point-in-time mapping and zero-response
state:

```sh
python3 development_sec_sources.py inspect \
  historical_batches/development_tranche_v2/sec_manifests/<manifest>.json
```

Commit and push the inspected manifest and public status before contacting SEC.
The later collector must consume only that committed manifest, record a terminal
result for every unique CIK, and keep all response bodies outside Git.

`development_sec_submissions.py` implements that bounded collector and its
independent inspector. It refuses provider access unless its own bytes and the
frozen manifest are present at a clean path in `HEAD` and `HEAD` equals the
pushed upstream. Each CIK receives an atomic private success or failure wrapper;
one failure does not stop other CIKs, and a resumed run skips only wrappers that
revalidate against the request, manifest, collector, and retained source hash.

The collector reads the shared SEC cache before downloading, records target-
specific cache provenance, rehashes raw response bytes, requires the response
CIK to match, and applies the frozen precise acceptance-time window. It derives
candidate accession-bound document requests and supplementary submissions
requests but cannot fetch them. Independent inspection re-reads every raw
success, rebuilds every join and aggregate, reconciles failures and the missing-
CIK row, and verifies the public/private boundary.

After the collector implementation is committed and pushed, stage one runs as:

```sh
python3 development_sec_submissions.py collect
python3 development_sec_submissions.py inspect
```

The derived supplemental and document graphs still require their own committed
manifests before any additional provider request.

## Inspected Submissions Result

The committed collector completed all 605 frozen submissions requests: 456
shared-cache hits, 149 SEC downloads, zero failures, zero pending requests, and
zero substitutions. It retained and independently rehashed 92,978,985 source
bytes. Precise acceptance-time filtering identified 590 unique candidate
primary documents, 620 pair/filing joins, and 508 pairs with at least one
candidate. These are discovery candidates, not verified positive catalysts.

The main submissions responses also exposed 689 historical supplemental-file
descriptors. A private preflight shows most are outside every target's four-day
window; a separate network-free freezer must independently apply those frozen
windows and commit the exact reduced supplemental graph before any such file is
requested. No supplemental file, primary document, or outcome has been read.

`development_sec_supplemental.py` implements that freezer. It independently
rebuilds all main-submission wrappers and source hashes, then intersects each
descriptor's `filingFrom`/`filingTo` range with the exact private pair windows.
An invalid or missing descriptor range is included conservatively; a provably
non-overlapping range is excluded. The real preflight retains six exact requests
and excludes 683 older-history descriptors, with zero target supplemental
artifacts.

The freezer also anchors the submissions collector to the immutable collector
commit recorded in the inspected result. A later documentation/result commit is
not mistaken for the original collector commit, while the collector bytes must
still match their recorded hash and pushed source. Commit and push this
implementation before freezing the six-request manifest.

Supplemental manifest `1e2900ae...bc558a` now independently rebuilds
`FROZEN_READY`. It binds all 689 descriptor decisions, exactly six overlap
requests, 683 exclusions, private contract hash `a6d5cc95...915df6`, request
graph hash `2baf6c43...15a6a4f`, decision hash
`6c1a78b7...fa7785`, unchanged source/strategy lineage, and zero target
supplemental responses. Commit and push the manifest and status before
requesting any of the six files.

`development_sec_supplemental_collection.py` implements the corresponding
pushed-source collector and independent inspector. It consumes only the six
private frozen requests, uses the shared SEC cache first, isolates every
provider or parser failure, and resumes from revalidated atomic wrappers. SEC
supplemental files are parsed as top-level columnar filing metadata; precise
acceptance time and the original 09:35 ET pair cutoff are re-applied before any
new accession-bound document candidate is retained.

Raw supplemental bytes, CIKs, symbols, accessions, and URLs remain outside Git.
The collector derives but cannot fetch primary documents, and it cannot access
outcomes. Commit and push the collector and tests before the six-file provider
pass.

The committed supplemental collector completed all six requests as SEC
downloads with zero failures, zero pending requests, and zero substitutions. It
retained and independently rehashed 2,113,173 bytes, adding seven time-window
candidate documents and seven pair/filing joins across four pairs. These remain
unclassified candidates. No primary document or outcome was accessed.

The next network-free stage must union and deduplicate the 590 main-submissions
candidates with these seven supplemental candidates, preserve every pair join,
and freeze the exact accession-bound document graph before requesting a document
body.

`development_sec_documents.py` now implements that network-free gate. It
independently rebuilds and rehashes both inspected collection indexes, requires
zero source failures, validates each CIK/accession/primary-document tuple against
its SEC-operated URL, fails closed on conflicting duplicates, and preserves each
source-specific pair join in the private graph. The current rebuild contains 597
unique document requests, 627 pair/document joins, and 512 pairs with at least
one candidate; the main and supplemental graphs have no overlapping document
URL in this corpus.

Exact source identities, joins, request paths, and later raw document bytes stay
outside Git. The public contract contains only counts, hashes, locks, and claim
boundaries. Commit and push the freezer and tests before it writes the private
graph or frozen manifest; then commit and push the inspected zero-response
manifest before any primary document request.

Primary-document manifest `73d7223a...ab8538` is now independently rebuilt
`FROZEN_READY`. It binds all 597 exact requests, 627 source-specific joins, 512
affected pairs, private contract hash `279cd141...f309dc`, request graph hash
`09a8bb48...aea5d`, join graph hash `bb9cae08...ff6a`, unchanged strategy and
source lineage, the 20-GiB reserve, and zero target responses. Commit and push
this zero-state before implementing or running the document collector.

development_sec_document_collection.py implements the corresponding
pushed-source collection and inspection gate. It consumes only the 597 private
frozen requests, uses exact URL-hash cache paths, preserves raw bytes outside
Git, rejects empty or SEC automated-access denial bodies, isolates failures,
and resumes from revalidated atomic wrappers. Independent inspection rehashes
every successful raw source; neither collection nor inspection extracts or
classifies document semantics.

The public collection surface contains aggregate terminal, cache, download,
failure, and byte counts plus graph and implementation hashes. CIKs, symbols,
accessions, URLs, joins, raw bodies, and provider errors remain private. Commit
and push the collector and tests before the first document request.

An inspected identity/request contract adds no verified catalyst, alpha,
confirmation, maturity, promotion, or production readiness by itself.

## Frozen Result

Manifest `ecabc373...83d264` resolves all 1,906 selected pairs exactly against
the attested point-in-time master. It retains 1,905 pairs with a CIK, one
explicit missing-CIK row, seven listing-scoped pair observations, and a private
605-request SEC submissions graph. Independent inspection rebuilt private
identity hash `3dcd96ea...7bc7f6`, request-graph hash
`5a48af6c...c31653`, and daily aggregate hash
`370ca3e3...a93d95e` under unchanged v3 rules hash
`00c3aa83...b5867`.

The dataset-specific response namespace remains empty. No submissions,
supplementary submissions file, primary document, selected-symbol detail, or
outcome was requested or read. The manifest and zero-state must be committed
and pushed before stage-one SEC submissions collection begins.
