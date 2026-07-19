# Catalyst Source Recovery

Status: independently inspected `READY` under manifest hash
`2f4258b23e2aac76529754f51e5df6618db3e655d01491bb7cb01e14a5b0d374`;
all 24 accession-bound sources returned HTTP 200, the two generic pages were not
requested, and SEC semantics remain unclassified

## Purpose

`catalyst_source_recovery.py` performs the first ordered recovery step after the
33-pair source-semantics gate retained only three verified-positive pairs. It
selects exactly the 26 frozen SEC responses that ended in HTTP 403, reconstructs
their 37 joins to 31 point-in-time selected pairs, and separates 24
accession-bound sources from two generic browse pages. The generic pages are
retained as `NO_ACCESSION` and are never requested because the recovery contract
forbids search-page substitution.

Each qualifying source is normalized to an HTTPS `www.sec.gov` EDGAR archive
path under the same filing CIK and accession. Inline-XBRL wrappers are removed;
index pages become the accession's complete submission text file; filing
documents retain their exact accession-relative path. Redirects must remain on
canonical accession-bound SEC archive paths.

The collector declares `Ensomniac RobinhoodCodexResearch
ryan@ensomniac.com`, requests gzip/deflate, sends at most two requests per
second, retries only transport, HTTP 429, and HTTP 5xx failures, and checkpoints
each source. This is below the SEC's published maximum of ten requests per
second and follows its declared-bot header format. It also reuses the existing
public-address, redirect, byte-limit, and 20-GiB disk-reserve protections.

## Workflow

Freeze and commit the returned hash-addressed manifest before any request:

```sh
python3 catalyst_source_recovery.py freeze
```

Then collect and inspect only under that exact manifest:

```sh
python3 catalyst_source_recovery.py collect --manifest <manifest>
python3 catalyst_source_recovery.py inspect --manifest <manifest>
```

Exact URLs, accessions, dates, symbols, article joins, identities, responses,
and errors remain in the external historical store. Git receives only hashes,
counts, lifecycle state, and aggregate response dispositions.

Because many recovered filing documents do not embed their EDGAR acceptance
header, `catalyst_sec_filing_metadata.py` performs a second accession-bound
metadata step. It freezes the same 24 accessions and retrieves only each
accession's canonical SEC filing-detail page. That page can supply
timestamp/CIK-shaped fields for a later review without substituting a filing,
opening an EDGAR search, or reading outcomes. Its collector must likewise be
committed before its manifest is frozen and its manifest committed before
requests.

The exact metadata manifest is frozen under hash
`46864d4285439be3e61d44dc98779b288a8bdd2b6f6f436be22ba812fe385bd2`
and is independently inspected `READY`. All 24 accession-detail pages returned
HTTP 200; every page contains both an acceptance-datetime candidate and
CIK-shaped identity evidence. These remain candidates until the frozen semantic
review accepts them against each pair's 09:35 cutoff and target CIK.

`catalyst_sec_semantics.py` implements that final SEC recovery gate. Manifest
`db9c81e3b8dd8c5d6cce78dc66247f7c0f8e364773e52a2463c3d65964e5d758`
now freezes the adjudication contract before extraction. Its
`freeze` command binds the recovered-source and filing-detail artifacts, the
exact 26-source/37-join/31-pair selection, the nine directory-CIK-match joins,
the parser implementations, the New York acceptance-time rule, the event
taxonomy, and the one-terminal-reason precedence before extraction. The
filing-detail page's own CIK evidence must also contain the point-in-time target
CIK; a matching archive directory alone is insufficient. Only an exact-CIK
join accepted no later than 09:35 ET can reach private relevance and event
review. Financing or dilution conflict is resolved before any positive label.

The remaining workflow is deliberately split across commits:

```sh
python3 catalyst_sec_semantics.py extract --manifest <manifest>
python3 catalyst_sec_semantics.py review --manifest <manifest>
python3 catalyst_sec_semantics.py review --manifest <manifest> \
  --review-input learning_runs/production_validation/sec-semantics-review.json
python3 catalyst_sec_semantics.py inspect --manifest <manifest>
```

The generated review file and all row-level context stay ignored. The public
result contains only terminal counts, hashes, a conservative maximum combined
positive count, and the next campaign phase. This gate never unlocks target
outcomes by itself.

Independent inspection is now `READY`. Five of the 37 joins reached verified
positive primary semantics, two reached verified financing conflict, one was
nonmaterial, one was irrelevant, 25 failed exact document-CIK binding, and
three had no accession. The earlier source-semantic pass retained three
positives, so even the deliberately conservative no-deduplication ceiling is
only eight against the required 20. The capacity gate therefore remains closed,
no outcome contract is permitted, and the next ordered recovery step is the
exact frozen retry of captured transport failures.

## Captured Transport-Failure Retry

`catalyst_transport_recovery.py` implements that next step. It reconstructs
exactly the 13 original `source request transport failure` records, their 13
pair/source joins, and six point-in-time pairs from the frozen capture,
discovery, and identity artifacts. Every row is an issuer-host candidate. No
other failure, URL, article, date, symbol, or source may enter the retry set.
Manifest `7837dd652471f90c578a829d2d59c677142ad15d75527e5af4171bb8b46261d9`
now freezes that exact request set and collector before network access.

The collector retries the same exact URL at a fixed maximum rate of two request
starts per second. It retains the original public-address validation, redirect
revalidation, timeout, response-size limit, and user agent; only transport,
HTTP 429, and HTTP 5xx failures are retryable. Every terminal source is
checkpointed, the 20-GiB reserve is checked before requests and response writes,
and neither provider nor secondary-source substitution is allowed.

```sh
python3 catalyst_transport_recovery.py collect --manifest <manifest>
python3 catalyst_transport_recovery.py inspect --manifest <manifest>
```

As with the SEC branch, the implementation must be committed before `freeze`,
and the returned manifest must be committed before `collect`. Successful byte
recovery does not prove issuer ownership, issuer binding, causal time, or event
direction. Those decisions belong to a later frozen canonical issuer-page and
document-chain contract.

Collection and independent inspection are now `READY`: all 13 same-URL retries
again ended in the exact transport-failure class, no HTTP response or response
byte was received, and no substitution occurred. This rules out another direct
retry as a useful next step. The campaign advances to canonical issuer-page and
document-chain recovery while retaining these failures in the denominator.

## Claim Boundary

Successful response recovery proves only that the exact SEC-operated filing
content was captured under the frozen request contract. It does not establish
that the filing CIK equals the target issuer CIK, that the filing was available
before 09:35 ET, that it is relevant or directionally positive, or that it
supports alpha. Those decisions require a separately frozen, outcome-blind SEC
semantics review. Secondary news never replaces a missing primary source, and
target outcomes remain inaccessible.

The inspected recovery captured 19,719,807 response bytes. Independent
inspection rebuilt the frozen selection and terminal counts and verified every
private response hash. The aggregate result is
`research_results/2026-07-19-catalyst-source-recovery-sec.json`; exact filing and
pair rows remain outside Git. SEC semantic inspection is published separately
at `research_results/2026-07-19-catalyst-sec-semantics.json`.
