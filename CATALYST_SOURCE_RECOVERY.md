# Catalyst Source Recovery

Status: exact SEC recovery manifest frozen under hash
`2f4258b23e2aac76529754f51e5df6618db3e655d01491bb7cb01e14a5b0d374`;
commit it before provider access

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

## Claim Boundary

Successful response recovery proves only that the exact SEC-operated filing
content was captured under the frozen request contract. It does not establish
that the filing CIK equals the target issuer CIK, that the filing was available
before 09:35 ET, that it is relevant or directionally positive, or that it
supports alpha. Those decisions require a separately frozen, outcome-blind SEC
semantics review. Secondary news never replaces a missing primary source, and
target outcomes remain inaccessible.
