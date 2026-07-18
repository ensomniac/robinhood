# Historical Throughput Plan

Updated: 2026-07-16

## Outcome

The historical system should process large, randomly frozen date sets without
changing the strategy, looking at target outcomes during discovery, substituting
failed dates or symbols after freeze, or weakening the evidence contract. The
timed 100-day run is complete with stage and provider telemetry. The ongoing
target is fast repeated strategy evaluation over immutable local data, where
IBKR or another source is contacted only when a verified artifact is not already
present.

## Measured 100-Day Result

The independently frozen run used seed `5387420213521377743`, retained the
original 100 dates, and finished with 95 validation-grade replays, five explicit
IBKR quote-boundary blockers, zero substitutions, and zero cascade errors. It
passed the 80% engineering-yield gate at 95%. The complete selection-through-
replay workflow took 19,681.079 seconds (5h 28m 1.08s).

The cold path confirms that network acquisition is the bottleneck:

- preflight examined 3,040 cached or cold symbol/date records in 9,190.685
  seconds, submitted 4,612 IBKR requests, and spent 3,673.363 seconds waiting on
  pacing;
- the main bundle-collection segment took 3,779.680 seconds for 1,623 completed
  requests and spent 817.962 seconds waiting on pacing;
- the first pass produced 84 dates; a 139.058-second recovery pass rebuilt stale
  derivation caches and converted invalid-but-rejectable candidate inputs into
  safe hard rejects, recovering 11 more dates;
- the final five blocked dates all lack a required historical bid/ask tick at
  the frozen evaluation boundary. They were not substituted or approximated.

The warm path is the intended strategy-development loop. The exact 100-date
preflight used 3,040 cache hits, zero misses, zero provider requests, and 0.704
seconds. A clean local replay evaluated 950 frozen signals across the 95 valid
days in 13.328 seconds, or 7.13 days per second, with zero provider calls. The
machine-readable result is retained in
`historical_batches/2026-07-16-one-hundred-days-speed-test.json`.

None of the 950 signals passed every production gate. This is a strategy
validation result, not an engineering failure, and the strategy remains
`UNVALIDATED` with no live broker actions performed.

## Measured Bottleneck

The Python evaluator and JSON validation are not the current bottleneck. The
network request graph is.

The 100-day telemetry also disproves contract lookup as the primary delay.
Contract details accounted for 990 of 4,612 submitted preflight requests but
only 29.161 of 14,049.332 summed request-seconds (0.21%). Historical bars used
the remaining 14,020.172 request-seconds, while the complete cached four-strategy
matrix ran in 2.003 seconds with zero provider calls. Repeated symbol proof is a
real secondary cost because it consumes pacing slots, but learning iterations
should prioritize the existing frozen corpus and treat new bar acquisition as a
separate corpus-expansion job.

The March 30 one-day replay provided a useful cold baseline:

- The ranked draft contained 77 names. Sixty ranks had to be examined to freeze
  ten viable names; six rows were rejected locally and 54 reached IBKR.
- Those 54 cold preflight cache entries accumulated 87.943 symbol-seconds.
  Thirty-eight failed the ADV gate and six failed ATR. Only ten needed opening
  history and final contract proof.
- The old graph issued 118 provider requests: 54 contract lookups, 54 daily-bar
  requests, and ten opening-history requests.
- The completed target-session raw files span 49.362 seconds from the first
  benchmark capture to the final candidate capture. Slow symbols blocked every
  later symbol in the serial loop.
- An exact preflight cache rerun is approximately one second. This proves that
  local serialization and validation are cheap relative to cold provider work.

A first four-worker experiment overlapped requests but issued the same 118-call
graph too aggressively for the historical farm. IBKR soft-throttled the stream;
the pass took 123.34 seconds. A subsequent two-worker pass used the reduced
74-call graph described below but ran inside the same throttling window and took
121.68 seconds. These are useful provider-behavior observations, not clean
speedup comparisons. The reduced graph's exact cached rerun used zero provider
requests and spent 0.038 seconds inside preflight (1.24 seconds including TWS
connection and process startup); the existing bundle-cache path spent 0.233
seconds inside the builder (1.25 seconds end to end). A clean cold benchmark
must start after the prior farm load has cleared and must retain request counts
alongside wall time.

## Constraints That Shape The Design

Interactive Brokers documents a maximum of 50 simultaneous historical-data
requests and recommends keeping the practical number much smaller. It also
warns that even one-minute-and-larger requests, whose old hard limit was lifted,
are subject to soft load balancing, throttling, and eventual disconnect if too
much data is requested. Historical bid/ask pages have their own cost and cannot
be inferred from OHLCV bars.

Therefore:

- Multiple independent Python processes are not the default. They would have
  separate, unaware rate limiters; duplicate connection overhead; no shared
  stop condition; and no deterministic first-failure boundary.
- One read-only TWS connection owns request IDs, callbacks, pacing, concurrency,
  error classification, and telemetry.
- A small worker pool overlaps server response latency. It does not attempt to
  defeat provider limits.
- Results are consumed in frozen input order even when callbacks complete out of
  order. Concurrency may change elapsed time, never evidence selection.
- Work is submitted one bounded batch at a time. A retryable provider failure or
  permanent date blocker prevents the next batch from launching. Already-started
  work may finish and is atomically cached for the resume.

References:

- <https://interactivebrokers.github.io/tws-api/historical_limitations.html>
- <https://ibkrcampus.com/campus/ibkr-api-page/twsapi-doc/>
- <https://interactivebrokers.github.io/tws-api/historical_data.html>

## Implemented Architecture

### 1. Request elimination before concurrency

Preflight now requests prior daily history first. A name that fails ADV or ATR
does not pay for explicit contract-details or opening-history calls. A surviving
name receives explicit US-stock contract verification and then the 28-day
opening-history request. Error 200 from the initial STK/USD historical request is
still a symbol-scoped unresolvable-security skip.

For the March 30 graph this reduces cold provider calls from 118 to 74:

- 54 daily histories,
- ten contract lookups for daily-gate survivors,
- ten opening histories for the same survivors.

The order does not change any eligibility rule. It only postpones work that
cannot affect an already-final rejection.

Contract-details requests are now additionally deduplicated across dates and
workers. The ignored `historical_data/contracts/` cache binds each result to the
exact STK request identity, verifies a content hash, expires positive metadata
after 30 days and error-200 metadata after one day by default, never stores
provider-wide failures, and single-flights concurrent same-symbol misses. The
100-day evidence contained 1,094 contract-proof occurrences across only 530
unique symbols, so an ideal cold pass under an unchanged provider truth could
avoid up to 564 repeated lookups. This is a request-count projection, not a
post-change wall-time benchmark.

### 2. Bounded deterministic workers

`historical_concurrency.py` provides a shared ordered, bounded execution
primitive. `historical_universe.py` uses it for ranked preflight and
`historical_bundle_builder.py` uses it for benchmarks and frozen candidates.

The CLI default is four workers, with `--workers` available for provider tuning.
The connection independently caps in-flight requests through
`IBKR_MAX_CONCURRENT_REQUESTS`, also defaulting to four. These are separate
controls: orchestration bounds speculative symbols, while the adapter bounds
actual provider requests.

Preflight can probe up to `workers - 1` names beyond the rank that supplies the
tenth accepted candidate. Those results are pre-session-only, atomically cached,
listed as speculative in the manifest, and never added to the frozen universe.

### 3. Correct global pacing

Concurrent callers reserve future send times while holding one lock. This closes
a race in which several threads could calculate the same delay, sleep together,
and all submit when they woke. The adapter still defaults to a conservative
0.4-second interval and now records:

- submitted/completed/failed requests by kind,
- configured and peak in-flight requests,
- cumulative pacing and slot waits,
- connection elapsed time.

TWS error 100 and equivalent message-rate text are classified as retryable
provider failures. They stop the stream and use the existing fresh-connection,
cache-resume path rather than becoming symbol fidelity failures.

### 4. Batch instrumentation

Preflight output records wall time, cache hits/misses, cold symbol-seconds,
worker count, and IBKR telemetry. Bundle collection reports connection profiles,
raw/bundle cache hits, cache misses, reused preflight histories, built bundles,
and end-to-end collection time.

This is necessary because wall time alone can mislead: a run may be slow because
it did more cold work, hit a provider throttle window, paged a quote-dense name,
or waited on a single long response.

Final collection telemetry is persisted in the atomic public batch status, not
only printed to the terminal. Incompatible ignored raw caches are refreshed once
under the current derivation contract. Optional fallback throttles no longer
interrupt or reconnect a healthy IBKR primary stream, and a fallback permission
failure disables that lane for the rest of the batch.

## Existing Single-Symbol Surface

The proposed “one symbol for one day” collector already exists:

```sh
python3 ibkr_historical.py candidate SYMBOL \
  --date YYYY-MM-DD \
  --evaluation-time HH:MM:SS \
  --output /path/to/raw.json
```

It collects the full regular session, prior opening volumes, daily bars, and
historical bid/ask evidence. The batch builder now calls the same logic through
bounded workers. Creating a second per-symbol implementation would duplicate
the fidelity rules and cache contract.

## Next Architecture Layers

### A. Automated point-in-time discovery

`historical_discovery.py` now starts from the independently frozen date set,
normalizes market-wide earnings-calendar results, maps supported NYSE/Nasdaq
symbols through official SEC data, joins 8-K acceptance/items metadata, screens
primary documents for strong dilution language, and emits a deep ranked draft
without target-session market prices. Daily indexes, submissions metadata, and
primary documents are immutable ignored cache entries. Discovery and SEC cache
telemetry are timed separately from IBKR collection.

One SEC registrant can expose common, preferred, and depositary tickers. The
builder keeps one deterministic representative ticker per CIK before preflight,
then requires IBKR `stockType=COMMON` for final acceptance. This prevents one
issuer from occupying several ranks and avoids sending HMDS history requests to
secondary instruments that can stall rather than fail quickly.

The 100-day cold-cache exercise exposed two important limits:

- all-market earnings plus every material 8-K expands to thousands of
  registrants that the strategy's ADV/ATR gates will reject;
- eager document screening scales with the entire reserve, even though
  preflight only needs enough clean names to freeze ten.

The production default for this strategy is therefore the connector's
high-market-cap earnings universe cross-checked against SEC filings, with an
80-name per-date reserve for large runs. A future refinement should screen
documents incrementally in rank order until each date has enough clean reserve
names. Exchange notices and a licensed historical scanner remain additive
discovery lanes when available; they must preserve timestamps, ranking scope,
and the no-target-price attestation.

Large preflight runs use `--continue-on-exhausted`: a sparse date is recorded as
blocked and later frozen dates continue. Provider-wide failures still stop the
stream, and neither behavior permits replacement dates or fewer than ten frozen
candidates.

### B. Immutable market-data store

Per-date JSON checkpoints are correct but not the final data-lake shape. Add a
provider-neutral, content-addressed store with interval coverage indexes:

- identity: provider, symbol/contract, field set, bar size, adjustment, RTH,
  start, end, schema, producer version;
- payload hash and atomic write;
- immutable historical policy and explicit current-data expiry;
- interval lookup so a larger cached range can satisfy a smaller request;
- single-flight locking so concurrent callers never download the same range;
- audit, inspect, coverage, size, and prune commands;
- no credentials, account data, or broker identifiers.

For repeated strategy iterations, bundles should be rebuilt from this immutable
store without provider calls. Strategy changes usually alter derivation and
evaluation, not the underlying bars and quotes.

### C. Provider lane separation

Keep IBKR primary under the current contract, but isolate provider lanes:

- bars and daily/opening history can use longer, efficient interval pulls;
- historical quote pages remain narrow around evaluation time;
- permanent IBKR gaps may use the configured Massive SIP fallback;
- transport or pacing failures never trigger a provider switch;
- a bulk/flat-file provider should be evaluated for large research corpora,
  because IBKR explicitly says it is not a specialized bulk market-data source.

Massive's official stock flat-file catalog is the concrete next candidate. It
publishes one daily aggregate file and one minute aggregate file across U.S.
equities, plus historical trade and quote files, specifically to avoid thousands
of per-symbol REST requests. The files are unadjusted, so an integration must
apply and audit split adjustments before it can satisfy this replay contract;
plan entitlements and quote-file size must also be verified with a small frozen
sample before changing the default provider lane. Do not infer S3 access from an
existing REST key.

References:

- <https://massive.com/docs/flat-files/stocks/overview>
- <https://massive.com/docs/flat-files/stocks/day-aggregates>
- <https://massive.com/docs/flat-files/quickstart>

### D. CPU-parallel research after data is local

Implemented in `historical_research.py`. Once immutable bundles are present,
one process-pool task loads one date and evaluates all selected versioned
strategies against the same frozen candidates. The parent is the only writer and
sorts isolated per-strategy/date shards deterministically. Generated shards are
ignored; compact aggregate results can be published under `research_results/`.

This research plane is intentionally separate from production historical
replay. It makes zero provider requests and never merges experimental results
into `SIGNALS.jsonl`, `trades/`, or maturity. Every plugin receives progressively
revealed immutable bar prefixes, so the runner enforces no-lookahead even for an
external plugin. Shared next-open entry, adverse slippage, stop-first ambiguity,
target, and force-flat semantics keep comparisons like-for-like. See
`HISTORICAL_RESEARCH.md` for the plugin and operator contracts.

The first evidence-bound matrix ran four strategies across the 100 requested
dates in 2.003 seconds with four processes versus 7.332 seconds with one, a 3.66x
speedup; both stable outputs were identical. It covered 95 dates and retained
the five known quote-boundary blockers. A pre-run identity audit also found four
older date bundles with the wrong frozen symbols. Bundle cache reuse now requires
an exact frozen-evidence hash, and the research runner independently compares
ordered bundle symbols with the evidence manifest. Repairing the corpus reused
926 raw candidates and needed only 36 fresh candidates, 92 IBKR requests, and
201.790 seconds. This reinforces the core bottleneck result: verified acquisition
and cache identity dominate; local multi-strategy evaluation is cheap.

## 100-Day Speed-Test Protocol

The timed run should record four clocks:

1. selection and discovery manifest construction;
2. preflight/freeze;
3. market-data collection and bundle validation;
4. replay, archive, ledger, audits, commit, and push.

It must also retain:

- requested, ready, blocked, completed, and already-archived date counts;
- exact selection seed and date set;
- no substitutions and no cascade errors;
- candidate buffer depth, examined ranks, skip reasons, and speculative cache;
- request counts by type, connection count, peak in-flight, pacing wait, provider
  failures, fallback recoveries, and cache hit rate;
- cold and warm/resume wall times;
- validation-grade yield and the existing 80% engineering gate.

The first run measures the complete current system. A second run over the exact
same frozen inputs measures reproducibility and cache-resume speed. Strategy
performance conclusions remain forbidden unless the resulting evidence meets
the normal bundle and sample contracts.

## Acceptance Criteria

The throughput work is complete only when all of the following hold:

- Four-worker and one-worker runs freeze identical symbols in identical order
  from the same draft and qualification rules.
- No preflight request observes target-session prices.
- A provider-wide error launches no later bounded batch and remains a batch
  blocker.
- A permanent frozen-symbol failure blocks that date without substitutions or a
  cascade of false failures.
- Every completed or speculative immutable response is checkpointed atomically
  and reused only after identity and integrity validation.
- Clean cold benchmarks show request counts and elapsed time; warm reruns show
  zero provider requests for complete cached inputs.
- The 100-day public status has zero substitutions and cascade errors and makes
  blocked dates explicit.
- Full unit tests, Ruff, sensitive-data audit, strategy-ledger audit, lifecycle
  audit, and progress-history audit pass before publication.
