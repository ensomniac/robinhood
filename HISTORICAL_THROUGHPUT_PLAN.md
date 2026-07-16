# Historical Throughput Plan

Updated: 2026-07-16

## Outcome

The historical system should process large, randomly frozen date sets without
changing the strategy, looking at target outcomes during discovery, substituting
failed dates or symbols after freeze, or weakening the evidence contract. The
near-term target is a timed 100-day run with stage and provider telemetry. The
long-term target is fast repeated strategy evaluation over immutable local data,
where IBKR or another source is contacted only when a verified artifact is not
already present.

## Measured Bottleneck

The Python evaluator and JSON validation are not the current bottleneck. The
network request graph is.

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

### D. CPU-parallel replay only after data is local

Once immutable bundles are present, strategy evaluation is independent by date
and can use process-level parallelism safely. This must be a separate phase from
provider collection. It should produce per-date temporary results and merge them
into the public ledger in frozen date order under one writer, preserving audit
and idempotency.

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
