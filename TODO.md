# Feature Backlog And Implementation Record

Sparse feature ideas are expanded here into explicit acceptance criteria before
implementation. Risk-bearing behavior must remain inside `AGENTS.md`; a backlog
item never grants live-trading authority or permission to weaken safety gates.

## Active Persistent Campaign

The production strategy remains `UNVALIDATED`. The completion contract,
resumable phases, waiting states, evidence sequence, and final integrity gate are
now machine-coordinated by `strategy_validation.py` under
`PRODUCTION_STRATEGY_VALIDATION.md`. The exact outcome-blind 33-pair catalyst
source-semantics gate is independently inspected `READY`: only 3 pairs were
verified positive, so the 20-pair capacity gate failed and returns remain
locked. The current bounded handoff is `SOURCE_RECOVERY`, beginning with the
frozen SEC 403 accession rows and then captured transport and issuer-chain
recovery. The SEC-only collector and tests are implemented; its exact
26-source, 31-pair manifest `2f4258b2...5b0d374` is independently inspected
`READY`: all 24 accession-bound requests returned HTTP 200 and the two generic
browse rows were not requested. The current bounded handoff is a separately
frozen, outcome-blind SEC acceptance-metadata stage followed by document-CIK,
relevance, direction, and financing-conflict review. The 24-accession filing
detail dataset is independently inspected `READY`: all 24 responses contain an
acceptance-datetime candidate and CIK-shaped identity evidence. The current
37-join semantic contract is independently inspected `READY` under
`db9c81e3...64e5d758`: five joins are verified positive, two are financing
conflicts, one is nonmaterial, one is irrelevant, 25 fail exact document-CIK
binding, and three have no accession. The conservative combined ceiling is only
eight positive pairs, below the required 20, so outcomes remain locked. The
current handoff is to freeze and commit the exact captured transport-failure
retry set before network access, then inspect it before issuer-chain recovery.
`catalyst_transport_recovery.py` now reconstructs exactly 13 failures, 13 joins,
and six pairs with fixed pacing, resumable checkpoints, no substitution, and
the 20-GiB reserve. Its exact manifest `7837dd65...b46261d9` is independently
inspected `READY`: all 13 retries remained transport failures with zero response
bytes. `catalyst_issuer_chain_recovery.py` now implements the next exact six-pair,
12-URL canonical-chain collector with a reviewed private plan, official-domain
redirect boundaries, resumable checkpoints, no substitution, and the same disk
reserve. The current handoff is to commit that implementation before freezing
its manifest. A wait, pause, or insufficient sample is progress state, not
completion.

## Completed 2026-07-19

### 16. Faithful Dynamic 09:35 Scanner Replay

Status: `dataset-production-scanner-replay-2026-07-19-v4` is independently
inspected `READY`; production strategy rules remain frozen.

- Collected all 118 frozen Alpaca raw SIP sessions without S3, substitution, or
  provider retries and retained every observation in the external canonical
  symbol/year/day store.
- Reconstructed 105,261 point-in-time common-stock evaluations across the exact
  20 dates, admitted 1,228 under the unchanged v3 universe gates, and retained
  389 selected pairs. The nine-name short day was not padded.
- Independently rehashed source artifacts and recomputed security-master
  populations, split factors, ADV, ATR, opening RVOL, dispositions, ranks, and
  shortlist hashes; reconciled 608,386 canonical documents back to source.
- Kept public evidence privacy-safe and aggregate-only. This closes universe
  selection fidelity, not catalyst, quote, execution, outcome, or alpha
  validation.
- Made frozen datasets first-class resumable learning objectives, corrected
  sparse/early-close cache reuse, and removed the terminally rejected reversal's
  unused 1,135-line shadow implementation and tests.
- Follow-on: the exact selected-candidate join and clean-trigger fidelity layers
  are now complete in items 17 and 18. Unchanged-champion evaluation remains
  blocked on the fields listed there.

### 17. Selected-Candidate Bar, News, And Trigger-Tape Join

Status: `dataset-selected-candidate-join-2026-07-19-v1` is inspected `READY`
for development-only pipeline fidelity; production strategy rules remain
frozen.

- Froze the exact 389 scanner-selected security-date pairs privately and
  published only hashes, dates, and aggregate counts.
- Passed a storage pilot with a 10 GiB reserve, then collected 389 candidate
  and 40 benchmark one-minute SIP sessions plus bounded news contexts.
- Located 325 crossing windows and retained 365,379 raw trades and 97,949
  quotes only around those windows; no full-session tick mirror was created.
- Measured 303 three-snapshot windows, 292 basic fresh/uncrossed windows, 177
  post-observation chase-cap passes, and a 0.1318% median usable spread.
- Hardened replay rules so bar highs, secondary news, top-of-book sizes, and
  current metadata cannot impersonate exact triggers, verified catalysts, full
  depth, or point-in-time truth.
- Follow-on: item 18 now closes dated CIK, bounded SEC primary-source candidate,
  and clean-condition mechanics. Directional catalyst classification and the
  remaining champion inputs are still required before 100+ new dates.

### 18. Point-In-Time Primary Sources And Clean SIP Triggers

Status: `dataset-selected-candidate-fidelity-2026-07-19-v1` is independently
inspected `READY` for development-only pipeline fidelity; production rules
remain frozen.

- Mapped all 389 frozen pairs to 283 dated issuer CIKs without current-identity
  backfill; 349 used ticker plus share-class FIGI and 40 used a unique dated
  ticker because the source row had no usable FIGI.
- Cached 283 SEC submissions documents and 100 unique primary filing documents
  externally. Eighty-one pairs had a time-valid filing candidate, including 18
  Item 2.02 candidates, and eight dilution conflicts were detected.
- Kept every filing unclassified and `verified_positive_catalyst=false`; primary
  source presence is not a favorable catalyst default.
- Transcribed Alpaca's tape-specific strictest-condition minute-bar rules and
  froze a narrower continuous-regular-cross contract with exhaustive tests.
- Replayed all 325 crossing windows: 197 first raw crosses were invalid, 195
  clean triggers moved later, only 155 passed the final chase cap, and median
  usable spread remained 0.13001%.
- Next: source-grounded directional classification for the 80 material primary
  candidates and explicit resolution of halt/tradability, resistance, sector,
  and depth inputs before champion returns or new-date alpha collection.

### 19. Champion Catalyst Classification And Official Halt History

Status: `dataset-champion-input-fidelity-2026-07-19-v2` is independently
inspected `READY` for development-only pipeline fidelity; production rules
remain frozen.

- Re-read 100 SEC complete submissions and enforced exact prior-session-close
  recency for 106 filing observations without observing target outcomes.
- Reduced the 389-pair corpus to 12 verified material catalyst pairs: 11 remain
  directionally unresolved and one has a high-precision positive issuer phrase.
  Six pairs are hard conflict rejects; 351 have no recent SEC primary source.
- Retained unrecognized filings and non-SEC event types as unresolved rather
  than using filing presence, document materiality, or secondary news as a
  favorable default.
- Collected and reparsed 966 official Nasdaq historical halt rows for all 20
  dates. None overlapped the 325 clean-trigger observation windows.
- Explicitly separated the historical active-listing/no-halt proxy from
  broker-specific tradability, which requires prospective broker qualification.
- Preserved the failed v1 source-shape contract and superseded it without date,
  pair, rule, or outcome substitution.
- Next: build an outcome-blind unchanged-champion readiness matrix for
  resistance, structural stop/noise, benchmark-relative strength, and
  conservative executable liquidity before exposing returns.

### 20. Outcome-Blind Unchanged-Champion Readiness

Status: `dataset-champion-input-readiness-2026-07-19-v1` is independently
inspected `READY` for development-only readiness evidence; production rules
remain frozen and no target outcomes were read.

- Rejoined all 389 exact pairs against catalyst, clean-trigger, quote, spread,
  chase, official halt, visible liquidity, completed-bar VWAP, SPY/QQQ, prior
  split-adjusted high, and opening-range stop inputs.
- Corrected the historical chase fidelity count from 155 upper-cap-only passes
  to 101 engine-compatible passes that also keep the final ask at or above the
  opening high.
- Found 53 non-catalyst execution-geometry passes and 44 that also pass the
  completed-bar market diagnostics.
- Found only 11 opening-low/0.10-ATR stop proxies inside the 0.8% cap and only
  50 known-overhead resistance proxies with at least 2.2% room; zero pairs pass
  every measured non-catalyst proxy together.
- Confirmed that the single verified positive primary-catalyst pair fails both
  spread and chase, leaving zero resolved hard-gate survivors before outcomes.
- Follow-on item 22 now supplies exact real-time structural invalidation/noise
  and technical resistance without tuning these dates. Next freeze 100+ new
  scanner dates with direct catalyst evidence and evaluate unchanged v3 first.

### 21. Condition-Aware SIP Bar And Prefix VWAP Fidelity

Status: `dataset-sip-bar-aggregation-validation-2026-07-19-v1` is independently
inspected `READY` for development-only input semantics; production rules remain
frozen and no target outcomes were read.

- Froze the complete Alpaca-documented tape A/B/C minute update matrix and the
  validation implementation before comparing outputs.
- Rebuilt 325 provider crossing-minute bars from 365,379 raw SIP trades with
  exact open, high, low, close, volume, and eligible-trade-count agreement;
  all 325 WAP values matched within $0.000001.
- Encountered zero unsupported trade conditions, then repeated the source join
  and every aggregate in a separate independent inspector.
- Proved that all 325 published-volume values differ from the WAP-eligible
  denominator; the median eligible/reported-volume ratio is about 0.8207.
- Added a fail-closed raw-prefix VWAP primitive so a future replay can compute
  session VWAP at the exact trigger timestamp without peeking to minute end.
- Follow-on item 22 now freezes the reproducible structural invalidation/noise
  and point-in-time resistance contracts. Collect the 100+ new scanner dates
  next; do not revisit target returns on these 20 development dates.

### 22. Point-In-Time Stop/Noise And Resistance Semantics

Status: `dataset-preentry-structure-fidelity-2026-07-19-v2` is independently
inspected `READY` for development-only input semantics; production rules remain
frozen and no target outcomes were read.

- Froze opening support at the exact five-minute high, ordinary noise at the
  greater of median observed spread and mean absolute completed-minute close
  increment, and structural invalidation just below that noise zone.
- Kept the existing 0.10-ATR stop floor, 0.8% maximum stop fraction, and 2.2%
  resistance-room threshold unchanged.
- Collected all 249 raw-SIP long-history requests, all 325 exact 04:00-09:30 ET
  premarket windows, and 2,268 Massive split events into the external store.
  Two hundred thirty-four identities have all 252 same-symbol sessions; 15
  retain explicit gaps that cannot prove price discovery.
- Derived 255 records; 153 stops fit the unchanged cap, 148 resistance records
  pass the unchanged room rule, and 89 pass both. Sixteen missing-snapshot and
  54 below-opening-high records remain unresolved.
- Preserved the failed v1 build contract and superseded only its missing-snapshot
  error path; v2 changed no dates, identities, thresholds, or semantics.
- Independently rebuilt all 325 terminal records, split factors, minute prefixes,
  aggregate counts, private hashes, and public privacy checks.
- Completed by sections 23 and 25: the 100-date scanner expansion is `READY`
  and direct-source acquisition reached a 33-pair structural ceiling. The
  frozen semantics gate retained only 3 verified-positive pairs, so returns and
  variants stay locked while source recovery proceeds.

### 23. One-Hundred-Date Dynamic Scanner Expansion

Status: completed on 2026-07-19 as independently inspected scanner-fidelity
evidence; no catalyst, trigger, execution, or outcome claim was made.

- Selected 100 H1-2026 dates with seed `20260719` after excluding all 20
  scanner-v4 targets; substitutions remain forbidden.
- Measured 133 required source sessions: 113 overlap the inspected v4 input
  contract and 20 require new full-universe market data.
- Generalized the Alpaca scanner adapter so dataset identity and security-source
  attestation come from the new frozen campaign rather than another copied
  collector.
- Added hash-bound source reuse with exact new-master symbol-delta collection;
  reuse can eliminate redundant provider work without omitting new or renamed
  listings.
- Bound split history and its public source attestation into the frozen scanner
  contract, and generalized the independent inspector to rehash both fresh and
  inherited session artifacts under any frozen scanner dataset identity.
- Published the hardened v3 configuration's exact file/canonical hashes and
  immutable source commit; the expansion manifest and inspector now bind that
  attestation before any target market row is collected.
- Explicitly disclose preexisting source rows and keep the campaign out of
  independent-alpha claims. Direct catalyst and outcome contracts remain
  separate future freezes.
- Removed the generic pre-freeze status path's stale Massive S3 credential
  blocker. It now reports the selected security master and splits explicitly
  and directs market collection to the frozen non-S3 Alpaca adapter; legacy
  flat files are observable but not required for this campaign.
- Completed 100/100 Massive dated reference snapshots without substitutions or
  target price access. The larger sample exposed simultaneous `ANAB`/`ANABV`
  listings that share a share-class FIGI but have distinct composite FIGIs.
- Corrected the master identity hierarchy to composite FIGI first and retained
  share class only as a fallback. Master validation now occurs before atomic
  publication, so a source collision cannot leave a false-ready artifact.
- Built and attested 5,692 master records across 5,620 identities, then collected
  and attested all 942 Massive split events covering the required range.
- Froze manifest `03a6eff...9e22b` before any new target market row. It binds a
  5,596-symbol union, 133 sessions, 113 hash-attested reuse inputs, exact symbol
  deltas, 20 new full-universe sessions, unchanged v3, and zero substitutions.
- Completed 133/133 source sessions with 2,808 Alpaca requests, zero retries,
  113 hash-attested reuse sessions, 2,147 exact delta symbols, and 20 new
  full-universe sessions.
- Independently recomputed all 100 dynamic rankings: 526,587 point-in-time
  evaluations, 6,024 eligible rows, and 1,987 selected ranks. Verified 686,079
  canonical regular/daily datasets and 628,899 exact opening datasets.
- Tightened canonical verification to require exact scanner-collection
  provenance when the persistent store also contains a legitimate overlapping
  general-purpose Alpaca dataset; no data was deleted or selected by recency.
- Next: freeze the exact 1,987 selected pairs into a separate point-in-time
  catalyst, trigger, and outcome contract. Evaluate unchanged v3 before
  proposing any new strategy rule.
- Froze the exact 1,987 private pairs in selection-only manifest
  `369efda1...22c140b` before any downstream field or outcome access. The
  freezer is source-generic, validates every rank/hash, is idempotent, and keeps
  symbols outside Git.
- Retargeted the research lock to the selected-pair evaluation. Next: implement
  and independently inspect the manifest-aware catalyst/trigger/outcome join,
  publishing unchanged-v3 gate attrition before returns.
- Froze base-join manifest `15d8baef...e2f6b` before network collection. The
  new adapter hash-binds and reuses the completed 389-pair collector without
  modifying or copying it; a pilot, 10 GiB reserve, exact 1,987-pair identity,
  and aggregate-only public outputs are mandatory.
- Next: pilot and collect one-minute bars, secondary news discovery, raw trigger
  tapes, and trigger-time NBBO. Then freeze primary-catalyst and clean-condition
  fidelity before any unchanged-v3 return evaluation.
- Completed that base join with zero errors: 1,987 candidate sessions, 200
  benchmark sessions, 850,029 one-minute bars, 1,460 raw trigger tapes, 1,331
  three-snapshot windows, and 1,279 preliminary fresh/uncrossed windows.
- The approximately 0.153% median observed snapshot spread is wider than v3's
  0.08% A+ and 0.10% operating gates. Keep those gates unchanged. Next: freeze
  and run primary-catalyst plus condition-aware trigger fidelity; still do not
  read returns.
- Froze that fidelity stage as manifest `ee9ba6f3...c780ce`, binding all 1,987
  exact pair-to-CIK mappings, 656 unique CIKs, 100 dated reference snapshots,
  1,460 source trigger windows, SEC cutoff rules, and validated SIP-condition
  semantics. Next: collect and inspect it; outcomes remain blocked.
- SEC collection completed: 696 time-valid primary filings, 591 pairs with a
  filing, 581 with a material candidate, and 40 dilution conflicts. Verified
  positive remains zero until source-grounded classification.
- Condition inspection found an explicit no-quote clean-cross window after
  1,025 completed records; the frozen legacy collector aborted on its second
  cache miss. Next: freeze a resumable quote-gap-aware trigger collector that
  records missing NBBO as a blocker and continues. Do not fabricate or switch.
- Froze that repair as manifest `44225398...e4d6ad4`. It binds the exact 1,460
  ordered trigger identities, checkpoints every 25 rows, records provider
  no-observation as `MISSING_TRIGGER_NBBO`, and retries only collection errors.
  Next: collect and independently inspect it before catalyst direction or returns.
- Completed and inspected all 1,460 windows with zero errors: 1,460 clean
  crosses, 1,340 three-snapshot windows, 1,267 fresh/uncrossed windows, 726
  chase-cap passes, and four explicit missing-NBBO blockers. Only 569 first raw
  crosses were clean; keep condition-aware timing unchanged.
- Next: classify SEC candidates for material positive direction and dilution
  conflicts, then join unchanged-v3 non-return gates. Returns remain blocked.
- Froze catalyst direction plus official halt-state manifest
  `043f21e4...a7b46a`, binding all 1,987 pair recency cutoffs and 1,460 clean
  trigger windows to the existing conservative classifier and Nasdaq source.
- Classified all 696 candidate filings and collected all 100 official halt
  dates. Only 14 of 1,987 pairs have verified-positive SEC primary direction;
  48 are conflict rejects, 183 have material-but-unresolved direction, 68 have
  another unresolved recent primary, and 1,674 have no qualifying recent SEC
  primary. None of 1,460 trigger windows overlaps an active official halt.
- Next: freeze and apply unchanged-v3 non-return gates to the 14 SEC-positive
  pairs. Separately design direct-source acquisition for issuer releases and
  attributed analyst actions; do not loosen the catalyst gate or read returns.
- Froze that exact 14-pair evaluation as manifest `3144c677...746da`, binding
  unchanged-v3 rules, source hashes, both readiness implementations, and an
  outcome-blind boundary.
- Independently rebuilt the full 14-pair non-return join: nine clean crosses,
  eight fresh three-snapshot windows, two spread passes, and zero joint
  spread-plus-chase survivors. No pair survives the resolved hard-gate cascade;
  none of eight stop proxies fits inside 0.8%.
- Do not read returns or loosen execution/risk gates from this tiny SEC-only
  slice. Next: increase direct primary-catalyst coverage for issuer releases and
  attributed analyst actions, then preregister another unchanged-v3 capacity
  join on disjoint evidence.
- Audited the cached secondary corpus: 1,416/1,987 pairs have articles; 7,391
  unique articles are all Benzinga URLs, 4,205 have summaries, and none retained
  full content. The 1,878 analyst-action keyword leads are high leverage but do
  not satisfy independent corroboration.
- Follow `CATALYST_EVIDENCE_ACQUISITION.md`: freeze Alpaca full-content
  enrichment, then causal direct-source and independent corroboration joins.
  Preserve absent or uncertain evidence as unresolved.
- Froze full-content enrichment as manifest `154e5831...4fff2e`, binding the
  exact 1,987 pairs, 100 dates, 7,391 articles, provider request, implementation,
  and outcome-blind boundary.
- Completed all 100 content queries with zero errors. All 7,391 unique articles
  returned; 4,205 contain 27,772,306 bytes of content and cover 1,183 pairs.
  The other 804 pairs remain without a content-complete lead.
- Next: freeze outbound-link and attributed-source extraction from the 4,205
  bodies, then collect direct issuer/regulator/analyst corroboration. Do not
  classify the Benzinga body itself as primary evidence.
- Froze offline outbound-link routing as manifest `14107372...de495a`, binding
  all content/source hashes, URL normalization, rejected platforms, and routing
  categories before derivation.
- Inspected all 4,205 bodies: rejected 42,836 article-level platform links;
  111 articles supply primary-routing leads covering 123 pairs and 293 supply
  corroboration-routing leads covering 227 pairs.
- Next: freeze exact candidate URLs and fetch them with source-specific pacing.
  Verify ownership, issuer binding, causal availability, materiality, direction,
  conflicts, and corroboration before any catalyst passes.
- Froze the exact 134 authority/exchange/potential-issuer URLs as manifest
  `409a807f...b41ef4`, with public-address validation, redirect checks, a 10 MiB
  response cap, per-URL checkpoints, and no classification authority.
- Completed all 134 terminal captures: 121 hashed responses totaling 23,793,420
  bytes and 13 transport errors. The responses include 84 HTTP 200, 35 HTTP 403,
  two HTTP 404, 107 HTML, 13 PDF, and one XML body.
- Next: freeze parsers for the captured formats and verify source ownership,
  issuer binding, original publication time, causal availability, direction,
  and conflicts. A successful response must not default to verified evidence.
- Froze offline HTML/header/redirect profiling as manifest
  `411be9d0...cb3151f`. Timestamp fields and canonical links are candidate
  structures only; none is accepted as causal or ownership evidence.
- Completed and independently inspected that profile. Among 71 successful HTML
  responses, publication candidates occur in recognized metadata on nine,
  JSON-LD on 14, and `<time datetime>` on 16; 24 expose canonical links. Zero
  timestamp candidates were accepted and no catalyst was verified.
- Next: preregister format-specific timestamp precedence, issuer-domain binding,
  source-ownership checks, and deterministic handling for the 35 forbidden,
  two not-found, and 13 transport-failure targets before source classification.
- Froze the pair-level source-readiness join as manifest
  `7ace3b39...568bbd0b`. Formal derivation may count response/format candidates
  across the exact 1,987 pairs but may not accept evidence or read returns.
- Inspected the pair join: 1,864 pairs have no primary route, 123 have a route,
  84 reach HTTP 200, 70 reach HTML, 15 reach PDF, and only 18 have a standard
  HTML timestamp candidate. Zero timestamps or catalysts were accepted.
- Keep returns locked. The 18 timestamp-candidate pairs are already below the
  20-signal provisional minimum before ownership, causality, v3 gates, and
  closed outcomes. Prioritize source-type-specific verification and bounded PDF
  parsing; do not invent or loosen strategy rules to manufacture capacity.
- Rendered and visually inspected the first page of all 13 successful PDFs;
  they mix issuer, SEC, court, government, weather, release, and presentation
  formats. Froze first-three-page structure profiling as manifest
  `b1f2bff3...96162e4c`; metadata and text dates remain unaccepted candidates.
- Inspected all 13 PDFs: 459 pages total, extractable front text in every file,
  and a month-name date candidate in every file. The PDFs touch 15 pairs with no
  overlap against the 18 HTML timestamp candidates, raising the structural
  ceiling to 33 pairs while accepting zero dates or catalysts.
- Independently inspected the exact source ownership, issuer binding, and
  source-type-specific date semantics for those 33 structural candidates. Only
  3 pairs are verified positive; one is verified negative, and the remainder
  fail ownership, binding, relevance, or timestamp proof. Keep returns locked
  while same-source recovery and disjoint acquisition seek sufficient capacity.
- Wrote the executable semantics contract in
  `CATALYST_SOURCE_SEMANTICS_PLAN.md`: exact 33-pair, 33-document, 38-join
  selection; source-specific ownership and issuer binding; timestamp precedence;
  same-day cutoff failure; direction/conflict taxonomy; terminal reason codes;
  attrition cascade; and a hard 20-row pre-return capacity gate.
- Full external-store audit passed with 844,904 files, 2,334,718 datasets,
  124,537 contexts, 6,605 symbols, 733 dates, and zero errors. About 79 GiB
  remains free; enforce the reserve before further bulk collection.

### 24. Fail-Closed Production Evaluator Relationships

Status: completed on 2026-07-19 without changing a numeric rule or strategy
version.

- Enforced the existing 0.08% A+ median-spread rule separately from the 0.10%
  operating spread rule. A 90-plus score with a wider clean spread is only
  `qualified`, and it is rejected while `UNVALIDATED` because pilots require A+.
- Added configuration invariants for strategy identity/schema, ordered session
  times, three-snapshot semantics, ordered spread limits, execution fractions,
  timeouts/heartbeat, risk bounds, universe minima and exact 14-session opening
  lookback, monotone maturity risk/allocation caps, score ranges, and every
  promotion metric and subcount relationship. `VALIDATED` gates cannot weaken
  `PROVISIONAL`, and its protection-latency budget cannot exceed the entry
  timeout. Malformed TOML now returns the engine's validation error contract
  instead of escaping as an unhandled parser exception.
- Added direct regression tests for the previously possible false A+
  classification and malformed configuration relationships.
- Promoted a planned stop at or above the observed bid from a warning to the
  existing hard outside-spread rejection promised by the strategy.
- Moved the unchanged three-loss, 2% rolling-five-session, and 4% strategy
  drawdown breakers from guard literals into `strategy_config.toml`; the guard
  now proves those values are its numeric source and renders the configured
  consecutive-loss limit in its blocker reason.
- Removed the redundant `candidate.opening_price` input from the evaluator and
  historical builder. The $5 gate now uses the validated first five-minute bar
  open, so conflicting duplicate fields cannot bypass the universe rule.
- Scoped the mixed-rules maturity blocker to closed, triggered return records.
  Rejected and no-trade history from an older hash remains in the ledger and in
  hash-specific diagnostics, but it cannot make later current-rule promotion
  permanently impossible; mixed performance samples still fail closed.
- Required every performance-bearing ledger row to be eligible, triggered,
  terminal, and a mode-matched `live` or `shadow` decision. The maturity filter
  independently enforces the same cohort, preventing rejected, missed, or
  malformed rows from inflating closed-signal counts or expectancy.
- Added cross-record ledger reconciliation: exactly one session per signal
  group, exact candidate and trigger counts, at most one executed decision,
  consistent `trade_taken`, and matching dates, modes, phases, hashes, and
  complete-capture claims. The existing 1,166-record ledger passes unchanged.
- Added daily ledger/archive alignment across public IDs, identity fields,
  context roles, terminal results, candidate counts, signal features, and net R.
  All 1,166 ledger rows and 1,166 archived contexts reconcile exactly.
- Hardened partial protection handling: `PROTECT_NOW` replaces an existing
  undersized stop rather than instructing a second full-size stop, and an
  inconsistent stop-count/covered-quantity snapshot activates flatten and
  reconciliation.
- Added explicit duplicate-entry and duplicate-exit states. Multiple flat-account
  entries are all canceled before one fresh logical order can be prepared;
  multiple exits during exposure activate the oversell kill switch. Partial-fill
  paths now cancel every remainder rather than referring to only one.

## Completed 2026-07-18

### 12. Bounded Edit And Learning Loop

Status: implemented by `LEARNING_PROGRAM.md`, `LEARNING_LOOP.md`, the public
`learning/` registries, `learning_loop.py`, `learning_cadence.py`, and the
evidence/data/statistics modules on 2026-07-18. External scheduler installation
remains an explicit operator action.

- Publishes a versioned eight-phase prompt subordinate to `AGENTS.md`: safety,
  ten working controls, bottleneck proof, at most five candidates, adversarial
  filtering, one apply slice, validation/benchmarking, and durable recording.
- Adds an explicit `learning` session mode with no broker authority and no
  automatic strategy activation.
- Inventories and preserves the dirty worktree, reads batch/evidence/research
  telemetry, and distinguishes cold data acquisition from fast local research.
- Validates machine-readable change plans, supports a deliberate no-op, refuses
  external/broker actions and `strategy_config.toml` edits, requires proposals
  to remain isolated, and caps the apply loop to one round.
- Keeps generated run and cadence state under ignored `learning_runs/`, while
  datasets, complete experiment families, negative results, and three-axis
  strategy evidence remain public and append-only.
- Adds selection-aware daily account statistics, point-in-time security and
  data-claim contracts, bounded hypothesis invention, paired champion/challenger
  evidence, degradation monitoring, and a finite daily-to-quarterly cadence.
- Tests cover prompt and authority boundaries, resume and lock behavior,
  evidence identity, no-lookahead/selection controls, strategy-transition
  refusal, no-op behavior, privacy, progress, and non-recursion.

### 13. Cross-Date Contract Resolution Cache

Status: implemented in `ibkr_historical.py` and `historical_universe.py` on
2026-07-18.

- Caches IBKR contract detail results by exact STK request identity with an
  integrity hash, atomic writes, public metadata classification, and telemetry.
- Resolved results expire after 30 days by default; error-200 unresolvable
  results expire after one day; transport, permission, pacing, and timeout
  failures are never cached.
- Concurrent same-symbol probes single-flight, process memory avoids repeated
  disk reads, and `--fresh-contracts` forces a current provider refresh.
- The 100-day evidence showed contract lookup is secondary, not primary: 29.161
  of 14,049.332 summed preflight request-seconds. The durable learning priority
  is local corpus reuse, then request elimination, then new collection only for
  missing coverage or confirmation.

### 14. Production-Aware Counterfactual Strategy Lab

Status: implemented by `historical_strategy_lab.py` and documented in
`HISTORICAL_RESEARCH.md` on 2026-07-18. Production rules remain frozen.

- Converts every executable stored signal into a research outcome while
  retaining the current production evaluator's exact rejection reasons. This
  restores labels and marginal gate evidence even when the complete production
  stack never trades.
- Verifies the public evidence manifest, exact scanner/candidate frozen hash,
  ordered symbols, full session/source attestations, bundle hashes, and
  production artifact isolation before publishing a result.
- Tests 15 predeclared one-trade policies across 0/5/10/20 bps per-side costs
  and 1/1.5/2/3R targets, with chronological stability phases, 20,000-sample
  base bootstraps, drawdown, concentration, stop geometry, and implied notional.
- On the 95-date usable corpus, only simple early reversal strength and the
  Item 2.02 earnings subset survived the severe-cost research gate. The
  earnings subset retained +9.813R and PF 1.282 at 20 bps per side but remained
  production-incompatible because its median structural stop was 2.088% and
  implied median notional was 11.4% at the current risk budget.
- Rejects tight-stop early ORB and the current VWAP-pullback definition,
  deprioritizes HOD continuation, and refuses to treat score/RVOL selection
  variants as winners after their 20 bps robustness failure.
- Freezes a public independent-confirmation contract without bypassing the
  production strategy-review cadence, editing `strategy_config.toml`, or
  making broker/provider calls.

### 15. Independent Reversal Confirmation And Retired Shadow Path

Status: historical confirmation completed and failed on 2026-07-18. The
challenger is `RETIRED`; the unused prospective shadow implementation was
removed, its Git history and evidence remain auditable, and production rules
remain frozen.

- Freezes a public hash-addressed manifest before target-session collection.
  It embeds at least 100 new dates, complete ordered Item 2.02 evidence,
  previously inspected run/date identities, the sole policy and plugin,
  execution grid, acceptance thresholds, deployment assumptions, production
  baseline, preregistration time, and implementation hashes.
- Rejects inspected-date overlap, altered manifests or implementations,
  pre-preregistration captures, phase relabeling, candidate reordering, date or
  symbol substitution, and any attempt to supply another policy or grid.
- Evaluates the independent sample at 5/10/20 bps and 1/1.5/2/3R, publishes
  every requested date/blocker and primary trade/exit, and applies the frozen
  date/signal, expectancy, PF, drawdown, bootstrap, halves, best-five, cost, and
  target gates. Failure stops without retuning.
- Adds structural-stop whole-share deployment at 0.25% account risk plus a 10
  bps reserve, with account compounding/log growth/drawdown, allocation and
  shortfall, stop slippage, binding cap, and largest-five-removal metrics.
  A separate cohort admits only naturally <=0.8% stops and needs 20 trades for
  inference; no stop is tightened and no allocation floor increases risk.
- The frozen 100-date confirmation stopped when the required 80 usable dates
  became mathematically unreachable. Its 22 primary trades returned -10.056R,
  -0.457R mean expectancy, 0.370 profit factor, and 10.492R maximum drawdown;
  every target and cost-stress cell was negative. No shadow sample was started,
  the hypothesis-specific shadow runner and its tests were deleted, and the
  failed contract may not be retuned.

## Completed 2026-07-16

### 11. Parallel Multi-Strategy Historical Research

Status: implemented by `historical_research.py`,
`historical_research_strategies.py`, and `HISTORICAL_RESEARCH.md` on 2026-07-16.

- Reuses complete local daily bundles with zero provider calls and hashes the
  public evidence manifest plus every available/missing bundle into a stable
  dataset identity.
- Binds every builder-produced date bundle to its exact frozen candidate and
  scanner evidence hash; the runner separately rejects ordered symbol mismatch.
- Evaluates versioned research plugins across dates in a bounded process pool.
  Each worker parses one date once for all strategies; one parent writer merges
  deterministic, isolated per-strategy/date shards.
- Enforces no-lookahead by exposing progressively revealed immutable bar
  prefixes and rejecting backdated decisions. All strategies enter on the next
  bar and share slippage, target, stop-first ambiguity, and force-flat rules.
- Makes unsupported requirements explicit instead of approximating absent
  depth, benchmark bars, or subminute data.
- Keeps generated shards outside Git and experimental signals outside the
  production ledger, trade archive, configuration, and maturity calculations.
- Produces a compact auditable result with per-strategy metrics and paired
  date-level comparisons. Tests prove worker-count determinism and production
  artifact isolation.

## Completed 2026-07-15

### 1. Daily Active Context And Date-Partitioned Archives

Status: implemented by `trade_lifecycle.py` and the lifecycle rules in
`AGENTS.md`.

- `trades/active/` contains only visible Markdown context for in-progress work on
  the current ET day. Historical mode may temporarily use a past date only while
  its validated replay is running and must leave no active files afterward.
- Rejected/stale ideas, flat trades, no-trade sessions, and completed sessions
  are terminal and move immediately to `trades/archived/YYYY_MM_DD/`.
- The lifecycle audit rejects stale or misnamed active context, terminal context
  left active, malformed archive folders, date mismatches, duplicate public
  context IDs, and archives without valid outcomes.
- Archive collisions fail closed; an existing day is never overwritten or used
  for a second historical simulation.

### 2. Terminal Outcome Dataset

Status: implemented by the embedded outcome schema in `trade_lifecycle.py`.

- Closing requires a concise result, primary reason, thesis result, what worked,
  what failed, at least one lesson, next-time actions, and relevant public
  metrics.
- The close operation adds both a readable Markdown review and the canonical JSON
  outcome to the same context before moving it. This keeps narrative and learning
  data together instead of maintaining a drift-prone side index.
- New outcomes must match the current strategy version/rules hash and cannot
  contain UUIDs, plaintext broker/account identifiers, secrets, or non-finite
  values. Historical outcomes remain auditable after later strategy versions.
- `STRATEGY_LEARNING.md` documents the schema, close command, audit, correction,
  and publishing workflow.

### 3. Evidence-Based Strategy Learning

Status: implemented safely by `strategy_learning.py`.

- Reports combine canonical ledger/maturity metrics, paired project-versus-EOD
  exits, archived outcome reasons, and bounded feature-cohort diagnostics.
- A review proposal is allowed only after both 20 new closed frozen-rule signals
  and 30 calendar days since version start or the last review, and only after
  the active catalyst-corpus research lock's required scanner dataset is
  independently inspected `READY` in its exact registered lane.
- Proposal creation rechecks the current research lock at write time; a stale
  previously unlocked report or fabricated `review_ready` mapping cannot race a
  newly activated fidelity lock.
- Generated changes are hypotheses for an evidence-backed delegated decision.
  The tool has no apply command and never edits `strategy_config.toml` or
  `AGENTS.md`.
- Any accepted change requires a separate production-change workflow, a new
  strategy version and rules hash, preserved prior sample, and preregistered
  confirmation evidence.
  This resolves the risk in automatic self-modification without discarding the
  requested continuous-learning capability.

### 4. Historical Learning Mode

Status: implemented by `historical_learning.py` and documented in
`HISTORICAL_LEARNING.md`.

- The workflow asks for a day count, randomly selects completed exchange trading
  days not already represented by an archive folder, retains the seed, and asks
  for an additional count after the requested batch.
- Each point-in-time replay bundle requires at least ten candidates; complete,
  non-interpolated regular-session minute bars; split adjustment; time-valid
  catalysts; full-universe capture; and historical quote/depth snapshots. Missing
  fidelity is a blocker rather than permission to invent data.
- Every candidate is evaluated with the frozen production engine. At most one
  trade is soft-executed, using earliest trigger and deterministic tie breaks.
  Later eligible signals are retained as daily-limit misses and excluded from
  return metrics.
- Replay is conservative and no-lookahead: same-bar stop/target ambiguity resolves
  stop-first, +2% runner and 3:50 force-flat rules are respected, and paired EOD
  shadow results are recorded.
- Candidate/session context is created in `trades/active/` during replay, then all
  records are outcome-completed, archived, and atomically appended to the signal
  ledger as a validated batch. Historical mode never calls broker order tools.
- Normal batches use ready-only replay: every valid date from the original
  selection runs immediately, missing dates remain explicit blockers, archived
  dates are idempotently skipped, and no substitute date is introduced.
- Atomic public batch status tracks selected, ready, replayed, blocked, and
  already completed dates. The engineering target is at least 80% validation-
  grade yield across 20 random dates with zero substitutions and cascade errors.

### 5. Agentic Session Mode Selector

Status: implemented by `session_mode.py` and the startup rules in `AGENTS.md`.

- Every new agentic trading workflow has five explicit choices: live,
  current-day shadow, historical replay, strategy review, or bounded repository
  learning. When one mode is clear, Codex selects it and continues without a
  redundant prompt.
- The numbered CLI also accepts stable named modes for automation and returns a
  machine-readable selection plus the next required safety step.
- Selection is declarative. Only live mode permits broker actions; no mode choice
  bypasses account, review, confirmation, evaluator, guard, lifecycle, or privacy
  rules. Shadow, historical, review, and learning modes cannot place/cancel
  orders or silently apply strategy changes.

### 6. Optional Interactive Brokers Historical Data Adapter

Status: implemented by `ibkr_historical.py` and documented in
`HISTORICAL_LEARNING.md`.

- Connects only to an authenticated local TWS/IB Gateway socket and exposes no
  account, portfolio, order, or execution methods.
- Collects regular-session historical bars, time-matched opening-volume
  lookbacks, daily bars, and historical top-of-book bid/ask ticks with sizes.
- Produces three timestamped, strategy-shaped quote snapshots for a recorded
  evaluation time and preserves explicit IBKR feed limitations.
- Uses ignored `.env` connection settings and requires no API private key or
  account identifier.
- Fails with an actionable TWS startup/login/API-socket message when the local
  service is unavailable.
- Classifies retryable transport/provider failures separately from permanent
  fidelity gaps, aborts a disconnected request stream immediately, reconnects
  once by default, and resumes from atomic per-provider caches.
- Optional Massive SIP and Alpaca adapters follow IBKR in the canonical
  provider chain. Retryable and permanent failures may advance, but each
  candidate is recollected from one provider; field/feed/adjustment provenance
  remains explicit and missing trade intervals are never filled.

### 9. Pre-Freeze Historical Symbol Viability

Status: implemented by `historical_universe.py` and the `probe` surface in
`ibkr_historical.py`.

- Historical discovery now produces a ranked draft pool with spare candidates
  before the final universe is frozen.
- `historical_discovery.py` automates the high-market-cap earnings/SEC lane:
  connector results are normalized, filings are restricted by point-in-time
  acceptance, strong primary-document dilution language is screened, and all
  source artifacts are resumably cached outside Git. Large runs retain an
  80-name reserve because a 40-name reserve was empirically too shallow for the
  immutable ADV/ATR gates.
- SEC registrants are deduplicated to one deterministic representative ticker
  before preflight. This removes preferred/depositary siblings from the ranked
  pool; IBKR `stockType=COMMON` remains the authoritative final proof.
- Before provider calls, the preflight rejects draft rows that are not explicitly
  common stock or already carry a dilution conflict. It then resolves IBKR stock
  contracts and evaluates prior daily ADV/ATR gates before verifying 14 positive
  prior opening-volume sessions, all without requesting target-session prices.
  A retired, unresolvable, ineligible, or input-incomplete symbol can be skipped
  in favor of the next ranked buffered name without using future performance to
  select the sample.
- Only symbol-scoped failures are skippable. Permissions, pacing, connection,
  and provider-wide failures still stop the batch instead of silently shrinking
  or distorting the universe.
- `--continue-on-exhausted` records one sparse date as blocked and continues
  later frozen dates without substitution. Strict fail-fast remains the default.
- Contract proof now requires IBKR `stockType=COMMON`; the pre-session cache
  contract was bumped so older entries without this proof cannot be reused.
- The frozen evidence manifest records accepted, skipped, and unused buffered
  symbols plus draft, qualification, and accepted-history hashes. After this
  point, the builder never replaces a candidate based on observed market data.
- Preflight now checkpoints every examined symbol/date under ignored storage,
  resumes exact reruns from a versioned cache, and streams progress. Only names
  that pass daily gates request opening history; the fixed 28-day window fits in
  one five-minute HMDS chunk while still proving all 14 required prior sessions.
- Accepted pre-session opening and daily bars are shared with bundle collection,
  eliminating duplicate provider requests. Cache identity and the no-target-
  price attestation are validated before reuse; cache misses use normal IBKR
  collection.
- A 2026-07-16 ten-date measurement completed 102 buffered preflight checks in
  736.5 seconds and repeated the exact cached pass in 1.13 seconds. Keep tracking
  cold-run, resume, bundle-collection, and blocked-date timing on larger batches.
  The daily-first/one-chunk optimization landed after this baseline and still
  needs a new cold-run benchmark.
- The 2026-07-16 throughput pass added bounded rank-ordered workers, global
  pacing reservation, provider telemetry, and daily-gate-first request
  elimination. On the March 30 graph, explicit contract lookups now apply only
  to the ten daily-gate survivors, reducing planned cold calls from 118 to 74.
  Initial concurrent trials ran inside an IBKR soft-throttle window and are not
  clean speedup evidence; retain worker count and request telemetry on the timed
  100-day run and its exact cached rerun.

### 10. Durable Progress History And Contribution Hook

Status: implemented by `progress_history.py`, `progress/HISTORY.jsonl`, and the
tracked `.githooks/pre-commit` hook.

- Meaningful breakthroughs, diagnosed failures, provider constraints,
  architecture decisions, and reusable workflow improvements have a structured,
  append-only home outside transient conversation history.
- The history audit validates IDs, timestamps, required findings/impact, unique
  entries, and repository-relative file references.
- Substantive staged changes require a new history contribution. Mechanical,
  generated-ledger, and test-only commits are exempt; an explicit bypass exists
  only for truly lesson-free maintenance.
- The first record captures the five-day replay pass: A/B/C market-data
  entitlements, the required TWS restart, resumable caching, the retired `SEMR`
  blocker, and the legitimate flat `LOB` opening bar.

## Outstanding

Items below are design-ready backlogs, not authority to weaken live-trading or
historical-fidelity rules. New ideas should include scope, safety boundaries,
data contracts, acceptance tests, and publishing behavior before implementation.

### 7. Valuable Data Cache

Status: core per-symbol/day market-data scope implemented on 2026-07-18 by
`historical_store.py`, `historical_service.py`, `historical_data_cli.py`, and
`historical_migration.py`. See `HISTORICAL_DATA_STORE.md`. Point-in-time web/SEC
cache lifecycle and optional pruning/reporting remain separate enhancements.

The implemented content-addressed cache for expensive, slow, or rate-limited
market-data inputs lets future work reuse verified evidence without confusing
provider feeds. Future web/SEC extensions should follow the same principles and
add equivalent lifecycle handling without confusing old data with current
truth.

Acceptance criteria:

- Every entry records source/provider, request identity, as-of time, retrieval
  time, schema version, producer version, content hash, privacy class, and
  freshness/immutability policy.
- Point-in-time historical artifacts are immutable. Current/live responses have
  explicit expiry and can never be described as current after expiry.
- Writes are atomic and integrity-audited; corrupt, partial, schema-incompatible,
  or provenance-free entries fail closed and are recollected when safe.
- Cache hits explain exactly what was reused and why it is still valid. A caller
  can demand fresh collection for drift-prone or safety-critical facts.
- Provide `audit`, `inspect`, and `prune` commands plus bounded retention and
  size reporting. Raw private inputs remain ignored and public summaries remain
  safe to commit.
- Never cache credentials, MFA material, session cookies, plaintext account or
  broker identifiers, or mutable live broker state.
- Extend the implemented historical market-data store to authoritative calendars
  and time-valid catalyst evidence, with deterministic tests for expiry and
  schema migration in those mutable/current data classes.
