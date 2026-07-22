# Accelerated-Share-Repurchase Capacity

The priority-three v2 family is an outcome-blind capacity test, not a strategy
backtest. Its exact contract is
`strategy_tournament/v2/asr/manifests/accelerated-share-repurchase-continuation-v1-afb19cf7e78db4afb008cbbce5c7ad462911a113efc1c2d36a4dfa80252f6cb7.json`.
Independent inspection `1587bff5e7c313cceae4510b6690bcf9b85af4e0f6a732631daaafad08545e18`
opens only the contract's SEC search and accession-document scope.

The preflight covers 2010-01-01 through 2025-12-31 and requires all of:

- an executed accelerated share or stock repurchase agreement;
- committed dollar notional;
- a point-in-time SEC acceptance timestamp; and
- delivery, valuation-period, or settlement mechanics capable of continuing
  after disclosure.

Board authorizations, ordinary open-market programs, generic repurchase intent,
and unresolved mechanics are retained in attrition but are not verified events.
Every unique search hit needs a terminal classification. No price, return, fill,
stop, or broker input is permitted.

The fixed disposition is fewer than 50 verified events =
`RETIRED_INSUFFICIENT_FORMAL_CAPACITY`; 50-99 =
`PRESERVED_LATER_SINGLE_RULE`; at least 100 = `CAPACITY_READY` for the generic
development-search pipeline. A capacity result has no maturity effect.

Operator boundary:

```sh
python3 asr_capacity.py status
```

The status must be `CAPACITY_CONTRACT_INSPECTED`, with SEC access true and all
market, outcome, and broker permissions false, before collection begins.

The committed collector reads each exact search phrase into the external
content-addressed historical store, reuses retained pages on resume, and stops
before matched-document access if EFTS does not provide an exact total:

```sh
python3 asr_capacity_collection.py collect
python3 asr_capacity_collection_inspection.py inspect
```

The public collection and inspection artifacts contain counts, hashes, request
telemetry, and the terminal source-completeness state. Raw result rows remain in
the ignored external store. The independently inspected denominator must be
complete before any filing-semantic classification can begin.
