# Expansion Unchanged-v3 Input Readiness

Dataset: `dataset-champion-input-readiness-2026-07-19-expansion-v1`

Status: frozen before non-return gate evaluation

Manifest: `3144c677ed68314df1526939a0020ce173445748dcc8ba194e4a9b955ab746da`

This outcome-blind contract filters the exact 1,987-pair expansion to the 14
already inspected pairs with verified-positive SEC primary direction and no
negative or financing conflict. It then applies the existing unchanged-v3
clean-cross, quote, spread, chase, official-halt, visible-liquidity,
completed-bar VWAP, benchmark, stop-proxy, and resistance-proxy calculations.

The adapter hash-binds and reuses both completed readiness implementations. The
private evaluation slice is content-hashed and remains outside Git. Public
artifacts contain only counts and hashes. Target returns, post-entry bars,
winner labels, alpha, strategy variants, maturity, and production changes are
forbidden.

```sh
python3 champion_input_readiness_expansion.py freeze
python3 champion_input_readiness_expansion.py build --manifest <manifest>
python3 champion_input_readiness_expansion.py inspect --manifest <manifest>
```
