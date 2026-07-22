"""Rebuild the portfolio campaign's bounded, read-only local data inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import statistics
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from historical_research import load_dataset
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalDayStore,
    _gzip_json_bytes,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_EVIDENCE_PATH = (
    PROJECT_ROOT / "historical_batches" / "evidence-2026-07-16-one-hundred-days.json"
)
DEFAULT_REPAIR_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "2026-07-16-evidence-bound-corpus-repair.json"
)
DEFAULT_LAB_RESULT_PATH = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-18-production-aware-strategy-lab.json"
)
DEFAULT_CONFIRMATION_RESULT_PATH = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-18-independent-early-earnings-reversal-confirmation.json"
)
DEFAULT_INVENTORY_PATH = (
    PROJECT_ROOT / "research_results" / "2026-07-21-portfolio-data-inventory.json"
)
STORE_SAMPLE_SYMBOLS = ("SPY", "QQQ", "AAPL")
PAIRED_MINUTE_SAMPLE_SYMBOLS = {"SPY", "QQQ"}
SCHEMA_VERSION = 1


class PortfolioDataInventoryError(RuntimeError):
    """The bounded inventory cannot be reproduced safely."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioDataInventoryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioDataInventoryError(f"{path} must contain an object")
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _inventory_hash(value: Mapping[str, Any]) -> str:
    payload = {key: item for key, item in value.items() if key != "inventory_sha256"}
    return hashlib.sha256(_canonical_bytes(payload)).hexdigest()


def _dataset_descriptor(dataset: Mapping[str, Any]) -> dict[str, Any]:
    quality = dataset.get("quality")
    if not isinstance(quality, Mapping):
        quality = {}
    rows = dataset.get("rows")
    return {
        "kind": dataset.get("kind"),
        "channel": dataset.get("channel"),
        "timeframe": dataset.get("timeframe"),
        "provider": dataset.get("provider"),
        "feed": dataset.get("feed"),
        "adjustment": dataset.get("adjustment"),
        "complete": quality.get("complete") is True,
        "row_count": int(quality.get("row_count", len(rows) if isinstance(rows, list) else 0)),
    }


def _minute_dates(store: HistoricalDayStore, symbol: str) -> list[str]:
    dates = []
    for day in store.dates(symbol):
        document = store.load(symbol, day)
        datasets = document.get("datasets") if isinstance(document, Mapping) else None
        if isinstance(datasets, list) and any(
            isinstance(item, Mapping) and item.get("timeframe") == "1m"
            for item in datasets
        ):
            dates.append(day)
    return dates


def _store_sample(store: HistoricalDayStore, symbol: str) -> dict[str, Any]:
    dates = store.dates(symbol)
    if symbol in PAIRED_MINUTE_SAMPLE_SYMBOLS:
        dates = sorted(
            set(_minute_dates(store, "SPY"))
            & set(_minute_dates(store, "QQQ"))
        )
    if not dates:
        return {"symbol": symbol, "dates": 0, "first": None, "last": None, "samples": []}
    selected = list(dict.fromkeys((dates[0], dates[len(dates) // 2], dates[-1])))
    samples = []
    for day in selected:
        document = store.load(symbol, day)
        if document is None:
            raise PortfolioDataInventoryError(f"sample document disappeared: {symbol} {day}")
        sampled_document = dict(document)
        if symbol in PAIRED_MINUTE_SAMPLE_SYMBOLS:
            sampled_document["datasets"] = [
                item
                for item in document["datasets"]
                if item.get("timeframe") == "1m"
            ]
        document_sha256 = hashlib.sha256(
            _gzip_json_bytes(sampled_document)
        ).hexdigest()
        samples.append(
            {
                "date": day,
                "document_sha256": document_sha256,
                "datasets": [
                    _dataset_descriptor(item)
                    for item in sampled_document["datasets"]
                    if isinstance(item, Mapping)
                ],
            }
        )
    return {
        "symbol": symbol,
        "dates": len(dates),
        "first": dates[0],
        "last": dates[-1],
        "samples": samples,
    }


def _legacy_corpus(evidence_path: Path, data_root: Path) -> dict[str, Any]:
    dataset = load_dataset(evidence_path, data_root)
    counts: list[int] = []
    symbols: set[str] = set()
    providers: Counter[str] = Counter()
    sample_phases: Counter[str] = Counter()
    benchmark_sets: Counter[tuple[str, ...]] = Counter()
    candidate_rows = 0
    minute_bars = 0
    interpolated_bars = 0
    quote_snapshots = 0
    bytes_total = 0
    complete_bundles = 0
    point_in_time_bundles = 0
    available_hashes: list[dict[str, str]] = []
    missing_dates: list[str] = []
    for item in dataset["bundles"]:
        day = str(item["date"])
        if item["status"] != "available":
            missing_dates.append(day)
            continue
        path = Path(str(item["path"]))
        bundle = _load_json(path)
        if bundle.get("date") != day:
            raise PortfolioDataInventoryError(f"bundle date drifted: {day}")
        candidates = bundle.get("candidates")
        if not isinstance(candidates, list):
            raise PortfolioDataInventoryError(f"bundle candidates are malformed: {day}")
        observed_symbols = [
            str(candidate.get("symbol", ""))
            for candidate in candidates
            if isinstance(candidate, Mapping)
        ]
        if observed_symbols != item["expected_symbols"]:
            raise PortfolioDataInventoryError(f"bundle symbol order drifted: {day}")
        counts.append(len(candidates))
        candidate_rows += len(candidates)
        bytes_total += path.stat().st_size
        complete_bundles += bundle.get("session_capture_complete") is True
        source = bundle.get("source")
        if not isinstance(source, Mapping):
            raise PortfolioDataInventoryError(f"bundle source is malformed: {day}")
        point_in_time_bundles += source.get("point_in_time") is True
        sample_phases[str(bundle.get("sample_phase"))] += 1
        market_providers = source.get("market_data_providers", [])
        if isinstance(market_providers, list):
            providers.update(str(value) for value in market_providers)
        benchmark_provider = source.get("benchmark_provider_by_symbol")
        benchmark_symbols = (
            tuple(sorted(str(value) for value in benchmark_provider))
            if isinstance(benchmark_provider, Mapping)
            else ()
        )
        benchmark_sets[benchmark_symbols] += 1
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                raise PortfolioDataInventoryError(f"candidate is malformed: {day}")
            symbols.add(str(candidate["symbol"]))
            bars = candidate.get("bars", [])
            if not isinstance(bars, list):
                raise PortfolioDataInventoryError(f"candidate bars are malformed: {day}")
            minute_bars += len(bars)
            interpolated_bars += sum(
                isinstance(row, Mapping) and bool(row.get("interpolated"))
                for row in bars
            )
            payload = candidate.get("evaluation_payload")
            quotes = payload.get("quotes", []) if isinstance(payload, Mapping) else []
            quote_snapshots += len(quotes) if isinstance(quotes, list) else 0
        available_hashes.append({"date": day, "sha256": str(item["sha256"])})
    return {
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed": True,
        "untouched_confirmation_eligible": False,
        "selection_boundary": "catalyst-selected ten-candidate daily universe",
        "evidence_path": str(evidence_path.relative_to(PROJECT_ROOT)),
        "evidence_sha256": dataset["evidence_sha256"],
        "dataset_hash": dataset["dataset_hash"],
        "requested_dates": len(dataset["dates"]),
        "available_dates": dataset["available_dates"],
        "missing_dates": missing_dates,
        "date_min": min(dataset["dates"]),
        "date_max": max(dataset["dates"]),
        "bundle_bytes": bytes_total,
        "bundle_hashes_sha256": hashlib.sha256(_canonical_bytes(available_hashes)).hexdigest(),
        "candidate_rows": candidate_rows,
        "unique_candidate_symbols": len(symbols),
        "candidates_per_date": {
            "minimum": min(counts),
            "median": statistics.median(counts),
            "maximum": max(counts),
        },
        "minute_bars": minute_bars,
        "interpolated_minute_bars": interpolated_bars,
        "quote_snapshots": quote_snapshots,
        "session_capture_complete_bundles": complete_bundles,
        "point_in_time_bundles": point_in_time_bundles,
        "sample_phases": dict(sorted(sample_phases.items())),
        "market_data_providers": dict(sorted(providers.items())),
        "benchmark_symbol_sets": {
            ",".join(key): value for key, value in sorted(benchmark_sets.items())
        },
    }


def _prior_research(lab_path: Path, confirmation_path: Path) -> dict[str, Any]:
    lab = _load_json(lab_path)
    confirmation = _load_json(confirmation_path)
    policies = lab.get("policy_summaries")
    if not isinstance(policies, list):
        raise PortfolioDataInventoryError("prior lab policy summaries are malformed")
    manifest = confirmation.get("manifest")
    result = confirmation.get("policy_result")
    decision = confirmation.get("decision")
    if not all(isinstance(value, Mapping) for value in (manifest, result, decision)):
        raise PortfolioDataInventoryError("prior confirmation result is malformed")
    return {
        "development_result": {
            "path": str(lab_path.relative_to(PROJECT_ROOT)),
            "sha256": sha256_file(lab_path),
            "declared_policy_trials": len(policies),
            "provider_requests": lab.get("runtime", {}).get("provider_requests"),
            "claim_scope": "FALSIFICATION_ONLY",
        },
        "retired_confirmation": {
            "path": str(confirmation_path.relative_to(PROJECT_ROOT)),
            "sha256": sha256_file(confirmation_path),
            "requested_dates": manifest.get("requested_dates"),
            "available_dates": manifest.get("available_dates"),
            "trades": result.get("trades"),
            "total_r": result.get("total_r"),
            "profit_factor": result.get("profit_factor"),
            "next_stage": decision.get("next_stage"),
            "reuse_boundary": "development/falsification only; never untouched confirmation",
        },
    }


def _strategy_data_routes() -> list[dict[str, str]]:
    return [
        {
            "strategy_family": "etf-opening-range-momentum",
            "status": "BOUNDED_SAMPLE_SUPPORTS_MANIFEST_FREEZE",
            "next_data_gate": "freeze disjoint SPY/QQQ dates and audit complete whole-provider minute bars",
        },
        {
            "strategy_family": "etf-vwap-mean-reversion",
            "status": "BOUNDED_SAMPLE_SUPPORTS_MANIFEST_FREEZE",
            "next_data_gate": "share only the preregistered ETF corpus, then use disjoint confirmation dates",
        },
        {
            "strategy_family": "equity-gap-continuation",
            "status": "LEGACY_SCREEN_ONLY",
            "next_data_gate": "freeze a representative point-in-time universe before development claims",
        },
        {
            "strategy_family": "equity-gap-recovery",
            "status": "LEGACY_SCREEN_ONLY",
            "next_data_gate": "freeze a representative point-in-time universe before development claims",
        },
        {
            "strategy_family": "relative-strength-continuation",
            "status": "BLOCKED_REPRESENTATIVE_UNIVERSE",
            "next_data_gate": "collect full cross-sectional ranking inputs without survivor substitution",
        },
        {
            "strategy_family": "volatility-compression-breakout",
            "status": "LEGACY_SCREEN_ONLY",
            "next_data_gate": "freeze daily lookback and representative universe inputs",
        },
        {
            "strategy_family": "cross-sectional-momentum",
            "status": "BLOCKED_MULTISESSION_UNIVERSE",
            "next_data_gate": "freeze point-in-time membership, corporate actions, and five-session timelines",
        },
        {
            "strategy_family": "short-horizon-oversold-reversal",
            "status": "LEGACY_SCREEN_ONLY",
            "next_data_gate": "screen without tuning, then freeze representative development dates",
        },
        {
            "strategy_family": "post-earnings-drift",
            "status": "LEGACY_SCREEN_ONLY_WITH_RETIRED_PRIOR_TRIAL",
            "next_data_gate": "count all prior trials and do not repair the failed confirmation version",
        },
        {
            "strategy_family": "catalyst-orb-retest",
            "status": "PRESERVED_SOURCE_ACQUISITION_LANE",
            "next_data_gate": "resume only if prioritized after tournament screening; never discard its frozen evidence",
        },
    ]


def build_inventory(
    *,
    evidence_path: Path = DEFAULT_EVIDENCE_PATH,
    repair_path: Path = DEFAULT_REPAIR_PATH,
    lab_path: Path = DEFAULT_LAB_RESULT_PATH,
    confirmation_path: Path = DEFAULT_CONFIRMATION_RESULT_PATH,
    data_root: Path = PROJECT_ROOT / "historical_data",
    env_path: Path = DEFAULT_ENV_PATH,
) -> dict[str, Any]:
    repair = _load_json(repair_path)
    store = HistoricalDayStore.from_env(env_path)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "inspection_kind": "bounded-local-data-inventory",
        "provider_requests": 0,
        "broker_actions": 0,
        "outcome_calculations": 0,
        "source_artifacts": {
            str(repair_path.relative_to(PROJECT_ROOT)): sha256_file(repair_path),
            str(lab_path.relative_to(PROJECT_ROOT)): sha256_file(lab_path),
            str(confirmation_path.relative_to(PROJECT_ROOT)): sha256_file(
                confirmation_path
            ),
        },
        "legacy_bundle_corpus": _legacy_corpus(evidence_path, data_root),
        "canonical_store_bounded_sample": [
            _store_sample(store, symbol) for symbol in STORE_SAMPLE_SYMBOLS
        ],
        "store_contract": {
            "reserve_bytes": store.min_free_bytes,
            "full_store_audit_required_before_promotion": True,
            "bounded_inventory_is_not_full_store_validation": True,
        },
        "repair_reconciliation": repair["final_coverage"],
        "prior_research": _prior_research(lab_path, confirmation_path),
        "strategy_data_routes": _strategy_data_routes(),
        "conclusions": [
            "existing catalyst bundles can accelerate Stage 0 falsification but cannot validate broad-market expectancy",
            "previously inspected outcomes cannot become untouched confirmation for any new strategy version",
            "SPY and QQQ cache depth justifies freezing an ETF development manifest before further outcome access",
            "cross-sectional and five-session strategies need new representative point-in-time universe contracts",
            "no provider purchase or credential action is justified by this bounded inventory",
        ],
    }
    value["inventory_sha256"] = _inventory_hash(value)
    return value


def inspect_inventory(path: Path = DEFAULT_INVENTORY_PATH) -> dict[str, Any]:
    recorded = _load_json(path)
    if recorded.get("inventory_sha256") != _inventory_hash(recorded):
        raise PortfolioDataInventoryError("recorded inventory content hash is invalid")
    rebuilt = build_inventory()
    if recorded != rebuilt:
        raise PortfolioDataInventoryError("recorded inventory does not match rebuilt local evidence")
    return {
        "valid": True,
        "path": str(path),
        "inventory_sha256": recorded["inventory_sha256"],
        "available_legacy_bundles": recorded["legacy_bundle_corpus"]["available_dates"],
        "provider_requests": 0,
        "broker_actions": 0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("build", help="rebuild and print the deterministic inventory")
    subparsers.add_parser("write", help="refresh the published deterministic inventory")
    inspect = subparsers.add_parser("inspect", help="rebuild and inspect a published inventory")
    inspect.add_argument("path", nargs="?", type=Path, default=DEFAULT_INVENTORY_PATH)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command in {"build", "write"}:
            result = build_inventory()
            if args.command == "write":
                DEFAULT_INVENTORY_PATH.write_text(
                    json.dumps(result, indent=2, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
        else:
            result = inspect_inventory(args.path)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (PortfolioDataInventoryError, OSError, ValueError) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
