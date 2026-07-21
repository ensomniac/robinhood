"""Freeze, collect, and evaluate post-earnings drift at Stage 0."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
import shutil
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import etf_or_momentum_stage0 as common
from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate
from scanner_replay import ScannerReplayError, load_split_actions
from scanner_replay_alpaca import AlpacaBulkBarsClient, AlpacaBulkConfig


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
SLATE_PATH = common.SLATE_PATH
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "preentry_structure"
    / "session-calendar-2024-12-through-2026-06.json"
)
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "development_tranche_v3"
    / "non_return_manifests"
    / "dataset-development-non-return-qualification-2026-07-20-tranche-v3-v1-7d5be928f1eea002294b0a407d88b38bc6b4ab047f9ab599419628bcedfdb829.json"
)
SOURCE_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "development_tranche_v3"
    / "non-return-contract-status.json"
)
SOURCE_PRIVATE_RELATIVE = (
    "_derived/development_non_return/"
    "dataset-development-non-return-qualification-2026-07-20-tranche-v3-v1/"
    "frozen-positive-preentry-contract.json.gz"
)
PRIOR_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-18-independent-early-earnings-reversal-confirmation.json"
)
SPLIT_PATHS = (
    PROJECT_ROOT / "learning_runs" / "scanner_expansion_v2" / "splits.json.gz",
    PROJECT_ROOT
    / "learning_runs"
    / "development_tranche_v3"
    / "scanner_replay"
    / "splits.json.gz",
)
VARIANT_ID = "post-earnings-drift-v1"
VARIANT_ORDINAL = 9
SCHEMA_VERSION = 1
DATASET_ID = "dataset-post-earnings-drift-stage0-2026-07-21-v1"
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
MINIMUM_FREE_BYTES = 20 * 1024**3
EXPECTED_PAIRS = 102
EXPECTED_DATES = 52
DISCARDED_EARNINGS_PROVIDER_REQUESTS = 168
FAILED_EARNINGS_INGESTION_ATTEMPTS = 2
EFFECTIVE_EARNINGS_PROVIDER_REQUESTS = 84
DISCARDED_MARKET_PROVIDER_REQUESTS = 3
FAILED_MARKET_COLLECTION_ATTEMPTS = 1
MARKET_DIAGNOSTIC_REQUESTS = 1
EARNINGS_SOURCE_MANIFEST_SHA256 = (
    "dbedd0065e72bedbb2bb26f88dd9da5d7c0c42de57bbb8b0fab55dfa2a1697d4"
)
EARNINGS_STATUS_SHA256 = (
    "50f34f7214875983dba988c8c298dab78b68e87db9cea76436cf2273456a825e"
)
MAXIMUM_HOLDING_DATES = 5
ACTIVATION_ROOT = PROJECT_ROOT / "strategy_tournament" / "activations"
INSPECTION_ROOT = PROJECT_ROOT / "strategy_tournament" / "inspections"
RESULT_ROOT = PROJECT_ROOT / "research_results"
OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament" / "post_earnings_drift"
PUBLIC_EARNINGS_STATUS = OUTPUT_ROOT / "earnings-status.json"
PUBLIC_MARKET_STATUS = OUTPUT_ROOT / "market-status.json"


class PostEarningsDriftError(RuntimeError):
    """The frozen post-earnings Stage 0 contract is incomplete or invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PostEarningsDriftError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PostEarningsDriftError(f"{path} must contain an object")
    return value


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise PostEarningsDriftError(f"cannot read {path}: {exc}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "post_earnings_drift_stage0" / DATASET_ID


def _candidate_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "frozen-candidates.json.gz"


def _earnings_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "earnings-metadata.json.gz"


def _market_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "market-inputs.json.gz"


def _input_index_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "input-index.json.gz"


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    matches = [
        row
        for row in _load_json(SLATE_PATH)["variants"]
        if row.get("variant_id") == VARIANT_ID
        and row.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(matches) != 1:
        raise PostEarningsDriftError("frozen slate variant is unavailable")
    return dict(matches[0])


def _calendar() -> list[str]:
    try:
        raw = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PostEarningsDriftError("session calendar is unavailable") from exc
    if not isinstance(raw, list):
        raise PostEarningsDriftError("session calendar is malformed")
    sessions = [str(row.get("date")) for row in raw if isinstance(row, Mapping)]
    if sessions != sorted(set(sessions)):
        raise PostEarningsDriftError("session calendar is not unique and ordered")
    return sessions


def _source_private_path(store: HistoricalDayStore) -> Path:
    return store.root / SOURCE_PRIVATE_RELATIVE


def _candidate_graph(store: HistoricalDayStore) -> dict[str, Any]:
    status = _load_json(SOURCE_STATUS)
    manifest = _load_json(SOURCE_MANIFEST)
    source_path = _source_private_path(store)
    if (
        status.get("status") != "FROZEN_READY"
        or status.get("inspected") is not True
        or status.get("verified_positive_pairs") != EXPECTED_PAIRS
        or status.get("target_outcomes_observed_or_derived") is not False
        or status.get("private_contract_file_sha256") != sha256_file(source_path)
        or status.get("manifest_sha256") != manifest.get("manifest_sha256")
    ):
        raise PostEarningsDriftError("source-verified candidate contract is unusable")
    private = _load_gzip(source_path)
    selection = private.get("selection")
    if (
        not isinstance(selection, Mapping)
        or selection.get("positive_pair_count") != EXPECTED_PAIRS
        or selection.get("target_outcomes_observed_or_derived") is not False
        or private.get("target_outcomes_observed_or_derived") is not False
    ):
        raise PostEarningsDriftError("private source candidate graph is invalid")
    raw_pairs = selection.get("positive_pairs")
    if not isinstance(raw_pairs, list) or len(raw_pairs) != EXPECTED_PAIRS:
        raise PostEarningsDriftError("private source candidate count drifted")
    pairs: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for raw in raw_pairs:
        if not isinstance(raw, Mapping):
            raise PostEarningsDriftError("source candidate is malformed")
        day = str(raw.get("date", ""))
        symbol = str(raw.get("symbol", "")).strip().upper()
        instrument_id = str(raw.get("instrument_id", ""))
        exchange = str(raw.get("primary_exchange", ""))
        rank = int(raw.get("rank", 0))
        date.fromisoformat(day)
        if not symbol or not instrument_id or exchange not in {"XNAS", "XNYS"}:
            raise PostEarningsDriftError("source candidate identity is malformed")
        identity = (day, symbol)
        if identity in seen:
            raise PostEarningsDriftError("source candidate identity repeats")
        seen.add(identity)
        pairs.append(
            {
                "date": day,
                "symbol": symbol,
                "instrument_id": instrument_id,
                "primary_exchange": exchange,
                "rank": rank,
            }
        )
    pairs.sort(key=lambda row: (row["date"], row["rank"], row["symbol"]))
    sessions = _calendar()
    positions = {day: index for index, day in enumerate(sessions)}
    windows: dict[str, dict[str, Any]] = {}
    for pair in pairs:
        day = pair["date"]
        if day not in positions or positions[day] < 1 or positions[day] + 4 >= len(sessions):
            raise PostEarningsDriftError(f"{day}: incomplete five-session window")
        windows[f"{day}|{pair['symbol']}"] = {
            "prior_session": sessions[positions[day] - 1],
            "holding_sessions": sessions[positions[day] : positions[day] + 5],
        }
    if len({row["date"] for row in pairs}) != EXPECTED_DATES:
        raise PostEarningsDriftError("source positive-date denominator drifted")
    graph = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "selection_information_cutoff": "SOURCE_VERIFIED_BEFORE_09_35_ET",
        "target_outcomes_observed_or_derived": False,
        "pairs": pairs,
        "windows": windows,
        "source_private_file_sha256": sha256_file(source_path),
        "source_positive_pair_identity_sha256": selection[
            "positive_pair_identity_sha256"
        ],
    }
    path = _candidate_path(store)
    if path.exists() and _load_gzip(path) != graph:
        raise PostEarningsDriftError("private frozen candidate graph drifted")
    if not path.exists():
        _write_gzip(path, graph)
    return graph


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    graph = _candidate_graph(source)
    variant = _variant()
    if shutil.disk_usage(source.root).free < MINIMUM_FREE_BYTES:
        raise PostEarningsDriftError("historical store is below its reserve")
    dates = sorted({row["date"] for row in graph["pairs"]})
    symbols = sorted({row["symbol"] for row in graph["pairs"]})
    selection_contract = {
        "candidate_universe": "all 102 source-verified positive primary-catalyst pairs in the inspected development-v3 non-return contract",
        "earnings_event": "Robinhood company-verified report metadata whose reaction session matches the frozen pair and whose numeric actual EPS strictly exceeds numeric estimated EPS",
        "reaction_session": "report date for before-market reports; next exchange session for after-market reports",
        "gap_fraction": "event-session 09:30 open divided by split-adjusted prior-session close minus one, inclusive from 0.01 through 0.08",
        "decision_time_et": "15:45:00 using only completed 09:30 through 15:44 one-minute bars",
        "close_strength": "15:44 close strictly above the 09:30 open and completed-session SIP VWAP",
        "price_gate": "event-session open strictly above 5 dollars",
        "missing_policy": "every frozen pair remains in the denominator; missing earnings, minute, daily, calendar, or split inputs cannot signal",
    }
    outcome_contract = {
        "entry": "15:46 regular-session one-minute open plus adverse per-side cost",
        "stop": "lowest SIP one-minute low from 09:30 through 15:44",
        "target": "none",
        "maximum_hold_trading_days": MAXIMUM_HOLDING_DATES,
        "exit": "stop first after entry; later gap below stop fills at worse open; otherwise close of fifth trading date including event date",
        "entry_exit_cost_bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
        "planned_risk_r": "cost-adjusted entry minus cost-adjusted planned stop",
    }
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-event-multisession-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 1,
        "slate_manifest_sha256": sha256_file(SLATE_PATH),
        "variant_ordinal": VARIANT_ORDINAL,
        "variant_id": VARIANT_ID,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "broker_actions_authorized": False,
        "provider_requests_authorized_before_inspection": False,
        "return_evaluation_authorized_before_input_inspection": False,
        "collection_transport_incident": {
            "discarded_earnings_provider_requests": DISCARDED_EARNINGS_PROVIDER_REQUESTS,
            "failed_ingestion_attempts": FAILED_EARNINGS_INGESTION_ATTEMPTS,
            "effective_earnings_provider_requests": EFFECTIVE_EARNINGS_PROVIDER_REQUESTS,
            "earnings_responses_retained": EFFECTIVE_EARNINGS_PROVIDER_REQUESTS,
            "discarded_market_provider_requests": DISCARDED_MARKET_PROVIDER_REQUESTS,
            "failed_market_collection_attempts": FAILED_MARKET_COLLECTION_ATTEMPTS,
            "market_diagnostic_requests": MARKET_DIAGNOSTIC_REQUESTS,
            "market_payloads_retained": 0,
            "market_outcomes_accessed": True,
            "strategy_returns_computed": 0,
            "retry_requires_this_activation_inspection": True,
        },
        "supersedes_manifest_sha256": EARNINGS_SOURCE_MANIFEST_SHA256,
        "declared_prior_policy_trials": 15,
        "prior_failed_confirmation": {
            "path": PRIOR_RESULT.relative_to(PROJECT_ROOT).as_posix(),
            "file_sha256": sha256_file(PRIOR_RESULT),
            "reused_as_confirmation": False,
        },
        "selection_contract": selection_contract,
        "outcome_contract": outcome_contract,
        "stage0_gate": {
            "minimum_closed_signals": 30,
            "minimum_expectancy_r_exclusive": 0.0,
            "minimum_profit_factor": 1.10,
            "maximum_drawdown_r": 8.0,
            "stress_20bps_total_r_exclusive": 0.0,
            "maximum_rule_violations": 0,
        },
        "denominator": {
            "candidate_pairs": len(graph["pairs"]),
            "candidate_dates": len(dates),
            "unique_symbols": len(symbols),
            "maximum_selected_signals": len(graph["pairs"]),
        },
        "candidate_dates": dates,
        "private_candidates": {
            "location": "LOCAL_HISTORICAL_DATA_ROOT/_derived/post_earnings_drift_stage0/"
            f"{DATASET_ID}/frozen-candidates.json.gz",
            "content_sha256": _hash(graph),
            "contains_target_returns": False,
        },
        "source_bindings": {
            SOURCE_MANIFEST.relative_to(PROJECT_ROOT).as_posix(): sha256_file(
                SOURCE_MANIFEST
            ),
            SOURCE_STATUS.relative_to(PROJECT_ROOT).as_posix(): sha256_file(
                SOURCE_STATUS
            ),
            CALENDAR_PATH.relative_to(PROJECT_ROOT).as_posix(): sha256_file(
                CALENDAR_PATH
            ),
            **{
                path.relative_to(PROJECT_ROOT).as_posix(): sha256_file(path)
                for path in SPLIT_PATHS
            },
        },
        "collection_contract": {
            "earnings_provider": "Robinhood read-only earnings results",
            "logical_earnings_requests": len(symbols),
            "discarded_earnings_provider_requests_before_this_activation": DISCARDED_EARNINGS_PROVIDER_REQUESTS,
            "effective_earnings_provider_requests_before_this_activation": EFFECTIVE_EARNINGS_PROVIDER_REQUESTS,
            "earnings_source_manifest_sha256": EARNINGS_SOURCE_MANIFEST_SHA256,
            "earnings_status_sha256": EARNINGS_STATUS_SHA256,
            "retry_accounting": "all provider calls count, including responses discarded by the failed ingestion transport",
            "market_provider": "Alpaca historical SIP",
            "market_feed": "sip",
            "market_adjustment": "raw with frozen local split handling",
            "intraday_timeframe": "1Min full regular session for every candidate pair",
            "provider_boundary_policy": "ignore same-date bars outside 09:30 inclusive through 16:00 exclusive before strict regular-session validation",
            "discarded_market_provider_requests_before_this_activation": DISCARDED_MARKET_PROVIDER_REQUESTS,
            "daily_timeframe": "1Day for every candidate symbol across the complete frozen window",
            "daily_start": min(
                item["prior_session"] for item in graph["windows"].values()
            ),
            "daily_end_inclusive": max(
                item["holding_sessions"][-1] for item in graph["windows"].values()
            ),
            "provider_switching_allowed": False,
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
        },
    }
    value["activation_rules_hash"] = _hash(
        {
            "base_rules_hash": value["base_rules_hash"],
            "selection_contract": selection_contract,
            "outcome_contract": outcome_contract,
            "stage0_gate": value["stage0_gate"],
        }
    )
    value["manifest_sha256"] = common._self_hash(value, "manifest_sha256")
    return value


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("manifest_sha256") != common._self_hash(
        manifest, "manifest_sha256"
    ):
        raise PostEarningsDriftError("activation manifest content hash is invalid")
    if (
        manifest.get("variant_id") != VARIANT_ID
        or manifest.get("variant_ordinal") != VARIANT_ORDINAL
        or manifest.get("claim_scope") != "FALSIFICATION_ONLY"
        or manifest.get("implementation_sha256")
        != sha256_file(Path(__file__).resolve())
    ):
        raise PostEarningsDriftError("activation identity or implementation drifted")
    if (
        manifest.get("provider_requests_authorized_before_inspection") is not False
        or manifest.get("return_evaluation_authorized_before_input_inspection")
        is not False
        or manifest.get("broker_actions_authorized") is not False
    ):
        raise PostEarningsDriftError("activation access gates are not closed")
    denominator = manifest.get("denominator", {})
    if (
        denominator.get("candidate_pairs") != EXPECTED_PAIRS
        or denominator.get("candidate_dates") != EXPECTED_DATES
        or denominator.get("maximum_selected_signals") != EXPECTED_PAIRS
    ):
        raise PostEarningsDriftError("activation denominator drifted")


def inspect_activation(
    manifest_path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    common._require_published((manifest_path,))
    recorded = _load_json(manifest_path)
    _validate_manifest(recorded)
    if recorded != build_manifest(source):
        raise PostEarningsDriftError("activation manifest does not rebuild")
    graph = _load_gzip(_candidate_path(source))
    if _hash(graph) != recorded["private_candidates"]["content_sha256"]:
        raise PostEarningsDriftError("private candidate graph drifted")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-event-activation-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "implementation_sha256": recorded["implementation_sha256"],
        "candidate_pairs": len(graph["pairs"]),
        "candidate_dates": len({row["date"] for row in graph["pairs"]}),
        "unique_symbols": len({row["symbol"] for row in graph["pairs"]}),
        "declared_prior_policy_trials": recorded["declared_prior_policy_trials"],
        "provider_requests": 0,
        "provider_requests_before_this_activation": (
            DISCARDED_EARNINGS_PROVIDER_REQUESTS
            + EFFECTIVE_EARNINGS_PROVIDER_REQUESTS
            + DISCARDED_MARKET_PROVIDER_REQUESTS
        ),
        "broker_actions": 0,
        "returns_computed": 0,
        "collection_authorized": True,
        "return_evaluation_authorized": False,
        "valid": True,
    }
    value["inspection_sha256"] = common._self_hash(value, "inspection_sha256")
    return value


def _validate_activation_inspection(
    manifest_path: Path,
    inspection_path: Path,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise PostEarningsDriftError("activation inspection hash is invalid")
    expected = inspect_activation(manifest_path, store)
    if recorded != expected or recorded.get("collection_authorized") is not True:
        raise PostEarningsDriftError("activation inspection does not rebuild")
    return manifest, recorded


def earnings_symbols(
    manifest_path: Path,
    inspection_path: Path,
    store: HistoricalDayStore | None = None,
) -> list[str]:
    source = store or HistoricalDayStore.from_env()
    _validate_activation_inspection(manifest_path, inspection_path, source)
    common._require_published((manifest_path, inspection_path))
    graph = _load_gzip(_candidate_path(source))
    return sorted({row["symbol"] for row in graph["pairs"]})


def _find_results(value: Any) -> list[Any] | None:
    if isinstance(value, Mapping):
        data = value.get("data")
        if isinstance(data, Mapping) and isinstance(data.get("results"), list):
            return data["results"]
        if isinstance(value.get("results"), list):
            return value["results"]
        for key in ("structuredContent", "structured_content"):
            found = _find_results(value.get(key))
            if found is not None:
                return found
        content = value.get("content")
        if isinstance(content, list):
            for item in content:
                found = _find_results(item)
                if found is not None:
                    return found
                if isinstance(item, Mapping) and isinstance(item.get("text"), str):
                    try:
                        parsed = json.loads(item["text"])
                    except json.JSONDecodeError:
                        continue
                    found = _find_results(parsed)
                    if found is not None:
                        return found
    return None


def _number_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _normalize_earnings(symbol: str, response: Any) -> dict[str, Any]:
    raw_results = _find_results(response)
    if raw_results is None:
        return {"symbol": symbol, "status": "provider_error", "results": []}
    results: list[dict[str, Any]] = []
    seen: set[tuple[int, int]] = set()
    for raw in raw_results:
        if not isinstance(raw, Mapping) or str(raw.get("symbol", "")).upper() != symbol:
            continue
        try:
            year = int(raw["year"])
            quarter = int(raw["quarter"])
        except (KeyError, TypeError, ValueError):
            continue
        if quarter not in {1, 2, 3, 4} or (year, quarter) in seen:
            continue
        seen.add((year, quarter))
        eps = raw.get("eps") if isinstance(raw.get("eps"), Mapping) else {}
        report = raw.get("report") if isinstance(raw.get("report"), Mapping) else None
        report_value = None
        if report is not None:
            report_date = report.get("date")
            timing = report.get("timing")
            try:
                date.fromisoformat(str(report_date))
            except ValueError:
                report_date = None
            if report_date and timing in {"am", "pm"}:
                report_value = {
                    "date": str(report_date),
                    "timing": str(timing),
                    "verified": report.get("verified") is True,
                }
        results.append(
            {
                "year": year,
                "quarter": quarter,
                "actual_eps": _number_or_none(eps.get("actual")),
                "estimated_eps": _number_or_none(eps.get("estimate")),
                "report": report_value,
            }
        )
    results.sort(key=lambda row: (row["year"], row["quarter"]))
    return {"symbol": symbol, "status": "ok", "results": results}


def ingest_earnings(
    manifest_path: Path,
    inspection_path: Path,
    lines: Sequence[str],
    discarded_provider_requests: int = DISCARDED_EARNINGS_PROVIDER_REQUESTS,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, inspection = _validate_activation_inspection(
        manifest_path, inspection_path, source
    )
    common._require_published((manifest_path, inspection_path))
    symbols = earnings_symbols(manifest_path, inspection_path, source)
    if discarded_provider_requests != DISCARDED_EARNINGS_PROVIDER_REQUESTS:
        raise PostEarningsDriftError("discarded provider-request count drifted")
    by_symbol: dict[str, dict[str, Any]] = {}
    for line in lines:
        if not line.strip() or line.strip() == "__END__":
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PostEarningsDriftError("earnings input line is not JSON") from exc
        if not isinstance(item, Mapping):
            raise PostEarningsDriftError("earnings input line must be an object")
        symbol = str(item.get("symbol", "")).strip().upper()
        if symbol not in symbols or symbol in by_symbol:
            raise PostEarningsDriftError("earnings input symbol is unknown or repeated")
        by_symbol[symbol] = _normalize_earnings(symbol, item.get("response"))
    if set(by_symbol) != set(symbols):
        missing = sorted(set(symbols) - set(by_symbol))
        raise PostEarningsDriftError(
            f"earnings results missing {len(missing)} frozen symbols"
        )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "activation_inspection_sha256": inspection["inspection_sha256"],
        "provider": "Robinhood read-only earnings results",
        "provider_requests": len(symbols) + discarded_provider_requests,
        "effective_provider_requests": len(symbols),
        "discarded_ingestion_transport_requests": discarded_provider_requests,
        "broker_actions": 0,
        "returns_computed": 0,
        "results_by_symbol": by_symbol,
    }
    path = _earnings_path(source)
    if path.exists() and _load_gzip(path) != payload:
        raise PostEarningsDriftError("private earnings metadata drifted")
    if not path.exists():
        _write_gzip(path, payload)
    counts = Counter(row["status"] for row in by_symbol.values())
    status: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status_kind": "stage0-earnings-metadata-collection",
        "dataset_id": DATASET_ID,
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "activation_inspection_sha256": inspection["inspection_sha256"],
        "requested_symbols": len(symbols),
        "provider_requests": len(symbols) + discarded_provider_requests,
        "effective_provider_requests": len(symbols),
        "discarded_ingestion_transport_requests": discarded_provider_requests,
        "provider_status_counts": dict(sorted(counts.items())),
        "earnings_rows": sum(len(row["results"]) for row in by_symbol.values()),
        "private_payload_sha256": sha256_file(path),
        "broker_actions": 0,
        "returns_computed": 0,
        "status": "READY",
    }
    status["status_sha256"] = common._self_hash(status, "status_sha256")
    _write_json(PUBLIC_EARNINGS_STATUS, status)
    return status


def _provider_day(timestamp: Any) -> str:
    try:
        return (
            datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            .astimezone(EASTERN)
            .date()
            .isoformat()
        )
    except ValueError as exc:
        raise PostEarningsDriftError("provider timestamp is invalid") from exc


def _normalize_daily(symbol: str, rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in rows:
        day = _provider_day(raw.get("t"))
        if day in seen:
            raise PostEarningsDriftError(f"{symbol} {day}: duplicate daily bar")
        seen.add(day)
        try:
            opened = float(raw["o"])
            high = float(raw["h"])
            low = float(raw["l"])
            close = float(raw["c"])
            volume = int(float(raw["v"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise PostEarningsDriftError(f"{symbol} {day}: malformed daily bar") from exc
        if (
            min(opened, high, low, close) <= 0
            or low > min(opened, close)
            or high < max(opened, close)
            or volume < 0
        ):
            raise PostEarningsDriftError(f"{symbol} {day}: invalid daily OHLCV")
        output.append(
            {
                "date": day,
                "open": opened,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
    output.sort(key=lambda row: row["date"])
    return output


def _normalize_minutes(
    symbol: str, day: str, rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in rows:
        try:
            observed = datetime.fromisoformat(str(raw["t"]).replace("Z", "+00:00"))
            observed = observed.astimezone(EASTERN)
        except (KeyError, TypeError, ValueError) as exc:
            raise PostEarningsDriftError(f"{symbol} {day}: malformed minute bar") from exc
        if observed.date().isoformat() != day:
            raise PostEarningsDriftError(f"{symbol} {day}: invalid minute bar")
        if not time(9, 30) <= observed.time() < time(16, 0):
            continue
        try:
            opened = float(raw["o"])
            high = float(raw["h"])
            low = float(raw["l"])
            close = float(raw["c"])
            volume = int(float(raw["v"]))
            wap = float(raw.get("vw") or 0)
        except (KeyError, TypeError, ValueError) as exc:
            raise PostEarningsDriftError(f"{symbol} {day}: malformed minute bar") from exc
        stamp = observed.isoformat()
        if (
            stamp in seen
            or min(opened, high, low, close) <= 0
            or low > min(opened, close)
            or high < max(opened, close)
            or volume < 0
            or wap < 0
        ):
            raise PostEarningsDriftError(f"{symbol} {day}: invalid minute bar")
        seen.add(stamp)
        output.append(
            {
                "time_et": stamp,
                "open": opened,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "wap": wap,
            }
        )
    output.sort(key=lambda row: row["time_et"])
    return output


def _validate_earnings_collection(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    status = _load_json(PUBLIC_EARNINGS_STATUS)
    path = _earnings_path(store)
    expected_manifest = manifest.get("collection_contract", {}).get(
        "earnings_source_manifest_sha256", manifest.get("manifest_sha256")
    )
    if (
        status.get("status_sha256") != common._self_hash(status, "status_sha256")
        or status.get("status") != "READY"
        or status.get("manifest_sha256") != expected_manifest
        or status.get("status_sha256")
        != manifest.get("collection_contract", {}).get(
            "earnings_status_sha256", status.get("status_sha256")
        )
        or status.get("private_payload_sha256") != sha256_file(path)
        or status.get("returns_computed") != 0
        or status.get("broker_actions") != 0
    ):
        raise PostEarningsDriftError("earnings collection is not usable")
    payload = _load_gzip(path)
    if payload.get("manifest_sha256") != expected_manifest:
        raise PostEarningsDriftError("private earnings payload identity drifted")
    return status, payload


def collect_market(
    manifest_path: Path,
    inspection_path: Path,
    *,
    env_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env(env_path)
    manifest, inspection = _validate_activation_inspection(
        manifest_path, inspection_path, source
    )
    common._require_published(
        (manifest_path, inspection_path, PUBLIC_EARNINGS_STATUS)
    )
    _validate_earnings_collection(manifest, source)
    graph = _load_gzip(_candidate_path(source))
    path = _market_path(source)
    if path.exists():
        payload = _load_gzip(path)
        if payload.get("manifest_sha256") != manifest["manifest_sha256"]:
            raise PostEarningsDriftError("private market payload identity drifted")
    else:
        if shutil.disk_usage(source.root).free < MINIMUM_FREE_BYTES:
            raise PostEarningsDriftError("historical store is below its reserve")
        config = AlpacaBulkConfig.from_env(env_path)
        symbols = sorted({row["symbol"] for row in graph["pairs"]})
        start_day = date.fromisoformat(manifest["collection_contract"]["daily_start"])
        end_day = date.fromisoformat(
            manifest["collection_contract"]["daily_end_inclusive"]
        )
        daily_by_symbol: dict[str, list[dict[str, Any]]] = {}
        minutes_by_pair: dict[str, list[dict[str, Any]]] = {}
        effective_provider_requests = 0
        provider_minute_rows = 0
        ignored_non_regular_session_rows = 0
        with AlpacaBulkBarsClient(config) as client:
            for offset in range(0, len(symbols), config.batch_size):
                batch = symbols[offset : offset + config.batch_size]
                fetched, pages = client.fetch(
                    batch,
                    timeframe="1Day",
                    start=datetime.combine(start_day, time(0), tzinfo=EASTERN),
                    end=datetime.combine(
                        end_day + timedelta(days=1), time(0), tzinfo=EASTERN
                    ),
                )
                effective_provider_requests += pages
                for symbol in batch:
                    daily_by_symbol[symbol] = _normalize_daily(
                        symbol, fetched.get(symbol, [])
                    )
            by_date: dict[str, list[str]] = defaultdict(list)
            for pair in graph["pairs"]:
                by_date[pair["date"]].append(pair["symbol"])
            for day in sorted(by_date):
                session_day = date.fromisoformat(day)
                day_symbols = sorted(by_date[day])
                for offset in range(0, len(day_symbols), config.batch_size):
                    batch = day_symbols[offset : offset + config.batch_size]
                    fetched, pages = client.fetch(
                        batch,
                        timeframe="1Min",
                        start=datetime.combine(
                            session_day, time(9, 30), tzinfo=EASTERN
                        ),
                        end=datetime.combine(session_day, time(16), tzinfo=EASTERN),
                    )
                    effective_provider_requests += pages
                    for symbol in batch:
                        raw_rows = fetched.get(symbol, [])
                        normalized = _normalize_minutes(symbol, day, raw_rows)
                        provider_minute_rows += len(raw_rows)
                        ignored_non_regular_session_rows += len(raw_rows) - len(
                            normalized
                        )
                        minutes_by_pair[f"{day}|{symbol}"] = normalized
        payload = {
            "schema_version": SCHEMA_VERSION,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "activation_inspection_sha256": inspection["inspection_sha256"],
            "provider": "Alpaca historical SIP",
            "feed": "sip",
            "adjustment": "raw",
            "provider_requests": (
                DISCARDED_MARKET_PROVIDER_REQUESTS + effective_provider_requests
            ),
            "effective_provider_requests": effective_provider_requests,
            "discarded_provider_requests": DISCARDED_MARKET_PROVIDER_REQUESTS,
            "provider_minute_rows": provider_minute_rows,
            "ignored_non_regular_session_rows": ignored_non_regular_session_rows,
            "broker_actions": 0,
            "returns_computed": 0,
            "daily_by_symbol": daily_by_symbol,
            "minutes_by_pair": minutes_by_pair,
        }
        _write_gzip(path, payload)
    daily = payload.get("daily_by_symbol", {})
    minutes = payload.get("minutes_by_pair", {})
    status: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status_kind": "stage0-event-market-collection",
        "dataset_id": DATASET_ID,
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "activation_inspection_sha256": inspection["inspection_sha256"],
        "requested_symbols": len({row["symbol"] for row in graph["pairs"]}),
        "requested_pairs": len(graph["pairs"]),
        "symbols_with_daily_rows": sum(bool(rows) for rows in daily.values()),
        "daily_rows": sum(len(rows) for rows in daily.values()),
        "pairs_with_minute_rows": sum(bool(rows) for rows in minutes.values()),
        "minute_rows": sum(len(rows) for rows in minutes.values()),
        "provider_requests": int(payload["provider_requests"]),
        "effective_provider_requests": int(payload["effective_provider_requests"]),
        "discarded_provider_requests": int(payload["discarded_provider_requests"]),
        "provider_minute_rows": int(payload["provider_minute_rows"]),
        "ignored_non_regular_session_rows": int(
            payload["ignored_non_regular_session_rows"]
        ),
        "private_payload_sha256": sha256_file(path),
        "broker_actions": 0,
        "returns_computed": 0,
        "status": "READY",
    }
    status["status_sha256"] = common._self_hash(status, "status_sha256")
    _write_json(PUBLIC_MARKET_STATUS, status)
    return status


def _validate_market_collection(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    status = _load_json(PUBLIC_MARKET_STATUS)
    path = _market_path(store)
    if (
        status.get("status_sha256") != common._self_hash(status, "status_sha256")
        or status.get("status") != "READY"
        or status.get("manifest_sha256") != manifest.get("manifest_sha256")
        or status.get("private_payload_sha256") != sha256_file(path)
        or status.get("returns_computed") != 0
        or status.get("broker_actions") != 0
    ):
        raise PostEarningsDriftError("market collection is not usable")
    payload = _load_gzip(path)
    if payload.get("manifest_sha256") != manifest.get("manifest_sha256"):
        raise PostEarningsDriftError("private market payload identity drifted")
    return status, payload


def _complete_minute_session(rows: Sequence[Mapping[str, Any]], day: str) -> bool:
    expected = [
        datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
        + timedelta(minutes=offset)
        for offset in range(390)
    ]
    try:
        observed = [datetime.fromisoformat(str(row["time_et"])) for row in rows]
    except (KeyError, ValueError):
        return False
    return observed == expected


def _build_input_index(
    manifest: Mapping[str, Any],
    store: HistoricalDayStore,
    earnings: Mapping[str, Any],
    market: Mapping[str, Any],
) -> dict[str, Any]:
    graph = _load_gzip(_candidate_path(store))
    if _hash(graph) != manifest["private_candidates"]["content_sha256"]:
        raise PostEarningsDriftError("candidate graph drifted before input inspection")
    earnings_by_symbol = earnings.get("results_by_symbol", {})
    daily_by_symbol = market.get("daily_by_symbol", {})
    minutes_by_pair = market.get("minutes_by_pair", {})
    records: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for pair in graph["pairs"]:
        key = f"{pair['date']}|{pair['symbol']}"
        window = graph["windows"][key]
        daily_rows = daily_by_symbol.get(pair["symbol"], [])
        daily_dates = {
            str(row.get("date"))
            for row in daily_rows
            if isinstance(row, Mapping)
        }
        required_dates = [window["prior_session"], *window["holding_sessions"]]
        earnings_row = earnings_by_symbol.get(pair["symbol"])
        minute_rows = minutes_by_pair.get(key, [])
        if not isinstance(earnings_row, Mapping):
            status = "missing_earnings_metadata"
        elif earnings_row.get("status") != "ok":
            status = "earnings_provider_error"
        elif any(day not in daily_dates for day in required_dates):
            status = "missing_daily_window"
        elif not isinstance(minute_rows, list) or not _complete_minute_session(
            minute_rows, pair["date"]
        ):
            status = "incomplete_minute_session"
        else:
            status = "complete"
        counts[status] += 1
        records.append({"key": key, "status": status})
    return {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "private_candidates_sha256": manifest["private_candidates"][
            "content_sha256"
        ],
        "private_earnings_file_sha256": sha256_file(_earnings_path(store)),
        "private_market_file_sha256": sha256_file(_market_path(store)),
        "records": records,
        "status_counts": dict(sorted(counts.items())),
        "returns_computed": 0,
    }


def inspect_inputs(
    manifest_path: Path,
    activation_inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, activation = _validate_activation_inspection(
        manifest_path, activation_inspection_path, source
    )
    if require_published:
        common._require_published(
            (
                manifest_path,
                activation_inspection_path,
                PUBLIC_EARNINGS_STATUS,
                PUBLIC_MARKET_STATUS,
            )
        )
    earnings_status, earnings = _validate_earnings_collection(manifest, source)
    market_status, market = _validate_market_collection(manifest, source)
    index = _build_input_index(manifest, source, earnings, market)
    path = _input_index_path(source)
    if path.exists() and _load_gzip(path) != index:
        raise PostEarningsDriftError("private input index drifted")
    if not path.exists():
        _write_gzip(path, index)
    counts = index["status_counts"]
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-event-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "activation_inspection_sha256": activation["inspection_sha256"],
        "earnings_status_sha256": earnings_status["status_sha256"],
        "market_status_sha256": market_status["status_sha256"],
        "candidate_pairs": manifest["denominator"]["candidate_pairs"],
        "complete_pairs": int(counts.get("complete", 0)),
        "status_counts": counts,
        "private_input_index_sha256": _hash(index),
        "provider_requests_during_inspection": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "return_evaluation_authorized": True,
        "valid": True,
    }
    value["inspection_sha256"] = common._self_hash(value, "inspection_sha256")
    return value


def _validate_input_inspection(
    manifest_path: Path,
    activation_inspection_path: Path,
    input_inspection_path: Path,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    recorded = _load_json(input_inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise PostEarningsDriftError("input inspection hash is invalid")
    rebuilt = inspect_inputs(
        manifest_path,
        activation_inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt or recorded.get("return_evaluation_authorized") is not True:
        raise PostEarningsDriftError("input inspection does not rebuild")
    _, earnings = _validate_earnings_collection(manifest, store)
    _, market = _validate_market_collection(manifest, store)
    graph = _load_gzip(_candidate_path(store))
    index = _load_gzip(_input_index_path(store))
    if _hash(index) != recorded["private_input_index_sha256"]:
        raise PostEarningsDriftError("private input index hash drifted")
    return manifest, recorded, graph, earnings, market


def _merged_splits() -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[Any, ...]] = set()
    for path in SPLIT_PATHS:
        for symbol, rows in load_split_actions(path).items():
            for row in rows:
                identity = (
                    symbol,
                    row["execution_date"],
                    float(row["split_from"]),
                    float(row["split_to"]),
                    row.get("id"),
                )
                if identity not in seen:
                    seen.add(identity)
                    merged[symbol].append(dict(row))
    for rows in merged.values():
        rows.sort(key=lambda row: row["execution_date"])
    return dict(merged)


def _reaction_day(report: Mapping[str, Any], sessions: Sequence[str]) -> str | None:
    report_day = str(report.get("date", ""))
    if report_day not in sessions:
        return None
    if report.get("timing") == "am":
        return report_day
    if report.get("timing") == "pm":
        index = sessions.index(report_day)
        return sessions[index + 1] if index + 1 < len(sessions) else None
    return None


def _matching_positive_earnings(
    day: str, metadata: Mapping[str, Any], sessions: Sequence[str]
) -> Mapping[str, Any] | None:
    matches = []
    for row in metadata.get("results", []):
        if not isinstance(row, Mapping):
            continue
        report = row.get("report")
        actual = row.get("actual_eps")
        estimate = row.get("estimated_eps")
        if (
            isinstance(report, Mapping)
            and report.get("verified") is True
            and isinstance(actual, (int, float))
            and isinstance(estimate, (int, float))
            and actual > estimate
            and _reaction_day(report, sessions) == day
        ):
            matches.append(row)
    if len(matches) > 1:
        raise PostEarningsDriftError(f"{day}: multiple matching earnings reports")
    return matches[0] if matches else None


def _split_in_window(
    symbol: str, dates: Sequence[str], splits: Mapping[str, Sequence[Mapping[str, Any]]]
) -> bool:
    return any(row["execution_date"].isoformat() in dates for row in splits.get(symbol, []))


def _trade_outcome(
    *,
    minute_rows: Sequence[Mapping[str, Any]],
    holding_dates: Sequence[str],
    daily_by_date: Mapping[str, Mapping[str, Any]],
    stop: float,
    cost_bps: int,
) -> dict[str, Any]:
    entry_row = minute_rows[376]
    raw_entry = float(entry_row["open"])
    if stop <= 0 or stop >= raw_entry:
        raise PostEarningsDriftError("planned stop is not positive and below entry")
    exit_price: float | None = None
    exit_day = holding_dates[0]
    exit_reason = ""
    for row in minute_rows[376:]:
        if float(row["low"]) <= stop:
            exit_price = stop
            exit_reason = "stop"
            break
    if exit_price is None:
        for offset, day in enumerate(holding_dates[1:], start=1):
            row = daily_by_date[day]
            opened = float(row["open"])
            if opened <= stop:
                exit_price = opened
                exit_reason = "stop_gap"
            elif float(row["low"]) <= stop:
                exit_price = stop
                exit_reason = "stop"
            elif offset == len(holding_dates) - 1:
                exit_price = float(row["close"])
                exit_reason = "maximum_hold_close"
            if exit_price is not None:
                exit_day = day
                break
    if exit_price is None:
        raise PostEarningsDriftError("trade did not resolve within five dates")
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    if planned_risk <= 0:
        raise PostEarningsDriftError("cost-adjusted risk is not positive")
    return {
        "entry_date": holding_dates[0],
        "exit_date": exit_day,
        "exit_reason": exit_reason,
        "stop_executed": exit_reason.startswith("stop"),
        "net_r": (exit_fill - entry_fill) / planned_risk,
    }


def build_result(
    manifest_path: Path,
    activation_inspection_path: Path,
    input_inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, inspection, graph, earnings, market = _validate_input_inspection(
        manifest_path, activation_inspection_path, input_inspection_path, source
    )
    if require_published:
        common._require_published(
            (manifest_path, activation_inspection_path, input_inspection_path)
        )
    sessions = _calendar()
    index = {
        row["key"]: row["status"] for row in _load_gzip(_input_index_path(source))["records"]
    }
    earnings_by_symbol = earnings["results_by_symbol"]
    daily_by_symbol = {
        symbol: {row["date"]: row for row in rows}
        for symbol, rows in market["daily_by_symbol"].items()
    }
    minutes_by_pair = market["minutes_by_pair"]
    splits = _merged_splits()
    dispositions: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    for pair in graph["pairs"]:
        day = pair["date"]
        symbol = pair["symbol"]
        key = f"{day}|{symbol}"
        if index[key] != "complete":
            dispositions[index[key]] += 1
            continue
        event = _matching_positive_earnings(day, earnings_by_symbol[symbol], sessions)
        if event is None:
            dispositions["not_verified_positive_earnings"] += 1
            continue
        window = graph["windows"][key]
        holding_dates = window["holding_sessions"]
        all_dates = [window["prior_session"], *holding_dates]
        if _split_in_window(symbol, all_dates, splits):
            dispositions["split_in_window"] += 1
            continue
        minute_rows = minutes_by_pair[key]
        decision_rows = minute_rows[:375]
        event_open = float(decision_rows[0]["open"])
        prior_close = float(daily_by_symbol[symbol][window["prior_session"]]["close"])
        gap_fraction = event_open / prior_close - 1
        if event_open <= 5:
            dispositions["open_not_above_5"] += 1
            continue
        if not 0.01 <= gap_fraction <= 0.08:
            dispositions["gap_outside_1_to_8_percent"] += 1
            continue
        decision_close = float(decision_rows[-1]["close"])
        total_volume = sum(int(row["volume"]) for row in decision_rows)
        if total_volume <= 0 or any(
            int(row["volume"]) > 0 and float(row["wap"]) <= 0
            for row in decision_rows
        ):
            dispositions["vwap_unavailable"] += 1
            continue
        vwap = sum(
            float(row["wap"]) * int(row["volume"]) for row in decision_rows
        ) / total_volume
        if decision_close <= event_open:
            dispositions["close_not_above_open"] += 1
            continue
        if decision_close <= vwap:
            dispositions["close_not_above_vwap"] += 1
            continue
        stop = min(float(row["low"]) for row in decision_rows)
        if stop >= float(minute_rows[376]["open"]):
            dispositions["invalid_stop_geometry"] += 1
            continue
        outcomes = {
            str(cost): _trade_outcome(
                minute_rows=minute_rows,
                holding_dates=holding_dates,
                daily_by_date=daily_by_symbol[symbol],
                stop=stop,
                cost_bps=cost,
            )
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        dispositions["selected"] += 1
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "event_date": day,
                "symbol": symbol,
                "gap_fraction": gap_fraction,
                "eps_surprise": float(event["actual_eps"])
                - float(event["estimated_eps"]),
                "entry_date": primary["entry_date"],
                "exit_date": primary["exit_date"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
    primary_metrics = common._metrics([float(row["net_r"]) for row in records])
    stress_metrics = {
        "10": common._metrics([float(row["stress_10bps_r"]) for row in records]),
        "20": common._metrics([float(row["stress_20bps_r"]) for row in records]),
    }
    gate = manifest["stage0_gate"]
    blockers: list[str] = []
    if len(records) < int(gate["minimum_closed_signals"]):
        blockers.append("closed signals are below the Stage 0 minimum")
    if primary_metrics["expectancy_r"] is None or primary_metrics["expectancy_r"] <= 0:
        blockers.append("primary expectancy is not positive")
    if not primary_metrics["profit_factor_infinite"] and (
        primary_metrics["profit_factor"] is None
        or primary_metrics["profit_factor"] < float(gate["minimum_profit_factor"])
    ):
        blockers.append("primary profit factor is below the Stage 0 minimum")
    if primary_metrics["maximum_drawdown_r"] > float(gate["maximum_drawdown_r"]):
        blockers.append("primary drawdown exceeds the Stage 0 maximum")
    if stress_metrics["20"]["total_r"] <= 0:
        blockers.append("20 bps-per-side total R is not positive")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-falsification",
        "variant_id": VARIANT_ID,
        "strategy_version": manifest["strategy_version"],
        "mechanism_family": manifest["mechanism_family"],
        "base_rules_hash": manifest["base_rules_hash"],
        "activation_rules_hash": manifest["activation_rules_hash"],
        "manifest_sha256": manifest["manifest_sha256"],
        "input_inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": manifest["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_during_evaluation": 0,
        "broker_actions": 0,
        "denominator": {
            "candidate_pairs": manifest["denominator"]["candidate_pairs"],
            "complete_pairs": inspection["complete_pairs"],
            "closed_signals": len(records),
            "rule_violations": 0,
        },
        "disposition_counts": dict(sorted(dispositions.items())),
        "primary_5bps": primary_metrics,
        "stress": stress_metrics,
        "stage0_survived": not blockers,
        "stage0_blockers": blockers,
        "next_action": (
            "freeze a representative development corpus without changing rules"
            if not blockers
            else "retire this exact variant and advance to the next frozen mechanism"
        ),
        "maturity_effect": "NONE",
        "records": records,
    }
    value["result_sha256"] = common._self_hash(value, "result_sha256")
    return value


def inspect_result(
    manifest_path: Path,
    activation_inspection_path: Path,
    input_inspection_path: Path,
    result_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise PostEarningsDriftError("Stage 0 result hash is invalid")
    rebuilt = build_result(
        manifest_path,
        activation_inspection_path,
        input_inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt:
        raise PostEarningsDriftError("Stage 0 result does not rebuild")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["input_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": recorded["denominator"]["closed_signals"],
        "stage0_survived": recorded["stage0_survived"],
        "maturity_effect": "NONE",
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = common._self_hash(value, "inspection_sha256")
    return value


def _publish(value: Mapping[str, Any], path: Path) -> None:
    if "inspection_kind" in value:
        identity = str(value.get("inspection_sha256", ""))
    elif "result_kind" in value:
        identity = str(value.get("result_sha256", ""))
    else:
        identity = str(value.get("manifest_sha256", ""))
    if not identity or not path.name.endswith(f"-{identity}.json"):
        raise PostEarningsDriftError("output filename must end with content hash")
    if path.resolve().parent not in {
        ACTIVATION_ROOT.resolve(),
        INSPECTION_ROOT.resolve(),
        RESULT_ROOT.resolve(),
    }:
        raise PostEarningsDriftError("output path is outside evidence roots")
    _write_json(path.resolve(), value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--output", type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--output", type=Path)
    symbols = commands.add_parser("earnings-symbols")
    symbols.add_argument("manifest", type=Path)
    symbols.add_argument("activation_inspection", type=Path)
    ingest = commands.add_parser("ingest-earnings")
    ingest.add_argument("manifest", type=Path)
    ingest.add_argument("activation_inspection", type=Path)
    ingest.add_argument(
        "--discarded-provider-requests",
        type=int,
        default=DISCARDED_EARNINGS_PROVIDER_REQUESTS,
    )
    collect = commands.add_parser("collect-market")
    collect.add_argument("manifest", type=Path)
    collect.add_argument("activation_inspection", type=Path)
    collect.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    inspect_data = commands.add_parser("inspect-inputs")
    inspect_data.add_argument("manifest", type=Path)
    inspect_data.add_argument("activation_inspection", type=Path)
    inspect_data.add_argument("--output", type=Path)
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("activation_inspection", type=Path)
    evaluate.add_argument("input_inspection", type=Path)
    evaluate.add_argument("--output", type=Path)
    inspect_outcome = commands.add_parser("inspect-result")
    inspect_outcome.add_argument("manifest", type=Path)
    inspect_outcome.add_argument("activation_inspection", type=Path)
    inspect_outcome.add_argument("input_inspection", type=Path)
    inspect_outcome.add_argument("result", type=Path)
    inspect_outcome.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            value: Any = build_manifest()
        elif args.command == "inspect":
            value = inspect_activation(args.manifest)
        elif args.command == "earnings-symbols":
            value = earnings_symbols(args.manifest, args.activation_inspection)
        elif args.command == "ingest-earnings":
            input_lines = []
            for line in sys.stdin:
                if line.strip() == "__END__":
                    break
                input_lines.append(line)
            value = ingest_earnings(
                args.manifest,
                args.activation_inspection,
                input_lines,
                discarded_provider_requests=args.discarded_provider_requests,
            )
        elif args.command == "collect-market":
            value = collect_market(
                args.manifest,
                args.activation_inspection,
                env_path=args.env_file,
            )
        elif args.command == "inspect-inputs":
            value = inspect_inputs(args.manifest, args.activation_inspection)
        elif args.command == "evaluate":
            value = build_result(
                args.manifest, args.activation_inspection, args.input_inspection
            )
        else:
            value = inspect_result(
                args.manifest,
                args.activation_inspection,
                args.input_inspection,
                args.result,
            )
        if args.command not in {
            "earnings-symbols",
            "ingest-earnings",
            "collect-market",
        } and args.output:
            _publish(value, args.output)
    except (
        PostEarningsDriftError,
        common.EtfOrbStage0Error,
        ScannerReplayError,
        OSError,
    ) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
