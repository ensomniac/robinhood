"""Freeze, collect, and evaluate representative equity-gap validation samples.

The workflow deliberately separates pre-09:35 selection from post-09:35
outcomes.  ``freeze`` binds both chronological samples before any strategy
outcome is calculated.  Development is then collected and evaluated first;
confirmation remains inaccessible until a committed development result passes
the unchanged robustness gates.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
import shutil
import statistics
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import equity_gap_continuation_stage0 as stage0
import etf_or_momentum_stage0 as common
import portfolio_maturity as maturity
from historical_store import (
    HistoricalDayStore,
    build_dataset,
    compact_bar,
    expand_bar,
    sha256_file,
)
from scanner_replay_alpaca import (
    ALPACA_BARS_URL,
    AlpacaBulkBarsClient,
    AlpacaBulkConfig,
    ScannerReplayError,
    _parse_provider_bar,
)


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
SCHEMA_VERSION = 1
DATASET_ID = "dataset-equity-gap-continuation-validation-2026-07-21-v3"
STRATEGY_ID = "equity-gap-continuation"
STRATEGY_VERSION = "1.0.0"
MECHANISM_FAMILY = "equity-gap-continuation"
SOURCE_VARIANT_ID = "equity-gap-continuation-v1"
SOURCE_VARIANT_ORDINAL = 3
DEVELOPMENT_DATE_COUNT = 120
EMBARGO_DATE_COUNT = 5
CONFIRMATION_DATE_COUNT = 75
MINIMUM_FREE_BYTES = 20 * 1024**3
PHASES = ("development", "confirmation")

STAGE0_ACTIVATION = (
    PROJECT_ROOT
    / "strategy_tournament"
    / "activations"
    / "equity-gap-continuation-v1-3bc6f70c2a331e70b00a1dda076b258ce46f1175d062ecee0a641e4c4ab06f3f.json"
)
STAGE0_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-21-equity-gap-continuation-stage0-1f447fb4e066b041463e62e26d7e752a12e1b90da7c13b6881ea42e10dda9e94.json"
)
STAGE0_RESULT_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament"
    / "inspections"
    / "equity-gap-continuation-v1-result-66ede02688e090973e482ecc9c491349238d14e2af63fd02c336b99a8564c7be.json"
)
PORTFOLIO_CONFIG = PROJECT_ROOT / "portfolio_config.toml"
OUTPUT_ROOT = PROJECT_ROOT / "strategy_validation" / "equity_gap_continuation"
MANIFEST_ROOT = OUTPUT_ROOT / "manifests"
INSPECTION_ROOT = OUTPUT_ROOT / "inspections"
RESULT_ROOT = PROJECT_ROOT / "research_results"
PUBLIC_STATUS = OUTPUT_ROOT / "collection-status.json"

SOURCE_TRANCHES = (
    {
        "name": "development-v2",
        "selection": PROJECT_ROOT
        / "historical_batches/development_tranche_v2/selection-2026-07-19-100-days.json",
        "status": PROJECT_ROOT
        / "historical_batches/development_tranche_v2/scanner-collection-status.json",
        "manifest": PROJECT_ROOT
        / "historical_batches/development_tranche_v2/scanner_manifests/dataset-production-scanner-replay-2026-07-19-development-v2-e500cf2a9a3f63f97496b76d31ffaf98d7e849696835c707fb7daf4fe85a3643.json",
        "detail": PROJECT_ROOT
        / "learning_runs/scanner_expansion_v2/scanner-replay-detail.json",
    },
    {
        "name": "development-v3",
        "selection": PROJECT_ROOT
        / "historical_batches/development_tranche_v3/selection-2026-07-20-100-days.json",
        "status": PROJECT_ROOT
        / "historical_batches/development_tranche_v3/scanner-collection-status.json",
        "manifest": PROJECT_ROOT
        / "historical_batches/development_tranche_v3/scanner_manifests/dataset-production-scanner-replay-2026-07-20-development-v3-1a37bc3d141dff3741bc965303e490eaa59d8f76071b9e5d1bd2b301eb878926.json",
        "detail": PROJECT_ROOT
        / "learning_runs/development_tranche_v3/scanner_replay/scanner-replay-detail.json",
    },
)


class GapValidationError(RuntimeError):
    """The representative validation evidence is incomplete or has drifted."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GapValidationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GapValidationError(f"{path} must contain an object")
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _json_hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


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


def _load_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise GapValidationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GapValidationError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise GapValidationError(f"public evidence path escaped repository: {path}") from exc


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "equity_gap_continuation_validation" / DATASET_ID


def _selection_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "frozen-candidates.json.gz"


def _input_index_path(store: HistoricalDayStore, phase: str) -> Path:
    return _private_root(store) / f"{phase}-input-index.json.gz"


def _collection_state_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "collection-state.json"


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _published(value: Mapping[str, Any], path: Path, identity_field: str) -> None:
    identity = str(value.get(identity_field, ""))
    if not identity or not path.name.endswith(f"-{identity}.json"):
        raise GapValidationError("output filename must end with its content hash")
    resolved = path.resolve()
    allowed = {MANIFEST_ROOT.resolve(), INSPECTION_ROOT.resolve(), RESULT_ROOT.resolve()}
    if resolved.parent not in allowed:
        raise GapValidationError("output path is outside an approved evidence directory")
    _write_json(resolved, value)


def _source_graph() -> tuple[list[str], dict[str, dict[str, Any]], list[dict[str, Any]]]:
    all_dates: list[str] = []
    by_date: dict[str, dict[str, Any]] = {}
    bindings: list[dict[str, Any]] = []
    for source in SOURCE_TRANCHES:
        selection = _load_json(source["selection"])
        status = _load_json(source["status"])
        manifest = _load_json(source["manifest"])
        detail = _load_json(source["detail"])
        dates = selection.get("selected_dates")
        if (
            not isinstance(dates, list)
            or len(dates) != 100
            or len(set(dates)) != 100
            or selection.get("target_outcomes_observed_or_derived") is not False
        ):
            raise GapValidationError(f"{source['name']}: source selection is not outcome-locked")
        if (
            status.get("status") != "READY"
            or status.get("target_outcomes_observed_or_derived") is not False
            or status.get("inspection", {}).get("valid") is not True
        ):
            raise GapValidationError(f"{source['name']}: scanner source is not inspected READY")
        if set(detail.get("dates", {})) != set(dates):
            raise GapValidationError(f"{source['name']}: detail date denominator drifted")
        if manifest.get("selection", {}).get("file_sha256") != sha256_file(source["selection"]):
            raise GapValidationError(f"{source['name']}: selection binding drifted")
        overlap = set(all_dates).intersection(dates)
        if overlap:
            raise GapValidationError(f"scanner tranches overlap on {sorted(overlap)[0]}")
        for day in dates:
            item = detail["dates"].get(day)
            if not isinstance(item, Mapping) or not isinstance(item.get("evaluations"), list):
                raise GapValidationError(f"{source['name']} {day}: evaluations are malformed")
            by_date[str(day)] = dict(item)
        all_dates.extend(str(day) for day in dates)
        bindings.append(
            {
                "name": source["name"],
                "selection_path": _repo_path(source["selection"]),
                "selection_sha256": sha256_file(source["selection"]),
                "status_path": _repo_path(source["status"]),
                "status_sha256": sha256_file(source["status"]),
                "manifest_path": _repo_path(source["manifest"]),
                "manifest_sha256": sha256_file(source["manifest"]),
                "detail_path": _repo_path(source["detail"]),
                "detail_sha256": sha256_file(source["detail"]),
                "dates": len(dates),
                "evaluated_symbol_sessions": sum(
                    len(detail["dates"][str(day)]["evaluations"]) for day in dates
                ),
            }
        )
    if len(all_dates) != 200:
        raise GapValidationError("representative source must contain exactly 200 dates")
    return sorted(all_dates), by_date, bindings


def _candidates(day: str, detail: Mapping[str, Any]) -> list[dict[str, Any]]:
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in detail["evaluations"]:
        if not isinstance(raw, Mapping):
            raise GapValidationError(f"{day}: malformed scanner evaluation")
        symbol = str(raw.get("symbol", ""))
        if not symbol or symbol in seen:
            raise GapValidationError(f"{day}: duplicate or empty scanner symbol")
        seen.add(symbol)
        try:
            opened = float(raw["open_price"])
            prior_close = float(raw["prior_close"])
        except (KeyError, TypeError, ValueError):
            continue
        if opened <= 5 or prior_close <= 0:
            continue
        gap = opened / prior_close - 1
        if not _gap_in_range(gap):
            continue
        selected.append(
            {
                "symbol": symbol,
                "instrument_id": str(raw.get("instrument_id", "")),
                "primary_exchange": str(raw.get("primary_exchange", "")),
                "open_price": opened,
                "prior_close": prior_close,
                "gap_fraction": gap,
            }
        )
    selected.sort(key=lambda row: str(row["symbol"]))
    return selected


def _gap_in_range(gap: float) -> bool:
    return 0.02 - 1e-12 <= gap <= 0.08 + 1e-12


def _evaluate_candidate(
    *, day: str, symbol: str, bars: list[dict[str, Any]], prior_close: float
) -> dict[str, Any]:
    """Apply the frozen Stage 0 signal semantics with stable inclusive gap bounds."""

    opening_price = float(bars[0]["open"])
    if opening_price <= 5:
        return {"date": day, "symbol": symbol, "status": "opening_price_not_above_5"}
    gap_fraction = opening_price / prior_close - 1
    if not _gap_in_range(gap_fraction):
        return {"date": day, "symbol": symbol, "status": "gap_outside_range"}
    opening = bars[: stage0.OPENING_RANGE_BARS]
    range_high = max(float(row["high"]) for row in opening)
    range_low = min(float(row["low"]) for row in opening)
    vwap = stage0._cumulative_vwap(bars)
    for index in range(stage0.OPENING_RANGE_BARS, len(bars) - 1):
        observed = stage0._bar_time(bars[index])
        if observed < stage0.SIGNAL_START:
            continue
        if observed > stage0.SIGNAL_END:
            break
        prior_volumes = [
            int(row["volume"])
            for row in bars[index - stage0.VOLUME_LOOKBACK_BARS : index]
        ]
        mean_volume = statistics.fmean(prior_volumes)
        volume_multiple = (
            int(bars[index]["volume"]) / mean_volume if mean_volume > 0 else 0.0
        )
        if (
            float(bars[index]["close"]) <= range_high
            or float(bars[index]["close"]) <= vwap[index]
            or volume_multiple < 1.5
        ):
            continue
        entry_index = index + 1
        entry_open = float(bars[entry_index]["open"])
        if entry_open <= range_low:
            return {
                "date": day,
                "symbol": symbol,
                "status": "nonpositive_stop_distance",
                "trigger_time_et": bars[index]["time_et"],
            }
        return {
            "date": day,
            "symbol": symbol,
            "status": "executable",
            "gap_fraction": gap_fraction,
            "volume_multiple": volume_multiple,
            "trigger_index": index,
            "entry_index": entry_index,
            "trigger_time_et": bars[index]["time_et"],
            "entry_time_et": bars[entry_index]["time_et"],
            "entry_open": entry_open,
            "stop": range_low,
        }
    return {"date": day, "symbol": symbol, "status": "no_breakout_trigger"}


def _stage0_binding() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    activation = _load_json(STAGE0_ACTIVATION)
    result = _load_json(STAGE0_RESULT)
    inspection = _load_json(STAGE0_RESULT_INSPECTION)
    stage0._validate_manifest(activation)
    if (
        result.get("result_sha256") != common._self_hash(result, "result_sha256")
        or result.get("stage0_survived") is not True
        or result.get("maturity_effect") != "NONE"
    ):
        raise GapValidationError("source Stage 0 result is not a valid survivor")
    if (
        inspection.get("inspection_sha256")
        != common._self_hash(inspection, "inspection_sha256")
        or inspection.get("result_sha256") != result.get("result_sha256")
        or inspection.get("stage0_survived") is not True
        or inspection.get("valid") is not True
    ):
        raise GapValidationError("source Stage 0 result inspection is invalid")
    return activation, result, inspection


def build_freeze(store: HistoricalDayStore | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    dates, details, source_bindings = _source_graph()
    activation, result, inspection = _stage0_binding()
    development_dates = dates[:DEVELOPMENT_DATE_COUNT]
    embargo_dates = dates[
        DEVELOPMENT_DATE_COUNT : DEVELOPMENT_DATE_COUNT + EMBARGO_DATE_COUNT
    ]
    confirmation_dates = dates[-CONFIRMATION_DATE_COUNT:]
    if not (
        max(development_dates) < min(embargo_dates) < max(embargo_dates) < min(confirmation_dates)
    ):
        raise GapValidationError("chronological development/embargo/confirmation split failed")
    phases: dict[str, Any] = {}
    for phase, phase_dates in (
        ("development", development_dates),
        ("confirmation", confirmation_dates),
    ):
        candidates_by_date = {
            day: _candidates(day, details[day]) for day in phase_dates
        }
        if any(not rows for rows in candidates_by_date.values()):
            raise GapValidationError(f"{phase} contains a date with no eligible gaps")
        phases[phase] = {
            "dates": phase_dates,
            "candidates_by_date": candidates_by_date,
            "candidate_symbol_sessions": sum(len(rows) for rows in candidates_by_date.values()),
        }
    production_universe = {
        "identity": "complete point-in-time active US common-stock universe at 09:35 ET",
        "source": "two inspected 2025 Alpaca SIP scanner-replay tranches backed by dated Massive type=CS security masters",
        "candidate_gate": "09:30 open strictly above 5 dollars and 2-8% inclusive versus prior completed regular-session close",
        "selection_time_et": "09:35:00",
        "substitutions_allowed": False,
        "missing_data_policy": "retain every frozen candidate in the denominator; a missing or noncontiguous full-session input cannot signal",
    }
    rules_hash = common._hash(
        {
            "base_rules_hash": activation["base_rules_hash"],
            "production_universe": production_universe,
            "selection_contract": activation["selection_contract"],
            "outcome_contract": activation["outcome_contract"],
        }
    )
    private = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": MECHANISM_FAMILY,
        "rules_hash": rules_hash,
        "selection_information_cutoff": "TARGET_SESSION_09:35_ET",
        "target_outcomes_observed_or_derived": False,
        "phases": phases,
        "embargo_dates": embargo_dates,
    }
    private_hash = _json_hash(private)
    existing_path = _selection_path(source)
    if existing_path.exists():
        if _json_hash(_load_gzip(existing_path)) != private_hash:
            raise GapValidationError("private frozen candidate graph has drifted")
    else:
        _write_gzip(existing_path, private)
    free_bytes = shutil.disk_usage(source.root).free
    if free_bytes < MINIMUM_FREE_BYTES:
        raise GapValidationError("historical store is below the 20 GiB validation reserve")
    config = maturity.load_config(PORTFOLIO_CONFIG)
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "representative-validation-freeze",
        "campaign_id": str(config.raw["campaign"]["id"]),
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": MECHANISM_FAMILY,
        "rules_hash": rules_hash,
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "portfolio_config_sha256": config.sha256,
        "claim_scope": "REPRESENTATIVE_DEVELOPMENT_AND_PREREGISTERED_CONFIRMATION",
        "production_universe": production_universe,
        "selection_contract": activation["selection_contract"],
        "outcome_contract": activation["outcome_contract"],
        "phase_contract": {
            "development": "first 120 chronological frozen source sessions",
            "embargo": "next five chronological frozen source sessions; excluded from every outcome sample",
            "confirmation": "final 75 chronological frozen source sessions; inaccessible until development passes unchanged",
            "maximum_hold_trading_days": int(config.raw["portfolio"]["maximum_holding_trading_days"]),
            "substitutions_allowed": False,
            "parameter_changes_allowed": False,
        },
        "denominator": {
            "source_dates": len(dates),
            "development_dates": len(development_dates),
            "development_candidate_symbol_sessions": phases["development"]["candidate_symbol_sessions"],
            "development_dates_sha256": _json_hash(development_dates),
            "embargo_dates": len(embargo_dates),
            "embargo_dates_sha256": _json_hash(embargo_dates),
            "confirmation_dates": len(confirmation_dates),
            "confirmation_candidate_symbol_sessions": phases["confirmation"]["candidate_symbol_sessions"],
            "confirmation_dates_sha256": _json_hash(confirmation_dates),
        },
        "private_selection": {
            "location": "LOCAL_HISTORICAL_DATA_ROOT/_derived/equity_gap_continuation_validation/"
            f"{DATASET_ID}/frozen-candidates.json.gz",
            "content_sha256": private_hash,
            "contains_target_returns": False,
        },
        "source_scanner_graph": source_bindings,
        "source_stage0": {
            "variant_id": SOURCE_VARIANT_ID,
            "variant_ordinal": SOURCE_VARIANT_ORDINAL,
            "activation_path": _repo_path(STAGE0_ACTIVATION),
            "activation_file_sha256": sha256_file(STAGE0_ACTIVATION),
            "activation_rules_hash": activation["activation_rules_hash"],
            "result_path": _repo_path(STAGE0_RESULT),
            "result_file_sha256": sha256_file(STAGE0_RESULT),
            "result_sha256": result["result_sha256"],
            "result_inspection_path": _repo_path(STAGE0_RESULT_INSPECTION),
            "result_inspection_file_sha256": sha256_file(STAGE0_RESULT_INSPECTION),
            "result_inspection_sha256": inspection["inspection_sha256"],
            "maturity_effect": "NONE",
        },
        "collection_contract": {
            "provider": "Alpaca historical SIP",
            "endpoint": ALPACA_BARS_URL,
            "feed": "sip",
            "adjustment": "raw",
            "asof": "-",
            "candidate_bars": "1Min 09:30:00 inclusive through 16:00:00 exclusive ET",
            "canonical_store_required": True,
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "provider_switching_allowed": False,
            "broker_actions_authorized": False,
        },
        "access_contract": {
            "return_evaluation_authorized_before_freeze_inspection": False,
            "development_collection_after_committed_freeze_inspection": True,
            "confirmation_collection_before_development_pass": False,
            "confirmation_outcomes_observed_or_derived": False,
        },
        "maturity_effect": "NONE_UNTIL_INSPECTED_RECORDS_ARE_APPENDED",
        "supersedes_freeze": {
            "manifest_sha256": "fc4674e3384b107043d80bf97a883ed297db3bc36b6f78f223e5cee297cc3809",
            "reason": "provider end timestamp was inclusive and returned a 16:00 bar; request boundary corrected to 15:59:59.999999 ET",
            "selection_rules_outcomes_or_costs_changed": False,
            "prior_return_results_computed": 0,
        },
    }
    manifest["manifest_sha256"] = common._self_hash(manifest, "manifest_sha256")
    return manifest, private


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("manifest_sha256") != common._self_hash(manifest, "manifest_sha256"):
        raise GapValidationError("validation manifest content hash is invalid")
    if (
        manifest.get("dataset_id") != DATASET_ID
        or manifest.get("strategy_id") != STRATEGY_ID
        or manifest.get("strategy_version") != STRATEGY_VERSION
        or manifest.get("mechanism_family") != MECHANISM_FAMILY
    ):
        raise GapValidationError("validation manifest identity is invalid")
    if manifest.get("implementation_sha256") != sha256_file(Path(__file__).resolve()):
        raise GapValidationError("validation implementation drifted")
    access = manifest.get("access_contract", {})
    if (
        access.get("return_evaluation_authorized_before_freeze_inspection") is not False
        or access.get("confirmation_collection_before_development_pass") is not False
        or access.get("confirmation_outcomes_observed_or_derived") is not False
    ):
        raise GapValidationError("validation access contract is not fail-closed")


def inspect_freeze(manifest_path: Path, store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    rebuilt_manifest, rebuilt_private = build_freeze(source)
    comparable_recorded = {
        key: value
        for key, value in manifest.items()
        if key not in {"registered_at", "manifest_sha256"}
    }
    comparable_rebuilt = {
        key: value
        for key, value in rebuilt_manifest.items()
        if key not in {"registered_at", "manifest_sha256"}
    }
    if comparable_recorded != comparable_rebuilt:
        raise GapValidationError("validation manifest source graph does not rebuild")
    selection = _load_gzip(_selection_path(source))
    private_hash = _json_hash(selection)
    if selection != rebuilt_private or private_hash != manifest["private_selection"]["content_sha256"]:
        raise GapValidationError("frozen private selection does not independently rebuild")
    if manifest["portfolio_config_sha256"] != sha256_file(PORTFOLIO_CONFIG):
        raise GapValidationError("portfolio promotion config drifted")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "representative-validation-freeze-inspection",
        "dataset_id": DATASET_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "rules_hash": manifest["rules_hash"],
        "manifest_sha256": manifest["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "private_selection_content_sha256": private_hash,
        "development_dates": len(selection["phases"]["development"]["dates"]),
        "development_candidate_symbol_sessions": selection["phases"]["development"]["candidate_symbol_sessions"],
        "embargo_trading_sessions": len(selection["embargo_dates"]),
        "confirmation_dates": len(selection["phases"]["confirmation"]["dates"]),
        "confirmation_candidate_symbol_sessions": selection["phases"]["confirmation"]["candidate_symbol_sessions"],
        "source_dates": 200,
        "source_date_overlap": 0,
        "returns_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
        "development_collection_authorized": True,
        "confirmation_collection_authorized": False,
        "development_universe_representative": True,
        "confirmation_untouched": True,
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _validate_freeze_inspection(
    manifest_path: Path, inspection_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(recorded, "inspection_sha256"):
        raise GapValidationError("freeze inspection content hash is invalid")
    if recorded != inspect_freeze(manifest_path, store):
        raise GapValidationError("freeze inspection does not independently rebuild")
    return manifest, recorded


def _phase_selection(store: HistoricalDayStore, manifest: Mapping[str, Any], phase: str) -> tuple[list[str], dict[str, list[dict[str, Any]]]]:
    if phase not in PHASES:
        raise GapValidationError(f"unsupported phase {phase}")
    frozen = _load_gzip(_selection_path(store))
    if _json_hash(frozen) != manifest["private_selection"]["content_sha256"]:
        raise GapValidationError("private selection hash drifted")
    selected = frozen["phases"][phase]
    return list(selected["dates"]), {
        str(day): [dict(row) for row in rows]
        for day, rows in selected["candidates_by_date"].items()
    }


def _full_minute_dataset(store: HistoricalDayStore, symbol: str, day: str) -> dict[str, Any] | None:
    return store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="1m",
        providers=("alpaca",),
        require_complete=True,
        feed="sip",
        adjustment="raw",
    )


def _confirmation_access_allowed(
    development_result_path: Path | None,
    development_result_inspection_path: Path | None,
) -> dict[str, Any]:
    if development_result_path is None or development_result_inspection_path is None:
        raise GapValidationError(
            "confirmation access requires a development result and independent inspection"
        )
    common._require_published(
        (development_result_path, development_result_inspection_path)
    )
    result = _load_json(development_result_path)
    if (
        result.get("result_kind") != "representative-validation-result"
        or result.get("sample_phase") != "development"
        or result.get("strategy_id") != STRATEGY_ID
        or result.get("development_passed") is not True
        or result.get("result_sha256") != common._self_hash(result, "result_sha256")
    ):
        raise GapValidationError("development did not pass unchanged; confirmation remains locked")
    inspection = _load_json(development_result_inspection_path)
    if (
        inspection.get("inspection_sha256")
        != common._self_hash(inspection, "inspection_sha256")
        or inspection.get("inspection_kind")
        != "representative-validation-result-inspection"
        or inspection.get("sample_phase") != "development"
        or inspection.get("result_sha256") != result.get("result_sha256")
        or inspection.get("result_file_sha256") != sha256_file(development_result_path)
        or inspection.get("phase_passed") is not True
        or inspection.get("valid") is not True
    ):
        raise GapValidationError(
            "development result inspection is invalid; confirmation remains locked"
        )
    return result


def collect_phase(
    manifest_path: Path,
    freeze_inspection_path: Path,
    *,
    phase: str,
    env_path: Path,
    development_result_path: Path | None = None,
    development_result_inspection_path: Path | None = None,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env(env_path)
    manifest, _ = _validate_freeze_inspection(manifest_path, freeze_inspection_path, source)
    common._require_published((manifest_path, freeze_inspection_path))
    if phase == "confirmation":
        _confirmation_access_allowed(
            development_result_path, development_result_inspection_path
        )
    dates, candidates_by_date = _phase_selection(source, manifest, phase)
    config = AlpacaBulkConfig.from_env(env_path)
    total_requested = total_cached = total_received = total_rows = 0
    requests = retries = 0
    unresolved: list[dict[str, Any]] = []
    started = datetime.now(UTC)
    with AlpacaBulkBarsClient(config) as client:
        for day in dates:
            candidates = candidates_by_date[day]
            symbols = [str(row["symbol"]) for row in candidates]
            missing = [symbol for symbol in symbols if _full_minute_dataset(source, symbol, day) is None]
            total_requested += len(symbols)
            total_cached += len(symbols) - len(missing)
            if missing:
                session_day = date.fromisoformat(day)
                session_start = datetime.combine(session_day, time(9, 30), tzinfo=EASTERN)
                session_end = datetime.combine(
                    session_day, time(15, 59, 59, 999999), tzinfo=EASTERN
                )
                for offset in range(0, len(missing), config.batch_size):
                    batch = missing[offset : offset + config.batch_size]
                    rows_by_symbol, _ = client.fetch(
                        batch,
                        timeframe="1Min",
                        start=session_start,
                        end=session_end,
                    )
                    captured_at = datetime.now(UTC).isoformat()
                    for symbol in batch:
                        raw_rows = rows_by_symbol.get(symbol, [])
                        if not raw_rows:
                            unresolved.append({"date": day, "symbol": symbol, "reason": "provider_returned_no_rows"})
                            continue
                        normalized = sorted(
                            (
                                _parse_provider_bar(row, day=day, window="regular")
                                for row in raw_rows
                            ),
                            key=lambda row: str(row["time_et"]),
                        )
                        timestamps = [str(row["time_et"]) for row in normalized]
                        if len(timestamps) != len(set(timestamps)) or len(normalized) > 390:
                            raise GapValidationError(f"{day} {symbol}: provider minute bars are ambiguous")
                        dataset = build_dataset(
                            kind="bars",
                            provider="alpaca",
                            rows=[compact_bar(row, day=day) for row in normalized],
                            channel="trades",
                            timeframe="1m",
                            feed="sip",
                            adjustment="raw",
                            session="regular",
                            scope="full_session",
                            quality={
                                "complete": True,
                                "requested_window_complete": True,
                                "sparse_intervals_allowed": True,
                            },
                            provenance={
                                "source_type": "alpaca_multi_symbol_gap_validation_collection",
                                "endpoint": ALPACA_BARS_URL,
                                "dataset_id": DATASET_ID,
                                "manifest_sha256": manifest["manifest_sha256"],
                                "sample_phase": phase,
                                "session_date": day,
                                "feed": "sip",
                                "adjustment": "raw",
                                "asof": "-",
                                "captured_at": captured_at,
                            },
                        )
                        source.merge(symbol, day, datasets=[dataset])
                        total_received += 1
                        total_rows += len(normalized)
            for symbol in symbols:
                if _full_minute_dataset(source, symbol, day) is None and not any(
                    row["date"] == day and row["symbol"] == symbol for row in unresolved
                ):
                    unresolved.append({"date": day, "symbol": symbol, "reason": "canonical_dataset_missing"})
        requests = client.request_count
        retries = client.retry_count
    completed = datetime.now(UTC)
    status = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "rules_hash": manifest["rules_hash"],
        "sample_phase": phase,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY" if not unresolved else "WAITING_DATA",
        "dates": len(dates),
        "candidate_symbol_sessions": total_requested,
        "cached_before_collection": total_cached,
        "provider_symbols_received": total_received,
        "provider_rows_received": total_rows,
        "provider_requests": requests,
        "provider_retries": retries,
        "unresolved_symbol_sessions": len(unresolved),
        "unresolved_reason_counts": dict(sorted(Counter(row["reason"] for row in unresolved).items())),
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
        "broker_actions": 0,
        "target_returns_computed": 0,
    }
    private_state = {
        **status,
        "unresolved": unresolved,
    }
    _write_json(_collection_state_path(source), private_state)
    _write_json(PUBLIC_STATUS, status)
    return status


def _dataset_rows(dataset: Mapping[str, Any], day: str, symbol: str) -> tuple[list[dict[str, Any]], bool]:
    rows = dataset.get("rows")
    if not isinstance(rows, list):
        raise GapValidationError(f"{day} {symbol}: dataset rows are malformed")
    expanded = [expand_bar(row) for row in rows]
    expanded.sort(key=lambda row: str(row["time_et"]))
    timestamps = [str(row["time_et"]) for row in expanded]
    if len(timestamps) != len(set(timestamps)) or len(expanded) > 390:
        raise GapValidationError(f"{day} {symbol}: minute timestamps are ambiguous")
    exact = len(expanded) == 390
    for index, row in enumerate(expanded):
        observed = datetime.fromisoformat(str(row["time_et"])).astimezone(EASTERN)
        if observed.date().isoformat() != day or not time(9, 30) <= observed.time() < time(16, 0):
            raise GapValidationError(f"{day} {symbol}: minute row escaped regular session")
        if row.get("interpolated") is not False:
            raise GapValidationError(f"{day} {symbol}: interpolated minute bar is forbidden")
        if exact:
            expected = datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN) + timedelta(minutes=index)
            if observed != expected:
                exact = False
    return expanded, exact


def inspect_phase_inputs(
    manifest_path: Path,
    freeze_inspection_path: Path,
    *,
    phase: str,
    development_result_path: Path | None = None,
    development_result_inspection_path: Path | None = None,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, _ = _validate_freeze_inspection(manifest_path, freeze_inspection_path, source)
    if phase == "confirmation":
        development = _confirmation_access_allowed(
            development_result_path, development_result_inspection_path
        )
        development_result_sha256 = development["result_sha256"]
    else:
        development_result_sha256 = None
    dates, candidates_by_date = _phase_selection(source, manifest, phase)
    index_rows: list[dict[str, Any]] = []
    total_bars = exact_sessions = sparse_sessions = 0
    for day in dates:
        for candidate in candidates_by_date[day]:
            symbol = str(candidate["symbol"])
            dataset = _full_minute_dataset(source, symbol, day)
            if dataset is None:
                raise GapValidationError(f"{day} {symbol}: full-session Alpaca SIP input is missing")
            rows, exact = _dataset_rows(dataset, day, symbol)
            if not rows:
                raise GapValidationError(f"{day} {symbol}: full-session dataset is empty")
            observed_open = float(rows[0]["open"])
            if not math.isclose(
                observed_open,
                float(candidate["open_price"]),
                rel_tol=0,
                abs_tol=1e-10,
            ):
                raise GapValidationError(
                    f"{day} {symbol}: frozen 09:30 open disagrees with full-session input"
                )
            total_bars += len(rows)
            exact_sessions += int(exact)
            sparse_sessions += int(not exact)
            index_rows.append(
                {
                    "date": day,
                    "symbol": symbol,
                    "dataset_id": dataset["id"],
                    "dataset_sha256": dataset["content_sha256"],
                    "rows": len(rows),
                    "exact_390_contiguous": exact,
                }
            )
    private_index = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "rules_hash": manifest["rules_hash"],
        "sample_phase": phase,
        "manifest_sha256": manifest["manifest_sha256"],
        "input_rows": index_rows,
    }
    _write_gzip(_input_index_path(source, phase), private_index)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "representative-validation-input-inspection",
        "dataset_id": DATASET_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "rules_hash": manifest["rules_hash"],
        "sample_phase": phase,
        "manifest_sha256": manifest["manifest_sha256"],
        "freeze_inspection_sha256": _load_json(freeze_inspection_path)["inspection_sha256"],
        "development_result_sha256": development_result_sha256,
        "dates": len(dates),
        "candidate_symbol_sessions": len(index_rows),
        "minute_bars": total_bars,
        "exact_390_contiguous_symbol_sessions": exact_sessions,
        "sparse_symbol_sessions_retained_as_no_signal": sparse_sessions,
        "private_input_index_content_sha256": _json_hash(private_index),
        "returns_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
        "return_evaluation_authorized": True,
        "confirmation_untouched_before_authorized_access": phase == "confirmation",
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _validate_phase_inspection(
    manifest_path: Path,
    freeze_inspection_path: Path,
    input_inspection_path: Path,
    *,
    phase: str,
    development_result_path: Path | None,
    development_result_inspection_path: Path | None,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest, freeze = _validate_freeze_inspection(manifest_path, freeze_inspection_path, store)
    inspection = _load_json(input_inspection_path)
    if inspection.get("inspection_sha256") != common._self_hash(inspection, "inspection_sha256"):
        raise GapValidationError("phase input inspection content hash is invalid")
    rebuilt = inspect_phase_inputs(
        manifest_path,
        freeze_inspection_path,
        phase=phase,
        development_result_path=development_result_path,
        development_result_inspection_path=development_result_inspection_path,
        store=store,
    )
    if inspection != rebuilt:
        raise GapValidationError("phase input inspection does not independently rebuild")
    return manifest, freeze, inspection


def _normalized_exact_rows(rows: Sequence[Mapping[str, Any]], day: str) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for row in rows:
        observed = datetime.fromisoformat(str(row["time_et"])).astimezone(EASTERN)
        result.append(
            {
                "time_et": observed.time().replace(tzinfo=None).isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": int(row.get("volume", 0)),
                "interpolated": False,
            }
        )
    stage0._validate_bars(result, "frozen", day)
    return result


def _phase_gate_blockers(metrics: maturity.RobustnessMetrics, phase: str) -> list[str]:
    config = maturity.load_config(PORTFOLIO_CONFIG)
    gate = config.raw["pilot_ready"]
    minimum = (
        int(gate["minimum_closed_historical_signals"])
        - int(gate["minimum_confirmation_signals"])
        if phase == "development"
        else int(gate["minimum_confirmation_signals"])
    )
    return maturity._robustness_blockers(
        phase,
        metrics,
        minimum_signals=minimum,
        expectancy_threshold=float(
            gate["minimum_expectancy_r"]
            if phase == "development"
            else gate["minimum_confirmation_expectancy_r"]
        ),
        gate=gate,
    )


def _serializable_metrics(metrics: maturity.RobustnessMetrics) -> dict[str, Any]:
    result = asdict(metrics)
    for field in (
        "profit_factor",
        "stress_10_profit_factor",
        "stress_20_profit_factor",
    ):
        value = result[field]
        result[f"{field}_infinite"] = isinstance(value, float) and math.isinf(value)
        if result[f"{field}_infinite"]:
            result[field] = None
    return result


def build_phase_result(
    manifest_path: Path,
    freeze_inspection_path: Path,
    input_inspection_path: Path,
    *,
    phase: str,
    development_result_path: Path | None = None,
    development_result_inspection_path: Path | None = None,
    store: HistoricalDayStore | None = None,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, freeze, inspection = _validate_phase_inspection(
        manifest_path,
        freeze_inspection_path,
        input_inspection_path,
        phase=phase,
        development_result_path=development_result_path,
        development_result_inspection_path=development_result_inspection_path,
        store=source,
    )
    if require_published:
        paths = [manifest_path, freeze_inspection_path, input_inspection_path]
        if development_result_path is not None:
            paths.append(development_result_path)
        if development_result_inspection_path is not None:
            paths.append(development_result_inspection_path)
        common._require_published(paths)
    dates, candidates_by_date = _phase_selection(source, manifest, phase)
    input_index = _load_gzip(_input_index_path(source, phase))
    if _json_hash(input_index) != inspection["private_input_index_content_sha256"]:
        raise GapValidationError("private phase input index drifted")
    index = {(row["date"], row["symbol"]): row for row in input_index["input_rows"]}
    dispositions: Counter[str] = Counter()
    ledger_records: list[dict[str, Any]] = []
    result_records: list[dict[str, Any]] = []
    recorded_at = str(manifest["registered_at"])
    for day in dates:
        executable: list[dict[str, Any]] = []
        bars_by_symbol: dict[str, list[dict[str, Any]]] = {}
        for candidate in candidates_by_date[day]:
            symbol = str(candidate["symbol"])
            frozen_input = index.get((day, symbol))
            dataset = _full_minute_dataset(source, symbol, day)
            if frozen_input is None or dataset is None or (
                dataset["id"] != frozen_input["dataset_id"]
                or dataset["content_sha256"] != frozen_input["dataset_sha256"]
            ):
                raise GapValidationError(f"{day} {symbol}: inspected input drifted")
            rows, exact = _dataset_rows(dataset, day, symbol)
            if not exact:
                dispositions["incomplete_or_noncontiguous_full_session"] += 1
                continue
            normalized = _normalized_exact_rows(rows, day)
            evaluated = _evaluate_candidate(
                day=day,
                symbol=symbol,
                bars=normalized,
                prior_close=float(candidate["prior_close"]),
            )
            dispositions[evaluated["status"]] += 1
            if evaluated["status"] == "executable":
                executable.append(evaluated)
                bars_by_symbol[symbol] = normalized
        executable.sort(
            key=lambda row: (
                str(row["entry_time_et"]),
                -float(row["volume_multiple"]),
                -float(row["gap_fraction"]),
                str(row["symbol"]),
            )
        )
        selected = executable[0] if executable else None
        ledger_records.append(
            {
                "schema_version": SCHEMA_VERSION,
                "record_type": "session",
                "recorded_at": recorded_at,
                "strategy_id": STRATEGY_ID,
                "strategy_version": STRATEGY_VERSION,
                "mechanism_family": MECHANISM_FAMILY,
                "rules_hash": manifest["rules_hash"],
                "date": day,
                "sample_phase": phase,
                "mode": "historical",
                "session_id": f"{day}-{STRATEGY_ID}-{phase}",
                "eligible_signal": selected is not None,
                "session_capture_complete": True,
                "rule_violations": [],
            }
        )
        if selected is None:
            dispositions["no_trade_date"] += 1
            continue
        dispositions["selected"] += 1
        dispositions["not_selected_daily_cap"] += len(executable) - 1
        outcomes = {
            str(cost): stage0._trade_outcome(
                selected,
                bars_by_symbol[str(selected["symbol"])],
                cost,
            )
            for cost in (stage0.PRIMARY_COST_BPS, *stage0.STRESS_COST_BPS)
        }
        primary = outcomes[str(stage0.PRIMARY_COST_BPS)]
        signal_id = f"{day}-{STRATEGY_ID}-{phase}-signal"
        ledger_record = {
            "schema_version": SCHEMA_VERSION,
            "record_type": "signal",
            "recorded_at": recorded_at,
            "strategy_id": STRATEGY_ID,
            "strategy_version": STRATEGY_VERSION,
            "mechanism_family": MECHANISM_FAMILY,
            "rules_hash": manifest["rules_hash"],
            "date": day,
            "sample_phase": phase,
            "mode": "historical",
            "signal_id": signal_id,
            "closed": True,
            "eligible": True,
            "net_r": primary["net_r"],
            "stress_10bps_r": outcomes["10"]["net_r"],
            "stress_20bps_r": outcomes["20"]["net_r"],
            "stop_executed": primary["stop_executed"],
            "session_capture_complete": True,
            "rule_violations": [],
        }
        ledger_records.append(ledger_record)
        result_records.append(
            {
                "date": day,
                "symbol": selected["symbol"],
                "trigger_time_et": selected["trigger_time_et"],
                "entry_time_et": selected["entry_time_et"],
                "exit_time_et": primary["exit_time_et"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
    config = maturity.load_config(PORTFOLIO_CONFIG)
    metrics = maturity._robustness_metrics(
        ledger_records,
        float(config.raw["pilot_ready"]["minimum_bootstrap_confidence"]),
    )
    blockers = _phase_gate_blockers(metrics, phase)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "representative-validation-result",
        "dataset_id": DATASET_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": MECHANISM_FAMILY,
        "rules_hash": manifest["rules_hash"],
        "sample_phase": phase,
        "manifest_sha256": manifest["manifest_sha256"],
        "freeze_inspection_sha256": freeze["inspection_sha256"],
        "input_inspection_sha256": inspection["inspection_sha256"],
        "development_result_sha256": (
            _load_json(development_result_path)["result_sha256"]
            if development_result_path is not None
            else None
        ),
        "denominator": {
            "dates": len(dates),
            "candidate_symbol_sessions": sum(len(rows) for rows in candidates_by_date.values()),
            "closed_signals": len(result_records),
            "no_trade_dates": sum(record["eligible_signal"] is False for record in ledger_records if record["record_type"] == "session"),
            "session_records": len(dates),
            "signal_records": len(result_records),
            "rule_violations": 0,
        },
        "disposition_counts": dict(sorted(dispositions.items())),
        "robustness": _serializable_metrics(metrics),
        "phase_blockers": blockers,
        f"{phase}_passed": not blockers,
        "provider_requests": 0,
        "broker_actions": 0,
        "maturity_effect": "ELIGIBLE_AFTER_INDEPENDENT_RESULT_INSPECTION",
        "records": result_records,
        "ledger_records": ledger_records,
    }
    result["result_sha256"] = common._self_hash(result, "result_sha256")
    return result


def inspect_phase_result(
    manifest_path: Path,
    freeze_inspection_path: Path,
    input_inspection_path: Path,
    result_path: Path,
    *,
    phase: str,
    development_result_path: Path | None = None,
    development_result_inspection_path: Path | None = None,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise GapValidationError("phase result content hash is invalid")
    rebuilt = build_phase_result(
        manifest_path,
        freeze_inspection_path,
        input_inspection_path,
        phase=phase,
        development_result_path=development_result_path,
        development_result_inspection_path=development_result_inspection_path,
        store=source,
        require_published=False,
    )
    if recorded != rebuilt:
        raise GapValidationError("phase result does not independently rebuild")
    audit: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "representative-validation-result-inspection",
        "dataset_id": DATASET_ID,
        "strategy_id": STRATEGY_ID,
        "strategy_version": STRATEGY_VERSION,
        "rules_hash": recorded["rules_hash"],
        "sample_phase": phase,
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["input_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": recorded["denominator"]["closed_signals"],
        "phase_passed": recorded[f"{phase}_passed"],
        "phase_blockers": recorded["phase_blockers"],
        "ledger_records_rebuilt": len(recorded["ledger_records"]),
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    audit["inspection_sha256"] = common._self_hash(audit, "inspection_sha256")
    return audit


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--output", type=Path)
    inspect = subparsers.add_parser("inspect-freeze")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--output", type=Path)
    collect = subparsers.add_parser("collect")
    collect.add_argument("manifest", type=Path)
    collect.add_argument("freeze_inspection", type=Path)
    collect.add_argument("--phase", choices=PHASES, required=True)
    collect.add_argument("--development-result", type=Path)
    collect.add_argument("--development-result-inspection", type=Path)
    collect.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    inspect_inputs = subparsers.add_parser("inspect-inputs")
    inspect_inputs.add_argument("manifest", type=Path)
    inspect_inputs.add_argument("freeze_inspection", type=Path)
    inspect_inputs.add_argument("--phase", choices=PHASES, required=True)
    inspect_inputs.add_argument("--development-result", type=Path)
    inspect_inputs.add_argument("--development-result-inspection", type=Path)
    inspect_inputs.add_argument("--output", type=Path)
    evaluate = subparsers.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("freeze_inspection", type=Path)
    evaluate.add_argument("input_inspection", type=Path)
    evaluate.add_argument("--phase", choices=PHASES, required=True)
    evaluate.add_argument("--development-result", type=Path)
    evaluate.add_argument("--development-result-inspection", type=Path)
    evaluate.add_argument("--output", type=Path)
    inspect_result = subparsers.add_parser("inspect-result")
    inspect_result.add_argument("manifest", type=Path)
    inspect_result.add_argument("freeze_inspection", type=Path)
    inspect_result.add_argument("input_inspection", type=Path)
    inspect_result.add_argument("result", type=Path)
    inspect_result.add_argument("--phase", choices=PHASES, required=True)
    inspect_result.add_argument("--development-result", type=Path)
    inspect_result.add_argument("--development-result-inspection", type=Path)
    inspect_result.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            value, _ = build_freeze()
            if args.output:
                _published(value, args.output, "manifest_sha256")
        elif args.command == "inspect-freeze":
            value = inspect_freeze(args.manifest)
            if args.output:
                _published(value, args.output, "inspection_sha256")
        elif args.command == "collect":
            value = collect_phase(
                args.manifest,
                args.freeze_inspection,
                phase=args.phase,
                env_path=args.env_file,
                development_result_path=args.development_result,
                development_result_inspection_path=args.development_result_inspection,
            )
        elif args.command == "inspect-inputs":
            value = inspect_phase_inputs(
                args.manifest,
                args.freeze_inspection,
                phase=args.phase,
                development_result_path=args.development_result,
                development_result_inspection_path=args.development_result_inspection,
            )
            if args.output:
                _published(value, args.output, "inspection_sha256")
        elif args.command == "evaluate":
            value = build_phase_result(
                args.manifest,
                args.freeze_inspection,
                args.input_inspection,
                phase=args.phase,
                development_result_path=args.development_result,
                development_result_inspection_path=args.development_result_inspection,
            )
            if args.output:
                _published(value, args.output, "result_sha256")
        else:
            value = inspect_phase_result(
                args.manifest,
                args.freeze_inspection,
                args.input_inspection,
                args.result,
                phase=args.phase,
                development_result_path=args.development_result,
                development_result_inspection_path=args.development_result_inspection,
            )
            if args.output:
                _published(value, args.output, "inspection_sha256")
    except (GapValidationError, ScannerReplayError, OSError, ValueError) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True, allow_nan=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
