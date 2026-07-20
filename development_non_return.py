"""Freeze the disjoint tranche's source-positive pre-entry qualification.

This module is deliberately network free.  It selects the exact private pairs
whose already-inspected SEC source semantics are verified positive, freezes the
causal pre-entry acquisition graph, and independently rebuilds that zero-state.
It never reads a target return or a post-entry price path.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import development_catalyst_contract as source_contract
import development_catalyst_source_semantics as source_semantics
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from strategy_engine import load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-non-return-qualification-2026-07-20-v3"
SELECTED_PAIR_MANIFEST = source_contract.SOURCE_MANIFEST
SEMANTICS_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec_semantics_manifests"
    / (
        "dataset-development-sec-source-semantics-2026-07-19-v2-"
        "1fa8b20e14727f4de78344fbada0e7633a91f99fcc747a17bddfebd8f705d81c.json"
    )
)
SEMANTICS_RESULT = source_semantics.DEFAULT_PUBLIC_RESULT
STRATEGY_CONFIG = PROJECT_ROOT / "strategy_config.toml"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/non_return_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/non-return-contract-status.json"
)
PRIVATE_NAMESPACE = "_derived/development_non_return"
PRIVATE_CONTRACT_FILE = "frozen-positive-preentry-contract.json.gz"
EXPECTED_SOURCE_PAIRS = 1_906
EXPECTED_POSITIVE_PAIRS = 21
MINIMUM_SURVIVORS = 20
MINIMUM_RESERVE_BYTES = 20 * 1024**3
CALENDAR_QUERY_START = "2023-12-01"
CALENDAR_QUERY_END = "2025-11-30"


class DevelopmentNonReturnError(RuntimeError):
    """The source-positive pre-entry contract is unsafe or has drifted."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentNonReturnError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentNonReturnError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentNonReturnError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentNonReturnError(f"{path} must contain an object")
    return value


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise DevelopmentNonReturnError(
            f"public evidence path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / DATASET_ID


def _private_contract_path(store_root: Path) -> Path:
    return _private_root(store_root) / PRIVATE_CONTRACT_FILE


def _target_artifact_count(store_root: Path) -> int:
    root = _private_root(store_root)
    if not root.exists():
        return 0
    selection = _private_contract_path(store_root).resolve()
    return sum(
        1
        for path in root.rglob("*")
        if path.is_file() and path.resolve() != selection
    )


def build_positive_selection(
    source_private: Mapping[str, Any],
    pair_dispositions: Mapping[str, Any],
    *,
    expected_source_pairs: int = EXPECTED_SOURCE_PAIRS,
    expected_positive_pairs: int = EXPECTED_POSITIVE_PAIRS,
) -> dict[str, Any]:
    """Rejoin private pair-disposition hashes to the frozen scanner rows."""

    pairs = source_private.get("selected_pairs")
    if not isinstance(pairs, list) or len(pairs) != expected_source_pairs:
        raise DevelopmentNonReturnError("source selection pair count differs")
    if len(pair_dispositions) != expected_source_pairs:
        raise DevelopmentNonReturnError("source-semantics pair denominator differs")
    by_hash: dict[str, dict[str, Any]] = {}
    for value in pairs:
        if not isinstance(value, Mapping):
            raise DevelopmentNonReturnError("source selected pair is malformed")
        day = str(value.get("date") or "")
        instrument_id = str(value.get("instrument_id") or "")
        date.fromisoformat(day)
        if not instrument_id:
            raise DevelopmentNonReturnError("source selected pair identity is missing")
        key = source_semantics._sha256_json((day, instrument_id))
        if key in by_hash:
            raise DevelopmentNonReturnError("source selected pair hash repeats")
        by_hash[key] = dict(value)
    if set(by_hash) != set(pair_dispositions):
        raise DevelopmentNonReturnError(
            "source-semantics pair hashes do not match the scanner selection"
        )
    positive = [
        by_hash[key]
        for key, disposition in sorted(pair_dispositions.items())
        if disposition == "VERIFIED_POSITIVE_PRIMARY"
    ]
    if len(positive) != expected_positive_pairs:
        raise DevelopmentNonReturnError(
            f"verified-positive selection is not exactly {expected_positive_pairs}"
        )
    positive.sort(
        key=lambda row: (
            str(row["date"]),
            int(row["rank"]),
            str(row["instrument_id"]),
        )
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_selected_pair_count": len(pairs),
        "positive_pair_count": len(positive),
        "selection_reason": "VERIFIED_POSITIVE_PRIMARY",
        "positive_pairs": positive,
        "positive_pair_identity_sha256": _sha256_json(
            [
                {
                    "date": row["date"],
                    "instrument_id": row["instrument_id"],
                    "primary_exchange": row["primary_exchange"],
                    "rank": row["rank"],
                    "symbol": row["symbol"],
                }
                for row in positive
            ]
        ),
        "target_outcomes_observed_or_derived": False,
    }


def _strategy_contract(selection: Mapping[str, Any]) -> dict[str, Any]:
    config = load_config(STRATEGY_CONFIG)
    universe = config.raw["universe"]
    execution = config.raw["execution"]
    risk = config.raw["risk"]
    failures: list[str] = []
    for row in selection["positive_pairs"]:
        fields = row.get("scanner_fields")
        if not isinstance(fields, Mapping):
            raise DevelopmentNonReturnError("positive pair scanner fields are missing")
        checks = {
            "minimum_open_price": float(fields["open_price"])
            >= float(universe["minimum_open_price"]),
            "bullish_opening_candle": float(fields["opening_close"])
            > float(fields["open_price"]),
            "minimum_average_daily_volume_14": float(
                fields["average_daily_volume_14"]
            )
            >= float(universe["minimum_average_daily_volume_14"]),
            "minimum_daily_atr_14": float(fields["daily_atr_14"])
            >= float(universe["minimum_daily_atr_14"]),
            "minimum_opening_relative_volume": float(
                fields["opening_relative_volume"]
            )
            >= float(universe["minimum_opening_relative_volume"]),
        }
        if not all(checks.values()):
            failures.extend(name for name, passed in checks.items() if not passed)
    if failures:
        raise DevelopmentNonReturnError(
            f"source-positive selection fails frozen scanner gates: {sorted(set(failures))}"
        )
    return {
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "strategy_config_sha256": _sha256_file(STRATEGY_CONFIG),
        "coarse_scanner_gates_rechecked": True,
        "entry_start_et": str(config.raw["strategy"]["entry_start_et"]),
        "entry_cutoff_et": str(config.raw["strategy"]["entry_cutoff_et"]),
        "quote_snapshot_count": int(execution["quote_snapshot_count"]),
        "maximum_quote_age_seconds": float(
            execution["maximum_quote_age_seconds"]
        ),
        "maximum_median_spread_fraction": float(
            execution["maximum_median_spread_fraction"]
        ),
        "maximum_single_spread_fraction": float(
            execution["maximum_single_spread_fraction"]
        ),
        "maximum_entry_chase_fraction": float(
            execution["maximum_entry_chase_fraction"]
        ),
        "maximum_depth_participation_fraction": float(
            execution["maximum_depth_participation_fraction"]
        ),
        "maximum_recent_volume_participation_fraction": float(
            execution["maximum_recent_volume_participation_fraction"]
        ),
        "atr_stop_fraction": float(risk["atr_stop_fraction"]),
        "maximum_stop_fraction": float(risk["maximum_stop_fraction"]),
        "minimum_resistance_room_fraction": float(
            risk["minimum_resistance_room_fraction"]
        ),
        "minimum_reward_risk": float(risk["minimum_reward_risk"]),
    }


def build_request_graph(selection: Mapping[str, Any]) -> dict[str, Any]:
    """Build only causal inputs needed through the final decision snapshot."""

    pairs = selection["positive_pairs"]
    dates = sorted({str(row["date"]) for row in pairs})
    candidate_prefixes = []
    premarket = []
    history = []
    for row in pairs:
        identity = {
            "date": row["date"],
            "symbol": row["symbol"],
            "instrument_id": row["instrument_id"],
        }
        candidate_prefixes.append(
            {
                **identity,
                "provider": "alpaca",
                "feed": "sip",
                "adjustment": "raw",
                "bar_size": "1 min",
                "use_rth": True,
                "start_local": "TARGET_DATE_09:30_ET",
                "end_local": "TARGET_DATE_10:30_ET",
                "end_exclusive": True,
            }
        )
        premarket.append(
            {
                **identity,
                "provider": "alpaca",
                "feed": "sip",
                "adjustment": "raw",
                "bar_size": "1 min",
                "use_rth": False,
                "start_local": "TARGET_DATE_04:00_ET",
                "end_local": "TARGET_DATE_09:30_ET_EXCLUSIVE",
            }
        )
        history.append(
            {
                **identity,
                "provider": "alpaca",
                "feed": "sip",
                "adjustment": "raw",
                "bar_size": "15 mins",
                "use_rth": True,
                "session_count": 252,
                "end_local": "TARGET_DATE_09:30_ET_EXCLUSIVE",
                "calendar_source": "FROZEN_ALPACA_CALENDAR_QUERY",
            }
        )
    benchmarks = [
        {
            "date": day,
            "symbol": symbol,
            "provider": "alpaca",
            "feed": "sip",
            "adjustment": "raw",
            "bar_size": "1 min",
            "use_rth": True,
            "start_local": "TARGET_DATE_09:30_ET",
            "end_local": "TARGET_DATE_10:30_ET",
            "end_exclusive": True,
        }
        for day in dates
        for symbol in ("QQQ", "SPY")
    ]
    halts = [
        {
            "date": day,
            "provider": "Nasdaq Trader official historical halt RPC",
            "scope": "complete trading date",
        }
        for day in dates
    ]
    graph = {
        "schema_version": 1,
        "calendar_query": {
            "provider": "Alpaca Market Calendar API",
            "start": CALENDAR_QUERY_START,
            "end": CALENDAR_QUERY_END,
            "purpose": "derive exact prior-252-session windows only",
        },
        "candidate_bar_prefixes": candidate_prefixes,
        "benchmark_bar_prefixes": benchmarks,
        "candidate_premarket_prefixes": premarket,
        "candidate_history_prefixes": history,
        "official_halt_dates": halts,
        "conditional_clean_cross_search": {
            "when": "an aggregate 1-minute high is above the frozen opening high from 09:35 through before 10:30",
            "provider": "alpaca",
            "feed": "sip",
            "window_seconds": 1,
            "order": "chronological aggregate crossing minutes then chronological one-second windows",
            "stop": "FIRST_CONDITION_VALID_CONTINUOUS_REGULAR_SALE_CROSS_OR_10:30_ET",
            "later_windows_after_clean_cross_allowed": False,
            "purpose": "find the first condition-valid continuous regular-sale cross without reading later tape",
        },
        "conditional_decision_trade_prefix": {
            "when": "a condition-valid continuous regular-sale cross exists",
            "provider": "alpaca",
            "feed": "sip",
            "start": "TARGET_DATE_09:30_ET",
            "end": "CLEAN_CROSS_PLUS_10_SECONDS_EXCLUSIVE",
            "purpose": "condition-aware session VWAP at the final decision snapshot",
        },
        "conditional_quote_window": {
            "when": "a condition-valid continuous regular-sale cross exists",
            "provider": "alpaca",
            "feed": "sip",
            "start": "CLEAN_CROSS_MINUS_1_SECOND",
            "end": "CLEAN_CROSS_PLUS_10_SECONDS_INCLUSIVE",
            "snapshot_offsets_seconds": [0, 5, 10],
        },
        "provider_policy": {
            "local_exact_alpaca_cache_first": True,
            "network_provider": "alpaca",
            "whole_provider_fidelity": True,
            "provider_switching_allowed": False,
            "reason": (
                "The frozen condition-aware SIP trade and quote semantics are "
                "Alpaca-specific; IBKR or Massive rows cannot be spliced into them."
            ),
        },
        "target_outcomes_observed_or_derived": False,
    }
    return {
        "graph": graph,
        "counts": {
            "positive_pairs": len(pairs),
            "positive_dates": len(dates),
            "candidate_bar_prefixes": len(candidate_prefixes),
            "benchmark_bar_prefixes": len(benchmarks),
            "candidate_premarket_prefixes": len(premarket),
            "candidate_history_prefixes": len(history),
            "official_halt_dates": len(halts),
        },
        "positive_date_identity_sha256": _sha256_json(dates),
        "request_graph_sha256": _sha256_json(graph),
    }


def _load_sources(env_path: Path) -> tuple[HistoricalStoreConfig, dict[str, Any], dict[str, Any]]:
    config = HistoricalStoreConfig.from_env(env_path)
    selected_manifest = load_frozen_dataset_contract(SELECTED_PAIR_MANIFEST)
    semantics_manifest = load_frozen_dataset_contract(SEMANTICS_MANIFEST)
    semantics_result = _read_object(SEMANTICS_RESULT)
    if (
        selected_manifest.get("requested_dates")
        != semantics_manifest.get("requested_dates")
        or int(
            selected_manifest.get("selection_contract", {}).get(
                "selected_pair_count", -1
            )
        )
        != int(
            semantics_manifest.get("selection_contract", {}).get(
                "selected_pairs", -2
            )
        )
    ):
        raise DevelopmentNonReturnError("semantics selection lineage differs")
    if (
        semantics_result.get("manifest_sha256")
        != semantics_manifest.get("manifest_sha256")
        or semantics_result.get("status") != "READY"
        or semantics_result.get("inspected") is not True
        or semantics_result.get("valid") is not True
        or semantics_result.get("verified_positive_pairs") != EXPECTED_POSITIVE_PAIRS
        or semantics_result.get("target_outcomes_observed_or_derived") is not False
    ):
        raise DevelopmentNonReturnError("source semantics is not inspected READY")
    source_private_path = source_contract._selection_private_path(
        config.root, str(selected_manifest["dataset_id"])
    )
    source_private = _read_gzip(source_private_path)
    selected_hash = selected_manifest.get("selection_contract", {}).get(
        "private_selection_content_sha256"
    )
    if source_contract._sha256_json(source_private) != selected_hash:
        raise DevelopmentNonReturnError("private source selection drifted")
    reviewed_path = source_semantics._reviewed_path(config.root)
    reviewed = _read_gzip(reviewed_path)
    if (
        reviewed.get("manifest_sha256") != semantics_manifest["manifest_sha256"]
        or reviewed.get("status") != "REVIEW_COMPLETE"
        or reviewed.get("verified_positive_pairs") != EXPECTED_POSITIVE_PAIRS
        or reviewed.get("target_outcomes_observed_or_derived") is not False
        or _sha256_file(reviewed_path) != semantics_result.get("private_result_sha256")
    ):
        raise DevelopmentNonReturnError("private source review is incomplete or stale")
    selection = build_positive_selection(
        source_private, reviewed.get("pair_dispositions", {})
    )
    bindings = {
        "selected_pair_manifest": {
            "path": _repo_path(SELECTED_PAIR_MANIFEST),
            "sha256": _sha256_file(SELECTED_PAIR_MANIFEST),
            "manifest_sha256": selected_manifest["manifest_sha256"],
        },
        "source_semantics_manifest": {
            "path": _repo_path(SEMANTICS_MANIFEST),
            "sha256": _sha256_file(SEMANTICS_MANIFEST),
            "manifest_sha256": semantics_manifest["manifest_sha256"],
        },
        "source_semantics_result": {
            "path": _repo_path(SEMANTICS_RESULT),
            "sha256": _sha256_file(SEMANTICS_RESULT),
        },
        "private_source_selection_sha256": _sha256_file(source_private_path),
        "private_source_review_sha256": _sha256_file(reviewed_path),
    }
    return config, selection, bindings


def _implementation_contract() -> dict[str, Any]:
    files = (
        "development_non_return.py",
        "preentry_structure.py",
        "sip_trade_conditions.py",
        "nasdaq_halts.py",
        "strategy_engine.py",
    )
    return {
        "files": {
            name: {"path": name, "sha256": _sha256_file(PROJECT_ROOT / name)}
            for name in files
        },
        "independent_rebuild_uses_frozen_sources": True,
    }


def _expected_contract(
    *,
    selected_manifest: Mapping[str, Any],
    selection: Mapping[str, Any],
    bindings: Mapping[str, Any],
    request: Mapping[str, Any],
    free_bytes: int,
) -> dict[str, Any]:
    strategy = _strategy_contract(selection)
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        # Preserve the complete public source-date surface. Positive identities and
        # their date subset stay in the private hash-bound contract.
        "requested_dates": list(selected_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SELECTED_PAIR_MANIFEST),
                _repo_path(SEMANTICS_MANIFEST),
                _repo_path(SEMANTICS_RESULT),
                "DEVELOPMENT_NON_RETURN.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            **dict(bindings),
            "source_selected_pair_count": selection["source_selected_pair_count"],
            "verified_positive_pair_count": selection["positive_pair_count"],
            "private_positive_selection_sha256": _sha256_json(selection),
            "positive_pair_identity_sha256": selection[
                "positive_pair_identity_sha256"
            ],
            "positive_date_count": request["counts"]["positive_dates"],
            "positive_date_identity_sha256": request[
                "positive_date_identity_sha256"
            ],
            "exact_symbols_dates_and_rows_public": False,
        },
        "strategy_contract": strategy,
        "acquisition_contract": {
            "request_counts": dict(request["counts"]),
            "private_request_graph_sha256": request["request_graph_sha256"],
            "calendar_query": {
                "start": CALENDAR_QUERY_START,
                "end": CALENDAR_QUERY_END,
                "provider": "Alpaca Market Calendar API",
            },
            "candidate_detail_only": True,
            "full_universe_detail_forbidden": True,
            "local_compatible_cache_first": True,
            "whole_provider_fidelity": True,
            "provider_switching_allowed": False,
            "substitutions_allowed": False,
            "post_entry_requests_allowed": False,
            "outcome_fields_allowed": False,
            "conditional_requests_are_derived_only_from_pre_entry_inputs": True,
            "provider_rows_after_final_decision_snapshot_allowed": False,
        },
        "evaluation_contract": {
            "unchanged_strategy_required": True,
            "minimum_complete_non_return_survivors": MINIMUM_SURVIVORS,
            "missing_inputs_default_favorable": False,
            "no_cross_before_cutoff_is_terminal_reject": True,
            "condition_aware_continuous_regular_sale_cross_required": True,
            "chronological_one_second_tape_search_stops_at_first_clean_cross": True,
            "intraminute_vwap_uses_condition_eligible_trade_prefix": True,
            "three_fresh_uncrossed_snapshots_required": True,
            "spread_chase_liquidity_halt_market_structure_and_score_gates_required": True,
            "broker_specific_historical_tradability": "PROSPECTIVE_ONLY_UNRECONSTRUCTABLE",
            "outcome_contract_permitted_at_this_stage": False,
        },
        "implementation_contract": _implementation_contract(),
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_RESERVE_BYTES,
            "observed_free_bytes_at_freeze": free_bytes,
        },
        "privacy_contract": {
            "private_contract_location": (
                f"LOCAL_HISTORICAL_DATA_ROOT/{PRIVATE_NAMESPACE}/{DATASET_ID}/"
                f"{PRIVATE_CONTRACT_FILE}"
            ),
            "symbols_dates_rows_requests_and_raw_inputs_public": False,
            "public_aggregates_and_hashes_only": True,
        },
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "return_fields_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "minimum_complete_non_return_survivors_before_outcomes": MINIMUM_SURVIVORS,
        },
    }


def _matching_manifest(
    output_root: Path, expected: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    matches = sorted(output_root.glob(f"{DATASET_ID}-*.json"))
    if len(matches) > 1:
        raise DevelopmentNonReturnError("non-return contract has multiple manifests")
    if not matches:
        return None
    manifest = load_frozen_dataset_contract(matches[0])
    for key, value in expected.items():
        if key == "capacity_contract":
            observed = manifest.get(key, {})
            if observed.get("minimum_free_bytes") != value["minimum_free_bytes"]:
                raise DevelopmentNonReturnError("existing capacity contract differs")
            continue
        if manifest.get(key) != value:
            raise DevelopmentNonReturnError(f"existing non-return {key} differs")
    return matches[0], manifest


def freeze(
    *, env_path: Path, output_root: Path, public_status_path: Path
) -> tuple[Path, dict[str, Any]]:
    config, selection, bindings = _load_sources(env_path)
    if _target_artifact_count(config.root):
        raise DevelopmentNonReturnError(
            "pre-entry target artifacts exist before the contract is frozen"
        )
    free_bytes = shutil.disk_usage(config.root).free
    if free_bytes < MINIMUM_RESERVE_BYTES:
        raise DevelopmentNonReturnError("historical store is below the 20-GiB reserve")
    request = build_request_graph(selection)
    selected_manifest = load_frozen_dataset_contract(SELECTED_PAIR_MANIFEST)
    expected = _expected_contract(
        selected_manifest=selected_manifest,
        selection=selection,
        bindings=bindings,
        request=request,
        free_bytes=free_bytes,
    )
    existing = _matching_manifest(output_root, expected)
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "selection": selection,
        "request_graph": request["graph"],
        "request_counts": request["counts"],
        "request_graph_sha256": request["request_graph_sha256"],
        "target_artifacts_at_freeze": 0,
        "target_outcomes_observed_or_derived": False,
    }
    private_path = _private_contract_path(config.root)
    if private_path.exists():
        if _sha256_json(_read_gzip(private_path)) != _sha256_json(private):
            raise DevelopmentNonReturnError("private pre-entry contract changed")
    else:
        _write_gzip(private_path, private)
    if existing is None:
        path, manifest = freeze_dataset_contract(
            {**expected, "registered_at": datetime.now(UTC).isoformat()}, output_root
        )
    else:
        path, manifest = existing
    status = _public_status(
        manifest=manifest,
        selection=selection,
        request=request,
        private_path=private_path,
        status="FROZEN_WAITING_INSPECTION",
        inspected=False,
    )
    _write_json(public_status_path, status)
    return path, manifest


def _public_status(
    *,
    manifest: Mapping[str, Any],
    selection: Mapping[str, Any],
    request: Mapping[str, Any],
    private_path: Path,
    status: str,
    inspected: bool,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": status,
        "inspected": inspected,
        "source_selected_pairs": selection["source_selected_pair_count"],
        "verified_positive_pairs": selection["positive_pair_count"],
        "minimum_non_return_survivors_before_outcomes": MINIMUM_SURVIVORS,
        "request_counts": dict(request["counts"]),
        "private_positive_selection_sha256": _sha256_json(selection),
        "positive_pair_identity_sha256": selection[
            "positive_pair_identity_sha256"
        ],
        "positive_date_identity_sha256": request["positive_date_identity_sha256"],
        "private_request_graph_sha256": request["request_graph_sha256"],
        "private_contract_file_sha256": _sha256_file(private_path),
        "target_artifacts": 0,
        "post_entry_requests_allowed": False,
        "outcome_contract_permitted": False,
        "symbols_dates_rows_requests_and_raw_inputs_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def inspect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentNonReturnError("unexpected non-return dataset")
    config, selection, bindings = _load_sources(env_path)
    request = build_request_graph(selection)
    selected_manifest = load_frozen_dataset_contract(SELECTED_PAIR_MANIFEST)
    expected = _expected_contract(
        selected_manifest=selected_manifest,
        selection=selection,
        bindings=bindings,
        request=request,
        free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DevelopmentNonReturnError(f"frozen non-return {key} drifted")
    private_path = _private_contract_path(config.root)
    private = _read_gzip(private_path)
    rebuilt = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "selection": selection,
        "request_graph": request["graph"],
        "request_counts": request["counts"],
        "request_graph_sha256": request["request_graph_sha256"],
        "target_artifacts_at_freeze": 0,
        "target_outcomes_observed_or_derived": False,
    }
    if private != rebuilt:
        raise DevelopmentNonReturnError("private pre-entry contract differs")
    if _target_artifact_count(config.root):
        raise DevelopmentNonReturnError(
            "pre-entry target artifacts appeared before manifest inspection"
        )
    result = _public_status(
        manifest=manifest,
        selection=selection,
        request=request,
        private_path=private_path,
        status="FROZEN_READY",
        inspected=True,
    )
    result["inspection"] = {
        "source_selection_rebuilt": True,
        "positive_pair_hash_join_rebuilt": True,
        "coarse_scanner_gates_rechecked": True,
        "request_graph_rebuilt": True,
        "implementation_hashes_rebuilt": True,
        "strategy_version_and_rules_hash_rebuilt": True,
        "private_public_boundary_rechecked": True,
        "zero_target_artifacts_rechecked": True,
        "valid": True,
    }
    _write_json(public_status_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "inspect"))
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument(
        "--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze(
                env_path=args.env,
                output_root=args.output_root,
                public_status_path=args.public_status,
            )
            result: Any = {
                "path": _repo_path(path),
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "verified_positive_pairs": manifest["selection_contract"][
                    "verified_positive_pair_count"
                ],
            }
        elif args.manifest is None:
            raise DevelopmentNonReturnError("inspect requires a manifest")
        else:
            result = inspect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.public_status,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentNonReturnError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"status": "error", "error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
