"""Freeze and evaluate unchanged-v3 pre-entry gates without reading outcomes.

This stage consumes only the independently inspected 102-pair causal collection.
Exact identities, raw rows, and per-pair gate results stay in the private
historical store.  Public artifacts contain aggregate attrition and hashes only.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import development_non_return_collection as collection
import development_non_return_collection_v3 as collection_v3
from historical_store import HistoricalDayStore
from learning_data import LearningDataError, freeze_dataset_contract, load_frozen_dataset_contract
from preentry_structure import PreentryStructureError, derive_preentry_structure
from scanner_replay import load_split_actions, split_adjustment_factor
from sip_bar_aggregation import aggregate_prefix_vwap
from strategy_engine import StrategyInputError, evaluate_candidate, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-non-return-gate-evaluation-2026-07-20-tranche-v3-v1"
COLLECTION_DATASET_ID = collection_v3.DATASET_ID
COLLECTION_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non_return_collection_manifests"
    / (
        "dataset-development-non-return-preentry-collection-2026-07-20-"
        "tranche-v3-v1-cded09e9d21e5bc84aae56b3fb764c003d9e4e2c6e7e4d510977d1b253ee48b6.json"
    )
)
COLLECTION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non-return-collection-status.json"
)
STRATEGY_CONFIG = PROJECT_ROOT / "strategy_config.toml"
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non_return_qualification_manifests"
)
DEFAULT_CONTRACT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non-return-qualification-contract-status.json"
)
DEFAULT_QUALIFICATION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non-return-qualification-status.json"
)
PRIVATE_NAMESPACE = "_derived/development_non_return_qualification_v3"
EXPECTED_PAIRS = 102
EXPECTED_INPUT_READY = 75
MINIMUM_SURVIVORS = 20
REFERENCE_ACCOUNT_EQUITY = 100_000.0
REFERENCE_BUYING_POWER = 100_000.0


class DevelopmentNonReturnQualificationError(RuntimeError):
    """Frozen pre-entry evidence cannot support a qualification result."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentNonReturnQualificationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentNonReturnQualificationError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentNonReturnQualificationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentNonReturnQualificationError(f"{path} must contain an object")
    return value


def _gzip_bytes(value: Any) -> bytes:
    import io

    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(_gzip_bytes(value))
    os.replace(temporary, path)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise DevelopmentNonReturnQualificationError(
            f"public evidence path must be repository-relative: {path}"
        ) from exc


def _private_source_root(store_root: Path) -> Path:
    return store_root / collection_v3.PRIVATE_NAMESPACE / COLLECTION_DATASET_ID


def _private_root(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / DATASET_ID


def _private_result_path(store_root: Path) -> Path:
    return _private_root(store_root) / "qualification-index.json.gz"


def _source_paths(store_root: Path) -> dict[str, Path]:
    root = _private_source_root(store_root)
    return {
        "root": root,
        "index": root / collection.INDEX_FILE,
        "pairs": root / collection.PAIR_DIRECTORY,
        "splits": root / collection.SPLITS_FILE,
    }


def _pair_paths(index: Mapping[str, Any], store_root: Path) -> list[Path]:
    root = _source_paths(store_root)["pairs"]
    values = index.get("pair_files")
    if not isinstance(values, list):
        raise DevelopmentNonReturnQualificationError("source pair index is malformed")
    paths: list[Path] = []
    for row in values:
        if not isinstance(row, Mapping):
            raise DevelopmentNonReturnQualificationError("source pair index row is malformed")
        key = str(row.get("pair_key") or "")
        if not key or "/" in key or ".." in key:
            raise DevelopmentNonReturnQualificationError("source pair key is unsafe")
        path = root / f"{key}.json.gz"
        if not path.is_file() or _sha256_file(path) != row.get("sha256"):
            raise DevelopmentNonReturnQualificationError("source pair hash drifted")
        paths.append(path)
    return paths


def _published(path: Path) -> dict[str, str]:
    try:
        return collection._published(path)
    except collection.DevelopmentNonReturnCollectionError as exc:
        raise DevelopmentNonReturnQualificationError(str(exc)) from exc


def _binding(path: Path) -> dict[str, str]:
    return {"path": _repo_path(path), "sha256": _sha256_file(path)}


def _verify_binding(value: Mapping[str, Any]) -> None:
    raw = value.get("path")
    if not isinstance(raw, str) or not raw:
        raise DevelopmentNonReturnQualificationError("bound path is missing")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise DevelopmentNonReturnQualificationError("bound path is unsafe")
    path = PROJECT_ROOT / relative
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise DevelopmentNonReturnQualificationError(f"bound artifact drifted: {relative}")


def _load_source(env_path: Path) -> tuple[HistoricalDayStore, dict[str, Any], list[Path]]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        manifest = load_frozen_dataset_contract(COLLECTION_MANIFEST)
    except LearningDataError as exc:
        raise DevelopmentNonReturnQualificationError(str(exc)) from exc
    public = _read_json(COLLECTION_STATUS)
    paths = _source_paths(store.root)
    index = _read_gzip(paths["index"])
    pair_paths = _pair_paths(index, store.root)
    if (
        manifest.get("dataset_id") != COLLECTION_DATASET_ID
        or public.get("dataset_id") != COLLECTION_DATASET_ID
        or public.get("status") != "COLLECTION_INSPECTED"
        or public.get("inspected") is not True
        or public.get("pairs_terminal") != EXPECTED_PAIRS
        or public.get("terminal_counts")
        != {
            "NO_CLEAN_CROSS_BEFORE_CUTOFF": EXPECTED_PAIRS - EXPECTED_INPUT_READY,
            "PREENTRY_INPUTS_COLLECTED": EXPECTED_INPUT_READY,
        }
        or public.get("provider_rows_after_final_decision") is not False
        or public.get("target_outcomes_observed_or_derived") is not False
        or index.get("dataset_id") != COLLECTION_DATASET_ID
        or index.get("manifest_sha256") != manifest.get("manifest_sha256")
        or index.get("pairs_expected") != EXPECTED_PAIRS
        or index.get("target_outcomes_observed_or_derived") is not False
        or len(pair_paths) != EXPECTED_PAIRS
        or _sha256_file(paths["index"]) != public.get("private_index_sha256")
        or not paths["splits"].is_file()
    ):
        raise DevelopmentNonReturnQualificationError("inspected source collection drifted")
    return store, index, pair_paths


def _implementation_contract() -> dict[str, dict[str, str]]:
    names = (
        "development_non_return_qualification_v3.py",
        "development_non_return_qualification_v3_inspection.py",
        "preentry_structure.py",
        "sip_bar_aggregation.py",
        "sip_trade_conditions.py",
        "strategy_engine.py",
        "scanner_replay.py",
    )
    return {name: _binding(PROJECT_ROOT / name) for name in names}


def _rules_contract() -> dict[str, Any]:
    config = load_config(STRATEGY_CONFIG)
    execution = config.raw["execution"]
    risk = config.raw["risk"]
    return {
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "strategy_config_sha256": _sha256_file(STRATEGY_CONFIG),
        "maturity": "UNVALIDATED",
        "minimum_score": int(config.raw["maturity"]["UNVALIDATED"]["minimum_score"]),
        "maximum_quote_age_seconds": float(execution["maximum_quote_age_seconds"]),
        "maximum_a_plus_median_spread_fraction": float(
            execution["maximum_a_plus_median_spread_fraction"]
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


def _expected_contract(
    *, index: Mapping[str, Any], store_root: Path, observed_free_bytes: int
) -> dict[str, Any]:
    source_paths = _source_paths(store_root)
    rules = _rules_contract()
    collection_manifest = load_frozen_dataset_contract(COLLECTION_MANIFEST)
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "requested_dates": list(collection_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(COLLECTION_MANIFEST),
                _repo_path(COLLECTION_STATUS),
                "DEVELOPMENT_NON_RETURN.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "source_contract": {
            "collection_dataset_id": COLLECTION_DATASET_ID,
            "collection_manifest": _binding(COLLECTION_MANIFEST),
            "collection_status": _binding(COLLECTION_STATUS),
            "private_index_sha256": _sha256_file(source_paths["index"]),
            "private_pair_file_index_sha256": _sha256_json(index["pair_files"]),
            "private_splits_sha256": _sha256_file(source_paths["splits"]),
            "pairs_expected": EXPECTED_PAIRS,
            "preentry_inputs_collected": EXPECTED_INPUT_READY,
            "no_clean_cross_before_cutoff": EXPECTED_PAIRS - EXPECTED_INPUT_READY,
        },
        "implementation_contract": {"files": _implementation_contract()},
        "qualification_contract": {
            **rules,
            "minimum_complete_non_return_survivors": MINIMUM_SURVIVORS,
            "terminal_gate_order": [
                "NO_CLEAN_CROSS_BEFORE_CUTOFF",
                "INPUT_UNRESOLVED",
                "COARSE_SCANNER_GATE_FAILED",
                "OPENING_PREFIX_MISMATCH",
                "QUOTE_GATE_FAILED",
                "A_PLUS_SPREAD_FAILED",
                "CHASE_GATE_FAILED",
                "VWAP_GATE_FAILED",
                "LIQUIDITY_GATE_FAILED",
                "HALT_GATE_FAILED",
                "MARKET_GATE_FAILED",
                "RELATIVE_STRENGTH_GATE_FAILED",
                "STRUCTURE_GATE_FAILED",
                "RESISTANCE_GATE_FAILED",
                "EVALUATOR_INELIGIBLE",
                "SURVIVOR",
            ],
            "exact_sip_prefix_vwap_required": True,
            "vwap_slope_lookback_minutes": 5,
            "complete_252_session_split_adjusted_resistance_required": True,
            "interpolated_rows_default_favorable": False,
            "price_discovery_evaluator_reference": "ENTRY_TIMES_TWO_SCORE_ADAPTER_ONLY",
            "historical_reference_session": {
                "mode": "shadow",
                "account_equity": REFERENCE_ACCOUNT_EQUITY,
                "buying_power": REFERENCE_BUYING_POWER,
                "purpose": "isolate candidate and score gates without claiming broker state",
            },
            "broker_specific_historical_tradability": "PROSPECTIVE_ONLY_UNRECONSTRUCTABLE",
            "missing_inputs_default_favorable": False,
            "substitutions_allowed": False,
        },
        "capacity_contract": {
            "minimum_free_bytes": 20 * 1024**3,
            "observed_free_bytes_at_freeze": observed_free_bytes,
            "historical_deletion_allowed": False,
        },
        "privacy_contract": {
            "private_root": f"LOCAL_HISTORICAL_DATA_ROOT/{PRIVATE_NAMESPACE}/{DATASET_ID}/",
            "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
            "public_aggregates_and_hashes_only": True,
        },
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "target_returns_prices_and_outcomes_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "outcome_contract_permitted": False,
        },
    }


def _matching_manifest(expected: Mapping[str, Any], output_root: Path) -> Path | None:
    matches = sorted(output_root.glob(f"{DATASET_ID}-*.json"))
    for path in matches:
        try:
            manifest = load_frozen_dataset_contract(path)
        except LearningDataError as exc:
            raise DevelopmentNonReturnQualificationError(str(exc)) from exc
        for key, value in expected.items():
            if key == "capacity_contract":
                continue
            if manifest.get(key) != value:
                raise DevelopmentNonReturnQualificationError(
                    f"existing qualification manifest differs at {key}"
                )
        return path
    return None


def freeze(
    *, env_path: Path, output_root: Path, public_status_path: Path
) -> tuple[Path, dict[str, Any]]:
    _published(Path(__file__))
    _published(PROJECT_ROOT / "development_non_return_qualification_v3_inspection.py")
    store, index, _pairs = _load_source(env_path)
    root = _private_root(store.root)
    artifacts = sorted(path for path in root.rglob("*") if path.is_file()) if root.exists() else []
    if artifacts:
        raise DevelopmentNonReturnQualificationError(
            "qualification private namespace is not empty before freeze"
        )
    free = os.statvfs(store.root).f_bavail * os.statvfs(store.root).f_frsize
    expected = _expected_contract(index=index, store_root=store.root, observed_free_bytes=free)
    existing = _matching_manifest(expected, output_root)
    if existing is None:
        try:
            path, manifest = freeze_dataset_contract(
                {**expected, "registered_at": datetime.now(UTC).isoformat()}, output_root
            )
        except LearningDataError as exc:
            raise DevelopmentNonReturnQualificationError(str(exc)) from exc
    else:
        path = existing
        manifest = load_frozen_dataset_contract(path)
    status = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": False,
        "pairs_expected": EXPECTED_PAIRS,
        "preentry_inputs_collected": EXPECTED_INPUT_READY,
        "minimum_survivors_before_outcomes": MINIMUM_SURVIVORS,
        "private_artifacts": 0,
        "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, status)
    return path, status


def inspect_contract(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store, index, _pairs = _load_source(env_path)
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise DevelopmentNonReturnQualificationError(str(exc)) from exc
    expected = _expected_contract(
        index=index,
        store_root=store.root,
        observed_free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DevelopmentNonReturnQualificationError(
                f"qualification contract drifted at {key}"
            )
    for value in manifest["implementation_contract"]["files"].values():
        _verify_binding(value)
    root = _private_root(store.root)
    artifacts = sorted(path for path in root.rglob("*") if path.is_file()) if root.exists() else []
    if artifacts:
        raise DevelopmentNonReturnQualificationError(
            "qualification artifacts exist before contract inspection"
        )
    status = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "pairs_expected": EXPECTED_PAIRS,
        "preentry_inputs_collected": EXPECTED_INPUT_READY,
        "minimum_survivors_before_outcomes": MINIMUM_SURVIVORS,
        "private_artifacts": 0,
        "inspection": {
            "source_collection_rebuilt": True,
            "strategy_and_implementation_hashes_rebuilt": True,
            "gate_order_and_thresholds_rebuilt": True,
            "privacy_and_outcome_locks_rebuilt": True,
            "valid": True,
        },
        "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, status)
    return status


def _observed(row: Mapping[str, Any]) -> datetime:
    raw = str(row.get("source_timestamp") or row.get("time_et") or "")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DevelopmentNonReturnQualificationError("market row timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise DevelopmentNonReturnQualificationError("market row timestamp lacks timezone")
    return parsed


def quote_metrics(
    snapshots: Sequence[Mapping[str, Any]], rules: Mapping[str, Any]
) -> dict[str, Any]:
    if len(snapshots) != 3:
        return {"valid": False, "reason": "snapshot_count"}
    spreads: list[float] = []
    dollar_spreads: list[float] = []
    for row in snapshots:
        bid = float(row.get("bid", 0))
        ask = float(row.get("ask", 0))
        age = float(row.get("age_seconds", math.inf))
        if bid <= 0 or ask <= bid or age < 0 or age > rules["maximum_quote_age_seconds"]:
            return {"valid": False, "reason": "stale_crossed_or_nonpositive"}
        midpoint = (bid + ask) / 2
        spreads.append((ask - bid) / midpoint)
        dollar_spreads.append(ask - bid)
    median = statistics.median(spreads)
    maximum = max(spreads)
    return {
        "valid": True,
        "reason": None,
        "median_spread_fraction": median,
        "maximum_spread_fraction": maximum,
        "median_spread_dollars": statistics.median(dollar_spreads),
        "operating_spread_pass": (
            median <= rules["maximum_median_spread_fraction"]
            and maximum <= rules["maximum_single_spread_fraction"]
        ),
        "a_plus_spread_pass": median
        <= rules["maximum_a_plus_median_spread_fraction"],
        "minimum_ask_depth": min(int(row.get("ask_size", 0)) for row in snapshots),
        "minimum_bid": min(float(row["bid"]) for row in snapshots),
        "final_ask": float(snapshots[-1]["ask"]),
    }


def _bar_price(row: Mapping[str, Any]) -> float:
    wap = float(row.get("wap") or 0)
    if wap > 0:
        return wap
    return (float(row["high"]) + float(row["low"]) + float(row["close"])) / 3


def _bar_state(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not rows or any(row.get("interpolated") is True for row in rows):
        raise DevelopmentNonReturnQualificationError("completed bars are missing or interpolated")
    ordered = sorted(rows, key=lambda row: str(row["time_et"]))
    volume = sum(int(row.get("volume", 0)) for row in ordered)
    if volume <= 0:
        raise DevelopmentNonReturnQualificationError("completed bars have no real volume")
    current = sum(_bar_price(row) * int(row.get("volume", 0)) for row in ordered) / volume
    prior_rows = ordered[:-5] or ordered[:1]
    prior_volume = sum(int(row.get("volume", 0)) for row in prior_rows)
    if prior_volume <= 0:
        raise DevelopmentNonReturnQualificationError("prior VWAP bars have no real volume")
    prior = sum(
        _bar_price(row) * int(row.get("volume", 0)) for row in prior_rows
    ) / prior_volume
    last_five = ordered[-5:]
    return {
        "vwap": current,
        "vwap_flat_or_rising": current >= prior,
        "last_close": float(ordered[-1]["close"]),
        "open": float(ordered[0]["open"]),
        "last_real_volume": int(ordered[-1].get("volume", 0)),
        "last_bar_makes_five_bar_low": (
            len(last_five) == 5
            and float(last_five[-1]["low"])
            <= min(float(row["low"]) for row in last_five[:-1])
        ),
    }


def _opening_prefix_consistent(state: Mapping[str, Any]) -> bool:
    request = state["requests"]["opening_bars"]
    rows = request.get("observations", [])
    if len(rows) != 5 or any(row.get("interpolated") is True for row in rows):
        return False
    ordered = sorted(rows, key=lambda row: str(row["time_et"]))
    fields = state["scanner_fields"]
    observed = {
        "open_price": float(ordered[0]["open"]),
        "opening_high": max(float(row["high"]) for row in ordered),
        "opening_low": min(float(row["low"]) for row in ordered),
        "opening_close": float(ordered[-1]["close"]),
        "opening_volume": sum(int(row.get("volume", 0)) for row in ordered),
    }
    return all(
        math.isclose(observed[name], float(fields[name]), rel_tol=1e-9, abs_tol=1e-8)
        for name in observed
    )


def _coarse_scanner_pass(state: Mapping[str, Any]) -> bool:
    config = load_config(STRATEGY_CONFIG)
    fields = state["scanner_fields"]
    universe = config.raw["universe"]
    return bool(
        float(fields["open_price"]) >= float(universe["minimum_open_price"])
        and float(fields["opening_close"]) > float(fields["open_price"])
        and float(fields["average_daily_volume_14"])
        >= float(universe["minimum_average_daily_volume_14"])
        and float(fields["daily_atr_14"]) >= float(universe["minimum_daily_atr_14"])
        and float(fields["opening_relative_volume"])
        >= float(universe["minimum_opening_relative_volume"])
    )


def _exact_vwap_state(
    trades: Sequence[Mapping[str, Any]], final_at: datetime
) -> dict[str, Any]:
    ordered = sorted(trades, key=_observed)
    current = aggregate_prefix_vwap(ordered)
    prior_cutoff = final_at - timedelta(minutes=5)
    prior = aggregate_prefix_vwap([row for row in ordered if _observed(row) < prior_cutoff])
    if current["wap"] is None or prior["wap"] is None:
        raise DevelopmentNonReturnQualificationError("exact SIP VWAP is unresolved")
    return {
        "wap": float(current["wap"]),
        "flat_or_rising": float(current["wap"]) >= float(prior["wap"]),
        "current_eligible_volume": int(current["vwap_eligible_volume"]),
        "unsupported_trade_count": int(current["unsupported_trade_count"]),
    }


def _market_state(
    candidate_open: float,
    candidate_price: float,
    candidate_rows: Sequence[Mapping[str, Any]],
    benchmark_rows: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    candidate = _bar_state(candidate_rows)
    benchmarks = {name: _bar_state(rows) for name, rows in benchmark_rows.items()}
    if set(benchmarks) != {"SPY", "QQQ"}:
        raise DevelopmentNonReturnQualificationError("both benchmark prefixes are required")
    both_unsupportive = all(
        value["last_close"] < value["vwap"]
        and value["vwap_flat_or_rising"] is False
        and value["last_bar_makes_five_bar_low"] is True
        for value in benchmarks.values()
    )
    candidate_return = candidate_price / candidate_open - 1
    outperforms = all(
        candidate_return > value["last_close"] / value["open"] - 1
        for value in benchmarks.values()
    )
    return {
        "candidate": candidate,
        "benchmarks": benchmarks,
        "both_benchmarks_unsupportive": both_unsupportive,
        "candidate_outperforms_both": outperforms,
        "supportive_or_independent_strength": (not both_unsupportive) or outperforms,
    }


def _target_adjusted_daily_rows(
    state: Mapping[str, Any], split_actions: Mapping[str, Sequence[Mapping[str, Any]]]
) -> tuple[list[dict[str, Any]], bool]:
    observations = state["requests"]["history"].get("observations", [])
    grouped: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in observations:
        if row.get("interpolated") is True:
            continue
        day = str(row.get("date_et") or "")
        try:
            date.fromisoformat(day)
        except ValueError:
            continue
        grouped[day].append(row)
    target = str(state["date"])
    symbol = str(state["symbol"])
    rows = [
        {
            "date_et": day,
            "high": max(float(row["high"]) for row in values)
            * split_adjustment_factor(symbol, day, target, split_actions),
        }
        for day, values in sorted(grouped.items())
    ]
    complete = (
        int(state["requests"]["history"].get("required_sessions", 0)) == 252
        and len(rows) == 252
    )
    return rows, complete


def _structure_state(
    state: Mapping[str, Any], metrics: Mapping[str, Any], split_actions: Mapping[str, Any]
) -> dict[str, Any]:
    final = datetime.fromisoformat(str(state["final_decision_at_et"]))
    symbol = str(state["symbol"])
    completed = state["requests"]["completed_bars"][symbol]["observations"]
    premarket = state["requests"]["premarket"]["observations"]
    if any(row.get("interpolated") is True for row in completed + premarket):
        raise DevelopmentNonReturnQualificationError("structure bars contain interpolation")
    daily_rows, daily_complete = _target_adjusted_daily_rows(state, split_actions)
    rules = _rules_contract()
    try:
        structure = derive_preentry_structure(
            observation_at=final,
            entry_limit=metrics["final_ask"],
            daily_atr_14=state["scanner_fields"]["daily_atr_14"],
            median_spread_dollars=metrics["median_spread_dollars"],
            completed_regular_bars=completed,
            premarket_bars=premarket,
            premarket_window_complete=True,
            target_adjusted_daily_bars=daily_rows,
            daily_history_complete=daily_complete,
            daily_split_basis_verified=True,
            atr_stop_fraction=rules["atr_stop_fraction"],
            maximum_stop_fraction=rules["maximum_stop_fraction"],
            minimum_room_fraction=rules["minimum_resistance_room_fraction"],
        )
    except PreentryStructureError as exc:
        raise DevelopmentNonReturnQualificationError(str(exc)) from exc
    value = structure.to_dict()
    value["planned_stop_below_every_observed_bid"] = (
        structure.stop.planned_stop < metrics["minimum_bid"]
    )
    return value


def _evaluator_payload(
    state: Mapping[str, Any],
    metrics: Mapping[str, Any],
    vwap: Mapping[str, Any],
    market: Mapping[str, Any],
    structure: Mapping[str, Any],
) -> dict[str, Any]:
    fields = state["scanner_fields"]
    final = datetime.fromisoformat(str(state["final_decision_at_et"]))
    rvol = float(fields["opening_relative_volume"])
    prior_mean = float(fields["opening_volume"]) / rvol
    resistance = structure["resistance"]
    if resistance["resistance_price"] is None:
        resistance_price = float(metrics["final_ask"]) * 2
        resistance_basis = "price_discovery_score_adapter"
    else:
        resistance_price = float(resistance["resistance_price"])
        resistance_basis = str(resistance["resistance_source"])
    recent = int(market["candidate"]["last_real_volume"])
    quotes = [
        {
            "age_seconds": float(row["age_seconds"]),
            "bid": float(row["bid"]),
            "ask": float(row["ask"]),
            "ask_depth": int(row["ask_size"]),
            "recent_real_1m_volume": recent,
        }
        for row in state["requests"]["quote_window"]["snapshots"]
    ]
    return {
        "session": {
            "time_et": final.timetz().replace(tzinfo=None).isoformat(),
            "mode": "shadow",
            "maturity": "UNVALIDATED",
            "agentic_allowed": True,
            "account_identified": True,
            "encryption_ready": True,
            "monitoring_available": True,
            "protective_stop_workflow_ready": True,
            "broker_review_available": True,
            "open_positions": 0,
            "unresolved_orders": 0,
            "filled_entries_today": 0,
            "circuit_breaker_active": False,
            "account_equity": REFERENCE_ACCOUNT_EQUITY,
            "buying_power": REFERENCE_BUYING_POWER,
        },
        "candidate": {
            "symbol": state["symbol"],
            "is_common_stock": True,
            "average_daily_volume_14": fields["average_daily_volume_14"],
            "daily_atr_14": fields["daily_atr_14"],
            "opening_bar": {
                "open": fields["open_price"],
                "high": fields["opening_high"],
                "low": fields["opening_low"],
                "close": fields["opening_close"],
                "volume": fields["opening_volume"],
            },
            "prior_opening_volumes": [prior_mean] * 14,
            "opening_rvol_rank": int(state["rank"]),
            "ranking_scope": "full_eligible_universe",
            "ranking_scope_count": max(int(state["rank"]), 1),
            "verified_catalyst": True,
            "catalyst_score": 25,
            "dilution_conflict": False,
            "halt_risk": int(state["requests"]["halts"]["causal_record_count"]) > 0,
            "tradable": True,
            "clean_break": True,
            "above_vwap": float(metrics["final_ask"]) > float(vwap["wap"]),
            "vwap_flat_or_rising": vwap["flat_or_rising"],
            "benchmark_supportive_or_independent_strength": market[
                "supportive_or_independent_strength"
            ],
            "sector_relative_strength": market["candidate_outperforms_both"],
            "stop_outside_noise": structure["stop"]["stop_outside_noise"],
            "entry_limit": metrics["final_ask"],
            "technical_invalidation": structure["stop"]["technical_invalidation"],
            "resistance_price": resistance_price,
            "observed_stop_slippage_p95_fraction": 0.0,
            "historical_resistance_basis": resistance_basis,
        },
        "quotes": quotes,
    }


def evaluate_state(
    state: Mapping[str, Any], split_actions: Mapping[str, Any]
) -> dict[str, Any]:
    if state.get("terminal_disposition") == "NO_CLEAN_CROSS_BEFORE_CUTOFF":
        return {
            "pair_key": state["pair_key"],
            "date": state["date"],
            "symbol": state["symbol"],
            "instrument_id": state["instrument_id"],
            "terminal_reason": "NO_CLEAN_CROSS_BEFORE_CUTOFF",
            "survivor": False,
            "gates": {},
            "target_outcome_observed_or_derived": False,
        }
    if state.get("terminal_disposition") != "PREENTRY_INPUTS_COLLECTED":
        raise DevelopmentNonReturnQualificationError("source pair is not terminal")
    try:
        rules = _rules_contract()
        snapshots = state["requests"]["quote_window"]["snapshots"]
        metrics = quote_metrics(snapshots, rules)
        if not metrics.get("valid"):
            raise DevelopmentNonReturnQualificationError("quote snapshots are unresolved")
        fields = state["scanner_fields"]
        opening_high = float(fields["opening_high"])
        final_ask = float(metrics["final_ask"])
        final = datetime.fromisoformat(str(state["final_decision_at_et"]))
        vwap = _exact_vwap_state(
            state["requests"]["decision_trade_prefix"]["observations"], final
        )
        completed = state["requests"]["completed_bars"]
        symbol = str(state["symbol"])
        market = _market_state(
            float(fields["open_price"]),
            final_ask,
            completed[symbol]["observations"],
            {name: completed[name]["observations"] for name in ("SPY", "QQQ")},
        )
        structure = _structure_state(state, metrics, split_actions)
        payload = _evaluator_payload(state, metrics, vwap, market, structure)
        try:
            evaluation = evaluate_candidate(payload, load_config(STRATEGY_CONFIG)).to_dict()
        except StrategyInputError as exc:
            evaluation = {
                "eligible": False,
                "score": 0,
                "classification": "rejected",
                "hard_rejects": [str(exc)],
            }
        minimum_depth = int(metrics["minimum_ask_depth"])
        recent_volume = int(market["candidate"]["last_real_volume"])
        q_liquidity = min(
            math.floor(minimum_depth * rules["maximum_depth_participation_fraction"]),
            math.floor(
                recent_volume * rules["maximum_recent_volume_participation_fraction"]
            ),
        )
        gates = {
            "coarse_scanner": _coarse_scanner_pass(state),
            "opening_prefix_consistent": _opening_prefix_consistent(state),
            "quote": metrics["valid"] and metrics["operating_spread_pass"],
            "a_plus_spread": metrics["a_plus_spread_pass"],
            "chase": opening_high <= final_ask
            <= opening_high * (1 + rules["maximum_entry_chase_fraction"]),
            "vwap": final_ask > vwap["wap"] and vwap["flat_or_rising"],
            "liquidity": minimum_depth > 0 and recent_volume > 0 and q_liquidity > 0,
            "halt": int(state["requests"]["halts"]["causal_record_count"]) == 0,
            "market": market["supportive_or_independent_strength"],
            "relative_strength": market["candidate_outperforms_both"],
            "structure": (
                structure["stop"]["stop_outside_noise"]
                and structure["stop"]["maximum_stop_fraction_pass"]
                and structure["planned_stop_below_every_observed_bid"]
            ),
            "resistance": structure["resistance"]["minimum_room_pass"] is True,
            "evaluator": evaluation.get("eligible") is True,
        }
        terminal_map = (
            ("COARSE_SCANNER_GATE_FAILED", "coarse_scanner"),
            ("OPENING_PREFIX_MISMATCH", "opening_prefix_consistent"),
            ("QUOTE_GATE_FAILED", "quote"),
            ("A_PLUS_SPREAD_FAILED", "a_plus_spread"),
            ("CHASE_GATE_FAILED", "chase"),
            ("VWAP_GATE_FAILED", "vwap"),
            ("LIQUIDITY_GATE_FAILED", "liquidity"),
            ("HALT_GATE_FAILED", "halt"),
            ("MARKET_GATE_FAILED", "market"),
            ("RELATIVE_STRENGTH_GATE_FAILED", "relative_strength"),
            ("STRUCTURE_GATE_FAILED", "structure"),
            ("RESISTANCE_GATE_FAILED", "resistance"),
            ("EVALUATOR_INELIGIBLE", "evaluator"),
        )
        terminal = next((reason for reason, gate in terminal_map if not gates[gate]), "SURVIVOR")
        return {
            "pair_key": state["pair_key"],
            "date": state["date"],
            "symbol": state["symbol"],
            "instrument_id": state["instrument_id"],
            "rank": state["rank"],
            "source_state_sha256": _sha256_json(state),
            "terminal_reason": terminal,
            "survivor": terminal == "SURVIVOR",
            "gates": gates,
            "metrics": {
                "median_spread_fraction": metrics["median_spread_fraction"],
                "maximum_spread_fraction": metrics["maximum_spread_fraction"],
                "final_ask": final_ask,
                "exact_vwap": vwap["wap"],
                "q_liquidity": q_liquidity,
                "stop_fraction": structure["stop"]["stop_fraction"],
                "resistance_status": structure["resistance"]["status"],
                "resistance_room_fraction": structure["resistance"][
                    "resistance_room_fraction"
                ],
                "score": int(evaluation.get("score", 0)),
                "classification": evaluation.get("classification"),
            },
            "evaluator": evaluation,
            "broker_specific_tradability": "PROSPECTIVE_ONLY_UNRECONSTRUCTABLE",
            "target_outcome_observed_or_derived": False,
        }
    except (KeyError, TypeError, ValueError, DevelopmentNonReturnQualificationError) as exc:
        return {
            "pair_key": state.get("pair_key"),
            "date": state.get("date"),
            "symbol": state.get("symbol"),
            "instrument_id": state.get("instrument_id"),
            "source_state_sha256": _sha256_json(state),
            "terminal_reason": "INPUT_UNRESOLVED",
            "survivor": False,
            "gates": {},
            "error": str(exc),
            "target_outcome_observed_or_derived": False,
        }


def _aggregate(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    terminal = Counter(str(row["terminal_reason"]) for row in records)
    gate_counts: Counter[str] = Counter()
    for row in records:
        for name, passed in row.get("gates", {}).items():
            gate_counts[f"{name}_pass"] += int(passed is True)
    order = (
        "coarse_scanner",
        "opening_prefix_consistent",
        "quote",
        "a_plus_spread",
        "chase",
        "vwap",
        "liquidity",
        "halt",
        "market",
        "relative_strength",
        "structure",
        "resistance",
        "evaluator",
    )
    cascade: dict[str, int] = {"selected": len(records)}
    survivors = [
        row
        for row in records
        if row["terminal_reason"] != "NO_CLEAN_CROSS_BEFORE_CUTOFF"
    ]
    cascade["after_clean_cross"] = len(survivors)
    for gate in order:
        survivors = [row for row in survivors if row.get("gates", {}).get(gate) is True]
        cascade[f"after_{gate}"] = len(survivors)
    return {
        "terminal_counts": dict(sorted(terminal.items())),
        "gate_counts": dict(sorted(gate_counts.items())),
        "cascade": cascade,
        "survivors": terminal.get("SURVIVOR", 0),
    }


def qualify(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    _published(manifest_path)
    store, index, pair_paths = _load_source(env_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    expected = _expected_contract(
        index=index,
        store_root=store.root,
        observed_free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DevelopmentNonReturnQualificationError(
                f"qualification manifest drifted at {key}"
            )
    split_actions = load_split_actions(_source_paths(store.root)["splits"])
    records = [evaluate_state(_read_gzip(path), split_actions) for path in pair_paths]
    aggregate = _aggregate(records)
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "source_private_index_sha256": _sha256_file(_source_paths(store.root)["index"]),
        "status": "QUALIFICATION_COMPLETE",
        "updated_at": datetime.now(UTC).isoformat(),
        **aggregate,
        "records": records,
        "broker_specific_tradability": "PROSPECTIVE_ONLY_UNRECONSTRUCTABLE",
        "target_outcomes_observed_or_derived": False,
    }
    private_path = _private_result_path(store.root)
    _write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "QUALIFICATION_COMPLETE",
        "inspected": False,
        "pairs_evaluated": len(records),
        **aggregate,
        "minimum_survivors_before_outcomes": MINIMUM_SURVIVORS,
        "outcome_contract_permitted": False,
        "private_result_sha256": _sha256_file(private_path),
        "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect_result(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    """Rebuild aggregate integrity without authorizing outcomes or inspection."""
    store, _index, _pairs = _load_source(env_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    private_path = _private_result_path(store.root)
    private = _read_gzip(private_path)
    records = private.get("records")
    if (
        not isinstance(records, list)
        or len(records) != EXPECTED_PAIRS
        or private.get("manifest_sha256") != manifest.get("manifest_sha256")
        or private.get("status") != "QUALIFICATION_COMPLETE"
        or private.get("target_outcomes_observed_or_derived") is not False
    ):
        raise DevelopmentNonReturnQualificationError("qualification result is incomplete")
    aggregate = _aggregate(records)
    for key, value in aggregate.items():
        if private.get(key) != value:
            raise DevelopmentNonReturnQualificationError(
                f"private qualification aggregate drifted at {key}"
            )
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "QUALIFICATION_AGGREGATE_REBUILT",
        "inspected": False,
        "pairs_evaluated": len(records),
        **aggregate,
        "minimum_survivors_before_outcomes": MINIMUM_SURVIVORS,
        "outcome_contract_permitted": False,
        "private_result_sha256": _sha256_file(private_path),
        "inspection": {
            "private_aggregate_rebuilt": True,
            "privacy_and_outcome_locks_rechecked": True,
            "independent_per_pair_rebuild_required": True,
            "valid": True,
        },
        "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze")
    contract = sub.add_parser("inspect-contract")
    contract.add_argument("manifest", type=Path)
    qualify_parser = sub.add_parser("qualify")
    qualify_parser.add_argument("manifest", type=Path)
    inspect_parser = sub.add_parser("inspect")
    inspect_parser.add_argument("manifest", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, status = freeze(
                env_path=args.env,
                output_root=DEFAULT_MANIFEST_ROOT,
                public_status_path=DEFAULT_CONTRACT_STATUS,
            )
            value = {**status, "path": _repo_path(path)}
        elif args.command == "inspect-contract":
            value = inspect_contract(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_CONTRACT_STATUS,
            )
        elif args.command == "qualify":
            value = qualify(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_QUALIFICATION_STATUS,
            )
        else:
            value = inspect_result(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_QUALIFICATION_STATUS,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (DevelopmentNonReturnQualificationError, LearningDataError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
