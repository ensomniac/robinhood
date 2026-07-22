"""Freeze and evaluate outcome-blind entry qualification for ORB retest v1."""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import challenger_orb_retest_qualification_collection as collection
import challenger_orb_retest_qualification_collection_compatibility as paths
import challenger_orb_retest_qualification_collection_compatibility2 as compatibility
import development_non_return_qualification_v3 as base
import preentry_structure as structure_base
from historical_store import HistoricalDayStore, HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract
from scanner_replay import load_split_actions
from strategy_engine import StrategyInputError, evaluate_candidate, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-challenger-orb-retest-entry-qualification-2026-07-22-v1"
SOURCE_MANIFEST = paths.SOURCE_MANIFEST
SOURCE_MANIFEST_SHA256 = paths.SOURCE_MANIFEST_SHA256
SOURCE_STATUS = paths.SOURCE_STATUS
SOURCE_INSPECTION = paths.DEFAULT_RESULT
COMPATIBILITY_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification_inspection_compatibility_manifests/"
    "dataset-challenger-orb-retest-qualification-inspection-compatibility-"
    "2026-07-22-v2-a26f26410757ee9045f12a776522ed00d2882e12ffcc2034c1b8d060cc6850bb.json"
)
COMPATIBILITY_MANIFEST_SHA256 = (
    "a26f26410757ee9045f12a776522ed00d2882e12ffcc2034c1b8d060cc6850bb"
)
COMPATIBILITY_STATUS = compatibility.DEFAULT_STATUS
HYPOTHESIS = (
    PROJECT_ROOT
    / "learning/hypotheses/"
    "experiment-catalyst-orb-retest-v1-"
    "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105.json"
)
HYPOTHESIS_SHA256 = (
    "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105"
)
STRATEGY_CONFIG = PROJECT_ROOT / "strategy_config.toml"
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_qualification_inspection.py"
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification_manifests"
)
DEFAULT_CONTRACT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "retest-qualification-contract-status.json"
)
DEFAULT_QUALIFICATION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "retest-qualification-status.json"
)
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-entry-qualification.json"
)
PRIVATE_NAMESPACE = "_derived/challenger_orb_retest_qualification"
EXPECTED_PAIRS = 60
EXPECTED_DATES = 52
MINIMUM_ELIGIBLE_SIGNALS = 50
REFERENCE_ACCOUNT_EQUITY = 100_000.0
REFERENCE_BUYING_POWER = 100_000.0


class ChallengerRetestQualificationError(RuntimeError):
    """The frozen retest qualification contract or evidence differs."""


def _private_source_root(store_root: Path) -> Path:
    return store_root / collection.PRIVATE_NAMESPACE / collection.DATASET_ID


def _private_root(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / DATASET_ID


def _private_result_path(store_root: Path) -> Path:
    return _private_root(store_root) / "qualification-index.json.gz"


def _source_index_path(store_root: Path) -> Path:
    return _private_source_root(store_root) / collection.base.INDEX_FILE


def _source_splits_path(store_root: Path) -> Path:
    return _private_source_root(store_root) / collection.base.SPLITS_FILE


def _published(path: Path) -> dict[str, str]:
    try:
        return collection.base._published(path)
    except collection.base.DevelopmentNonReturnCollectionError as exc:
        raise ChallengerRetestQualificationError(str(exc)) from exc


def _binding(path: Path) -> dict[str, str]:
    return {
        "path": collection.base._repo_path(path),
        "sha256": base._sha256_file(path),
    }


def _verify_binding(value: Any) -> None:
    if not isinstance(value, Mapping):
        raise ChallengerRetestQualificationError("bound artifact is malformed")
    raw = value.get("path")
    if not isinstance(raw, str) or not raw:
        raise ChallengerRetestQualificationError("bound artifact path is missing")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise ChallengerRetestQualificationError("bound artifact path is unsafe")
    path = PROJECT_ROOT / relative
    if not path.is_file() or base._sha256_file(path) != value.get("sha256"):
        raise ChallengerRetestQualificationError(f"bound artifact drifted: {relative}")


def _load_source(
    env_path: Path,
) -> tuple[
    HistoricalDayStore,
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
]:
    store = HistoricalDayStore.from_env(env_path)
    manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    status = base._read_json(SOURCE_STATUS)
    inspection = base._read_json(SOURCE_INSPECTION)
    compatibility_manifest = compatibility.load_manifest(COMPATIBILITY_MANIFEST)
    compatibility_status = base._read_json(COMPATIBILITY_STATUS)
    if not (
        manifest.get("manifest_sha256") == SOURCE_MANIFEST_SHA256
        and status == inspection
        and status.get("status") == "COLLECTION_INSPECTED"
        and status.get("inspected") is True
        and status.get("valid") is True
        and status.get("pairs_terminal") == EXPECTED_PAIRS
        and status.get("distinct_trigger_sessions") == EXPECTED_DATES
        and status.get("provider_rows_after_final_decision") is False
        and status.get("post_entry_data_access_allowed") is False
        and status.get("target_outcomes_observed_or_derived") is False
        and compatibility_manifest.get("manifest_sha256")
        == COMPATIBILITY_MANIFEST_SHA256
        and compatibility_status.get("manifest_sha256")
        == COMPATIBILITY_MANIFEST_SHA256
        and compatibility_status.get("status") == "READY"
        and compatibility_status.get("inspected") is True
        and compatibility_status.get("valid") is True
        and compatibility_status.get("v1_public_status_mutated") is False
        and compatibility_status.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerRetestQualificationError(
            "inspected qualification collection source differs"
        )
    _base, _public, private, rebuilt_store = collection._load_base(env_path)
    if rebuilt_store.root != store.root:
        raise ChallengerRetestQualificationError("historical store root changed")
    pairs = private["selection"]["positive_pairs"]
    index_path = _source_index_path(store.root)
    split_path = _source_splits_path(store.root)
    index = base._read_gzip(index_path)
    indexed = {
        str(row["pair_key"]): str(row["sha256"])
        for row in index.get("pair_files", [])
        if isinstance(row, Mapping)
    }
    states: list[dict[str, Any]] = []
    for pair in pairs:
        path = paths._pair_path(store.root, pair)
        if indexed.get(paths._pair_key(pair)) != base._sha256_file(path):
            raise ChallengerRetestQualificationError("source pair hash differs")
        state = base._read_gzip(path)
        if not (
            state.get("pair_key") == paths._pair_key(pair)
            and state.get("status") == "TERMINAL"
            and state.get("terminal_disposition") == "PREENTRY_INPUTS_COLLECTED"
            and state.get("target_outcome_observed_or_derived") is False
            and state.get("provider_rows_after_final_decision") is False
            and state.get("frozen_retest_trigger") == pair["trigger"]
        ):
            raise ChallengerRetestQualificationError("source pair state differs")
        states.append(state)
    if not (
        len(pairs) == EXPECTED_PAIRS
        and len(states) == EXPECTED_PAIRS
        and len(indexed) == EXPECTED_PAIRS
        and len({str(row["date"]) for row in pairs}) == EXPECTED_DATES
        and index.get("manifest_sha256") == SOURCE_MANIFEST_SHA256
        and index.get("target_outcomes_observed_or_derived") is False
        and base._sha256_file(index_path) == status["private_index_sha256"]
        and split_path.is_file()
    ):
        raise ChallengerRetestQualificationError("source denominator differs")
    return store, index, pairs, states


def _rules_contract() -> dict[str, Any]:
    hypothesis = base._read_json(HYPOTHESIS)
    config = load_config(STRATEGY_CONFIG)
    parameters = hypothesis.get("primary_parameters")
    expected_parameters = {
        "initial_break_required": True,
        "rebreak_chase_cap_fraction": 0.0015,
        "retest_hold_completed_bars": 1,
        "retest_rule": "touch_opening_range_high_and_close_at_or_above",
        "stop_atr_fraction": 0.1,
        "stop_maximum_fraction": 0.008,
        "target_fraction": 0.02,
    }
    if not (
        hypothesis.get("contract_sha256") == HYPOTHESIS_SHA256
        and parameters == expected_parameters
    ):
        raise ChallengerRetestQualificationError("frozen retest hypothesis differs")
    execution = config.raw["execution"]
    risk = config.raw["risk"]
    maturity = config.raw["maturity"]["UNVALIDATED"]
    return {
        "hypothesis_sha256": HYPOTHESIS_SHA256,
        "primary_trial_id": str(hypothesis["primary_trial_id"]),
        "strategy_id": str(hypothesis["strategy_id"]),
        "primary_parameters": expected_parameters,
        "strategy_config_sha256": base._sha256_file(STRATEGY_CONFIG),
        "strategy_config_version": config.version,
        "strategy_config_rules_hash": config.rules_hash,
        "minimum_score": int(maturity["minimum_score"]),
        "maximum_quote_age_seconds": float(execution["maximum_quote_age_seconds"]),
        "maximum_median_spread_fraction": float(
            execution["maximum_median_spread_fraction"]
        ),
        "maximum_single_spread_fraction": float(
            execution["maximum_single_spread_fraction"]
        ),
        "maximum_a_plus_median_spread_fraction": float(
            execution["maximum_a_plus_median_spread_fraction"]
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


def _implementation_contract() -> dict[str, dict[str, str]]:
    names = (
        "challenger_orb_retest_qualification.py",
        "challenger_orb_retest_qualification_inspection.py",
        "challenger_orb_retest_qualification_collection.py",
        "development_non_return_qualification_v3.py",
        "preentry_structure.py",
        "sip_bar_aggregation.py",
        "strategy_engine.py",
        "scanner_replay.py",
    )
    return {name: _binding(PROJECT_ROOT / name) for name in names}


def _source_snapshot(
    *, store: HistoricalDayStore, index: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "collection_manifest": _binding(SOURCE_MANIFEST),
        "collection_status": _binding(SOURCE_STATUS),
        "collection_inspection": _binding(SOURCE_INSPECTION),
        "compatibility_manifest": _binding(COMPATIBILITY_MANIFEST),
        "compatibility_status": _binding(COMPATIBILITY_STATUS),
        "private_index_sha256": base._sha256_file(_source_index_path(store.root)),
        "private_pair_file_index_sha256": base._sha256_json(index["pair_files"]),
        "private_splits_sha256": base._sha256_file(_source_splits_path(store.root)),
        "pairs_expected": EXPECTED_PAIRS,
        "distinct_trigger_sessions": EXPECTED_DATES,
        "causal_rows_rehashed": 1_006_966,
    }


def _expected_contract(
    *, store: HistoricalDayStore, index: Mapping[str, Any], observed_free_bytes: int
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_contract": _source_snapshot(store=store, index=index),
        "implementation_contract": {"files": _implementation_contract()},
        "qualification_contract": {
            **_rules_contract(),
            "minimum_eligible_one_per_session_signals": MINIMUM_ELIGIBLE_SIGNALS,
            "terminal_gate_order": [
                "INPUT_UNRESOLVED",
                "COARSE_SCANNER_GATE_FAILED",
                "OPENING_PREFIX_MISMATCH",
                "TRIGGER_COMPATIBILITY_FAILED",
                "RETEST_VWAP_GATE_FAILED",
                "QUOTE_GATE_FAILED",
                "A_PLUS_SPREAD_FAILED",
                "REBREAK_CHASE_GATE_FAILED",
                "DECISION_VWAP_GATE_FAILED",
                "LIQUIDITY_GATE_FAILED",
                "HALT_GATE_FAILED",
                "MARKET_GATE_FAILED",
                "RELATIVE_STRENGTH_GATE_FAILED",
                "RETEST_STRUCTURE_GATE_FAILED",
                "RESISTANCE_GATE_FAILED",
                "EVALUATOR_INELIGIBLE",
                "DAILY_RANK_NOT_SELECTED",
                "SURVIVOR",
            ],
            "retest_noise_definition": (
                "MAX_MEDIAN_SPREAD_OR_MEAN_COMPLETED_CLOSE_INCREMENT"
            ),
            "technical_invalidation": "RETEST_BAR_LOW_MINUS_NORMAL_NOISE",
            "stop_distance": "MAX_0.10_ATR_OR_ENTRY_MINUS_INVALIDATION",
            "daily_ranking": [
                "OPENING_RELATIVE_VOLUME_DESC",
                "DIRECT_CATALYST_QUALITY_DESC",
                "MEDIAN_SPREAD_ASC",
                "PRIVATE_PAIR_KEY_ASC",
            ],
            "maximum_selected_per_session": 1,
            "exact_sip_retest_and_decision_vwap_required": True,
            "complete_252_session_split_adjusted_resistance_required": True,
            "broker_specific_historical_tradability": (
                "PROSPECTIVE_ONLY_UNRECONSTRUCTABLE"
            ),
            "missing_inputs_default_favorable": False,
            "substitutions_allowed": False,
        },
        "capacity_contract": {
            "minimum_free_bytes": 20 * 1024**3,
            "observed_free_bytes_at_freeze": observed_free_bytes,
            "historical_deletion_allowed": False,
        },
        "privacy_contract": {
            "private_root": (
                f"LOCAL_HISTORICAL_DATA_ROOT/{PRIVATE_NAMESPACE}/{DATASET_ID}/"
            ),
            "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
            "public_aggregates_and_hashes_only": True,
        },
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "fills_returns_prices_and_outcomes_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "outcome_contract_permitted_before_inspected_capacity": False,
        },
    }


def _write_manifest(
    value: Mapping[str, Any], output_root: Path
) -> tuple[Path, dict[str, Any]]:
    content = dict(value)
    content.pop("manifest_sha256", None)
    fingerprint = base._sha256_json(content)
    frozen = {**content, "manifest_sha256": fingerprint}
    path = output_root / f"{DATASET_ID}-{fingerprint}.json"
    if path.exists() and base._read_json(path) != frozen:
        raise ChallengerRetestQualificationError(
            "hash-addressed qualification manifest has other content"
        )
    base._write_json(path, frozen)
    return path, frozen


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = base._read_json(path)
    content = dict(manifest)
    recorded = content.pop("manifest_sha256", None)
    expected = base._sha256_json(content)
    if not (
        manifest.get("schema_version") == 1
        and manifest.get("dataset_id") == DATASET_ID
        and recorded == expected
        and path.name == f"{DATASET_ID}-{expected}.json"
    ):
        raise ChallengerRetestQualificationError(
            "qualification manifest was mutated or renamed"
        )
    return manifest


def freeze(
    *, env_path: Path, output_root: Path, status_path: Path
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__),
        INSPECTOR,
        SOURCE_MANIFEST,
        SOURCE_STATUS,
        SOURCE_INSPECTION,
        COMPATIBILITY_MANIFEST,
        COMPATIBILITY_STATUS,
        HYPOTHESIS,
        STRATEGY_CONFIG,
    ):
        _published(path)
    store, index, _pairs, _states = _load_source(env_path)
    root = _private_root(store.root)
    artifacts = list(root.rglob("*")) if root.exists() else []
    if any(path.is_file() for path in artifacts) or DEFAULT_RESULT.exists():
        raise ChallengerRetestQualificationError(
            "qualification result exists before contract freeze"
        )
    free = os.statvfs(store.root).f_bavail * os.statvfs(store.root).f_frsize
    contract = {
        **_expected_contract(store=store, index=index, observed_free_bytes=free),
        "registered_at": datetime.now(UTC).isoformat(),
    }
    path, manifest = _write_manifest(contract, output_root)
    status = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_WAITING_INSPECTION",
        "inspected": False,
        "pairs_expected": EXPECTED_PAIRS,
        "distinct_trigger_sessions": EXPECTED_DATES,
        "minimum_eligible_signals": MINIMUM_ELIGIBLE_SIGNALS,
        "private_artifacts": 0,
        "outcome_contract_permitted": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    base._write_json(status_path, status)
    return path, manifest


def derive_retest_stop(
    *,
    entry_limit: float,
    retest_bar_low: float,
    average_close_increment: float,
    median_spread_dollars: float,
    daily_atr_14: float,
    minimum_observed_bid: float,
) -> dict[str, Any]:
    values = (
        entry_limit,
        retest_bar_low,
        median_spread_dollars,
        daily_atr_14,
        minimum_observed_bid,
    )
    if any(not math.isfinite(float(value)) or float(value) <= 0 for value in values):
        raise ChallengerRetestQualificationError("retest stop inputs must be positive")
    if not math.isfinite(average_close_increment) or average_close_increment < 0:
        raise ChallengerRetestQualificationError("retest noise increment is invalid")
    rules = _rules_contract()
    noise = max(float(median_spread_dollars), float(average_close_increment))
    invalidation = float(retest_bar_low) - noise
    if invalidation <= 0 or invalidation >= entry_limit:
        raise ChallengerRetestQualificationError("retest invalidation is invalid")
    atr_distance = rules["atr_stop_fraction"] * float(daily_atr_14)
    distance = max(atr_distance, entry_limit - invalidation)
    planned = entry_limit - distance
    if planned <= 0:
        raise ChallengerRetestQualificationError("planned retest stop is nonpositive")
    fraction = distance / entry_limit
    return {
        "retest_bar_low": float(retest_bar_low),
        "average_close_increment": float(average_close_increment),
        "median_spread_dollars": float(median_spread_dollars),
        "normal_noise_dollars": noise,
        "technical_invalidation": invalidation,
        "atr_stop_distance": atr_distance,
        "stop_distance": distance,
        "planned_stop": planned,
        "stop_fraction": fraction,
        "stop_outside_noise": planned <= invalidation,
        "maximum_stop_fraction_pass": fraction <= rules["maximum_stop_fraction"],
        "planned_stop_below_every_observed_bid": planned < minimum_observed_bid,
    }


def _retest_bar(
    state: Mapping[str, Any], trigger: Mapping[str, Any]
) -> tuple[dict[str, Any], datetime]:
    symbol = str(state["symbol"])
    rows = state["requests"]["completed_bars"][symbol]["observations"]
    target = paths.parse_aware(trigger["retest_bar_start_et"]).astimezone(
        structure_base.EASTERN
    )
    matches = [
        row
        for row in rows
        if base._observed(row).astimezone(structure_base.EASTERN) == target
    ]
    if len(matches) != 1:
        raise ChallengerRetestQualificationError("frozen retest bar is missing")
    row = dict(matches[0])
    for name, trigger_name in (
        ("high", "retest_bar_high"),
        ("low", "retest_bar_low"),
        ("close", "retest_bar_close"),
    ):
        if not math.isclose(
            float(row[name]), float(trigger[trigger_name]), rel_tol=1e-9, abs_tol=1e-8
        ):
            raise ChallengerRetestQualificationError("frozen retest bar changed")
    opening_high = float(state["scanner_fields"]["opening_high"])
    if not (
        float(row["low"]) <= opening_high
        and float(row["close"]) >= opening_high
        and row.get("interpolated") is not True
    ):
        raise ChallengerRetestQualificationError("retest hold contract differs")
    return row, target


def _retest_structure(
    state: Mapping[str, Any],
    trigger: Mapping[str, Any],
    quote: Mapping[str, Any],
    split_actions: Mapping[str, Any],
) -> dict[str, Any]:
    final = paths.parse_aware(state["final_decision_at_et"]).astimezone(
        structure_base.EASTERN
    )
    symbol = str(state["symbol"])
    completed = state["requests"]["completed_bars"][symbol]["observations"]
    premarket = state["requests"]["premarket"]["observations"]
    if any(row.get("interpolated") is True for row in completed + premarket):
        raise ChallengerRetestQualificationError("structure bars contain interpolation")
    _opening_high, average_increment, completed_count = (
        structure_base._opening_and_noise(completed, final)
    )
    retest, _retest_start = _retest_bar(state, trigger)
    stop = derive_retest_stop(
        entry_limit=float(quote["final_ask"]),
        retest_bar_low=float(retest["low"]),
        average_close_increment=average_increment,
        median_spread_dollars=float(quote["median_spread_dollars"]),
        daily_atr_14=float(state["scanner_fields"]["daily_atr_14"]),
        minimum_observed_bid=float(quote["minimum_bid"]),
    )
    stop["completed_noise_bars"] = completed_count
    daily_rows, daily_complete = base._target_adjusted_daily_rows(
        state, split_actions
    )
    resistance = structure_base.derive_resistance_structure(
        observation_at=final,
        entry_limit=float(quote["final_ask"]),
        premarket_bars=premarket,
        premarket_window_complete=True,
        target_adjusted_daily_bars=daily_rows,
        daily_history_complete=daily_complete,
        daily_split_basis_verified=True,
        minimum_room_fraction=_rules_contract()[
            "minimum_resistance_room_fraction"
        ],
    ).__dict__
    return {"stop": stop, "resistance": resistance}


def _evaluator_payload(
    state: Mapping[str, Any],
    trigger: Mapping[str, Any],
    quote: Mapping[str, Any],
    vwap: Mapping[str, Any],
    market: Mapping[str, Any],
    structure: Mapping[str, Any],
) -> dict[str, Any]:
    value = base._evaluator_payload(state, quote, vwap, market, structure)
    value["candidate"]["opening_bar"]["high"] = float(trigger["retest_bar_high"])
    value["candidate"]["technical_invalidation"] = float(
        structure["stop"]["technical_invalidation"]
    )
    value["candidate"]["stop_outside_noise"] = bool(
        structure["stop"]["stop_outside_noise"]
    )
    return value


def evaluate_pair(
    pair: Mapping[str, Any],
    state: Mapping[str, Any],
    split_actions: Mapping[str, Any],
) -> dict[str, Any]:
    try:
        rules = _rules_contract()
        trigger = pair["trigger"]
        snapshots = state["requests"]["quote_window"]["snapshots"]
        quote = base.quote_metrics(snapshots, rules)
        if quote.get("valid") is not True:
            raise ChallengerRetestQualificationError("quote snapshots are unresolved")
        final = paths.parse_aware(state["final_decision_at_et"])
        retest, retest_start = _retest_bar(state, trigger)
        retest_end = retest_start + timedelta(minutes=1)
        trades = state["requests"]["decision_trade_prefix"]["observations"]
        retest_vwap = base._exact_vwap_state(
            [row for row in trades if base._observed(row) < retest_end], retest_end
        )
        decision_vwap = base._exact_vwap_state(trades, final)
        completed = state["requests"]["completed_bars"]
        symbol = str(state["symbol"])
        final_ask = float(quote["final_ask"])
        market = base._market_state(
            float(state["scanner_fields"]["open_price"]),
            final_ask,
            completed[symbol]["observations"],
            {name: completed[name]["observations"] for name in ("SPY", "QQQ")},
        )
        structure = _retest_structure(state, trigger, quote, split_actions)
        payload = _evaluator_payload(
            state, trigger, quote, decision_vwap, market, structure
        )
        try:
            evaluation = evaluate_candidate(payload, load_config(STRATEGY_CONFIG)).to_dict()
        except StrategyInputError as exc:
            evaluation = {
                "eligible": False,
                "score": 0,
                "classification": "rejected",
                "hard_rejects": [str(exc)],
            }
        recent_volume = int(market["candidate"]["last_real_volume"])
        q_liquidity = min(
            math.floor(
                int(quote["minimum_ask_depth"])
                * rules["maximum_depth_participation_fraction"]
            ),
            math.floor(
                recent_volume
                * rules["maximum_recent_volume_participation_fraction"]
            ),
        )
        trigger_compatible = (
            state.get("frozen_retest_trigger") == trigger
            and paths.same_instant(
                state.get("clean_cross", {}).get("observed_at_et"),
                trigger["rebreak_at_et"],
            )
            and paths.same_instant(
                state.get("final_decision_at_et"), trigger["decision_at_et"]
            )
            and float(state.get("clean_cross", {}).get("price", 0))
            == float(trigger["rebreak_price"])
        )
        rebreak_trigger = float(trigger["retest_bar_high"])
        gates = {
            "coarse_scanner": base._coarse_scanner_pass(state),
            "opening_prefix_consistent": base._opening_prefix_consistent(state),
            "trigger_compatibility": trigger_compatible,
            "retest_vwap": retest_vwap["flat_or_rising"],
            "quote": quote["valid"] and quote["operating_spread_pass"],
            "a_plus_spread": quote["a_plus_spread_pass"],
            "rebreak_chase": rebreak_trigger <= final_ask
            <= rebreak_trigger * (1 + rules["maximum_entry_chase_fraction"]),
            "decision_vwap": final_ask > decision_vwap["wap"]
            and decision_vwap["flat_or_rising"],
            "liquidity": int(quote["minimum_ask_depth"]) > 0
            and recent_volume > 0
            and q_liquidity > 0,
            "halt": int(state["requests"]["halts"]["causal_record_count"]) == 0,
            "market": market["supportive_or_independent_strength"],
            "relative_strength": market["candidate_outperforms_both"],
            "retest_structure": structure["stop"]["stop_outside_noise"]
            and structure["stop"]["maximum_stop_fraction_pass"]
            and structure["stop"]["planned_stop_below_every_observed_bid"],
            "resistance": structure["resistance"]["minimum_room_pass"] is True,
            "evaluator": evaluation.get("eligible") is True,
        }
        terminal_order = (
            ("COARSE_SCANNER_GATE_FAILED", "coarse_scanner"),
            ("OPENING_PREFIX_MISMATCH", "opening_prefix_consistent"),
            ("TRIGGER_COMPATIBILITY_FAILED", "trigger_compatibility"),
            ("RETEST_VWAP_GATE_FAILED", "retest_vwap"),
            ("QUOTE_GATE_FAILED", "quote"),
            ("A_PLUS_SPREAD_FAILED", "a_plus_spread"),
            ("REBREAK_CHASE_GATE_FAILED", "rebreak_chase"),
            ("DECISION_VWAP_GATE_FAILED", "decision_vwap"),
            ("LIQUIDITY_GATE_FAILED", "liquidity"),
            ("HALT_GATE_FAILED", "halt"),
            ("MARKET_GATE_FAILED", "market"),
            ("RELATIVE_STRENGTH_GATE_FAILED", "relative_strength"),
            ("RETEST_STRUCTURE_GATE_FAILED", "retest_structure"),
            ("RESISTANCE_GATE_FAILED", "resistance"),
            ("EVALUATOR_INELIGIBLE", "evaluator"),
        )
        terminal = next(
            (reason for reason, gate in terminal_order if gates[gate] is not True),
            "RAW_SURVIVOR",
        )
        return {
            "pair_key": state["pair_key"],
            "date": state["date"],
            "symbol": state["symbol"],
            "instrument_id": state["instrument_id"],
            "rank": state["rank"],
            "source_state_sha256": base._sha256_json(state),
            "terminal_reason": terminal,
            "raw_survivor": terminal == "RAW_SURVIVOR",
            "survivor": False,
            "selected": False,
            "gates": gates,
            "ranking": {
                "opening_relative_volume": float(
                    state["scanner_fields"]["opening_relative_volume"]
                ),
                "direct_catalyst_quality": 25,
                "median_spread_fraction": quote["median_spread_fraction"],
                "private_pair_key": state["pair_key"],
            },
            "metrics": {
                "rebreak_trigger": rebreak_trigger,
                "rebreak_observed_price": float(trigger["rebreak_price"]),
                "final_ask": final_ask,
                "median_spread_fraction": quote["median_spread_fraction"],
                "maximum_spread_fraction": quote["maximum_spread_fraction"],
                "retest_vwap": retest_vwap["wap"],
                "decision_vwap": decision_vwap["wap"],
                "q_liquidity": q_liquidity,
                "normal_noise_dollars": structure["stop"][
                    "normal_noise_dollars"
                ],
                "technical_invalidation": structure["stop"][
                    "technical_invalidation"
                ],
                "planned_stop": structure["stop"]["planned_stop"],
                "stop_fraction": structure["stop"]["stop_fraction"],
                "resistance_status": structure["resistance"]["status"],
                "resistance_room_fraction": structure["resistance"][
                    "resistance_room_fraction"
                ],
                "score": int(evaluation.get("score", 0)),
                "classification": evaluation.get("classification"),
            },
            "evaluator": evaluation,
            "broker_specific_tradability": (
                "PROSPECTIVE_ONLY_UNRECONSTRUCTABLE"
            ),
            "target_outcome_observed_or_derived": False,
        }
    except (
        KeyError,
        TypeError,
        ValueError,
        structure_base.PreentryStructureError,
        ChallengerRetestQualificationError,
        base.DevelopmentNonReturnQualificationError,
    ) as exc:
        return {
            "pair_key": state.get("pair_key"),
            "date": state.get("date"),
            "symbol": state.get("symbol"),
            "instrument_id": state.get("instrument_id"),
            "source_state_sha256": base._sha256_json(state),
            "terminal_reason": "INPUT_UNRESOLVED",
            "raw_survivor": False,
            "survivor": False,
            "selected": False,
            "gates": {},
            "error": str(exc),
            "target_outcome_observed_or_derived": False,
        }


def apply_daily_ranking(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = [dict(row) for row in records]
    by_date: dict[str, list[int]] = defaultdict(list)
    for index, row in enumerate(result):
        if row.get("raw_survivor") is True:
            by_date[str(row["date"])].append(index)
    for indexes in by_date.values():
        def key(index: int) -> tuple[Any, ...]:
            ranking = result[index]["ranking"]
            return (
                -float(ranking["opening_relative_volume"]),
                -int(ranking["direct_catalyst_quality"]),
                float(ranking["median_spread_fraction"]),
                str(ranking["private_pair_key"]),
            )

        winner = min(indexes, key=key)
        for index in indexes:
            if index == winner:
                result[index]["terminal_reason"] = "SURVIVOR"
                result[index]["survivor"] = True
                result[index]["selected"] = True
            else:
                result[index]["terminal_reason"] = "DAILY_RANK_NOT_SELECTED"
    return result


def evaluate_all(
    *,
    pairs: Sequence[Mapping[str, Any]],
    states: Sequence[Mapping[str, Any]],
    split_actions: Mapping[str, Any],
) -> list[dict[str, Any]]:
    raw = [
        evaluate_pair(pair, state, split_actions)
        for pair, state in zip(pairs, states, strict=True)
    ]
    return apply_daily_ranking(raw)


def aggregate(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    terminal = Counter(str(row["terminal_reason"]) for row in records)
    gate_counts: Counter[str] = Counter()
    for row in records:
        for name, passed in row.get("gates", {}).items():
            gate_counts[f"{name}_pass"] += int(passed is True)
    gate_order = (
        "coarse_scanner",
        "opening_prefix_consistent",
        "trigger_compatibility",
        "retest_vwap",
        "quote",
        "a_plus_spread",
        "rebreak_chase",
        "decision_vwap",
        "liquidity",
        "halt",
        "market",
        "relative_strength",
        "retest_structure",
        "resistance",
        "evaluator",
    )
    cascade: dict[str, int] = {"pairs": len(records)}
    survivors = list(records)
    for gate in gate_order:
        survivors = [row for row in survivors if row.get("gates", {}).get(gate) is True]
        cascade[f"after_{gate}"] = len(survivors)
    cascade["after_daily_ranking"] = terminal.get("SURVIVOR", 0)
    raw_dates = {
        str(row["date"]) for row in records if row.get("raw_survivor") is True
    }
    selected_dates = {
        str(row["date"]) for row in records if row.get("selected") is True
    }
    if len(selected_dates) != terminal.get("SURVIVOR", 0):
        raise ChallengerRetestQualificationError(
            "one-per-session selected denominator differs"
        )
    return {
        "terminal_counts": dict(sorted(terminal.items())),
        "gate_counts": dict(sorted(gate_counts.items())),
        "cascade": cascade,
        "raw_survivor_pairs": sum(row.get("raw_survivor") is True for row in records),
        "raw_survivor_sessions": len(raw_dates),
        "daily_rank_not_selected": terminal.get("DAILY_RANK_NOT_SELECTED", 0),
        "eligible_signals": terminal.get("SURVIVOR", 0),
        "eligible_signal_sessions": len(selected_dates),
        "minimum_eligible_signals": MINIMUM_ELIGIBLE_SIGNALS,
        "minimum_capacity_passed": (
            terminal.get("SURVIVOR", 0) >= MINIMUM_ELIGIBLE_SIGNALS
        ),
    }


def qualify(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    _published(manifest_path)
    manifest = load_manifest(manifest_path)
    contract_status = base._read_json(DEFAULT_CONTRACT_STATUS)
    if not (
        contract_status.get("manifest_sha256") == manifest["manifest_sha256"]
        and contract_status.get("status") == "FROZEN_READY"
        and contract_status.get("inspected") is True
        and contract_status.get("outcome_contract_permitted") is False
        and contract_status.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerRetestQualificationError(
            "qualification contract is not independently ready"
        )
    store, index, pairs, states = _load_source(env_path)
    expected = _expected_contract(
        store=store,
        index=index,
        observed_free_bytes=int(
            manifest["capacity_contract"]["observed_free_bytes_at_freeze"]
        ),
    )
    for name, value in expected.items():
        if manifest.get(name) != value:
            raise ChallengerRetestQualificationError(
                f"qualification contract drifted at {name}"
            )
    split_actions = load_split_actions(_source_splits_path(store.root))
    records = evaluate_all(pairs=pairs, states=states, split_actions=split_actions)
    summary = aggregate(records)
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "source_private_index_sha256": base._sha256_file(
            _source_index_path(store.root)
        ),
        "status": "QUALIFICATION_COMPLETE",
        "updated_at": datetime.now(UTC).isoformat(),
        **summary,
        "records": records,
        "target_outcomes_observed_or_derived": False,
    }
    private_path = _private_result_path(store.root)
    base._write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "QUALIFICATION_COMPLETE_UNINSPECTED",
        "inspected": False,
        "pairs_evaluated": len(records),
        "distinct_trigger_sessions": EXPECTED_DATES,
        **summary,
        "outcome_contract_permitted": False,
        "private_result_sha256": base._sha256_file(private_path),
        "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    base._write_json(status_path, public)
    return public


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze")
    qualify_parser = sub.add_parser("qualify")
    qualify_parser.add_argument("manifest", type=Path)
    sub.add_parser("status")
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze(
                env_path=args.env,
                output_root=DEFAULT_MANIFEST_ROOT,
                status_path=DEFAULT_CONTRACT_STATUS,
            )
            value = {"manifest": collection.base._repo_path(path), **manifest}
        elif args.command == "status":
            value = base._read_json(DEFAULT_QUALIFICATION_STATUS)
        else:
            value = qualify(
                manifest_path=args.manifest,
                env_path=args.env,
                status_path=DEFAULT_QUALIFICATION_STATUS,
            )
    except (
        ChallengerRetestQualificationError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
