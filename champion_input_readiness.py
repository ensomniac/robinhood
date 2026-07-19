"""Quantify unchanged-champion input readiness without reading target outcomes.

This development-only join consumes the exact frozen 389 scanner-selected
security-date pairs and their already-inspected input-fidelity layers. It emits
only pre-entry facts, conservative proxies, unresolved fields, and gate
attrition. It never computes a post-entry return or invents a strategy variant.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
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

from historical_store import HistoricalDayStore, expand_bar
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from strategy_engine import load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-champion-input-readiness-2026-07-19-v1"
SOURCE_DATASET_ID = "dataset-champion-input-fidelity-2026-07-19-v2"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_fidelity"
    / "manifests"
    / "dataset-champion-input-fidelity-2026-07-19-v2-3098b32480cc59ce0a51624a84db2c0f194a518341f12824e0e615583809b3a5.json"
)
CLEAN_TRIGGER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity"
    / "manifests"
    / "dataset-selected-candidate-fidelity-2026-07-19-v1-38f74c64c6f3f86d3cefbe656efd075345c21e7fcc22329936e5c5e78c2a7dcd.json"
)
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "session-calendar-2025-12-through-2026-06.json"
)
SPLIT_ATTESTATION_PATH = (
    PROJECT_ROOT / "historical_batches" / "scanner_replay" / "split-actions-source.json"
)
SPLIT_ACTIONS_PATH = (
    PROJECT_ROOT / "learning_runs" / "scanner_replay" / "splits.json.gz"
)
STRATEGY_CONFIG_PATH = PROJECT_ROOT / "strategy_config.toml"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "champion_input_readiness" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_readiness"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-champion-input-readiness.json"
)
QUOTE_SIZE_SOURCE_URL = "https://docs.alpaca.markets/us/v1.1/changelog?page=3"
QUOTE_SIZE_SHARES_EFFECTIVE = date(2025, 11, 3)


class ChampionInputReadinessError(RuntimeError):
    """Frozen evidence cannot support a deterministic readiness result."""


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


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionInputReadinessError(f"cannot read {path}: {exc}") from exc


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionInputReadinessError(f"cannot read {path}: {exc}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(buffer.getvalue())
    os.replace(temporary, path)


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ChampionInputReadinessError(
            f"public evidence path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "champion_input_readiness" / DATASET_ID


def _private_result_path(store_root: Path) -> Path:
    return _private_root(store_root) / "readiness-index.json.gz"


def _source_paths(store_root: Path) -> dict[str, Path]:
    champion = store_root / "_derived" / "champion_input_fidelity" / SOURCE_DATASET_ID
    clean = (
        store_root
        / "_derived"
        / "selected_candidate_fidelity"
        / "dataset-selected-candidate-fidelity-2026-07-19-v1"
    )
    return {
        "selection": champion / "selection.json.gz",
        "champion_evidence": champion / "evidence-index.json.gz",
        "clean_triggers": clean / "clean-trigger-index.json.gz",
    }


def _strategy_rules() -> dict[str, Any]:
    config = load_config(STRATEGY_CONFIG_PATH)
    return {
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "maximum_quote_age_seconds": float(
            config.raw["execution"]["maximum_quote_age_seconds"]
        ),
        "maximum_median_spread_fraction": float(
            config.raw["execution"]["maximum_median_spread_fraction"]
        ),
        "maximum_single_spread_fraction": float(
            config.raw["execution"]["maximum_single_spread_fraction"]
        ),
        "maximum_entry_chase_fraction": float(
            config.raw["execution"]["maximum_entry_chase_fraction"]
        ),
        "maximum_depth_participation_fraction": float(
            config.raw["execution"]["maximum_depth_participation_fraction"]
        ),
        "maximum_recent_volume_participation_fraction": float(
            config.raw["execution"]["maximum_recent_volume_participation_fraction"]
        ),
        "atr_stop_fraction": float(config.raw["risk"]["atr_stop_fraction"]),
        "maximum_stop_fraction": float(config.raw["risk"]["maximum_stop_fraction"]),
        "minimum_resistance_room_fraction": float(
            config.raw["risk"]["minimum_resistance_room_fraction"]
        ),
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
        clean_manifest = load_frozen_dataset_contract(CLEAN_TRIGGER_MANIFEST)
    except LearningDataError as exc:
        raise ChampionInputReadinessError(str(exc)) from exc
    paths = _source_paths(store.root)
    selection = _read_gzip(paths["selection"])
    champion = _read_gzip(paths["champion_evidence"])
    clean = _read_gzip(paths["clean_triggers"])
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise ChampionInputReadinessError("unexpected champion-fidelity source")
    if champion.get("status") != "COLLECTION_COMPLETE":
        raise ChampionInputReadinessError("champion input source is incomplete")
    if clean.get("status") != "CLEAN_TRIGGER_INSPECTION_COMPLETE":
        raise ChampionInputReadinessError("clean-trigger source is incomplete")
    if int(selection.get("selected_pair_count", -1)) != 389:
        raise ChampionInputReadinessError("source selection is not the exact 389 pairs")
    split_attestation = _read_json(SPLIT_ATTESTATION_PATH)
    split_artifact = split_attestation.get("artifact", {})
    if _sha256_file(SPLIT_ACTIONS_PATH) != split_artifact.get("sha256"):
        raise ChampionInputReadinessError("split artifact differs from its attestation")
    rules = _strategy_rules()
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                _repo_path(CLEAN_TRIGGER_MANIFEST),
                "CHAMPION_INPUT_FIDELITY.md",
                "STRATEGY_LEARNING_EXECUTION_PLAN.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "clean_trigger_manifest_sha256": clean_manifest["manifest_sha256"],
            "selected_pair_count": 389,
            "private_selection_content_sha256": _sha256_json(selection),
            "source_artifacts": {
                name: _sha256_file(path) for name, path in sorted(paths.items())
            },
            "calendar_sha256": _sha256_file(CALENDAR_PATH),
            "split_actions_sha256": _sha256_file(SPLIT_ACTIONS_PATH),
            "strategy_config_sha256": _sha256_file(STRATEGY_CONFIG_PATH),
            "strategy_rules_hash": rules["rules_hash"],
            "strategy_version": rules["strategy_version"],
            "symbols_and_rows_public": False,
        },
        "calculation_contract": {
            "builder_sha256": _sha256_file(Path(__file__)),
            "quote_size_source_url": QUOTE_SIZE_SOURCE_URL,
            "quote_size_in_shares_effective": QUOTE_SIZE_SHARES_EFFECTIVE.isoformat(),
            "quote_snapshot_offsets_seconds": [0, 5, 10],
            "recent_volume": "last fully completed real one-minute bar at final snapshot",
            "vwap": "fully completed provider one-minute bars at final snapshot only",
            "benchmark_reject": (
                "both SPY and QQQ are below falling completed-bar VWAP and their "
                "last completed bar makes a five-bar low"
            ),
            "candidate_relative_strength": (
                "candidate return from open to final ask exceeds both benchmark "
                "returns from open to last completed close"
            ),
            "resistance_proxy": (
                "nearest split-adjusted prior-15-session daily high above final ask"
            ),
            "invalidation_proxy": "opening-range low with the frozen 0.10 ATR floor",
            "target_outcomes_observed_or_derived": False,
            "strategy_variant_invented": False,
            "missing_inputs_default_favorable": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def quote_readiness(
    snapshots: Sequence[Mapping[str, Any]],
    opening_high: float,
    rules: Mapping[str, Any],
) -> dict[str, Any]:
    if len(snapshots) != 3:
        return {
            "three_snapshots": False,
            "basic_fresh_uncrossed": False,
            "spread_pass": False,
            "chase_pass": False,
            "median_spread_fraction": None,
            "maximum_spread_fraction": None,
            "final_ask": None,
            "minimum_visible_ask_depth_shares": None,
        }
    spreads: list[float] = []
    basic = True
    for snapshot in snapshots:
        bid = float(snapshot.get("bid", 0))
        ask = float(snapshot.get("ask", 0))
        age = float(snapshot.get("age_seconds", math.inf))
        if bid <= 0 or ask <= bid or not 0 <= age <= rules["maximum_quote_age_seconds"]:
            basic = False
        midpoint = (bid + ask) / 2
        spreads.append(
            (ask - bid) / midpoint if midpoint > 0 and ask >= bid else math.inf
        )
    median_spread = statistics.median(spreads)
    maximum_spread = max(spreads)
    final_ask = float(snapshots[-1]["ask"])
    spread_pass = (
        basic
        and median_spread <= rules["maximum_median_spread_fraction"]
        and maximum_spread <= rules["maximum_single_spread_fraction"]
    )
    chase_pass = basic and opening_high <= final_ask <= opening_high * (
        1 + rules["maximum_entry_chase_fraction"]
    )
    return {
        "three_snapshots": True,
        "basic_fresh_uncrossed": basic,
        "spread_pass": spread_pass,
        "chase_pass": chase_pass,
        "median_spread_fraction": median_spread,
        "maximum_spread_fraction": maximum_spread,
        "final_ask": final_ask,
        "minimum_visible_ask_depth_shares": min(
            int(snapshot.get("ask_size", 0)) for snapshot in snapshots
        ),
    }


def _vwap(rows: Sequence[Mapping[str, Any]]) -> float | None:
    total_volume = sum(int(row.get("volume", 0)) for row in rows)
    if total_volume <= 0:
        return None
    total = 0.0
    for row in rows:
        volume = int(row.get("volume", 0))
        price = float(row.get("wap") or 0)
        if price <= 0:
            price = (float(row["high"]) + float(row["low"]) + float(row["close"])) / 3
        total += price * volume
    return total / total_volume


def completed_bar_state(
    rows: Sequence[Mapping[str, Any]], cutoff: datetime
) -> dict[str, Any] | None:
    completed = sorted(
        (
            row
            for row in rows
            if datetime.fromisoformat(str(row["time_et"])) + timedelta(minutes=1)
            <= cutoff
        ),
        key=lambda row: str(row["time_et"]),
    )
    if not completed:
        return None
    current_vwap = _vwap(completed)
    prior_vwap = _vwap(completed[:-5] or completed[:1])
    if current_vwap is None or prior_vwap is None:
        return None
    recent = completed[-5:]
    return {
        "completed_bar_count": len(completed),
        "last_close": float(completed[-1]["close"]),
        "last_completed_1m_volume": int(completed[-1].get("volume", 0)),
        "vwap": current_vwap,
        "vwap_flat_or_rising": current_vwap >= prior_vwap,
        "return_from_open": float(completed[-1]["close"]) / float(completed[0]["open"])
        - 1,
        "last_bar_makes_five_bar_low": (
            len(recent) == 5
            and float(recent[-1]["low"])
            <= min(float(row["low"]) for row in recent[:-1])
        ),
    }


def benchmark_alignment(
    candidate_open: float,
    candidate_price: float,
    benchmarks: Mapping[str, Mapping[str, Any] | None],
) -> dict[str, Any]:
    if set(benchmarks) != {"SPY", "QQQ"} or any(
        value is None for value in benchmarks.values()
    ):
        return {
            "available": False,
            "benchmark_supportive": False,
            "candidate_outperforms_both": False,
            "both_benchmarks_unsupportive": None,
        }
    typed = {key: value for key, value in benchmarks.items() if value is not None}
    unsupportive = {
        key: (
            float(value["last_close"]) < float(value["vwap"])
            and value["vwap_flat_or_rising"] is False
            and value["last_bar_makes_five_bar_low"] is True
        )
        for key, value in typed.items()
    }
    candidate_return = candidate_price / candidate_open - 1
    outperforms = all(
        candidate_return > float(value["return_from_open"]) for value in typed.values()
    )
    both_unsupportive = all(unsupportive.values())
    return {
        "available": True,
        "benchmark_supportive": not both_unsupportive,
        "candidate_outperforms_both": outperforms,
        "both_benchmarks_unsupportive": both_unsupportive,
        "candidate_return_from_open": candidate_return,
        "benchmark_returns": {
            key: float(value["return_from_open"]) for key, value in typed.items()
        },
    }


def _bar_rows(
    store: HistoricalDayStore, symbol: str, day: str, timeframe: str
) -> list[dict[str, Any]] | None:
    dataset = store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe=timeframe,
        providers=("alpaca",),
        require_complete=True,
        feed="sip",
        adjustment="raw",
    )
    if dataset is None:
        return None
    return [expand_bar(row) for row in dataset["rows"]]


def _load_split_actions() -> dict[str, list[dict[str, Any]]]:
    rows = _read_gzip(SPLIT_ACTIONS_PATH)
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        normalized = dict(row)
        normalized["execution_date"] = date.fromisoformat(str(row["execution_date"]))
        grouped[str(row["ticker"]).upper()].append(normalized)
    return dict(grouped)


def _split_factor(
    symbol: str,
    observed_day: str,
    target_day: str,
    actions: Mapping[str, Sequence[Mapping[str, Any]]],
) -> float:
    observed = date.fromisoformat(observed_day)
    target = date.fromisoformat(target_day)
    factor = 1.0
    for event in actions.get(symbol, ()):
        if observed < event["execution_date"] <= target:
            factor *= float(event["split_from"]) / float(event["split_to"])
    return factor


def resistance_proxy(
    store: HistoricalDayStore,
    symbol: str,
    target_day: str,
    entry: float,
    prior_days: Sequence[str],
    split_actions: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    highs: list[float] = []
    for prior_day in prior_days:
        rows = _bar_rows(store, symbol, prior_day, "15m")
        if not rows:
            return {"available": False, "reason": "missing_prior_session_bars"}
        factor = _split_factor(symbol, prior_day, target_day, split_actions)
        highs.append(max(float(row["high"]) for row in rows) * factor)
    overhead = sorted(value for value in highs if value > entry)
    if not overhead:
        return {
            "available": True,
            "known_overhead_high": None,
            "room_fraction": None,
            "pass": None,
            "reason": "no_prior_15_session_daily_high_above_entry",
        }
    nearest = overhead[0]
    room = nearest / entry - 1
    return {
        "available": True,
        "known_overhead_high": nearest,
        "room_fraction": room,
        "pass": None,
        "reason": "conservative_proxy_not_confirmed_technical_resistance",
    }


def _prior_days(target_day: str, calendar: Sequence[str], count: int = 15) -> list[str]:
    try:
        index = list(calendar).index(target_day)
    except ValueError as exc:
        raise ChampionInputReadinessError(f"calendar lacks {target_day}") from exc
    if index < count:
        raise ChampionInputReadinessError(f"calendar lacks lookback for {target_day}")
    return list(calendar[index - count : index])


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Path]]:
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise ChampionInputReadinessError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChampionInputReadinessError("unexpected readiness dataset")
    contract = manifest["selection_contract"]
    paths = _source_paths(store.root)
    for name, path in paths.items():
        if _sha256_file(path) != contract["source_artifacts"].get(name):
            raise ChampionInputReadinessError(f"source artifact changed: {name}")
    if _sha256_file(Path(__file__)) != manifest["calculation_contract"].get(
        "builder_sha256"
    ):
        raise ChampionInputReadinessError("builder differs from frozen contract")
    if _sha256_file(STRATEGY_CONFIG_PATH) != contract.get("strategy_config_sha256"):
        raise ChampionInputReadinessError("production strategy config changed")
    if _strategy_rules()["rules_hash"] != contract.get("strategy_rules_hash"):
        raise ChampionInputReadinessError("production rules hash changed")
    return manifest, paths


def build(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, paths = _verify_contract(manifest_path, store)
    selection = _read_gzip(paths["selection"])
    champion = _read_gzip(paths["champion_evidence"])
    clean = _read_gzip(paths["clean_triggers"])
    catalyst_by_key = {
        (str(row["date"]), str(row["symbol"])): row
        for row in champion["catalyst_records"]
    }
    halt_by_key = {
        (str(row["date"]), str(row["symbol"])): row for row in champion["halt_records"]
    }
    trigger_by_key = {
        (str(row["date"]), str(row["symbol"])): row for row in clean["records"]
    }
    calendar = _read_json(CALENDAR_PATH)
    split_actions = _load_split_actions()
    rules = _strategy_rules()
    records: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    cascade = Counter()
    for pair in selection["pairs"]:
        key = (str(pair["date"]), str(pair["symbol"]))
        catalyst = catalyst_by_key[key]
        halt = halt_by_key[key]
        trigger = trigger_by_key.get(key)
        positive = catalyst["verified_positive_direction"] is True
        material = catalyst["verified_material_catalyst"] is True
        conflict = catalyst["dilution_or_negative_conflict"] is True
        clean_cross = bool(trigger and isinstance(trigger.get("clean_cross"), Mapping))
        snapshots = trigger.get("quote_snapshots", []) if trigger else []
        quote = quote_readiness(
            snapshots, float(pair["scanner_fields"]["opening_high"]), rules
        )
        halt_clear = halt.get("halt_risk") is False if clean_cross else False
        final_snapshot_at = (
            datetime.fromisoformat(str(snapshots[-1]["target_at_et"]))
            if quote["three_snapshots"]
            else None
        )
        candidate_state = None
        benchmark_states: dict[str, dict[str, Any] | None] = {"SPY": None, "QQQ": None}
        market = {
            "available": False,
            "benchmark_supportive": False,
            "candidate_outperforms_both": False,
            "both_benchmarks_unsupportive": None,
        }
        liquidity = {
            "available": False,
            "visible_quantity_cap": None,
            "recent_volume": None,
        }
        resistance = {"available": False, "reason": "no_final_entry_ask"}
        stop = {"available": False, "reason": "no_final_entry_ask"}
        if final_snapshot_at is not None and quote["final_ask"] is not None:
            rows = _bar_rows(store, key[1], key[0], "1m")
            if rows is not None:
                candidate_state = completed_bar_state(rows, final_snapshot_at)
            for benchmark in ("SPY", "QQQ"):
                rows = _bar_rows(store, benchmark, key[0], "1m")
                if rows is not None:
                    benchmark_states[benchmark] = completed_bar_state(
                        rows, final_snapshot_at
                    )
            market = benchmark_alignment(
                float(pair["scanner_fields"]["open_price"]),
                float(quote["final_ask"]),
                benchmark_states,
            )
            if candidate_state is not None:
                recent_volume = int(candidate_state["last_completed_1m_volume"])
                minimum_ask = int(quote["minimum_visible_ask_depth_shares"] or 0)
                liquidity = {
                    "available": True,
                    "visible_quantity_cap": min(
                        math.floor(
                            minimum_ask * rules["maximum_depth_participation_fraction"]
                        ),
                        math.floor(
                            recent_volume
                            * rules["maximum_recent_volume_participation_fraction"]
                        ),
                    ),
                    "minimum_visible_ask_depth_shares": minimum_ask,
                    "recent_volume": recent_volume,
                    "quote_size_basis": "shares",
                }
            resistance = resistance_proxy(
                store,
                key[1],
                key[0],
                float(quote["final_ask"]),
                _prior_days(key[0], calendar),
                split_actions,
            )
            entry = float(quote["final_ask"])
            opening_low = float(pair["scanner_fields"]["opening_low"])
            atr = float(pair["scanner_fields"]["daily_atr_14"])
            distance = max(rules["atr_stop_fraction"] * atr, entry - opening_low)
            stop = {
                "available": True,
                "invalidation_proxy": opening_low,
                "stop_distance": distance,
                "stop_fraction": distance / entry,
                "within_maximum_fraction": distance / entry
                <= rules["maximum_stop_fraction"],
                "outside_proxy_structure_and_atr_floor": (
                    entry - distance <= opening_low
                    and distance >= rules["atr_stop_fraction"] * atr
                ),
                "exact_production_invalidation_and_noise_resolved": False,
            }
        candidate_above_completed_vwap = bool(
            candidate_state is not None
            and quote["final_ask"] is not None
            and float(quote["final_ask"]) > float(candidate_state["vwap"])
        )
        candidate_vwap_rising = bool(
            candidate_state is not None
            and candidate_state["vwap_flat_or_rising"] is True
        )
        exact_known_gates = {
            "verified_positive_primary_catalyst": positive and not conflict,
            "clean_continuous_cross": clean_cross,
            "three_snapshots": quote["three_snapshots"],
            "basic_fresh_uncrossed": quote["basic_fresh_uncrossed"],
            "spread": quote["spread_pass"],
            "chase": quote["chase_pass"],
            "official_halt_clear": halt_clear,
            "visible_liquidity_positive": bool(
                liquidity["available"]
                and int(liquidity["visible_quantity_cap"] or 0) > 0
            ),
            "above_completed_bar_vwap": candidate_above_completed_vwap,
            "completed_bar_vwap_flat_or_rising": candidate_vwap_rising,
            "benchmark_supportive": market["benchmark_supportive"] is True,
            "candidate_outperforms_spy_qqq": market["candidate_outperforms_both"]
            is True,
        }
        unresolved = {
            "broker_specific_tradability": True,
            "intraminute_vwap_at_final_snapshot": True,
            "confirmed_technical_resistance": True,
            "confirmed_structural_invalidation_and_noise": True,
            "sector_specific_relative_strength": True,
        }
        known_hard_gate_pass = all(
            exact_known_gates[field]
            for field in (
                "verified_positive_primary_catalyst",
                "clean_continuous_cross",
                "three_snapshots",
                "basic_fresh_uncrossed",
                "spread",
                "chase",
                "official_halt_clear",
                "visible_liquidity_positive",
            )
        )
        record = {
            "date": key[0],
            "symbol": key[1],
            "instrument_id": pair["instrument_id"],
            "rank": pair["rank"],
            "catalyst": {
                "disposition": catalyst["disposition"],
                "verified_material": material,
                "verified_positive": positive,
                "conflict": conflict,
            },
            "trigger_available": clean_cross,
            "quote": quote,
            "halt_clear": halt_clear,
            "candidate_completed_bar_state": candidate_state,
            "benchmark_completed_bar_states": benchmark_states,
            "market": market,
            "liquidity": liquidity,
            "resistance_proxy": resistance,
            "stop_proxy": stop,
            "exact_known_gates": exact_known_gates,
            "unresolved_production_inputs": unresolved,
            "known_hard_gate_pass": known_hard_gate_pass,
            "full_champion_input_ready": False,
            "target_outcome_observed_or_derived": False,
        }
        records.append(record)
        counts["selected_pairs"] += 1
        counts["verified_material_catalysts"] += int(material and not conflict)
        counts["verified_positive_catalysts"] += int(positive and not conflict)
        counts["conflict_pairs"] += int(conflict)
        counts["clean_triggers"] += int(clean_cross)
        for name, passed in exact_known_gates.items():
            counts[f"gate_{name}_pass"] += int(passed)
        counts["known_hard_gate_pass"] += int(known_hard_gate_pass)
        counts["full_champion_input_ready"] += 0
    ordered_cascade = [
        ("selected", lambda row: True),
        (
            "verified_positive_primary_catalyst",
            lambda row: row["exact_known_gates"]["verified_positive_primary_catalyst"],
        ),
        (
            "clean_continuous_cross",
            lambda row: row["exact_known_gates"]["clean_continuous_cross"],
        ),
        (
            "basic_fresh_uncrossed",
            lambda row: row["exact_known_gates"]["basic_fresh_uncrossed"],
        ),
        ("spread", lambda row: row["exact_known_gates"]["spread"]),
        ("chase", lambda row: row["exact_known_gates"]["chase"]),
        (
            "official_halt_clear",
            lambda row: row["exact_known_gates"]["official_halt_clear"],
        ),
        (
            "visible_liquidity_positive",
            lambda row: row["exact_known_gates"]["visible_liquidity_positive"],
        ),
    ]
    survivors = list(records)
    for name, predicate in ordered_cascade:
        survivors = [row for row in survivors if predicate(row)]
        cascade[f"after_{name}"] = len(survivors)
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READINESS_JOIN_COMPLETE",
        "updated_at": _timestamp_now(),
        "counts": dict(sorted(counts.items())),
        "cascade": dict(cascade),
        "records": records,
        "errors": [],
        "target_outcomes_observed_or_derived": False,
    }
    private_path = _private_result_path(store.root)
    _write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["status"],
        "counts": private["counts"],
        "cascade": private["cascade"],
        "private_result_sha256": _sha256_file(private_path),
        "symbols_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
        "production_rule_change_earned": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, _paths = _verify_contract(manifest_path, store)
    private_path = _private_result_path(store.root)
    private = _read_gzip(private_path)
    counts = private.get("counts", {})
    complete = (
        private.get("manifest_sha256") == manifest["manifest_sha256"]
        and private.get("status") == "READINESS_JOIN_COMPLETE"
        and counts.get("selected_pairs") == 389
        and private.get("target_outcomes_observed_or_derived") is False
        and private.get("errors") == []
    )
    if not complete:
        raise ChampionInputReadinessError("readiness result is incomplete")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": counts,
        "cascade": private["cascade"],
        "private_result_sha256": _sha256_file(private_path),
        "findings": {
            "known_hard_gate_survivors": counts["known_hard_gate_pass"],
            "full_champion_input_ready": counts["full_champion_input_ready"],
            "target_outcomes_observed_or_derived": False,
            "unchanged_champion_outcome_evaluation_allowed": False,
            "production_rule_change_earned": False,
        },
        "remaining_fidelity_gaps": [
            "non-SEC issuer events and attributed analyst actions need direct evidence",
            "broker-specific tradability remains prospective",
            "intraminute trigger-time VWAP is not reconstructed from completed bars",
            "known overhead daily highs are not confirmed technical resistance",
            "opening-range-low stop geometry is not confirmed ordinary-noise evidence",
            "sector-specific relative strength remains absent",
        ],
        "claim_boundary": (
            "Outcome-blind unchanged-champion input readiness on already-inspected "
            "dates; not alpha, confirmation, promotion, or a strategy variant."
        ),
        "symbols_and_rows_public": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "build", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest": _repo_path(path), **manifest}
        else:
            if args.manifest is None:
                raise ChampionInputReadinessError("--manifest is required")
            if args.command == "build":
                output = build(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_status_path=args.public_status,
                )
            else:
                output = inspect(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_result_path=args.public_result,
                )
    except (ChampionInputReadinessError, LearningDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
