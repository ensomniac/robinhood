"""Independently rebuild the frozen unchanged-champion readiness join."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

from historical_store import HistoricalDayStore, expand_bar
from learning_data import LearningDataError, load_frozen_dataset_contract
from strategy_engine import load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-champion-input-readiness-2026-07-19-v1"
SOURCE_ROOT = "dataset-champion-input-fidelity-2026-07-19-v2"
CLEAN_ROOT = "dataset-selected-candidate-fidelity-2026-07-19-v1"
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "session-calendar-2025-12-through-2026-06.json"
)
SPLIT_ACTIONS_PATH = (
    PROJECT_ROOT / "learning_runs" / "scanner_replay" / "splits.json.gz"
)
DEFAULT_OUTPUT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-champion-input-readiness-inspection.json"
)


class ChampionReadinessInspectionError(RuntimeError):
    """Independent readiness reconstruction found a mismatch."""


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChampionReadinessInspectionError(f"cannot read {path}: {exc}") from exc


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _paths(root: Path) -> dict[str, Path]:
    source = root / "_derived" / "champion_input_fidelity" / SOURCE_ROOT
    clean = root / "_derived" / "selected_candidate_fidelity" / CLEAN_ROOT
    return {
        "selection": source / "selection.json.gz",
        "champion_evidence": source / "evidence-index.json.gz",
        "clean_triggers": clean / "clean-trigger-index.json.gz",
        "private": (
            root
            / "_derived"
            / "champion_input_readiness"
            / DATASET_ID
            / "readiness-index.json.gz"
        ),
    }


def _rules() -> dict[str, float]:
    config = load_config()
    return {
        "age": float(config.raw["execution"]["maximum_quote_age_seconds"]),
        "median_spread": float(
            config.raw["execution"]["maximum_median_spread_fraction"]
        ),
        "single_spread": float(
            config.raw["execution"]["maximum_single_spread_fraction"]
        ),
        "chase": float(config.raw["execution"]["maximum_entry_chase_fraction"]),
        "depth": float(config.raw["execution"]["maximum_depth_participation_fraction"]),
        "volume": float(
            config.raw["execution"]["maximum_recent_volume_participation_fraction"]
        ),
        "atr": float(config.raw["risk"]["atr_stop_fraction"]),
        "stop": float(config.raw["risk"]["maximum_stop_fraction"]),
    }


def _quote(
    snapshots: Sequence[Mapping[str, Any]],
    opening_high: float,
    rules: Mapping[str, float],
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
    fractions: list[float] = []
    basic = True
    for row in snapshots:
        bid = float(row.get("bid", 0))
        ask = float(row.get("ask", 0))
        age = float(row.get("age_seconds", math.inf))
        basic &= bid > 0 and ask > bid and 0 <= age <= rules["age"]
        midpoint = (bid + ask) / 2
        fractions.append(
            (ask - bid) / midpoint if midpoint > 0 and ask >= bid else math.inf
        )
    median = statistics.median(fractions)
    maximum = max(fractions)
    final_ask = float(snapshots[-1]["ask"])
    return {
        "three_snapshots": True,
        "basic_fresh_uncrossed": bool(basic),
        "spread_pass": bool(
            basic
            and median <= rules["median_spread"]
            and maximum <= rules["single_spread"]
        ),
        "chase_pass": bool(
            basic and opening_high <= final_ask <= opening_high * (1 + rules["chase"])
        ),
        "median_spread_fraction": median,
        "maximum_spread_fraction": maximum,
        "final_ask": final_ask,
        "minimum_visible_ask_depth_shares": min(
            int(row.get("ask_size", 0)) for row in snapshots
        ),
    }


def _bars(
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
    return None if dataset is None else [expand_bar(row) for row in dataset["rows"]]


def _vwap(rows: Sequence[Mapping[str, Any]]) -> float | None:
    volume = sum(int(row.get("volume", 0)) for row in rows)
    if volume <= 0:
        return None
    total = 0.0
    for row in rows:
        row_volume = int(row.get("volume", 0))
        price = float(row.get("wap") or 0)
        if price <= 0:
            price = (float(row["high"]) + float(row["low"]) + float(row["close"])) / 3
        total += price * row_volume
    return total / volume


def _completed(
    rows: Sequence[Mapping[str, Any]], cutoff: datetime
) -> dict[str, Any] | None:
    available = sorted(
        (
            row
            for row in rows
            if datetime.fromisoformat(str(row["time_et"])) + timedelta(minutes=1)
            <= cutoff
        ),
        key=lambda row: str(row["time_et"]),
    )
    if not available:
        return None
    current = _vwap(available)
    prior = _vwap(available[:-5] or available[:1])
    if current is None or prior is None:
        return None
    last_five = available[-5:]
    return {
        "completed_bar_count": len(available),
        "last_close": float(available[-1]["close"]),
        "last_completed_1m_volume": int(available[-1].get("volume", 0)),
        "vwap": current,
        "vwap_flat_or_rising": current >= prior,
        "return_from_open": float(available[-1]["close"]) / float(available[0]["open"])
        - 1,
        "last_bar_makes_five_bar_low": (
            len(last_five) == 5
            and float(last_five[-1]["low"])
            <= min(float(row["low"]) for row in last_five[:-1])
        ),
    }


def _market(
    open_price: float,
    final_ask: float,
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
    unsupportive = [
        float(value["last_close"]) < float(value["vwap"])
        and value["vwap_flat_or_rising"] is False
        and value["last_bar_makes_five_bar_low"] is True
        for value in typed.values()
    ]
    candidate_return = final_ask / open_price - 1
    return {
        "available": True,
        "benchmark_supportive": not all(unsupportive),
        "candidate_outperforms_both": all(
            candidate_return > float(value["return_from_open"])
            for value in typed.values()
        ),
        "both_benchmarks_unsupportive": all(unsupportive),
        "candidate_return_from_open": candidate_return,
        "benchmark_returns": {
            key: float(value["return_from_open"]) for key, value in typed.items()
        },
    }


def _split_actions() -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for source in _read_gzip(SPLIT_ACTIONS_PATH):
        row = dict(source)
        row["execution_date"] = date.fromisoformat(str(source["execution_date"]))
        grouped[str(source["ticker"]).upper()].append(row)
    return dict(grouped)


def _factor(
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


def _resistance(
    store: HistoricalDayStore,
    symbol: str,
    target_day: str,
    final_ask: float,
    prior_days: Sequence[str],
    actions: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    highs: list[float] = []
    for day in prior_days:
        rows = _bars(store, symbol, day, "15m")
        if not rows:
            return {"available": False, "reason": "missing_prior_session_bars"}
        highs.append(
            max(float(row["high"]) for row in rows)
            * _factor(symbol, day, target_day, actions)
        )
    overhead = sorted(value for value in highs if value > final_ask)
    if not overhead:
        return {
            "available": True,
            "known_overhead_high": None,
            "room_fraction": None,
            "pass": None,
            "reason": "no_prior_15_session_daily_high_above_entry",
        }
    nearest = overhead[0]
    return {
        "available": True,
        "known_overhead_high": nearest,
        "room_fraction": nearest / final_ask - 1,
        "pass": None,
        "reason": "conservative_proxy_not_confirmed_technical_resistance",
    }


def _assert_same(observed: Any, expected: Any, field: str) -> None:
    if observed != expected:
        raise ChampionReadinessInspectionError(f"independent mismatch: {field}")


def inspect(
    *, manifest_path: Path, env_path: Path, output_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise ChampionReadinessInspectionError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChampionReadinessInspectionError("unexpected readiness dataset")
    paths = _paths(store.root)
    frozen_hashes = manifest["selection_contract"]["source_artifacts"]
    for name in ("selection", "champion_evidence", "clean_triggers"):
        if _sha256_file(paths[name]) != frozen_hashes.get(name):
            raise ChampionReadinessInspectionError(f"source hash changed: {name}")
    selection = _read_gzip(paths["selection"])
    champion = _read_gzip(paths["champion_evidence"])
    clean = _read_gzip(paths["clean_triggers"])
    private = _read_gzip(paths["private"])
    if private.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise ChampionReadinessInspectionError("private result is not manifest-bound")
    private_by_key = {
        (str(row["date"]), str(row["symbol"])): row for row in private["records"]
    }
    catalyst_by_key = {
        (str(row["date"]), str(row["symbol"])): row
        for row in champion["catalyst_records"]
    }
    halt_by_key = {
        (str(row["date"]), str(row["symbol"])): row for row in champion["halt_records"]
    }
    clean_by_key = {
        (str(row["date"]), str(row["symbol"])): row for row in clean["records"]
    }
    calendar = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    calendar_positions = {day: index for index, day in enumerate(calendar)}
    actions = _split_actions()
    rules = _rules()
    counts: Counter[str] = Counter()
    diagnostics: Counter[str] = Counter()
    rebuilt_records: list[dict[str, Any]] = []
    for pair in selection["pairs"]:
        key = (str(pair["date"]), str(pair["symbol"]))
        actual = private_by_key.get(key)
        if actual is None:
            raise ChampionReadinessInspectionError("private pair is missing")
        catalyst = catalyst_by_key[key]
        halt = halt_by_key[key]
        trigger = clean_by_key.get(key)
        clean_cross = bool(trigger and isinstance(trigger.get("clean_cross"), Mapping))
        snapshots = trigger.get("quote_snapshots", []) if trigger else []
        quote = _quote(snapshots, float(pair["scanner_fields"]["opening_high"]), rules)
        halt_clear = halt.get("halt_risk") is False if clean_cross else False
        candidate_state = None
        benchmarks: dict[str, dict[str, Any] | None] = {"SPY": None, "QQQ": None}
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
        if quote["three_snapshots"] and quote["final_ask"] is not None:
            cutoff = datetime.fromisoformat(str(snapshots[-1]["target_at_et"]))
            rows = _bars(store, key[1], key[0], "1m")
            candidate_state = _completed(rows, cutoff) if rows is not None else None
            for benchmark in ("SPY", "QQQ"):
                rows = _bars(store, benchmark, key[0], "1m")
                benchmarks[benchmark] = (
                    _completed(rows, cutoff) if rows is not None else None
                )
            market = _market(
                float(pair["scanner_fields"]["open_price"]),
                float(quote["final_ask"]),
                benchmarks,
            )
            if candidate_state is not None:
                visible = int(quote["minimum_visible_ask_depth_shares"] or 0)
                recent = int(candidate_state["last_completed_1m_volume"])
                liquidity = {
                    "available": True,
                    "visible_quantity_cap": min(
                        math.floor(visible * rules["depth"]),
                        math.floor(recent * rules["volume"]),
                    ),
                    "minimum_visible_ask_depth_shares": visible,
                    "recent_volume": recent,
                    "quote_size_basis": "shares",
                }
            position = calendar_positions[key[0]]
            resistance = _resistance(
                store,
                key[1],
                key[0],
                float(quote["final_ask"]),
                calendar[position - 15 : position],
                actions,
            )
            entry = float(quote["final_ask"])
            opening_low = float(pair["scanner_fields"]["opening_low"])
            atr = float(pair["scanner_fields"]["daily_atr_14"])
            distance = max(rules["atr"] * atr, entry - opening_low)
            stop = {
                "available": True,
                "invalidation_proxy": opening_low,
                "stop_distance": distance,
                "stop_fraction": distance / entry,
                "within_maximum_fraction": distance / entry <= rules["stop"],
                "outside_proxy_structure_and_atr_floor": (
                    entry - distance <= opening_low and distance >= rules["atr"] * atr
                ),
                "exact_production_invalidation_and_noise_resolved": False,
            }
        positive = catalyst["verified_positive_direction"] is True
        conflict = catalyst["dilution_or_negative_conflict"] is True
        exact = {
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
            "above_completed_bar_vwap": bool(
                candidate_state is not None
                and quote["final_ask"] is not None
                and float(quote["final_ask"]) > float(candidate_state["vwap"])
            ),
            "completed_bar_vwap_flat_or_rising": bool(
                candidate_state is not None
                and candidate_state["vwap_flat_or_rising"] is True
            ),
            "benchmark_supportive": market["benchmark_supportive"] is True,
            "candidate_outperforms_spy_qqq": market["candidate_outperforms_both"]
            is True,
        }
        known_pass = all(
            exact[field]
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
        for field, expected in (
            ("quote", quote),
            ("halt_clear", halt_clear),
            ("candidate_completed_bar_state", candidate_state),
            ("benchmark_completed_bar_states", benchmarks),
            ("market", market),
            ("liquidity", liquidity),
            ("resistance_proxy", resistance),
            ("stop_proxy", stop),
            ("exact_known_gates", exact),
            ("known_hard_gate_pass", known_pass),
        ):
            _assert_same(actual[field], expected, field)
        if (
            actual.get("target_outcome_observed_or_derived") is not False
            or actual.get("full_champion_input_ready") is not False
            or not all(actual["unresolved_production_inputs"].values())
        ):
            raise ChampionReadinessInspectionError("claim boundary was violated")
        counts["selected_pairs"] += 1
        counts["verified_material_catalysts"] += int(
            catalyst["verified_material_catalyst"] is True and not conflict
        )
        counts["verified_positive_catalysts"] += int(positive and not conflict)
        counts["conflict_pairs"] += int(conflict)
        counts["clean_triggers"] += int(clean_cross)
        for name, passed in exact.items():
            counts[f"gate_{name}_pass"] += int(passed)
        counts["known_hard_gate_pass"] += int(known_pass)
        counts["full_champion_input_ready"] += 0
        execution_geometry = all(
            exact[field]
            for field in (
                "clean_continuous_cross",
                "three_snapshots",
                "basic_fresh_uncrossed",
                "spread",
                "chase",
                "official_halt_clear",
                "visible_liquidity_positive",
            )
        )
        diagnostics["execution_geometry_pass"] += int(execution_geometry)
        diagnostics["execution_and_completed_bar_market_pass"] += int(
            execution_geometry
            and exact["above_completed_bar_vwap"]
            and exact["completed_bar_vwap_flat_or_rising"]
            and exact["benchmark_supportive"]
            and exact["candidate_outperforms_spy_qqq"]
        )
        diagnostics["resistance_proxy_available"] += int(resistance["available"])
        diagnostics["resistance_proxy_known_overhead"] += int(
            resistance.get("known_overhead_high") is not None
        )
        diagnostics["resistance_proxy_no_overhead_in_15_sessions"] += int(
            resistance.get("reason") == "no_prior_15_session_daily_high_above_entry"
        )
        diagnostics["resistance_proxy_room_at_least_2_2_percent"] += int(
            resistance.get("room_fraction") is not None
            and float(resistance["room_fraction"]) >= 0.022
        )
        diagnostics["opening_low_stop_proxy_available"] += int(stop["available"])
        diagnostics["opening_low_stop_proxy_within_0_8_percent"] += int(
            stop.get("within_maximum_fraction") is True
        )
        diagnostics["all_measured_non_catalyst_proxies_pass"] += int(
            execution_geometry
            and exact["above_completed_bar_vwap"]
            and exact["completed_bar_vwap_flat_or_rising"]
            and exact["benchmark_supportive"]
            and exact["candidate_outperforms_spy_qqq"]
            and stop.get("within_maximum_fraction") is True
            and resistance.get("room_fraction") is not None
            and float(resistance["room_fraction"]) >= 0.022
        )
        rebuilt_records.append(actual)
    _assert_same(dict(sorted(counts.items())), private["counts"], "aggregate counts")
    survivors = rebuilt_records
    cascade: dict[str, int] = {}
    for name in (
        "selected",
        "verified_positive_primary_catalyst",
        "clean_continuous_cross",
        "basic_fresh_uncrossed",
        "spread",
        "chase",
        "official_halt_clear",
        "visible_liquidity_positive",
    ):
        if name != "selected":
            survivors = [row for row in survivors if row["exact_known_gates"][name]]
        cascade[f"after_{name}"] = len(survivors)
    _assert_same(cascade, private["cascade"], "cascade")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "independently_inspected": True,
        "inspection_implementation_sha256": _sha256_file(Path(__file__)),
        "private_result_sha256": _sha256_file(paths["private"]),
        "counts": dict(sorted(counts.items())),
        "diagnostic_counts": dict(sorted(diagnostics.items())),
        "cascade": cascade,
        "checks": {
            "all_frozen_source_hashes_verified": True,
            "all_389_pairs_rejoined": True,
            "quotes_spreads_and_chase_recomputed": True,
            "completed_bar_boundaries_recomputed": True,
            "benchmark_alignment_recomputed": True,
            "visible_liquidity_recomputed": True,
            "split_adjusted_resistance_proxies_recomputed": True,
            "stop_proxies_recomputed": True,
            "target_outcomes_absent": True,
            "aggregate_counts_and_cascade_match": True,
            "symbols_and_rows_public": False,
        },
        "claim_boundary": (
            "Independent outcome-blind input-readiness reconstruction on "
            "already-inspected dates; not alpha or a strategy change."
        ),
    }
    _write_json(output_path, result)
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    try:
        result = inspect(
            manifest_path=args.manifest,
            env_path=args.env_file,
            output_path=args.output,
        )
    except (ChampionReadinessInspectionError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
