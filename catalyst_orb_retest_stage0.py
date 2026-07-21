"""Run the frozen catalyst ORB-retest Stage 0 screen on preserved evidence."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import equity_gap_continuation_stage0 as continuation
import etf_or_momentum_stage0 as common
from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
SLATE_PATH = common.SLATE_PATH
VARIANT_ID = "catalyst-orb-retest-v1"
VARIANT_ORDINAL = 10
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
OPENING_RANGE_BARS = 5
SIGNAL_START = time(9, 40)
SIGNAL_END = time(11, 0)
RETEST_TOLERANCE = 0.002
DATASET_ID = "dataset-catalyst-orb-retest-stage0-2026-07-21-v1"

SELECTION_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/scanner_expansion_selected_pairs/manifests/"
    "dataset-selected-candidate-contract-2026-07-19-expansion-v1-"
    "369efda1967ad83096e3c26672e349b44bb8d0afd694f391490042a9e22c140b.json"
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results/2026-07-19-catalyst-source-semantics.json"
)
SEC_RESULT = PROJECT_ROOT / "research_results/2026-07-19-catalyst-sec-semantics.json"
ISSUER_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-19-catalyst-issuer-chain-semantics.json"
)
PRIOR_HYPOTHESIS = (
    PROJECT_ROOT
    / "learning/hypotheses/experiment-catalyst-orb-retest-v1-"
    "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105.json"
)
PRIVATE_SELECTION = (
    "_derived/scanner_selected_pairs/"
    "dataset-selected-candidate-contract-2026-07-19-expansion-v1/"
    "selected-pairs.json.gz"
)
PRIVATE_REVIEWS = (
    (
        "_derived/catalyst_source_semantics/"
        "dataset-catalyst-source-semantics-2026-07-19-expansion-v1/"
        "reviewed-result.json.gz",
        SOURCE_RESULT,
        3,
    ),
    (
        "_derived/catalyst_sec_semantics/"
        "dataset-catalyst-sec-semantics-2026-07-19-expansion-v1/"
        "reviewed.json.gz",
        SEC_RESULT,
        5,
    ),
    (
        "_derived/catalyst_issuer_chain_semantics/"
        "dataset-catalyst-issuer-chain-semantics-2026-07-19-expansion-v1/"
        "reviewed.json.gz",
        ISSUER_RESULT,
        0,
    ),
)


class CatalystOrbRetestStage0Error(RuntimeError):
    """The frozen catalyst ORB-retest screen is incomplete or inconsistent."""


def _hash_exact(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    ).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalystOrbRetestStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystOrbRetestStage0Error(f"{path} must contain an object")
    return value


def _load_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalystOrbRetestStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystOrbRetestStage0Error(f"{path} must contain an object")
    return value


def _write_gzip(path: Path, value: Mapping[str, Any]) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode())
        stream.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    matches = [
        row
        for row in _load_json(SLATE_PATH)["variants"]
        if row.get("variant_id") == VARIANT_ID
        and row.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(matches) != 1:
        raise CatalystOrbRetestStage0Error("frozen slate variant is unavailable")
    return dict(matches[0])


def _source_candidates(store: HistoricalDayStore) -> list[dict[str, Any]]:
    selection_manifest = _load_json(SELECTION_MANIFEST)
    selection_path = store.root / PRIVATE_SELECTION
    selection = _load_gzip(selection_path)
    contract = selection_manifest.get("selection_contract")
    if (
        selection_manifest.get("manifest_sha256")
        != common._self_hash(selection_manifest, "manifest_sha256")
        or not isinstance(contract, Mapping)
        or contract.get("private_selection_content_sha256") != _hash_exact(selection)
        or selection.get("selected_pair_count") != 1987
    ):
        raise CatalystOrbRetestStage0Error("preserved scanner selection drifted")
    selected = selection.get("selected_pairs")
    if not isinstance(selected, list):
        raise CatalystOrbRetestStage0Error("preserved scanner pairs are missing")
    by_identity = {
        (str(row.get("date")), str(row.get("symbol"))): row
        for row in selected
        if isinstance(row, Mapping)
    }
    positives: dict[tuple[str, str], dict[str, Any]] = {}
    for relative, public_path, expected_count in PRIVATE_REVIEWS:
        public = _load_json(public_path)
        private_path = store.root / relative
        private = _load_gzip(private_path)
        rows = private.get("rows")
        if (
            public.get("status") != "READY"
            or public.get("inspected") is not True
            or public.get("target_outcomes_observed_or_derived") is not False
            or public.get(
                "verified_positive_pairs",
                public.get("issuer_chain_verified_positive_pairs"),
            )
            != expected_count
            or not isinstance(rows, list)
        ):
            raise CatalystOrbRetestStage0Error("preserved catalyst review drifted")
        positive_rows = [
            row
            for row in rows
            if isinstance(row, Mapping)
            and row.get("terminal_disposition") == "VERIFIED_POSITIVE_PRIMARY"
        ]
        if len({(row.get("date"), row.get("symbol")) for row in positive_rows}) != expected_count:
            raise CatalystOrbRetestStage0Error("positive catalyst count drifted")
        for row in positive_rows:
            identity = (str(row["date"]), str(row["symbol"]))
            source = by_identity.get(identity)
            if source is None:
                raise CatalystOrbRetestStage0Error(
                    "verified catalyst is outside the preserved scanner selection"
                )
            current = {
                "date": identity[0],
                "symbol": identity[1],
                "instrument_id": str(source.get("instrument_id")),
                "primary_exchange": str(source.get("primary_exchange")),
                "rank": int(source.get("rank", 0)),
                "scanner_fields": dict(source.get("scanner_fields", {})),
            }
            if identity in positives and positives[identity] != current:
                raise CatalystOrbRetestStage0Error("positive catalyst identity conflicts")
            positives[identity] = current
    if len(positives) != 8:
        raise CatalystOrbRetestStage0Error("legacy screen must contain exactly 8 positives")
    return [positives[key] for key in sorted(positives)]


def _market_binding(
    store: HistoricalDayStore, candidate: Mapping[str, Any]
) -> dict[str, Any]:
    day = str(candidate["date"])
    symbol = str(candidate["symbol"])
    path = store.path_for(symbol, day)
    dataset = store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="1m",
        providers=("alpaca",),
        require_complete=True,
        adjustment="raw",
    )
    rows = dataset.get("rows") if isinstance(dataset, Mapping) else None
    if not path.is_file() or not isinstance(rows, list) or len(rows) != 390:
        return {"status": "missing"}
    return {
        "status": "available",
        "store_key": path.relative_to(store.root).as_posix(),
        "document_sha256": sha256_file(path),
        "dataset_id": str(dataset["id"]),
        "dataset_sha256": str(dataset["content_sha256"]),
        "rows": len(rows),
    }


def _private_input_path(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived/catalyst_orb_retest_stage0"
        / DATASET_ID
        / "frozen-inputs.json.gz"
    )


def _private_input_graph(store: HistoricalDayStore) -> dict[str, Any]:
    candidates = _source_candidates(store)
    return {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "inputs": [
            {**candidate, "market_input": _market_binding(store, candidate)}
            for candidate in candidates
        ],
        "contains_private_source_identities": True,
        "contains_target_returns": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    variant = _variant()
    graph = _private_input_graph(source)
    inputs = graph["inputs"]
    available = sum(row["market_input"]["status"] == "available" for row in inputs)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 1,
        "variant_ordinal": VARIANT_ORDINAL,
        "variant_id": VARIANT_ID,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "slate_manifest_sha256": _load_json(SLATE_PATH)["manifest_sha256"],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed": True,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_authorized": False,
        "broker_actions_authorized": False,
        "return_evaluation_authorized_before_inspection": False,
        "declared_prior_related_trials": 1,
        "prior_related_trial": {
            "path": PRIOR_HYPOTHESIS.relative_to(PROJECT_ROOT).as_posix(),
            "file_sha256": sha256_file(PRIOR_HYPOTHESIS),
            "rules_reused": False,
            "maturity_evidence_reused": False,
        },
        "source_selection": {
            "route": "preserved catalyst lane legacy screen before resumed source acquisition",
            "candidate_rule": "all exactly source-verified positive common-stock pairs in the exhausted 2026-07-19 legacy source corpus",
            "daily_cap": 1,
            "daily_tie_break": "earliest entry, then frozen scanner rank, then symbol",
            "substitutions": 0,
        },
        "selection_contract": {
            "asset_gate": "point-in-time common equity with 09:30 open strictly above 5 dollars and a source-verified positive catalyst before 09:35 ET",
            "opening_range": "high of completed 09:30 through 09:34 one-minute bars",
            "session_vwap": "cumulative Alpaca SIP bar VWAP weighted by bar volume through the completed decision bar",
            "first_trigger": "first completed 09:40 through 11:00 bar closing strictly above opening-range high and session VWAP",
            "retest": "first later completed bar whose low is within 0.2 percent of opening-range high",
            "rebreak": "first still-later completed bullish bar closing strictly above opening-range high by 11:00",
            "entry": "next observed one-minute open",
            "miss_policy": "missing or invalid market input, absent sequence, or nonpositive stop distance remains in the denominator and cannot signal",
        },
        "outcome_contract": {
            "stop": "retest-bar low",
            "target": "two times raw entry-to-stop distance above raw entry",
            "entry_exit_cost_bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
            "same_interval_ambiguity": "stop_first",
            "stop_gap_fill": "worse of stop and observed bar open",
            "target_gap_fill": "target price without favorable gap improvement",
            "force_flat_et": "15:50:00 at observed bar open",
            "planned_risk_r": "cost-adjusted entry minus cost-adjusted planned stop",
        },
        "stage0_gate": {
            "minimum_closed_signals": 30,
            "minimum_expectancy_r_exclusive": 0,
            "minimum_profit_factor": 1.10,
            "maximum_drawdown_r": 8,
            "require_positive_20bps_total_r": True,
            "maximum_rule_violations": 0,
            "effect": "SURVIVE_TO_REPRESENTATIVE_DEVELOPMENT only; never PILOT_READY",
        },
        "denominator": {
            "legacy_selected_pairs": 1987,
            "source_verified_positive_pairs": len(inputs),
            "candidate_dates": len({row["date"] for row in inputs}),
            "market_inputs_available": available,
            "market_inputs_missing": len(inputs) - available,
            "maximum_closed_signals": len({row["date"] for row in inputs}),
        },
        "private_inputs": {
            "location": "LOCAL_HISTORICAL_DATA_ROOT/_derived/"
            f"catalyst_orb_retest_stage0/{DATASET_ID}/frozen-inputs.json.gz",
            "content_sha256": common._hash(graph),
            "input_count": len(inputs),
            "contains_private_source_identities": True,
            "contains_target_returns": False,
        },
        "source_bindings": {
            path.relative_to(PROJECT_ROOT).as_posix(): sha256_file(path)
            for path in (
                SELECTION_MANIFEST,
                SOURCE_RESULT,
                SEC_RESULT,
                ISSUER_RESULT,
                PRIOR_HYPOTHESIS,
            )
        },
        "private_source_bindings": {
            PRIVATE_SELECTION: sha256_file(source.root / PRIVATE_SELECTION),
            **{
                relative: sha256_file(source.root / relative)
                for relative, _, _ in PRIVATE_REVIEWS
            },
        },
    }
    value["activation_rules_hash"] = common._hash(
        {
            "base_rules_hash": value["base_rules_hash"],
            "source_selection": value["source_selection"],
            "selection_contract": value["selection_contract"],
            "outcome_contract": value["outcome_contract"],
            "stage0_gate": value["stage0_gate"],
        }
    )
    value["manifest_sha256"] = common._self_hash(value, "manifest_sha256")
    return value


def _validate_manifest(value: Mapping[str, Any]) -> None:
    if value.get("manifest_sha256") != common._self_hash(value, "manifest_sha256"):
        raise CatalystOrbRetestStage0Error("activation manifest hash is invalid")
    if (
        value.get("variant_id") != VARIANT_ID
        or value.get("variant_ordinal") != VARIANT_ORDINAL
        or value.get("claim_scope") != "FALSIFICATION_ONLY"
        or value.get("outcomes_previously_accessed") is not True
    ):
        raise CatalystOrbRetestStage0Error("activation identity is invalid")
    for field in (
        "development_evidence_eligible",
        "confirmation_evidence_eligible",
        "provider_requests_authorized",
        "broker_actions_authorized",
        "return_evaluation_authorized_before_inspection",
    ):
        if value.get(field) is not False:
            raise CatalystOrbRetestStage0Error(f"activation {field} must be false")
    denominator = value.get("denominator", {})
    if (
        denominator.get("source_verified_positive_pairs") != 8
        or denominator.get("market_inputs_available") != 8
        or denominator.get("market_inputs_missing") != 0
        or denominator.get("maximum_closed_signals") != 8
    ):
        raise CatalystOrbRetestStage0Error("legacy catalyst denominator drifted")


def _normalize_rows(rows: Any, day: str, symbol: str) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or len(rows) != 390:
        raise CatalystOrbRetestStage0Error(f"{day} {symbol}: expected 390 bars")
    normalized = []
    for raw in rows:
        if not isinstance(raw, Mapping):
            raise CatalystOrbRetestStage0Error(f"{day} {symbol}: malformed bar")
        try:
            observed = datetime.fromisoformat(str(raw["t"]))
            if observed.tzinfo is None:
                raise ValueError
            normalized.append(
                {
                    "time_et": observed.astimezone(EASTERN).time().isoformat(),
                    "open": float(raw["o"]),
                    "high": float(raw["h"]),
                    "low": float(raw["l"]),
                    "close": float(raw["c"]),
                    "volume": int(raw["v"]),
                    "bar_vwap": float(raw["vw"]),
                    "interpolated": bool(raw.get("i", False)),
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CatalystOrbRetestStage0Error(
                f"{day} {symbol}: malformed OHLCV/VWAP"
            ) from exc
    return continuation._validate_bars(normalized, symbol, day)


def _load_inputs(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> list[dict[str, Any]]:
    private_path = _private_input_path(store)
    private = _load_gzip(private_path)
    if (
        common._hash(private) != manifest["private_inputs"]["content_sha256"]
        or private != _private_input_graph(store)
        or private.get("contains_target_returns") is not False
    ):
        raise CatalystOrbRetestStage0Error("private frozen input graph drifted")
    result = []
    for item in private["inputs"]:
        day, symbol = str(item["date"]), str(item["symbol"])
        binding = item.get("market_input")
        path = store.path_for(symbol, day)
        if (
            not isinstance(binding, Mapping)
            or binding.get("status") != "available"
            or binding.get("store_key") != path.relative_to(store.root).as_posix()
            or not path.is_file()
            or binding.get("document_sha256") != sha256_file(path)
        ):
            raise CatalystOrbRetestStage0Error(f"{day} {symbol}: market binding drifted")
        dataset = store.select_dataset(
            symbol,
            day,
            kind="bars",
            channel="trades",
            timeframe="1m",
            providers=("alpaca",),
            require_complete=True,
            adjustment="raw",
        )
        if not isinstance(dataset, Mapping) or (
            binding.get("dataset_id") != dataset.get("id")
            or binding.get("dataset_sha256") != dataset.get("content_sha256")
        ):
            raise CatalystOrbRetestStage0Error(f"{day} {symbol}: dataset drifted")
        result.append({**item, "bars": _normalize_rows(dataset.get("rows"), day, symbol)})
    return result


def inspect_activation(
    path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    recorded = _load_json(path)
    _validate_manifest(recorded)
    source = store or HistoricalDayStore.from_env()
    if recorded != build_manifest(source):
        raise CatalystOrbRetestStage0Error("activation does not exactly rebuild")
    inputs = _load_inputs(recorded, source)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "implementation_sha256": recorded["implementation_sha256"],
        "source_verified_positive_pairs": len(inputs),
        "selected_dates": len({row["date"] for row in inputs}),
        "selected_symbol_sessions": len(inputs),
        "selected_bars": sum(len(row["bars"]) for row in inputs),
        "maximum_closed_signals": recorded["denominator"]["maximum_closed_signals"],
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "return_evaluation_authorized": True,
        "claim_scope": "FALSIFICATION_ONLY",
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _validate_inspection(
    manifest_path: Path, inspection_path: Path, store: HistoricalDayStore
) -> dict[str, Any]:
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ) or recorded != inspect_activation(manifest_path, store):
        raise CatalystOrbRetestStage0Error("activation inspection does not rebuild")
    if recorded.get("return_evaluation_authorized") is not True:
        raise CatalystOrbRetestStage0Error("return evaluation is not authorized")
    return recorded


def _session_vwap(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    numerator = 0.0
    volume_total = 0
    result = []
    for row in rows:
        volume = int(row["volume"])
        numerator += float(row["bar_vwap"]) * volume
        volume_total += volume
        result.append(numerator / volume_total if volume_total else float(row["close"]))
    return result


def _candidate(item: Mapping[str, Any]) -> dict[str, Any]:
    day, symbol = str(item["date"]), str(item["symbol"])
    bars = item["bars"]
    if float(bars[0]["open"]) <= 5:
        return {"date": day, "symbol": symbol, "status": "opening_price_not_above_5"}
    opening_high = max(float(row["high"]) for row in bars[:OPENING_RANGE_BARS])
    vwap = _session_vwap(bars)
    start = next(
        index
        for index, row in enumerate(bars)
        if continuation._bar_time(row) == SIGNAL_START
    )
    end = next(
        index
        for index, row in enumerate(bars)
        if continuation._bar_time(row) == SIGNAL_END
    )
    first_trigger = next(
        (
            index
            for index in range(start, end + 1)
            if float(bars[index]["close"]) > opening_high
            and float(bars[index]["close"]) > vwap[index]
        ),
        None,
    )
    if first_trigger is None:
        return {"date": day, "symbol": symbol, "status": "no_first_trigger"}
    retest = next(
        (
            index
            for index in range(first_trigger + 1, end)
            if abs(float(bars[index]["low"]) / opening_high - 1)
            <= RETEST_TOLERANCE + 1e-12
        ),
        None,
    )
    if retest is None:
        return {"date": day, "symbol": symbol, "status": "no_retest"}
    rebreak = next(
        (
            index
            for index in range(retest + 1, end + 1)
            if float(bars[index]["close"]) > float(bars[index]["open"])
            and float(bars[index]["close"]) > opening_high
        ),
        None,
    )
    if rebreak is None or rebreak + 1 >= len(bars):
        return {"date": day, "symbol": symbol, "status": "no_rebreak"}
    entry_index = rebreak + 1
    entry_open = float(bars[entry_index]["open"])
    stop = float(bars[retest]["low"])
    if entry_open <= stop:
        return {"date": day, "symbol": symbol, "status": "nonpositive_stop_distance"}
    return {
        "date": day,
        "symbol": symbol,
        "rank": int(item["rank"]),
        "status": "executable",
        "first_trigger_time_et": bars[first_trigger]["time_et"],
        "retest_time_et": bars[retest]["time_et"],
        "trigger_time_et": bars[rebreak]["time_et"],
        "entry_time_et": bars[entry_index]["time_et"],
        "entry_index": entry_index,
        "entry_open": entry_open,
        "stop": stop,
    }


def build_result(
    manifest_path: Path,
    inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    inspection = _validate_inspection(manifest_path, inspection_path, source)
    if require_published:
        common._require_published((manifest_path, inspection_path))
    inputs = _load_inputs(manifest, source)
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    disposition: Counter[str] = Counter()
    for item in inputs:
        candidate = _candidate(item)
        disposition[candidate["status"]] += 1
        if candidate["status"] == "executable":
            by_day[str(item["date"])].append({**candidate, "bars": item["bars"]})
    records = []
    for day in sorted({str(item["date"]) for item in inputs}):
        candidates = sorted(
            by_day.get(day, []),
            key=lambda row: (row["entry_time_et"], row["rank"], row["symbol"]),
        )
        if not candidates:
            continue
        selected = candidates[0]
        disposition["selected"] += 1
        disposition["not_selected_daily_cap"] += len(candidates) - 1
        outcomes = {
            str(cost): continuation._trade_outcome(selected, selected["bars"], cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "date": day,
                "symbol": selected["symbol"],
                "first_trigger_time_et": selected["first_trigger_time_et"],
                "retest_time_et": selected["retest_time_et"],
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
    primary_metrics = common._metrics([float(row["net_r"]) for row in records])
    stress = {
        "10": common._metrics([float(row["stress_10bps_r"]) for row in records]),
        "20": common._metrics([float(row["stress_20bps_r"]) for row in records]),
    }
    gate = manifest["stage0_gate"]
    blockers = []
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
    if stress["20"]["total_r"] <= 0:
        blockers.append("20 bps-per-side total R is not positive")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-falsification",
        "variant_id": VARIANT_ID,
        "strategy_version": manifest["strategy_version"],
        "mechanism_family": manifest["mechanism_family"],
        "base_rules_hash": manifest["base_rules_hash"],
        "activation_rules_hash": manifest["activation_rules_hash"],
        "manifest_sha256": manifest["manifest_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": manifest["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "denominator": {
            "legacy_selected_pairs": manifest["denominator"]["legacy_selected_pairs"],
            "source_verified_positive_pairs": len(inputs),
            "dates": len({str(row["date"]) for row in inputs}),
            "closed_signals": len(records),
            "no_trade_dates": len({str(row["date"]) for row in inputs}) - len(records),
            "rule_violations": 0,
        },
        "disposition_counts": dict(sorted(disposition.items())),
        "primary_5bps": primary_metrics,
        "stress": stress,
        "stage0_survived": not blockers,
        "stage0_blockers": blockers,
        "next_action": (
            "freeze a representative development corpus without changing rules"
            if not blockers
            else "retire this exact variant and publish the first-wave failure taxonomy"
        ),
        "maturity_effect": "NONE",
        "records": records,
    }
    result["result_sha256"] = common._self_hash(result, "result_sha256")
    return result


def inspect_result(
    manifest_path: Path,
    inspection_path: Path,
    result_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise CatalystOrbRetestStage0Error("Stage 0 result hash is invalid")
    if recorded != build_result(
        manifest_path, inspection_path, store, require_published=False
    ):
        raise CatalystOrbRetestStage0Error("Stage 0 result does not rebuild")
    audit: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": recorded["denominator"]["closed_signals"],
        "stage0_survived": recorded["stage0_survived"],
        "maturity_effect": "NONE",
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    audit["inspection_sha256"] = common._self_hash(audit, "inspection_sha256")
    return audit


def _publish(value: Mapping[str, Any], path: Path) -> None:
    if "inspection_kind" in value:
        identity = str(value.get("inspection_sha256", ""))
    elif "result_kind" in value:
        identity = str(value.get("result_sha256", ""))
    else:
        identity = str(value.get("manifest_sha256", ""))
    if not identity or not path.name.endswith(f"-{identity}.json"):
        raise CatalystOrbRetestStage0Error("output filename must end with content hash")
    resolved = path.resolve()
    allowed = {
        (PROJECT_ROOT / "strategy_tournament/activations").resolve(),
        (PROJECT_ROOT / "strategy_tournament/inspections").resolve(),
        (PROJECT_ROOT / "research_results").resolve(),
    }
    if resolved.parent not in allowed:
        raise CatalystOrbRetestStage0Error("output path is outside evidence roots")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--output", type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--output", type=Path)
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("inspection", type=Path)
    evaluate.add_argument("--output", type=Path)
    audit = commands.add_parser("inspect-result")
    audit.add_argument("manifest", type=Path)
    audit.add_argument("inspection", type=Path)
    audit.add_argument("result", type=Path)
    audit.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            source = HistoricalDayStore.from_env()
            _write_gzip(_private_input_path(source), _private_input_graph(source))
            result = build_manifest(source)
        elif args.command == "inspect":
            result = inspect_activation(args.manifest)
        elif args.command == "evaluate":
            result = build_result(args.manifest, args.inspection)
        else:
            result = inspect_result(args.manifest, args.inspection, args.result)
        if args.output is not None:
            _publish(result, args.output)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        CatalystOrbRetestStage0Error,
        common.EtfOrbStage0Error,
        continuation.EquityGapStage0Error,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
