"""Freeze the selection-aware protection-capped gap successor."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import gap_protection_collection as collection
import gap_protection_successor as successor
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
SUCCESSOR_ID = "equity-gap-protection-continuation-v1-development-search"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
DATA_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-development-minutes-2026-07-24-v1/"
    "data-inspection/"
    "gap-protection-development-data-inspection-"
    "13f9b9c3431a2f5b9c48cfbfeb9c2ee6a9b977f6d91218fa5d5a2039a6dab078.json"
)


class GapProtectionDiscoveryError(RuntimeError):
    """The successor contract or its evidence graph drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GapProtectionDiscoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GapProtectionDiscoveryError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise GapProtectionDiscoveryError(f"path escaped repository: {path}") from exc


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise GapProtectionDiscoveryError(f"hash-addressed contract drifted: {path}")
    if path.exists():
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _scope(
    dates: list[str],
    inventory: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(
                str(row["symbol"])
                for row in inventory["candidates_by_date"][day]
            )
            for day in dates
        },
    }


def freeze(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any], Path]:
    try:
        observed = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GapProtectionDiscoveryError("created_at is invalid") from exc
    if observed.tzinfo is None:
        raise GapProtectionDiscoveryError("created_at needs a timezone")
    source = store or HistoricalDayStore.from_env()
    for path in (
        DATA_INSPECTION,
        PROJECT_ROOT / "gap_protection_discovery.py",
        PROJECT_ROOT / "gap_protection_plugin.py",
        PROJECT_ROOT / "gap_protection_collection.py",
        PROJECT_ROOT / "gap_protection_successor.py",
        PROJECT_ROOT / "dense_strategy_runtime.py",
    ):
        strategy_discovery.require_committed(path)
    inspection = _read(DATA_INSPECTION)
    inventory = collection._load_gzip(successor._inventory_path(source))
    if (
        inspection.get("valid") is not True
        or inspection.get("state") != "DEVELOPMENT_DATA_INSPECTED_READY"
        or inventory.get("content_sha256")
        != _read(collection.PREENTRY_INSPECTION)["inventory"]["content_sha256"]
    ):
        raise GapProtectionDiscoveryError("development evidence graph is invalid")
    development = list(inventory["partitions"]["development"])
    embargo = list(inventory["partitions"]["embargo"])
    confirmation = list(inventory["partitions"]["confirmation"])
    development_scope = _scope(development, inventory)
    confirmation_scope = _scope(confirmation, inventory)
    records = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, records):
        raise GapProtectionDiscoveryError(
            "development scope is not indexed as exposed"
        )
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    evidence_paths = [
        _repo_path(successor.PREENTRY_INSPECTION),
        _repo_path(collection.PREENTRY_INSPECTION),
        _repo_path(DATA_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-development",
            "registered_at": created_at,
            "requested_dates": development,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "gap_protection_capacity": {
                    "family_id": runtime.EQUITY_GAP_CONTINUATION_FAMILY,
                    "mechanism_family": "equity-gap-continuation",
                    "formal_capacity": inspection[
                        "candidate_symbol_sessions"
                    ],
                    "capacity_unit": "frozen candidate symbol-sessions",
                    "development_sessions": len(development),
                    "embargo_sessions": len(embargo),
                    "confirmation_sessions": len(confirmation),
                    "confirmation_signal_capacity": len(confirmation),
                    "development_scope_indexed_as_exposed": True,
                    "development_data_inspected": True,
                    "confirmation_access_permitted": False,
                },
                "gap_protection_runtime": {
                    "family_id": runtime.EQUITY_GAP_CONTINUATION_FAMILY,
                    "sample_phase": "development",
                    "input_inspection_path": _repo_path(DATA_INSPECTION),
                    "input_inspection_file_sha256": sha256_file(
                        DATA_INSPECTION
                    ),
                    "input_inspection_sha256": inspection[
                        "inspection_sha256"
                    ],
                    "private_input_index_content_sha256": inspection[
                        "private_input_index_content_sha256"
                    ],
                    "preentry_inventory_content_sha256": inventory[
                        "content_sha256"
                    ],
                    "provider_requests": 0,
                },
            },
        },
        root / SUCCESSOR_ID / "capacity",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": successor.CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": runtime.EQUITY_GAP_CONTINUATION_FAMILY,
        "mechanism_family": "equity-gap-continuation",
        "strategy_id": "equity-gap-continuation",
        "parent_experiment_id": "equity-gap-continuation-v2-development-search",
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": "existing_family_successor",
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 2,
        "mechanism": (
            "Continuation after a completed high-volume five-minute "
            "opening-range breakout in a point-in-time positive-gap common stock."
        ),
        "expected_holding_behavior": (
            "Long only, next-minute entry, and flat by 15:50 ET on the signal day."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "security_type": "point-in-time active U.S. common stocks",
            "opening_price_minimum": 5.0,
            "opening_gap_fraction": [0.02, 0.08],
            "selection_time_et": "09:35:00",
            "complete_candidate_denominator": True,
        },
        "entry_rule": (
            "After the five-minute opening range, require a completed close "
            "above the range high and cumulative VWAP with the frozen prior-15-"
            "bar volume multiple, then enter at the next minute open."
        ),
        "stop_rule": (
            "Use the five-minute opening-range low only when the structural "
            "distance is no more than the frozen 3% or 4% protection cap; "
            "otherwise reject without substituting another candidate."
        ),
        "exit_rule": (
            "Resolve target or stop with stop-first same-minute ambiguity and "
            "otherwise force flat at the 15:50 bar open."
        ),
        "ranking_rule": (
            "Earliest next-minute entry, then highest breakout-volume multiple, "
            "largest opening gap, and lexical symbol."
        ),
        "selection_rule": (
            "At most one family entry per day under authoritative portfolio "
            "risk, notional, entry, and capital-contention caps."
        ),
        "parameter_grid": successor.PARAMETER_GRID,
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 1,
            "missing_data": "retained_denominator_no_signal",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "volume_lookback_completed_bars": 15,
            "force_flat_et": "15:50:00",
        },
        "falsification_criteria": {
            "minimum_20bps_log_growth": 0.0,
            "minimum_stressed_profit_factor": 1.2,
            "maximum_drawdown_r": 6.0,
            "minimum_deflated_sharpe_probability": 0.9,
            "maximum_pbo_probability": 0.5,
        },
        "minimum_evidence": {
            "configured_floor": 50,
            "confirmation_floor": 20,
            "power": 0.8,
            "alpha": 0.1,
        },
        "contamination_risks": [
            "The stop-cap thesis came from contaminated screening and cannot promote this version.",
            "The 57-date development partition is globally indexed as exposed.",
            "The 38-date confirmation reserve must remain unopened until one exact winner freezes its power counts.",
            "Any global date-symbol exposure disqualifies confirmation.",
        ],
        "production_compatibility_risks": [
            "Live completeness, fresh quote, spread, depth, halt, tradability, timing, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This successor adds a prospectively frozen causal protection cap "
            "to eliminate structurally unprotectable opening ranges on a fresh "
            "historical inventory."
        ),
        "development_dates": development,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_signal_dates": development,
        "confirmation_signal_dates": confirmation,
        "confirmation_signal_capacity": len(confirmation),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "identity": (
                "complete point-in-time active common-stock 2-8% opening-gap "
                "candidates at 09:35 ET"
            ),
            "preentry_inspection": _repo_path(
                collection.PREENTRY_INSPECTION
            ),
            "development_candidate_symbol_sessions": inspection[
                "candidate_symbol_sessions"
            ],
            "confirmation_candidate_symbol_sessions": sum(
                len(inventory["candidates_by_date"][day])
                for day in confirmation
            ),
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": False,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "gap_protection_discovery.py",
            "gap_protection_plugin.py",
            "gap_protection_collection.py",
            "gap_protection_successor.py",
            "equity_gap_continuation_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "gap_protection_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
        "dataset_manifest": _repo_path(capacity_path),
    }
    contract = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(_canonical(contract)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    _write(path, contract)
    return path, contract, capacity_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze",))
    parser.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, contract, capacity = freeze(created_at=args.created_at)
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "capacity_manifest": _repo_path(capacity),
                    "state": contract["status"],
                    "trial_count": len(contract["trial_family"]),
                    "confirmation_access_permitted": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        GapProtectionDiscoveryError,
        OSError,
        ValueError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
