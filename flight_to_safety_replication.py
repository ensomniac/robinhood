"""Freeze an exact flight-to-safety mechanism replication on disjoint evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.FLIGHT_TO_SAFETY_REPLICATION_FAMILY
MECHANISM_FAMILY = "cross-asset-flight-to-safety-rebound"
STRATEGY_ID = "flight-to-safety-equity-rebound-replication"
SUCCESSOR_ID = "flight-to-safety-equity-rebound-replication-v1"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = artifact_support.CALENDAR_PATH
CALENDAR_INSPECTION = artifact_support.CALENDAR_INSPECTION
TARGET_SYMBOLS = list(
    runtime.FLIGHT_TO_SAFETY_REPLICATION_TARGET_SYMBOLS
)
FEATURE_SYMBOLS = [runtime.FLIGHT_TO_SAFETY_FEATURE_SYMBOL]
SYMBOLS = [*TARGET_SYMBOLS, *FEATURE_SYMBOLS]
WARMUP_SESSIONS = 200
MAXIMUM_HOLD_SESSIONS = 5
PREDECESSOR_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "flight-to-safety-equity-rebound-v1/family-contract/"
    "contract-4d367cc0d8de39a6c9cba5424b848bb5140f26ed09fc84dc020029b4ffa18440.json"
)
PREDECESSOR_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound/search/"
    "cross-asset-flight-to-safety-equity-rebound-search-"
    "92af69d8461b9fba2cfb977e58db026643fcf22a04f58dc38924a95fdbcc88c3.json"
)
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound/development/"
    "cross-asset-flight-to-safety-equity-rebound-development-"
    "9216aa103e88478a97a1cdca3ea41215361f7df4b508ccc9904c8c6df9c9c446.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound/development-inspection/"
    "cross-asset-flight-to-safety-equity-rebound-development-inspection-"
    "dc95e24907f58a8cfd2ff0f50eb13220202dfcb044a2d957933a6822e1c4f9ae.json"
)


class FlightToSafetyReplicationError(ValueError):
    """The exact replication contract or evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FlightToSafetyReplicationError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise FlightToSafetyReplicationError(
            f"{field} needs a timezone"
        )
    if parsed.date() > date.today():
        raise FlightToSafetyReplicationError(
            f"{field} cannot be future-dated"
        )
    return parsed


def _calendar_authority(*, enforce_commit: bool) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(CALENDAR_INSPECTION)
    inspection = strategy_discovery.load_artifact(
        CALENDAR_INSPECTION,
        expected_kind="continuous-successor-calendar-data-inspection",
    )
    if not (
        inspection.get("state") == "CALENDAR_INSPECTED_READY"
        and inspection.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and inspection.get("calendar_sha256") == sha256_file(CALENDAR_PATH)
        and inspection.get("target_outcomes_accessed") is False
    ):
        raise FlightToSafetyReplicationError(
            "continuous calendar authority drifted"
        )
    return inspection


def _predecessor_graph(
    *, enforce_commit: bool
) -> dict[str, dict[str, Any]]:
    paths = (
        PREDECESSOR_CONTRACT,
        PREDECESSOR_SEARCH,
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    try:
        contract = json.loads(
            PREDECESSOR_CONTRACT.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise FlightToSafetyReplicationError(
            "predecessor family contract cannot be read"
        ) from exc
    search = strategy_discovery.load_artifact(
        PREDECESSOR_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        PREDECESSOR_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        PREDECESSOR_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        isinstance(contract, Mapping)
        and contract.get("family_id")
        == runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY
        and search.get("family_contract", {}).get("parameter_grid")
        == contract.get("parameter_grid")
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
        and inspection.get("selection", {}).get("selected_trial_id")
        is None
    ):
        raise FlightToSafetyReplicationError(
            "adverse predecessor evidence graph drifted"
        )
    return {
        "contract": dict(contract),
        "search": search,
        "result": result,
        "inspection": inspection,
    }


def _full_sessions() -> list[str]:
    return artifact_support._full_sessions()


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    full = _full_sessions()
    development = [
        day
        for day in full
        if day.startswith(("2017-", "2018-", "2019-"))
    ]
    embargo = [day for day in full if day.startswith("2020-")]
    confirmation = [day for day in full if day.startswith("2021-")]
    if not (
        len(development) == 746
        and len(embargo) == 251
        and len(confirmation) == 251
    ):
        raise FlightToSafetyReplicationError(
            "replication partition capacity drifted"
        )
    development_start = full.index(development[0])
    warmup = full[
        development_start - WARMUP_SESSIONS : development_start
    ]
    confirmation_start = full.index(confirmation[0])
    confirmation_warmup = full[
        confirmation_start - WARMUP_SESSIONS : confirmation_start
    ]
    return (
        warmup,
        development,
        development[:-MAXIMUM_HOLD_SESSIONS],
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation[:-MAXIMUM_HOLD_SESSIONS],
    )


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(TARGET_SYMBOLS)}


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    overlaps = outcome_exposure.find_overlaps(
        contract["development_scope"], records
    )
    if not overlaps:
        return
    overlap_ids = {item["exposure_id"] for item in overlaps}
    matching = [
        record
        for record in records
        if record["exposure_id"] in overlap_ids
    ]
    expected_prefix = (
        f"strategy_tournament/v2/discovery/{FAMILY_ID}/development/"
    )
    if not (
        len(matching) == 1
        and matching[0]["lane"] == "development"
        and matching[0]["source_path"].startswith(expected_prefix)
        and matching[0]["scope"] == contract["development_scope"]
    ):
        raise FlightToSafetyReplicationError(
            "development scope has foreign or partial outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    predecessor = _predecessor_graph(enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar_inspection = _calendar_authority(
        enforce_commit=enforce_commit
    )
    (
        warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = _partitions()
    development_scope = _scope(development)
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(CALENDAR_INSPECTION),
        _repo_path(PREDECESSOR_CONTRACT),
        _repo_path(PREDECESSOR_SEARCH),
        _repo_path(PREDECESSOR_RESULT),
        _repo_path(PREDECESSOR_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": sorted(
                {
                    *warmup,
                    *development,
                    *embargo,
                    *confirmation_warmup,
                    *confirmation,
                }
            ),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": len(development_signals),
                    "capacity_unit": (
                        "frozen max-one-entry development decision dates"
                    ),
                    "development_sessions": len(development),
                    "development_signal_dates": len(development_signals),
                    "embargo_sessions": len(embargo),
                    "confirmation_sessions": len(confirmation),
                    "confirmation_signal_dates": len(
                        confirmation_signals
                    ),
                    "calendar_sha256": sha256_file(CALENDAR_PATH),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        DEFAULT_ROOT / SUCCESSOR_ID / "capacity",
    )
    parameter_grid = predecessor["contract"]["parameter_grid"]
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": predecessor["contract"]["experiment_id"],
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "existing_successor_validator": {
            "module": "flight_to_safety_replication",
            "function": "validate_existing_successor_contract",
        },
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 1,
        "predecessor": {
            "contract_path": _repo_path(PREDECESSOR_CONTRACT),
            "contract_file_sha256": sha256_file(PREDECESSOR_CONTRACT),
            "search_path": _repo_path(PREDECESSOR_SEARCH),
            "search_sha256": predecessor["search"]["artifact_sha256"],
            "result_path": _repo_path(PREDECESSOR_RESULT),
            "result_sha256": predecessor["result"]["artifact_sha256"],
            "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
            "inspection_sha256": predecessor["inspection"][
                "artifact_sha256"
            ],
            "promotion_evidence_reused": False,
            "parameter_grid_changed": False,
        },
        "mechanism": (
            "Replicate the frozen flight-to-safety rule: buy a broad-equity "
            "ETF one session after a completed target decline accompanied by "
            "a completed TLT bid, with identical thresholds, stops, and holds."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "target_symbols": list(TARGET_SYMBOLS),
            "feature_symbols": list(FEATURE_SYMBOLS),
            "complete_frozen_daily_history": True,
            "selection_basis": (
                "Three different liquid broad-equity ETFs on untouched "
                "2017-2019 target evidence; TLT remains feature-only."
            ),
        },
        "entry_rule": predecessor["contract"]["entry_rule"],
        "stop_rule": predecessor["contract"]["stop_rule"],
        "exit_rule": predecessor["contract"]["exit_rule"],
        "ranking_rule": predecessor["contract"]["ranking_rule"],
        "selection_rule": predecessor["contract"]["selection_rule"],
        "parameter_grid": parameter_grid,
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": predecessor["contract"]["primary_outcome"],
        "execution_assumptions": predecessor["contract"][
            "execution_assumptions"
        ],
        "falsification_criteria": predecessor["contract"][
            "falsification_criteria"
        ],
        "minimum_evidence": predecessor["contract"]["minimum_evidence"],
        "contamination_risks": [
            "The exact 2017-2019 target date-symbol pairs are globally untouched at freeze.",
            "The rejected predecessor motivates replication only and contributes no promotion evidence.",
            "No threshold, stop, hold, or selection gate changed after predecessor outcomes.",
            "All of 2020 is a fixed embargo and 2021 target pairs remain untouched confirmation.",
        ],
        "production_compatibility_risks": predecessor["contract"][
            "production_compatibility_risks"
        ],
        "material_difference_rationale": (
            "This is an exact-mechanism replication, not a repair: it preserves "
            "the complete predecessor grid and evaluator while changing only "
            "the target ETFs and using a longer, disjoint historical corpus."
        ),
        "development_dates": development,
        "development_signal_dates": development_signals,
        "development_warmup_dates": warmup,
        "embargo_dates": embargo,
        "confirmation_warmup_dates": confirmation_warmup,
        "confirmation_dates": confirmation,
        "confirmation_signal_dates": confirmation_signals,
        "confirmation_signal_capacity": len(confirmation_signals),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_inspection_path": _repo_path(CALENDAR_INSPECTION),
        "calendar_inspection_sha256": calendar_inspection[
            "artifact_sha256"
        ],
        "universe": {
            "symbols": list(SYMBOLS),
            "target_symbols": list(TARGET_SYMBOLS),
            "feature_symbols": list(FEATURE_SYMBOLS),
            "point_in_time": True,
        },
        "historical_data_contract": {
            "daily_provider": "alpaca",
            "daily_endpoint": "/v2/stocks/{symbol}/bars",
            "daily_feed": "sip",
            "daily_adjustment": "raw",
            "daily_request_mode": "symbol_range",
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "predecessor_corpus_disjoint": True,
            "full_2020_embargo": True,
        },
        "falsifiers": predecessor["contract"]["falsifiers"],
        "implementation_files": [
            "flight_to_safety_replication.py",
            "dense_data_collection.py",
            "dense_data_collection_inspection.py",
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "dense_strategy_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
    }
    contract = strategy_discovery._validate_family_contract(contract)
    validate_existing_successor_contract(
        contract, enforce_commit=enforce_commit
    )
    digest = hashlib.sha256(
        artifact_support._canonical(contract)
    ).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    artifact_support._write_json(path, contract)
    return path, contract, capacity_path


def validate_existing_successor_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    predecessor = _predecessor_graph(enforce_commit=enforce_commit)
    (
        warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = _partitions()
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 1
        and contract.get("parameter_grid")
        == predecessor["contract"]["parameter_grid"]
        and len(contract.get("trial_family", [])) == 32
        and contract.get("development_warmup_dates") == warmup
        and contract.get("development_dates") == development
        and contract.get("development_signal_dates")
        == development_signals
        and contract.get("embargo_dates") == embargo
        and contract.get("confirmation_warmup_dates")
        == confirmation_warmup
        and contract.get("confirmation_dates") == confirmation
        and contract.get("confirmation_signal_dates")
        == confirmation_signals
        and contract.get("development_scope") == _scope(development)
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("existing_successor_validator")
        == {
            "module": "flight_to_safety_replication",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("historical_data_contract", {}).get(
            "daily_request_mode"
        )
        == "symbol_range"
    ):
        raise FlightToSafetyReplicationError(
            "flight-to-safety replication contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar = _calendar_authority(enforce_commit=enforce_commit)
    if (
        contract.get("calendar_inspection_sha256")
        != calendar["artifact_sha256"]
    ):
        raise FlightToSafetyReplicationError(
            "replication calendar binding drifted"
        )
    _validate_exposure_state(contract)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze",))
    parser.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, contract, capacity = freeze_contract(
            created_at=args.created_at
        )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": contract["status"],
                    "trial_count": len(contract["trial_family"]),
                    "development_sessions": len(
                        contract["development_dates"]
                    ),
                    "confirmation_sessions": len(
                        contract["confirmation_dates"]
                    ),
                    "confirmation_signal_capacity": contract[
                        "confirmation_signal_capacity"
                    ],
                    "capacity_manifest": _repo_path(capacity),
                    "new_mechanism_family_slot_consumed": False,
                    "confirmation_access_permitted": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        FlightToSafetyReplicationError,
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
