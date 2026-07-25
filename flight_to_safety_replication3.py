"""Freeze an exact long-history flight-to-safety rebound replication."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dense_data_collection
import dense_strategy_runtime as runtime
import etf_pullback_replication as calendar_support
import flight_to_safety_replication as v1
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.FLIGHT_TO_SAFETY_REPLICATION_V3_FAMILY
MECHANISM_FAMILY = v1.MECHANISM_FAMILY
STRATEGY_ID = "flight-to-safety-equity-rebound-replication-v3-long-history"
SUCCESSOR_ID = STRATEGY_ID
RESEARCH_GENERATION = v1.RESEARCH_GENERATION
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = calendar_support.CALENDAR_PATH
TARGET_SYMBOLS = list(
    runtime.FLIGHT_TO_SAFETY_REPLICATION_V3_TARGET_SYMBOLS
)
FEATURE_SYMBOLS = [runtime.FLIGHT_TO_SAFETY_FEATURE_SYMBOL]
SYMBOLS = [*TARGET_SYMBOLS, *FEATURE_SYMBOLS]
WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_000
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 293
MAXIMUM_HOLD_SESSIONS = 5
TOTAL_SESSIONS = (
    WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)
V2_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "flight-to-safety-equity-rebound-replication-v2/family-contract/"
    "contract-98425bf1fc1a0cc35e8c581c6a770ffb09d0871bb2810acfb60626e621dd2290.json"
)
V2_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound-replication-v2/search/"
    "cross-asset-flight-to-safety-equity-rebound-replication-v2-search-"
    "1b037ec35cfcbd59b4dde1801f79f22decd9d7bd8818984e67d6ba0b835c1c28.json"
)
V2_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound-replication-v2/development/"
    "cross-asset-flight-to-safety-equity-rebound-replication-v2-development-"
    "4e554b610cd04158e0f44fac68995129f43a1349fc3f2ee4dea44dd24e82361a.json"
)
V2_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound-replication-v2/"
    "development-inspection/"
    "cross-asset-flight-to-safety-equity-rebound-replication-v2-"
    "development-inspection-"
    "3e10a372ad7444fb672afe90e5ac503f9147f607d3a4e928a4bb56d3d91bdd16.json"
)


class FlightToSafetyReplication3Error(ValueError):
    """The long-history replication contract or evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FlightToSafetyReplication3Error(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise FlightToSafetyReplication3Error(
            f"{field} needs a timezone"
        )
    if parsed.date() > date.today():
        raise FlightToSafetyReplication3Error(
            f"{field} cannot be future-dated"
        )
    return parsed


def _read_contract(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FlightToSafetyReplication3Error(
            "v2 contract cannot be read"
        ) from exc
    if not isinstance(value, dict):
        raise FlightToSafetyReplication3Error(
            "v2 contract is malformed"
        )
    return value


def _predecessor_graph(
    *, enforce_commit: bool
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    for path in (V2_CONTRACT, V2_SEARCH, V2_RESULT, V2_INSPECTION):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    contract = _read_contract(V2_CONTRACT)
    search = strategy_discovery.load_artifact(
        V2_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        V2_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        V2_INSPECTION, expected_kind="development-search-inspection"
    )
    if not (
        contract.get("family_id")
        == runtime.FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY
        and search.get("family_contract", {}).get("parameter_grid")
        == contract.get("parameter_grid")
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
        and inspection.get("selection", {}).get("selected_trial_id")
        is None
    ):
        raise FlightToSafetyReplication3Error(
            "v2 adverse evidence graph drifted"
        )
    return contract, search, result, inspection


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    dates = [
        day
        for day in calendar_support._calendar_dates()
        if day <= "2013-12-31"
    ]
    if len(dates) != TOTAL_SESSIONS:
        raise FlightToSafetyReplication3Error(
            "2008-2013 calendar partition drifted"
        )
    warmup = dates[:WARMUP_SESSIONS]
    development = dates[
        WARMUP_SESSIONS : WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    ]
    embargo_start = WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    embargo = dates[
        embargo_start : embargo_start + EMBARGO_SESSIONS
    ]
    confirmation = dates[-CONFIRMATION_SESSIONS:]
    confirmation_start = dates.index(confirmation[0])
    confirmation_warmup = dates[
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
    exposure_ids = {item["exposure_id"] for item in overlaps}
    matching = [
        record
        for record in records
        if record["exposure_id"] in exposure_ids
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
        raise FlightToSafetyReplication3Error(
            "v3 development scope has foreign or partial outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    base, search, result, inspection = _predecessor_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar_inspection_path, calendar_inspection = (
        calendar_support._calendar_data_inspection(
            enforce_commit=enforce_commit
        )
    )
    if (
        calendar_inspection.get("calendar_sha256")
        != sha256_file(CALENDAR_PATH)
    ):
        raise FlightToSafetyReplication3Error(
            "calendar inspection drifted"
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
    development_scope = _scope([*warmup, *development])
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(calendar_inspection_path),
        _repo_path(V2_CONTRACT),
        _repo_path(V2_SEARCH),
        _repo_path(V2_RESULT),
        _repo_path(V2_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": [
                *warmup,
                *development,
                *embargo,
                *confirmation,
            ],
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
                    "development_signal_dates": len(
                        development_signals
                    ),
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
    contract = copy.deepcopy(base)
    for field in (
        "implementation_hashes",
        "primary_trial_id",
        "rolling_origin_plan",
        "trial_family",
    ):
        contract.pop(field, None)
    contract.update(
        {
            "campaign_id": CAMPAIGN_ID,
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "family_id": FAMILY_ID,
            "mechanism_family": MECHANISM_FAMILY,
            "strategy_id": STRATEGY_ID,
            "parent_experiment_id": base["experiment_id"],
            "created_at": created_at,
            "status": "INVENTED",
            "successor_id": SUCCESSOR_ID,
            "existing_successor_validator": {
                "module": "flight_to_safety_replication3",
                "function": "validate_existing_successor_contract",
            },
            "new_mechanism_family_slot_consumed": False,
            "prior_family_attempt_count": 3,
            "predecessor": {
                "contract_path": _repo_path(V2_CONTRACT),
                "contract_file_sha256": sha256_file(V2_CONTRACT),
                "search_path": _repo_path(V2_SEARCH),
                "search_sha256": search["artifact_sha256"],
                "result_path": _repo_path(V2_RESULT),
                "result_sha256": result["artifact_sha256"],
                "inspection_path": _repo_path(V2_INSPECTION),
                "inspection_sha256": inspection["artifact_sha256"],
                "promotion_evidence_reused": False,
                "parameter_grid_changed": False,
            },
            "mechanism": (
                "Replicate the unchanged completed-session flight-to-safety "
                "rebound grid over a longer, disjoint market regime."
            ),
            "universe_requirements": {
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "complete_frozen_daily_history": True,
                "selection_basis": (
                    "DIA, IWM, and QQQ are fixed liquid U.S. index ETFs "
                    "with complete 2008-2013 history; TLT is the frozen "
                    "Treasury flight-to-safety feature."
                ),
            },
            "contamination_risks": [
                "The exact 2008-2013 target date-symbol pairs were absent from the global exposure index before freeze.",
                "The rejected 2017-2019 replication informs only the unchanged mechanism and contributes no promotion evidence.",
                "The complete 32-trial grid is unchanged; no predecessor outcome repaired a parameter.",
                "Warmup is feature-only and is explicitly included in development exposure.",
            ],
            "material_difference_rationale": (
                "This exact-rule temporal and instrument replication tests "
                "the prior short-sample flight-to-safety effect across a "
                "1,000-session development regime with disjoint index ETFs."
            ),
            "development_warmup_dates": warmup,
            "development_dates": development,
            "development_signal_dates": development_signals,
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
            "calendar_sha256": sha256_file(CALENDAR_PATH),
            "calendar_inspection_path": _repo_path(
                calendar_inspection_path
            ),
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
                "daily_provider": "yahoo",
                "daily_endpoint": (
                    dense_data_collection.YAHOO_CHART_ENDPOINT
                ),
                "daily_request_mode": "symbol_range",
                "daily_adjustment": (
                    dense_data_collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT
                ),
                "split_provider": "massive",
                "provider_substitutions_allowed": False,
                "no_purchase_required": True,
                "retries_permitted": 0,
            },
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "predecessor_corpora_disjoint": True,
            },
            "implementation_files": [
                "flight_to_safety_replication3.py",
                "etf_pullback_replication.py",
                "dense_data_collection.py",
                "dense_data_collection_inspection.py",
                "dense_collection_plan_inspection.py",
                "dense_strategy_plugin.py",
                "dense_strategy_runtime.py",
                "learning_statistics.py",
                "learning_experiment.py",
                "strategy_discovery.py",
                "outcome_exposure.py",
                "portfolio_maturity.py",
                "portfolio_config.toml",
            ],
            "capacity_manifest": _repo_path(capacity_path),
            "selection_mode": "development_search",
            "winner_selection": DEVELOPMENT_SEARCH_RULE,
        }
    )
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
    base, search, result, inspection = _predecessor_graph(
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
    expected_data_contract = {
        "daily_provider": "yahoo",
        "daily_endpoint": dense_data_collection.YAHOO_CHART_ENDPOINT,
        "daily_request_mode": "symbol_range",
        "daily_adjustment": (
            dense_data_collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT
        ),
        "split_provider": "massive",
        "provider_substitutions_allowed": False,
        "no_purchase_required": True,
        "retries_permitted": 0,
    }
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 3
        and contract.get("parameter_grid") == base["parameter_grid"]
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
        and contract.get("development_scope")
        == _scope([*warmup, *development])
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("historical_data_contract")
        == expected_data_contract
        and contract.get("predecessor", {}).get("search_sha256")
        == search["artifact_sha256"]
        and contract.get("predecessor", {}).get("result_sha256")
        == result["artifact_sha256"]
        and contract.get("predecessor", {}).get("inspection_sha256")
        == inspection["artifact_sha256"]
        and contract.get("existing_successor_validator")
        == {
            "module": "flight_to_safety_replication3",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("calendar_sha256")
        == sha256_file(CALENDAR_PATH)
    ):
        raise FlightToSafetyReplication3Error(
            "long-history flight-to-safety contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar_path, calendar_inspection = (
        calendar_support._calendar_data_inspection(
            enforce_commit=enforce_commit
        )
    )
    if not (
        contract.get("calendar_inspection_path")
        == _repo_path(calendar_path)
        and contract.get("calendar_inspection_sha256")
        == calendar_inspection["artifact_sha256"]
    ):
        raise FlightToSafetyReplication3Error(
            "long-history calendar binding drifted"
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
        FlightToSafetyReplication3Error,
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
