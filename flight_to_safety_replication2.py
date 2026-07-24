"""Freeze the identity-safe flight-to-safety replication successor."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dense_collection_recovery as recovery
import dense_strategy_runtime as runtime
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
FAMILY_ID = runtime.FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY
MECHANISM_FAMILY = v1.MECHANISM_FAMILY
STRATEGY_ID = "flight-to-safety-equity-rebound-replication-v2"
SUCCESSOR_ID = "flight-to-safety-equity-rebound-replication-v2"
RESEARCH_GENERATION = v1.RESEARCH_GENERATION
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = v1.CALENDAR_PATH
CALENDAR_INSPECTION = v1.CALENDAR_INSPECTION
TARGET_SYMBOLS = list(
    runtime.FLIGHT_TO_SAFETY_REPLICATION_V2_TARGET_SYMBOLS
)
FEATURE_SYMBOLS = [runtime.FLIGHT_TO_SAFETY_FEATURE_SYMBOL]
SYMBOLS = [*TARGET_SYMBOLS, *FEATURE_SYMBOLS]
V1_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "flight-to-safety-equity-rebound-replication-v1/family-contract/"
    "contract-b3b040e3b107af23e8bd7fed541daf83920a3b66b765b6c88dbb7fe577efa3e6.json"
)
V1_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound-replication/"
    "development-collection-failure/"
    "cross-asset-flight-to-safety-equity-rebound-replication-"
    "development-collection-failure-"
    "ab50d7aeeb14068f2d91e0735dba7b7c191f9879f73c3021ea3b5206c4c9c10c.json"
)
V1_FAILURE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-asset-flight-to-safety-equity-rebound-replication/"
    "development-collection-failure-inspection/"
    "cross-asset-flight-to-safety-equity-rebound-replication-"
    "development-collection-failure-inspection-"
    "0decf5ccb9216affbac2ab645093d6576f0ec2cec5d04f98664dce3294b998ec.json"
)


class FlightToSafetyReplication2Error(ValueError):
    """The structural replication successor or evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FlightToSafetyReplication2Error(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise FlightToSafetyReplication2Error(
            f"{field} needs a timezone"
        )
    if parsed.date() > date.today():
        raise FlightToSafetyReplication2Error(
            f"{field} cannot be future-dated"
        )
    return parsed


def _read_contract(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FlightToSafetyReplication2Error(
            "v1 replication contract cannot be read"
        ) from exc
    if not isinstance(value, dict):
        raise FlightToSafetyReplication2Error(
            "v1 replication contract is malformed"
        )
    return value


def _structural_predecessor(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    for path in (V1_CONTRACT, V1_FAILURE, V1_FAILURE_INSPECTION):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    contract = _read_contract(V1_CONTRACT)
    failure = strategy_discovery.load_artifact(
        V1_FAILURE, expected_kind=recovery.FAILURE_KIND
    )
    inspection = strategy_discovery.load_artifact(
        V1_FAILURE_INSPECTION,
        expected_kind="dense-data-collection-failure-inspection",
    )
    if not (
        contract.get("family_id") == v1.FAMILY_ID
        and contract.get("parameter_grid")
        == v1._predecessor_graph(
            enforce_commit=enforce_commit
        )["contract"]["parameter_grid"]
        and failure.get("state") == recovery.FAILURE_STATE
        and failure.get("failure_code")
        == recovery.INCOMPLETE_FIXED_DAILY_RANGE
        and failure.get("strategy_metrics_accessed") is False
        and failure.get("data_outcomes_accessed") is True
        and inspection.get("state") == "COLLECTION_FAILURE_INSPECTED"
        and inspection.get("failure_sha256")
        == failure["artifact_sha256"]
    ):
        raise FlightToSafetyReplication2Error(
            "v1 structural failure graph drifted"
        )
    return contract, failure, inspection


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
        raise FlightToSafetyReplication2Error(
            "v2 development scope has foreign or partial outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    base, failure, failure_inspection = _structural_predecessor(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar = v1._calendar_authority(enforce_commit=enforce_commit)
    (
        warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = v1._partitions()
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
        _repo_path(V1_CONTRACT),
        _repo_path(V1_FAILURE),
        _repo_path(V1_FAILURE_INSPECTION),
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
                "module": "flight_to_safety_replication2",
                "function": "validate_existing_successor_contract",
            },
            "prior_family_attempt_count": 2,
            "predecessor": {
                "contract_path": _repo_path(V1_CONTRACT),
                "contract_file_sha256": sha256_file(V1_CONTRACT),
                "failure_path": _repo_path(V1_FAILURE),
                "failure_sha256": failure["artifact_sha256"],
                "failure_inspection_path": _repo_path(
                    V1_FAILURE_INSPECTION
                ),
                "failure_inspection_sha256": failure_inspection[
                    "artifact_sha256"
                ],
                "failure_code": failure["failure_code"],
                "strategy_metrics_accessed": False,
                "promotion_evidence_reused": False,
                "parameter_grid_changed": False,
            },
            "universe_requirements": {
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "complete_frozen_daily_history": True,
                "selection_basis": (
                    "Three long-history liquid broad-equity ETFs replacing "
                    "the structurally incomplete SPTM basket before metrics."
                ),
            },
            "contamination_risks": [
                "The exact ITOT, RSP, and VV 2017-2019 target pairs are globally untouched at freeze.",
                "The failed v1 checkpoints and target identities contribute no strategy metrics or promotion evidence.",
                "The complete strategy grid and all evidence dates remain unchanged.",
                "All of 2020 remains embargo and 2021 confirmation target pairs are untouched.",
            ],
            "material_difference_rationale": (
                "This structural successor replaces the data-incomplete "
                "IWB/SCHX/SPTM target basket with ITOT/RSP/VV before any "
                "strategy metrics; every mechanism rule and date is unchanged."
            ),
            "development_scope": development_scope,
            "confirmation_scope": confirmation_scope,
            "outcome_exposure_index_sha256": outcome_exposure.audit()[
                "index_sha256"
            ],
            "calendar_inspection_sha256": calendar["artifact_sha256"],
            "universe": {
                "symbols": list(SYMBOLS),
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "point_in_time": True,
            },
            "implementation_files": [
                "flight_to_safety_replication2.py",
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
    base, failure, inspection = _structural_predecessor(
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
    ) = v1._partitions()
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 2
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
        and contract.get("development_scope") == _scope(development)
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("predecessor", {}).get("failure_sha256")
        == failure["artifact_sha256"]
        and contract.get("predecessor", {}).get(
            "failure_inspection_sha256"
        )
        == inspection["artifact_sha256"]
        and contract.get("existing_successor_validator")
        == {
            "module": "flight_to_safety_replication2",
            "function": "validate_existing_successor_contract",
        }
    ):
        raise FlightToSafetyReplication2Error(
            "identity-safe replication contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar = v1._calendar_authority(enforce_commit=enforce_commit)
    if (
        contract.get("calendar_inspection_sha256")
        != calendar["artifact_sha256"]
    ):
        raise FlightToSafetyReplication2Error(
            "identity-safe replication calendar binding drifted"
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
        FlightToSafetyReplication2Error,
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
