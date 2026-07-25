"""Freeze the exact-rule cross-style v2 source-capacity successor."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import cross_style_breadth_discovery as predecessor_support
import dense_strategy_runtime as runtime
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY
MECHANISM_FAMILY = predecessor_support.MECHANISM_FAMILY
STRATEGY_ID = predecessor_support.STRATEGY_ID
SUCCESSOR_ID = "cross-style-etf-breadth-continuation-v2"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = predecessor_support.CALENDAR_PATH
CALENDAR_INSPECTION = predecessor_support.CALENDAR_INSPECTION
SYMBOLS = list(runtime.CROSS_STYLE_BREADTH_SYMBOLS)
WARMUP_SESSIONS = 200
EMBARGO_SESSIONS = 5
MAXIMUM_HOLD_SESSIONS = 5
PREDECESSOR_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "cross-style-etf-breadth-continuation-v1/family-contract/"
    "contract-275f3ec6ac0388cbafb28e9d0b48e2f82a65fe64d1b92e3be3ad737fb7984e01.json"
)
PREDECESSOR_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-style-etf-breadth-continuation/search/"
    "cross-style-etf-breadth-continuation-search-"
    "af484c7171fa335a20d8984bd8bc0edae6e4f2b24a8ef0b1b849116bf84841a4.json"
)
PREDECESSOR_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-style-etf-breadth-continuation/development-collection-failure/"
    "cross-style-etf-breadth-continuation-development-collection-failure-"
    "32f1ed08430f94e1491eb60be39ae1372675a48c6c663500da0cc8dc574b4426.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-style-etf-breadth-continuation/"
    "development-collection-failure-inspection/"
    "cross-style-etf-breadth-continuation-development-collection-failure-"
    "inspection-a4e6f82cc9bb8fb937bf92e439c470ec17c188709dd616d3f396c589b394e34f.json"
)
PREDECESSOR_EXPOSURE_ID = (
    "dense-collection-failure-32f1ed08430f94e1491e"
)


class CrossStyleBreadthSuccessorError(ValueError):
    """The exact-rule successor or its adverse lineage drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise CrossStyleBreadthSuccessorError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise CrossStyleBreadthSuccessorError(
            f"{field} needs a timezone"
        )
    if parsed.date() > date.today():
        raise CrossStyleBreadthSuccessorError(
            f"{field} cannot be future-dated"
        )
    return parsed


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossStyleBreadthSuccessorError(
            f"cannot read {path}"
        ) from exc
    if not isinstance(value, dict):
        raise CrossStyleBreadthSuccessorError(
            f"{path} must contain an object"
        )
    return value


def _predecessor_graph(
    *, enforce_commit: bool
) -> dict[str, dict[str, Any]]:
    paths = (
        PREDECESSOR_CONTRACT,
        PREDECESSOR_SEARCH,
        PREDECESSOR_FAILURE,
        PREDECESSOR_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    contract = _read(PREDECESSOR_CONTRACT)
    search = strategy_discovery.load_artifact(
        PREDECESSOR_SEARCH, expected_kind="frozen-development-search"
    )
    failure = strategy_discovery.load_artifact(
        PREDECESSOR_FAILURE,
        expected_kind="dense-data-collection-failure",
    )
    inspection = strategy_discovery.load_artifact(
        PREDECESSOR_INSPECTION,
        expected_kind="dense-data-collection-failure-inspection",
    )
    if not (
        contract.get("family_id") == FAMILY_ID
        and contract.get("successor_id")
        == "cross-style-etf-breadth-continuation-v1"
        and len(contract.get("trial_family", [])) == 1
        and search.get("family_contract") == contract
        and failure.get("family_id") == FAMILY_ID
        and failure.get("failure_code")
        == "INCOMPLETE_FIXED_DAILY_SYMBOL_RANGE"
        and failure.get("data_outcomes_accessed") is True
        and failure.get("strategy_metrics_accessed") is False
        and failure.get("confirmation_outcomes_accessed") is False
        and inspection.get("failure_sha256")
        == failure["artifact_sha256"]
        and inspection.get("state")
        == "COLLECTION_FAILURE_INSPECTED"
        and all(inspection.get("checks", {}).values())
    ):
        raise CrossStyleBreadthSuccessorError(
            "v1 adverse source-capacity lineage drifted"
        )
    return {
        "contract": contract,
        "search": search,
        "failure": failure,
        "inspection": inspection,
    }


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    full = predecessor_support._full_sessions()
    warmup = [
        day for day in full if "2016-01-04" <= day <= "2016-10-17"
    ]
    development = [
        day for day in full if "2016-10-18" <= day <= "2018-12-31"
    ]
    year_2019_onward = [
        day for day in full if "2019-01-02" <= day <= "2020-12-31"
    ]
    embargo = year_2019_onward[:EMBARGO_SESSIONS]
    confirmation = year_2019_onward[EMBARGO_SESSIONS:]
    confirmation_start = full.index(confirmation[0])
    confirmation_warmup = full[
        confirmation_start - WARMUP_SESSIONS : confirmation_start
    ]
    if not (
        len(warmup) == 200
        and len(development) == 548
        and len(embargo) == 5
        and len(confirmation_warmup) == 200
        and len(confirmation) == 495
    ):
        raise CrossStyleBreadthSuccessorError(
            "v2 source-capacity partitions drifted"
        )
    return (
        warmup,
        development,
        predecessor_support._weekly_decisions(development),
        embargo,
        confirmation_warmup,
        confirmation,
        predecessor_support._weekly_decisions(confirmation),
    )


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    matching = [
        record
        for record in records
        if record["exposure_id"] == PREDECESSOR_EXPOSURE_ID
    ]
    overlaps = outcome_exposure.find_overlaps(
        contract["development_scope"], records
    )
    if not (
        len(matching) == 1
        and overlaps
        and {item["exposure_id"] for item in overlaps}
        == {PREDECESSOR_EXPOSURE_ID}
        and matching[0]["lane"] == "development"
        and matching[0]["source_sha256"]
        == _predecessor_graph(enforce_commit=False)["failure"][
            "artifact_sha256"
        ]
    ):
        raise CrossStyleBreadthSuccessorError(
            "v2 development is not bound to the sole declared contamination"
        )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    outcome_exposure.assert_disjoint(
        [contract["development_scope"], contract["confirmation_scope"]]
    )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    predecessor = _predecessor_graph(enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar_inspection = predecessor_support._calendar_authority(
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
    provisional = {
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
    }
    _validate_exposure_state(provisional)
    evidence_paths = [
        _repo_path(PREDECESSOR_CONTRACT),
        _repo_path(PREDECESSOR_SEARCH),
        _repo_path(PREDECESSOR_FAILURE),
        _repo_path(PREDECESSOR_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
        _repo_path(CALENDAR_PATH),
        _repo_path(CALENDAR_INSPECTION),
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
                    "capacity_unit": "frozen weekly development decisions",
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
    base = {
        key: value
        for key, value in predecessor["contract"].items()
        if key
        not in {
            "implementation_hashes",
            "rolling_origin_plan",
            "trial_family",
            "primary_trial_id",
            "rolling_active_family_slot",
            "rolling_slot_authority",
        }
    }
    base.update(
        {
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "parent_experiment_id": predecessor["contract"][
                "experiment_id"
            ],
            "created_at": created_at,
            "research_generation": RESEARCH_GENERATION,
            "successor_id": SUCCESSOR_ID,
            "existing_successor_validator": {
                "module": "cross_style_breadth_successor",
                "function": "validate_existing_successor_contract",
            },
            "new_mechanism_family_slot_consumed": False,
            "prior_family_attempt_count": 1,
            "predecessor": {
                "contract_path": _repo_path(PREDECESSOR_CONTRACT),
                "contract_file_sha256": sha256_file(
                    PREDECESSOR_CONTRACT
                ),
                "search_path": _repo_path(PREDECESSOR_SEARCH),
                "search_sha256": predecessor["search"][
                    "artifact_sha256"
                ],
                "failure_path": _repo_path(PREDECESSOR_FAILURE),
                "failure_sha256": predecessor["failure"][
                    "artifact_sha256"
                ],
                "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
                "inspection_sha256": predecessor["inspection"][
                    "artifact_sha256"
                ],
                "outcome_exposure_id": PREDECESSOR_EXPOSURE_ID,
                "strategy_metrics_reused": False,
                "confirmation_evidence_reused": False,
                "parameter_grid_changed": False,
            },
            "mechanism": (
                "Preserve the exact v1 weekly cross-style risk-on rule while "
                "using the prospectively frozen, provider-supported history "
                "boundary and retaining v1 as contaminated adverse history."
            ),
            "material_difference_rationale": (
                "This is a source-capacity successor, not a rule repair: "
                "the single parameter combination and evaluator are unchanged; "
                "only the evidence split moves to the independently observed "
                "Alpaca history boundary before any strategy metric existed."
            ),
            "contamination_risks": [
                "The 2016-2017 portion of development is explicitly contaminated by the inspected v1 source failure.",
                "No v1 strategy metric or confirmation outcome existed or is reused.",
                "New 2018 development and 2019-2020 confirmation dates freeze before successor provider access.",
                "The exact one-trial rule, symbols, costs, evaluator, and selection gates are unchanged.",
            ],
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
            "calendar_inspection_sha256": calendar_inspection[
                "artifact_sha256"
            ],
            "historical_data_contract": {
                "daily_provider": "alpaca",
                "daily_endpoint": "/v2/stocks/{symbol}/bars",
                "daily_feed": "sip",
                "daily_adjustment": "raw",
                "daily_request_mode": "symbol_range",
                "split_provider": "massive",
                "provider_substitutions_allowed": False,
            },
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "five_session_embargo": True,
                "warmup_feature_only": True,
                "contaminated_training_declared": True,
            },
            "implementation_files": [
                "cross_style_breadth_successor.py",
                "dense_collection_plan_inspection.py",
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
        }
    )
    contract = strategy_discovery._validate_family_contract(base)
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
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 1
        and contract.get("parameter_grid")
        == predecessor["contract"]["parameter_grid"]
        and len(contract.get("trial_family", [])) == 1
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
        and contract.get("confirmation_signal_capacity")
        == len(confirmation_signals)
        and contract.get("development_scope") == _scope(development)
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("existing_successor_validator")
        == {
            "module": "cross_style_breadth_successor",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("historical_data_contract", {}).get(
            "daily_provider"
        )
        == "alpaca"
    ):
        raise CrossStyleBreadthSuccessorError(
            "cross-style v2 successor contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    predecessor_support._calendar_authority(
        enforce_commit=enforce_commit
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
                    "development_signal_dates": len(
                        contract["development_signal_dates"]
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
        CrossStyleBreadthSuccessorError,
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
