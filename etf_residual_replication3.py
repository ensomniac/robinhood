"""Freeze the corrected, identity-safe ETF residual-reversal successor."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import etf_residual_replication as v1
import etf_residual_replication2 as v2
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_RESIDUAL_REPLICATION_V3_FAMILY
MECHANISM_FAMILY = v1.MECHANISM_FAMILY
STRATEGY_ID = "liquid-etf-market-residual-reversal-replication-v3"
SUCCESSOR_ID = "liquid-etf-market-residual-reversal-replication-v3"
RESEARCH_GENERATION = v1.RESEARCH_GENERATION
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = v1.CALENDAR_PATH
CALENDAR_INSPECTION = v1.CALENDAR_INSPECTION
TARGET_SYMBOLS = list(
    runtime.ETF_RESIDUAL_REPLICATION_V3_TARGET_SYMBOLS
)
FEATURE_SYMBOLS = [runtime.ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL]
SYMBOLS = [*TARGET_SYMBOLS, *FEATURE_SYMBOLS]
V2_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "liquid-etf-market-residual-reversal-replication-v2/"
    "family-contract/"
    "contract-75a33ce2fb819f97aaec93ecb2f9e618862598d65ed8d4c421431862c2d0f033.json"
)
V2_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v2/search/"
    "liquid-etf-market-residual-reversal-replication-v2-search-"
    "d42f7a6416cdf97bddd8c807a7e1a9b184037d0c194885310b72ca2d26d8c105.json"
)
V2_DATA_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v2/"
    "development-collection-inspection/"
    "liquid-etf-market-residual-reversal-replication-v2-"
    "development-collection-inspection-"
    "11d797f89c6fb23c5e716112bad9f46f4172281467e5ead3a384819668b44a50.json"
)
V2_EXPOSURE_ID = (
    "development-evaluation-failure-etf-residual-v2-20260724"
)
V2_DEVELOPMENT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v2/development"
)
V2_DEVELOPMENT_INSPECTION_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v2/development-inspection"
)


class EtfResidualReplication3Error(ValueError):
    """The corrected successor or its failure boundary drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EtfResidualReplication3Error(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise EtfResidualReplication3Error(
            f"{field} needs a timezone"
        )
    if parsed.date() > date.today():
        raise EtfResidualReplication3Error(
            f"{field} cannot be future-dated"
        )
    return parsed


def _predecessor_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    for path in (V2_CONTRACT, V2_SEARCH, V2_DATA_INSPECTION):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    contract = v2._read_contract(V2_CONTRACT)
    search = strategy_discovery.load_artifact(
        V2_SEARCH, expected_kind="frozen-development-search"
    )
    inspection = strategy_discovery.load_artifact(
        V2_DATA_INSPECTION,
        expected_kind="dense-data-collection-inspection",
    )
    matches = [
        record
        for record in outcome_exposure.read_index()
        if record["exposure_id"] == V2_EXPOSURE_ID
    ]
    development_paths = sorted(V2_DEVELOPMENT_ROOT.glob("*.json"))
    development_boundary_valid = not development_paths
    if development_paths:
        inspection_paths = sorted(
            V2_DEVELOPMENT_INSPECTION_ROOT.glob("*.json")
        )
        if len(development_paths) == 1 and len(inspection_paths) == 1:
            if enforce_commit:
                strategy_discovery.require_committed(development_paths[0])
                strategy_discovery.require_committed(inspection_paths[0])
            development_result = strategy_discovery.load_artifact(
                development_paths[0],
                expected_kind="development-search-result",
            )
            development_inspection = strategy_discovery.load_artifact(
                inspection_paths[0],
                expected_kind="development-search-inspection",
            )
            development_boundary_valid = (
                development_result.get("state") == "DEVELOPMENT_EVALUATED"
                and development_inspection.get("state") == "REJECTED"
                and development_inspection.get("result_path")
                == _repo_path(development_paths[0])
                and development_inspection.get("result_sha256")
                == development_result.get("artifact_sha256")
            )
    if not (
        contract.get("family_id") == v2.FAMILY_ID
        and search.get("family_contract", {}).get("parameter_grid")
        == contract.get("parameter_grid")
        and search.get("state") == "SEARCH_FROZEN"
        and inspection.get("state") == "DATASET_INSPECTED_READY"
        and len(matches) == 1
        and matches[0]["lane"] == "development"
        and matches[0]["scope"] == contract["development_scope"]
        and matches[0]["source_path"] == _repo_path(V2_DATA_INSPECTION)
        and matches[0]["source_sha256"] == sha256_file(V2_DATA_INSPECTION)
        and development_boundary_valid
    ):
        raise EtfResidualReplication3Error(
            "v2 evaluation-failure boundary drifted"
        )
    return contract, search, matches[0]


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
        raise EtfResidualReplication3Error(
            "v3 ETF residual scope has foreign outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    base, search, exposure = _predecessor_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar = v1.partition_support._calendar_authority(
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
        _repo_path(V2_CONTRACT),
        _repo_path(V2_SEARCH),
        _repo_path(V2_DATA_INSPECTION),
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
            "research_generation": RESEARCH_GENERATION,
            "new_mechanism_family_slot_consumed": False,
            "prior_family_attempt_count": 4,
            "existing_successor_validator": {
                "module": "etf_residual_replication3",
                "function": "validate_existing_successor_contract",
            },
            "predecessor": {
                "contract_path": _repo_path(V2_CONTRACT),
                "contract_file_sha256": sha256_file(V2_CONTRACT),
                "search_path": _repo_path(V2_SEARCH),
                "search_sha256": search["artifact_sha256"],
                "dataset_inspection_path": _repo_path(
                    V2_DATA_INSPECTION
                ),
                "dataset_inspection_sha256": (
                    strategy_discovery.load_artifact(
                        V2_DATA_INSPECTION,
                        expected_kind="dense-data-collection-inspection",
                    )["artifact_sha256"]
                ),
                "exposure_id": exposure["exposure_id"],
                "exposure_record_sha256": exposure["record_sha256"],
                "failure_code": (
                    "SHARED_STRESSED_COST_SIGNAL_IDENTITY"
                ),
                "result_artifact_emitted": False,
                "promotion_evidence_reused": False,
                "parameter_grid_changed": False,
            },
            "universe_requirements": {
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "complete_frozen_daily_history": True,
                "fixed_denominator": True,
                "selection_basis": (
                    "Ten inception-2000 iShares U.S. sector ETFs replacing "
                    "every outcome-exposed v2 target identity."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "point_in_time": True,
            },
            "development_scope": development_scope,
            "confirmation_scope": confirmation_scope,
            "contamination_risks": [
                "SPY remains feature-only contaminated training input and is never a target return.",
                "All ten v3 target ETF/date pairs are globally untouched at freeze.",
                "No v2 result artifact or selection metric is reused for promotion.",
                "The complete 48-trial grid and all evidence dates remain unchanged.",
                "All of 2020 remains embargo and 2021 v3 target outcomes are untouched.",
            ],
            "material_difference_rationale": (
                "This structural successor binds the corrected cross-cost "
                "quantity replay to a wholly disjoint target basket after "
                "the v2 evaluator failed before emitting a result. It "
                "changes no mechanism, parameter, date, cost, selection, "
                "execution, or evidence gate."
            ),
            "implementation_files": [
                "etf_residual_replication3.py",
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
            "outcome_exposure_index_sha256": outcome_exposure.audit()[
                "index_sha256"
            ],
            "calendar_inspection_sha256": calendar["artifact_sha256"],
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
    base, search, exposure = _predecessor_graph(
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
        and contract.get("prior_family_attempt_count") == 4
        and contract.get("parameter_grid") == base["parameter_grid"]
        and len(contract.get("trial_family", [])) == 48
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
        and contract.get("predecessor", {}).get("search_sha256")
        == search["artifact_sha256"]
        and contract.get("predecessor", {}).get("exposure_record_sha256")
        == exposure["record_sha256"]
        and contract.get("existing_successor_validator")
        == {
            "module": "etf_residual_replication3",
            "function": "validate_existing_successor_contract",
        }
    ):
        raise EtfResidualReplication3Error(
            "corrected ETF residual contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar = v1.partition_support._calendar_authority(
        enforce_commit=enforce_commit
    )
    if (
        contract.get("calendar_inspection_sha256")
        != calendar["artifact_sha256"]
    ):
        raise EtfResidualReplication3Error(
            "corrected ETF residual calendar binding drifted"
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
        EtfResidualReplication3Error,
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
