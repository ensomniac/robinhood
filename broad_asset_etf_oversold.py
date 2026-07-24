"""Freeze an outcome-clean broad-asset ETF oversold-reversal replication."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import high_beta_etf_oversold as predecessor
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.BROAD_ASSET_ETF_OVERSOLD_FAMILY
MECHANISM_FAMILY = predecessor.MECHANISM_FAMILY
STRATEGY_ID = "broad-asset-etf-oversold-reversal"
SUCCESSOR_ID = "broad-asset-etf-oversold-reversal-v1-outcome-clean"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
SYMBOLS = ["AGG", "HYG", "LQD", "MDY", "USO", "UUP", "VNQ", "VOO", "VTI"]
CALENDAR_PATH = predecessor.CALENDAR_PATH
DEVELOPMENT_WARMUP_SESSIONS = predecessor.DEVELOPMENT_WARMUP_SESSIONS
DEVELOPMENT_SESSIONS = predecessor.DEVELOPMENT_SESSIONS
EMBARGO_SESSIONS = predecessor.EMBARGO_SESSIONS
CONFIRMATION_SESSIONS = predecessor.CONFIRMATION_SESSIONS
TOTAL_SESSIONS = predecessor.TOTAL_SESSIONS
BASE_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "high-beta-etf-oversold-reversal-v1-outcome-clean/family-contract/"
    "contract-4bc808a0a3fd02321e48845fe1e7db7a62206b916318044132a97b7ec8849b82.json"
)
PREDECESSOR_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/high-beta-etf-oversold-reversal/search/"
    "high-beta-etf-oversold-reversal-search-"
    "e6053ce5ccf01d0e574a675b9b662999a711040632fd674babf56c11feba5153.json"
)
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/high-beta-etf-oversold-reversal/development/"
    "high-beta-etf-oversold-reversal-development-"
    "20b82cb23b89e99205abdd6c0146fcb689abb46638148ae72f0a4f368dfb5ea2.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "high-beta-etf-oversold-reversal/development-inspection/"
    "high-beta-etf-oversold-reversal-development-inspection-"
    "f59817d3fbcb59162da369f94b8f44ff35c39bf0ced7a0dce96b534ac47b2ce9.json"
)


class BroadAssetEtfOversoldError(ValueError):
    """The broad-asset replication contract or evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _full_sessions() -> list[str]:
    return predecessor._full_sessions()


def _read_plain(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BroadAssetEtfOversoldError(
            f"adverse evidence cannot be loaded: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise BroadAssetEtfOversoldError(
            f"adverse evidence is malformed: {path.name}"
        )
    return value


def _adverse_predecessor(*, enforce_commit: bool) -> dict[str, dict[str, Any]]:
    paths = (
        predecessor.CALENDAR_PATH,
        predecessor.CALENDAR_INSPECTION,
        BASE_CONTRACT,
        PREDECESSOR_SEARCH,
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    base = _read_plain(BASE_CONTRACT)
    search = strategy_discovery.load_artifact(
        PREDECESSOR_SEARCH,
        expected_kind="frozen-development-search",
    )
    result = strategy_discovery.load_artifact(
        PREDECESSOR_RESULT,
        expected_kind="development-search-result",
    )
    inspection = strategy_discovery.load_artifact(
        PREDECESSOR_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        base.get("family_id") == predecessor.FAMILY_ID
        and base.get("mechanism_family") == MECHANISM_FAMILY
        and search.get("family_contract", {}).get("family_id")
        == predecessor.FAMILY_ID
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
    ):
        raise BroadAssetEtfOversoldError(
            "high-beta oversold adverse evidence graph drifted"
        )
    return {
        "base": base,
        "search": search,
        "result": result,
        "inspection": inspection,
    }


def _scope(dates: list[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(contract["confirmation_scope"], records)
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
        raise BroadAssetEtfOversoldError(
            "development scope has foreign or partial outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    artifact_support._timestamp(created_at, "created_at")
    adverse = _adverse_predecessor(enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())

    base = copy.deepcopy(adverse["base"])
    for field in (
        "implementation_hashes",
        "primary_trial_id",
        "rolling_origin_plan",
        "trial_family",
    ):
        base.pop(field, None)
    warmup = list(base["development_warmup_dates"])
    development = list(base["development_dates"])
    embargo = list(base["embargo_dates"])
    confirmation = list(base["confirmation_dates"])
    selected = [*warmup, *development, *embargo, *confirmation]
    development_scope = _scope([*warmup, *development])
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )

    evidence_paths = [
        _repo_path(predecessor.CALENDAR_PATH),
        _repo_path(predecessor.CALENDAR_INSPECTION),
        _repo_path(BASE_CONTRACT),
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
            "requested_dates": selected,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": len(development) * len(SYMBOLS),
                    "capacity_unit": (
                        "frozen instrument-session observations"
                    ),
                    "development_sessions": len(development),
                    "embargo_sessions": len(embargo),
                    "confirmation_sessions": len(confirmation),
                    "calendar_sha256": sha256_file(
                        predecessor.CALENDAR_PATH
                    ),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        DEFAULT_ROOT / SUCCESSOR_ID / "capacity",
    )

    contract: dict[str, Any] = {
        **base,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": base["experiment_id"],
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "existing_successor_validator": {
            "module": "broad_asset_etf_oversold",
            "function": "validate_existing_successor_contract",
        },
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 4,
        "predecessor": {
            "search_path": _repo_path(PREDECESSOR_SEARCH),
            "search_sha256": adverse["search"]["artifact_sha256"],
            "result_path": _repo_path(PREDECESSOR_RESULT),
            "result_sha256": adverse["result"]["artifact_sha256"],
            "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
            "inspection_sha256": adverse["inspection"]["artifact_sha256"],
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Replicate the frozen completed one-session oversold reversal rule "
            "without parameter repair across a broad, liquid, multi-asset ETF "
            "universe whose exact date-symbol pairs are outcome-clean."
        ),
        "universe_requirements": {
            "symbols": list(SYMBOLS),
            "complete_frozen_daily_history": True,
            "selection_basis": (
                "Nine liquid broad equity, rates, credit, real-asset, and "
                "currency ETFs inventoried before predecessor inspection and "
                "globally outcome-clean on the exact 2016-2022 partition."
            ),
        },
        "contamination_risks": [
            "The rejected high-beta ETF outcome can falsify that exact corpus but cannot count toward this replication.",
            "The exact broad-asset ETF date-symbol pairs are absent from the global exposure index before development.",
            "The 32-trial parameter grid is unchanged so predecessor outcomes cannot repair the rule.",
            "Warmup is feature-only and cannot count as target evidence.",
        ],
        "material_difference_rationale": (
            "This preregistered replication preserves the exact oversold rule "
            "grid while replacing the rejected high-beta ETF corpus with a "
            "fixed, disjoint broad-asset universe selected before that result."
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "symbols": list(SYMBOLS),
            "point_in_time": True,
        },
        "implementation_files": [
            "broad_asset_etf_oversold.py",
            "high_beta_etf_oversold.py",
            "sector_etf_gap_drift.py",
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
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 4
        and contract.get("selection_mode") == "development_search"
        and len(contract.get("trial_family", [])) == 32
        and contract.get("parameter_grid")
        == _adverse_predecessor(
            enforce_commit=enforce_commit
        )["base"]["parameter_grid"]
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("existing_successor_validator")
        == {
            "module": "broad_asset_etf_oversold",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("historical_data_contract")
        == _adverse_predecessor(
            enforce_commit=enforce_commit
        )["base"]["historical_data_contract"]
        and contract.get("calendar_path")
        == _repo_path(predecessor.CALENDAR_PATH)
        and isinstance(
            contract.get("outcome_exposure_index_sha256"), str
        )
        and len(contract["outcome_exposure_index_sha256"]) == 64
    ):
        raise BroadAssetEtfOversoldError(
            "broad-asset ETF oversold contract drifted"
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
                    "confirmation_access_permitted": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        BroadAssetEtfOversoldError,
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
