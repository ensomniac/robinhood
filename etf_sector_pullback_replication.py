"""Freeze an outcome-clean Vanguard-sector ETF pullback replication."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Mapping
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
FAMILY_ID = runtime.ETF_PULLBACK_REPLICATION_FAMILY
MECHANISM_FAMILY = "broad-etf-trend-pullback"
STRATEGY_ID = "vanguard-sector-etf-trend-pullback"
SUCCESSOR_ID = "vanguard-sector-etf-trend-pullback-v5"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/continuous_v2/"
    "session-calendar-2014-01-through-2022-12.json"
)
CALENDAR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "broad-etf-trend-pullback-v2-cost-floor/calendar/data-inspection/"
    "continuous-successor-calendar-data-inspection-"
    "f4da078f7a8b857d42b727efc543ec766b15f34d3f2d54d526e5f326a72e42b8.json"
)
BASE_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "macro-etf-trend-pullback-v4-outcome-clean-replication/"
    "family-contract/"
    "contract-35d42c5d8858f5ff3995cf8a738aa683"
    "bf074b959318e148a9eda4e40b0f3a4e.json"
)
BASE_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-trend-pullback-cost-floor/search/"
    "liquid-etf-trend-pullback-cost-floor-search-"
    "b2667de929472d5a1ada6a6d3da7bd9905b8b2e20cb7bd109e526ac2c0e33646.json"
)
BASE_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-trend-pullback-cost-floor/development/"
    "liquid-etf-trend-pullback-cost-floor-development-"
    "cb2f7224ff59eb77838a07482739e5cea9a339a22b9296009a147145898bc108.json"
)
BASE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-trend-pullback-cost-floor/development-inspection/"
    "liquid-etf-trend-pullback-cost-floor-development-inspection-"
    "ba93c8a2c50ce3c34effb27378d8c0f605297f9cd157e6cd1525b2842961d489.json"
)
SYMBOLS = list(runtime.ETF_PULLBACK_REPLICATION_SYMBOLS)


class EtfSectorPullbackReplicationError(ValueError):
    """The exact replication contract or adverse evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EtfSectorPullbackReplicationError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise EtfSectorPullbackReplicationError(
            f"{field} needs a timezone"
        )
    if parsed.date() > date.today():
        raise EtfSectorPullbackReplicationError(
            f"{field} cannot be future-dated"
        )
    return parsed


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EtfSectorPullbackReplicationError(
            f"cannot load {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise EtfSectorPullbackReplicationError(
            f"{path.name} is malformed"
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
    paths = (
        CALENDAR_PATH,
        CALENDAR_INSPECTION,
        BASE_CONTRACT,
        BASE_SEARCH,
        BASE_RESULT,
        BASE_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    calendar = strategy_discovery.load_artifact(
        CALENDAR_INSPECTION,
        expected_kind="continuous-successor-calendar-data-inspection",
    )
    base = _load_json(BASE_CONTRACT)
    search = strategy_discovery.load_artifact(
        BASE_SEARCH,
        expected_kind="frozen-development-search",
    )
    result = strategy_discovery.load_artifact(
        BASE_RESULT,
        expected_kind="development-search-result",
    )
    inspection = strategy_discovery.load_artifact(
        BASE_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        calendar.get("state") == "CALENDAR_INSPECTED_READY"
        and calendar.get("calendar_sha256") == sha256_file(CALENDAR_PATH)
        and base.get("family_id") == runtime.ETF_PULLBACK_FAMILY
        and base.get("mechanism_family") == MECHANISM_FAMILY
        and base.get("experiment_id")
        == "experiment-macro-etf-trend-pullback-v4-outcome-clean-replication"
        and len(base.get("trial_family", [])) == 32
        and search.get("family_contract", {}).get("experiment_id")
        == base["experiment_id"]
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
        and inspection.get("selection", {}).get("selected_trial_id")
        is None
    ):
        raise EtfSectorPullbackReplicationError(
            "v4 adverse predecessor evidence graph drifted"
        )
    return base, search, result, inspection


def _scope(dates: list[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"],
        records,
    )
    overlaps = outcome_exposure.find_overlaps(
        contract["development_scope"],
        records,
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
        raise EtfSectorPullbackReplicationError(
            "v5 pullback scope has foreign outcome exposure"
        )


def freeze_contract(
    *,
    created_at: str,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    base, search, result, inspection = _predecessor_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    development_scope = _scope(
        [
            *base["development_warmup_dates"],
            *base["development_dates"],
        ]
    )
    confirmation_scope = _scope(base["confirmation_dates"])
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    requested_dates = sorted(
        {
            *base["development_warmup_dates"],
            *base["development_dates"],
            *base["embargo_dates"],
            *base["confirmation_warmup_dates"],
            *base["confirmation_dates"],
        }
    )
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": requested_dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    _repo_path(CALENDAR_PATH),
                    _repo_path(CALENDAR_INSPECTION),
                    _repo_path(BASE_CONTRACT),
                    _repo_path(BASE_SEARCH),
                    _repo_path(BASE_RESULT),
                    _repo_path(BASE_INSPECTION),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": len(base["development_dates"]),
                    "capacity_unit": (
                        "frozen max-one-entry development decision dates"
                    ),
                    "development_sessions": len(
                        base["development_dates"]
                    ),
                    "embargo_sessions": len(base["embargo_dates"]),
                    "confirmation_sessions": len(
                        base["confirmation_dates"]
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
            "prior_family_attempt_count": 5,
            "existing_successor_validator": {
                "module": "etf_sector_pullback_replication",
                "function": "validate_existing_successor_contract",
            },
            "predecessor": {
                "contract_path": _repo_path(BASE_CONTRACT),
                "contract_file_sha256": sha256_file(BASE_CONTRACT),
                "search_path": _repo_path(BASE_SEARCH),
                "search_sha256": search["artifact_sha256"],
                "result_path": _repo_path(BASE_RESULT),
                "result_sha256": result["artifact_sha256"],
                "inspection_path": _repo_path(BASE_INSPECTION),
                "inspection_sha256": inspection["artifact_sha256"],
                "disposition": "REJECTED",
                "promotion_evidence_reused": False,
                "parameter_grid_changed": False,
            },
            "universe_requirements": {
                "symbols": list(SYMBOLS),
                "complete_frozen_daily_history": True,
                "selection_basis": (
                    "Ten long-history Vanguard U.S. sector ETFs whose exact "
                    "2016-2022 symbol-date pairs were globally outcome-clean "
                    "before price access."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "point_in_time": True,
            },
            "development_scope": development_scope,
            "confirmation_scope": confirmation_scope,
            "contamination_risks": [
                "All v4 development outcomes are immutable adverse history and cannot satisfy v5 promotion.",
                "All ten v5 target ETF/date pairs were globally untouched before contract freeze.",
                "The exact 32-trial grid, evidence dates, costs, execution rules, and selection gates remain unchanged.",
            ],
            "material_difference_rationale": (
                "This existing-mechanism replication preserves the exact "
                "RSI2 trend-pullback search and all evidence gates while "
                "moving to a wholly disjoint Vanguard sector ETF basket "
                "selected from identity and inception history before prices."
            ),
            "historical_data_contract": {
                "daily_provider": "alpaca",
                "daily_endpoint": "/v2/stocks/{symbol}/bars",
                "daily_feed": "sip",
                "daily_adjustment": "raw",
                "daily_request_mode": "symbol_range",
                "split_provider": "massive",
                "provider_substitutions_allowed": False,
            },
            "implementation_files": [
                "etf_sector_pullback_replication.py",
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
            "confirmation_signal_capacity": len(
                base["confirmation_dates"]
            ),
        }
    )
    contract = strategy_discovery._validate_family_contract(contract)
    validate_existing_successor_contract(
        contract,
        enforce_commit=enforce_commit,
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
    contract: Mapping[str, Any],
    *,
    enforce_commit: bool = True,
) -> None:
    base, search, result, inspection = _predecessor_graph(
        enforce_commit=enforce_commit
    )
    expected_provider = {
        "daily_provider": "alpaca",
        "daily_endpoint": "/v2/stocks/{symbol}/bars",
        "daily_feed": "sip",
        "daily_adjustment": "raw",
        "daily_request_mode": "symbol_range",
        "split_provider": "massive",
        "provider_substitutions_allowed": False,
    }
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 5
        and contract.get("parameter_grid") == base["parameter_grid"]
        and len(contract.get("trial_family", [])) == 32
        and contract.get("development_warmup_dates")
        == base["development_warmup_dates"]
        and contract.get("development_dates")
        == base["development_dates"]
        and contract.get("embargo_dates") == base["embargo_dates"]
        and contract.get("confirmation_warmup_dates")
        == base["confirmation_warmup_dates"]
        and contract.get("confirmation_dates")
        == base["confirmation_dates"]
        and contract.get("development_scope")
        == _scope(
            [
                *base["development_warmup_dates"],
                *base["development_dates"],
            ]
        )
        and contract.get("confirmation_scope")
        == _scope(base["confirmation_dates"])
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("historical_data_contract") == expected_provider
        and contract.get("predecessor", {}).get("search_sha256")
        == search["artifact_sha256"]
        and contract.get("predecessor", {}).get("result_sha256")
        == result["artifact_sha256"]
        and contract.get("predecessor", {}).get("inspection_sha256")
        == inspection["artifact_sha256"]
        and contract.get("existing_successor_validator")
        == {
            "module": "etf_sector_pullback_replication",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and contract.get("outcome_exposure_index_sha256")
        == outcome_exposure.audit()["index_sha256"]
    ):
        raise EtfSectorPullbackReplicationError(
            "v5 Vanguard-sector pullback contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
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
        EtfSectorPullbackReplicationError,
        OSError,
        ValueError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(
            json.dumps(
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
