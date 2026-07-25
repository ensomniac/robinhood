"""Freeze an exact sector-SPDR residual-reversal temporal replication."""

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
import etf_pullback_replication as calendar_support
import etf_residual_replication as v1
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_RESIDUAL_REPLICATION_V4_FAMILY
MECHANISM_FAMILY = v1.MECHANISM_FAMILY
STRATEGY_ID = "liquid-etf-market-residual-reversal-replication-v4"
SUCCESSOR_ID = STRATEGY_ID
RESEARCH_GENERATION = v1.RESEARCH_GENERATION
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = calendar_support.CALENDAR_PATH
TARGET_SYMBOLS = list(runtime.ETF_RESIDUAL_REPLICATION_V4_TARGET_SYMBOLS)
FEATURE_SYMBOLS = [runtime.ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL]
SYMBOLS = [*TARGET_SYMBOLS, *FEATURE_SYMBOLS]
WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_200
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 500
MAXIMUM_HOLD_SESSIONS = 5
TOTAL_SESSIONS = (
    WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)
V3_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "liquid-etf-market-residual-reversal-replication-v3/family-contract/"
    "contract-e4915ebdf178a5ca0ac3e1669e39074a79a099442c165da89630a8415a2f38ee.json"
)
V3_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v3/search/"
    "liquid-etf-market-residual-reversal-replication-v3-search-"
    "ee45e8fae09e90b7c1d5d7b36c5862d40a9a32b4f1389eeb742747ab1d7fda82.json"
)
V3_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v3/development/"
    "liquid-etf-market-residual-reversal-replication-v3-development-"
    "e04bf02280d8a3acbb24f85fbb39d4a881659572d875860320047100380b9f78.json"
)
V3_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-etf-market-residual-reversal-replication-v3/"
    "development-inspection/"
    "liquid-etf-market-residual-reversal-replication-v3-"
    "development-inspection-"
    "b8ecf3a1e7f0c9b34fd85096f6404ffaa29196462bc157567af02a621ead75c4.json"
)


class EtfResidualReplication4Error(ValueError):
    """The sector-SPDR replication or its evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EtfResidualReplication4Error(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise EtfResidualReplication4Error(f"{field} needs a timezone")
    if parsed.date() > date.today():
        raise EtfResidualReplication4Error(
            f"{field} cannot be future-dated"
        )
    return parsed


def _read_contract(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EtfResidualReplication4Error(
            f"cannot read predecessor contract: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise EtfResidualReplication4Error(
            "predecessor contract must contain an object"
        )
    return value


def _predecessor_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    for path in (V3_CONTRACT, V3_SEARCH, V3_RESULT, V3_INSPECTION):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    contract = _read_contract(V3_CONTRACT)
    search = strategy_discovery.load_artifact(
        V3_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        V3_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        V3_INSPECTION, expected_kind="development-search-inspection"
    )
    if not (
        contract.get("family_id")
        == runtime.ETF_RESIDUAL_REPLICATION_V3_FAMILY
        and len(contract.get("trial_family", [])) == 48
        and search.get("state") == "SEARCH_FROZEN"
        and search.get("family_contract", {}).get("parameter_grid")
        == contract.get("parameter_grid")
        and result.get("state") == "DEVELOPMENT_EVALUATED"
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("selection", {}).get("selected_trial_id") is None
    ):
        raise EtfResidualReplication4Error(
            "v3 rejected predecessor graph drifted"
        )
    return contract, search, inspection


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    dates = calendar_support._calendar_dates()
    if len(dates) < TOTAL_SESSIONS:
        raise EtfResidualReplication4Error(
            "pre-2016 calendar has insufficient sessions"
        )
    selected = dates[-TOTAL_SESSIONS:]
    warmup = selected[:WARMUP_SESSIONS]
    development = selected[
        WARMUP_SESSIONS : WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    ]
    embargo_start = WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    embargo = selected[embargo_start : embargo_start + EMBARGO_SESSIONS]
    confirmation = selected[-CONFIRMATION_SESSIONS:]
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
        and matching[0]["scope"] == contract["development_scope"]
        and matching[0]["source_path"].startswith(expected_prefix)
    ):
        raise EtfResidualReplication4Error(
            "v4 development scope has foreign outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    base, search, predecessor = _predecessor_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar_inspection_path, calendar_inspection = (
        calendar_support._calendar_data_inspection(
            enforce_commit=enforce_commit
        )
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
        _repo_path(calendar_support.CALENDAR_SOURCE_PATH),
        _repo_path(calendar_inspection_path),
        _repo_path(V3_CONTRACT),
        _repo_path(V3_SEARCH),
        _repo_path(V3_RESULT),
        _repo_path(V3_INSPECTION),
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
                    "confirmation_signal_dates": len(confirmation_signals),
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
        "capacity_manifest",
        "data_contract_path",
        "data_contract_sha256",
        "data_inspection_path",
        "data_inspection_sha256",
        "dataset_manifest",
        "development_search_sha256",
        "implementation_hashes",
        "input_normalization",
        "predecessor",
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
                "module": "etf_residual_replication4",
                "function": "validate_existing_successor_contract",
            },
            "predecessor": {
                "contract_path": _repo_path(V3_CONTRACT),
                "contract_file_sha256": sha256_file(V3_CONTRACT),
                "search_path": _repo_path(V3_SEARCH),
                "search_sha256": search["artifact_sha256"],
                "result_path": _repo_path(V3_RESULT),
                "result_file_sha256": sha256_file(V3_RESULT),
                "inspection_path": _repo_path(V3_INSPECTION),
                "inspection_sha256": predecessor["artifact_sha256"],
                "state": "REJECTED",
                "promotion_evidence_reused": False,
                "parameter_grid_changed": False,
            },
            "universe_requirements": {
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "complete_frozen_daily_history": True,
                "fixed_denominator": True,
                "selection_basis": (
                    "Nine sector SPDR target ETFs with inception before the "
                    "frozen pre-2016 window and globally untouched target "
                    "date pairs."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "point_in_time": True,
            },
            "development_warmup_dates": warmup,
            "development_dates": development,
            "development_signal_dates": development_signals,
            "development_scope": development_scope,
            "embargo_dates": embargo,
            "confirmation_warmup_dates": confirmation_warmup,
            "confirmation_dates": confirmation,
            "confirmation_signal_dates": confirmation_signals,
            "confirmation_signal_capacity": len(confirmation_signals),
            "confirmation_scope": confirmation_scope,
            "partitions": {
                "account_calendar_includes_zero_and_mark_to_market_days": True,
                "confirmation_untouched": True,
                "development_disjoint_replication": True,
                "five_session_embargo": True,
                "rolling_origin": True,
            },
            "contamination_risks": [
                "SPY is a feature-only input and never contributes a target return.",
                "All nine sector-SPDR target/date pairs are globally untouched at freeze.",
                "The rejected v3 result is adverse history and contributes no promotion evidence.",
                "The exact 48-trial grid is unchanged from the predecessor.",
                "Confirmation prices remain inaccessible until an exact winner is frozen.",
            ],
            "material_difference_rationale": (
                "This is an exact temporal and universe replication of the "
                "declared market-residual reversal mechanism. It changes no "
                "parameter, execution, cost, selection, or evidence gate; "
                "only globally untouched sector-SPDR identities and the "
                "inspected 2008-2015 calendar replace v3 evidence."
            ),
            "implementation_files": [
                "etf_residual_replication4.py",
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
            "calendar_path": _repo_path(CALENDAR_PATH),
            "calendar_inspection_path": _repo_path(
                calendar_inspection_path
            ),
            "calendar_inspection_sha256": calendar_inspection[
                "artifact_sha256"
            ],
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
    base, search, predecessor = _predecessor_graph(
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
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and contract.get("predecessor", {}).get("search_sha256")
        == search["artifact_sha256"]
        and contract.get("predecessor", {}).get("inspection_sha256")
        == predecessor["artifact_sha256"]
        and contract.get("existing_successor_validator")
        == {
            "module": "etf_residual_replication4",
            "function": "validate_existing_successor_contract",
        }
    ):
        raise EtfResidualReplication4Error(
            "sector-SPDR residual replication contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    inspection_path, inspection = (
        calendar_support._calendar_data_inspection(
            enforce_commit=enforce_commit
        )
    )
    if not (
        contract.get("calendar_inspection_path")
        == _repo_path(inspection_path)
        and contract.get("calendar_inspection_sha256")
        == inspection["artifact_sha256"]
        and inspection.get("calendar_sha256")
        == sha256_file(CALENDAR_PATH)
    ):
        raise EtfResidualReplication4Error(
            "sector-SPDR calendar binding drifted"
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
        EtfResidualReplication4Error,
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
