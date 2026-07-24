"""Freeze an outcome-clean fixed-ETF replication of residual reversal."""

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
import flight_to_safety_replication as partition_support
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_RESIDUAL_REPLICATION_FAMILY
MECHANISM_FAMILY = "two-to-three-day-cross-sectional-reversal"
STRATEGY_ID = "liquid-etf-market-residual-reversal"
SUCCESSOR_ID = "liquid-etf-market-residual-reversal-replication-v1"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = partition_support.CALENDAR_PATH
CALENDAR_INSPECTION = partition_support.CALENDAR_INSPECTION
TARGET_SYMBOLS = list(runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS)
FEATURE_SYMBOLS = [runtime.ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL]
SYMBOLS = [*TARGET_SYMBOLS, *FEATURE_SYMBOLS]
PREDECESSOR_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-equity-market-residual-reversal-replication/search/"
    "liquid-equity-market-residual-reversal-replication-search-"
    "d4008bcb6190bf70fe81caada532ccc8afb57bfbd2288a34f57d866b2cd6079f.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-equity-market-residual-reversal-replication/"
    "development-inspection/"
    "liquid-equity-market-residual-reversal-replication-"
    "development-inspection-"
    "5af65a86d919ed857c6d36adfc0df7e29b1ec0781b8099027595f5131aa275a9.json"
)


class EtfResidualReplicationError(ValueError):
    """The fixed-ETF replication contract or evidence graph drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EtfResidualReplicationError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise EtfResidualReplicationError(f"{field} needs a timezone")
    if parsed.date() > date.today():
        raise EtfResidualReplicationError(
            f"{field} cannot be future-dated"
        )
    return parsed


def _predecessor_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    for path in (PREDECESSOR_SEARCH, PREDECESSOR_INSPECTION):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    search = strategy_discovery.load_artifact(
        PREDECESSOR_SEARCH, expected_kind="frozen-development-search"
    )
    inspection = strategy_discovery.load_artifact(
        PREDECESSOR_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        search.get("state") == "SEARCH_FROZEN"
        and search.get("family_contract", {}).get("mechanism_family")
        == MECHANISM_FAMILY
        and len(search.get("family_contract", {}).get("trial_family", []))
        == 48
        and inspection.get("state") == "REJECTED"
        and inspection.get("family_id")
        == runtime.EQUITY_RESIDUAL_REPLICATION_FAMILY
        and inspection.get("selection", {}).get("selected_trial_id")
        is None
    ):
        raise EtfResidualReplicationError(
            "residual-reversal predecessor graph drifted"
        )
    return dict(search["family_contract"]), inspection


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    return partition_support._partitions()


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
        raise EtfResidualReplicationError(
            "ETF residual development scope has foreign outcome exposure"
        )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    base, predecessor = _predecessor_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar = partition_support._calendar_authority(
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
        _repo_path(PREDECESSOR_SEARCH),
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
    contract = copy.deepcopy(base)
    for field in (
        "capacity_manifest",
        "data_contract_path",
        "data_contract_sha256",
        "data_inspection_path",
        "data_inspection_sha256",
        "dataset_manifest",
        "implementation_hashes",
        "input_normalization",
        "primary_trial_id",
        "rolling_origin_plan",
        "structural_predecessor",
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
            "prior_family_attempt_count": 2,
            "existing_successor_validator": {
                "module": "etf_residual_replication",
                "function": "validate_existing_successor_contract",
            },
            "predecessor": {
                "search_path": _repo_path(PREDECESSOR_SEARCH),
                "search_sha256": base.get("development_search_sha256"),
                "search_artifact_sha256": strategy_discovery.load_artifact(
                    PREDECESSOR_SEARCH,
                    expected_kind="frozen-development-search",
                )["artifact_sha256"],
                "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
                "inspection_sha256": predecessor["artifact_sha256"],
                "state": predecessor["state"],
                "promotion_evidence_reused": False,
                "outcome_guided_parameter_change": False,
            },
            "mechanism": (
                "Buy the largest one- or three-session ETF downside "
                "residual versus SPY after a standardized overshoot."
            ),
            "entry_rule": (
                "After the completed close, subtract the matching SPY "
                "return from each frozen target ETF return, standardize "
                "against the preceding sixty observations, require the "
                "frozen downside z threshold and SPY trend gate, then enter "
                "the most negative eligible residual at the next open."
            ),
            "ranking_rule": (
                "Most negative completed residual z-score, then lexical "
                "symbol; at most one new family entry per session."
            ),
            "universe_requirements": {
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "complete_frozen_daily_history": True,
                "fixed_denominator": True,
                "selection_basis": (
                    "Ten long-history liquid U.S. style, size, and broad "
                    "market ETFs whose target outcome pairs were globally "
                    "untouched when frozen."
                ),
            },
            "universe": {
                "symbols": list(SYMBOLS),
                "target_symbols": list(TARGET_SYMBOLS),
                "feature_symbols": list(FEATURE_SYMBOLS),
                "point_in_time": True,
            },
            "historical_data_contract": {
                "daily_provider": "alpaca",
                "daily_endpoint": "/v2/stocks/{symbol}/bars",
                "daily_request_mode": "symbol_range",
                "daily_feed": "sip",
                "daily_adjustment": "raw",
                "split_provider": "massive",
                "provider_substitutions_allowed": False,
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
                "full_2020_embargo": True,
                "rolling_origin": True,
            },
            "contamination_risks": [
                "SPY is feature-only contaminated training input and is never a target return.",
                "All ten target ETF/date pairs are globally untouched at freeze.",
                "The rejected common-stock predecessor contributes adverse mechanism evidence only.",
                "The entire 2020 calendar is embargoed and 2021 target outcomes remain untouched.",
            ],
            "material_difference_rationale": (
                "This is a disjoint fixed-ETF replication of the already "
                "declared short-horizon market-residual reversal mechanism. "
                "It preserves the exact 48-trial grid, costs, entry timing, "
                "stops, holds, and selection gates while replacing the "
                "common-stock denominator with predeclared ETF identities."
            ),
            "production_compatibility_risks": [
                "The complete fixed ETF denominator and SPY history must be fresh.",
                "Overnight positions require confirmed GTC protection.",
                "Missing, halted, or structurally unprotectable targets are missed.",
            ],
            "plugin": {
                "module": "dense_strategy_plugin",
                "preflight": "preflight",
                "evaluate_development": "evaluate_development",
                "evaluate_confirmation": "evaluate_confirmation",
                "evaluate_production": "evaluate_production",
            },
            "implementation_files": [
                "etf_residual_replication.py",
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
    base, predecessor = _predecessor_graph(
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
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 2
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
        and contract.get("predecessor", {}).get("inspection_sha256")
        == predecessor["artifact_sha256"]
        and contract.get("existing_successor_validator")
        == {
            "module": "etf_residual_replication",
            "function": "validate_existing_successor_contract",
        }
    ):
        raise EtfResidualReplicationError(
            "fixed-ETF residual replication contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar = partition_support._calendar_authority(
        enforce_commit=enforce_commit
    )
    if (
        contract.get("calendar_inspection_sha256")
        != calendar["artifact_sha256"]
    ):
        raise EtfResidualReplicationError(
            "fixed-ETF residual calendar binding drifted"
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
        EtfResidualReplicationError,
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
