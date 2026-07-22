"""Freeze the v2 priority-one ETF trend capacity contract without outcomes."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import portfolio_successor_activation as successor


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = successor.CAMPAIGN_ID
AUTHORIZATION_SHA256 = (
    "6a7e27481ff80cd3e457d972c10347ea831801d6532fdc34b9e9fb0453c0fd6e"
)
AUTHORIZATION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/successor_proposal/authorizations/"
    f"{CAMPAIGN_ID}-authorization-{AUTHORIZATION_SHA256}.json"
)
ACTIVATION_STATUS_PATH = successor.DEFAULT_STATUS
INSPECTOR_PATH = PROJECT_ROOT / "multi_asset_etf_tsmom_capacity_inspection.py"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/multi_asset_etf_tsmom/manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/multi_asset_etf_tsmom/capacity-status.json"
)

THEME_ID = "multi-asset-etf-time-series-momentum"
CANDIDATE_ID = "multi-asset-etf-tsmom-v1"
STRATEGY_VERSION = "2026-07-22-multi-asset-etf-tsmom-v1"
ISO_WEEK = "2026-W30"
FAMILY_SLOT = 1
UNIVERSE = (
    ("SPY", "us-large-cap-equity"),
    ("EFA", "developed-ex-us-equity"),
    ("EEM", "emerging-markets-equity"),
    ("IEF", "intermediate-us-treasury"),
    ("GLD", "gold"),
    ("DBC", "broad-commodities"),
    ("UUP", "us-dollar"),
)
COLLECTION_START = "2021-12-01"
COLLECTION_END_EXCLUSIVE = "2026-01-06"
EVALUATION_WEEK_START = "2023-01-02"
EVALUATION_WEEK_END = "2025-12-29"
LOOKBACK_SESSIONS = 252
MINIMUM_STAGE0_SIGNALS = 30
MINIMUM_TOTAL_HISTORICAL_SIGNALS = 50


class MultiAssetEtfTsmomCapacityError(RuntimeError):
    """The outcome-blind capacity contract is missing or inconsistent."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MultiAssetEtfTsmomCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MultiAssetEtfTsmomCapacityError(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _assert_campaign_ready() -> dict[str, Any]:
    authorization = successor.load_authorization(AUTHORIZATION_PATH)
    status = _read_json(ACTIVATION_STATUS_PATH)
    if not (
        authorization.get("authorization_sha256") == AUTHORIZATION_SHA256
        and authorization.get("objective") == "FIRST_PILOT_READY_LIVE_STARTED"
        and authorization.get("first_authorized_action")
        == "PRIORITY_ONE_OUTCOME_BLIND_CAPACITY_PREFLIGHT"
        and authorization.get("scope", {}).get(
            "maximum_new_mechanism_families_per_iso_week"
        )
        == 3
        and status.get("campaign_id") == CAMPAIGN_ID
        and status.get("authorization_sha256") == AUTHORIZATION_SHA256
        and status.get("status") == "AUTHORIZED_READY"
        and status.get("candidate_preregistration_permitted") is True
        and status.get("provider_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise MultiAssetEtfTsmomCapacityError("v2 campaign is not ready")
    return authorization


def build_contract() -> dict[str, Any]:
    """Build the exact capacity contract without opening the historical store."""

    authorization = _assert_campaign_ready()
    bound_files = (
        Path(__file__).resolve(),
        INSPECTOR_PATH,
        AUTHORIZATION_PATH,
        PROJECT_ROOT / "PORTFOLIO_VALIDATION_V2.md",
        PROJECT_ROOT / "portfolio_config.toml",
        PROJECT_ROOT / "portfolio_maturity.py",
    )
    universe = [
        {"symbol": symbol, "asset_class": asset_class, "weighting": "not-applicable"}
        for symbol, asset_class in UNIVERSE
    ]
    contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "authorization_sha256": AUTHORIZATION_SHA256,
        "objective": authorization["objective"],
        "iso_week": ISO_WEEK,
        "new_mechanism_family_slot": FAMILY_SLOT,
        "maximum_new_mechanism_families_this_iso_week": 3,
        "theme_id": THEME_ID,
        "candidate_id": CANDIDATE_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": THEME_ID,
        "mechanism": (
            "long-only persistence in each scheduled ETF's own 252-session price "
            "history"
        ),
        "distinctness_contract": {
            "uses_own_price_history_only": True,
            "cross_sectional_ranking_used": False,
            "sector_rotation_used": False,
            "opening_range_or_intraday_pullback_used": False,
            "failed_v1_parameters_reused_or_repaired": False,
        },
        "universe": universe,
        "source_contract": {
            "provider": "IBKR",
            "historical_store_outside_repository": True,
            "symbols": [item[0] for item in UNIVERSE],
            "collection_start": COLLECTION_START,
            "collection_end_exclusive": COLLECTION_END_EXCLUSIVE,
            "timeframe": "1d",
            "channel": "trades",
            "regular_trading_hours_only": True,
            "feed": "smart",
            "adjustment": "provider_adjusted_unknown_basis",
            "permitted_capacity_fields": ["timestamp", "close"],
            "provider_substitution_permitted": False,
        },
        "schedule_contract": {
            "anchor_iso_week_monday": EVALUATION_WEEK_START,
            "last_iso_week_monday": EVALUATION_WEEK_END,
            "one_candidate_opportunity_per_iso_week": True,
            "scheduled_symbol_rule": (
                "whole ISO weeks since anchor modulo the frozen universe order"
            ),
            "opportunity_session_rule": (
                "first complete common regular session in the scheduled ISO week"
            ),
            "decision_session_rule": (
                "last complete common regular session before the opportunity session"
            ),
            "maximum_candidate_opportunities_per_week": 1,
            "date_or_symbol_substitution_permitted": False,
        },
        "signal_contract": {
            "lookback_sessions": LOOKBACK_SESSIONS,
            "formula": "decision_close / close_252_complete_common_sessions_ago - 1",
            "long_signal_rule": "strictly_greater_than_zero",
            "decision_close_available_before_candidate_entry": True,
            "future_bar_access_permitted": False,
            "cross_asset_values_used": False,
            "signal_threshold_tuning_permitted": False,
        },
        "missing_data_contract": {
            "incomplete_common_session": "exclude_from_common_session_calendar",
            "missing_scheduled_decision_or_lookback": "ineligible_preserve_denominator",
            "alternate_symbol_or_date_substitution_permitted": False,
            "minimum_lookback_must_be_exact": True,
        },
        "capacity_gate": {
            "minimum_stage0_signals": MINIMUM_STAGE0_SIGNALS,
            "minimum_total_historical_signals": MINIMUM_TOTAL_HISTORICAL_SIGNALS,
            "capacity_pass_rule": (
                "causal_long_signals_at_least minimum_total_historical_signals"
            ),
            "negative_disposition": (
                "retire this exact candidate without parameter repair on this corpus"
            ),
        },
        "access_contract": {
            "provider_collection_before_inspection_permitted": False,
            "provider_collection_after_inspection_permitted": True,
            "capacity_signal_count_after_inspection_permitted": True,
            "entry_fill_access_permitted": False,
            "exit_or_stop_access_permitted": False,
            "forward_return_computation_permitted": False,
            "stage0_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "stage0_handoff": {
            "full_rules_and_outcome_contract_frozen_after_capacity_pass": True,
            "independent_zero-result_inspection_required": True,
            "maximum_holding_sessions": 5,
            "cost_grid_bps_per_side": [5, 10, 20],
            "capacity_result_eligible_for_maturity": False,
        },
        "implementation_and_authority_hashes": {
            str(path.relative_to(PROJECT_ROOT)): successor._hash_file(path)
            for path in bound_files
        },
        "claim_limit": (
            "This artifact freezes only a causal capacity test. It contains no "
            "signal count, fill, exit, return, profit factor, or maturity evidence."
        ),
    }
    contract["contract_sha256"] = successor._self_hash(contract, "contract_sha256")
    return contract


def default_contract_path(contract: Mapping[str, Any]) -> Path:
    return DEFAULT_OUTPUT_ROOT / f"{CANDIDATE_ID}-{contract['contract_sha256']}.json"


def load_contract(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == successor._self_hash(value, "contract_sha256")
        and path.name == f"{CANDIDATE_ID}-{digest}.json"
    ):
        raise MultiAssetEtfTsmomCapacityError(
            "capacity contract was mutated or renamed"
        )
    return value


def write_contract(
    *, output_root: Path = DEFAULT_OUTPUT_ROOT, status_path: Path = DEFAULT_STATUS
) -> tuple[Path, dict[str, Any]]:
    contract = build_contract()
    path = output_root / f"{CANDIDATE_ID}-{contract['contract_sha256']}.json"
    if path.exists() and _read_json(path) != contract:
        raise MultiAssetEtfTsmomCapacityError(
            "content-addressed capacity contract has other content"
        )
    _write_json(contract, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "candidate_id": CANDIDATE_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "CAPACITY_CONTRACT_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "capacity_signal_count_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, contract


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, contract = write_contract()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": contract["contract_sha256"],
                "provider_access_permitted": False,
                "outcome_access_permitted": False,
            }
        else:
            result = _read_json(DEFAULT_STATUS)
    except (MultiAssetEtfTsmomCapacityError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
