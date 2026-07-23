"""Prepare the exact three-family v2 batch without spending next week's budget."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import asr_capacity as capacity
import portfolio_maturity


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
TARGET_ISO_WEEK = "2026-W31"
ACTIVATION_NOT_BEFORE = date(2026, 7, 27)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/next_batch/plans"
DEFAULT_STATUS = PROJECT_ROOT / "strategy_tournament/v2/next_batch/status.json"
ASR_DISPOSITION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/asr/dispositions/"
    "accelerated-share-repurchase-continuation-v1-"
    "4395f72eecb5b1d37fa08475e270e7dd59fd4d05fe80339f98b0ec6d6f0b9b01.json"
)
SUPERSEDED_PLAN_SHA256 = (
    "0bb78f430e56ac56cc4ca20661569ec1f2c66316e5fe12cd97e71224261bd278"
)


class NextWeekBatchError(RuntimeError):
    """The future batch is early, incomplete, or inconsistent."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        _canonical_bytes({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _grid_count(grid: Mapping[str, Sequence[Any]]) -> int:
    return len(list(itertools.product(*grid.values())))


def _authority() -> tuple[portfolio_maturity.PortfolioConfig, dict[str, Any]]:
    config = portfolio_maturity.load_config()
    asr = capacity._read_object(ASR_DISPOSITION_PATH)
    if not (
        config.schema_version == 2
        and config.active_research_campaign_id == CAMPAIGN_ID
        and config.raw["campaign"]["maximum_new_mechanism_families_per_iso_week"]
        == 3
        and asr.get("disposition_sha256")
        == capacity._self_hash(asr, "disposition_sha256")
        and asr.get("disposition") == "RETIRED_INSUFFICIENT_SOURCE_COMPLETENESS"
        and asr.get("first_pilot_fast_lane_eligible") is False
        and asr.get("market_outcomes_accessed") is False
        and asr.get("broker_actions") == 0
    ):
        raise NextWeekBatchError("v2 campaign or ASR handoff is invalid")
    return config, asr


def build_plan() -> dict[str, Any]:
    config, asr = _authority()
    common = {
        "selection_mode": "development_search",
        "long_only": True,
        "maximum_holding_trading_days": 5,
        "execution": "next observable bar or open; no same-bar lookahead",
        "missing_data_or_invalid_structural_stop": "missed_trade_no_substitute",
        "same_interval_stop_target_ambiguity": "stop_first",
        "maximum_new_entries_per_family_per_day": 1,
        "portfolio_caps_authoritative": True,
        "minimum_expected_gross_move_to_primary_round_trip_cost": 5.0,
        "costs_bps_per_side": [5, 10, 20],
        "confirmation_global_outcome_exposure_overlap_permitted": False,
        "confirmation_parameter_alternatives": 0,
        "maximum_trials_per_family": 64,
    }
    families = [
        {
            "priority": 1,
            "family_id": "liquid-equity-market-residual-reversal",
            "mechanism": "short-horizon idiosyncratic overshoot mean reversion",
            "parameter_grid": {
                "prior_return_sessions": [1, 3],
                "residual_z_threshold": [-1.5, -2.0, -2.5],
                "market_trend_gate": ["SPY>SMA100", "SPY>SMA200"],
                "stop_atr14": [1.0, 1.5],
                "hold_sessions": [2, 5],
            },
            "trial_count": 48,
            "universe": {
                "security_type": "point-in-time US common stocks",
                "prior_close_minimum": 10.0,
                "prior_20_session_median_dollar_volume_minimum": 50_000_000,
                "ranking": "top 250 by prior 60-session dollar volume",
                "historical_identity": "retain delisted and renamed securities",
            },
        },
        {
            "priority": 2,
            "family_id": "intraday-index-etf-opening-reversal",
            "mechanism": "opening downside dislocation followed by VWAP reclaim",
            "parameter_grid": {
                "opening_window_minutes": [15, 30],
                "downside_z_threshold": [-1.5, -2.0],
                "vwap_reclaim_completed_bars": [1, 2],
                "stop_intraday_atr": [1.0, 1.5],
                "target_r": [1.0, 1.5],
            },
            "trial_count": 32,
            "universe": {
                "symbols": ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLK", "XLE", "XLV"],
                "data": "complete SIP regular-session minute bars",
            },
        },
        {
            "priority": 3,
            "family_id": "liquid-etf-trend-pullback-cost-floor",
            "mechanism": "short pullback inside a persistent liquid-ETF uptrend",
            "parameter_grid": {
                "trend_sma": [100, 200],
                "rsi2_maximum": [5, 10],
                "three_session_decline_fraction": [0.02, 0.03],
                "stop_atr14": [1.0, 1.5],
                "maximum_hold_sessions": [3, 5],
            },
            "trial_count": 32,
            "universe": {
                "symbols": [
                    "SPY",
                    "QQQ",
                    "IWM",
                    "DIA",
                    "EFA",
                    "EEM",
                    "IEF",
                    "TLT",
                    "GLD",
                    "DBC",
                    "XLB",
                    "XLE",
                    "XLF",
                    "XLI",
                    "XLK",
                    "XLP",
                    "XLU",
                    "XLV",
                    "XLY",
                ],
                "history_start": "only after each instrument has complete frozen lookback",
            },
        },
    ]
    for family in families:
        if _grid_count(family["parameter_grid"]) != family["trial_count"]:
            raise NextWeekBatchError(f"trial grid drifted: {family['family_id']}")
    plan: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "prospective-three-family-discovery-batch-plan",
        "campaign_id": CAMPAIGN_ID,
        "target_iso_week": TARGET_ISO_WEEK,
        "activation_not_before": ACTIVATION_NOT_BEFORE.isoformat(),
        "state": "WAITING_ISO_WEEK_RESET",
        "family_budget": config.raw["campaign"][
            "maximum_new_mechanism_families_per_iso_week"
        ],
        "families": families,
        "common_contract": common,
        "evidence_freeze_handoff": {
            "development_and_confirmation_dates_frozen": False,
            "global_outcome_exposure_index_checked": False,
            "disjoint_symbols_and_dates_required": True,
            "development_may_use_contaminated_training_only_if_labeled": True,
            "confirmation_must_be_untouched": True,
            "five_session_embargo_required": True,
            "provider_access_permitted": False,
            "outcome_access_permitted": False,
        },
        "asr_disposition_sha256": asr["disposition_sha256"],
        "supersedes_plan_sha256": SUPERSEDED_PLAN_SHA256,
        "supersession_reason": (
            "Restore the authorized nine-sector-SPDR universe by removing XLC "
            "and XLRE before any family contract, provider request, or outcome access."
        ),
        "activation_before_reset_permitted": False,
        "family_contracts_frozen": 0,
        "provider_requests": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "claim_limit": (
            "This artifact predeclares the next batch only. It consumes no family "
            "slot and freezes no evidence dates before the ISO-week reset."
        ),
    }
    plan["plan_sha256"] = _self_hash(plan, "plan_sha256")
    return plan


def prepare(
    *, root: Path = DEFAULT_ROOT, status_path: Path = DEFAULT_STATUS
) -> tuple[Path, dict[str, Any]]:
    plan = build_plan()
    path = root / f"v2-three-family-2026-w31-{plan['plan_sha256']}.json"
    if path.exists() and capacity._read_object(path) != plan:
        raise NextWeekBatchError("content-addressed next-week plan differs")
    capacity._write_object(plan, path)
    capacity._write_object(
        {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "plan_sha256": plan["plan_sha256"],
            "target_iso_week": TARGET_ISO_WEEK,
            "activation_not_before": ACTIVATION_NOT_BEFORE.isoformat(),
            "state": "WAITING_ISO_WEEK_RESET",
            "family_contracts_frozen": 0,
            "provider_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": True,
        },
        status_path,
    )
    return path, plan


def activation_status(
    *, today: date | None = None, status_path: Path = DEFAULT_STATUS
) -> dict[str, Any]:
    current = today or date.today()
    status = capacity._read_object(status_path)
    if current < ACTIVATION_NOT_BEFORE:
        return {
            **status,
            "as_of": current.isoformat(),
            "activation_permitted": False,
            "blockers": [
                f"ISO-week reset has not occurred; wait until {ACTIVATION_NOT_BEFORE.isoformat()}"
            ],
        }
    return {
        **status,
        "as_of": current.isoformat(),
        "state": "READY_FOR_DISJOINT_EVIDENCE_FREEZE",
        "activation_permitted": True,
        "blockers": [
            "freeze exact disjoint development, embargo, and confirmation evidence before provider access"
        ],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("prepare", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "prepare":
            path, value = prepare()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "plan_sha256": value["plan_sha256"],
                "state": value["state"],
                "family_contracts_frozen": 0,
                "provider_access_permitted": False,
            }
        else:
            result = activation_status()
    except (
        NextWeekBatchError,
        capacity.AsrCapacityError,
        portfolio_maturity.PortfolioMaturityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
