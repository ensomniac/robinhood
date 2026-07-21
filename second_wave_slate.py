"""Freeze and inspect the only authorized second-wave Stage 0 slate."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from portfolio_funnel import SECOND_WAVE, _self_hash, load_failure_taxonomy_status
from portfolio_maturity import build_report


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
SCHEMA_VERSION = 1
ETF_CALENDAR = {
    "calendar": "XNYS",
    "decision_sessions": "every eligible XNYS session in the inclusive range",
    "evaluation_start": "2023-01-03",
    "evaluation_end": "2025-12-31",
    "lookback_collection_start": "2022-01-03",
    "outcome_collection_end": "2026-01-08",
}
EQUITY_TARGET_DATES = (
    "2025-03-03",
    "2025-03-14",
    "2025-03-27",
    "2025-04-08",
    "2025-04-23",
    "2025-05-02",
    "2025-05-15",
    "2025-05-29",
    "2025-06-10",
    "2025-06-25",
    "2025-07-08",
    "2025-07-22",
    "2025-07-31",
    "2025-08-12",
    "2025-08-25",
    "2025-09-08",
    "2025-09-18",
    "2025-10-01",
    "2025-10-14",
    "2025-10-24",
    "2025-11-07",
    "2025-11-20",
    "2025-12-02",
    "2025-12-15",
)
SECTOR_ETFS = (
    "XLB",
    "XLC",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLRE",
    "XLU",
    "XLV",
    "XLY",
)
BROAD_ETFS = ("SPY", "QQQ", "IWM", "DIA")
MEMBERSHIP_PATH = (
    DATA_ROOT
    / "_derived/cross_sectional_momentum_stage0/"
    "dataset-cross-sectional-momentum-stage0-2026-07-21-v1/"
    "frozen-membership.json.gz"
)
TAXONOMY_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/"
    "first-wave-failure-taxonomy-"
    "73d54349f24f52f62125fc9fcc7105ffa9b8a38e0771bf1fb994decadfa90292.json"
)
TAXONOMY_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "first-wave-failure-taxonomy-"
    "6f2619090a6312763d16ab9f3e01f63047c59b7fae1907803aea78b0ffa74f58.json"
)


class SecondWaveSlateError(RuntimeError):
    """The second-wave rules or prerequisite evidence are incomplete."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise SecondWaveSlateError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _rules_hash(value: Mapping[str, Any]) -> str:
    return _self_hash(value, "rules_hash")


def _variant(
    *,
    variant_id: str,
    mechanism_family: str,
    hypothesis: str,
    universe: Mapping[str, Any],
    data_contract: Mapping[str, Any],
    signal: Mapping[str, Any],
    execution: Mapping[str, Any],
    exit_rule: Mapping[str, Any],
    maximum_holding_trading_days: int,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "variant_id": variant_id,
        "version": "0.1.0-stage0",
        "mechanism_family": mechanism_family,
        "hypothesis": hypothesis,
        "claim_scope": "FALSIFICATION_ONLY",
        "universe": dict(universe),
        "data_contract": dict(data_contract),
        "signal": dict(signal),
        "execution": dict(execution),
        "exit": dict(exit_rule),
        "maximum_holding_trading_days": maximum_holding_trading_days,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "maturity_effect": "NONE",
    }
    value["rules_hash"] = _rules_hash(value)
    return value


def _daily_execution() -> dict[str, Any]:
    return {
        "signal_information": "completed split-consistent regular-session daily bars only",
        "entry": "next XNYS session open plus adverse per-side cost",
        "protective_stop": "good-til-canceled stop modeled from entry; gaps fill at the worse of session open or stop",
        "same_session_ambiguity": "stop_first",
        "primary_cost_bps_per_side": 5,
        "stress_cost_bps_per_side": [10, 20],
        "maximum_concurrent_positions": 1,
        "maximum_new_entries_per_day": 1,
    }


def build_manifest() -> dict[str, Any]:
    taxonomy_status = load_failure_taxonomy_status(build_report())
    if taxonomy_status["inspected"] is not True:
        raise SecondWaveSlateError("inspected first-wave taxonomy is required")
    membership_relative = str(MEMBERSHIP_PATH.relative_to(DATA_ROOT))
    common_daily = {
        "provider": "IBKR",
        "feed": "SMART",
        "adjustment": "provider_adjusted_unknown_basis",
        "timeframe": "1d",
        "whole_provider_per_signal": True,
        "provider_substitution": False,
        "calendar": ETF_CALENDAR,
    }
    equity_daily = {
        "provider": "Massive",
        "feed": "SIP",
        "adjustment": "split_adjusted",
        "timeframe": "1d",
        "whole_provider_per_signal": True,
        "provider_substitution": False,
        "lookback_collection_start": "2024-02-01",
        "outcome_collection_end": "2025-12-23",
        "target_dates": list(EQUITY_TARGET_DATES),
        "membership_path": membership_relative,
        "membership_file_sha256": sha256_file(MEMBERSHIP_PATH),
        "membership_outcomes_observed_or_derived": False,
    }
    variants = [
        _variant(
            variant_id="sector-etf-rotation-v1",
            mechanism_family="sector-etf-rotation",
            hypothesis="persistent sector leadership survives a five-session liquid-ETF holding period",
            universe={"symbols": list(SECTOR_ETFS), "benchmark": "SPY"},
            data_contract=common_daily,
            signal={
                "decision_time": "after each completed regular session while flat",
                "lookback_trading_sessions": 20,
                "trend_filter": "close above simple moving average 50",
                "benchmark_filter": "20-session return strictly above SPY",
                "selection": "highest 20-session return; symbol ascending tie-break",
            },
            execution=_daily_execution(),
            exit_rule={
                "stop": "1.5 times signal-session ATR14 below entry",
                "time_exit": "fifth trading-session close after entry",
            },
            maximum_holding_trading_days=5,
        ),
        _variant(
            variant_id="broad-etf-trend-pullback-v1",
            mechanism_family="broad-etf-trend-pullback",
            hypothesis="short liquid-index pullbacks resume within an established long trend",
            universe={"symbols": list(BROAD_ETFS)},
            data_contract=common_daily,
            signal={
                "decision_time": "after each completed regular session while flat",
                "trend_filter": "close above simple moving average 200",
                "pullback_filter": "three-session close return at or below -1 percent and RSI2 at or below 15",
                "selection": "lowest RSI2, then lowest three-session return, then symbol ascending",
            },
            execution=_daily_execution(),
            exit_rule={
                "stop": "1.5 times signal-session ATR14 below entry",
                "recovery_exit": "first completed close at or above simple moving average 5",
                "time_exit": "fifth trading-session close after entry",
            },
            maximum_holding_trading_days=5,
        ),
        _variant(
            variant_id="close-to-open-etf-momentum-v1",
            mechanism_family="close-to-open-etf-momentum",
            hypothesis="strong broad-ETF regular-session momentum persists into the next opening auction",
            universe={"symbols": list(BROAD_ETFS)},
            data_contract={
                **common_daily,
                "signal_timeframe": "15m",
                "entry_bar_timeframe": "15m",
                "required_complete_regular_session_bars": 26,
            },
            signal={
                "decision_time_et": "15:45:00",
                "information_cutoff": "completed 15:30-15:45 ET bar",
                "minimum_session_return_fraction": 0.0075,
                "trend_filter": "15:45 signal close above prior completed-session SMA20",
                "selection": "highest session return; symbol ascending tie-break",
            },
            execution={
                "entry": "15:45 ET bar open plus adverse per-side cost",
                "protective_stop": "good-til-canceled stop one prior-session ATR14 below entry; overnight gaps fill at next open",
                "same_interval_ambiguity": "stop_first",
                "primary_cost_bps_per_side": 5,
                "stress_cost_bps_per_side": [10, 20],
                "maximum_concurrent_positions": 1,
                "maximum_new_entries_per_day": 1,
            },
            exit_rule={
                "time_exit": "next XNYS session 09:30 bar open",
                "gap_rule": "if next open is below stop, exit at open before costs",
            },
            maximum_holding_trading_days=1,
        ),
        _variant(
            variant_id="two-to-three-day-cross-sectional-reversal-v1",
            mechanism_family="two-to-three-day-cross-sectional-reversal",
            hypothesis="extreme liquid three-session losers mean-revert over the next three sessions",
            universe={
                "membership": "complete dated XNAS/XNYS common-stock membership",
                "maximum_names_per_target_date": 3,
            },
            data_contract=equity_daily,
            signal={
                "decision_time": "after the completed session preceding each target date",
                "minimum_price": 5,
                "minimum_prior_20_session_average_dollar_volume": 20_000_000,
                "maximum_three_session_return_fraction": -0.08,
                "selection": "lowest three-session return; symbol ascending tie-break",
            },
            execution=_daily_execution(),
            exit_rule={
                "stop": "1.5 times signal-session ATR14 below entry",
                "time_exit": "third trading-session close after entry",
            },
            maximum_holding_trading_days=3,
        ),
        _variant(
            variant_id="five-day-52-week-high-continuation-v1",
            mechanism_family="five-day-52-week-high-continuation",
            hypothesis="liquid equities near a 252-session high continue over the next five sessions",
            universe={
                "membership": "complete dated XNAS/XNYS common-stock membership",
                "maximum_names_per_target_date": 3,
            },
            data_contract=equity_daily,
            signal={
                "decision_time": "after the completed session preceding each target date",
                "minimum_price": 5,
                "minimum_prior_20_session_average_dollar_volume": 20_000_000,
                "minimum_five_session_return_fraction": 0.02,
                "minimum_close_to_prior_252_session_high_fraction": 0.98,
                "selection": "highest five-session return, then symbol ascending",
            },
            execution=_daily_execution(),
            exit_rule={
                "stop": "1.5 times signal-session ATR14 below entry",
                "time_exit": "fifth trading-session close after entry",
            },
            maximum_holding_trading_days=5,
        ),
        _variant(
            variant_id="turn-of-month-etf-seasonality-v1",
            mechanism_family="turn-of-month-etf-seasonality",
            hypothesis="broad-market calendar flows concentrate from the final through third trading session of each month",
            universe={"symbols": ["SPY"]},
            data_contract=common_daily,
            signal={
                "decision_time": "after the penultimate XNYS session of each month",
                "entry_session": "final XNYS session of the month",
                "selection": "SPY only; one signal per eligible month",
            },
            execution=_daily_execution(),
            exit_rule={
                "stop": "1.5 times signal-session ATR14 below entry",
                "time_exit": "third XNYS session close of the next month",
            },
            maximum_holding_trading_days=4,
        ),
    ]
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "manifest_kind": "portfolio-stage0-second-wave-slate",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "wave": 2,
        "implementation_path": "second_wave_slate.py",
        "implementation_sha256": sha256_file(Path(__file__)),
        "failure_taxonomy_path": str(TAXONOMY_PATH.relative_to(PROJECT_ROOT)),
        "failure_taxonomy_sha256": "73d54349f24f52f62125fc9fcc7105ffa9b8a38e0771bf1fb994decadfa90292",
        "failure_taxonomy_file_sha256": sha256_file(TAXONOMY_PATH),
        "failure_taxonomy_inspection_path": str(
            TAXONOMY_INSPECTION_PATH.relative_to(PROJECT_ROOT)
        ),
        "failure_taxonomy_inspection_sha256": "6f2619090a6312763d16ab9f3e01f63047c59b7fae1907803aea78b0ffa74f58",
        "failure_taxonomy_inspection_file_sha256": sha256_file(
            TAXONOMY_INSPECTION_PATH
        ),
        "prior_trial_accounting": {
            "first_wave_stage0_dispositions": 10,
            "first_wave_development_retirements": 1,
            "minimum_declared_policy_trials": 18,
            "parameter_repair_permitted": False,
            "maturity_inheritance_permitted": False,
        },
        "stage0_falsification": {
            "minimum_closed_signals": 30,
            "require_positive_expectancy": True,
            "minimum_profit_factor": 1.10,
            "maximum_drawdown_r": 8,
            "require_positive_20bps_total_r": True,
            "rule_violations": 0,
            "effect": "screening survival only; never PILOT_READY or maturity evidence",
        },
        "ordered_variant_ids": [item[0] for item in SECOND_WAVE],
        "variants": variants,
        "outcomes_previously_accessed_for_exact_rules": False,
        "return_evaluation_authorized_before_inspection": False,
        "provider_requests_authorized_before_inspection": False,
        "broker_actions_authorized": False,
        "maturity_effect": "NONE",
    }
    manifest["manifest_sha256"] = _self_hash(manifest, "manifest_sha256")
    validate_manifest(manifest)
    return manifest


def validate_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("manifest_sha256") != _self_hash(manifest, "manifest_sha256"):
        raise SecondWaveSlateError("second-wave slate content hash is invalid")
    if manifest.get("wave") != 2 or manifest.get("maturity_effect") != "NONE":
        raise SecondWaveSlateError("second-wave slate scope drifted")
    variants = manifest.get("variants")
    if not isinstance(variants, list) or len(variants) != len(SECOND_WAVE):
        raise SecondWaveSlateError("second-wave variant count drifted")
    expected = list(SECOND_WAVE)
    actual = [
        (item.get("variant_id"), item.get("mechanism_family")) for item in variants
    ]
    if actual != expected:
        raise SecondWaveSlateError("second-wave order or family drifted")
    if len({item.get("rules_hash") for item in variants}) != len(variants):
        raise SecondWaveSlateError("second-wave rules hashes must be unique")
    for item in variants:
        if item.get("rules_hash") != _rules_hash(item):
            raise SecondWaveSlateError(f"rules hash drifted: {item.get('variant_id')}")
        holding_days = item.get("maximum_holding_trading_days")
        if not isinstance(holding_days, int) or not 1 <= holding_days <= 5:
            raise SecondWaveSlateError("holding period exceeds the authorized envelope")
        if item.get("maturity_effect") != "NONE":
            raise SecondWaveSlateError("Stage 0 cannot affect maturity")
    gate = manifest.get("stage0_falsification")
    if not isinstance(gate, Mapping) or gate.get("minimum_closed_signals") != 30:
        raise SecondWaveSlateError("Stage 0 signal gate drifted")
    if gate.get("minimum_profit_factor") != 1.10:
        raise SecondWaveSlateError("Stage 0 profit-factor gate drifted")
    if gate.get("maximum_drawdown_r") != 8:
        raise SecondWaveSlateError("Stage 0 drawdown gate drifted")


def inspect_manifest(manifest_path: Path) -> dict[str, Any]:
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SecondWaveSlateError(f"cannot read {manifest_path}: {exc}") from exc
    if not isinstance(manifest, dict):
        raise SecondWaveSlateError("second-wave manifest must be an object")
    validate_manifest(manifest)
    if manifest != build_manifest():
        raise SecondWaveSlateError("published second-wave slate does not rebuild")
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "portfolio-stage0-second-wave-slate-inspection",
        "manifest_sha256": manifest["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "variant_count": len(manifest["variants"]),
        "ordered_variant_ids": manifest["ordered_variant_ids"],
        "membership_file_sha256": sha256_file(MEMBERSHIP_PATH),
        "outcomes_accessed": 0,
        "returns_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "return_evaluation_authorized": True,
        "valid": True,
    }
    inspection["inspection_sha256"] = _self_hash(
        inspection, "inspection_sha256"
    )
    return inspection


def _write(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "inspect"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            value = build_manifest()
            path = (
                PROJECT_ROOT
                / "strategy_tournament/second_wave/manifests/"
                f"portfolio-stage0-second-wave-slate-{value['manifest_sha256']}.json"
            )
        else:
            matches = sorted(
                (PROJECT_ROOT / "strategy_tournament/second_wave/manifests").glob(
                    "portfolio-stage0-second-wave-slate-*.json"
                )
            )
            if len(matches) != 1:
                raise SecondWaveSlateError(
                    f"expected one published second-wave slate; found {len(matches)}"
                )
            value = inspect_manifest(matches[0])
            path = (
                PROJECT_ROOT
                / "strategy_tournament/second_wave/inspections/"
                "portfolio-stage0-second-wave-slate-"
                f"{value['inspection_sha256']}.json"
            )
        _write(value, path)
        print(
            json.dumps(
                {
                    "sha256": value.get("manifest_sha256")
                    or value["inspection_sha256"],
                    "written": str(path.relative_to(PROJECT_ROOT)),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (OSError, SecondWaveSlateError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
