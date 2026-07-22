"""Freeze and inspect the outcome-locked first-wave portfolio tournament slate."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from portfolio_maturity import load_config


PROJECT_ROOT = Path(__file__).resolve().parent
LEGACY_CONFIG_PATH = PROJECT_ROOT / "portfolio_config_v1.toml"
LEGACY_IMPLEMENTATION_SHA256 = (
    "a0aac8231a904369021f2b86aaee4596709b26e9b5d830954a8e57b046e9be33"
)
INVENTORY_PATH = (
    PROJECT_ROOT / "research_results" / "2026-07-21-portfolio-data-inventory.json"
)
MANIFEST_ROOT = PROJECT_ROOT / "strategy_tournament" / "manifests"
SCHEMA_VERSION = 1

COMMON_INTRADAY_EXECUTION = {
    "signal_information": "completed one-minute bars only",
    "entry": "next observed one-minute open plus adverse per-side cost",
    "same_interval_ambiguity": "stop_first",
    "primary_cost_bps_per_side": 5,
    "stress_cost_bps_per_side": [10, 20],
    "maximum_entries_per_strategy_per_day": 1,
    "regular_hours_only": True,
    "force_flat_et": "15:50:00",
}


class PortfolioTournamentError(RuntimeError):
    """The tournament slate or its frozen identity is invalid."""


def _canonical_bytes(value: Any) -> bytes:
    def normalize_numbers(item: Any) -> Any:
        if isinstance(item, dict):
            return {key: normalize_numbers(child) for key, child in item.items()}
        if isinstance(item, list):
            return [normalize_numbers(child) for child in item]
        if isinstance(item, float) and item.is_integer():
            return int(item)
        return item

    return json.dumps(
        normalize_numbers(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise PortfolioTournamentError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioTournamentError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioTournamentError(f"{path} must contain an object")
    return value


def _intraday_spec(
    *,
    ordinal: int,
    variant_id: str,
    family: str,
    asset_scope: str,
    hypothesis: str,
    signal: Mapping[str, Any],
    exit_rule: Mapping[str, Any],
    data_route: str,
) -> dict[str, Any]:
    spec: dict[str, Any] = {
        "variant_ordinal": ordinal,
        "variant_id": variant_id,
        "mechanism_family": family,
        "version": "0.1.0-stage0",
        "asset_scope": asset_scope,
        "maximum_holding_trading_days": 1,
        "hypothesis": hypothesis,
        "signal": dict(signal),
        "exit": dict(exit_rule),
        "execution": dict(COMMON_INTRADAY_EXECUTION),
        "data_route": data_route,
        "claim_scope": "FALSIFICATION_ONLY",
    }
    spec["rules_hash"] = _hash(spec)
    return spec


def _slate() -> list[dict[str, Any]]:
    variants = [
        _intraday_spec(
            ordinal=1,
            variant_id="etf-or-momentum-v1",
            family="etf-opening-range-momentum",
            asset_scope="SPY,QQQ",
            hypothesis="liquid index ETFs persist after a directional opening auction",
            signal={
                "window_et": "09:35:00-11:00:00",
                "opening_range_minutes": 5,
                "opening_candle": "close>open",
                "trigger": "first completed close above opening-range high and session VWAP",
                "maximum_chase_fraction": 0.0015,
            },
            exit_rule={
                "stop": "opening-range low",
                "maximum_stop_fraction": 0.012,
                "target_r": 2.0,
            },
            data_route="cached SPY/QQQ screening; new disjoint manifest required for development",
        ),
        _intraday_spec(
            ordinal=2,
            variant_id="etf-vwap-mean-reversion-v1",
            family="etf-vwap-mean-reversion",
            asset_scope="SPY,QQQ",
            hypothesis="liquid index ETFs mean-revert after an intraday volatility-normalized washout",
            signal={
                "window_et": "10:00:00-14:30:00",
                "minimum_below_vwap_fraction": 0.0075,
                "rsi_period_bars": 5,
                "maximum_rsi": 25.0,
                "trigger": "bullish completed close above prior bar high while still below VWAP",
            },
            exit_rule={
                "stop": "signal low minus 5 bps",
                "target": "session VWAP observed after entry",
            },
            data_route="cached SPY/QQQ screening; new disjoint manifest required for development",
        ),
        _intraday_spec(
            ordinal=3,
            variant_id="equity-gap-continuation-v1",
            family="equity-gap-continuation",
            asset_scope="common equities above $5",
            hypothesis="moderate liquid gaps continue after a stable first fifteen minutes",
            signal={
                "window_et": "09:45:00-11:30:00",
                "gap_fraction": [0.02, 0.08],
                "opening_range_minutes": 15,
                "trigger": "completed close above first-15-minute high and VWAP",
                "minimum_breakout_volume_multiple": 1.5,
            },
            exit_rule={"stop": "first-15-minute low", "target_r": 2.0},
            data_route="legacy catalyst corpus screen only; representative universe required next",
        ),
        _intraday_spec(
            ordinal=4,
            variant_id="equity-gap-recovery-v1",
            family="equity-gap-recovery",
            asset_scope="common equities above $5",
            hypothesis="liquid gap-down overreactions recover after reclaiming both the open and VWAP",
            signal={
                "window_et": "09:45:00-12:00:00",
                "gap_fraction": [-0.08, -0.02],
                "opening_range_minutes": 15,
                "trigger": "completed close above session open and VWAP after no new low for 10 minutes",
            },
            exit_rule={"stop": "session low", "target": "prior close or 2R, whichever is nearer"},
            data_route="legacy catalyst corpus screen only; representative universe required next",
        ),
        _intraday_spec(
            ordinal=5,
            variant_id="relative-strength-continuation-v1",
            family="relative-strength-continuation",
            asset_scope="common equities above $5",
            hypothesis="large benchmark-relative leaders continue when breaking intraday highs",
            signal={
                "window_et": "10:00:00-14:30:00",
                "minimum_return_minus_spy_fraction": 0.02,
                "trigger": "completed close above prior high of day and session VWAP",
                "minimum_breakout_volume_multiple": 1.5,
            },
            exit_rule={"stop": "last five-bar swing low", "target_r": 2.0},
            data_route="blocked until full point-in-time cross-sectional ranking is frozen",
        ),
        _intraday_spec(
            ordinal=6,
            variant_id="volatility-compression-breakout-v1",
            family="volatility-compression-breakout",
            asset_scope="liquid common equities and ETFs above $5",
            hypothesis="intraday range compression precedes executable directional expansion",
            signal={
                "window_et": "10:15:00-14:30:00",
                "compression_bars": 20,
                "maximum_compression_to_first30_range": 0.60,
                "trigger": "completed close above compression high and VWAP",
                "minimum_breakout_volume_multiple": 1.5,
            },
            exit_rule={"stop": "compression low", "target_r": 2.0},
            data_route="legacy catalyst corpus screen only; representative universe required next",
        ),
        _intraday_spec(
            ordinal=8,
            variant_id="short-horizon-oversold-reversal-v1",
            family="short-horizon-oversold-reversal",
            asset_scope="liquid common equities and ETFs above $5",
            hypothesis="deep short-horizon selloffs rebound after an executable price reversal",
            signal={
                "window_et": "10:00:00-14:30:00",
                "lookback_bars": 30,
                "maximum_return_fraction": -0.03,
                "rsi_period_bars": 5,
                "maximum_rsi_before_trigger": 20.0,
                "trigger": "bullish completed close above prior bar high and session VWAP",
            },
            exit_rule={"stop": "session low", "target_r": 1.5},
            data_route="legacy catalyst corpus screen only; representative universe required next",
        ),
        _intraday_spec(
            ordinal=10,
            variant_id="catalyst-orb-retest-v1",
            family="catalyst-orb-retest",
            asset_scope="catalyst-verified common equities above $5",
            hypothesis="a clean post-break retest filters false catalyst opening breakouts",
            signal={
                "window_et": "09:40:00-11:00:00",
                "opening_range_minutes": 5,
                "first_trigger": "completed close above opening-range high and VWAP",
                "retest_tolerance_fraction": 0.002,
                "trigger": "later completed bullish close back above opening-range high after retest",
            },
            exit_rule={"stop": "retest low", "target_r": 2.0},
            data_route="preserved catalyst lane; legacy screen before any resumed source acquisition",
        ),
    ]
    cross_sectional = {
        "variant_ordinal": 7,
        "variant_id": "cross-sectional-momentum-v1",
        "mechanism_family": "cross-sectional-momentum",
        "version": "0.1.0-stage0",
        "asset_scope": "point-in-time US common-equity universe",
        "maximum_holding_trading_days": 5,
        "hypothesis": "the strongest liquid monthly leaders persist over the next trading week",
        "signal": {
            "ranking_time": "15:45:00 ET",
            "lookback_trading_days": 20,
            "selection": "top decile total return, price above 50-day simple moving average",
            "maximum_names": 3,
        },
        "exit": {"stop": "1.5 times prior-day ATR14", "maximum_hold_trading_days": 5},
        "execution": {
            "entry": "next regular-session open plus adverse per-side cost",
            "primary_cost_bps_per_side": 5,
            "stress_cost_bps_per_side": [10, 20],
            "same_interval_ambiguity": "stop_first",
        },
        "data_route": "blocked until point-in-time membership and split-adjusted five-session timelines are frozen",
        "claim_scope": "FALSIFICATION_ONLY",
    }
    cross_sectional["rules_hash"] = _hash(cross_sectional)
    pead = {
        "variant_ordinal": 9,
        "variant_id": "post-earnings-drift-v1",
        "mechanism_family": "post-earnings-drift",
        "version": "0.1.0-stage0",
        "asset_scope": "primary-source verified positive earnings common equities above $5",
        "maximum_holding_trading_days": 5,
        "hypothesis": "material positive earnings surprises drift after the announcement session closes strong",
        "signal": {
            "event": "verified positive primary-source earnings release",
            "gap_fraction": [0.01, 0.08],
            "announcement_session": "close>open and close above session VWAP",
            "decision_time": "15:45:00 ET",
        },
        "exit": {"stop": "announcement-session low", "maximum_hold_trading_days": 5},
        "execution": {
            "entry": "15:46 one-minute open plus adverse per-side cost",
            "primary_cost_bps_per_side": 5,
            "stress_cost_bps_per_side": [10, 20],
            "same_interval_ambiguity": "stop_first",
        },
        "data_route": "legacy earnings screen with all 15 prior policy trials counted; new source-verified dates required next",
        "claim_scope": "FALSIFICATION_ONLY",
    }
    pead["rules_hash"] = _hash(pead)
    variants.extend((cross_sectional, pead))
    return sorted(variants, key=lambda item: int(item["variant_ordinal"]))


def build_manifest() -> dict[str, Any]:
    config = load_config(LEGACY_CONFIG_PATH)
    inventory = _load_json(INVENTORY_PATH)
    if inventory.get("inventory_sha256") != "dd77fb3c226732c652000e6a80a2f4336e876a7d1ba005eb9718e0719e63397b":
        raise PortfolioTournamentError("unexpected data inventory identity")
    variants = _slate()
    if len(variants) != int(config.raw["campaign"]["initial_mechanism_families"]):
        raise PortfolioTournamentError("slate does not contain exactly ten families")
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": config.raw["campaign"]["id"],
        "tournament_wave": 1,
        "slate_kind": "mechanism-preregistration",
        "outcome_access_authorized": False,
        "provider_requests_authorized": False,
        "broker_actions_authorized": False,
        "portfolio_config_sha256": config.sha256,
        "data_inventory_sha256": inventory["inventory_sha256"],
        # V1 is immutable adverse history. Rebuild its original manifest identity
        # from the frozen schema-1 config rather than rebinding it to v2 code.
        "implementation_sha256": LEGACY_IMPLEMENTATION_SHA256,
        "prior_policy_trials_to_retain": 15,
        "maximum_initial_variants": int(config.raw["campaign"]["maximum_initial_variants"]),
        "unused_initial_variant_capacity": int(
            config.raw["campaign"]["maximum_initial_variants"]
        )
        - len(variants),
        "variants": variants,
        "stage0_falsification": {
            "minimum_closed_signals": 30,
            "require_positive_expectancy": True,
            "minimum_profit_factor": 1.10,
            "maximum_drawdown_r": 8.0,
            "require_positive_20bps_total_r": True,
            "rule_violations": 0,
            "effect": "screening survival only; never PILOT_READY or production evidence",
        },
        "activation_contract": {
            "required_before_any_variant_outcome_evaluation": [
                "versioned evaluator implementation hash",
                "exact ordered dates and symbols",
                "input artifact hashes and point-in-time claim boundary",
                "complete denominator and missing-data policy",
                "frozen execution and cost contract",
            ],
            "failed_variant_action": "retain exact failure and do not tune on its evaluation corpus",
        },
    }
    manifest["manifest_sha256"] = _hash(manifest)
    return manifest


def default_manifest_path(manifest: Mapping[str, Any]) -> Path:
    return MANIFEST_ROOT / f"portfolio-stage0-slate-{manifest['manifest_sha256']}.json"


def inspect_manifest(path: Path) -> dict[str, Any]:
    recorded = _load_json(path)
    supplied_hash = recorded.get("manifest_sha256")
    payload = {key: value for key, value in recorded.items() if key != "manifest_sha256"}
    if supplied_hash != _hash(payload):
        raise PortfolioTournamentError("manifest content hash is invalid")
    rebuilt = build_manifest()
    if recorded != rebuilt:
        raise PortfolioTournamentError("manifest does not match the frozen implementation")
    ordinals = [int(item["variant_ordinal"]) for item in recorded["variants"]]
    families = [str(item["mechanism_family"]) for item in recorded["variants"]]
    rules = [str(item["rules_hash"]) for item in recorded["variants"]]
    if ordinals != list(range(1, 11)) or len(families) != len(set(families)):
        raise PortfolioTournamentError("variant ordinals or families are not unique")
    if len(rules) != len(set(rules)):
        raise PortfolioTournamentError("variant rules hashes are not unique")
    if recorded.get("outcome_access_authorized") is not False:
        raise PortfolioTournamentError("slate must remain outcome locked")
    return {
        "valid": True,
        "path": str(path),
        "manifest_sha256": supplied_hash,
        "mechanism_families": len(families),
        "variants": len(rules),
        "outcome_access_authorized": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("build", help="rebuild and print the deterministic slate")
    inspect = subparsers.add_parser("inspect", help="inspect a published slate")
    inspect.add_argument("path", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = build_manifest() if args.command == "build" else inspect_manifest(args.path)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (PortfolioTournamentError, OSError, ValueError) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
