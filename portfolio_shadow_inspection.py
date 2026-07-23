"""Independently replay, inspect, and admit prospective shadow evidence."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import portfolio_execution
import portfolio_maturity
import portfolio_shadow
import strategy_discovery


DEFAULT_ROOT = strategy_discovery.DEFAULT_ROOT
SCHEMA_VERSION = 1


class PortfolioShadowInspectionError(ValueError):
    """A shadow artifact cannot be independently reproduced or admitted."""


def _load_bound_artifact(
    relative: Any,
    expected_sha256: Any,
    kind: str,
    *,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any]]:
    if not isinstance(relative, str):
        raise PortfolioShadowInspectionError(f"{kind} path is missing")
    path = portfolio_shadow.PROJECT_ROOT / relative
    if enforce_commit:
        strategy_discovery.require_committed(path)
    artifact = strategy_discovery.load_artifact(path, expected_kind=kind)
    if artifact.get("artifact_sha256") != expected_sha256:
        raise PortfolioShadowInspectionError(f"{kind} binding drifted")
    return path, artifact


def inspect_shadow(
    final_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    """Replay the plugin, simulated fill, lifecycle, and maturity row."""
    if enforce_commit:
        strategy_discovery.require_committed(final_path)
    final = strategy_discovery.load_artifact(
        final_path, expected_kind="prospective-shadow-final"
    )
    entry_path, entry = _load_bound_artifact(
        final.get("entry_path"),
        final.get("entry_sha256"),
        "prospective-shadow-entry",
        enforce_commit=enforce_commit,
    )
    _, winner = _load_bound_artifact(
        entry.get("winner_path"),
        entry.get("winner_sha256"),
        "frozen-strategy-winner",
        enforce_commit=enforce_commit,
    )
    evaluation_now = portfolio_shadow._timestamp(
        entry.get("evaluation_now"), "evaluation_now"
    )
    try:
        rebuilt_evaluation = portfolio_execution.evaluate_frozen_winner(
            winner,
            portfolio_shadow._object(entry["setup"]["market_facts"], "market_facts"),
            portfolio_shadow._object(entry["setup"]["account"], "account"),
            portfolio_maturity.load_config(),
            now=evaluation_now,
        )
    except portfolio_execution.PortfolioExecutionError as exc:
        raise PortfolioShadowInspectionError(str(exc)) from exc
    if rebuilt_evaluation != entry.get("production_evaluation"):
        raise PortfolioShadowInspectionError("production evaluation does not replay")
    rebuilt_fill = portfolio_shadow._fill_from_observation(
        rebuilt_evaluation,
        portfolio_shadow._object(
            entry["setup"]["fill_observation"], "fill_observation"
        ),
        evaluation_now=evaluation_now,
    )
    if rebuilt_fill != entry.get("fill"):
        raise PortfolioShadowInspectionError("shadow fill simulation does not replay")
    rebuilt_final, rebuilt_record = portfolio_shadow.rebuild_final(
        entry, final["closure"]
    )
    if rebuilt_final != final.get("final"):
        raise PortfolioShadowInspectionError("shadow close facts do not replay")
    if rebuilt_record != final.get("maturity_record"):
        raise PortfolioShadowInspectionError("shadow maturity row does not replay")
    if final.get("broker_actions_performed") != 0:
        raise PortfolioShadowInspectionError("shadow artifact recorded broker actions")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "prospective-shadow-inspection",
        "campaign_id": final["campaign_id"],
        "family_id": final["family_id"],
        "strategy_id": final["strategy_id"],
        "strategy_version": final["strategy_version"],
        "rules_hash": final["rules_hash"],
        "signal_id": final["signal_id"],
        "state": "SHADOW_INSPECTED_ADMISSION_READY",
        "final_path": strategy_discovery._relative(final_path),
        "final_sha256": final["artifact_sha256"],
        "entry_path": strategy_discovery._relative(entry_path),
        "entry_sha256": entry["artifact_sha256"],
        "inspection": {
            "exact_plugin_replayed": True,
            "production_evaluation_rebuilt": True,
            "bid_ask_fill_rebuilt": True,
            "protection_timing_rebuilt": True,
            "exit_and_costs_rebuilt": True,
            "maturity_record_rebuilt": True,
            "privacy_safe_zero_broker_boundary_rebuilt": True,
            "valid": True,
        },
        "maturity_record": rebuilt_record,
        "ledger_admission_permitted": True,
        "broker_actions_performed": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(final["family_id"]) / "shadow-inspection",
        final["signal_id"],
    )


def admit_shadow(
    inspection_path: Path,
    *,
    ledger_path: Path = portfolio_maturity.DEFAULT_LEDGER_PATH,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    """Append only the exact maturity row from a committed valid inspection."""
    if enforce_commit:
        strategy_discovery.require_committed(inspection_path)
    inspection = strategy_discovery.load_artifact(
        inspection_path, expected_kind="prospective-shadow-inspection"
    )
    if inspection.get("state") != "SHADOW_INSPECTED_ADMISSION_READY":
        raise PortfolioShadowInspectionError("shadow inspection is not admission-ready")
    if inspection.get("inspection", {}).get("valid") is not True:
        raise PortfolioShadowInspectionError("shadow inspection is invalid")
    if inspection.get("ledger_admission_permitted") is not True:
        raise PortfolioShadowInspectionError("shadow ledger admission is forbidden")
    record = inspection.get("maturity_record")
    if not isinstance(record, Mapping):
        raise PortfolioShadowInspectionError("shadow maturity record is missing")
    portfolio_maturity.append_record(record, ledger_path)
    return {
        "state": "SHADOW_ADMITTED",
        "signal_id": record["signal_id"],
        "eligible_clean_closed_shadow": bool(record["eligible"]),
        "ledger_path": str(ledger_path),
        "broker_actions_performed": 0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    parser.add_argument(
        "--ledger", type=Path, default=portfolio_maturity.DEFAULT_LEDGER_PATH
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("final", type=Path)
    admit = subparsers.add_parser("admit")
    admit.add_argument("inspection", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect":
            path, artifact = inspect_shadow(args.final, root=args.root)
            result = {
                "written": strategy_discovery._relative(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "broker_actions_performed": 0,
            }
        else:
            result = admit_shadow(args.inspection, ledger_path=args.ledger)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (OSError, PortfolioShadowInspectionError, ValueError) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
