"""Independently replay and admit one flat reconciled controlled live close."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import portfolio_execution
import portfolio_guard
import portfolio_live
import portfolio_maturity
import strategy_discovery


DEFAULT_ROOT = portfolio_live.DEFAULT_ROOT
SCHEMA_VERSION = 1


class PortfolioLiveInspectionError(ValueError):
    """A controlled live close cannot be reproduced or admitted."""


def _load_bound(
    relative: Any,
    expected_sha256: Any,
    kind: str,
    *,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any]]:
    if not isinstance(relative, str):
        raise PortfolioLiveInspectionError(f"{kind} path is missing")
    path = portfolio_live.PROJECT_ROOT / relative
    if enforce_commit:
        strategy_discovery.require_committed(path)
    artifact = strategy_discovery.load_artifact(path, expected_kind=kind)
    if artifact.get("artifact_sha256") != expected_sha256:
        raise PortfolioLiveInspectionError(f"{kind} binding drifted")
    return path, artifact


def inspect_live(
    final_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(final_path)
    final = strategy_discovery.load_artifact(
        final_path, expected_kind="controlled-live-final"
    )
    protection_path, protection = _load_bound(
        final.get("protection_path"),
        final.get("protection_sha256"),
        "controlled-live-protection-result",
        enforce_commit=enforce_commit,
    )
    kind = exposure_kind(final)
    exposure_path, exposure = _load_bound(
        final.get("exposure_path"),
        final.get("exposure_sha256"),
        kind,
        enforce_commit=enforce_commit,
    )
    preparation_path, preparation = _load_bound(
        protection.get("preparation_path"),
        protection.get("preparation_sha256"),
        "controlled-live-preparation",
        enforce_commit=enforce_commit,
    )
    winner_path, winner = _load_bound(
        preparation.get("winner_path"),
        preparation.get("winner_sha256"),
        "frozen-strategy-winner",
        enforce_commit=enforce_commit,
    )
    for field in (
        "campaign_id",
        "family_id",
        "strategy_id",
        "strategy_version",
        "rules_hash",
    ):
        identities = {
            str(final.get(field)),
            str(protection.get(field)),
            str(exposure.get(field)),
            str(preparation.get(field)),
            str(winner.get(field)),
        }
        if len(identities) != 1:
            raise PortfolioLiveInspectionError(
                f"controlled live {field} binding drifted"
            )
    try:
        prepared_at = portfolio_live._timestamp(
            preparation.get("prepared_at"), "prepared_at"
        )
        replayed_evaluation = portfolio_execution.evaluate_frozen_winner(
            winner,
            preparation.get("market_facts"),
            preparation.get("account"),
            portfolio_maturity.load_config(),
            now=prepared_at,
        )
        if replayed_evaluation != preparation.get("production_evaluation"):
            raise PortfolioLiveInspectionError(
                "live production evaluation does not replay"
            )
        market_observed_at = portfolio_live._timestamp(
            replayed_evaluation["market"]["observed_at"],
            "market observed_at",
        )
        review = portfolio_live._validate_review(
            preparation.get("broker_review"),
            replayed_evaluation,
            market_observed_at=market_observed_at,
            now=prepared_at,
        )
        reviewed_at = portfolio_live._timestamp(
            review["reviewed_at"], "broker_review.reviewed_at"
        )
        snapshot = portfolio_live._validate_guard_snapshot(
            preparation.get("guard_snapshot"),
            winner,
            replayed_evaluation,
            review,
            reviewed_at=reviewed_at,
        )
        maturity_assessment = preparation.get("maturity_assessment")
        if not isinstance(maturity_assessment, Mapping):
            raise PortfolioLiveInspectionError(
                "live preparation maturity assessment is missing"
            )
        replayed_guard = portfolio_guard.evaluate_entry(
            snapshot,
            {"strategies": [dict(maturity_assessment)]},
            portfolio_maturity.load_config(),
            now=prepared_at,
        )
        if replayed_guard != preparation.get("portfolio_guard"):
            raise PortfolioLiveInspectionError("live portfolio guard does not replay")
        if enforce_commit:
            portfolio_live.verify_recorded_git_provenance(
                preparation.get("repository_checks", {})
            )
            current = [
                assessment
                for assessment in portfolio_maturity.build_report().get(
                    "strategies", []
                )
                if assessment.get("strategy_id") == winner["strategy_id"]
                and assessment.get("strategy_version") == winner["strategy_version"]
                and assessment.get("rules_hash") == winner["rules_hash"]
            ]
            if len(current) != 1 or current[0].get("pilot_ready") is not True:
                raise PortfolioLiveInspectionError(
                    "exact strategy no longer has unambiguous PILOT_READY evidence"
                )
    except (
        portfolio_execution.PortfolioExecutionError,
        portfolio_guard.PortfolioGuardError,
        portfolio_live.PortfolioLiveError,
        TypeError,
    ) as exc:
        raise PortfolioLiveInspectionError(
            f"live preparation does not independently replay: {exc}"
        ) from exc
    rebuilt_facts, rebuilt_record = portfolio_live.rebuild_close(
        protection, exposure, preparation, final["closure"]
    )
    if rebuilt_facts != final.get("close_facts"):
        raise PortfolioLiveInspectionError("live close facts do not replay")
    if rebuilt_record != final.get("maturity_record"):
        raise PortfolioLiveInspectionError("live maturity row does not replay")
    eligible = rebuilt_facts["eligible_reconciled_live_close"] is True
    expected_state = (
        "LIVE_CLOSED_RECONCILED_INSPECTION_REQUIRED"
        if eligible
        else "LIVE_CLOSED_SAFETY_FAILURE"
    )
    if final.get("state") != expected_state or (
        final.get("ledger_admission_permitted_after_inspection") is not eligible
    ):
        raise PortfolioLiveInspectionError("live final disposition drifted")
    if eligible:
        portfolio_maturity.validate_record(rebuilt_record)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "controlled-live-inspection",
        "campaign_id": final["campaign_id"],
        "family_id": final["family_id"],
        "strategy_id": final["strategy_id"],
        "strategy_version": final["strategy_version"],
        "rules_hash": final["rules_hash"],
        "signal_id": rebuilt_record["signal_id"],
        "state": (
            "LIVE_CLOSE_INSPECTED_ADMISSION_READY"
            if eligible
            else "LIVE_CLOSE_INSPECTED_NONQUALIFYING"
        ),
        "final_path": strategy_discovery._relative(final_path),
        "final_sha256": final["artifact_sha256"],
        "protection_path": strategy_discovery._relative(protection_path),
        "protection_sha256": protection["artifact_sha256"],
        "exposure_path": strategy_discovery._relative(exposure_path),
        "exposure_sha256": exposure["artifact_sha256"],
        "preparation_path": strategy_discovery._relative(preparation_path),
        "preparation_sha256": preparation["artifact_sha256"],
        "winner_path": strategy_discovery._relative(winner_path),
        "winner_sha256": winner["artifact_sha256"],
        "inspection": {
            "exact_identity_rebuilt": True,
            "winner_and_production_evaluation_rebuilt": True,
            "broker_review_and_portfolio_guard_rebuilt": True,
            "encrypted_identifier_authentication_rebuilt": True,
            "entry_and_protection_timing_rebuilt": True,
            "realized_return_and_r_rebuilt": True,
            "flat_account_reconciliation_rebuilt": True,
            "terminal_residual_orders_rebuilt": True,
            "journal_hash_rebuilt": True,
            "maturity_record_rebuilt": True,
            "valid": True,
        },
        "maturity_record": rebuilt_record,
        "ledger_admission_permitted": eligible,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(final["family_id"]) / "inspection",
        f"{rebuilt_record['date']}-{final['strategy_id']}-live-inspection",
    )


def exposure_kind(final: Mapping[str, Any]) -> str:
    """Resolve the exact permitted entry artifact kind without trusting a field."""
    relative = final.get("exposure_path")
    if not isinstance(relative, str):
        raise PortfolioLiveInspectionError("live exposure path is missing")
    value = strategy_discovery.load_artifact(portfolio_live.PROJECT_ROOT / relative)
    kind = value.get("artifact_kind")
    if kind not in {
        "controlled-live-entry-result",
        "controlled-live-entry-reconciliation",
    }:
        raise PortfolioLiveInspectionError("live exposure artifact kind is invalid")
    return str(kind)


def admit_live(
    inspection_path: Path,
    *,
    ledger_path: Path = portfolio_maturity.DEFAULT_LEDGER_PATH,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(inspection_path)
        strategy_discovery.require_committed(ledger_path)
    inspection = strategy_discovery.load_artifact(
        inspection_path, expected_kind="controlled-live-inspection"
    )
    if (
        inspection.get("state") != "LIVE_CLOSE_INSPECTED_ADMISSION_READY"
        or inspection.get("ledger_admission_permitted") is not True
        or inspection.get("inspection", {}).get("valid") is not True
    ):
        raise PortfolioLiveInspectionError(
            "live inspection is not eligible for maturity admission"
        )
    record = inspection.get("maturity_record")
    if not isinstance(record, Mapping):
        raise PortfolioLiveInspectionError("live maturity record is missing")
    if enforce_commit:
        before = portfolio_maturity.build_report(
            portfolio_maturity.read_records(ledger_path),
            portfolio_maturity.load_config(),
        )
        exact_before = [
            item
            for item in before["strategies"]
            if item["strategy_id"] == record["strategy_id"]
            and item["strategy_version"] == record["strategy_version"]
            and item["rules_hash"] == record["rules_hash"]
        ]
        if len(exact_before) != 1 or exact_before[0].get("pilot_ready") is not True:
            raise PortfolioLiveInspectionError(
                "exact strategy is not PILOT_READY in the committed portfolio ledger"
            )
    portfolio_maturity.append_record(record, ledger_path)
    records = portfolio_maturity.read_records(ledger_path)
    report = portfolio_maturity.build_report(records, portfolio_maturity.load_config())
    exact = [
        item
        for item in report["strategies"]
        if item["strategy_id"] == record["strategy_id"]
        and item["strategy_version"] == record["strategy_version"]
    ]
    return {
        "state": "LIVE_CLOSE_ADMITTED",
        "signal_id": record["signal_id"],
        "strategy_maturity": exact[0]["maturity"] if len(exact) == 1 else None,
        "first_pilot_milestone": report["first_pilot_milestone"],
        "account_flat_reconciled": True,
        "residual_orders_terminal": True,
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
            path, artifact = inspect_live(args.final, root=args.root)
            result = {
                "written": strategy_discovery._relative(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
            }
        else:
            result = admit_live(args.inspection, ledger_path=args.ledger)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        PortfolioLiveInspectionError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
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
