"""Independently inspect and index the terminal v13 source failure."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import earnings_sec_corrected_expansion as capacity
import earnings_sec_reaction_v13_failure as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecReactionV13FailureInspectionError(RuntimeError):
    """The v13 partial-source failure failed reconstruction."""


def inspect_failure(
    failure_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = capacity.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(failure_path)
    failure = capacity._read(failure_path)
    rebuilt = source.build_failure(
        failed_at=str(failure["failed_at"]), store=store
    )
    retained = failure.get("retained_tasks", [])
    checks = {
        "failure_hash_valid": failure.get("failure_sha256")
        == capacity.self_hash(failure, "failure_sha256"),
        "exact_failure_rebuild": failure == rebuilt,
        "committed_inspected_search_bound": failure.get("search_sha256")
        == "36a9738a580d04c53bd94b5a683b341febdc115e1666396f58d5d1ac22852da7"
        and failure.get("inspection_sha256")
        == "52bbde4c0d4136aaf7c7e6d1a486cee745064afdc1efc0ab719679ebc3ab64cd",
        "retained_prefix_exact": len(retained) == 26
        and [row["symbol"] for row in retained]
        == failure["opened_scope"]["symbols"][:26],
        "failed_request_exact": failure.get("failure", {}).get(
            "failed_ordinal"
        )
        == 26
        and failure.get("failure", {}).get("failed_request", {}).get(
            "symbol"
        )
        == "AMCF"
        and failure.get("failure", {}).get("code")
        == source.FAILURE_CODE,
        "request_accounting_exact": failure.get(
            "provider_telemetry", {}
        ).get("attempted_requests")
        == 27
        and failure.get("provider_telemetry", {}).get("retained_tasks")
        == 26
        and failure.get("provider_telemetry", {}).get("failures") == 1,
        "opened_scope_exact": len(
            failure.get("opened_scope", {}).get("symbols", [])
        )
        == 27
        and len(failure.get("opened_scope", {}).get("dates", [])) > 700,
        "same_version_terminal": failure.get(
            "same_version_resume_permitted"
        )
        is False,
        "bounded_successor_only": failure.get(
            "successor_authority", {}
        ).get("remaining_untouched_symbols")
        == 608
        and failure.get("successor_authority", {}).get(
            "search_freeze_required_before_remaining_access"
        )
        is True,
        "zero_metrics_confirmation_or_broker": failure.get(
            "strategy_metrics_computed"
        )
        == 0
        and failure.get("confirmation_prices_accessed") is False
        and failure.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsSecReactionV13FailureInspectionError(
            "v13 source failure inspection failed"
        )
    exposure_id = (
        "source-failure-earnings-sec-reaction-v13-"
        f"{failure['failure_sha256'][:16]}"
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v13-source-failure-inspection"
        ),
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": failure["family_id"],
        "successor_id": failure["successor_id"],
        "state": "REACTION_V13_INVALID_OHLCV_INSPECTED_TERMINAL",
        "inspected_at": capacity._timestamp(
            inspected_at, "inspected_at"
        ),
        "failure_path": capacity._repo_path(failure_path),
        "failure_file_sha256": sha256_file(failure_path),
        "failure_sha256": failure["failure_sha256"],
        "checks": checks,
        "exposure_id": exposure_id,
        "exposure_scope": failure["opened_scope"],
        "v13_resume_permitted": False,
        "remaining_608_symbols_successor_permitted": True,
        "successor_search_freeze_required_before_access": True,
        "invalid_ohlcv_permanent_missing_policy_permitted": True,
        "confirmation_access_authorized": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.self_hash(
        value, "inspection_sha256"
    )
    output = (
        root
        / "development-source-failure-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    capacity._write(output, value)
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=exposure_id,
            campaign_id=capacity.CAMPAIGN_ID,
            lane="development",
            recorded_at=value["inspected_at"],
            source_path=capacity._repo_path(output),
            source_sha256=sha256_file(output),
            scope=failure["opened_scope"],
        )
    )
    return output, value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("failure", type=Path)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value = inspect_failure(
        args.failure, inspected_at=args.inspected_at
    )
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "inspection_sha256": value["inspection_sha256"],
                "state": value["state"],
                "exposure_id": value["exposure_id"],
                "remaining_untouched_symbols": 608,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
