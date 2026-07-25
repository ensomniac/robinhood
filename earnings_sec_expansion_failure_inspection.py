"""Independently inspect the terminal v12 SEC source-root failure."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_expansion_capacity as capacity
import earnings_sec_expansion_failure as source
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecExpansionFailureInspectionError(RuntimeError):
    """The v12 SEC source failure failed independent reconstruction."""


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
    checks = {
        "failure_hash_valid": failure.get("failure_sha256")
        == capacity.self_hash(failure, "failure_sha256"),
        "exact_failure_rebuild": failure == rebuilt,
        "committed_inspected_plan_bound": failure.get("plan_sha256")
        == "b17f1cd4982ee5e7c043dedda85e4bacbcbbe4679e91c12cc51c74c40b434b43"
        and failure.get("inspection_sha256")
        == "59705455815bd3b1f87c2f2d4418024fbdb78d88028335ff0f4bbafd2885c336",
        "first_request_exact": failure.get("failed_request", {}).get(
            "quarter"
        )
        == "2012q1"
        and failure.get("failed_request", {}).get("ordinal") == 0,
        "http_404_permanent": failure.get("provider_response", {}).get(
            "http_status"
        )
        == 404
        and failure.get("provider_response", {}).get("classification")
        == "PERMANENT_SOURCE_PATH_NOT_FOUND",
        "source_root_difference_exact": failure.get(
            "root_path_disposition", {}
        ).get("official_index_root")
        == source.OFFICIAL_ARCHIVE_ROOT
        and failure.get("root_path_disposition", {}).get(
            "same_version_retry_permitted"
        )
        is False,
        "zero_retained_metadata": failure.get(
            "provider_telemetry", {}
        ).get("retained_archives")
        == 0
        and failure.get("provider_telemetry", {}).get("retained_bytes") == 0
        and failure.get("metadata_rows_accessed") == 0,
        "zero_outcome_or_broker": failure.get("market_prices_accessed")
        is False
        and failure.get("forward_returns_accessed") is False
        and failure.get("strategy_metrics_computed") == 0
        and failure.get("confirmation_outcomes_accessed") is False
        and failure.get("broker_actions") == 0,
        "bounded_successor_only": failure.get(
            "successor_authority", {}
        ).get("source_root_correction_permitted_after_inspection")
        is True
        and failure.get("successor_authority", {}).get(
            "market_price_access_permitted"
        )
        is False,
    }
    if not all(checks.values()):
        raise EarningsSecExpansionFailureInspectionError(
            "v12 SEC source failure inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-expansion-source-failure-inspection"
        ),
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "state": "SEC_EXPANSION_ARCHIVE_ROOT_404_INSPECTED_TERMINAL",
        "inspected_at": capacity._timestamp(inspected_at, "inspected_at"),
        "failure_path": capacity._repo_path(failure_path),
        "failure_file_sha256": sha256_file(failure_path),
        "failure_sha256": failure["failure_sha256"],
        "checks": checks,
        "v12_resume_permitted": False,
        "corrected_source_successor_permitted": True,
        "market_price_access_authorized": False,
        "confirmation_access_authorized": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.self_hash(
        value, "inspection_sha256"
    )
    output = (
        root
        / "metadata-source-failure-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    capacity._write(output, value)
    return output, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
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
                "valid": value["valid"],
                "market_price_access_authorized": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
