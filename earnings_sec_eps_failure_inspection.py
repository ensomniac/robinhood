"""Inspect the no-metrics SEC EPS CSV field-limit collection failure."""

from __future__ import annotations

import argparse
import csv
import io
import json
import zipfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as source
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


DEFAULT_FAILURE = (
    source.DEFAULT_ROOT
    / "metadata-collection-failure"
    / "failure-9e7a0cdc16439521ced59f00b8fab8051fb61d45d39e2f8ba1fc66de86410c07.json"
)
DEFAULT_ROOT = source.DEFAULT_ROOT / "metadata-collection-failure-inspection"


class EarningsSecEpsFailureInspectionError(RuntimeError):
    """The field-limit failure or cached archive set did not reconstruct."""


def reproduce_csv_limit(path: Path) -> str:
    """Return the exact default CSV error raised by the first SEC TXT archive."""

    try:
        with zipfile.ZipFile(path) as archive:
            member = source._member_name(archive, "txt.tsv")
            with archive.open(member) as raw:
                with io.TextIOWrapper(raw, encoding="utf-8", newline="") as text:
                    for _row in csv.DictReader(text, delimiter="\t"):
                        pass
    except csv.Error as exc:
        return str(exc)
    raise EarningsSecEpsFailureInspectionError(
        "the first archive no longer reproduces the CSV field-size failure"
    )


def inspect_failure(
    failure_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(failure_path)
    failure = source._read(failure_path)
    contract_path = source.PROJECT_ROOT / str(failure["contract_path"])
    contract_inspection_path = source.PROJECT_ROOT / str(failure["inspection_path"])
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(contract_inspection_path)
    contract = source._read(contract_path)
    contract_inspection = source._read(contract_inspection_path)
    historical_store = store or HistoricalDayStore.from_env()
    archive_rows: list[dict[str, Any]] = []
    total_bytes = 0
    for request in contract["requests"]:
        relative = (
            Path(source.PRIVATE_NAMESPACE)
            / "archives"
            / f"{request['request_sha256']}.zip"
        )
        archive_path = historical_store.root / relative
        try:
            with zipfile.ZipFile(archive_path) as archive:
                crc_failure = archive.testzip()
                members = {
                    required: source._member_name(archive, required)
                    for required in ("sub.tsv", "num.tsv", "txt.tsv")
                }
        except (OSError, zipfile.BadZipFile, source.EarningsSecEpsCapacityError) as exc:
            raise EarningsSecEpsFailureInspectionError(
                f"cached archive differs for {request['quarter']}"
            ) from exc
        if crc_failure is not None:
            raise EarningsSecEpsFailureInspectionError(
                f"cached archive CRC differs for {request['quarter']}"
            )
        size = archive_path.stat().st_size
        total_bytes += size
        archive_rows.append(
            {
                "quarter": request["quarter"],
                "request_sha256": request["request_sha256"],
                "file_sha256": sha256_file(archive_path),
                "bytes": size,
                "required_members": members,
            }
        )
    first = (
        historical_store.root
        / source.PRIVATE_NAMESPACE
        / "archives"
        / f"{contract['requests'][0]['request_sha256']}.zip"
    )
    error = reproduce_csv_limit(first)
    checks = {
        "failure_hash_valid": failure.get("failure_sha256")
        == source.self_hash(failure, "failure_sha256"),
        "contract_file_hash_valid": sha256_file(contract_path)
        == failure.get("contract_file_sha256"),
        "contract_hash_valid": contract.get("contract_sha256")
        == failure.get("contract_sha256")
        == source.self_hash(contract, "contract_sha256"),
        "contract_inspection_file_hash_valid": sha256_file(
            contract_inspection_path
        )
        == failure.get("inspection_file_sha256"),
        "contract_inspection_valid": contract_inspection.get("inspection_sha256")
        == failure.get("inspection_sha256")
        and contract_inspection.get("state") == "SEC_EPS_CONTRACT_INSPECTED_READY"
        and contract_inspection.get("valid") is True,
        "eight_archives_rebuilt": len(archive_rows)
        == failure.get("archives_cached")
        == len(source.ARCHIVES),
        "archive_bytes_rebuilt": total_bytes
        == failure.get("cached_archive_bytes"),
        "first_request_binding_valid": contract["requests"][0]["request_sha256"]
        == failure.get("failed_request_sha256"),
        "csv_default_limit_reproduced": error
        == "field larger than field limit (131072)",
        "provider_requests_accounted": failure.get("provider_telemetry", {}).get(
            "request_count"
        )
        == len(source.ARCHIVES),
        "no_provider_failure_retry_or_substitution": (
            failure.get("provider_telemetry", {}).get("failures") == 0
            and failure.get("provider_telemetry", {}).get("retries") == 0
            and failure.get("provider_telemetry", {}).get("substitutions") == 0
        ),
        "no_derived_or_private_event_artifact": failure.get("derived_event_rows")
        == 0
        and failure.get("private_event_artifact_created") is False,
        "market_prices_absent": failure.get("market_prices_accessed") is False,
        "forward_returns_absent": failure.get("forward_returns_accessed") is False,
        "strategy_metrics_absent": failure.get("strategy_metrics_computed") == 0,
        "confirmation_outcomes_absent": failure.get(
            "confirmation_outcomes_accessed"
        )
        is False,
        "broker_actions_zero": failure.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsSecEpsFailureInspectionError(
            "SEC EPS field-limit failure inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-fsnds-eps-collection-failure-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "SEC_EPS_COLLECTION_FAILURE_INSPECTED_RECOVERY_READY",
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "failure_path": source._repo_path(failure_path),
        "failure_file_sha256": sha256_file(failure_path),
        "failure_sha256": failure["failure_sha256"],
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "archive_count": len(archive_rows),
        "archive_bytes": total_bytes,
        "archive_artifacts": archive_rows,
        "reproduced_error": error,
        "recovery_authority": {
            "implementation_only": True,
            "permitted_change": (
                "raise Python CSV field_size_limit before parsing SEC TSV rows"
            ),
            "archive_changes_permitted": False,
            "event_semantic_changes_permitted": False,
            "partition_changes_permitted": False,
            "capacity_threshold_changes_permitted": False,
            "additional_provider_requests_permitted": 0,
        },
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(value, "inspection_sha256")
    path = root / f"inspection-{value['inspection_sha256']}.json"
    source._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("failure", type=Path, nargs="?", default=DEFAULT_FAILURE)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value = inspect_failure(
        args.failure,
        inspected_at=args.inspected_at,
    )
    print(
        json.dumps(
            {
                "path": source._repo_path(path),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
                "archive_count": value["archive_count"],
                "provider_requests": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
