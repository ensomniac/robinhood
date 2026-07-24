"""Independently inspect the oversold scanner target information boundary."""

from __future__ import annotations

import argparse
import csv
import gzip
import json
from collections.abc import Sequence
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any

import oversold_replication_reserve_v2 as reserve
import oversold_replication_source_v3 as source
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
INSPECTION_ROOT = source.ROOT / "scanner-v2-target-boundary-inspection"
COLLECTION_INSPECTION_ROOT = (
    source.ROOT / "scanner-v2-target-collection-inspection"
)


class OversoldScannerTargetAuditError(RuntimeError):
    """The target-session scanner boundary is missing or drifted."""


def inspect(
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    manifest_path = source._scanner_manifest_path()
    required = (
        Path(__file__).resolve(),
        manifest_path,
        source.CONTROLLER_BINDING_PATH,
        source.SCANNER_SELECTION_PATH,
    )
    for path in required:
        strategy_discovery.require_committed(path)
    manifest = source.base.alpaca.load_contract(manifest_path)
    binding = source._read(source.CONTROLLER_BINDING_PATH)
    selection = source._read(source.SCANNER_SELECTION_PATH)
    target_dates = set(map(str, manifest["requested_dates"]))
    reusable = manifest["collection_contract"].get(
        "reusable_source"
    ) or {}
    reused_dates = set(map(str, reusable.get("session_dates", [])))
    status = source.base.alpaca.collection_status(
        manifest,
        store=store or HistoricalDayStore.from_env(),
    )
    checks = {
        "exact_selection": manifest["requested_dates"]
        == selection["selected_dates"],
        "target_opening_only": manifest["collection_contract"].get(
            "target_session_collection"
        )
        == "opening_query_only_no_post_09_35_rows",
        "target_reuse_disjoint": not (target_dates & reused_dates),
        "controller_bound": binding["scanner_manifest_sha256"]
        == manifest["manifest_sha256"],
        "zero_ready_files": status["session_files"]["ready"] == 0,
        "zero_provider_requests": status["provider_requests"] == 0,
        "zero_provider_retries": status["provider_retries"] == 0,
    }
    if not all(checks.values()):
        raise OversoldScannerTargetAuditError(
            "scanner target boundary inspection failed"
        )
    content = {
        "schema_version": 1,
        "artifact_kind": (
            "oversold_replication_scanner_target_boundary_inspection"
        ),
        "state": "TARGET_0935_BOUNDARY_INSPECTED_PROVIDER_READY",
        "manifest_path": source.base._repo_path(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
        "manifest_sha256": manifest["manifest_sha256"],
        "target_dates": len(target_dates),
        "required_sessions": len(
            manifest["collection_contract"]["required_session_dates"]
        ),
        "reused_prior_sessions": len(reused_dates),
        "reused_target_sessions": len(target_dates & reused_dates),
        "checks": checks,
        "provider_requests": 0,
        "full_session_target_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return reserve._publish(
        INSPECTION_ROOT,
        "oversold-replication-scanner-target-boundary-inspection",
        content,
        "inspection_sha256",
    )


def inspect_collection(
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    manifest_path = source._scanner_manifest_path()
    boundary_path = reserve._one(INSPECTION_ROOT)
    required = (
        Path(__file__).resolve(),
        manifest_path,
        boundary_path,
        source.SCANNER_STATUS_PATH,
        source.SCANNER_SELECTION_PATH,
    )
    for path in required:
        strategy_discovery.require_committed(path)
    manifest = source.base.alpaca.load_contract(manifest_path)
    public_status = source._read(source.SCANNER_STATUS_PATH)
    selection = source._read(source.SCANNER_SELECTION_PATH)
    target_dates = list(map(str, selection["selected_dates"]))
    day_store = store or HistoricalDayStore.from_env()
    status = source.base.alpaca.collection_status(
        manifest,
        store=day_store,
    )
    root = source.base.alpaca.index_root(
        day_store,
        str(manifest["dataset_id"]),
    )
    target_rows = 0
    latest_target_time: str | None = None
    for day in target_dates:
        sidecar = source._read(
            root / "attestations" / day[:4] / f"{day}.json"
        )
        if not (
            sidecar.get("information_cutoff")
            == "TARGET_SESSION_09:35_ET"
            and sidecar.get("daily_symbols") == 0
            and sidecar.get("regular_session_rows") == 0
            and sidecar.get("reused_source") is None
        ):
            raise OversoldScannerTargetAuditError(
                f"target sidecar crossed 09:35 boundary: {day}"
            )
        path = root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
        with gzip.open(path, "rt", encoding="utf-8", newline="") as stream:
            for row in csv.DictReader(stream):
                observed = datetime.fromtimestamp(
                    int(row["window_start"]) / 1e9,
                    UTC,
                ).astimezone(source.base.scanner_replay.EASTERN)
                if not time(9, 30) <= observed.time() < time(9, 35):
                    raise OversoldScannerTargetAuditError(
                        f"target row crossed 09:35 boundary: {day}"
                    )
                target_rows += 1
                rendered = observed.isoformat()
                if latest_target_time is None or rendered > latest_target_time:
                    latest_target_time = rendered
    checks = {
        "collection_complete": status["complete"] is True,
        "all_sessions_ready": status["session_files"]["ready"]
        == status["session_files"]["required"]
        == 155,
        "public_status_exact": all(
            public_status[key] == status[key]
            for key in (
                "provider_requests",
                "provider_retries",
                "derived_rows",
                "reused_sessions",
                "delta_symbols_requested",
                "session_files",
            )
        ),
        "zero_substitutions": public_status["substitutions"] == 0,
        "all_target_sidecars_opening_only": True,
        "all_target_rows_before_0935": True,
        "no_target_reuse": True,
        "target_rows_nonempty": target_rows > 0,
    }
    if not all(checks.values()):
        raise OversoldScannerTargetAuditError(
            "scanner target collection inspection failed"
        )
    content = {
        "schema_version": 1,
        "artifact_kind": (
            "oversold_replication_scanner_target_collection_inspection"
        ),
        "state": "TARGET_0935_COLLECTION_INSPECTED_SCANNER_READY",
        "manifest_path": source.base._repo_path(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
        "manifest_sha256": manifest["manifest_sha256"],
        "collection_status_path": source.base._repo_path(
            source.SCANNER_STATUS_PATH
        ),
        "collection_status_file_sha256": sha256_file(
            source.SCANNER_STATUS_PATH
        ),
        "target_dates": len(target_dates),
        "target_rows": target_rows,
        "latest_target_timestamp_et": latest_target_time,
        "checks": checks,
        "provider_telemetry": {
            "requests": status["provider_requests"],
            "retries": status["provider_retries"],
            "reused_prior_sessions": status["reused_sessions"],
            "delta_symbols_requested": status["delta_symbols_requested"],
        },
        "full_session_target_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return reserve._publish(
        COLLECTION_INSPECTION_ROOT,
        "oversold-replication-scanner-target-collection-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("inspect", "inspect-collection"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect":
            path, value = inspect()
        else:
            path, value = inspect_collection()
        print(
            json.dumps(
                {"path": source.base._repo_path(path), **value},
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        OSError,
        OversoldScannerTargetAuditError,
        source.OversoldReplicationSourceV3Error,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"state": "BLOCKED", "error": str(exc)},
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
