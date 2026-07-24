"""Build the inspected target-cutoff oversold scanner replay."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import oversold_replication_reserve_v2 as reserve
import oversold_replication_source_v3 as source
import oversold_scanner_target_audit as target_audit
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
BUILD_STATUS_ROOT = source.ROOT / "scanner-v2-build-status"


class OversoldScannerBuilderError(RuntimeError):
    """The inspected scanner inputs or replay build have drifted."""


def build(
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    manifest_path = source._scanner_manifest_path()
    target_inspection_path = reserve._one(
        target_audit.COLLECTION_INSPECTION_ROOT
    )
    required = (
        Path(__file__).resolve(),
        manifest_path,
        source.CONTROLLER_BINDING_PATH,
        source.SCANNER_STATUS_PATH,
        target_inspection_path,
    )
    for path in required:
        strategy_discovery.require_committed(path)
    source.validate_controller_binding()
    manifest = source.base.alpaca.load_contract(manifest_path)
    target_inspection = reserve._load_hashed(
        target_inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_scanner_target_collection_inspection"
        ),
    )
    if not (
        target_inspection["state"]
        == "TARGET_0935_COLLECTION_INSPECTED_SCANNER_READY"
        and target_inspection["valid"] is True
        and target_inspection["manifest_sha256"]
        == manifest["manifest_sha256"]
    ):
        raise OversoldScannerBuilderError(
            "target-cutoff collection is not independently ready"
        )
    day_store = store or HistoricalDayStore.from_env()
    summary = source.base.alpaca.build_contract(
        manifest,
        calendar_path=source.base.CALENDAR_PATH,
        rules_path=source.base.RULES_PATH,
        splits_path=source.base.SPLITS,
        store=day_store,
        run_root=source.RUN_ROOT,
        summary_output=source.SUMMARY_PATH,
    )
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_scanner_build_status",
        "state": "SCANNER_REPLAY_BUILT_AWAITING_INSPECTION",
        "manifest_path": source.base._repo_path(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
        "manifest_sha256": manifest["manifest_sha256"],
        "target_collection_inspection_path": source.base._repo_path(
            target_inspection_path
        ),
        "target_collection_inspection_file_sha256": sha256_file(
            target_inspection_path
        ),
        "target_collection_inspection_sha256": target_inspection[
            "inspection_sha256"
        ],
        "detail_path": source.base._repo_path(source.DETAIL_PATH),
        "detail_file_sha256": sha256_file(source.DETAIL_PATH),
        "summary_path": source.base._repo_path(source.SUMMARY_PATH),
        "summary_file_sha256": sha256_file(source.SUMMARY_PATH),
        "dates": len(manifest["requested_dates"]),
        "completed_dates": summary["completed_dates"],
        "provider_requests": 0,
        "full_session_target_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    return reserve._publish(
        BUILD_STATUS_ROOT,
        "oversold-replication-scanner-build-status",
        content,
        "status_sha256",
    )


def main(argv: Sequence[str] | None = None) -> int:
    argparse.ArgumentParser(description=__doc__).parse_args(argv)
    try:
        path, value = build()
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
        OversoldScannerBuilderError,
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
