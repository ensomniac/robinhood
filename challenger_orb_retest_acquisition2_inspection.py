"""Independently inspect the challenger's second acquisition boundary."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_acquisition_inspection as prior_inspection
import challenger_orb_retest_acquisition2 as acquisition2
from historical_store import HistoricalStoreConfig
from learning_data import LearningDataError, load_security_master, security_master_sha256
from scanner_replay import ScannerReplayError


class ChallengerAcquisition2InspectionError(RuntimeError):
    """Independent second-tranche acquisition reconstruction found a mismatch."""


def inspect_reference() -> dict[str, Any]:
    with acquisition2.configured():
        return prior_inspection.inspect_reference()


def inspect_inputs(*, env_path: Path) -> dict[str, Any]:
    with acquisition2.configured() as acquisition:
        selection = acquisition._selection()
        expected_dates = sorted(selection["selected_dates"])
        source = acquisition._read_object(acquisition.SECURITY_SOURCE)
        snapshots = source.get("snapshots")
        if (
            source.get("requested_dates") != expected_dates
            or not isinstance(snapshots, list)
            or [str(row.get("date") or "") for row in snapshots] != expected_dates
        ):
            raise ChallengerAcquisition2InspectionError(
                "security-master source dates differ"
            )
        raw_hashes: list[dict[str, Any]] = []
        logical_hashes: list[dict[str, Any]] = []
        for public in snapshots:
            day = str(public["date"])
            path = acquisition.REFERENCE_ROOT / f"{day}.json.gz"
            rows = acquisition._read_gzip_array(path)
            symbols = [str(row.get("ticker") or "").strip().upper() for row in rows]
            observed = {
                "date": day,
                "rows": len(rows),
                "sha256": acquisition._sha256_file(path),
            }
            if (
                observed != public
                or not rows
                or any(not symbol for symbol in symbols)
                or len(symbols) != len(set(symbols))
            ):
                raise ChallengerAcquisition2InspectionError(
                    f"reference snapshot differs: {day}"
                )
            raw_hashes.append(observed)
            logical_hashes.append(
                {
                    "date": day,
                    "rows": len(rows),
                    "content_sha256": acquisition._sha256_json(rows),
                }
            )

        master = load_security_master(acquisition.SECURITY_MASTER)
        master_public = source.get("security_master")
        if not isinstance(master_public, Mapping) or any(
            (
                master_public.get("sha256")
                != security_master_sha256(acquisition.SECURITY_MASTER),
                master_public.get("records") != len(master),
                master_public.get("instruments")
                != len({str(row["instrument_id"]) for row in master}),
            )
        ):
            raise ChallengerAcquisition2InspectionError("security master differs")

        split_source = acquisition._read_object(acquisition.SPLIT_SOURCE)
        split_rows = acquisition._read_gzip_array(acquisition.SPLITS)
        rebuilt_split = acquisition2._split_attestation()
        if split_source != rebuilt_split:
            raise ChallengerAcquisition2InspectionError("split actions differ")
        config = HistoricalStoreConfig.from_env(env_path)
        market_root = acquisition.alpaca.index_root(
            acquisition.HistoricalDayStore(config.root), acquisition.SCANNER_DATASET_ID
        )
        market_artifacts = (
            sum(1 for path in market_root.rglob("*") if path.is_file())
            if market_root.exists()
            else 0
        )
        if market_artifacts or shutil.disk_usage(config.root).free < config.min_free_bytes:
            raise ChallengerAcquisition2InspectionError(
                "market zero-state or reserve differs"
            )
        return {
            "snapshot_count": len(raw_hashes),
            "snapshot_set_sha256": acquisition._sha256_json(raw_hashes),
            "logical_snapshot_set_sha256": acquisition._sha256_json(logical_hashes),
            "security_master_sha256": master_public["sha256"],
            "security_master_records": len(master),
            "split_actions_sha256": rebuilt_split["artifact"]["sha256"],
            "split_action_events": len(split_rows),
            "split_query_range": rebuilt_split["source"]["query_range"],
            "pre_freeze_target_market_artifacts": market_artifacts,
            "capacity_ready": True,
        }


def inspect(
    *, manifest_path: Path, scanner_manifest_path: Path, env_path: Path
) -> dict[str, Any]:
    with acquisition2.configured():
        original = prior_inspection.inspect_inputs
        prior_inspection.inspect_inputs = inspect_inputs
        try:
            return prior_inspection.inspect(
                manifest_path=manifest_path,
                scanner_manifest_path=scanner_manifest_path,
                env_path=env_path,
            )
        finally:
            prior_inspection.inspect_inputs = original


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, nargs="?")
    parser.add_argument("scanner_manifest", type=Path, nargs="?")
    parser.add_argument("--env", type=Path, default=acquisition2.PROJECT_ROOT / ".env")
    parser.add_argument("--reference-only", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.reference_only:
            if args.manifest is not None or args.scanner_manifest is not None:
                raise ChallengerAcquisition2InspectionError(
                    "reference-only inspection does not accept manifests"
                )
            result = inspect_reference()
        else:
            if args.manifest is None or args.scanner_manifest is None:
                raise ChallengerAcquisition2InspectionError(
                    "outer and scanner manifests are required"
                )
            result = inspect(
                manifest_path=args.manifest,
                scanner_manifest_path=args.scanner_manifest,
                env_path=args.env,
            )
            acquisition2.base._write_json(acquisition2.DEFAULT_STATUS, result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerAcquisition2InspectionError,
        prior_inspection.ChallengerAcquisitionInspectionError,
        acquisition2.ChallengerAcquisition2Error,
        acquisition2.base.ChallengerAcquisitionError,
        LearningDataError,
        ScannerReplayError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
