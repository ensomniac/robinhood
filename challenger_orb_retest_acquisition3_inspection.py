"""Independently inspect the challenger's third acquisition boundary."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

import challenger_orb_retest_acquisition2_inspection as base_inspection
import challenger_orb_retest_acquisition3 as challenger
from learning_data import LearningDataError
from scanner_replay import ScannerReplayError


class ChallengerAcquisition3InspectionError(RuntimeError):
    """Independent third-tranche acquisition reconstruction differs."""


def _configured_module_values():
    return challenger._module_values()


def inspect_reference():
    values = _configured_module_values()
    original = {name: getattr(challenger.base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(challenger.base, name, value)
        return base_inspection.inspect_reference()
    finally:
        for name, value in original.items():
            setattr(challenger.base, name, value)


def inspect(*, manifest_path: Path, scanner_manifest_path: Path, env_path: Path):
    values = _configured_module_values()
    original = {name: getattr(challenger.base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(challenger.base, name, value)
        result = base_inspection.inspect(
            manifest_path=manifest_path,
            scanner_manifest_path=scanner_manifest_path,
            env_path=env_path,
        )
    finally:
        for name, value in original.items():
            setattr(challenger.base, name, value)
    if not (
        result.get("dataset_id") == challenger.DATASET_ID
        and result.get("status") == "FROZEN_READY"
        and result.get("inspected") is True
        and result.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerAcquisition3InspectionError(
            "third-tranche acquisition inspection boundary differs"
        )
    challenger.base.base._write_json(challenger.DEFAULT_STATUS, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, nargs="?")
    parser.add_argument("scanner_manifest", type=Path, nargs="?")
    parser.add_argument("--env", type=Path, default=challenger.PROJECT_ROOT / ".env")
    parser.add_argument("--reference-only", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.reference_only:
            if args.manifest is not None or args.scanner_manifest is not None:
                raise ChallengerAcquisition3InspectionError(
                    "reference-only inspection does not accept manifests"
                )
            result = inspect_reference()
        else:
            if args.manifest is None or args.scanner_manifest is None:
                raise ChallengerAcquisition3InspectionError(
                    "outer and scanner manifests are required"
                )
            result = inspect(
                manifest_path=args.manifest,
                scanner_manifest_path=args.scanner_manifest,
                env_path=args.env,
            )
    except (
        ChallengerAcquisition3InspectionError,
        challenger.ChallengerAcquisition3Error,
        challenger.base.ChallengerAcquisition2Error,
        challenger.base.base.ChallengerAcquisitionError,
        base_inspection.ChallengerAcquisition2InspectionError,
        LearningDataError,
        ScannerReplayError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
