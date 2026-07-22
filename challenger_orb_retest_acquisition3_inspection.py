"""Independently inspect the challenger's third acquisition boundary."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections.abc import Mapping
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


def _inspect_normalization() -> dict[str, object]:
    with challenger.configured(
        reference_root=challenger.RAW_REFERENCE_ROOT
    ) as acquisition:
        selection = acquisition._selection()
        expected_dates = sorted(selection["selected_dates"])
        expected_names = {f"{day}.json.gz" for day in expected_dates}
        for root in (challenger.RAW_REFERENCE_ROOT, challenger.REFERENCE_ROOT):
            observed_names = {
                path.name
                for path in root.rglob("*")
                if path.is_file() and path.suffix != ".tmp"
            }
            temporary = [
                path
                for path in root.rglob("*")
                if path.is_file() and path.suffix == ".tmp"
            ]
            if observed_names != expected_names or temporary:
                raise ChallengerAcquisition3InspectionError(
                    "raw or canonical reference denominator differs"
                )

        raw_snapshots: list[dict[str, object]] = []
        raw_logical: list[dict[str, object]] = []
        canonical_snapshots: list[dict[str, object]] = []
        canonical_logical: list[dict[str, object]] = []
        totals = {
            "source_rows": 0,
            "canonical_rows": 0,
            "case_normalized_rows": 0,
            "collision_groups_excluded": 0,
            "collision_rows_excluded": 0,
        }
        for day in expected_dates:
            raw_path = challenger.RAW_REFERENCE_ROOT / f"{day}.json.gz"
            canonical_path = challenger.REFERENCE_ROOT / f"{day}.json.gz"
            raw_rows = acquisition._read_gzip_array(raw_path)
            canonical_rows = acquisition._read_gzip_array(canonical_path)
            grouped: dict[str, list[tuple[str, Mapping[str, object]]]] = defaultdict(
                list
            )
            exact: set[str] = set()
            for row in raw_rows:
                raw_symbol = str(row.get("ticker") or "").strip()
                if not raw_symbol or raw_symbol in exact:
                    raise ChallengerAcquisition3InspectionError(
                        f"raw case-sensitive ticker differs: {day}"
                    )
                exact.add(raw_symbol)
                grouped[raw_symbol.upper()].append((raw_symbol, row))
            rebuilt: list[dict[str, object]] = []
            collision_groups = 0
            collision_rows = 0
            normalized_rows = 0
            for execution_symbol, members in sorted(grouped.items()):
                if len(members) != 1:
                    collision_groups += 1
                    collision_rows += len(members)
                    continue
                raw_symbol, row = members[0]
                normalized = dict(row)
                normalized["ticker"] = execution_symbol
                rebuilt.append(normalized)
                normalized_rows += raw_symbol != execution_symbol
            if rebuilt != canonical_rows:
                raise ChallengerAcquisition3InspectionError(
                    f"canonical ticker transform differs: {day}"
                )
            per_date = {
                "source_rows": len(raw_rows),
                "canonical_rows": len(canonical_rows),
                "case_normalized_rows": normalized_rows,
                "collision_groups_excluded": collision_groups,
                "collision_rows_excluded": collision_rows,
            }
            for key, value in per_date.items():
                totals[key] += value
            raw_snapshots.append(
                {
                    "date": day,
                    "rows": len(raw_rows),
                    "sha256": acquisition._sha256_file(raw_path),
                }
            )
            raw_logical.append(
                {
                    "date": day,
                    "rows": len(raw_rows),
                    "content_sha256": acquisition._sha256_json(raw_rows),
                }
            )
            canonical_snapshots.append(
                {
                    "date": day,
                    "rows": len(canonical_rows),
                    "sha256": acquisition._sha256_file(canonical_path),
                }
            )
            canonical_logical.append(
                {
                    "date": day,
                    "rows": len(canonical_rows),
                    "content_sha256": acquisition._sha256_json(canonical_rows),
                }
            )
        return {
            "schema_version": 1,
            "algorithm": "uppercase-unique-exclude-all-collisions-v1",
            "requested": len(expected_dates),
            "raw_ready": len(raw_snapshots),
            "canonical_ready": len(canonical_snapshots),
            "complete": True,
            **totals,
            "raw_snapshot_set_sha256": acquisition._sha256_json(raw_snapshots),
            "raw_logical_snapshot_set_sha256": acquisition._sha256_json(raw_logical),
            "canonical_snapshot_set_sha256": acquisition._sha256_json(
                canonical_snapshots
            ),
            "canonical_logical_snapshot_set_sha256": acquisition._sha256_json(
                canonical_logical
            ),
            "date_substitution_allowed": False,
            "target_market_data_accessed": False,
            "target_outcomes_observed_or_derived": False,
        }


def inspect_reference():
    values = _configured_module_values()
    original = {name: getattr(challenger.base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(challenger.base, name, value)
        result = base_inspection.inspect_reference()
    finally:
        for name, value in original.items():
            setattr(challenger.base, name, value)
    result["reference_normalization"] = _inspect_normalization()
    return result


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
    normalization = _inspect_normalization()
    source = challenger.base.base._read_object(challenger.SECURITY_SOURCE)
    if source.get("reference_normalization") != normalization:
        raise ChallengerAcquisition3InspectionError(
            "reference normalization attestation differs"
        )
    result["reference_normalization"] = normalization
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
