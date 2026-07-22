"""Operate the challenger's third-tranche pre-outcome acquisition boundary."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import defaultdict
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import challenger_orb_retest_acquisition2 as base
import challenger_orb_retest_tranche3 as tranche3
from learning_data import LearningDataError
from scanner_replay import ScannerReplayError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "64f4dd1091053e6b3ec751248a8d46d5a15a70a3141672773e89d76912355240"
)
DATASET_ID = "dataset-challenger-orb-retest-acquisition-2026-07-22-tranche3-v1"
SCANNER_DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-22-"
    "challenger-orb-retest-tranche3-v1"
)
SELECTION_MANIFEST = (
    tranche3.DEFAULT_MANIFEST_ROOT
    / (
        "dataset-challenger-orb-retest-development-2026-07-22-v3-"
        "90dde41a23e82c07f7fa28f4b45ab60944cd7a4745a0614b60eea7ecab152bfa.json"
    )
)
SELECTION_STATUS = tranche3.DEFAULT_STATUS
ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche3"
DEFAULT_SCANNER_MANIFEST_ROOT = ROOT / "scanner_manifests"
DEFAULT_OUTPUT_ROOT = ROOT / "acquisition_manifests"
DEFAULT_STATUS = ROOT / "acquisition-contract-status.json"
RUN_ROOT = PROJECT_ROOT / "learning_runs/challenger_orb_retest_v1_tranche3/scanner_replay"
RAW_REFERENCE_ROOT = RUN_ROOT / "reference"
REFERENCE_ROOT = RUN_ROOT / "reference-canonical"
ACQUISITION_LOCK = RUN_ROOT / "provider-acquisition.lock"
SECURITY_MASTER = ROOT / "security-master.jsonl"
SECURITY_SOURCE = ROOT / "security-master-source.json"
SPLITS = RUN_ROOT / "splits.json.gz"
SPLIT_SOURCE = ROOT / "split-actions-source.json"
INPUT_STATUS = ROOT / "reference-and-split-status.json"
SCANNER_CONTRACT_STATUS = ROOT / "scanner-contract-status.json"
REUSE_SCANNER_MANIFEST = base.REUSE_SCANNER_MANIFEST
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_acquisition3_inspection.py"


class ChallengerAcquisition3Error(RuntimeError):
    """The third-tranche acquisition adapter or frozen base has drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _module_values(*, reference_root: Path = REFERENCE_ROOT) -> dict[str, Any]:
    tranche3.configure_base()
    return {
        "tranche": tranche3.base,
        "DATASET_ID": DATASET_ID,
        "SCANNER_DATASET_ID": SCANNER_DATASET_ID,
        "SELECTION_MANIFEST": SELECTION_MANIFEST,
        "SELECTION_STATUS": SELECTION_STATUS,
        "ROOT": ROOT,
        "DEFAULT_SCANNER_MANIFEST_ROOT": DEFAULT_SCANNER_MANIFEST_ROOT,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_STATUS": DEFAULT_STATUS,
        "RUN_ROOT": RUN_ROOT,
        "REFERENCE_ROOT": reference_root,
        "ACQUISITION_LOCK": ACQUISITION_LOCK,
        "SECURITY_MASTER": SECURITY_MASTER,
        "SECURITY_SOURCE": SECURITY_SOURCE,
        "SPLITS": SPLITS,
        "SPLIT_SOURCE": SPLIT_SOURCE,
        "INPUT_STATUS": INPUT_STATUS,
        "SCANNER_CONTRACT_STATUS": SCANNER_CONTRACT_STATUS,
        "REUSE_SCANNER_MANIFEST": REUSE_SCANNER_MANIFEST,
        "INSPECTOR": INSPECTOR,
        "__file__": str(Path(__file__).resolve()),
    }


@contextmanager
def configured(*, reference_root: Path = REFERENCE_ROOT) -> Iterator[Any]:
    """Scope third-tranche values through the proven acquisition adapter."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerAcquisition3Error(
            "frozen second-tranche acquisition adapter drifted"
        )
    values = _module_values(reference_root=reference_root)
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        with base.configured() as acquisition:
            yield acquisition
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def _call(name: str, *args: Any, **kwargs: Any):
    with configured() as acquisition:
        return getattr(acquisition, name)(*args, **kwargs)


def _canonicalize_reference_rows(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Normalize case-sensitive provider tickers without guessing collisions."""

    grouped: dict[str, list[tuple[str, Mapping[str, Any]]]] = defaultdict(list)
    exact_symbols: set[str] = set()
    for row in rows:
        raw_symbol = str(row.get("ticker") or "").strip()
        if not raw_symbol or raw_symbol in exact_symbols:
            raise ChallengerAcquisition3Error(
                "raw reference snapshot has an empty or duplicate case-sensitive ticker"
            )
        exact_symbols.add(raw_symbol)
        grouped[raw_symbol.upper()].append((raw_symbol, row))

    canonical: list[dict[str, Any]] = []
    collision_groups = 0
    collision_rows = 0
    normalized_rows = 0
    for execution_symbol, members in sorted(grouped.items()):
        if len(members) != 1:
            collision_groups += 1
            collision_rows += len(members)
            continue
        raw_symbol, source = members[0]
        normalized = dict(source)
        normalized["ticker"] = execution_symbol
        canonical.append(normalized)
        normalized_rows += raw_symbol != execution_symbol
    if not canonical:
        raise ChallengerAcquisition3Error(
            "reference normalization produced an empty canonical snapshot"
        )
    return canonical, {
        "source_rows": len(rows),
        "canonical_rows": len(canonical),
        "case_normalized_rows": normalized_rows,
        "collision_groups_excluded": collision_groups,
        "collision_rows_excluded": collision_rows,
    }


def _write_gzip_array(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    rendered = json.dumps(
        list(rows), sort_keys=True, separators=(",", ":")
    ).encode()
    if path.exists():
        with gzip.open(path, "rt", encoding="utf-8") as source:
            existing = json.load(source)
        if existing != list(rows):
            raise ChallengerAcquisition3Error(
                "canonical reference snapshot already exists with different content"
            )
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as target:
                target.write(rendered)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _reference_normalization_status() -> dict[str, Any]:
    with configured(reference_root=RAW_REFERENCE_ROOT) as acquisition:
        selection = acquisition._selection()
        expected_dates = sorted(selection["selected_dates"])
        raw_snapshots: list[dict[str, Any]] = []
        raw_logical: list[dict[str, Any]] = []
        canonical_snapshots: list[dict[str, Any]] = []
        canonical_logical: list[dict[str, Any]] = []
        totals = {
            "source_rows": 0,
            "canonical_rows": 0,
            "case_normalized_rows": 0,
            "collision_groups_excluded": 0,
            "collision_rows_excluded": 0,
        }
        raw_ready = 0
        canonical_ready = 0
        for day in expected_dates:
            raw_path = RAW_REFERENCE_ROOT / f"{day}.json.gz"
            canonical_path = REFERENCE_ROOT / f"{day}.json.gz"
            if not raw_path.is_file():
                continue
            rows = acquisition._read_gzip_array(raw_path)
            canonical, metrics = _canonicalize_reference_rows(rows)
            raw_ready += 1
            for key, value in metrics.items():
                totals[key] += value
            raw_snapshots.append(
                {
                    "date": day,
                    "rows": len(rows),
                    "sha256": acquisition._sha256_file(raw_path),
                }
            )
            raw_logical.append(
                {
                    "date": day,
                    "rows": len(rows),
                    "content_sha256": acquisition._sha256_json(rows),
                }
            )
            if not canonical_path.is_file():
                continue
            observed = acquisition._read_gzip_array(canonical_path)
            if observed != canonical:
                raise ChallengerAcquisition3Error(
                    f"canonical reference reconstruction differs: {day}"
                )
            canonical_ready += 1
            canonical_snapshots.append(
                {
                    "date": day,
                    "rows": len(observed),
                    "sha256": acquisition._sha256_file(canonical_path),
                }
            )
            canonical_logical.append(
                {
                    "date": day,
                    "rows": len(observed),
                    "content_sha256": acquisition._sha256_json(observed),
                }
            )
        return {
            "schema_version": 1,
            "algorithm": "uppercase-unique-exclude-all-collisions-v1",
            "requested": len(expected_dates),
            "raw_ready": raw_ready,
            "canonical_ready": canonical_ready,
            "complete": raw_ready == canonical_ready == len(expected_dates),
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


def normalize_reference() -> dict[str, Any]:
    with configured(reference_root=RAW_REFERENCE_ROOT) as acquisition:
        acquisition._published(Path(__file__))
        selection = acquisition._selection()
        expected_dates = sorted(selection["selected_dates"])
        with acquisition._exclusive_run_lock(
            ACQUISITION_LOCK, operation="third-tranche reference normalization"
        ):
            for day in expected_dates:
                source = RAW_REFERENCE_ROOT / f"{day}.json.gz"
                if not source.is_file():
                    raise ChallengerAcquisition3Error(
                        f"raw reference snapshot is missing: {day}"
                    )
                rows = acquisition._read_gzip_array(source)
                canonical, _metrics = _canonicalize_reference_rows(rows)
                _write_gzip_array(REFERENCE_ROOT / source.name, canonical)
    status = _reference_normalization_status()
    if status["complete"] is not True:
        raise ChallengerAcquisition3Error(
            "reference normalization did not complete the exact denominator"
        )
    return status


def collect_reference(*, env_path: Path) -> dict[str, Any]:
    with configured(reference_root=RAW_REFERENCE_ROOT) as acquisition:
        acquisition._published(Path(__file__))
        selection = acquisition._selection()
        with acquisition._exclusive_run_lock(
            ACQUISITION_LOCK, operation="third-tranche dated-reference collection"
        ):
            result = acquisition.scanner_replay.collect_reference_snapshots(
                selection["selected_dates"],
                config=acquisition.scanner_replay.MassiveReferenceConfig.from_env(
                    env_path
                ),
                output_root=RAW_REFERENCE_ROOT,
            )
    normalization = normalize_reference()
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "REFERENCE_COLLECTION_COMPLETE",
        "collection": result,
        "reference_normalization": normalization,
        "date_substitution_allowed": False,
        "target_market_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def reference_status() -> dict[str, Any]:
    canonical = _call("reference_status")
    normalization = _reference_normalization_status()
    canonical["complete"] = bool(
        canonical.get("complete") and normalization["complete"]
    )
    canonical["reference_normalization"] = normalization
    return canonical


def _normalization_attestation() -> dict[str, Any]:
    status = _reference_normalization_status()
    if status["complete"] is not True:
        raise ChallengerAcquisition3Error(
            "reference normalization is incomplete"
        )
    return status


def build_master() -> dict[str, Any]:
    result = _call("build_master")
    source = base.base._read_object(SECURITY_SOURCE)
    source["reference_normalization"] = _normalization_attestation()
    base.base._write_json(SECURITY_SOURCE, source)
    result["reference_normalization"] = source["reference_normalization"]
    return result


def audit_inputs(*, env_path: Path) -> dict[str, Any]:
    result = _call("audit_inputs", env_path=env_path)
    source = base.base._read_object(SECURITY_SOURCE)
    expected = _normalization_attestation()
    if source.get("reference_normalization") != expected:
        raise ChallengerAcquisition3Error(
            "security-master normalization attestation differs"
        )
    result["reference_normalization"] = expected
    base.base._write_json(INPUT_STATUS, result)
    return result


def collect_splits(*, env_path: Path) -> dict[str, Any]:
    values = _module_values()
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        return base.collect_splits(env_path=env_path)
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def _scanner_manifest_path() -> Path:
    with configured() as acquisition:
        return acquisition._scanner_manifest_path(DEFAULT_SCANNER_MANIFEST_ROOT)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--scanner-manifest", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("collect-reference")
    sub.add_parser("normalize-reference")
    sub.add_parser("reference-status")
    sub.add_parser("build-master")
    sub.add_parser("collect-splits")
    sub.add_parser("audit-inputs")
    sub.add_parser("freeze-scanner")
    sub.add_parser("freeze")
    collect = sub.add_parser("collect-scanner")
    collect.add_argument("manifest", type=Path)
    collect.add_argument("--max-days", type=int)
    status = sub.add_parser("status")
    status.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "collect-reference":
            result = collect_reference(env_path=args.env)
        elif args.command == "normalize-reference":
            result = normalize_reference()
        elif args.command == "reference-status":
            result = reference_status()
        elif args.command == "build-master":
            result = build_master()
        elif args.command == "collect-splits":
            result = collect_splits(env_path=args.env)
        elif args.command == "audit-inputs":
            result = audit_inputs(env_path=args.env)
        elif args.command == "freeze-scanner":
            _path, result = _call("freeze_scanner", env_path=args.env)
        elif args.command == "freeze":
            scanner_path = args.scanner_manifest or _scanner_manifest_path()
            path, manifest = _call(
                "freeze_contract",
                scanner_manifest_path=scanner_path,
                env_path=args.env,
                output_root=DEFAULT_OUTPUT_ROOT,
            )
            result = {
                "schema_version": 1,
                "dataset_id": DATASET_ID,
                "path": base.base._repo_path(path),
                "manifest_sha256": manifest["manifest_sha256"],
                "status": "FROZEN_READY",
                "inspected": False,
                "target_outcomes_observed_or_derived": False,
            }
            base.base._write_json(DEFAULT_STATUS, result)
        elif args.command == "collect-scanner":
            result = _call(
                "collect_scanner",
                manifest_path=args.manifest,
                env_path=args.env,
                max_days=args.max_days,
            )
        else:
            result = _call("status", manifest_path=args.manifest, env_path=args.env)
    except (
        ChallengerAcquisition3Error,
        base.ChallengerAcquisition2Error,
        base.base.ChallengerAcquisitionError,
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
