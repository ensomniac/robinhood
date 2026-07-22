"""Operate the challenger's second-tranche pre-outcome acquisition boundary."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import challenger_orb_retest_acquisition as base
import challenger_orb_retest_tranche2 as tranche
import scanner_replay
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_CONTROLLER = PROJECT_ROOT / "challenger_orb_retest_acquisition.py"
DATASET_ID = "dataset-challenger-orb-retest-acquisition-2026-07-21-tranche2-v1"
SCANNER_DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-21-challenger-orb-retest-tranche2-v1"
)
SELECTION_MANIFEST = (
    tranche.DEFAULT_MANIFEST_ROOT
    / (
        "dataset-challenger-orb-retest-development-2026-07-21-v2-"
        "cfa2e9fed75d3f4adc5a8756c7f2192a9f058670a7858ed377365348040d55a7.json"
    )
)
SELECTION_STATUS = tranche.DEFAULT_STATUS
ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche2"
DEFAULT_SCANNER_MANIFEST_ROOT = ROOT / "scanner_manifests"
DEFAULT_OUTPUT_ROOT = ROOT / "acquisition_manifests"
DEFAULT_STATUS = ROOT / "acquisition-contract-status.json"
RUN_ROOT = PROJECT_ROOT / "learning_runs/challenger_orb_retest_v1_tranche2/scanner_replay"
REFERENCE_ROOT = RUN_ROOT / "reference"
ACQUISITION_LOCK = RUN_ROOT / "provider-acquisition.lock"
SECURITY_MASTER = ROOT / "security-master.jsonl"
SECURITY_SOURCE = ROOT / "security-master-source.json"
SPLITS = RUN_ROOT / "splits.json.gz"
SPLIT_SOURCE = ROOT / "split-actions-source.json"
INPUT_STATUS = ROOT / "reference-and-split-status.json"
SCANNER_CONTRACT_STATUS = ROOT / "scanner-contract-status.json"
REUSE_SCANNER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/scanner_manifests"
    / (
        "dataset-production-scanner-replay-2026-07-21-challenger-orb-retest-v2-"
        "77dc80b560b67aaf3ab3991e7a67ffef0cb344ab79ece4e79e868d4b50b96640.json"
    )
)
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_acquisition2_inspection.py"
_BASE_EXPECTED_CONTRACT = base._expected_contract
_BASE_FILE = str(BASE_CONTROLLER)


class ChallengerAcquisition2Error(RuntimeError):
    """The second-tranche acquisition boundary cannot be proven or operated."""


def _configured_values() -> dict[str, Any]:
    return {
        "tranche": tranche,
        "DATASET_ID": DATASET_ID,
        "SCANNER_DATASET_ID": SCANNER_DATASET_ID,
        "SELECTION_MANIFEST": SELECTION_MANIFEST,
        "SELECTION_STATUS": SELECTION_STATUS,
        "DEFAULT_SCANNER_MANIFEST_ROOT": DEFAULT_SCANNER_MANIFEST_ROOT,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_STATUS": DEFAULT_STATUS,
        "RUN_ROOT": RUN_ROOT,
        "REFERENCE_ROOT": REFERENCE_ROOT,
        "ACQUISITION_LOCK": ACQUISITION_LOCK,
        "SECURITY_MASTER": SECURITY_MASTER,
        "SECURITY_SOURCE": SECURITY_SOURCE,
        "SPLITS": SPLITS,
        "SPLIT_SOURCE": SPLIT_SOURCE,
        "INPUT_STATUS": INPUT_STATUS,
        "SCANNER_CONTRACT_STATUS": SCANNER_CONTRACT_STATUS,
        "REUSE_SCANNER_MANIFEST": REUSE_SCANNER_MANIFEST,
        "INSPECTOR": INSPECTOR,
        "_expected_contract": _expected_contract,
        "_split_attestation": _split_attestation,
        "__file__": str(Path(__file__).resolve()),
    }


@contextmanager
def configured():
    values = _configured_values()
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        yield base
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def _expected_contract(**kwargs: Any) -> dict[str, Any]:
    result = _BASE_EXPECTED_CONTRACT(**kwargs)
    implementations = dict(result["implementation_contract"])
    implementations["base_controller"] = base._binding(BASE_CONTROLLER)
    result["implementation_contract"] = implementations
    return result


def _split_query_bounds() -> tuple[str, str]:
    selection = base._selection()
    requested = sorted(str(value) for value in selection["selected_dates"])
    required = scanner_replay.required_sessions(
        requested,
        tranche._calendar_dates(),
        prior_sessions=tranche.PRIOR_SESSIONS,
    )
    if not required:
        raise ChallengerAcquisition2Error("second-tranche session graph is empty")
    return required[0], required[-1]


def _split_attestation() -> dict[str, Any]:
    rows = base._read_gzip_array(SPLITS)
    if not rows:
        raise ChallengerAcquisition2Error("split action result is unexpectedly empty")
    start, end = _split_query_bounds()
    ordered: list[tuple[str, str]] = []
    for row in rows:
        execution = str(row.get("execution_date") or "")
        ticker = str(row.get("ticker") or "").strip().upper()
        try:
            split_from = float(row["split_from"])
            split_to = float(row["split_to"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ChallengerAcquisition2Error("split row is malformed") from exc
        if not start <= execution <= end or not ticker or split_from <= 0 or split_to <= 0:
            raise ChallengerAcquisition2Error("split row is outside the frozen query")
        ordered.append((execution, ticker))
    if ordered != sorted(ordered):
        raise ChallengerAcquisition2Error("split actions are not deterministically sorted")
    return {
        "schema_version": 1,
        "source": {
            "provider": "Massive",
            "endpoint": "https://api.massive.com/stocks/v1/splits",
            "query_range": {
                "execution_date_gte": start,
                "execution_date_lte": end,
            },
        },
        "artifact": {
            "local_ignored_path": base._repo_path(SPLITS),
            "events": len(rows),
            "sha256": base._sha256_file(SPLITS),
        },
    }


def collect_splits(*, env_path: Path) -> dict[str, Any]:
    with configured():
        base._published(Path(__file__))
        with base._exclusive_run_lock(
            ACQUISITION_LOCK, operation="second-tranche split-action collection"
        ):
            start, end = _split_query_bounds()
            result = scanner_replay.collect_split_actions(
                start=start,
                end=end,
                config=scanner_replay.MassiveReferenceConfig.from_env(env_path),
                output=SPLITS,
            )
            attestation = _split_attestation()
            base._write_json(SPLIT_SOURCE, attestation)
        return {
            "schema_version": 1,
            "dataset_id": tranche.DATASET_ID,
            "status": "SPLIT_ACTIONS_READY",
            "collection": result,
            "attestation": attestation,
            "target_market_data_accessed": False,
            "target_outcomes_observed_or_derived": False,
        }


def _scanner_manifest_path() -> Path:
    with configured():
        return base._scanner_manifest_path(DEFAULT_SCANNER_MANIFEST_ROOT)


def _call(name: str, *args: Any, **kwargs: Any):
    with configured():
        return getattr(base, name)(*args, **kwargs)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--scanner-manifest", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("collect-reference")
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
            result = _call("collect_reference", env_path=args.env)
        elif args.command == "reference-status":
            result = _call("reference_status")
        elif args.command == "build-master":
            result = _call("build_master")
        elif args.command == "collect-splits":
            result = collect_splits(env_path=args.env)
        elif args.command == "audit-inputs":
            result = _call("audit_inputs", env_path=args.env)
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
                "path": base._repo_path(path),
                "manifest_sha256": manifest["manifest_sha256"],
                "status": "FROZEN_READY",
                "inspected": False,
                "target_outcomes_observed_or_derived": False,
            }
            base._write_json(DEFAULT_STATUS, result)
        elif args.command == "collect-scanner":
            result = _call(
                "collect_scanner",
                manifest_path=args.manifest,
                env_path=args.env,
                max_days=args.max_days,
            )
        else:
            result = _call("status", manifest_path=args.manifest, env_path=args.env)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerAcquisition2Error,
        base.ChallengerAcquisitionError,
        LearningDataError,
        scanner_replay.ScannerReplayError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
