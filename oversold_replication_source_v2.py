"""Operate the exact-date scanner recovery for oversold replication.

This wrapper preserves the committed v1 date selection and scanner semantics,
but replaces the sparse predecessor identity master with the separately
collected and inspected 135-date master.  No strategy grid, date, embargo,
provider, price, or outcome rule changes.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import oversold_replication_source as base
from historical_store import sha256_file
from learning_data import security_master_sha256


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-24-"
    "oversold-replication-confirmation-v2"
)
ROOT = PROJECT_ROOT / "historical_batches/oversold_replication_v4"
SCANNER_MANIFEST_ROOT = ROOT / "scanner-v2-manifests"
CONTROLLER_BINDING_PATH = ROOT / "scanner-v2-controller-binding.json"
SCANNER_CONTRACT_INSPECTION_PATH = ROOT / "scanner-v2-contract-inspection.json"
SCANNER_STATUS_PATH = ROOT / "scanner-v2-collection-status.json"
RUN_ROOT = PROJECT_ROOT / "learning_runs/oversold_replication_v4/scanner_replay_v2"
DETAIL_PATH = RUN_ROOT / "scanner-replay-detail.json"
SUMMARY_PATH = RUN_ROOT / "scanner-replay-summary.json"
SECURITY_MASTER = ROOT / "security-master.jsonl"
SECURITY_SOURCE = ROOT / "security-master-source.json"
SECURITY_INSPECTION_ROOT = ROOT / "security-master-inspection"


class OversoldReplicationSourceV2Error(RuntimeError):
    """The identity-recovered scanner source is incomplete or drifted."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationSourceV2Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise OversoldReplicationSourceV2Error(f"{path} must contain an object")
    return value


def _master_inspection_path() -> Path:
    paths = sorted(SECURITY_INSPECTION_ROOT.glob("*.json"))
    if len(paths) != 1:
        raise OversoldReplicationSourceV2Error(
            "expected one exact-date security-master inspection"
        )
    return paths[0]


def validate_identity_recovery() -> dict[str, Any]:
    for path in (SECURITY_MASTER, SECURITY_SOURCE, _master_inspection_path()):
        base._require_committed(path)
    inspection = _read(_master_inspection_path())
    source = _read(SECURITY_SOURCE)
    checks = {
        "inspection_ready": inspection.get("state")
        == "SECURITY_MASTER_INSPECTED_READY",
        "master_file_bound": inspection.get("security_master_file_sha256")
        == sha256_file(SECURITY_MASTER),
        "master_semantics_bound": inspection.get("security_master_sha256")
        == security_master_sha256(SECURITY_MASTER),
        "source_file_bound": inspection.get("source_file_sha256")
        == sha256_file(SECURITY_SOURCE),
        "exact_dates": source.get("requested_dates")
        == base._selected_dates(),
        "reference_only": inspection.get("market_prices_accessed") is False,
        "no_outcomes": inspection.get("target_outcomes_observed_or_derived")
        is False,
        "no_broker_actions": inspection.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationSourceV2Error(
            "exact-date identity recovery is not ready"
        )
    return checks


def _scanner_manifest_path() -> Path:
    with configured():
        return base._scanner_manifest_path()


def _build_controller_binding() -> dict[str, Any]:
    manifest_path = _scanner_manifest_path()
    manifest = base._read(manifest_path)
    inspection_path = _master_inspection_path()
    result = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_scanner_v2_controller_binding",
        "state": "SCANNER_V2_CONTROLLER_BOUND_AWAITING_INSPECTION",
        "dataset_id": DATASET_ID,
        "controller_path": base._repo_path(Path(__file__).resolve()),
        "controller_sha256": sha256_file(Path(__file__).resolve()),
        "base_controller_path": base._repo_path(Path(base.__file__).resolve()),
        "base_controller_sha256": sha256_file(Path(base.__file__).resolve()),
        "selection_path": base._repo_path(base.SELECTION_PATH),
        "selection_file_sha256": sha256_file(base.SELECTION_PATH),
        "security_master_path": base._repo_path(SECURITY_MASTER),
        "security_master_file_sha256": sha256_file(SECURITY_MASTER),
        "security_source_path": base._repo_path(SECURITY_SOURCE),
        "security_source_file_sha256": sha256_file(SECURITY_SOURCE),
        "security_inspection_path": base._repo_path(inspection_path),
        "security_inspection_file_sha256": sha256_file(inspection_path),
        "scanner_manifest_path": base._repo_path(manifest_path),
        "scanner_manifest_file_sha256": sha256_file(manifest_path),
        "scanner_manifest_sha256": manifest["manifest_sha256"],
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    result["binding_sha256"] = base._hash(result)
    return result


def validate_controller_binding(*, require_committed: bool = True) -> dict[str, Any]:
    if require_committed:
        for path in (
            Path(__file__).resolve(),
            Path(base.__file__).resolve(),
            CONTROLLER_BINDING_PATH,
            _scanner_manifest_path(),
        ):
            base._require_committed(path)
    frozen = _read(CONTROLLER_BINDING_PATH)
    expected = _build_controller_binding()
    if frozen != expected:
        raise OversoldReplicationSourceV2Error(
            "scanner v2 controller binding is missing or drifted"
        )
    return {
        "exact_rebuild": True,
        "controller_bound": True,
        "base_controller_bound": True,
        "identity_recovery_bound": True,
        "scanner_manifest_bound": True,
        "no_outcomes": True,
        "no_broker_actions": True,
    }


@contextmanager
def configured():
    values = {
        "DATASET_ID": DATASET_ID,
        "SCANNER_MANIFEST_ROOT": SCANNER_MANIFEST_ROOT,
        "SCANNER_CONTRACT_INSPECTION_PATH": SCANNER_CONTRACT_INSPECTION_PATH,
        "SCANNER_STATUS_PATH": SCANNER_STATUS_PATH,
        "RUN_ROOT": RUN_ROOT,
        "DETAIL_PATH": DETAIL_PATH,
        "SUMMARY_PATH": SUMMARY_PATH,
        "SECURITY_MASTER": SECURITY_MASTER,
        "SECURITY_SOURCE": SECURITY_SOURCE,
    }
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        yield
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def freeze_scanner(env_path: Path) -> dict[str, Any]:
    checks = validate_identity_recovery()
    base._require_committed(Path(__file__).resolve())
    with configured():
        result = base.freeze_scanner(env_path)
    binding = _build_controller_binding()
    base._write(CONTROLLER_BINDING_PATH, binding)
    return {
        **result,
        "controller_binding_path": base._repo_path(CONTROLLER_BINDING_PATH),
        "controller_binding_sha256": binding["binding_sha256"],
        "identity_recovery_checks": checks,
    }


def inspect_scanner_contract(env_path: Path) -> dict[str, Any]:
    validate_identity_recovery()
    binding_checks = validate_controller_binding()
    with configured():
        result = base.inspect_scanner_contract(env_path)
    result["controller_binding_checks"] = binding_checks
    base._write(SCANNER_CONTRACT_INSPECTION_PATH, result)
    return result


def collect_scanner(env_path: Path, max_days: int | None) -> dict[str, Any]:
    validate_identity_recovery()
    validate_controller_binding()
    with configured():
        return base.collect_scanner(env_path, max_days)


def status(env_path: Path) -> dict[str, Any]:
    with configured():
        return base.status(env_path)


def build_scanner(env_path: Path) -> dict[str, Any]:
    validate_identity_recovery()
    validate_controller_binding()
    with configured():
        return base.build_scanner(env_path)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze-scanner")
    sub.add_parser("inspect-scanner-contract")
    collect = sub.add_parser("collect-scanner")
    collect.add_argument("--max-days", type=int)
    sub.add_parser("status")
    sub.add_parser("build-scanner")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-scanner":
            result = freeze_scanner(args.env)
        elif args.command == "inspect-scanner-contract":
            result = inspect_scanner_contract(args.env)
        elif args.command == "collect-scanner":
            result = collect_scanner(args.env, args.max_days)
        elif args.command == "status":
            result = status(args.env)
        else:
            result = build_scanner(args.env)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        base.OversoldReplicationSourceError,
        OversoldReplicationSourceV2Error,
    ) as exc:
        print(
            json.dumps(
                {"state": "BLOCKED", "error": str(exc), "broker_actions": 0},
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
