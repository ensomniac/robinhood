"""Build the exact dated identity source for oversold replication.

The first scanner freeze correctly failed because a sparse reference master did
not observe every 2026 target date.  This recovery preserves the already frozen
135-session selection and collects only Massive common-stock reference rows.
It never accesses a market price, target return, or broker.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import scanner_replay
from historical_store import sha256_file
from learning_data import (
    load_security_master,
    security_master_sha256,
    security_record_covers,
)


PROJECT_ROOT = Path(__file__).resolve().parent
ROOT = PROJECT_ROOT / "historical_batches/oversold_replication_v4"
SELECTION_PATH = ROOT / "selection-2026-oversold-replication.json"
SELECTION_INSPECTION_PATH = ROOT / "selection-inspection.json"
V1_FAILURE_ROOT = ROOT / "scanner-v1-failure"
REFERENCE_CONTRACT_ROOT = ROOT / "reference-contract"
REFERENCE_CONTRACT_INSPECTION_ROOT = ROOT / "reference-contract-inspection"
REFERENCE_STATUS_ROOT = ROOT / "reference-status"
REFERENCE_DATA_INSPECTION_ROOT = ROOT / "reference-data-inspection"
SECURITY_MASTER = ROOT / "security-master.jsonl"
SECURITY_SOURCE = ROOT / "security-master-source.json"
SECURITY_INSPECTION_ROOT = ROOT / "security-master-inspection"
PRIVATE_REFERENCE_ROOT = (
    PROJECT_ROOT / "learning_runs/oversold_replication_v4/reference"
)
MASSIVE_ENDPOINT = "https://api.massive.com/v3/reference/tickers"


class OversoldReplicationReferenceError(RuntimeError):
    """The reference-only recovery chain is incomplete or drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationReferenceError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationReferenceError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationReferenceError(
            f"path escaped repository: {path}"
        ) from exc


def _require_committed(path: Path) -> None:
    relative = _repo_path(path)
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    if tracked.returncode != 0 or clean.returncode != 0:
        raise OversoldReplicationReferenceError(
            f"reference predecessor must be committed: {relative}"
        )


def _publish(
    root: Path,
    prefix: str,
    content: Mapping[str, Any],
    *,
    identity_field: str,
) -> tuple[Path, dict[str, Any]]:
    value = dict(content)
    identity = _hash(value)
    value[identity_field] = identity
    path = root / f"{prefix}-{identity}.json"
    if path.exists() and _read(path) != value:
        raise OversoldReplicationReferenceError(
            f"hash-addressed artifact drifted: {path}"
        )
    if not path.exists():
        _write(path, value)
    return path, value


def _one(root: Path, pattern: str) -> Path:
    paths = sorted(root.glob(pattern))
    if len(paths) != 1:
        raise OversoldReplicationReferenceError(
            f"expected exactly one artifact in {root}"
        )
    return paths[0]


def _selection() -> tuple[dict[str, Any], dict[str, Any]]:
    selection = _read(SELECTION_PATH)
    inspection = _read(SELECTION_INSPECTION_PATH)
    dates = selection.get("selected_dates")
    if not (
        isinstance(dates, list)
        and len(dates) == 135
        and dates == sorted(dates)
        and dates[0] == "2026-01-02"
        and dates[-1] == "2026-07-17"
        and selection.get("target_outcomes_observed_or_derived") is False
        and inspection.get("state") == "SELECTION_INSPECTED_READY"
        and inspection.get("selection_file_sha256") == sha256_file(SELECTION_PATH)
        and inspection.get("target_outcomes_observed_or_derived") is False
    ):
        raise OversoldReplicationReferenceError(
            "frozen selection predecessor is invalid"
        )
    return selection, inspection


def record_v1_failure() -> dict[str, Any]:
    _require_committed(SELECTION_PATH)
    _require_committed(SELECTION_INSPECTION_PATH)
    selection, inspection = _selection()
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_scanner_freeze_failure",
        "state": "FAILED_IDENTITY_COVERAGE_BEFORE_PROVIDER_ACCESS",
        "selection_path": _repo_path(SELECTION_PATH),
        "selection_file_sha256": sha256_file(SELECTION_PATH),
        "selection_inspection_path": _repo_path(SELECTION_INSPECTION_PATH),
        "selection_inspection_sha256": inspection["inspection_sha256"],
        "dates": len(selection["selected_dates"]),
        "first_missing_identity_date": "2026-01-02",
        "failure": (
            "sparse predecessor security master has no observed common-stock "
            "universe on the first exact target date"
        ),
        "recovery_limit": (
            "collect exact-date reference identities only; preserve all dates, "
            "embargo, strategy grid, outcome boundaries, and provider choices"
        ),
        "scanner_contract_written": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    _path, value = _publish(
        V1_FAILURE_ROOT,
        "oversold-replication-scanner-v1-failure",
        content,
        identity_field="failure_sha256",
    )
    return value


def build_reference_contract() -> dict[str, Any]:
    selection, inspection = _selection()
    dates = selection["selected_dates"]
    existing = sorted(PRIVATE_REFERENCE_ROOT.glob("*.json.gz"))
    return {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_reference_contract",
        "state": "REFERENCE_CONTRACT_FROZEN",
        "selection_path": _repo_path(SELECTION_PATH),
        "selection_file_sha256": sha256_file(SELECTION_PATH),
        "selection_inspection_path": _repo_path(SELECTION_INSPECTION_PATH),
        "selection_inspection_sha256": inspection["inspection_sha256"],
        "dates": dates,
        "dates_sha256": _hash(dates),
        "query": {
            "provider": "Massive",
            "endpoint": MASSIVE_ENDPOINT,
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "active": True,
            "point_in_time_parameter": "date",
        },
        "private_output_root": _repo_path(PRIVATE_REFERENCE_ROOT),
        "pre_freeze_snapshot_count": len(existing),
        "substitutions_allowed": False,
        "provider_switching_allowed": False,
        "implementation_binding": {
            "controller_path": _repo_path(Path(__file__)),
            "controller_sha256": sha256_file(Path(__file__)),
            "collector_path": "scanner_replay.py",
            "collector_sha256": sha256_file(PROJECT_ROOT / "scanner_replay.py"),
        },
        "market_prices_permitted": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def freeze_reference_contract() -> dict[str, Any]:
    for path in (
        Path(__file__),
        PROJECT_ROOT / "scanner_replay.py",
        SELECTION_PATH,
        SELECTION_INSPECTION_PATH,
    ):
        _require_committed(path)
    content = build_reference_contract()
    if content["pre_freeze_snapshot_count"] != 0:
        raise OversoldReplicationReferenceError(
            "reference snapshots exist before contract freeze"
        )
    path, value = _publish(
        REFERENCE_CONTRACT_ROOT,
        "oversold-replication-reference-contract",
        content,
        identity_field="contract_sha256",
    )
    return {
        "state": "REFERENCE_CONTRACT_FROZEN_AWAITING_INSPECTION",
        "path": _repo_path(path),
        "contract_sha256": value["contract_sha256"],
        "dates": len(value["dates"]),
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def inspect_reference_contract() -> dict[str, Any]:
    path = _one(REFERENCE_CONTRACT_ROOT, "*.json")
    _require_committed(path)
    supplied = _read(path)
    identity = supplied.pop("contract_sha256", None)
    expected = build_reference_contract()
    checks = {
        "content_hash": identity == _hash(supplied),
        "exact_rebuild": supplied == expected,
        "exact_dates": len(supplied["dates"]) == 135,
        "zero_snapshot_state": supplied["pre_freeze_snapshot_count"] == 0
        and not list(PRIVATE_REFERENCE_ROOT.glob("*.json.gz")),
        "reference_only": supplied["market_prices_permitted"] is False,
        "no_outcomes": supplied["target_outcomes_observed_or_derived"] is False,
        "no_broker_actions": supplied["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationReferenceError(
            "reference contract inspection failed"
        )
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_reference_contract_inspection",
        "state": "REFERENCE_CONTRACT_INSPECTED_PROVIDER_READY",
        "contract_path": _repo_path(path),
        "contract_file_sha256": sha256_file(path),
        "contract_sha256": identity,
        "checks": checks,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    _path, value = _publish(
        REFERENCE_CONTRACT_INSPECTION_ROOT,
        "oversold-replication-reference-contract-inspection",
        content,
        identity_field="inspection_sha256",
    )
    return value


def collect_reference(env_path: Path) -> dict[str, Any]:
    contract_path = _one(REFERENCE_CONTRACT_ROOT, "*.json")
    inspection_path = _one(REFERENCE_CONTRACT_INSPECTION_ROOT, "*.json")
    for path in (contract_path, inspection_path):
        _require_committed(path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        inspection.get("state") == "REFERENCE_CONTRACT_INSPECTED_PROVIDER_READY"
        and inspection.get("contract_file_sha256") == sha256_file(contract_path)
    ):
        raise OversoldReplicationReferenceError(
            "reference collection lacks inspected contract"
        )
    result = scanner_replay.collect_reference_snapshots(
        contract["dates"],
        config=scanner_replay.MassiveReferenceConfig.from_env(env_path),
        output_root=PRIVATE_REFERENCE_ROOT,
    )
    collected = sum(row["disposition"] == "collected" for row in result["snapshots"])
    cached = sum(row["disposition"] == "cached" for row in result["snapshots"])
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_reference_status",
        "state": "REFERENCE_COLLECTED_AWAITING_INSPECTION",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "dates": len(result["requested_dates"]),
        "snapshots": [
            {
                "date": row["date"],
                "rows": row["rows"],
                "sha256": row["sha256"],
            }
            for row in result["snapshots"]
        ],
        "provider_requests": collected,
        "cache_hits": cached,
        "provider_failures": 0,
        "substitutions": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    _path, value = _publish(
        REFERENCE_STATUS_ROOT,
        "oversold-replication-reference-status",
        content,
        identity_field="status_sha256",
    )
    return value


def inspect_reference_data() -> dict[str, Any]:
    contract_path = _one(REFERENCE_CONTRACT_ROOT, "*.json")
    status_path = _one(REFERENCE_STATUS_ROOT, "*.json")
    for path in (contract_path, status_path):
        _require_committed(path)
    contract = _read(contract_path)
    status = _read(status_path)
    rebuilt: list[dict[str, Any]] = []
    for day in contract["dates"]:
        path = PRIVATE_REFERENCE_ROOT / f"{day}.json.gz"
        if not path.is_file():
            raise OversoldReplicationReferenceError(
                f"reference snapshot is missing: {day}"
            )
        import gzip

        with gzip.open(path, "rt", encoding="utf-8") as stream:
            rows = json.load(stream)
        if not isinstance(rows, list) or not rows:
            raise OversoldReplicationReferenceError(
                f"reference snapshot is empty: {day}"
            )
        tickers = [str(row.get("ticker") or "") for row in rows]
        if any(not ticker for ticker in tickers) or len(tickers) != len(set(tickers)):
            raise OversoldReplicationReferenceError(
                f"reference identities are ambiguous: {day}"
            )
        rebuilt.append(
            {"date": day, "rows": len(rows), "sha256": sha256_file(path)}
        )
    checks = {
        "exact_dates": [row["date"] for row in rebuilt] == contract["dates"],
        "status_rebuilt": rebuilt == status["snapshots"],
        "all_nonempty": all(row["rows"] > 0 for row in rebuilt),
        "no_substitutions": status["substitutions"] == 0,
        "reference_only": status["market_prices_accessed"] is False,
        "no_outcomes": status["target_outcomes_observed_or_derived"] is False,
        "no_broker_actions": status["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationReferenceError("reference data inspection failed")
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_reference_data_inspection",
        "state": "REFERENCE_DATA_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "status_path": _repo_path(status_path),
        "status_sha256": status["status_sha256"],
        "dates": len(rebuilt),
        "minimum_rows": min(row["rows"] for row in rebuilt),
        "maximum_rows": max(row["rows"] for row in rebuilt),
        "snapshot_graph_sha256": _hash(rebuilt),
        "checks": checks,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    _path, value = _publish(
        REFERENCE_DATA_INSPECTION_ROOT,
        "oversold-replication-reference-data-inspection",
        content,
        identity_field="inspection_sha256",
    )
    return value


def build_master() -> dict[str, Any]:
    status_path = _one(REFERENCE_STATUS_ROOT, "*.json")
    inspection_path = _one(REFERENCE_DATA_INSPECTION_ROOT, "*.json")
    for path in (status_path, inspection_path):
        _require_committed(path)
    status = _read(status_path)
    inspection = _read(inspection_path)
    if not (
        inspection.get("state") == "REFERENCE_DATA_INSPECTED_READY"
        and inspection.get("status_sha256") == status["status_sha256"]
    ):
        raise OversoldReplicationReferenceError(
            "security master lacks inspected reference data"
        )
    manifest = scanner_replay.build_security_master(
        _selection()[0]["selected_dates"],
        snapshots_root=PRIVATE_REFERENCE_ROOT,
        output=SECURITY_MASTER,
        source_manifest=SECURITY_SOURCE,
    )
    return {
        "state": "SECURITY_MASTER_BUILT_AWAITING_INSPECTION",
        "security_master": manifest["security_master"],
        "source_path": _repo_path(SECURITY_SOURCE),
        "source_sha256": sha256_file(SECURITY_SOURCE),
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def inspect_master() -> dict[str, Any]:
    for path in (SECURITY_MASTER, SECURITY_SOURCE):
        _require_committed(path)
    selection, _inspection = _selection()
    source = _read(SECURITY_SOURCE)
    records = load_security_master(SECURITY_MASTER)
    coverage: list[dict[str, Any]] = []
    from datetime import date

    for day in selection["selected_dates"]:
        observed = date.fromisoformat(day)
        symbols = {
            str(record["symbol"])
            for record in records
            if record.get("security_type") == "COMMON"
            and security_record_covers(record, observed)
        }
        if not symbols:
            raise OversoldReplicationReferenceError(
                f"security master lacks exact-date common stocks: {day}"
            )
        coverage.append({"date": day, "symbols": len(symbols)})
    checks = {
        "source_dates_bound": source["requested_dates"]
        == selection["selected_dates"],
        "semantic_hash": source["security_master"]["sha256"]
        == security_master_sha256(SECURITY_MASTER),
        "exact_date_coverage": len(coverage) == 135,
        "all_dates_nonempty": all(row["symbols"] > 0 for row in coverage),
        "reference_only": source["source"]["endpoint"] == MASSIVE_ENDPOINT,
    }
    if not all(checks.values()):
        raise OversoldReplicationReferenceError(
            "security master inspection failed"
        )
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_security_master_inspection",
        "state": "SECURITY_MASTER_INSPECTED_READY",
        "security_master_path": _repo_path(SECURITY_MASTER),
        "security_master_file_sha256": sha256_file(SECURITY_MASTER),
        "security_master_sha256": security_master_sha256(SECURITY_MASTER),
        "source_path": _repo_path(SECURITY_SOURCE),
        "source_file_sha256": sha256_file(SECURITY_SOURCE),
        "dates": len(coverage),
        "minimum_common_stocks": min(row["symbols"] for row in coverage),
        "maximum_common_stocks": max(row["symbols"] for row in coverage),
        "coverage_sha256": _hash(coverage),
        "checks": checks,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    _path, value = _publish(
        SECURITY_INSPECTION_ROOT,
        "oversold-replication-security-master-inspection",
        content,
        identity_field="inspection_sha256",
    )
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("record-v1-failure")
    sub.add_parser("freeze-reference")
    sub.add_parser("inspect-reference-contract")
    sub.add_parser("collect-reference")
    sub.add_parser("inspect-reference-data")
    sub.add_parser("build-master")
    sub.add_parser("inspect-master")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "record-v1-failure":
            result = record_v1_failure()
        elif args.command == "freeze-reference":
            result = freeze_reference_contract()
        elif args.command == "inspect-reference-contract":
            result = inspect_reference_contract()
        elif args.command == "collect-reference":
            result = collect_reference(args.env)
        elif args.command == "inspect-reference-data":
            result = inspect_reference_data()
        elif args.command == "build-master":
            result = build_master()
        else:
            result = inspect_master()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationReferenceError,
        scanner_replay.ScannerReplayError,
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
