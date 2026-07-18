"""Point-in-time security identity and learning-dataset claim validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from learning_registry import REGISTRY_ROOT, current_entities


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_SECURITY_MASTER = REGISTRY_ROOT / "SECURITY_MASTER.jsonl"
SECURITY_SCHEMA_VERSION = 1
DATASET_LANES = {
    "production_scanner_replay",
    "catalyst_falsification",
    "development",
    "confirmation",
    "shadow",
    "live",
}
CLAIM_SCOPES = {
    "PRODUCTION_POLICY_REPLAY",
    "FALSIFICATION_ONLY",
    "DEVELOPMENT_ONLY",
    "EXACT_PREREGISTERED_CONTRACT_ONLY",
    "EXECUTION_QUALIFICATION_ONLY",
    "LIVE_CALIBRATION",
}
LANE_CLAIMS = {
    "production_scanner_replay": {"PRODUCTION_POLICY_REPLAY"},
    "catalyst_falsification": {"FALSIFICATION_ONLY"},
    "development": {"DEVELOPMENT_ONLY", "FALSIFICATION_ONLY"},
    "confirmation": {"EXACT_PREREGISTERED_CONTRACT_ONLY"},
    "shadow": {"EXECUTION_QUALIFICATION_ONLY"},
    "live": {"LIVE_CALIBRATION"},
}


class LearningDataError(ValueError):
    """Raised when point-in-time identity or a dataset claim is unsafe."""


def _iso_date(value: Any, field: str, *, nullable: bool = False) -> date | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise LearningDataError(f"{field} must be an ISO date")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise LearningDataError(f"{field} must be an ISO date") from exc


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise LearningDataError(f"{field} must be an ISO timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LearningDataError(f"{field} must be an ISO timestamp") from exc
    if result.tzinfo is None:
        raise LearningDataError(f"{field} must include a timezone")
    return result


def _repo_paths(value: Any, field: str, *, required: bool = True) -> list[str]:
    if not isinstance(value, list) or (required and not value):
        raise LearningDataError(f"{field} must be a non-empty path array")
    result: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise LearningDataError(f"{field} values must be non-empty strings")
        path = Path(item)
        if path.is_absolute() or ".." in path.parts:
            raise LearningDataError(f"{field} paths must be repository relative")
        result.append(item)
    return result


def validate_security_record(
    value: Any, *, line_number: int | None = None
) -> dict[str, Any]:
    prefix = f"line {line_number}: " if line_number else ""
    if not isinstance(value, Mapping):
        raise LearningDataError(f"{prefix}security record must be an object")
    record = dict(value)
    if record.get("schema_version") != SECURITY_SCHEMA_VERSION:
        raise LearningDataError(f"{prefix}security schema_version must be 1")
    for field in ("record_id", "instrument_id", "symbol", "primary_exchange"):
        if not isinstance(record.get(field), str) or not record[field].strip():
            raise LearningDataError(f"{prefix}{field} must be non-empty")
    if record["symbol"] != record["symbol"].strip().upper():
        raise LearningDataError(f"{prefix}symbol must be normalized uppercase")
    if record.get("security_type") not in {"COMMON", "ADR", "ETF", "OTHER"}:
        raise LearningDataError(f"{prefix}security_type is invalid")
    if record.get("status") not in {"ACTIVE", "RENAMED", "DELISTED", "RETIRED"}:
        raise LearningDataError(f"{prefix}status is invalid")
    valid_from = _iso_date(record.get("valid_from"), f"{prefix}valid_from")
    valid_to = _iso_date(record.get("valid_to"), f"{prefix}valid_to", nullable=True)
    if valid_to is not None and valid_from is not None and valid_to < valid_from:
        raise LearningDataError(f"{prefix}valid_to precedes valid_from")
    _timestamp(record.get("recorded_at"), f"{prefix}recorded_at")
    _repo_paths(record.get("provenance_paths"), f"{prefix}provenance_paths")
    return record


def load_security_master(path: Path = DEFAULT_SECURITY_MASTER) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    record_ids: set[str] = set()
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            raise LearningDataError(f"{path}: line {number} is blank")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise LearningDataError(f"{path}: line {number}: invalid JSON") from exc
        record = validate_security_record(value, line_number=number)
        if record["record_id"] in record_ids:
            raise LearningDataError(
                f"{path}: duplicate record_id {record['record_id']}"
            )
        record_ids.add(str(record["record_id"]))
        records.append(record)
    _validate_security_intervals(records)
    return records


def _intervals_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_start = date.fromisoformat(str(left["valid_from"]))
    right_start = date.fromisoformat(str(right["valid_from"]))
    left_end = (
        date.fromisoformat(str(left["valid_to"])) if left.get("valid_to") else date.max
    )
    right_end = (
        date.fromisoformat(str(right["valid_to"]))
        if right.get("valid_to")
        else date.max
    )
    return left_start <= right_end and right_start <= left_end


def _validate_security_intervals(records: Sequence[Mapping[str, Any]]) -> None:
    for index, left in enumerate(records):
        for right in records[index + 1 :]:
            same_instrument = left["instrument_id"] == right["instrument_id"]
            same_listing = (
                left["symbol"] == right["symbol"]
                and left["primary_exchange"] == right["primary_exchange"]
            )
            if (same_instrument or same_listing) and _intervals_overlap(left, right):
                raise LearningDataError(
                    "security-master intervals overlap for an instrument or listing"
                )


def append_security_record(
    value: Mapping[str, Any], path: Path = DEFAULT_SECURITY_MASTER
) -> None:
    record = validate_security_record(value)
    records = load_security_master(path)
    _validate_security_intervals([*records, record])
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(descriptor, line.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def security_master_sha256(path: Path = DEFAULT_SECURITY_MASTER) -> str:
    records = load_security_master(path)
    encoded = json.dumps(records, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def resolve_security(
    symbol: str,
    as_of: date,
    *,
    primary_exchange: str | None = None,
    path: Path = DEFAULT_SECURITY_MASTER,
) -> dict[str, Any]:
    normalized = symbol.strip().upper()
    matches: list[dict[str, Any]] = []
    for record in load_security_master(path):
        if record["symbol"] != normalized:
            continue
        if primary_exchange and record["primary_exchange"] != primary_exchange:
            continue
        start = date.fromisoformat(record["valid_from"])
        end = (
            date.fromisoformat(record["valid_to"])
            if record.get("valid_to")
            else date.max
        )
        if start <= as_of <= end:
            matches.append(record)
    if len(matches) != 1:
        raise LearningDataError(
            f"{symbol} does not resolve to exactly one point-in-time instrument on {as_of}"
        )
    return matches[0]


def validate_dataset_payload(
    entity_id: str, payload: Mapping[str, Any]
) -> dict[str, Any]:
    lane = payload.get("lane")
    if lane not in DATASET_LANES:
        raise LearningDataError(f"{entity_id}: unsupported dataset lane {lane!r}")
    claim = payload.get("claim_scope")
    if claim not in CLAIM_SCOPES or claim not in LANE_CLAIMS[str(lane)]:
        raise LearningDataError(
            f"{entity_id}: claim_scope is incompatible with lane {lane}"
        )
    _repo_paths(payload.get("evidence_paths"), f"{entity_id}.evidence_paths")
    if not isinstance(payload.get("inspected"), bool):
        raise LearningDataError(f"{entity_id}: inspected must be boolean")
    if lane == "production_scanner_replay":
        universe = payload.get("universe_contract")
        if not isinstance(universe, Mapping):
            raise LearningDataError(
                f"{entity_id}: scanner replay needs universe_contract"
            )
        required = {
            "selection_time_et": "09:35:00",
            "information_cutoff": "TARGET_SESSION_09:35_ET",
            "selection_is_dynamic": True,
            "complete_universe": True,
        }
        for field, expected in required.items():
            if universe.get(field) != expected:
                raise LearningDataError(
                    f"{entity_id}: universe_contract.{field} must be {expected!r}"
                )
        for field in ("scanner_rules_sha256", "security_master_sha256"):
            value = universe.get(field)
            if not isinstance(value, str) or len(value) != 64:
                raise LearningDataError(f"{entity_id}: {field} must be a SHA-256")
    elif lane == "catalyst_falsification":
        if payload.get("point_in_time_evidence") is not True:
            raise LearningDataError(
                f"{entity_id}: catalyst falsification needs point-in-time evidence"
            )
    elif lane == "confirmation":
        manifest_hash = payload.get("preregistration_sha256")
        if not isinstance(manifest_hash, str) or len(manifest_hash) != 64:
            raise LearningDataError(
                f"{entity_id}: confirmation needs preregistration_sha256"
            )
        _timestamp(payload.get("preregistered_at"), f"{entity_id}.preregistered_at")
        if payload.get("capture_after_preregistration_attested") is not True:
            raise LearningDataError(
                f"{entity_id}: confirmation must attest post-registration capture"
            )
    elif lane == "shadow" and payload.get("broker_actions_allowed") is not False:
        raise LearningDataError(
            f"{entity_id}: shadow evidence must forbid broker actions"
        )
    return dict(payload)


def audit_dataset_claims(
    root: Path = REGISTRY_ROOT,
    *,
    security_path: Path = DEFAULT_SECURITY_MASTER,
) -> dict[str, Any]:
    entities = current_entities("datasets", root)
    lane_counts: dict[str, int] = {}
    for entity_id, event in entities.items():
        payload = validate_dataset_payload(entity_id, event["payload"])
        lane = str(payload["lane"])
        if lane == "production_scanner_replay":
            recorded = payload["universe_contract"]["security_master_sha256"]
            if recorded != security_master_sha256(security_path):
                raise LearningDataError(
                    f"{entity_id}: production replay security-master hash is stale"
                )
        lane_counts[lane] = lane_counts.get(lane, 0) + 1
    return {
        "valid": True,
        "datasets": len(entities),
        "lane_counts": dict(sorted(lane_counts.items())),
    }


def audit_learning_data(
    *,
    registry_root: Path = REGISTRY_ROOT,
    security_path: Path = DEFAULT_SECURITY_MASTER,
) -> dict[str, Any]:
    records = load_security_master(security_path)
    return {
        "valid": True,
        "datasets": audit_dataset_claims(registry_root, security_path=security_path),
        "security_master": {
            "records": len(records),
            "instruments": len({item["instrument_id"] for item in records}),
            "sha256": security_master_sha256(security_path),
        },
    }


def validate_dataset_contract(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise LearningDataError("dataset contract must be an object")
    contract = dict(value)
    if contract.get("schema_version") != 1:
        raise LearningDataError("dataset contract schema_version must be 1")
    dataset_id = contract.get("dataset_id")
    if not isinstance(dataset_id, str) or not dataset_id.startswith("dataset-"):
        raise LearningDataError("dataset_id must start with dataset-")
    _timestamp(contract.get("registered_at"), "registered_at")
    requested = contract.get("requested_dates")
    if not isinstance(requested, list) or not requested:
        raise LearningDataError("requested_dates must be a non-empty array")
    parsed = [_iso_date(item, "requested_dates") for item in requested]
    if len(set(requested)) != len(requested) or parsed != sorted(parsed):
        raise LearningDataError("requested_dates must be unique and chronological")
    payload = contract.get("dataset_payload")
    if not isinstance(payload, Mapping):
        raise LearningDataError("dataset_payload must be an object")
    validate_dataset_payload(dataset_id, payload)
    forbidden = {"outcomes", "returns", "selected_symbols_by_date"}.intersection(
        contract
    )
    if forbidden:
        raise LearningDataError(
            f"frozen dataset contract contains target-session results: {sorted(forbidden)}"
        )
    return contract


def freeze_dataset_contract(
    value: Mapping[str, Any], output_root: Path
) -> tuple[Path, dict[str, Any]]:
    contract = validate_dataset_contract(value)
    content = dict(contract)
    content.pop("manifest_sha256", None)
    fingerprint = hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    frozen = {**content, "manifest_sha256": fingerprint}
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / f"{content['dataset_id']}-{fingerprint}.json"
    rendered = json.dumps(frozen, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise LearningDataError("hash-addressed dataset contract has other content")
    else:
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
    return path, frozen


def load_frozen_dataset_contract(path: Path) -> dict[str, Any]:
    contract = _read_object(path)
    recorded = contract.get("manifest_sha256")
    content = dict(contract)
    content.pop("manifest_sha256", None)
    expected = hashlib.sha256(
        json.dumps(content, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    if (
        recorded != expected
        or path.name != f"{content.get('dataset_id')}-{expected}.json"
    ):
        raise LearningDataError("frozen dataset contract was mutated or renamed")
    validate_dataset_contract(content)
    return contract


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearningDataError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LearningDataError("input must contain an object")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("audit", help="audit dataset claims and security identity")
    append = subparsers.add_parser(
        "append-security", help="append one security interval"
    )
    append.add_argument("input", type=Path)
    resolve = subparsers.add_parser("resolve", help="resolve a historical listing")
    resolve.add_argument("symbol")
    resolve.add_argument("--as-of", type=date.fromisoformat, required=True)
    resolve.add_argument("--exchange")
    freeze = subparsers.add_parser(
        "freeze-dataset", help="freeze a hash-addressed dataset contract"
    )
    freeze.add_argument("input", type=Path)
    freeze.add_argument(
        "--output-root",
        type=Path,
        default=PROJECT_ROOT / "learning_runs" / "dataset_manifests",
    )
    validate = subparsers.add_parser(
        "validate-dataset", help="validate a frozen dataset contract"
    )
    validate.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "audit":
            result: Any = audit_learning_data()
        elif args.command == "append-security":
            append_security_record(_read_object(args.input))
            result = {"appended": str(args.input), "sha256": security_master_sha256()}
        elif args.command == "resolve":
            result = resolve_security(
                args.symbol, args.as_of, primary_exchange=args.exchange
            )
        elif args.command == "freeze-dataset":
            path, manifest = freeze_dataset_contract(
                _read_object(args.input), args.output_root
            )
            result = {
                "path": str(path),
                "manifest_sha256": manifest["manifest_sha256"],
            }
        else:
            result = load_frozen_dataset_contract(args.manifest)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (LearningDataError, OSError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
