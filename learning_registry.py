"""Append-only public registries for datasets, experiments, and strategies."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
REGISTRY_ROOT = PROJECT_ROOT / "learning"
SCHEMA_VERSION = 1
ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")


@dataclass(frozen=True)
class RegistrySpec:
    name: str
    filename: str
    required_payload_fields: tuple[str, ...]


REGISTRIES = {
    "datasets": RegistrySpec(
        "datasets",
        "DATASETS.jsonl",
        ("lane", "status", "evidence_paths", "inspected"),
    ),
    "experiments": RegistrySpec(
        "experiments",
        "EXPERIMENTS.jsonl",
        ("family_id", "status", "hypothesis", "result_paths", "disposition"),
    ),
    "strategies": RegistrySpec(
        "strategies",
        "STRATEGIES.jsonl",
        (
            "version",
            "alpha_state",
            "execution_state",
            "operations_state",
            "production_role",
        ),
    ),
}


class RegistryError(RuntimeError):
    """Raised when a public learning registry violates its contract."""


def registry_path(name: str, root: Path = REGISTRY_ROOT) -> Path:
    try:
        return root / REGISTRIES[name].filename
    except KeyError as exc:
        raise RegistryError(f"unknown registry {name!r}") from exc


def _timestamp(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise RegistryError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RegistryError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise RegistryError(f"{field} must include a timezone")
    return value


def _identifier(value: Any, field: str) -> str:
    if not isinstance(value, str) or not ID_PATTERN.fullmatch(value):
        raise RegistryError(
            f"{field} must contain 3-128 lowercase identifier characters"
        )
    return value


def _string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise RegistryError(f"{field} must be an array of non-empty strings")
    return list(value)


def _validate_repo_paths(value: Any, field: str) -> list[str]:
    result = _string_list(value, field)
    for item in result:
        path = Path(item)
        if path.is_absolute() or ".." in path.parts:
            raise RegistryError(f"{field} paths must be repository relative")
    return result


def validate_event(
    value: Any, name: str, *, line_number: int | None = None
) -> dict[str, Any]:
    prefix = f"line {line_number}: " if line_number is not None else ""
    if not isinstance(value, Mapping):
        raise RegistryError(f"{prefix}event must be an object")
    event = dict(value)
    if event.get("schema_version") != SCHEMA_VERSION:
        raise RegistryError(f"{prefix}schema_version must be {SCHEMA_VERSION}")
    _identifier(event.get("event_id"), f"{prefix}event_id")
    _identifier(event.get("entity_id"), f"{prefix}entity_id")
    _timestamp(event.get("recorded_at"), f"{prefix}recorded_at")
    event_type = _identifier(event.get("event_type"), f"{prefix}event_type")
    if event_type not in {"registered", "status", "corrected", "retired"}:
        raise RegistryError(f"{prefix}unsupported event_type {event_type!r}")
    payload = event.get("payload")
    if not isinstance(payload, Mapping):
        raise RegistryError(f"{prefix}payload must be an object")
    spec = REGISTRIES.get(name)
    if spec is None:
        raise RegistryError(f"unknown registry {name!r}")
    missing = [field for field in spec.required_payload_fields if field not in payload]
    if missing:
        raise RegistryError(f"{prefix}payload is missing required fields {missing}")
    if name == "datasets":
        _validate_repo_paths(payload["evidence_paths"], f"{prefix}evidence_paths")
        if not isinstance(payload["inspected"], bool):
            raise RegistryError(f"{prefix}inspected must be boolean")
    elif name == "experiments":
        _identifier(payload["family_id"], f"{prefix}family_id")
        _validate_repo_paths(payload["result_paths"], f"{prefix}result_paths")
        if (
            not isinstance(payload["hypothesis"], str)
            or not payload["hypothesis"].strip()
        ):
            raise RegistryError(f"{prefix}hypothesis must be non-empty")
    else:
        if payload["alpha_state"] not in {
            "UNTESTED",
            "DEVELOPMENT",
            "CONFIRMED",
            "DEGRADED",
            "RETIRED",
        }:
            raise RegistryError(f"{prefix}invalid alpha_state")
        if payload["execution_state"] not in {
            "UNVERIFIED",
            "BAR_ONLY",
            "SHADOW_VERIFIED",
            "LIVE_CALIBRATED",
        }:
            raise RegistryError(f"{prefix}invalid execution_state")
        if payload["operations_state"] not in {"READY", "PAUSED", "KILL_SWITCH"}:
            raise RegistryError(f"{prefix}invalid operations_state")
    return event


def load_registry(name: str, root: Path = REGISTRY_ROOT) -> list[dict[str, Any]]:
    path = registry_path(name, root)
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    event_ids: set[str] = set()
    registered_entities: set[str] = set()
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            raise RegistryError(f"{path}: line {number} is blank")
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RegistryError(
                f"{path}: line {number}: invalid JSON: {exc.msg}"
            ) from exc
        event = validate_event(value, name, line_number=number)
        event_id = str(event["event_id"])
        entity_id = str(event["entity_id"])
        if event_id in event_ids:
            raise RegistryError(f"{path}: duplicate event_id {event_id}")
        if event["event_type"] == "registered":
            if entity_id in registered_entities:
                raise RegistryError(f"{path}: entity {entity_id} registered twice")
            registered_entities.add(entity_id)
        elif entity_id not in registered_entities:
            raise RegistryError(f"{path}: {entity_id} changed before registration")
        event_ids.add(event_id)
        events.append(event)
    return events


def current_entities(
    name: str, root: Path = REGISTRY_ROOT
) -> dict[str, dict[str, Any]]:
    current: dict[str, dict[str, Any]] = {}
    for event in load_registry(name, root):
        current[str(event["entity_id"])] = event
    return current


def append_event(
    name: str, event: Mapping[str, Any], root: Path = REGISTRY_ROOT
) -> None:
    normalized = validate_event(event, name)
    existing = load_registry(name, root)
    if any(item["event_id"] == normalized["event_id"] for item in existing):
        raise RegistryError(f"duplicate event_id {normalized['event_id']}")
    entity_exists = any(
        item["entity_id"] == normalized["entity_id"] for item in existing
    )
    if normalized["event_type"] == "registered" and entity_exists:
        raise RegistryError(f"entity {normalized['entity_id']} is already registered")
    if normalized["event_type"] != "registered" and not entity_exists:
        raise RegistryError(f"entity {normalized['entity_id']} is not registered")
    path = registry_path(name, root)
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(normalized, sort_keys=True, separators=(",", ":")) + "\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(descriptor, line.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def registry_fingerprint(name: str, root: Path = REGISTRY_ROOT) -> str:
    events = load_registry(name, root)
    rendered = json.dumps(events, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(rendered).hexdigest()


def audit_registries(root: Path = REGISTRY_ROOT) -> dict[str, Any]:
    result: dict[str, Any] = {"valid": True, "schema_version": SCHEMA_VERSION}
    for name in REGISTRIES:
        events = load_registry(name, root)
        result[name] = {
            "events": len(events),
            "entities": len({event["entity_id"] for event in events}),
            "sha256": registry_fingerprint(name, root),
        }
    return result


def _read_event(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RegistryError(f"cannot read event {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RegistryError("event input must contain an object")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("audit", help="validate every public learning registry")
    status = subparsers.add_parser("status", help="show current registry entities")
    status.add_argument("--registry", choices=tuple(REGISTRIES))
    append = subparsers.add_parser("append", help="append one validated event")
    append.add_argument("--registry", choices=tuple(REGISTRIES), required=True)
    append.add_argument("--input", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "audit":
            result: Any = audit_registries()
        elif args.command == "status":
            names = (args.registry,) if args.registry else tuple(REGISTRIES)
            result = {name: current_entities(name) for name in names}
        else:
            append_event(args.registry, _read_event(args.input))
            result = {"appended": str(args.input), "registry": args.registry}
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (RegistryError, OSError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
