"""Persist and audit the production-strategy validation campaign.

This controller coordinates bounded work. It never contacts a provider, opens a
broker connection, changes production rules, or submits an order.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from historical_store import DEFAULT_ENV_PATH, HistoricalDayStore
from learning_data import audit_learning_data
from learning_registry import audit_registries, current_entities
from learning_strategy import audit_strategy_evidence, strategy_readiness
from progress_history import load_history
from sensitive_data import SensitiveDataCipher, audit_context_files
from strategy_engine import load_config
from strategy_ledger import audit_ledger, build_report, read_records
from trade_lifecycle import audit_lifecycle


PROJECT_ROOT = Path(__file__).resolve().parent
RUN_ROOT = PROJECT_ROOT / "learning_runs" / "production_validation"
PLAN_PATH = PROJECT_ROOT / "PRODUCTION_STRATEGY_VALIDATION.md"
STATE_PATH = RUN_ROOT / "state.json"
EVENTS_PATH = RUN_ROOT / "events.jsonl"
LOCK_PATH = RUN_ROOT / ".lock"
SCHEMA_VERSION = 1
CAMPAIGN_ID = "production-strategy-validation"
SAFETY_SNAPSHOT_MAX_AGE_SECONDS = 15 * 60

PHASES = (
    "SOURCE_SEMANTICS",
    "SOURCE_RECOVERY",
    "DEVELOPMENT_ACQUISITION",
    "DEVELOPMENT_OUTCOMES",
    "CHALLENGER_REVIEW",
    "CONFIRMATION",
    "SHADOW_QUALIFICATION",
    "LIVE_PILOT",
    "PROMOTION_AUDIT",
    "VALIDATED",
)
NONTERMINAL_STATUSES = {
    "READY",
    "WAITING_MARKET",
    "WAITING_USER_CONFIRMATION",
    "WAITING_PROVIDER",
    "WAITING_SUBSCRIPTION",
    "WAITING_NEW_SESSIONS",
    "PAUSED_SAFETY",
}
WAITING_STATUSES = NONTERMINAL_STATUSES - {"READY"}
CORE_ARTIFACTS = (
    "AGENTS.md",
    "PRODUCTION_STRATEGY_VALIDATION.md",
    "CATALYST_SOURCE_SEMANTICS_PLAN.md",
    "strategy_config.toml",
    "SIGNALS.jsonl",
    "learning/DATASETS.jsonl",
    "learning/EXPERIMENTS.jsonl",
    "learning/STRATEGIES.jsonl",
)
ALLOWED_PHASE_TRANSITIONS = {
    "SOURCE_SEMANTICS": {
        "SOURCE_SEMANTICS",
        "SOURCE_RECOVERY",
        "DEVELOPMENT_ACQUISITION",
    },
    "SOURCE_RECOVERY": {
        "SOURCE_RECOVERY",
        "SOURCE_SEMANTICS",
        "DEVELOPMENT_ACQUISITION",
    },
    "DEVELOPMENT_ACQUISITION": {
        "DEVELOPMENT_ACQUISITION",
        "SOURCE_SEMANTICS",
        "DEVELOPMENT_OUTCOMES",
        "CHALLENGER_REVIEW",
    },
    "DEVELOPMENT_OUTCOMES": {
        "DEVELOPMENT_OUTCOMES",
        "CHALLENGER_REVIEW",
        "CONFIRMATION",
    },
    "CHALLENGER_REVIEW": {
        "CHALLENGER_REVIEW",
        "DEVELOPMENT_ACQUISITION",
        "CONFIRMATION",
    },
    "CONFIRMATION": {"CONFIRMATION", "CHALLENGER_REVIEW", "SHADOW_QUALIFICATION"},
    "SHADOW_QUALIFICATION": {
        "SHADOW_QUALIFICATION",
        "LIVE_PILOT",
        "CHALLENGER_REVIEW",
    },
    "LIVE_PILOT": {
        "LIVE_PILOT",
        "SHADOW_QUALIFICATION",
        "CHALLENGER_REVIEW",
        "PROMOTION_AUDIT",
    },
    "PROMOTION_AUDIT": {
        "PROMOTION_AUDIT",
        "LIVE_PILOT",
        "SHADOW_QUALIFICATION",
        "CHALLENGER_REVIEW",
    },
    "VALIDATED": {"VALIDATED"},
}

PHASE_HANDOFFS: dict[str, dict[str, str]] = {
    "SOURCE_SEMANTICS": {
        "objective": "close-frozen-catalyst-source-semantics",
        "action": "Freeze or resume the exact 33-pair source-semantics dataset, derive private rows without outcomes, and independently inspect its aggregate.",
        "command": "python3 catalyst_source_semantics.py inspect",
    },
    "SOURCE_RECOVERY": {
        "objective": "recover-primary-source-capacity",
        "action": "Freeze one recovery slice in priority order: accession-bound SEC endpoints, captured transport retries, then canonical issuer document chains.",
        "command": "python3 catalyst_source_semantics.py inspect",
    },
    "DEVELOPMENT_ACQUISITION": {
        "objective": "acquire-disjoint-development-capacity",
        "action": "Freeze the next exact disjoint 100-session tranche with no substitutions, then collect from cache before whole-provider fallbacks while preserving the disk reserve.",
        "command": "python3 historical_data_cli.py check",
    },
    "DEVELOPMENT_OUTCOMES": {
        "objective": "build-frozen-development-outcomes",
        "action": "Freeze the executable fill, exit, ambiguity, and cost-stress contract before reading post-entry data, then publish only independently inspected records.",
        "command": "python3 strategy_ledger.py audit",
    },
    "CHALLENGER_REVIEW": {
        "objective": "record-falsification-or-preregister-challenger",
        "action": "Record the champion's exact capacity or alpha failure; if earned, preregister one causal challenger mechanism without touching the failed corpus.",
        "command": "python3 learning_experiment.py audit",
    },
    "CONFIRMATION": {
        "objective": "collect-untouched-confirmation",
        "action": "Freeze chronologically separated confirmation dates before outcome access and apply the exact champion contract without parameter choice.",
        "command": "python3 strategy_ledger.py audit",
    },
    "SHADOW_QUALIFICATION": {
        "objective": "collect-prospective-shadow-executions",
        "action": "During an eligible current market session, run the complete production path in shadow mode and retain every capture and operational result without orders.",
        "command": "python3 session_mode.py --mode shadow",
    },
    "LIVE_PILOT": {
        "objective": "collect-compliant-live-pilots",
        "action": "During an eligible market session, enter live mode and follow every account, evaluator, guard, review, confirmation, protection, monitoring, and journal gate in AGENTS.md.",
        "command": "python3 session_mode.py --mode live",
    },
    "PROMOTION_AUDIT": {
        "objective": "recompute-terminal-promotion",
        "action": "Reconcile broker state flat, refresh strategy axes and private safety evidence through their authoritative workflows, commit and push, then rerun the campaign audit.",
        "command": "python3 strategy_validation.py audit",
    },
    "VALIDATED": {
        "objective": "campaign-complete",
        "action": "No further validation work is required for this frozen champion; continue normal degradation monitoring.",
        "command": "python3 strategy_validation.py status",
    },
}


class StrategyValidationError(RuntimeError):
    """The persistent validation campaign is malformed or unsafe."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise StrategyValidationError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise StrategyValidationError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise StrategyValidationError(f"{path} must contain an object")
    return value


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise StrategyValidationError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise StrategyValidationError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise StrategyValidationError(f"{field} must include a timezone")
    return parsed


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _nonempty(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise StrategyValidationError(f"{field} must be non-empty")
    return value.strip()


def _artifact_hashes(root: Path) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for relative in CORE_ARTIFACTS:
        path = root / relative
        if not path.is_file():
            raise StrategyValidationError(
                f"required campaign artifact is missing: {relative}"
            )
        hashes[relative] = _sha256_file(path)
    return hashes


def _lineage(root: Path) -> dict[str, Any]:
    config = load_config(root / "strategy_config.toml")
    registry_root = root / "learning"
    strategy_audit = audit_strategy_evidence(registry_root)
    champion_id = str(strategy_audit["champion"])
    champion = current_entities("strategies", registry_root)[champion_id]["payload"]
    if champion.get("version") != config.version:
        raise StrategyValidationError(
            "registered champion version does not match production configuration"
        )
    plan = root / "PRODUCTION_STRATEGY_VALIDATION.md"
    return {
        "champion_id": champion_id,
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "plan_sha256": _sha256_file(plan),
        "upstream_artifact_hashes": _artifact_hashes(root),
    }


def _validate_safety_snapshot(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise StrategyValidationError("safety snapshot must be an object")
    snapshot = dict(value)
    allowed = {
        "schema_version",
        "observed_at",
        "broker_state",
        "account_reconciled",
        "positions_count",
        "open_orders_count",
        "unknown_orders_count",
        "source",
    }
    extras = sorted(set(snapshot) - allowed)
    if extras:
        raise StrategyValidationError(
            f"safety snapshot contains forbidden fields: {extras}"
        )
    if snapshot.get("schema_version") != 1:
        raise StrategyValidationError("safety snapshot schema_version must be 1")
    _timestamp(snapshot.get("observed_at"), "safety_snapshot.observed_at")
    if snapshot.get("broker_state") not in {"FLAT_RECONCILED", "EXPOSED", "UNKNOWN"}:
        raise StrategyValidationError("invalid safety snapshot broker_state")
    if not isinstance(snapshot.get("account_reconciled"), bool):
        raise StrategyValidationError(
            "safety snapshot account_reconciled must be boolean"
        )
    for field in ("positions_count", "open_orders_count", "unknown_orders_count"):
        value = snapshot.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise StrategyValidationError(
                f"safety snapshot {field} must be nonnegative"
            )
    snapshot["source"] = _nonempty(snapshot.get("source"), "safety_snapshot.source")
    return snapshot


def _evidence_hashes(paths: Sequence[Path], root: Path) -> dict[str, str]:
    result: dict[str, str] = {}
    for supplied in paths:
        path = supplied if supplied.is_absolute() else root / supplied
        try:
            relative = str(path.resolve().relative_to(root.resolve()))
        except ValueError as exc:
            raise StrategyValidationError(
                f"campaign evidence must be inside the repository: {path}"
            ) from exc
        if not path.is_file():
            raise StrategyValidationError(
                f"campaign evidence is not a file: {relative}"
            )
        result[relative] = _sha256_file(path)
    return dict(sorted(result.items()))


def validate_phase_transition(previous: str, current: str) -> None:
    if previous not in ALLOWED_PHASE_TRANSITIONS:
        raise StrategyValidationError(f"unknown previous phase {previous!r}")
    if current not in ALLOWED_PHASE_TRANSITIONS[previous]:
        raise StrategyValidationError(
            f"unsafe campaign phase transition {previous} -> {current}"
        )


def _event_hash(event: Mapping[str, Any]) -> str:
    payload = {key: value for key, value in event.items() if key != "event_sha256"}
    return _sha256_bytes(_canonical_bytes(payload))


def _validate_event(
    value: Any, *, previous_hash: str, expected_sequence: int
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise StrategyValidationError("campaign event must be an object")
    event = dict(value)
    if event.get("schema_version") != SCHEMA_VERSION:
        raise StrategyValidationError("campaign event schema_version must be 1")
    if event.get("campaign_id") != CAMPAIGN_ID:
        raise StrategyValidationError("campaign event has the wrong campaign_id")
    if event.get("sequence") != expected_sequence:
        raise StrategyValidationError("campaign event sequence is not contiguous")
    _timestamp(event.get("recorded_at"), "event.recorded_at")
    if event.get("previous_event_sha256") != previous_hash:
        raise StrategyValidationError("campaign event hash chain is broken")
    if event.get("event_sha256") != _event_hash(event):
        raise StrategyValidationError("campaign event content hash is invalid")
    phase = event.get("phase")
    if phase not in PHASES:
        raise StrategyValidationError(f"invalid campaign phase {phase!r}")
    status = event.get("status")
    if status not in NONTERMINAL_STATUSES:
        raise StrategyValidationError(f"invalid campaign status {status!r}")
    _nonempty(event.get("active_objective"), "event.active_objective")
    if phase != "VALIDATED":
        _nonempty(event.get("next_action"), "event.next_action")
    if status in WAITING_STATUSES:
        _nonempty(event.get("blocker"), "event.blocker")
    for field in ("champion_id", "strategy_version", "rules_hash", "plan_sha256"):
        _nonempty(event.get(field), f"event.{field}")
    artifacts = event.get("upstream_artifact_hashes")
    if not isinstance(artifacts, Mapping) or not artifacts:
        raise StrategyValidationError("campaign event lacks upstream artifact hashes")
    evidence = event.get("evidence_hashes")
    if not isinstance(evidence, Mapping):
        raise StrategyValidationError(
            "campaign event evidence_hashes must be an object"
        )
    snapshot = event.get("safety_snapshot")
    if snapshot is not None:
        event["safety_snapshot"] = _validate_safety_snapshot(snapshot)
    return event


def _load_events(events_path: Path) -> list[dict[str, Any]]:
    if not events_path.exists():
        return []
    events: list[dict[str, Any]] = []
    previous_hash = ""
    try:
        lines = events_path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise StrategyValidationError(f"cannot read campaign events: {exc}") from exc
    for index, line in enumerate(lines, 1):
        if not line.strip():
            raise StrategyValidationError(f"campaign event line {index} is blank")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise StrategyValidationError(
                f"campaign event line {index} is invalid JSON: {exc.msg}"
            ) from exc
        event = _validate_event(
            value, previous_hash=previous_hash, expected_sequence=index
        )
        if events:
            previous_phase = str(events[-1]["phase"])
            current_phase = str(event["phase"])
            if event.get("transition_kind") not in {"VERSION_RESET", "VALIDATED"}:
                validate_phase_transition(previous_phase, current_phase)
        events.append(event)
        previous_hash = str(event["event_sha256"])
    return events


def _atomic_write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as target:
            target.write(json.dumps(value, indent=2, sort_keys=True) + "\n")
            target.flush()
            os.fsync(target.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _project_state(event: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": CAMPAIGN_ID,
        "current_event_sha256": event["event_sha256"],
        "sequence": event["sequence"],
        "phase": event["phase"],
        "status": event["status"],
        "active_objective": event["active_objective"],
        "blocker": event["blocker"],
        "next_action": event["next_action"],
        "champion_id": event["champion_id"],
        "strategy_version": event["strategy_version"],
        "rules_hash": event["rules_hash"],
        "plan_sha256": event["plan_sha256"],
        "updated_at": event["recorded_at"],
    }


def _load_current(run_root: Path) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    events_path = run_root / "events.jsonl"
    state_path = run_root / "state.json"
    events = _load_events(events_path)
    if not events:
        raise StrategyValidationError(
            "campaign is not initialized; run strategy_validation.py init"
        )
    state = _read_json(state_path)
    projected = _project_state(events[-1])
    if state != projected:
        raise StrategyValidationError(
            "campaign state projection does not match event log"
        )
    return events, events[-1]


def _append_event(
    *,
    root: Path,
    run_root: Path,
    phase: str,
    status: str,
    objective: str,
    blocker: str,
    next_action: str,
    transition_kind: str,
    evidence_paths: Sequence[Path] = (),
    safety_snapshot: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if phase not in PHASES:
        raise StrategyValidationError(f"invalid campaign phase {phase!r}")
    if status not in NONTERMINAL_STATUSES:
        raise StrategyValidationError(f"invalid campaign status {status!r}")
    objective = _nonempty(objective, "active objective")
    if phase != "VALIDATED":
        next_action = _nonempty(next_action, "next action")
    if status in WAITING_STATUSES:
        blocker = _nonempty(blocker, "blocker")
    lineage = _lineage(root)
    run_root.mkdir(parents=True, exist_ok=True)
    lock_path = run_root / ".lock"
    events_path = run_root / "events.jsonl"
    state_path = run_root / "state.json"
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        events = _load_events(events_path)
        if events and transition_kind not in {"VERSION_RESET", "VALIDATED"}:
            validate_phase_transition(str(events[-1]["phase"]), phase)
        previous_hash = str(events[-1]["event_sha256"]) if events else ""
        if safety_snapshot is None and events:
            safety_snapshot = events[-1].get("safety_snapshot")
        normalized_snapshot = (
            _validate_safety_snapshot(safety_snapshot)
            if safety_snapshot is not None
            else None
        )
        event: dict[str, Any] = {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "sequence": len(events) + 1,
            "recorded_at": _now(),
            "transition_kind": transition_kind,
            "phase": phase,
            "status": status,
            "active_objective": objective,
            "blocker": blocker.strip(),
            "next_action": next_action.strip(),
            **lineage,
            "evidence_hashes": _evidence_hashes(evidence_paths, root),
            "safety_snapshot": normalized_snapshot,
            "previous_event_sha256": previous_hash,
        }
        event["event_sha256"] = _event_hash(event)
        _validate_event(
            event,
            previous_hash=previous_hash,
            expected_sequence=len(events) + 1,
        )
        encoded = json.dumps(event, sort_keys=True, separators=(",", ":")) + "\n"
        with events_path.open("a", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        _atomic_write_json(state_path, _project_state(event))
        fcntl.flock(lock.fileno(), fcntl.LOCK_UN)
    return event


def initialize_campaign(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    if (run_root / "events.jsonl").exists():
        _, current = _load_current(run_root)
        return {
            "initialized": True,
            "idempotent": True,
            "state": _project_state(current),
        }
    event = _append_event(
        root=root,
        run_root=run_root,
        phase="SOURCE_SEMANTICS",
        status="READY",
        objective="close-frozen-catalyst-source-semantics",
        blocker="",
        next_action=PHASE_HANDOFFS["SOURCE_SEMANTICS"]["action"],
        transition_kind="INITIALIZED",
    )
    return {"initialized": True, "idempotent": False, "state": _project_state(event)}


def _reset_for_lineage_change(
    *, root: Path, run_root: Path, current: Mapping[str, Any]
) -> dict[str, Any] | None:
    lineage = _lineage(root)
    changed = [
        field
        for field in ("champion_id", "strategy_version", "rules_hash")
        if current.get(field) != lineage[field]
    ]
    if not changed:
        return None
    detail = ", ".join(
        f"{field}: {current.get(field)} -> {lineage[field]}" for field in changed
    )
    return _append_event(
        root=root,
        run_root=run_root,
        phase="DEVELOPMENT_ACQUISITION",
        status="READY",
        objective="restart-development-for-current-rules",
        blocker=f"Prior maturity sample invalidated by lineage change ({detail}).",
        next_action=PHASE_HANDOFFS["DEVELOPMENT_ACQUISITION"]["action"],
        transition_kind="VERSION_RESET",
    )


def _git_state(root: Path) -> dict[str, Any]:
    def run(*args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            ["git", *args], cwd=root, check=False, capture_output=True, text=True
        )

    status_result = run("status", "--porcelain=v1", "--untracked-files=all")
    if status_result.returncode:
        return {"valid": False, "error": status_result.stderr.strip()}
    dirty_paths = sorted(
        line[3:] for line in status_result.stdout.splitlines() if len(line) >= 4
    )
    upstream = run("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{upstream}")
    if upstream.returncode:
        return {
            "valid": True,
            "clean": not dirty_paths,
            "dirty_paths": dirty_paths,
            "upstream": None,
            "ahead": None,
            "behind": None,
            "head_equals_upstream": False,
        }
    upstream_name = upstream.stdout.strip()
    counts = run("rev-list", "--left-right", "--count", f"HEAD...{upstream_name}")
    if counts.returncode:
        return {"valid": False, "error": counts.stderr.strip()}
    ahead_text, behind_text = counts.stdout.split()
    ahead = int(ahead_text)
    behind = int(behind_text)
    return {
        "valid": True,
        "clean": not dirty_paths,
        "dirty_paths": dirty_paths,
        "upstream": upstream_name,
        "ahead": ahead,
        "behind": behind,
        "head_equals_upstream": ahead == 0 and behind == 0,
    }


def _store_capacity(root: Path) -> dict[str, Any]:
    try:
        store = HistoricalDayStore.from_env(root / DEFAULT_ENV_PATH.name)
        usage = shutil.disk_usage(store.root)
        return {
            "available": True,
            "root": str(store.root),
            "free_bytes": usage.free,
            "reserve_bytes": store.min_free_bytes,
            "capacity_ready": usage.free >= store.min_free_bytes,
        }
    except Exception as exc:
        return {
            "available": False,
            "capacity_ready": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _privacy_audit(root: Path) -> dict[str, Any]:
    try:
        cipher = SensitiveDataCipher.from_env(root / ".env")
        result = audit_context_files([root / "TRADES.md", root / "trades"], cipher)
        return {
            **asdict(result),
            "valid": not result.violations,
        }
    except Exception as exc:
        return {
            "valid": False,
            "error_type": type(exc).__name__,
            "error": str(exc),
        }


def _authoritative_snapshot(root: Path) -> dict[str, Any]:
    config = load_config(root / "strategy_config.toml")
    registry_root = root / "learning"
    ledger_path = root / "SIGNALS.jsonl"
    records = read_records(ledger_path)
    ledger_audit = audit_ledger(ledger_path, config)
    ledger_report = build_report(records, config)
    registry_audit = audit_registries(registry_root)
    strategy_audit = audit_strategy_evidence(registry_root)
    entities = current_entities("strategies", registry_root)
    champion_id = str(strategy_audit["champion"])
    champion = dict(entities[champion_id]["payload"])
    maturity = str(ledger_report["maturity"]["earned_maturity"])
    champion_readiness = strategy_readiness(champion, earned_maturity=maturity)
    lifecycle = audit_lifecycle(
        active_root=root / "trades" / "active",
        archive_root=root / "trades" / "archived",
        config=config,
    )
    learning = audit_learning_data(
        registry_root=registry_root,
        security_path=registry_root / "SECURITY_MASTER.jsonl",
    )
    return {
        "lineage": _lineage(root),
        "ledger_audit": {**asdict(ledger_audit), "valid": ledger_audit.valid},
        "ledger_report": ledger_report,
        "registry_audit": registry_audit,
        "strategy_audit": strategy_audit,
        "champion": {**champion, "readiness": champion_readiness},
        "lifecycle_audit": {**asdict(lifecycle), "valid": lifecycle.valid},
        "learning_data_audit": learning,
        "privacy_audit": _privacy_audit(root),
        "progress_audit": {
            "valid": True,
            "entries": len(load_history(root / "progress" / "HISTORY.jsonl")),
        },
        "store_capacity": _store_capacity(root),
        "git": _git_state(root),
    }


def _safety_snapshot_blockers(
    snapshot: Mapping[str, Any] | None, *, now: datetime | None = None
) -> list[str]:
    if snapshot is None:
        return ["fresh reconciled-flat broker safety snapshot is missing"]
    normalized = _validate_safety_snapshot(snapshot)
    blockers: list[str] = []
    current = now or datetime.now(UTC)
    observed = _timestamp(normalized["observed_at"], "safety_snapshot.observed_at")
    age = (current - observed.astimezone(UTC)).total_seconds()
    if age < 0 or age > SAFETY_SNAPSHOT_MAX_AGE_SECONDS:
        blockers.append("broker safety snapshot is stale")
    if normalized["broker_state"] != "FLAT_RECONCILED":
        blockers.append("broker safety snapshot is not reconciled flat")
    if normalized["account_reconciled"] is not True:
        blockers.append("broker account is not reconciled")
    for field in ("positions_count", "open_orders_count", "unknown_orders_count"):
        if normalized[field] != 0:
            blockers.append(f"broker safety snapshot has nonzero {field}")
    return blockers


def _finalization_blockers(
    current: Mapping[str, Any], snapshot: Mapping[str, Any]
) -> list[str]:
    blockers: list[str] = []
    maturity = snapshot["ledger_report"]["maturity"]
    if maturity["earned_maturity"] != "VALIDATED":
        blockers.extend(str(value) for value in maturity["validated_blockers"])
    champion = snapshot["champion"]
    expected = {
        "alpha_state": "CONFIRMED",
        "execution_state": "LIVE_CALIBRATED",
        "operations_state": "READY",
        "readiness": "VALIDATED",
    }
    for field, required in expected.items():
        if champion.get(field) != required:
            blockers.append(f"champion {field} is not {required}")
    for field in (
        "ledger_audit",
        "registry_audit",
        "strategy_audit",
        "lifecycle_audit",
        "learning_data_audit",
        "privacy_audit",
        "progress_audit",
    ):
        if snapshot[field].get("valid") is not True:
            blockers.append(f"{field} is not valid")
    if snapshot["store_capacity"].get("capacity_ready") is not True:
        blockers.append("historical store capacity reserve is not ready")
    lineage = snapshot["lineage"]
    for field in ("champion_id", "strategy_version", "rules_hash", "plan_sha256"):
        if current.get(field) != lineage[field]:
            blockers.append(f"campaign {field} is stale")
    if current.get("upstream_artifact_hashes") != lineage["upstream_artifact_hashes"]:
        blockers.append("campaign upstream artifact binding is stale")
    blockers.extend(_safety_snapshot_blockers(current.get("safety_snapshot")))
    git = snapshot["git"]
    if git.get("valid") is not True:
        blockers.append("Git state is unavailable")
    else:
        if git.get("clean") is not True:
            blockers.append("Git worktree is not clean")
        if git.get("head_equals_upstream") is not True:
            blockers.append("HEAD does not equal its configured upstream")
    return list(dict.fromkeys(blockers))


def campaign_status(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    events, current = _load_current(run_root)
    authoritative = _authoritative_snapshot(root)
    lineage = authoritative["lineage"]
    drift = {
        field: {"recorded": current.get(field), "current": lineage[field]}
        for field in ("champion_id", "strategy_version", "rules_hash", "plan_sha256")
        if current.get(field) != lineage[field]
    }
    artifact_drift = (
        current.get("upstream_artifact_hashes") != lineage["upstream_artifact_hashes"]
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "campaign_id": CAMPAIGN_ID,
        "terminal": current["phase"] == "VALIDATED",
        "events": len(events),
        "state": _project_state(current),
        "lineage_drift": drift,
        "upstream_artifact_drift": artifact_drift,
        "maturity": authoritative["ledger_report"]["maturity"],
        "champion": authoritative["champion"],
        "store_capacity": authoritative["store_capacity"],
        "safety_snapshot": current.get("safety_snapshot"),
        "finalization_blockers": _finalization_blockers(current, authoritative),
    }


def record_transition(
    *,
    phase: str,
    status: str,
    objective: str,
    blocker: str,
    next_action: str,
    evidence_paths: Sequence[Path],
    safety_path: Path | None,
    root: Path = PROJECT_ROOT,
    run_root: Path = RUN_ROOT,
) -> dict[str, Any]:
    if phase == "VALIDATED":
        raise StrategyValidationError(
            "VALIDATED cannot be recorded manually; run strategy_validation.py audit"
        )
    _, current = _load_current(run_root)
    reset = _reset_for_lineage_change(root=root, run_root=run_root, current=current)
    if reset is not None:
        _, current = _load_current(run_root)
    validate_phase_transition(str(current["phase"]), phase)
    snapshot = _read_json(safety_path) if safety_path is not None else None
    event = _append_event(
        root=root,
        run_root=run_root,
        phase=phase,
        status=status,
        objective=objective,
        blocker=blocker,
        next_action=next_action,
        transition_kind="RECORDED",
        evidence_paths=evidence_paths,
        safety_snapshot=snapshot,
    )
    return {"recorded": True, "state": _project_state(event)}


def next_handoff(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    _, current = _load_current(run_root)
    reset = _reset_for_lineage_change(root=root, run_root=run_root, current=current)
    if reset is not None:
        current = reset
    phase = str(current["phase"])
    handoff = PHASE_HANDOFFS[phase]
    waiting = str(current["status"]) in WAITING_STATUSES
    return {
        "campaign_id": CAMPAIGN_ID,
        "phase": phase,
        "status": current["status"],
        "active_objective": current["active_objective"],
        "blocker": current["blocker"],
        "bounded_handoff": {
            **handoff,
            "action": current["next_action"] or handoff["action"],
            "perform_by_controller": False,
            "waiting_state_must_be_resolved_first": waiting,
        },
    }


def audit_campaign(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    events, current = _load_current(run_root)
    authoritative = _authoritative_snapshot(root)
    blockers = _finalization_blockers(current, authoritative)
    controller_valid = True
    if current["phase"] == "VALIDATED" and blockers:
        controller_valid = False
    finalized = False
    if not blockers and current["phase"] != "VALIDATED":
        event = _append_event(
            root=root,
            run_root=run_root,
            phase="VALIDATED",
            status="READY",
            objective="campaign-complete",
            blocker="",
            next_action="",
            transition_kind="VALIDATED",
            safety_snapshot=current.get("safety_snapshot"),
        )
        current = event
        events.append(event)
        finalized = True
    return {
        "valid": controller_valid,
        "campaign_id": CAMPAIGN_ID,
        "events": len(events),
        "state": _project_state(current),
        "terminal": current["phase"] == "VALIDATED",
        "finalized_now": finalized,
        "finalization_blockers": blockers,
        "integrity": {
            key: authoritative[key]
            for key in (
                "ledger_audit",
                "registry_audit",
                "strategy_audit",
                "lifecycle_audit",
                "learning_data_audit",
                "privacy_audit",
                "progress_audit",
                "store_capacity",
                "git",
            )
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="initialize or resume the persistent campaign")
    subparsers.add_parser(
        "status", help="compose current authoritative campaign status"
    )
    subparsers.add_parser("next", help="emit one bounded handoff without performing it")
    record = subparsers.add_parser(
        "record", help="append one evidence-bound transition"
    )
    record.add_argument("--phase", choices=PHASES[:-1], required=True)
    record.add_argument("--status", choices=sorted(NONTERMINAL_STATUSES), required=True)
    record.add_argument("--objective", required=True)
    record.add_argument("--blocker", default="")
    record.add_argument("--next-action", required=True)
    record.add_argument("--evidence", action="append", type=Path, default=[])
    record.add_argument(
        "--safety-snapshot",
        type=Path,
        help="privacy-safe private JSON produced from fresh broker reconciliation",
    )
    subparsers.add_parser(
        "audit", help="audit the campaign and finalize only if every gate passes"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "init":
            result = initialize_campaign()
        elif args.command == "status":
            result = campaign_status()
        elif args.command == "next":
            result = next_handoff()
        elif args.command == "record":
            result = record_transition(
                phase=args.phase,
                status=args.status,
                objective=args.objective,
                blocker=args.blocker,
                next_action=args.next_action,
                evidence_paths=args.evidence,
                safety_path=args.safety_snapshot,
            )
        else:
            result = audit_campaign()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("valid", True) else 1
    except Exception as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
