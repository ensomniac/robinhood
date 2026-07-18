"""Persistent bounded cadence for deterministic learning checks and agent handoffs."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from learning_data import audit_learning_data
from learning_experiment import (
    MAX_NEW_HYPOTHESES_PER_ISO_WEEK,
    audit_experiment_program,
    weekly_hypothesis_count,
)
from learning_registry import audit_registries, current_entities
from learning_strategy import audit_strategy_evidence, build_strategy_evidence_report
from progress_history import load_history
from sensitive_data import audit_context_files, get_cipher
from strategy_ledger import audit_ledger
from trade_lifecycle import audit_lifecycle


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_STATE_PATH = PROJECT_ROOT / "learning_runs" / "cadence-state.json"
STATE_SCHEMA_VERSION = 1


@dataclass(frozen=True)
class CadenceTask:
    key: str
    interval: timedelta
    deterministic: bool
    description: str


TASKS = (
    CadenceTask(
        "daily_integrity",
        timedelta(days=1),
        True,
        "Audit registries, evidence claims, strategy axes, ledgers, lifecycle, privacy, and progress.",
    ),
    CadenceTask(
        "nightly_frozen_collection",
        timedelta(days=1),
        False,
        "Resume only already-frozen incomplete dataset collection; never substitute dates or symbols.",
    ),
    CadenceTask(
        "weekly_hypothesis_review",
        timedelta(days=7),
        False,
        "Invent or delete at most the remaining weekly mechanism budget and preregister before evaluation.",
    ),
    CadenceTask(
        "monthly_confirmation_review",
        timedelta(days=30),
        True,
        "Queue only experiments already marked CONFIRMATION_QUEUED or record a no-op.",
    ),
    CadenceTask(
        "quarterly_execution_calibration",
        timedelta(days=90),
        True,
        "Reconcile strategy readiness and execution evidence without changing production rules.",
    ),
)
TASK_BY_KEY = {task.key: task for task in TASKS}


class LearningCadenceError(RuntimeError):
    """Raised when cadence state or a deterministic audit is unsafe."""


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise LearningCadenceError(f"{field} must be an ISO timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LearningCadenceError(f"{field} must be an ISO timestamp") from exc
    if result.tzinfo is None:
        raise LearningCadenceError(f"{field} must include a timezone")
    return result


def _new_state() -> dict[str, Any]:
    return {
        "schema_version": STATE_SCHEMA_VERSION,
        "automatic_strategy_application": False,
        "broker_actions_allowed": False,
        "provider_actions_allowed": False,
        "completions": {},
    }


def load_state(path: Path = DEFAULT_STATE_PATH) -> dict[str, Any]:
    if not path.exists():
        return _new_state()
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearningCadenceError(f"cannot read cadence state {path}: {exc}") from exc
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != STATE_SCHEMA_VERSION
    ):
        raise LearningCadenceError("cadence state schema is invalid")
    if value.get("automatic_strategy_application") is not False:
        raise LearningCadenceError(
            "cadence state cannot authorize strategy application"
        )
    if (
        value.get("broker_actions_allowed") is not False
        or value.get("provider_actions_allowed") is not False
    ):
        raise LearningCadenceError(
            "cadence state cannot authorize broker or provider actions"
        )
    completions = value.get("completions")
    if not isinstance(completions, Mapping):
        raise LearningCadenceError("cadence completions must be an object")
    for key, completion in completions.items():
        if key not in TASK_BY_KEY or not isinstance(completion, Mapping):
            raise LearningCadenceError("cadence completion references an unknown task")
        _timestamp(completion.get("completed_at"), f"{key}.completed_at")
        if completion.get("outcome") not in {"completed", "no_op", "blocked"}:
            raise LearningCadenceError(f"{key}.outcome is invalid")
        evidence_paths = completion.get("evidence_paths", [])
        if not isinstance(evidence_paths, list) or any(
            not isinstance(item, str)
            or Path(item).is_absolute()
            or ".." in Path(item).parts
            for item in evidence_paths
        ):
            raise LearningCadenceError(f"{key}.evidence_paths are invalid")
    return value


def _write_state(path: Path, value: Mapping[str, Any]) -> None:
    resolved = path.resolve()
    allowed = (PROJECT_ROOT / "learning_runs").resolve()
    if allowed not in resolved.parents:
        raise LearningCadenceError("cadence state must remain under learning_runs/")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = resolved.with_suffix(resolved.suffix + ".tmp")
    descriptor = os.open(temporary, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, 0o644)
    try:
        os.write(descriptor, rendered.encode("utf-8"))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    temporary.replace(resolved)


def cadence_status(
    *, state_path: Path = DEFAULT_STATE_PATH, as_of: datetime | None = None
) -> dict[str, Any]:
    state = load_state(state_path)
    now = as_of or datetime.now(UTC)
    if now.tzinfo is None:
        raise LearningCadenceError("as_of must include a timezone")
    tasks: list[dict[str, Any]] = []
    for task in TASKS:
        completion = state["completions"].get(task.key)
        last = (
            _timestamp(completion["completed_at"], "completed_at")
            if completion
            else None
        )
        due_at = last + task.interval if last else None
        due = last is None or now >= due_at
        tasks.append(
            {
                "task": task.key,
                "description": task.description,
                "deterministic": task.deterministic,
                "due": due,
                "last_completed_at": last.isoformat() if last else None,
                "next_due_at": due_at.isoformat() if due_at else "NOW",
            }
        )
    return {
        "as_of": now.isoformat(),
        "due_tasks": sum(item["due"] for item in tasks),
        "tasks": tasks,
        "automatic_strategy_application": False,
        "broker_actions_allowed": False,
        "provider_actions_allowed": False,
    }


def complete_task(
    task: str,
    outcome: str,
    *,
    evidence_paths: Sequence[str] = (),
    state_path: Path = DEFAULT_STATE_PATH,
    completed_at: datetime | None = None,
) -> dict[str, Any]:
    if task not in TASK_BY_KEY:
        raise LearningCadenceError(f"unknown cadence task {task!r}")
    if outcome not in {"completed", "no_op", "blocked"}:
        raise LearningCadenceError("outcome must be completed, no_op, or blocked")
    for item in evidence_paths:
        path = Path(item)
        if path.is_absolute() or ".." in path.parts:
            raise LearningCadenceError("evidence paths must be repository relative")
    state = load_state(state_path)
    instant = completed_at or datetime.now(UTC)
    if instant.tzinfo is None:
        raise LearningCadenceError("completed_at must include a timezone")
    state["completions"][task] = {
        "completed_at": instant.isoformat(),
        "outcome": outcome,
        "evidence_paths": list(evidence_paths),
    }
    _write_state(state_path, state)
    return state


def _run_integrity() -> dict[str, Any]:
    ledger = audit_ledger()
    lifecycle = audit_lifecycle()
    privacy = audit_context_files(
        [PROJECT_ROOT / "TRADES.md", PROJECT_ROOT / "trades"], get_cipher()
    )
    if not ledger.valid or not lifecycle.valid or privacy.violations:
        raise LearningCadenceError("daily repository integrity audit failed")
    return {
        "registries": audit_registries(),
        "learning_data": audit_learning_data(),
        "experiments": audit_experiment_program(),
        "strategies": audit_strategy_evidence(),
        "ledger_records": ledger.records,
        "archived_contexts": lifecycle.archived_contexts,
        "privacy_checked_files": privacy.checked_files,
        "progress_entries": len(load_history()),
    }


def _run_monthly_confirmation_review() -> dict[str, Any]:
    queued = [
        entity_id
        for entity_id, event in current_entities("experiments").items()
        if event["payload"]["status"] == "CONFIRMATION_QUEUED"
    ]
    return {
        "queued_experiments": sorted(queued),
        "outcome": "blocked" if queued else "no_op",
        "next_action": (
            "freeze a new independent dataset for each queued experiment"
            if queued
            else "no experiment has earned independent confirmation"
        ),
    }


def _run_quarterly_calibration() -> dict[str, Any]:
    report = build_strategy_evidence_report()
    return {
        "production_maturity": report["production_maturity"],
        "champion": report["champion"],
        "strategy_readiness": {
            key: value["readiness"] for key, value in report["strategies"].items()
        },
    }


def run_due_tasks(
    *,
    max_tasks: int,
    state_path: Path = DEFAULT_STATE_PATH,
    as_of: datetime | None = None,
) -> dict[str, Any]:
    if isinstance(max_tasks, bool) or not 1 <= max_tasks <= len(TASKS):
        raise LearningCadenceError(f"max_tasks must be between 1 and {len(TASKS)}")
    now = as_of or datetime.now(UTC)
    status = cadence_status(state_path=state_path, as_of=now)
    results: list[dict[str, Any]] = []
    for item in (value for value in status["tasks"] if value["due"]):
        if len(results) >= max_tasks:
            break
        key = item["task"]
        if key == "daily_integrity":
            evidence = _run_integrity()
            complete_task(key, "completed", state_path=state_path, completed_at=now)
            results.append({"task": key, "status": "completed", "evidence": evidence})
        elif key == "nightly_frozen_collection":
            pending = [
                entity_id
                for entity_id, event in current_entities("datasets").items()
                if event["payload"]["status"] in {"FROZEN", "COLLECTING"}
            ]
            if pending:
                results.append(
                    {
                        "task": key,
                        "status": "needs_agent",
                        "pending_datasets": sorted(pending),
                        "next_action": "resume exact frozen collection without substitution",
                    }
                )
            else:
                complete_task(key, "no_op", state_path=state_path, completed_at=now)
                results.append({"task": key, "status": "no_op", "pending_datasets": []})
        elif key == "weekly_hypothesis_review":
            used = weekly_hypothesis_count(now)
            remaining = max(0, MAX_NEW_HYPOTHESES_PER_ISO_WEEK - used)
            if remaining:
                results.append(
                    {
                        "task": key,
                        "status": "needs_agent",
                        "remaining_hypothesis_budget": remaining,
                        "next_action": "review current evidence and register only distinct mechanisms that can be falsified",
                    }
                )
            else:
                complete_task(key, "no_op", state_path=state_path, completed_at=now)
                results.append({"task": key, "status": "budget_exhausted"})
        elif key == "monthly_confirmation_review":
            evidence = _run_monthly_confirmation_review()
            if evidence["outcome"] == "no_op":
                complete_task(key, "no_op", state_path=state_path, completed_at=now)
                results.append({"task": key, "status": "no_op", "evidence": evidence})
            else:
                results.append(
                    {"task": key, "status": "needs_agent", "evidence": evidence}
                )
        else:
            evidence = _run_quarterly_calibration()
            complete_task(key, "completed", state_path=state_path, completed_at=now)
            results.append({"task": key, "status": "completed", "evidence": evidence})
    return {
        "as_of": now.isoformat(),
        "results": results,
        "remaining": cadence_status(state_path=state_path, as_of=now),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, default=DEFAULT_STATE_PATH)
    parser.add_argument("--as-of", type=datetime.fromisoformat)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status", help="show due bounded learning tasks")
    run = subparsers.add_parser(
        "run", help="run safe deterministic tasks and surface handoffs"
    )
    run.add_argument("--max-tasks", type=int, default=len(TASKS))
    complete = subparsers.add_parser("complete", help="record a reviewed task outcome")
    complete.add_argument("--task", choices=tuple(TASK_BY_KEY), required=True)
    complete.add_argument(
        "--outcome", choices=("completed", "no_op", "blocked"), required=True
    )
    complete.add_argument("--evidence-path", action="append", default=[])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "status":
            result = cadence_status(state_path=args.state, as_of=args.as_of)
        elif args.command == "run":
            result = run_due_tasks(
                max_tasks=args.max_tasks, state_path=args.state, as_of=args.as_of
            )
        else:
            result = complete_task(
                args.task,
                args.outcome,
                evidence_paths=args.evidence_path,
                state_path=args.state,
                completed_at=args.as_of,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (LearningCadenceError, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
