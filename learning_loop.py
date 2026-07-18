"""Inspect and bound the repository's edit/learning improvement loop.

The controller does not edit source, invoke a model, contact a provider, access
the broker, or apply strategy changes. It loads the versioned public prompt,
captures the existing worktree, classifies measured learning latency, and
validates one-slice change plans before Codex applies them under ``AGENTS.md``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from contextlib import contextmanager
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from learning_data import audit_learning_data
from learning_experiment import audit_experiment_program
from learning_registry import (
    REGISTRIES,
    RegistryError,
    audit_registries,
    current_entities,
    registry_fingerprint,
)
from learning_strategy import audit_strategy_evidence


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PROMPT_PATH = PROJECT_ROOT / "LEARNING_LOOP.md"
DEFAULT_BATCH_STATUS = (
    PROJECT_ROOT / "historical_batches" / "2026-07-16-one-hundred-days-speed-test.json"
)
DEFAULT_EVIDENCE = (
    PROJECT_ROOT / "historical_batches" / "evidence-2026-07-16-one-hundred-days.json"
)
DEFAULT_RESEARCH_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-16-one-hundred-days-multi-strategy.json"
)
PROMPT_VERSION_PATTERN = re.compile(r"^Prompt version: `([^`]+)`$", re.MULTILINE)
PHASES = (
    "Phase 0 - Safety Gate",
    "Phase 1 - Evidence And Strengths",
    "Phase 2 - Bottleneck Proof",
    "Phase 3 - Improvement Candidates",
    "Phase 4 - Adversarial Filter",
    "Phase 5 - Apply Once",
    "Phase 6 - Validate And Benchmark",
    "Phase 7 - Record And Stop",
)
ALLOWED_CHANGE_KINDS = {
    "engineering",
    "test",
    "documentation",
    "strategy_proposal",
    "generated",
}
PROTECTED_PATHS = {"strategy_config.toml", ".env"}
RUN_ROOT = PROJECT_ROOT / "learning_runs"
PROGRAM_PATH = PROJECT_ROOT / "LEARNING_PROGRAM.md"
RUN_SCHEMA_VERSION = 2
RUN_ID_PATTERN = re.compile(r"^learning-[0-9]{8}t[0-9]{6}z-[a-z0-9][a-z0-9._-]{2,63}$")
OBJECTIVE_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{2,127}$")
RUN_STATUSES = {
    "CREATED",
    "INVENTORIED",
    "HYPOTHESIS_REGISTERED",
    "PREREGISTERED",
    "DATA_READY",
    "EVALUATED",
    "ADVERSARIALLY_REVIEWED",
    "REJECTED",
    "CONFIRMATION_QUEUED",
    "SHADOW_QUEUED",
    "CLOSED",
}
PROTECTED_PRODUCTION_PATHS = (
    Path("strategy_config.toml"),
    Path("SIGNALS.jsonl"),
    Path("TRADES.md"),
    Path("trades"),
)


class LearningLoopError(RuntimeError):
    """Raised when learning mode cannot preserve its bounded safety contract."""


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearningLoopError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LearningLoopError(f"{path} must contain a JSON object")
    return value


def load_prompt(path: Path = DEFAULT_PROMPT_PATH) -> dict[str, Any]:
    try:
        raw = path.read_bytes()
    except OSError as exc:
        raise LearningLoopError(f"cannot read learning prompt {path}: {exc}") from exc
    text = raw.decode("utf-8")
    match = PROMPT_VERSION_PATTERN.search(text)
    if match is None:
        raise LearningLoopError("learning prompt needs an explicit Prompt version")
    positions = []
    for phase in PHASES:
        marker = f"## {phase}"
        position = text.find(marker)
        if position < 0:
            raise LearningLoopError(f"learning prompt is missing {marker}")
        positions.append(position)
    if positions != sorted(positions):
        raise LearningLoopError("learning prompt phases are out of order")
    required_boundaries = (
        "broker_actions_allowed=false",
        "Do not edit `strategy_config.toml`",
        "at most one coherent",
        "Do not recursively launch another learning loop",
    )
    missing = [value for value in required_boundaries if value not in text]
    if missing:
        raise LearningLoopError(
            f"learning prompt is missing safety boundaries: {missing}"
        )
    return {
        "path": str(path),
        "version": match.group(1),
        "sha256": _sha256_bytes(raw),
        "phases": list(PHASES),
        "max_apply_rounds": 1,
    }


def _git_dirty_paths(root: Path) -> list[str]:
    completed = subprocess.run(
        ["git", "status", "--porcelain=v1", "-z", "--untracked-files=all"],
        cwd=root,
        check=False,
        capture_output=True,
    )
    if completed.returncode != 0:
        message = completed.stderr.decode("utf-8", errors="replace").strip()
        raise LearningLoopError(f"cannot inventory git worktree: {message}")
    entries = completed.stdout.decode("utf-8", errors="surrogateescape").split("\0")
    paths: list[str] = []
    index = 0
    while index < len(entries):
        entry = entries[index]
        index += 1
        if not entry:
            continue
        if len(entry) < 4:
            raise LearningLoopError(f"cannot parse git status entry: {entry!r}")
        status = entry[:2]
        paths.append(entry[3:])
        if "R" in status or "C" in status:
            if index < len(entries) and entries[index]:
                paths.append(entries[index])
                index += 1
    return sorted(set(paths))


def _active_contexts(root: Path) -> list[str]:
    active = root / "trades" / "active"
    if not active.is_dir():
        return []
    return sorted(str(path.relative_to(root)) for path in active.glob("*.md"))


def _progress_entries(root: Path) -> int:
    path = root / "progress" / "HISTORY.jsonl"
    if not path.is_file():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line)


def classify_bottleneck(
    batch: Mapping[str, Any],
    evidence: Mapping[str, Any],
    research: Mapping[str, Any],
) -> dict[str, Any]:
    cold = batch.get("cold_path", {})
    warm = batch.get("warm_path", {})
    end_to_end = cold.get("end_to_end", {})
    preflight = cold.get("preflight", {})
    warm_replay = warm.get("full_local_replay", {})
    request_telemetry = (
        evidence.get("preflight", {}).get("performance", {}).get("ibkr_requests", {})
    )
    request_counts = request_telemetry.get("submitted_by_kind", {})
    request_seconds = request_telemetry.get("request_seconds_by_kind", {})
    contract_requests = int(
        request_counts.get(
            "contract-details", preflight.get("contract_detail_requests", 0)
        )
    )
    bar_requests = int(
        request_counts.get(
            "historical-bars", preflight.get("historical_bar_requests_submitted", 0)
        )
    )
    contract_seconds = float(request_seconds.get("contract-details", 0.0))
    bar_seconds = float(request_seconds.get("historical-bars", 0.0))
    total_request_seconds = contract_seconds + bar_seconds
    contract_time_fraction = (
        contract_seconds / total_request_seconds if total_request_seconds else None
    )
    cold_seconds = float(end_to_end.get("elapsed_seconds", 0.0))
    warm_seconds = float(warm_replay.get("elapsed_seconds", 0.0))
    research_seconds = float(research.get("runtime", {}).get("elapsed_seconds", 0.0))
    acquisition_dominates = cold_seconds > 10 * max(
        warm_seconds, research_seconds, 0.001
    )
    symbol_lookup_primary = (
        contract_seconds > bar_seconds or contract_requests > bar_requests
    )
    return {
        "primary_bottleneck": (
            "cold_market_data_acquisition"
            if acquisition_dominates
            else "local_evaluation_or_interpretation"
        ),
        "symbol_lookup_assessment": (
            "primary" if symbol_lookup_primary else "secondary_repeated_cost"
        ),
        "cold_end_to_end_seconds": cold_seconds,
        "warm_replay_seconds": warm_seconds,
        "local_research_seconds": research_seconds,
        "contract_detail_requests": contract_requests,
        "historical_bar_requests": bar_requests,
        "contract_request_seconds": contract_seconds,
        "historical_bar_request_seconds": bar_seconds,
        "contract_time_fraction": (
            round(contract_time_fraction, 6)
            if contract_time_fraction is not None
            else None
        ),
        "provider_requests_during_local_research": int(
            research.get("runtime", {}).get("provider_requests", 0)
        ),
        "priority_order": [
            "reuse the frozen local corpus for hypothesis iteration",
            "remove redundant provider requests and preserve resume caches",
            "collect new dates only for missing coverage or independent confirmation",
        ],
    }


def build_inventory(
    *,
    root: Path = PROJECT_ROOT,
    prompt_path: Path = DEFAULT_PROMPT_PATH,
    batch_path: Path = DEFAULT_BATCH_STATUS,
    evidence_path: Path = DEFAULT_EVIDENCE,
    research_path: Path = DEFAULT_RESEARCH_RESULT,
) -> dict[str, Any]:
    prompt = load_prompt(prompt_path)
    dirty_paths = _git_dirty_paths(root)
    active_contexts = _active_contexts(root)
    return {
        "schema_version": 1,
        "mode": "learning",
        "broker_actions_allowed": False,
        "automatic_strategy_application": False,
        "prompt": prompt,
        "worktree": {
            "dirty_paths": dirty_paths,
            "preserve_as_baseline": True,
        },
        "safety": {
            "active_contexts": active_contexts,
            "local_active_context_warning": bool(active_contexts),
            "broker_state_checked": False,
        },
        "progress_entries_before": _progress_entries(root),
        "artifacts": {
            "batch_status": str(batch_path),
            "evidence": str(evidence_path),
            "research_result": str(research_path),
        },
        "bottleneck": classify_bottleneck(
            _load_json(batch_path),
            _load_json(evidence_path),
            _load_json(research_path),
        ),
    }


def review_change_plan(plan: Mapping[str, Any]) -> dict[str, Any]:
    if plan.get("broker_actions") not in (None, False):
        raise LearningLoopError("learning mode cannot authorize broker actions")
    external = plan.get("external_actions", [])
    if not isinstance(external, list) or external:
        raise LearningLoopError(
            "learning mode change plans cannot authorize external application actions"
        )
    rounds = plan.get("apply_rounds", 1)
    if isinstance(rounds, bool) or not isinstance(rounds, int) or not 0 <= rounds <= 1:
        raise LearningLoopError("learning mode permits at most one apply round")
    changes = plan.get("changes", [])
    if not isinstance(changes, list):
        raise LearningLoopError("changes must be an array")
    if not changes:
        return {
            "status": "no_op",
            "apply_rounds": 0,
            "requires_progress_entry": False,
        }
    normalized: list[dict[str, str]] = []
    substantive = False
    for index, raw in enumerate(changes, 1):
        if not isinstance(raw, Mapping):
            raise LearningLoopError(f"change {index} must be an object")
        path = str(raw.get("path", "")).strip()
        kind = str(raw.get("kind", "")).strip()
        reason = str(raw.get("reason", "")).strip()
        if not path or Path(path).is_absolute() or ".." in Path(path).parts:
            raise LearningLoopError(f"change {index} has an unsafe repository path")
        if kind not in ALLOWED_CHANGE_KINDS:
            raise LearningLoopError(f"change {index} has unsupported kind {kind!r}")
        if not reason:
            raise LearningLoopError(f"change {index} needs a reason")
        if path in PROTECTED_PATHS or path.startswith("historical_data/"):
            raise LearningLoopError(
                f"learning mode cannot plan a repository edit to {path}"
            )
        if kind == "strategy_proposal" and not path.startswith("strategy_proposals/"):
            raise LearningLoopError(
                "strategy proposals must remain under strategy_proposals/"
            )
        if path.startswith("strategy_proposals/") and kind != "strategy_proposal":
            raise LearningLoopError(
                "strategy_proposals paths must use the strategy_proposal kind"
            )
        substantive = substantive or kind in {
            "engineering",
            "documentation",
            "strategy_proposal",
        }
        normalized.append({"path": path, "kind": kind, "reason": reason})
    return {
        "status": "approved_for_bounded_apply",
        "apply_rounds": rounds,
        "changes": normalized,
        "requires_progress_entry": substantive,
    }


def _write_state(
    path: Path, value: Mapping[str, Any], *, allowed_root: Path = RUN_ROOT
) -> None:
    resolved = path.resolve()
    resolved_root = allowed_root.resolve()
    if resolved_root not in resolved.parents:
        raise LearningLoopError("learning state must be written under learning_runs/")
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_suffix(resolved.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(resolved)


def _sha256_path(path: Path) -> str | None:
    """Hash a file or directory without following symlinks."""
    if not path.exists():
        return None
    digest = hashlib.sha256()
    if path.is_file():
        digest.update(b"file\0")
        digest.update(path.read_bytes())
        return digest.hexdigest()
    digest.update(b"directory\0")
    for child in sorted(item for item in path.rglob("*") if item.is_file()):
        digest.update(str(child.relative_to(path)).encode())
        digest.update(b"\0")
        digest.update(child.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def protected_production_hashes(root: Path = PROJECT_ROOT) -> dict[str, str | None]:
    return {
        str(relative): _sha256_path(root / relative)
        for relative in PROTECTED_PRODUCTION_PATHS
    }


def _run_path(run_id: str, run_root: Path = RUN_ROOT) -> Path:
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise LearningLoopError(f"invalid learning run id {run_id!r}")
    return run_root / run_id / "state.json"


def _load_state(path: Path) -> dict[str, Any]:
    state = _load_json(path)
    if state.get("schema_version") != RUN_SCHEMA_VERSION:
        raise LearningLoopError(f"{path} has an unsupported run schema")
    run_id = state.get("run_id")
    if not isinstance(run_id, str) or path != _run_path(run_id, path.parents[1]):
        raise LearningLoopError(f"{path} run identity does not match its location")
    if state.get("status") not in RUN_STATUSES:
        raise LearningLoopError(f"{path} has an invalid run status")
    transitions = state.get("transitions")
    if not isinstance(transitions, list) or not transitions:
        raise LearningLoopError(f"{path} needs a transition history")
    return state


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _transition(state: dict[str, Any], status: str, reason: str) -> None:
    if status not in RUN_STATUSES:
        raise LearningLoopError(f"unsupported run transition {status}")
    now = _utc_now().isoformat()
    state["status"] = status
    state["updated_at"] = now
    state["transitions"].append({"at": now, "status": status, "reason": reason})


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


@contextmanager
def _run_lock(path: Path, *, stale_after_seconds: int = 3600):
    lock_path = path.with_name("run.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"pid": os.getpid(), "created_at_epoch": time.time()}
    while True:
        try:
            descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(payload, stream, sort_keys=True)
            break
        except FileExistsError:
            try:
                current = json.loads(lock_path.read_text(encoding="utf-8"))
                age = time.time() - float(current.get("created_at_epoch", 0))
                pid = int(current.get("pid", 0))
            except (OSError, ValueError, TypeError, json.JSONDecodeError):
                age, pid = stale_after_seconds + 1, 0
            if age <= stale_after_seconds or _pid_is_alive(pid):
                raise LearningLoopError(f"learning run is locked: {lock_path}")
            lock_path.unlink(missing_ok=True)
    try:
        yield
    finally:
        lock_path.unlink(missing_ok=True)


def initialize_program(*, root: Path = PROJECT_ROOT) -> dict[str, Any]:
    if not (root / PROGRAM_PATH.name).is_file():
        raise LearningLoopError("persistent learning program document is missing")
    registry_status = audit_registries(root / "learning")
    data_status = audit_learning_data(
        registry_root=root / "learning",
        security_path=root / "learning" / "SECURITY_MASTER.jsonl",
    )
    experiment_status = audit_experiment_program(
        registry_root=root / "learning",
        hypothesis_root=root / "learning" / "hypotheses",
    )
    strategy_status = audit_strategy_evidence(root / "learning")
    return {
        "initialized": True,
        "program": str(PROGRAM_PATH.relative_to(PROJECT_ROOT)),
        "registries": registry_status,
        "learning_data": data_status,
        "experiment_program": experiment_status,
        "strategy_evidence": strategy_status,
        "broker_actions_allowed": False,
        "automatic_strategy_application": False,
    }


def start_run(
    objective_id: str,
    *,
    root: Path = PROJECT_ROOT,
    run_root: Path = RUN_ROOT,
    now: datetime | None = None,
) -> dict[str, Any]:
    if not OBJECTIVE_ID_PATTERN.fullmatch(objective_id):
        raise LearningLoopError("objective id must be a stable lowercase identifier")
    initialize_program(root=root)
    experiments = current_entities("experiments", root / "learning")
    if objective_id not in experiments:
        raise LearningLoopError(
            "objective must be registered in learning/EXPERIMENTS.jsonl before starting"
        )
    instant = now or _utc_now()
    stamp = instant.strftime("%Y%m%dt%H%M%Sz").lower()
    slug = objective_id.removeprefix("experiment-")[:64]
    run_id = f"learning-{stamp}-{slug}"
    path = _run_path(run_id, run_root)
    if path.exists():
        raise LearningLoopError(f"learning run already exists: {run_id}")
    recorded_at = instant.isoformat()
    state = {
        "schema_version": RUN_SCHEMA_VERSION,
        "run_id": run_id,
        "objective_id": objective_id,
        "status": "CREATED",
        "started_at": recorded_at,
        "updated_at": recorded_at,
        "broker_actions_allowed": False,
        "automatic_strategy_application": False,
        "baseline": {
            "dirty_paths": _git_dirty_paths(root),
            "production_hashes": protected_production_hashes(root),
            "registry_hashes": {
                name: registry_fingerprint(name, root / "learning")
                for name in REGISTRIES
            },
        },
        "transitions": [
            {"at": recorded_at, "status": "CREATED", "reason": "run started"}
        ],
        "next_action": "inventory the registered objective and protected state",
    }
    _write_state(path, state, allowed_root=run_root)
    return state


def _latest_experiment(objective_id: str, root: Path) -> dict[str, Any]:
    try:
        return current_entities("experiments", root / "learning")[objective_id]
    except KeyError as exc:
        raise LearningLoopError(
            f"registered objective disappeared: {objective_id}"
        ) from exc


def _desired_run_status(experiment_status: str) -> tuple[str, str]:
    mapping = {
        "INVENTED": ("HYPOTHESIS_REGISTERED", "preregister the hypothesis contract"),
        "PREREGISTERED": ("PREREGISTERED", "collect or attach the frozen dataset"),
        "DATA_READY": ("DATA_READY", "run the registered evaluation"),
        "EVALUATED": ("EVALUATED", "perform adversarial review"),
        "ADVERSARIALLY_REVIEWED": (
            "ADVERSARIALLY_REVIEWED",
            "record reject, confirmation, or shadow disposition",
        ),
        "CONFIRMATION_QUEUED": (
            "CONFIRMATION_QUEUED",
            "start a separately frozen confirmation dataset",
        ),
        "SHADOW_QUEUED": ("SHADOW_QUEUED", "start prospective shadow qualification"),
        "FAILED": ("REJECTED", "close without threshold tuning"),
        "REJECTED": ("REJECTED", "close the rejected objective"),
        "RETIRED": ("REJECTED", "close the retired objective"),
        "CLOSED": ("REJECTED", "close the completed objective"),
    }
    try:
        return mapping[experiment_status]
    except KeyError as exc:
        raise LearningLoopError(
            f"experiment has unsupported lifecycle status {experiment_status!r}"
        ) from exc


def run_next(
    run_id: str, *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    path = _run_path(run_id, run_root)
    with _run_lock(path):
        state = _load_state(path)
        if state["status"] == "CLOSED":
            return {**state, "progressed": False}
        current_hashes = protected_production_hashes(root)
        if current_hashes != state["baseline"]["production_hashes"]:
            raise LearningLoopError(
                "protected production artifacts changed during learning run"
            )
        previous = str(state["status"])
        if previous == "CREATED":
            _transition(state, "INVENTORIED", "protected state and registries verified")
            state["next_action"] = "align the run with the current experiment event"
        else:
            experiment = _latest_experiment(str(state["objective_id"]), root)
            desired, next_action = _desired_run_status(
                str(experiment["payload"]["status"])
            )
            if previous in {"REJECTED", "CONFIRMATION_QUEUED", "SHADOW_QUEUED"}:
                _transition(
                    state, "CLOSED", f"terminal disposition recorded from {previous}"
                )
                state["outcome"] = previous.lower()
                state["next_action"] = "start a new explicit bounded learning run"
            elif previous == desired:
                state["next_action"] = next_action
                state["updated_at"] = _utc_now().isoformat()
            else:
                _transition(
                    state,
                    desired,
                    f"synchronized with experiment event {experiment['event_id']}",
                )
                state["next_action"] = next_action
        _write_state(path, state, allowed_root=run_root)
        return {**state, "progressed": previous != state["status"]}


def run_bounded(
    run_id: str,
    max_steps: int,
    *,
    root: Path = PROJECT_ROOT,
    run_root: Path = RUN_ROOT,
) -> dict[str, Any]:
    if isinstance(max_steps, bool) or not 1 <= max_steps <= 20:
        raise LearningLoopError("max_steps must be between 1 and 20")
    steps: list[dict[str, Any]] = []
    for _ in range(max_steps):
        state = run_next(run_id, root=root, run_root=run_root)
        steps.append({"status": state["status"], "progressed": state["progressed"]})
        if not state["progressed"] or state["status"] == "CLOSED":
            break
    return {"run_id": run_id, "steps": steps, "state": state}


def run_status(run_id: str, *, run_root: Path = RUN_ROOT) -> dict[str, Any]:
    return _load_state(_run_path(run_id, run_root))


def close_run(
    run_id: str,
    outcome: str,
    *,
    root: Path = PROJECT_ROOT,
    run_root: Path = RUN_ROOT,
) -> dict[str, Any]:
    if outcome not in {"completed", "failed", "blocked"}:
        raise LearningLoopError("close outcome must be completed, failed, or blocked")
    path = _run_path(run_id, run_root)
    with _run_lock(path):
        state = _load_state(path)
        if protected_production_hashes(root) != state["baseline"]["production_hashes"]:
            raise LearningLoopError(
                "protected production artifacts changed during learning run"
            )
        if state["status"] != "CLOSED":
            _transition(state, "CLOSED", f"run explicitly closed: {outcome}")
        state["outcome"] = outcome
        state["next_action"] = "start a new explicit bounded learning run"
        _write_state(path, state, allowed_root=run_root)
        return state


def audit_program(
    *, root: Path = PROJECT_ROOT, run_root: Path = RUN_ROOT
) -> dict[str, Any]:
    program = initialize_program(root=root)
    runs: list[dict[str, Any]] = []
    if run_root.exists():
        for path in sorted(run_root.glob("learning-*/state.json")):
            state = _load_state(path)
            runs.append(
                {
                    "run_id": state["run_id"],
                    "status": state["status"],
                    "objective_id": state["objective_id"],
                }
            )
    return {"valid": True, "program": program, "runs": runs}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("validate-prompt", help="validate the versioned loop prompt")

    inspect = subparsers.add_parser(
        "inspect", help="classify the measured learning path"
    )
    inspect.add_argument("--batch-status", type=Path, default=DEFAULT_BATCH_STATUS)
    inspect.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    inspect.add_argument(
        "--research-result", type=Path, default=DEFAULT_RESEARCH_RESULT
    )
    inspect.add_argument("--output", type=Path)

    plan = subparsers.add_parser(
        "review-plan", help="validate a one-slice machine-readable change plan"
    )
    plan.add_argument("plan", type=Path)
    subparsers.add_parser("init", help="validate the persistent program and registries")
    status = subparsers.add_parser("status", help="show one resumable run")
    status.add_argument("--run-id", required=True)
    start = subparsers.add_parser("start", help="start a registered bounded objective")
    start.add_argument("--objective", required=True)
    next_step = subparsers.add_parser(
        "run-next", help="perform one deterministic transition"
    )
    next_step.add_argument("--run-id", required=True)
    run = subparsers.add_parser("run", help="perform bounded deterministic transitions")
    run.add_argument("--run-id", required=True)
    run.add_argument("--max-steps", type=int, required=True)
    subparsers.add_parser("audit", help="audit registries and resumable run state")
    close = subparsers.add_parser("close", help="explicitly close one bounded run")
    close.add_argument("--run-id", required=True)
    close.add_argument(
        "--outcome", choices=("completed", "failed", "blocked"), required=True
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "validate-prompt":
            result = load_prompt()
        elif args.command == "review-plan":
            result = review_change_plan(_load_json(args.plan))
        elif args.command == "inspect":
            result = build_inventory(
                batch_path=args.batch_status,
                evidence_path=args.evidence,
                research_path=args.research_result,
            )
            result["inspected_at"] = datetime.now(UTC).isoformat()
            if args.output is not None:
                _write_state(args.output, result)
        elif args.command == "init":
            result = initialize_program()
        elif args.command == "status":
            result = run_status(args.run_id)
        elif args.command == "start":
            result = start_run(args.objective)
        elif args.command == "run-next":
            result = run_next(args.run_id)
        elif args.command == "run":
            result = run_bounded(args.run_id, args.max_steps)
        elif args.command == "audit":
            result = audit_program()
        else:
            result = close_run(args.run_id, args.outcome)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (LearningLoopError, RegistryError, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
