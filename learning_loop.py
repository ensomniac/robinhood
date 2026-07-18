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
import re
import subprocess
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


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


def _write_state(path: Path, value: Mapping[str, Any]) -> None:
    resolved = path.resolve()
    allowed_root = (PROJECT_ROOT / "learning_runs").resolve()
    if allowed_root not in resolved.parents:
        raise LearningLoopError("learning state must be written under learning_runs/")
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_suffix(resolved.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(resolved)


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
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "validate-prompt":
            result = load_prompt()
        elif args.command == "review-plan":
            result = review_change_plan(_load_json(args.plan))
        else:
            result = build_inventory(
                batch_path=args.batch_status,
                evidence_path=args.evidence,
                research_path=args.research_result,
            )
            result["inspected_at"] = datetime.now(UTC).isoformat()
            if args.output is not None:
                _write_state(args.output, result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (LearningLoopError, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
