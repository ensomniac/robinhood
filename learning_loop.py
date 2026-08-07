"""Inspect and audit bounded strategy-neutral engineering/data learning work."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

import outcome_exposure
import progress_history
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from session_mode import MODES


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PROMPT = PROJECT_ROOT / "LEARNING_LOOP.md"
ARCHIVE_TAG = "legacy-pre-clean-slate-2026-08-07"
REQUIRED_SECTIONS = (
    "## Objective",
    "## Scope",
    "## Evidence",
    "## Acceptance checks",
    "## Safety boundaries",
)
REQUIRED_BOUNDARIES = (
    "No active strategy",
    "No broker order review, placement, cancellation, replacement, or mutation",
    "No deletion or relocation of preserved historical data",
)
REQUIRED_FILES = (
    "AGENTS.md",
    "HISTORICAL_DATA_STORE.md",
    "IDENTIFIER_ENCRYPTION.md",
    "LEARNING_LOOP.md",
    "README.md",
    "history/ACCOUNT_HISTORY.md",
    "history/OUTCOME_EXPOSURE_INDEX.jsonl",
    "history/PRE_RESET_CANONICAL_AUDIT.json",
    "history/PRE_RESET_MANIFEST.json",
    "history/legacy/PORTFOLIO_SIGNALS.jsonl",
    "history/legacy/SIGNALS.jsonl",
    "progress/HISTORY.jsonl",
)
ACTIVE_STRATEGY_FILES = {
    "portfolio_config.toml",
    "portfolio_live.py",
    "session_guard.py",
    "strategy_config.toml",
    "strategy_engine.py",
    "strategy_lab.py",
}
ACTIVE_STRATEGY_PREFIXES = ("strategy_lab/", "strategy_tournament/")
PROTECTED_PLAN_PATHS = {
    "history/PRE_RESET_CANONICAL_AUDIT.json",
    "history/PRE_RESET_MANIFEST.json",
    "history/legacy/PORTFOLIO_SIGNALS.jsonl",
    "history/legacy/SIGNALS.jsonl",
}
VERSION_PATTERN = re.compile(r"^Contract version: `([^`]+)`$", re.MULTILINE)


class LearningLoopError(RuntimeError):
    """The neutral learning contract or repository boundary is invalid."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _git(root: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _tracked_files(root: Path) -> list[str]:
    return [line for line in _git(root, "ls-files").splitlines() if line]


def _active_strategy_files(paths: Sequence[str]) -> list[str]:
    return sorted(
        path
        for path in paths
        if path in ACTIVE_STRATEGY_FILES or path.startswith(ACTIVE_STRATEGY_PREFIXES)
    )


def _dirty_paths(root: Path) -> list[str]:
    paths: set[str] = set()
    for line in _git(root, "status", "--porcelain").splitlines():
        value = line[3:].strip()
        if " -> " in value:
            value = value.split(" -> ", 1)[1]
        if value:
            paths.add(value)
    return sorted(paths)


def load_prompt(path: Path = DEFAULT_PROMPT) -> dict[str, Any]:
    """Validate the ordered public contract and return its content identity."""
    resolved = path.resolve()
    if not resolved.is_file():
        raise LearningLoopError(f"learning prompt is unavailable: {resolved}")
    if resolved.stat().st_size > 256 * 1024:
        raise LearningLoopError("learning prompt is unexpectedly large")
    text = resolved.read_text(encoding="utf-8")
    match = VERSION_PATTERN.search(text)
    if match is None:
        raise LearningLoopError("learning prompt lacks its contract version")
    positions = []
    for heading in REQUIRED_SECTIONS:
        count = text.count(heading)
        if count != 1:
            raise LearningLoopError(f"learning prompt needs exactly one {heading}")
        positions.append(text.index(heading))
    if positions != sorted(positions):
        raise LearningLoopError("learning prompt sections are out of order")
    for boundary in REQUIRED_BOUNDARIES:
        if boundary not in text:
            raise LearningLoopError(f"learning prompt lacks boundary: {boundary}")
    return {
        "valid": True,
        "path": str(resolved),
        "version": match.group(1),
        "sections": list(REQUIRED_SECTIONS),
        "sha256": _sha256(resolved),
    }


def _historical_boundary(root: Path) -> dict[str, Any]:
    env_path = root / ".env"
    if not env_path.exists():
        return {
            "configured": False,
            "reason": "private .env is absent",
            "full_document_audit_performed": False,
        }
    try:
        config = HistoricalStoreConfig.from_env(env_path)
    except HistoricalStoreError as exc:
        return {
            "configured": False,
            "reason": str(exc),
            "full_document_audit_performed": False,
        }
    return {
        "configured": True,
        "root": str(config.root),
        "outside_repository": root.resolve() not in config.root.parents
        and config.root != root.resolve(),
        "store_marker_exists": (config.root / "_store.json").is_file(),
        "archive_inventory_exists": (
            config.root / "_archive/pre-clean-slate-2026-08-07/summary.json"
        ).is_file(),
        "full_document_audit_performed": False,
    }


def inspect_repository(root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Return a read-only baseline without opening providers or scanning all data."""
    root = root.resolve()
    tracked = _tracked_files(root)
    dirty = _dirty_paths(root)
    progress_path = root / "progress/HISTORY.jsonl"
    exposure_path = root / "history/OUTCOME_EXPOSURE_INDEX.jsonl"
    progress_entries = progress_history.load_history(progress_path)
    exposure = outcome_exposure.audit(exposure_path)
    return {
        "valid": True,
        "repository": {
            "root": str(root),
            "branch": _git(root, "rev-parse", "--abbrev-ref", "HEAD"),
            "head": _git(root, "rev-parse", "HEAD"),
            "tracked_files": len(tracked),
            "dirty_paths": dirty[:100],
            "dirty_path_count": len(dirty),
        },
        "historical_store": _historical_boundary(root),
        "history": {
            "progress_entries": len(progress_entries),
            "outcome_exposure": exposure,
        },
        "safety": {
            "live_trading_enabled": False,
            "broker_action_modes": [
                mode.key for mode in MODES if mode.broker_actions_allowed
            ],
            "active_strategy_files": _active_strategy_files(tracked),
        },
        "note": "This inspection does not perform a full canonical document audit.",
    }


def _normalize_scope(value: str) -> str:
    text = value.strip().replace("\\", "/")
    path = PurePosixPath(text)
    if not text or path.is_absolute() or ".." in path.parts:
        raise LearningLoopError(f"scope must be a repository-relative path: {value!r}")
    normalized = path.as_posix()
    if normalized in PROTECTED_PLAN_PATHS:
        raise LearningLoopError(f"frozen preservation path is out of scope: {normalized}")
    if normalized in ACTIVE_STRATEGY_FILES or normalized.startswith(
        ACTIVE_STRATEGY_PREFIXES
    ):
        raise LearningLoopError(f"active strategy surface is out of scope: {normalized}")
    return normalized


def review_change_plan(objective: str, scopes: Sequence[str]) -> dict[str, Any]:
    """Build a non-executing bounded engineering/data review plan."""
    normalized_objective = objective.strip()
    if not normalized_objective or len(normalized_objective) > 500:
        raise LearningLoopError("objective must contain 1-500 characters")
    normalized_scopes = sorted({_normalize_scope(value) for value in scopes})
    if not normalized_scopes or len(normalized_scopes) > 20:
        raise LearningLoopError("plan needs between 1 and 20 exact scope paths")
    return {
        "status": "approved_for_bounded_engineering_review",
        "objective": normalized_objective,
        "scope": normalized_scopes,
        "strategy_allowed": False,
        "broker_actions_allowed": False,
        "external_writes_allowed": False,
        "requires_progress_entry": True,
        "phases": [
            {
                "phase": "inspect",
                "outcome": "capture the current truth surface and preserve user changes",
            },
            {
                "phase": "implement",
                "outcome": "make only the bounded engineering or data-quality change",
            },
            {
                "phase": "verify",
                "outcome": "run explicit acceptance checks and the retained suite",
            },
            {
                "phase": "record",
                "outcome": "append durable progress evidence and inspect the staged diff",
            },
        ],
    }


def _validate_preservation(root: Path) -> dict[str, Any]:
    manifest_path = root / "history/PRE_RESET_MANIFEST.json"
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearningLoopError(f"cannot read preservation manifest: {exc}") from exc
    if manifest.get("schema_version") != 1:
        raise LearningLoopError("preservation manifest schema drifted")
    checks = {
        "canonical_audit": (
            root / str(manifest["canonical_store_audit"]["path"]),
            str(manifest["canonical_store_audit"]["audit_sha256"]),
        ),
        "outcome_exposure": (
            root / str(manifest["compact_public_history"]["outcome_exposure_index"]["path"]),
            str(manifest["compact_public_history"]["outcome_exposure_index"]["sha256"]),
        ),
        "portfolio_signals": (
            root / str(manifest["compact_public_history"]["portfolio_signals"]["path"]),
            str(manifest["compact_public_history"]["portfolio_signals"]["sha256"]),
        ),
        "strategy_signals": (
            root / str(manifest["compact_public_history"]["strategy_signals"]["path"]),
            str(manifest["compact_public_history"]["strategy_signals"]["sha256"]),
        ),
    }
    observed = {}
    for label, (path, expected) in checks.items():
        actual = _sha256(path)
        if actual != expected:
            raise LearningLoopError(f"preserved {label} hash drifted")
        observed[label] = actual
    if manifest["canonical_store_audit"].get("valid") is not True:
        raise LearningLoopError("frozen canonical audit is not valid")
    return {"valid": True, "hashes": observed}


def audit_program(root: Path = PROJECT_ROOT) -> dict[str, Any]:
    """Validate clean-slate, history, and live-disabled invariants."""
    root = root.resolve()
    prompt = load_prompt(root / "LEARNING_LOOP.md")
    missing = [path for path in REQUIRED_FILES if not (root / path).is_file()]
    if missing:
        raise LearningLoopError(f"required clean-slate files are missing: {missing}")
    tracked = _tracked_files(root)
    active = _active_strategy_files(tracked)
    if active:
        raise LearningLoopError(f"active strategy surfaces are tracked: {active}")
    broker_modes = [mode.key for mode in MODES if mode.broker_actions_allowed]
    if broker_modes:
        raise LearningLoopError(f"broker-enabled modes are forbidden: {broker_modes}")
    progress = progress_history.load_history(root / "progress/HISTORY.jsonl")
    exposure = outcome_exposure.audit(root / "history/OUTCOME_EXPOSURE_INDEX.jsonl")
    preservation = _validate_preservation(root)
    try:
        archive_commit = _git(root, "rev-list", "-n", "1", ARCHIVE_TAG)
    except subprocess.CalledProcessError as exc:
        raise LearningLoopError(f"archive tag is unavailable: {ARCHIVE_TAG}") from exc
    return {
        "valid": True,
        "live_trading_enabled": False,
        "broker_action_modes": broker_modes,
        "active_strategy_files": active,
        "archive_tag": ARCHIVE_TAG,
        "archive_commit": archive_commit,
        "prompt": prompt,
        "preservation": preservation,
        "progress_entries": len(progress),
        "outcome_exposure": exposure,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("inspect", help="report the repository/data baseline")
    validate = subparsers.add_parser(
        "validate-prompt", help="validate an ordered bounded prompt"
    )
    validate.add_argument("path", nargs="?", type=Path, default=DEFAULT_PROMPT)
    review = subparsers.add_parser(
        "review-plan", help="review a bounded non-executing change plan"
    )
    review.add_argument("--objective", required=True)
    review.add_argument("--scope", action="append", required=True)
    subparsers.add_parser("audit", help="validate every clean-slate invariant")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect":
            result = inspect_repository()
        elif args.command == "validate-prompt":
            result = load_prompt(args.path)
        elif args.command == "review-plan":
            result = review_change_plan(args.objective, args.scope)
        else:
            result = audit_program()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        LearningLoopError,
        outcome_exposure.OutcomeExposureError,
        progress_history.ProgressHistoryError,
        subprocess.CalledProcessError,
        OSError,
        KeyError,
        TypeError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
