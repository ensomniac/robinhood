"""Append and audit durable repository progress findings."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_HISTORY_PATH = PROJECT_ROOT / "progress" / "HISTORY.jsonl"
ENTRY_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}-[a-z0-9][a-z0-9-]*$")
MEANINGFUL_EXEMPT_PREFIXES = ("progress/", ".githooks/", "tests/", "trades/")
MEANINGFUL_EXEMPT_FILES = {
    "LICENSE",
    "SIGNALS.jsonl",
    "TRADES.md",
}


class ProgressHistoryError(RuntimeError):
    """Raised when progress history or its contribution hook is invalid."""


def _nonempty_text(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ProgressHistoryError(f"{name} must be a non-empty string")
    return value.strip()


def _text_list(value: Any, name: str, *, required: bool = False) -> list[str]:
    if not isinstance(value, list):
        raise ProgressHistoryError(f"{name} must be an array")
    result = [
        _nonempty_text(item, f"{name}[{index}]") for index, item in enumerate(value)
    ]
    if required and not result:
        raise ProgressHistoryError(f"{name} cannot be empty")
    return result


def validate_entry(value: Any, *, line_number: int | None = None) -> dict[str, Any]:
    prefix = f"line {line_number}: " if line_number is not None else ""
    if not isinstance(value, Mapping):
        raise ProgressHistoryError(f"{prefix}entry must be an object")
    entry = dict(value)
    if entry.get("schema_version") != 1:
        raise ProgressHistoryError(f"{prefix}schema_version must be 1")
    entry_id = _nonempty_text(entry.get("id"), f"{prefix}id")
    if not ENTRY_ID_PATTERN.fullmatch(entry_id):
        raise ProgressHistoryError(
            f"{prefix}id must start with YYYY-MM-DD and contain lowercase slug text"
        )
    recorded_at = _nonempty_text(entry.get("recorded_at"), f"{prefix}recorded_at")
    try:
        parsed = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProgressHistoryError(
            f"{prefix}recorded_at must be an ISO timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise ProgressHistoryError(f"{prefix}recorded_at must include a timezone")
    _nonempty_text(entry.get("category"), f"{prefix}category")
    _nonempty_text(entry.get("title"), f"{prefix}title")
    _nonempty_text(entry.get("summary"), f"{prefix}summary")
    _text_list(entry.get("findings"), f"{prefix}findings", required=True)
    _text_list(entry.get("impact"), f"{prefix}impact", required=True)
    _text_list(entry.get("follow_ups", []), f"{prefix}follow_ups")
    related = _text_list(entry.get("related_files", []), f"{prefix}related_files")
    for index, path_text in enumerate(related):
        path = PurePosixPath(path_text)
        if path.is_absolute() or ".." in path.parts:
            raise ProgressHistoryError(
                f"{prefix}related_files[{index}] must be a repository-relative path"
            )
    return entry


def load_history(path: Path = DEFAULT_HISTORY_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    entries: list[dict[str, Any]] = []
    identifiers: set[str] = set()
    for line_number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not raw.strip():
            raise ProgressHistoryError(
                f"line {line_number}: blank lines are not allowed"
            )
        try:
            value = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ProgressHistoryError(
                f"line {line_number}: invalid JSON: {exc.msg}"
            ) from exc
        entry = validate_entry(value, line_number=line_number)
        entry_id = str(entry["id"])
        if entry_id in identifiers:
            raise ProgressHistoryError(f"line {line_number}: duplicate id {entry_id}")
        identifiers.add(entry_id)
        entries.append(entry)
    return entries


def append_entry(entry: Mapping[str, Any], path: Path = DEFAULT_HISTORY_PATH) -> None:
    normalized = validate_entry(entry)
    existing = load_history(path)
    if any(value["id"] == normalized["id"] for value in existing):
        raise ProgressHistoryError(f"duplicate id {normalized['id']}")
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(normalized, sort_keys=True, separators=(",", ":")))
        stream.write("\n")


def is_meaningful_path(path: str) -> bool:
    normalized = path.strip().replace("\\", "/")
    if not normalized or normalized in MEANINGFUL_EXEMPT_FILES:
        return False
    return not normalized.startswith(MEANINGFUL_EXEMPT_PREFIXES)


def contribution_required(paths: Sequence[str]) -> bool:
    return any(is_meaningful_path(path) for path in paths)


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def staged_paths() -> list[str]:
    return [
        line.strip()
        for line in _git(
            "diff", "--cached", "--name-only", "--diff-filter=ACMR"
        ).splitlines()
        if line.strip()
    ]


def staged_history_changes() -> tuple[int, int]:
    output = _git("diff", "--cached", "--numstat", "--", "progress/HISTORY.jsonl")
    if not output.strip():
        return 0, 0
    fields = output.split()
    additions = int(fields[0]) if fields[0].isdigit() else 0
    deletions = int(fields[1]) if len(fields) > 1 and fields[1].isdigit() else 0
    return additions, deletions


def check_staged_contribution() -> dict[str, Any]:
    load_history()
    changed = staged_paths()
    required = contribution_required(changed)
    bypassed = os.environ.get("PROGRESS_SKIP") == "1"
    additions, deletions = staged_history_changes()
    if deletions:
        raise ProgressHistoryError(
            "progress/HISTORY.jsonl is append-only; existing records cannot be "
            "modified or deleted"
        )
    if required and not bypassed and additions < 1:
        raise ProgressHistoryError(
            "substantive staged changes need a new progress/HISTORY.jsonl entry; "
            "run progress_history.py add or set PROGRESS_SKIP=1 only for a truly "
            "mechanical/generated commit"
        )
    return {
        "valid": True,
        "meaningful_change": required,
        "history_additions": additions,
        "history_deletions": deletions,
        "bypassed": bypassed,
        "staged_paths": changed,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("audit", help="validate the append-only progress history")
    subparsers.add_parser(
        "check-staged",
        help="require a new finding when substantive repository changes are staged",
    )
    subparsers.add_parser(
        "install-hook",
        help="configure this checkout to use the tracked .githooks directory",
    )
    add = subparsers.add_parser("add", help="append one structured progress finding")
    add.add_argument("--id", required=True)
    add.add_argument("--recorded-at", required=True)
    add.add_argument("--category", required=True)
    add.add_argument("--title", required=True)
    add.add_argument("--summary", required=True)
    add.add_argument("--finding", action="append", required=True)
    add.add_argument("--impact", action="append", required=True)
    add.add_argument("--follow-up", action="append", default=[])
    add.add_argument("--related-file", action="append", default=[])
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "audit":
            entries = load_history()
            result: Any = {"valid": True, "entries": len(entries)}
        elif args.command == "check-staged":
            result = check_staged_contribution()
        elif args.command == "install-hook":
            _git("config", "core.hooksPath", ".githooks")
            result = {"installed": True, "core.hooksPath": ".githooks"}
        else:
            append_entry(
                {
                    "schema_version": 1,
                    "id": args.id,
                    "recorded_at": args.recorded_at,
                    "category": args.category,
                    "title": args.title,
                    "summary": args.summary,
                    "findings": args.finding,
                    "impact": args.impact,
                    "follow_ups": args.follow_up,
                    "related_files": args.related_file,
                }
            )
            result = {"appended": args.id, "path": str(DEFAULT_HISTORY_PATH)}
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (ProgressHistoryError, OSError, subprocess.CalledProcessError) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
