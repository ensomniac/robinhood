"""Safely stage the tracked-file reduction after the archive tag is pushed."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path, PurePosixPath
from typing import Any, Sequence


PROJECT_ROOT = Path(__file__).resolve().parent
EXPECTED_ROOT = Path("/Users/ensomniac/trade/robinhood_codex")
HISTORICAL_ROOT = Path("/Users/ensomniac/trade/historical_data")
ARCHIVE_TAG = "legacy-pre-clean-slate-2026-08-07"
INVENTORY_SUMMARY = (
    HISTORICAL_ROOT / "_archive/pre-clean-slate-2026-08-07/summary.json"
)
EXPECTED_INVENTORY_SHA256 = (
    "042a37225d2ff0c4c7e55f74b8835ce1ccd1d98f285d9b837264aa780303b679"
)
EXPECTED_HISTORY_HASHES = {
    "history/OUTCOME_EXPOSURE_INDEX.jsonl": (
        "d393e2c7e20164cc66200afcdc38180f9bc40e705365db5b41f34705aff95db4"
    ),
    "history/PRE_RESET_CANONICAL_AUDIT.json": (
        "50cbcf9ce6ec8d1ab0cd59ec6f5aecae44c0bc5e1f367def7020ca5730254e66"
    ),
    "history/PRE_RESET_MANIFEST.json": (
        "157fa7c754cc9d2b12a705974b433cfafcc4bb67145b64586d083bb66abd5957"
    ),
    "history/legacy/PORTFOLIO_SIGNALS.jsonl": (
        "5661ab39a26767e0646c39995a1496f97761384f5fb2e8c72b9d6b6c7fd8f5a7"
    ),
    "history/legacy/SIGNALS.jsonl": (
        "6fd0ae96796851f23c86797a58452b08ab3f641aab43f81507d84c08dd2fb2f4"
    ),
}
KEEP_FILES = {
    ".env.example",
    ".githooks/pre-commit",
    ".gitignore",
    "AGENTS.md",
    "HISTORICAL_DATA_STORE.md",
    "IDENTIFIER_ENCRYPTION.md",
    "LEARNING_LOOP.md",
    "LICENSE",
    "README.md",
    "historical_concurrency.py",
    "historical_data_cli.py",
    "historical_metrics.py",
    "historical_migration.py",
    "historical_providers.py",
    "historical_service.py",
    "historical_store.py",
    "ibkr_historical.py",
    "learning_loop.py",
    "outcome_exposure.py",
    "progress/HISTORY.jsonl",
    "progress_history.py",
    "requirements-dev.txt",
    "requirements.txt",
    "sensitive_data.py",
    "session_mode.py",
    "tests/__init__.py",
    "tests/test_historical_concurrency.py",
    "tests/test_historical_metrics.py",
    "tests/test_historical_migration.py",
    "tests/test_historical_providers.py",
    "tests/test_historical_service.py",
    "tests/test_historical_store.py",
    "tests/test_ibkr_historical.py",
    "tests/test_learning_loop.py",
    "tests/test_outcome_exposure.py",
    "tests/test_progress_history.py",
    "tests/test_sensitive_data.py",
    "tests/test_session_mode.py",
}
KEEP_PREFIXES = ("history/",)


class CleanupError(RuntimeError):
    """The preservation or cleanup boundary is not exact."""


def _git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def _validate_path(path_text: str) -> None:
    path = PurePosixPath(path_text)
    if path.is_absolute() or not path.parts or ".." in path.parts:
        raise CleanupError(f"unsafe tracked path: {path_text!r}")
    resolved = (PROJECT_ROOT / path_text).resolve()
    if PROJECT_ROOT not in resolved.parents:
        raise CleanupError(f"tracked path escapes repository: {path_text!r}")


def _plan() -> dict[str, Any]:
    if PROJECT_ROOT.resolve() != EXPECTED_ROOT:
        raise CleanupError(f"unexpected project root: {PROJECT_ROOT}")
    if _git("status", "--porcelain"):
        raise CleanupError("worktree must be clean before the tracked-file reduction")
    head = _git("rev-parse", "HEAD")
    tag_head = _git("rev-list", "-n", "1", ARCHIVE_TAG)
    if tag_head != head:
        raise CleanupError(f"{ARCHIVE_TAG} must resolve to the current archive commit")
    if not (HISTORICAL_ROOT / "_store.json").is_file():
        raise CleanupError("canonical historical store marker is unavailable")
    if _sha256(INVENTORY_SUMMARY) != EXPECTED_INVENTORY_SHA256:
        raise CleanupError("private preservation inventory summary drifted")
    for relative, expected in EXPECTED_HISTORY_HASHES.items():
        if _sha256(PROJECT_ROOT / relative) != expected:
            raise CleanupError(f"preserved compact ledger drifted: {relative}")

    tracked = [line for line in _git("ls-files").splitlines() if line]
    modes = {
        fields[3]: fields[0]
        for line in _git("ls-files", "-s").splitlines()
        if len(fields := line.split(maxsplit=3)) == 4
    }
    for path in tracked:
        _validate_path(path)
        if modes.get(path) == "120000":
            raise CleanupError(f"refusing tracked symbolic link: {path}")
    kept = sorted(
        path
        for path in tracked
        if path in KEEP_FILES or path.startswith(KEEP_PREFIXES)
    )
    missing = sorted(path for path in KEEP_FILES if path not in tracked)
    if missing:
        raise CleanupError(f"required retained paths are not tracked: {missing}")
    removed = sorted(set(tracked) - set(kept))
    plan = {
        "schema_version": 1,
        "kind": "robinhood_codex_clean_slate_tracked_reduction",
        "archive_tag": ARCHIVE_TAG,
        "archive_commit": head,
        "tracked_before": len(tracked),
        "tracked_kept": len(kept),
        "tracked_removed": len(removed),
        "kept": kept,
        "removed": removed,
    }
    return {**plan, "manifest_sha256": _canonical_sha256(plan)}


def _apply(paths: Sequence[str]) -> None:
    for offset in range(0, len(paths), 100):
        batch = list(paths[offset : offset + 100])
        subprocess.run(["git", "rm", "--", *batch], cwd=PROJECT_ROOT, check=True)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--expected-manifest-sha256")
    args = parser.parse_args()
    plan = _plan()
    if args.apply:
        if args.expected_manifest_sha256 != plan["manifest_sha256"]:
            raise CleanupError("expected manifest hash does not match the dry run")
        _apply(plan["removed"])
        print(
            json.dumps(
                {
                    "applied": True,
                    "manifest_sha256": plan["manifest_sha256"],
                    "tracked_removed": plan["tracked_removed"],
                },
                indent=2,
                sort_keys=True,
            )
        )
    else:
        print(json.dumps(plan, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
