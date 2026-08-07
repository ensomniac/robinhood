"""Create the private preservation inventory used by the clean-slate reset.

This is a one-time archival tool.  It records the canonical Strategy Lab file
catalog without reopening four million day documents, and hashes every retained
non-canonical or repository-local ignored artifact.  Secrets are recorded only
by metadata and are never hashed or copied.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import stat
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parent
EXPECTED_PROJECT_ROOT = Path("/Users/ensomniac/trade/robinhood_codex")
HISTORICAL_ROOT = Path("/Users/ensomniac/trade/historical_data")
STATE_ROOT = HISTORICAL_ROOT / "_strategy_lab"
DATABASE_PATH = STATE_ROOT / "strategy_lab.duckdb"
DEFAULT_OUTPUT = HISTORICAL_ROOT / "_archive" / "pre-clean-slate-2026-08-07"
ARCHIVE_TAG = "legacy-pre-clean-slate-2026-08-07"
SECRET_PATHS = {STATE_ROOT / "bridge_hmac_secret"}
CHUNK_SIZE = 1024 * 1024


class InventoryError(RuntimeError):
    """Raised when the preservation boundary cannot be frozen safely."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK_SIZE), b""):
            digest.update(chunk)
    return digest.hexdigest()


def git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    return completed.stdout.strip()


def canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def atomic_gzip_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    count = 0
    logical_bytes = 0
    try:
        with os.fdopen(descriptor, "wb") as raw:
            with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as stream:
                for row in rows:
                    encoded = (canonical_json(row) + "\n").encode("utf-8")
                    stream.write(encoded)
                    count += 1
                    logical_bytes += len(encoded)
            raw.flush()
            os.fsync(raw.fileno())
        os.replace(temporary_name, path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
    return {
        "path": str(path),
        "records": count,
        "logical_bytes": logical_bytes,
        "compressed_bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def iter_paths(label: str, root: Path) -> Iterator[tuple[str, Path, str]]:
    if not root.exists():
        return
    if root.is_symlink():
        yield label, root, "."
        return
    if root.is_file():
        yield label, root, root.name
        return
    for directory, dirnames, filenames in os.walk(root, followlinks=False):
        base = Path(directory)
        dirnames.sort()
        filenames.sort()
        symlink_dirs = [name for name in dirnames if (base / name).is_symlink()]
        dirnames[:] = [name for name in dirnames if name not in symlink_dirs]
        for name in symlink_dirs:
            path = base / name
            yield label, path, path.relative_to(root).as_posix()
        for name in filenames:
            path = base / name
            yield label, path, path.relative_to(root).as_posix()


def hash_record(task: tuple[str, Path, str]) -> dict[str, Any]:
    label, path, relative = task
    details = path.lstat()
    mode = stat.S_IMODE(details.st_mode)
    common = {
        "root": label,
        "path": relative,
        "mode": f"{mode:04o}",
        "size_bytes": details.st_size,
        "modified_ns": details.st_mtime_ns,
    }
    if path in SECRET_PATHS:
        return {**common, "kind": "secret_metadata", "content_hashed": False}
    if stat.S_ISLNK(details.st_mode):
        return {
            **common,
            "kind": "symlink",
            "target": os.readlink(path),
            "content_hashed": False,
        }
    if not stat.S_ISREG(details.st_mode):
        return {**common, "kind": "other", "content_hashed": False}
    return {
        **common,
        "kind": "file",
        "content_hashed": True,
        "sha256": sha256_file(path),
    }


def inventory_rows(
    roots: list[tuple[str, Path]], *, workers: int
) -> Iterator[dict[str, Any]]:
    tasks = (
        task
        for label, root in roots
        for task in iter_paths(label, root)
    )
    completed = 0
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="archive-hash") as pool:
        for record in pool.map(hash_record, tasks, chunksize=1):
            completed += 1
            if completed % 1000 == 0:
                print(f"hashed {completed} retained artifacts", file=sys.stderr, flush=True)
            yield record


def export_catalog(output: Path) -> dict[str, Any]:
    connection = duckdb.connect(str(DATABASE_PATH), read_only=True)
    try:
        summary_row = connection.execute(
            """
            SELECT count(*), coalesce(sum(size_bytes), 0),
                   min(session_date), max(session_date)
            FROM raw_files
            """
        ).fetchone()
        dispositions = {
            str(name): int(count)
            for name, count in connection.execute(
                "SELECT disposition, count(*) FROM raw_files GROUP BY 1 ORDER BY 1"
            ).fetchall()
        }
        temporary = output.with_suffix(output.suffix + ".tmp")
        if temporary.exists():
            raise InventoryError(f"temporary catalog export already exists: {temporary}")
        escaped = str(temporary).replace("'", "''")
        connection.execute(
            f"""
            COPY (
              SELECT path, size_bytes, modified_ns, content_identity, disposition,
                     symbol, session_date
              FROM raw_files
              ORDER BY path
            ) TO '{escaped}' (FORMAT CSV, HEADER, COMPRESSION GZIP)
            """
        )
        os.replace(temporary, output)
    finally:
        connection.close()
    return {
        "path": str(output),
        "records": int(summary_row[0]),
        "bytes": int(summary_row[1]),
        "first_session": summary_row[2].isoformat() if summary_row[2] else None,
        "last_session": summary_row[3].isoformat() if summary_row[3] else None,
        "dispositions": dispositions,
        "compressed_bytes": output.stat().st_size,
        "sha256": sha256_file(output),
        "identity_note": (
            "content_identity is the frozen Strategy Lab catalog identity; "
            "the canonical store audit separately validates every document and dataset hash"
        ),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    root = PROJECT_ROOT.resolve()
    if root != EXPECTED_PROJECT_ROOT:
        raise InventoryError(f"unexpected project root: {root}")
    if git("status", "--porcelain"):
        allowed = {
            "?? history/",
            "?? pre_reset_canonical_audit.py",
            "?? pre_reset_cleanup.py",
            "?? pre_reset_inventory.py",
        }
        status = set(git("status", "--porcelain").splitlines())
        if status != allowed:
            raise InventoryError(f"unexpected dirty worktree: {sorted(status)}")
    if args.workers < 1 or args.workers > 32:
        raise InventoryError("workers must be between 1 and 32")
    output = args.output.resolve()
    if output != DEFAULT_OUTPUT:
        raise InventoryError(f"unexpected output root: {output}")
    if output.exists() and any(output.iterdir()):
        raise InventoryError(f"refusing to overwrite non-empty inventory: {output}")
    output.mkdir(parents=True, exist_ok=True)

    catalog = export_catalog(output / "canonical-catalog.csv.gz")
    roots = [
        ("external_sources", HISTORICAL_ROOT / "_sources"),
        ("external_derived", HISTORICAL_ROOT / "_derived"),
        ("external_migrations", HISTORICAL_ROOT / "_migrations"),
        ("external_strategy_lab", STATE_ROOT),
        ("repo_legacy_history", PROJECT_ROOT / "historical_data"),
        ("repo_learning_runs", PROJECT_ROOT / "learning_runs"),
        ("repo_research_runs", PROJECT_ROOT / "research_runs"),
        ("repo_dense_calendar", PROJECT_ROOT / "historical_batches" / "dense_v2"),
        ("repo_private_security_master", PROJECT_ROOT / "learning" / "SECURITY_MASTER.jsonl"),
        ("repo_private_security_masters", PROJECT_ROOT / "learning" / "security_masters"),
    ]
    noncanonical = atomic_gzip_jsonl(
        output / "retained-artifacts.jsonl.gz",
        inventory_rows(roots, workers=args.workers),
    )
    summary = {
        "schema_version": 1,
        "kind": "robinhood_codex_pre_clean_slate_private_inventory",
        "created_at": datetime.now(UTC).isoformat(),
        "archive_tag": ARCHIVE_TAG,
        "repository": {
            "root": str(PROJECT_ROOT),
            "head": git("rev-parse", "HEAD"),
            "branch": git("rev-parse", "--abbrev-ref", "HEAD"),
            "tracked_files": int(git("ls-files").count("\n") + 1),
        },
        "historical_root": str(HISTORICAL_ROOT),
        "catalog": catalog,
        "retained_artifacts": noncanonical,
        "secret_policy": (
            "secret contents are neither copied nor hashed; only mode, size, and timestamps are recorded"
        ),
    }
    summary_path = output / "summary.json"
    encoded = (json.dumps(summary, indent=2, sort_keys=True) + "\n").encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=".summary.", suffix=".tmp", dir=output
    )
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_name, summary_path)
    except Exception:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise
    result = {
        "valid": True,
        "summary_path": str(summary_path),
        "summary_sha256": sha256_file(summary_path),
        "catalog": catalog,
        "retained_artifacts": noncanonical,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
