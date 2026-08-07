"""Parallel, read-only audit of every canonical historical day document."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic
from typing import Any

from historical_store import HistoricalDayStore, HistoricalStoreError, _load_gzip_json


EXPECTED_ROOT = Path("/Users/ensomniac/trade/historical_data")


@dataclass(frozen=True)
class SymbolAudit:
    symbol: str
    files: int
    dates: tuple[str, ...]
    datasets: int
    contexts: int
    providers: dict[str, int]
    errors: tuple[dict[str, str], ...]


def _audit_symbol(directory_text: str) -> SymbolAudit:
    directory = Path(directory_text)
    files = sorted(directory.glob("[0-9][0-9][0-9][0-9]/*.json.gz"))
    dates: set[str] = set()
    datasets = 0
    contexts = 0
    providers: Counter[str] = Counter()
    errors: list[dict[str, str]] = []
    symbol = directory.name
    for path in files:
        try:
            value = _load_gzip_json(path)
            HistoricalDayStore._validate_document(value, path)
            dates.add(str(value["date"]))
            datasets += len(value["datasets"])
            contexts += len(value["contexts"])
            providers.update(
                str(dataset.get("provider", "unknown"))
                for dataset in value["datasets"]
            )
        except (HistoricalStoreError, OSError, KeyError, TypeError) as exc:
            errors.append({"path": str(path), "error": str(exc)})
    return SymbolAudit(
        symbol=symbol,
        files=len(files),
        dates=tuple(sorted(dates)),
        datasets=datasets,
        contexts=contexts,
        providers=dict(providers),
        errors=tuple(errors),
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=EXPECTED_ROOT)
    parser.add_argument("--workers", type=int, default=min(8, os.cpu_count() or 1))
    args = parser.parse_args()
    root = args.root.resolve()
    if root != EXPECTED_ROOT:
        raise SystemExit(f"unexpected canonical root: {root}")
    if args.workers < 1 or args.workers > 16:
        raise SystemExit("workers must be between 1 and 16")
    directories = sorted(
        path
        for path in root.iterdir()
        if path.is_dir() and not path.name.startswith("_")
    )
    started = monotonic()
    file_count = 0
    symbols: set[str] = set()
    dates: set[str] = set()
    dataset_count = 0
    context_count = 0
    providers: Counter[str] = Counter()
    errors: list[dict[str, str]] = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        results = pool.map(
            _audit_symbol,
            (str(path) for path in directories),
            chunksize=1,
            buffersize=args.workers * 2,
        )
        for completed, result in enumerate(results, 1):
            file_count += result.files
            if result.files > len(result.errors):
                symbols.add(result.symbol)
            dates.update(result.dates)
            dataset_count += result.datasets
            context_count += result.contexts
            providers.update(result.providers)
            errors.extend(result.errors)
            if completed % 100 == 0:
                print(
                    f"audited {completed}/{len(directories)} symbols; "
                    f"{file_count} documents",
                    file=sys.stderr,
                    flush=True,
                )
    result: dict[str, Any] = {
        "schema_version": 1,
        "kind": "robinhood_codex_parallel_canonical_audit",
        "audited_at": datetime.now(UTC).isoformat(),
        "root": str(root),
        "workers": args.workers,
        "elapsed_seconds": monotonic() - started,
        "files": file_count,
        "symbols": len(symbols),
        "dates": len(dates),
        "datasets": dataset_count,
        "contexts": context_count,
        "providers": dict(sorted(providers.items())),
        "errors": errors,
        "valid": not errors,
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
