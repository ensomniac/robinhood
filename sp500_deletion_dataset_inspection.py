#!/usr/bin/env python3
"""Stable administrative CLI for S&P deletion dataset inspection.

The outcome-affecting inspector remains byte-identical to the implementation
frozen in the family search.  This wrapper supplies the corrected summary
rendering and binds its own committed controller without changing strategy
semantics after historical prices have been collected.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Sequence

import sp500_deletion_collection_inspection as inspection
import strategy_discovery


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "status",
        type=Path,
        help="committed deletion collection status artifact",
    )
    parser.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        strategy_discovery.require_committed(Path(__file__).resolve())
        (
            inspection_path,
            inspected,
            manifest_path,
            manifest,
        ) = inspection.inspect_collection(
            args.status,
            inspected_at=args.inspected_at,
        )
        result = {
            "written": strategy_discovery._relative(inspection_path),
            "artifact_sha256": inspected["artifact_sha256"],
            "state": inspected["state"],
            "dataset_manifest": strategy_discovery._relative(manifest_path),
            "dataset_manifest_sha256": manifest["manifest_sha256"],
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        inspection.Sp500DeletionInspectionError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
