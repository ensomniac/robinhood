"""Independently inspect second-tranche causal pre-entry transport."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

import challenger_orb_retest_preentry2 as challenger
import challenger_orb_retest_preentry_collection_inspection as base_inspection
from historical_providers import HistoricalProviderError
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-preentry-tranche2-collection-inspection.json"
)


class ChallengerPreentryCollectionInspection2Error(RuntimeError):
    """The second-tranche causal collection does not reconcile."""


def inspect_collection(
    *, manifest_path: Path, env_path: Path, status_path: Path, result_path: Path
) -> dict:
    challenger.configure_base()
    result = base_inspection.inspect_collection(
        manifest_path=manifest_path,
        env_path=env_path,
        status_path=status_path,
        result_path=result_path,
    )
    counts = result.get("counts", {})
    if not (
        result.get("dataset_id") == challenger.DATASET_ID
        and counts.get("expected_requests") == challenger.EXPECTED_TOTAL_REQUESTS
        and counts.get("successful_requests") == challenger.EXPECTED_TOTAL_REQUESTS
        and counts.get("failed_requests") == 0
        and counts.get("pending_requests") == 0
        and result.get("status") == "COLLECTION_INSPECTED"
        and result.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerPreentryCollectionInspection2Error(
            "second-tranche collection boundary differs"
        )
    result["inspection_implementation"].update(
        {
            "adapter_inspector_sha256": challenger.base._sha256_file(
                Path(__file__)
            ),
            "base_inspector_sha256": challenger.base._sha256_file(
                Path(base_inspection.__file__)
            ),
        }
    )
    challenger.base._write_json(result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=challenger.DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--result", type=Path, default=DEFAULT_RESULT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        value = inspect_collection(
            manifest_path=args.manifest,
            env_path=args.env_file,
            status_path=args.status,
            result_path=args.result,
        )
    except (
        ChallengerPreentryCollectionInspection2Error,
        challenger.ChallengerPreentry2Error,
        challenger.base.ChallengerPreentryError,
        base_inspection.ChallengerPreentryCollectionInspectionError,
        HistoricalProviderError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
