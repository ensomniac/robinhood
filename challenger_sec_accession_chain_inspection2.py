"""Independently inspect the second challenger's accession-chain contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path

import challenger_sec_accession_chain_recovery2 as challenger
import development_sec_accession_chain_contract_inspection as base_inspection
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


class ChallengerSecAccessionChainInspection2Error(RuntimeError):
    """The independently rebuilt second-tranche contract differs."""


def inspect_contract(*, manifest_path: Path, env_path: Path, status_path: Path):
    challenger.configure_base()
    result = base_inspection.inspect_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        status_path=status_path,
    )
    if not (
        result.get("dataset_id") == challenger.DATASET_ID
        and result.get("counts")
        == {
            "candidate_pairs": challenger.EXPECTED_UNRESOLVED_PAIRS,
            "prior_pair_source_joins": challenger.EXPECTED_PRIOR_JOINS,
            "unique_accessions": challenger.EXPECTED_UNIQUE_ACCESSIONS,
        }
        and result.get("status") == "FROZEN_READY"
        and result.get("inspected") is True
        and result.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerSecAccessionChainInspection2Error(
            "second challenger recovery inspection boundary differs"
        )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=challenger.PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=challenger.DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_contract(
            manifest_path=args.manifest,
            env_path=args.env_file,
            status_path=args.status,
        )
    except (
        ChallengerSecAccessionChainInspection2Error,
        challenger.ChallengerSecAccessionChain2Error,
        challenger.base.DevelopmentSecAccessionChainError,
        base_inspection.DevelopmentSecAccessionChainInspectionError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
