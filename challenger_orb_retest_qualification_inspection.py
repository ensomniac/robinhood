"""Independently inspect the frozen ORB-retest entry qualification result."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import challenger_orb_retest_qualification as qualification
from historical_store import HistoricalStoreError
from learning_data import LearningDataError
from scanner_replay import load_split_actions


class ChallengerRetestQualificationInspectionError(RuntimeError):
    """The frozen contract or independent qualification rebuild differs."""


def inspect_contract(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    manifest = qualification.load_manifest(manifest_path)
    current = qualification.base._read_json(status_path)
    store, index, _pairs, _states = qualification._load_source(env_path)
    expected = qualification._expected_contract(
        store=store,
        index=index,
        observed_free_bytes=int(
            manifest["capacity_contract"]["observed_free_bytes_at_freeze"]
        ),
    )
    for name, value in expected.items():
        if manifest.get(name) != value:
            raise ChallengerRetestQualificationInspectionError(
                f"qualification contract drifted at {name}"
            )
    for binding in manifest["implementation_contract"]["files"].values():
        qualification._verify_binding(binding)
    private_root = qualification._private_root(store.root)
    artifacts = list(private_root.rglob("*")) if private_root.exists() else []
    if not (
        current.get("manifest_sha256") == manifest["manifest_sha256"]
        and current.get("status") == "FROZEN_WAITING_INSPECTION"
        and current.get("inspected") is False
        and current.get("private_artifacts") == 0
        and current.get("outcome_contract_permitted") is False
        and current.get("target_outcomes_observed_or_derived") is False
        and not any(path.is_file() for path in artifacts)
        and not qualification.DEFAULT_RESULT.exists()
    ):
        raise ChallengerRetestQualificationInspectionError(
            "qualification zero-result state differs"
        )
    result = {
        "schema_version": 1,
        "dataset_id": qualification.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "pairs_expected": qualification.EXPECTED_PAIRS,
        "distinct_trigger_sessions": qualification.EXPECTED_DATES,
        "minimum_eligible_signals": qualification.MINIMUM_ELIGIBLE_SIGNALS,
        "private_artifacts": 0,
        "outcome_contract_permitted": False,
        "inspection": {
            "source_collection_rebuilt": True,
            "implementation_and_rules_rebuilt": True,
            "retest_stop_and_ranking_contract_rebuilt": True,
            "privacy_and_outcome_locks_rebuilt": True,
            "valid": True,
        },
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    qualification.base._write_json(status_path, result)
    return result


def inspect_result(
    *, manifest_path: Path, env_path: Path, status_path: Path, result_path: Path
) -> dict[str, Any]:
    manifest = qualification.load_manifest(manifest_path)
    store, _index, pairs, states = qualification._load_source(env_path)
    private_path = qualification._private_result_path(store.root)
    private = qualification.base._read_gzip(private_path)
    observed = private.get("records")
    if not (
        isinstance(observed, list)
        and len(observed) == qualification.EXPECTED_PAIRS
        and private.get("manifest_sha256") == manifest["manifest_sha256"]
        and private.get("status") == "QUALIFICATION_COMPLETE"
        and private.get("target_outcomes_observed_or_derived") is False
        and not result_path.exists()
    ):
        raise ChallengerRetestQualificationInspectionError(
            "private qualification result is incomplete"
        )
    split_actions = load_split_actions(
        qualification._source_splits_path(store.root)
    )
    rebuilt = qualification.evaluate_all(
        pairs=pairs, states=states, split_actions=split_actions
    )
    if qualification.base._canonical_bytes(rebuilt) != qualification.base._canonical_bytes(
        observed
    ):
        raise ChallengerRetestQualificationInspectionError(
            "independent per-pair qualification rebuild differs"
        )
    summary = qualification.aggregate(rebuilt)
    for name, value in summary.items():
        if private.get(name) != value:
            raise ChallengerRetestQualificationInspectionError(
                f"private qualification aggregate drifted at {name}"
            )
    terminal = Counter(str(row["terminal_reason"]) for row in rebuilt)
    selected_dates = [str(row["date"]) for row in rebuilt if row.get("selected") is True]
    if not (
        sum(terminal.values()) == qualification.EXPECTED_PAIRS
        and len(selected_dates) == len(set(selected_dates))
        and len(selected_dates) == summary["eligible_signals"]
    ):
        raise ChallengerRetestQualificationInspectionError(
            "one-per-session result denominator differs"
        )
    passed = summary["minimum_capacity_passed"] is True
    result = {
        "schema_version": 1,
        "dataset_id": qualification.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "QUALIFICATION_INSPECTED" if passed else "INSUFFICIENT_CAPACITY",
        "inspected": True,
        "pairs_evaluated": len(rebuilt),
        "distinct_trigger_sessions": qualification.EXPECTED_DATES,
        **summary,
        "outcome_contract_permitted": passed,
        "next_phase": (
            "FREEZE_OUTCOME_CONTRACT"
            if passed
            else "RETIRE_INSUFFICIENT_CAPACITY"
        ),
        "private_result_sha256": qualification.base._sha256_file(private_path),
        "inspection": {
            "source_pair_hashes_rebuilt": True,
            "per_pair_gate_records_rebuilt": True,
            "terminal_precedence_rebuilt": True,
            "one_per_session_ranking_rebuilt": True,
            "aggregate_counts_and_cascade_rebuilt": True,
            "privacy_and_outcome_locks_rechecked": True,
            "valid": True,
        },
        "symbols_dates_instrument_ids_raw_rows_and_gate_records_public": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    qualification.base._write_json(result_path, result)
    qualification.base._write_json(status_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    contract = sub.add_parser("inspect-contract")
    contract.add_argument("manifest", type=Path)
    result = sub.add_parser("inspect")
    result.add_argument("manifest", type=Path)
    parser.add_argument("--env", type=Path, default=qualification.PROJECT_ROOT / ".env")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            value = inspect_contract(
                manifest_path=args.manifest,
                env_path=args.env,
                status_path=qualification.DEFAULT_CONTRACT_STATUS,
            )
        else:
            value = inspect_result(
                manifest_path=args.manifest,
                env_path=args.env,
                status_path=qualification.DEFAULT_QUALIFICATION_STATUS,
                result_path=qualification.DEFAULT_RESULT,
            )
    except (
        ChallengerRetestQualificationInspectionError,
        qualification.ChallengerRetestQualificationError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
