"""Independently inspect the three-corpus ORB-retest capacity manifest."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import challenger_orb_retest_cumulative_capacity as challenger
from historical_store import HistoricalStoreConfig, HistoricalStoreError


class ChallengerCumulativeCapacityInspectionError(RuntimeError):
    """The frozen cumulative capacity snapshot does not rebuild exactly."""


def inspect(
    *,
    manifest_path: Path,
    env_path: Path,
    status_path: Path,
    result_path: Path,
) -> dict[str, Any]:
    manifest = challenger.load_manifest(manifest_path)
    status = challenger._read_json(status_path)
    if not (
        status.get("manifest_sha256") == manifest["manifest_sha256"]
        and status.get("status") == "FROZEN_AWAITING_INSPECTION"
        and status.get("inspected") is False
        and status.get("post_entry_data_accessed") is False
        and status.get("target_outcomes_observed_or_derived") is False
        and not result_path.exists()
    ):
        raise ChallengerCumulativeCapacityInspectionError(
            "cumulative capacity zero-result state differs"
        )
    for value in manifest.get("implementation_contract", {}).values():
        path = challenger.PROJECT_ROOT / str(value["path"])
        if challenger._sha256_file(path) != value.get("sha256"):
            raise ChallengerCumulativeCapacityInspectionError(
                "cumulative capacity implementation binding differs"
            )
    config = HistoricalStoreConfig.from_env(env_path)
    rebuilt, trigger_sets, corpus_sets = challenger.build_capacity_state(config.root)
    if rebuilt != manifest.get("capacity_snapshot"):
        raise ChallengerCumulativeCapacityInspectionError(
            "cumulative capacity snapshot differs"
        )
    trigger_intersections = challenger._pairwise_counts(trigger_sets)
    corpus_intersections = challenger._pairwise_counts(corpus_sets)
    if not (
        trigger_intersections == [0, 0, 0]
        and corpus_intersections == [0, 0, 0]
        and rebuilt["cumulative_distinct_trigger_sessions"] == 52
        and rebuilt["minimum_required_development_signals"] == 50
        and rebuilt["minimum_development_signal_capacity_passed"] is True
        and rebuilt["post_entry_data_accessed"] is False
        and rebuilt["target_outcomes_observed_or_derived"] is False
    ):
        raise ChallengerCumulativeCapacityInspectionError(
            "cumulative causal capacity gate does not pass"
        )
    result = {
        "schema_version": 1,
        "dataset_id": challenger.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": {
            "source_corpora": rebuilt["source_corpus_count"],
            "source_trigger_session_counts": rebuilt[
                "source_trigger_session_counts"
            ],
            "source_trigger_pair_counts": rebuilt["source_trigger_pair_counts"],
            "cumulative_distinct_trigger_sessions": rebuilt[
                "cumulative_distinct_trigger_sessions"
            ],
            "cumulative_trigger_pairs": rebuilt["cumulative_trigger_pairs"],
            "minimum_required_development_signals": rebuilt[
                "minimum_required_development_signals"
            ],
        },
        "pairwise_source_session_intersections": corpus_intersections,
        "pairwise_trigger_session_intersections": trigger_intersections,
        "private_trigger_session_union_sha256": rebuilt[
            "private_trigger_session_union_sha256"
        ],
        "minimum_development_signal_capacity_passed": True,
        "next_phase": "FREEZE_OUTCOME_CONTRACT",
        "inspection": {
            "all_public_results_rehashed": True,
            "all_private_trigger_indexes_rehashed": True,
            "all_terminal_records_reloaded": True,
            "source_corpora_pairwise_disjoint": True,
            "trigger_sessions_pairwise_disjoint": True,
            "one_signal_per_session_capacity_rebuilt": True,
        },
        "exact_dates_symbols_and_instrument_ids_public": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "production_rule_change_earned": False,
        "valid": True,
    }
    challenger._write_json(result_path, result)
    challenger._write_json(status_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument(
        "--env-file", type=Path, default=challenger.PROJECT_ROOT / ".env"
    )
    parser.add_argument("--status", type=Path, default=challenger.DEFAULT_STATUS)
    parser.add_argument("--result", type=Path, default=challenger.DEFAULT_RESULT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        value = inspect(
            manifest_path=args.manifest,
            env_path=args.env_file,
            status_path=args.status,
            result_path=args.result,
        )
    except (
        ChallengerCumulativeCapacityInspectionError,
        challenger.ChallengerCumulativeCapacityError,
        HistoricalStoreError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
