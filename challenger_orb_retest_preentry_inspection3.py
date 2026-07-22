"""Independently inspect third-tranche pre-entry contracts and triggers."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Sequence
from pathlib import Path

import challenger_orb_retest_preentry3 as challenger
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


class ChallengerPreentryInspection3Error(RuntimeError):
    """The independently reconstructed third-tranche evidence differs."""


def inspect_contract(*, manifest_path: Path, env_path: Path) -> dict:
    challenger.configure_base()
    preentry = challenger.base
    manifest = load_frozen_dataset_contract(manifest_path)
    config = HistoricalStoreConfig.from_env(env_path)
    selection = preentry._read_gzip(preentry._selection_path(config.root))
    rebuilt = challenger.build_selection(config.root)
    contract = manifest.get("selection_contract", {})
    if not (
        manifest.get("dataset_id") == challenger.DATASET_ID
        and rebuilt == selection
        and preentry._sha256_file(preentry._selection_path(config.root))
        == contract.get("private_selection_sha256")
        and rebuilt["pair_identity_sha256"] == contract.get("pair_identity_sha256")
        and rebuilt["request_graph_sha256"] == contract.get("request_graph_sha256")
        and rebuilt["counts"]["verified_positive_pairs"]
        == challenger.EXPECTED_POSITIVE_PAIRS
        and rebuilt["counts"]["verified_positive_dates"]
        == challenger.EXPECTED_POSITIVE_DATES
        and rebuilt["counts"]["total_requests"]
        == challenger.EXPECTED_TOTAL_REQUESTS
    ):
        raise ChallengerPreentryInspection3Error(
            "third-tranche pre-entry selection does not rebuild"
        )
    for value in manifest.get("implementation_contract", {}).values():
        path = challenger.PROJECT_ROOT / str(value["path"])
        if preentry._sha256_file(path) != value.get("sha256"):
            raise ChallengerPreentryInspection3Error(
                "third-tranche implementation binding differs"
            )
    root = preentry._private_root(config.root)
    artifacts = {
        "frozen_selections": int(preentry._selection_path(config.root).is_file()),
        "terminal_wrappers": (
            len(list((root / "wrappers").glob("*.json.gz")))
            if (root / "wrappers").exists()
            else 0
        ),
        "collection_indexes": int(preentry._index_path(config.root).is_file()),
        "trigger_indexes": int(preentry._trigger_path(config.root).is_file()),
    }
    if artifacts != {
        "frozen_selections": 1,
        "terminal_wrappers": 0,
        "collection_indexes": 0,
        "trigger_indexes": 0,
    }:
        raise ChallengerPreentryInspection3Error(
            "third-tranche pre-entry zero state differs"
        )
    if shutil.disk_usage(config.root).free < config.min_free_bytes:
        raise ChallengerPreentryInspection3Error("historical reserve is unavailable")
    return {
        "schema_version": 1,
        "dataset_id": challenger.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "counts": rebuilt["counts"],
        "pair_identity_sha256": rebuilt["pair_identity_sha256"],
        "request_graph_sha256": rebuilt["request_graph_sha256"],
        "private_artifact_counts": artifacts,
        "outcome_lock_verified": True,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }


def inspect_result(
    *, manifest_path: Path, env_path: Path, result_path: Path
) -> dict:
    challenger.configure_base()
    preentry = challenger.base
    manifest, config, selection = preentry._load_contract(
        manifest_path=manifest_path, env_path=env_path, require_published=False
    )
    private_path = preentry._trigger_path(config.root)
    recorded = preentry._read_gzip(private_path)
    rebuilt = preentry.build_trigger_index(manifest, selection, config.root)
    if rebuilt != recorded:
        raise ChallengerPreentryInspection3Error(
            "third-tranche trigger result differs"
        )
    counts = recorded["counts"]
    result = {
        "schema_version": 1,
        "dataset_id": challenger.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": counts,
        "terminal_reason_counts": recorded["terminal_reason_counts"],
        "private_result_sha256": preentry._sha256_file(private_path),
        "minimum_development_signal_capacity_passed": (
            counts["maximum_daily_closed_signals"]
            >= counts["minimum_required_development_signals"]
        ),
        "inspection": {
            "selection_rebuilt": True,
            "collection_index_rebuilt": True,
            "canonical_rows_rehashed": True,
            "causal_boundaries_rebuilt": True,
            "trigger_results_rebuilt": True,
            "complete_pair_denominator_reconciled": True,
        },
        "next_phase": (
            "FREEZE_OUTCOME_CONTRACT"
            if counts["maximum_daily_closed_signals"]
            >= counts["minimum_required_development_signals"]
            else "DEVELOPMENT_ACQUISITION"
        ),
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "production_rule_change_earned": False,
        "valid": True,
    }
    preentry._write_json(result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-contract", "inspect-result"))
    parser.add_argument("manifest", type=Path)
    parser.add_argument(
        "--env-file", type=Path, default=challenger.PROJECT_ROOT / ".env"
    )
    parser.add_argument("--result", type=Path, default=challenger.DEFAULT_PUBLIC_RESULT)
    parser.add_argument("--status", type=Path, default=challenger.DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            value = inspect_contract(
                manifest_path=args.manifest, env_path=args.env_file
            )
            challenger.base._write_json(args.status, value)
        else:
            value = inspect_result(
                manifest_path=args.manifest,
                env_path=args.env_file,
                result_path=args.result,
            )
    except (
        ChallengerPreentryInspection3Error,
        challenger.ChallengerPreentry3Error,
        challenger.base.ChallengerPreentryError,
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
