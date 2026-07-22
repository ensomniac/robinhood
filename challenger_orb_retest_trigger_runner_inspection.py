"""Independently inspect the challenger trigger runner and derived result."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import challenger_orb_retest_preentry as preentry
import challenger_orb_retest_preentry_inspection as preentry_inspection
import challenger_orb_retest_trigger_runner as runner
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


class ChallengerTriggerRunnerInspectionError(RuntimeError):
    """The trigger runner contract or result does not independently rebuild."""


def inspect_contract(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    manifest, config, selection = runner.load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=False,
    )
    if preentry._trigger_path(config.root).exists():
        raise ChallengerTriggerRunnerInspectionError(
            "trigger artifact exists before runner inspection"
        )
    compatibility = manifest.get("compatibility_contract", {})
    if not (
        compatibility.get("only_runtime_substitution")
        == "preentry.LocalHistoricalClient"
        and compatibility.get("replacement") == "FrozenCausalWindowClient"
        and compatibility.get("exact_request_provenance_required") is True
        and compatibility.get("trigger_rule_change_allowed") is False
        and selection["counts"]["total_requests"] == 284
        and selection["counts"]["verified_positive_pairs"] == 95
        and selection["counts"]["verified_positive_dates"] == 47
    ):
        raise ChallengerTriggerRunnerInspectionError(
            "trigger compatibility contract differs"
        )
    result = {
        "schema_version": 1,
        "dataset_id": runner.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "source_preentry_manifest_sha256": manifest["source_contract"][
            "preentry_manifest_sha256"
        ],
        "status": "FROZEN_READY",
        "inspected": True,
        "counts": selection["counts"],
        "implementation_contract": manifest["implementation_contract"],
        "source_contract": manifest["source_contract"],
        "trigger_contract": manifest["trigger_contract"],
        "trigger_artifacts_present": False,
        "outcome_lock_verified": True,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    preentry._write_json(status_path, result)
    return result


def inspect_result(
    *, manifest_path: Path, env_path: Path, status_path: Path, result_path: Path
) -> dict[str, Any]:
    manifest, config, _selection = runner.load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=False,
    )
    status = preentry._read_json(status_path)
    private_path = preentry._trigger_path(config.root)
    if not (
        status.get("manifest_sha256") == manifest["manifest_sha256"]
        and status.get("status") == "TRIGGER_REVIEW_COMPLETE_UNINSPECTED"
        and status.get("private_trigger_sha256") == preentry._sha256_file(private_path)
        and status.get("post_entry_data_accessed") is False
        and status.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerTriggerRunnerInspectionError(
            "uninspected trigger status differs"
        )
    with runner.using_exact_reader():
        result = preentry_inspection.inspect_result(
            manifest_path=runner.SOURCE_MANIFEST,
            env_path=env_path,
            result_path=result_path,
        )
    result.update(
        {
            "trigger_runner_dataset_id": runner.DATASET_ID,
            "trigger_runner_manifest_sha256": manifest["manifest_sha256"],
            "compatibility_contract": manifest["compatibility_contract"],
        }
    )
    preentry._write_json(result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-contract", "inspect-result"))
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=runner.PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=runner.DEFAULT_STATUS)
    parser.add_argument("--result", type=Path, default=preentry.DEFAULT_PUBLIC_RESULT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            value = inspect_contract(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.status,
            )
        else:
            value = inspect_result(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.status,
                result_path=args.result,
            )
    except (
        ChallengerTriggerRunnerInspectionError,
        runner.ChallengerTriggerRunnerError,
        preentry.ChallengerPreentryError,
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
