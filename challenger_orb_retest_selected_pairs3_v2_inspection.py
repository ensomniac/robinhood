"""Independently inspect the repaired third ORB-retest tranche's selected pairs."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
from collections.abc import Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path

import challenger_orb_retest_selected_pairs3_v2 as tranche3
import challenger_orb_retest_selected_pairs_inspection as base
from historical_store import HistoricalStoreError
from learning_data import LearningDataError
from learning_experiment import LearningExperimentError


PROJECT_ROOT = Path(__file__).resolve().parent


def _verify_contract_bindings_v2(
    manifest: Mapping[str, object], *, require_published: bool
) -> None:
    """Verify pinned inputs at their pushed pre-freeze commits.

    The base inspector requires every input's recorded publication commit to
    equal the later manifest-publication HEAD.  That cannot hold when the
    implementation is committed before freeze and the manifest is committed
    before inspection.  This isolated repair retains every byte/path check and
    instead proves each recorded commit is a pushed ancestor of the clean
    inspection HEAD and contains the exact bound blob.
    """

    source_bindings = manifest.get("source_bindings")
    implementations = manifest.get("implementation_contract")
    publication = manifest.get("publication_contract")
    expected_sources = tranche3.source_paths()
    expected_implementations = tranche3.implementation_paths()
    if (
        not isinstance(source_bindings, Mapping)
        or set(source_bindings) != set(expected_sources)
        or not isinstance(implementations, Mapping)
        or set(implementations) != set(expected_implementations)
        or not isinstance(publication, Mapping)
    ):
        raise base.ChallengerSelectedPairsInspectionError(
            "contract bindings are incomplete"
        )
    for name, path in expected_sources.items():
        base._verify_binding(source_bindings[name], path)
    for name, path in expected_implementations.items():
        base._verify_binding(implementations[name], path)

    all_bindings = {**source_bindings, **implementations}
    if not require_published:
        if publication:
            raise base.ChallengerSelectedPairsInspectionError(
                "test contract unexpectedly claims publication"
            )
        return
    if set(publication) != set(all_bindings):
        raise base.ChallengerSelectedPairsInspectionError(
            "publication surface is incomplete"
        )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    upstream = subprocess.run(
        ["git", "rev-parse", "@{upstream}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != upstream:
        raise base.ChallengerSelectedPairsInspectionError(
            "inspection requires HEAD to equal its pushed upstream"
        )
    for name, binding in all_bindings.items():
        value = publication[name]
        if not isinstance(value, Mapping):
            raise base.ChallengerSelectedPairsInspectionError(
                f"publication binding differs for {name}"
            )
        commit = str(value.get("commit") or "")
        path = str(value.get("path") or "")
        expected_path = str(binding.get("path") or "")
        expected_hash = str(binding.get("sha256") or "")
        if (
            not re.fullmatch(r"[0-9a-f]{40}", commit)
            or path != expected_path
            or value.get("sha256") != expected_hash
            or subprocess.run(
                ["git", "merge-base", "--is-ancestor", commit, head],
                cwd=PROJECT_ROOT,
                check=False,
                capture_output=True,
            ).returncode
            != 0
        ):
            raise base.ChallengerSelectedPairsInspectionError(
                f"publication binding differs for {name}"
            )
        historical = subprocess.run(
            ["git", "show", f"{commit}:{path}"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
        ).stdout
        if hashlib.sha256(historical).hexdigest() != expected_hash:
            raise base.ChallengerSelectedPairsInspectionError(
                f"published blob differs for {name}"
            )
        status = subprocess.run(
            ["git", "status", "--porcelain", "--", path],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        if status.strip():
            raise base.ChallengerSelectedPairsInspectionError(
                f"published input is dirty: {path}"
            )


@contextmanager
def configured_base():
    """Scope tranche-three identities and binding surfaces around the base inspector."""

    values = {
        "DATASET_ID": tranche3.DATASET_ID,
        "PREENTRY_DATASET_ID": tranche3.PREENTRY_DATASET_ID,
        "SCANNER_DATASET_ID": tranche3.SCANNER_DATASET_ID,
        "EXPECTED_SELECTED_PAIRS": tranche3.EXPECTED_SELECTED_PAIRS,
        "EXPECTED_DATES": tranche3.EXPECTED_DATES,
        "MINIMUM_FREE_BYTES": tranche3.MINIMUM_FREE_BYTES,
        "SCANNER_MANIFEST_SHA256": tranche3.SCANNER_MANIFEST_SHA256,
        "OUTER_MANIFEST_SHA256": tranche3.OUTER_MANIFEST_SHA256,
        "HYPOTHESIS_SHA256": tranche3.HYPOTHESIS_SHA256,
        "PRIMARY_TRIAL_ID": tranche3.PRIMARY_TRIAL_ID,
        "DEFAULT_SUMMARY": tranche3.DEFAULT_SUMMARY,
        "DEFAULT_INSPECTION": tranche3.DEFAULT_INSPECTION,
        "DEFAULT_SCANNER_MANIFEST": tranche3.DEFAULT_SCANNER_MANIFEST,
        "DEFAULT_OUTER_MANIFEST": tranche3.DEFAULT_OUTER_MANIFEST,
        "DEFAULT_HYPOTHESIS": tranche3.DEFAULT_HYPOTHESIS,
        "DEFAULT_MANIFEST_ROOT": tranche3.DEFAULT_OUTPUT_ROOT,
        "DEFAULT_STATUS": tranche3.DEFAULT_STATUS,
        "_expected_source_paths": tranche3.source_paths,
        "_expected_implementation_paths": tranche3.implementation_paths,
        "_verify_contract_bindings": _verify_contract_bindings_v2,
    }
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        yield base
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def default_manifest() -> Path:
    matches = sorted(tranche3.DEFAULT_OUTPUT_ROOT.glob(f"{tranche3.DATASET_ID}-*.json"))
    if len(matches) != 1:
        raise base.ChallengerSelectedPairsInspectionError(
            "exactly one third-tranche selected-pair manifest is required"
        )
    return matches[0]


def inspect_selected_pairs(
    *,
    manifest_path: Path,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = tranche3.DEFAULT_STATUS,
    require_published: bool = True,
) -> dict[str, object]:
    with configured_base():
        return base.inspect_selected_pairs(
            manifest_path=manifest_path,
            env_path=env_path,
            status_path=status_path,
            require_published=require_published,
        )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, nargs="?")
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=tranche3.DEFAULT_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_selected_pairs(
            manifest_path=args.manifest or default_manifest(),
            env_path=args.env,
            status_path=args.status,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        base.ChallengerSelectedPairsInspectionError,
        HistoricalStoreError,
        LearningDataError,
        LearningExperimentError,
        OSError,
        subprocess.SubprocessError,
        TypeError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
