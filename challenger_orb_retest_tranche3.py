"""Freeze the challenger's third exact disjoint 100-session tranche."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_tranche2 as base
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "9ec8e778b67bd4631c974b06ecd3318420b839dfe57f151cd50c970d297f1428"
)
DATASET_ID = "dataset-challenger-orb-retest-development-2026-07-22-v3"
SEED = 2026072201
TARGET_COUNT = 100
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_tranche3_inspection.py"
PRIOR_SELECTIONS = (*base.PRIOR_SELECTIONS, base.DEFAULT_SELECTION)
ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche3"
DEFAULT_SELECTION = ROOT / "selection-2026-07-22-100-days-v3.json"
DEFAULT_MANIFEST_ROOT = ROOT / "selection_manifests"
DEFAULT_STATUS = ROOT / "selection-status.json"
PRIVATE_NAMESPACE = "challenger_orb_retest_v1_tranche3"


class ChallengerTranche3Error(RuntimeError):
    """The third-tranche adapter or its frozen base has drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _private_path(store_root: Path) -> Path:
    return store_root / "_derived" / PRIVATE_NAMESPACE / DATASET_ID / "exclusions.json.gz"


def _exclusions() -> dict[str, Any]:
    return base.base.build_exclusion_snapshot(
        prior_selection_paths=PRIOR_SELECTIONS,
        inspected_evidence=base._registered_inspected_exclusions(),
    )


def configure_base() -> None:
    """Install third-tranche identities into the hash-pinned selector."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerTranche3Error("frozen second-tranche selector drifted")
    values: dict[str, Any] = {
        "DATASET_ID": DATASET_ID,
        "SEED": SEED,
        "TARGET_COUNT": TARGET_COUNT,
        "INSPECTOR": INSPECTOR,
        "PRIOR_SELECTIONS": PRIOR_SELECTIONS,
        "ROOT": ROOT,
        "DEFAULT_SELECTION": DEFAULT_SELECTION,
        "DEFAULT_MANIFEST_ROOT": DEFAULT_MANIFEST_ROOT,
        "DEFAULT_STATUS": DEFAULT_STATUS,
        "PRIVATE_NAMESPACE": PRIVATE_NAMESPACE,
        "_private_path": _private_path,
        "_exclusions": _exclusions,
        "__file__": str(Path(__file__).resolve()),
    }
    for name, value in values.items():
        setattr(base, name, value)


def preflight() -> dict[str, Any]:
    configure_base()
    exclusions = _exclusions()
    selection, required = base.build_selection(
        calendar_dates=base._calendar_dates(),
        exclusion_snapshot=exclusions,
        seed=SEED,
        target_count=TARGET_COUNT,
    )
    return {
        "dataset_id": DATASET_ID,
        "selected_dates": len(selection["selected_dates"]),
        "eligible_dates": selection["eligible_date_count"],
        "excluded_dates": selection["excluded_date_count"],
        "required_sessions": len(required),
        "selected_dates_sha256": selection["selected_dates_sha256"],
        "required_sessions_sha256": selection["required_sessions_sha256"],
        "target_outcomes_observed_or_derived": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight", "freeze"))
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "preflight":
            result = preflight()
        else:
            configure_base()
            path, manifest = base.freeze(
                env_path=args.env, output_root=DEFAULT_MANIFEST_ROOT
            )
            selection = base._read_json(DEFAULT_SELECTION)
            result = {
                "schema_version": 1,
                "dataset_id": DATASET_ID,
                "manifest_sha256": manifest["manifest_sha256"],
                "path": base._repo_path(path),
                "status": "FROZEN_READY",
                "inspected": False,
                "selected_dates": TARGET_COUNT,
                "eligible_dates": selection["eligible_date_count"],
                "excluded_dates": selection["excluded_date_count"],
                "required_sessions": selection["required_session_count"],
                "date_substitution_allowed": False,
                "target_outcomes_observed_or_derived": False,
            }
            base._write_json(DEFAULT_STATUS, result)
    except (
        ChallengerTranche3Error,
        base.ChallengerTranche2Error,
        base.prior.ChallengerTrancheError,
        base.base.DevelopmentTrancheError,
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
