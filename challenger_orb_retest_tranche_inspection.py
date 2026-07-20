"""Independently rebuild the frozen challenger date selection."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_tranche as tranche
import development_tranche as base
from historical_store import HistoricalDayStore
from learning_data import LearningDataError, load_frozen_dataset_contract


class ChallengerTrancheInspectionError(RuntimeError):
    """Independent date selection reconstruction found a mismatch."""


def inspect(*, manifest_path: Path, env_path: Path) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != tranche.DATASET_ID:
        raise ChallengerTrancheInspectionError("selection dataset identity differs")
    for value in manifest["implementation_contract"].values():
        tranche._verify_binding(value)
    store = HistoricalDayStore.from_env(env_path)
    private_path = tranche._private_path(store.root)
    contract = manifest["selection_contract"]
    if tranche._sha256_file(private_path) != contract.get("private_exclusion_sha256"):
        raise ChallengerTrancheInspectionError("private exclusions changed")
    if tranche._sha256_file(tranche.DEFAULT_SELECTION) != contract.get("public_selection_sha256"):
        raise ChallengerTrancheInspectionError("public selection changed")
    exclusions = base._read_gzip(private_path)
    selection = tranche._read_json(tranche.DEFAULT_SELECTION)
    current = tranche._exclusions()
    comparisons = {
        "signals": (exclusions["signal_ledger"]["sha256"], current["signal_ledger"]["sha256"]),
        "archives": (exclusions["archived_contexts"]["artifact_set_sha256"], current["archived_contexts"]["artifact_set_sha256"]),
        "prior selections": (tranche._sha256_json(exclusions["prior_selections"]), tranche._sha256_json(current["prior_selections"])),
        "inspected entities": (exclusions["registered_inspected_evidence"]["inspected_entity_set_sha256"], current["registered_inspected_evidence"]["inspected_entity_set_sha256"]),
        "inspected artifacts": (exclusions["registered_inspected_evidence"]["artifact_set_sha256"], current["registered_inspected_evidence"]["artifact_set_sha256"]),
        "excluded dates": (exclusions["excluded_dates_sha256"], current["excluded_dates_sha256"]),
    }
    for label, (frozen, observed) in comparisons.items():
        if frozen != observed:
            raise ChallengerTrancheInspectionError(f"{label} changed after freeze")
    rebuilt, required = tranche.build_selection(
        calendar_dates=tranche._calendar_dates(),
        exclusion_snapshot=exclusions,
        seed=int(selection["seed"]),
        target_count=tranche.TARGET_COUNT,
    )
    if rebuilt != selection:
        raise ChallengerTrancheInspectionError("date selection does not rebuild")
    if set(selection["selected_dates"]) & set(exclusions["excluded_dates"]):
        raise ChallengerTrancheInspectionError("selection overlaps excluded dates")
    if sorted(selection["selected_dates"]) != manifest.get("requested_dates"):
        raise ChallengerTrancheInspectionError("manifest date set differs")
    result = {
        "schema_version": 1,
        "dataset_id": tranche.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "selected_dates": len(selection["selected_dates"]),
        "eligible_dates": selection["eligible_date_count"],
        "excluded_dates": selection["excluded_date_count"],
        "required_sessions": len(required),
        "disjoint": True,
        "selection_rebuilt": True,
        "date_substitution_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    tranche._write_json(tranche.DEFAULT_STATUS, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env", type=Path, default=tranche.PROJECT_ROOT / ".env")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect(manifest_path=args.manifest, env_path=args.env)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerTrancheInspectionError,
        tranche.ChallengerTrancheError,
        base.DevelopmentTrancheError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
