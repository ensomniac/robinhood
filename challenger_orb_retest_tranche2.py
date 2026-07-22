"""Freeze the challenger's second exact disjoint 100-session tranche."""

from __future__ import annotations

import argparse
import json
import random
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest as retest
import challenger_orb_retest_tranche as prior
import development_non_return_collection as publication
import development_tranche as base
from historical_store import HistoricalDayStore
from learning_data import LearningDataError, freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-challenger-orb-retest-development-2026-07-21-v2"
SEED = 2026072106
TARGET_COUNT = 100
PRIOR_SESSIONS = prior.PRIOR_SESSIONS
ELIGIBLE_START = prior.ELIGIBLE_START
ELIGIBLE_END = prior.ELIGIBLE_END
CALENDAR = prior.CALENDAR
CALENDAR_SOURCE = prior.CALENDAR_SOURCE
CALENDAR_STATUS = prior.CALENDAR_STATUS
QUALIFICATION_RESULT = prior.QUALIFICATION_RESULT
HYPOTHESIS = prior.HYPOTHESIS
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_tranche2_inspection.py"
PRIOR_SELECTIONS = (*prior.PRIOR_SELECTIONS, prior.DEFAULT_SELECTION)
ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche2"
DEFAULT_SELECTION = ROOT / "selection-2026-07-21-100-days-v2.json"
DEFAULT_MANIFEST_ROOT = ROOT / "selection_manifests"
DEFAULT_STATUS = ROOT / "selection-status.json"
PRIVATE_NAMESPACE = "challenger_orb_retest_v1_tranche2"


class ChallengerTranche2Error(RuntimeError):
    """The second challenger selection is not exact, disjoint, or immutable."""


def _sha256_json(value: Any) -> str:
    return prior._sha256_json(value)


def _sha256_file(path: Path) -> str:
    return prior._sha256_file(path)


def _repo_path(path: Path) -> str:
    return prior._repo_path(path)


def _write_json(path: Path, value: Any) -> None:
    prior._write_json(path, value)


def _read_json(path: Path) -> dict[str, Any]:
    return prior._read_json(path)


def _binding(path: Path) -> dict[str, str]:
    return prior._binding(path)


def _verify_binding(value: Mapping[str, Any]) -> None:
    prior._verify_binding(value)


def _calendar_dates() -> list[str]:
    return prior._calendar_dates()


def _validate_prerequisites() -> None:
    prior._validate_prerequisites()


def _private_path(store_root: Path) -> Path:
    return store_root / "_derived" / PRIVATE_NAMESPACE / DATASET_ID / "exclusions.json.gz"


def _registered_inspected_exclusions():
    return prior._registered_inspected_exclusions()


def _exclusions() -> dict[str, Any]:
    return base.build_exclusion_snapshot(
        prior_selection_paths=PRIOR_SELECTIONS,
        inspected_evidence=_registered_inspected_exclusions(),
    )


def build_selection(
    *,
    calendar_dates: Sequence[str],
    exclusion_snapshot: Mapping[str, Any],
    seed: int = SEED,
    target_count: int = TARGET_COUNT,
) -> tuple[dict[str, Any], list[str]]:
    normalized = [date.fromisoformat(value).isoformat() for value in calendar_dates]
    if normalized != sorted(set(normalized)):
        raise ChallengerTranche2Error("calendar dates must be unique and chronological")
    excluded = set(exclusion_snapshot.get("excluded_dates", []))
    eligible = [
        value
        for index, value in enumerate(normalized)
        if ELIGIBLE_START <= value <= ELIGIBLE_END
        and value not in excluded
        and index >= PRIOR_SESSIONS
    ]
    if len(eligible) < target_count:
        raise ChallengerTranche2Error(
            f"eligible disjoint pool has {len(eligible)} dates, needs {target_count}"
        )
    selected = random.Random(seed).sample(eligible, target_count)
    if set(selected) & excluded:
        raise ChallengerTranche2Error("selected tranche overlaps excluded dates")
    required = base.required_sessions(
        selected, normalized, prior_sessions=PRIOR_SESSIONS
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "hypothesis_sha256": retest.HYPOTHESIS_SHA256,
        "seed": seed,
        "selection_algorithm": f"random.Random(seed).sample(eligible_dates, {target_count})",
        "eligible_start": ELIGIBLE_START,
        "eligible_end": ELIGIBLE_END,
        "eligible_date_count": len(eligible),
        "eligible_dates_sha256": _sha256_json(eligible),
        "excluded_date_count": len(excluded),
        "excluded_dates_sha256": exclusion_snapshot["excluded_dates_sha256"],
        "selected_dates": selected,
        "selected_dates_sha256": _sha256_json(selected),
        "required_prior_sessions": PRIOR_SESSIONS,
        "required_session_count": len(required),
        "required_sessions_sha256": _sha256_json(required),
        "substitution_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }, required


def _expected_contract(
    *,
    exclusions: Mapping[str, Any],
    selection: Mapping[str, Any],
    required_sessions_count: int,
    store: HistoricalDayStore,
    private_path: Path,
) -> dict[str, Any]:
    return {
        "implementation_contract": {
            "selector": _binding(Path(__file__)),
            "inspector": _binding(INSPECTOR),
            "prior_selector": _binding(Path(prior.__file__)),
            "base_selector": _binding(Path(base.__file__)),
            "trigger": _binding(Path(retest.__file__)),
            "hypothesis": _binding(HYPOTHESIS),
        },
        "selection_contract": {
            "calendar": _binding(CALENDAR),
            "calendar_source": _binding(CALENDAR_SOURCE),
            "calendar_status": _binding(CALENDAR_STATUS),
            "v3_falsification": _binding(QUALIFICATION_RESULT),
            "signals_sha256": exclusions["signal_ledger"]["sha256"],
            "archived_context_artifact_set_sha256": exclusions["archived_contexts"]["artifact_set_sha256"],
            "inspected_evidence_artifact_set_sha256": exclusions["registered_inspected_evidence"]["artifact_set_sha256"],
            "inspected_entity_set_sha256": exclusions["registered_inspected_evidence"]["inspected_entity_set_sha256"],
            "prior_selection_artifacts": exclusions["prior_selections"],
            "private_exclusion_sha256": _sha256_file(private_path),
            "public_selection_sha256": _sha256_file(DEFAULT_SELECTION),
            "excluded_date_count": selection["excluded_date_count"],
            "excluded_dates_sha256": selection["excluded_dates_sha256"],
            "eligible_start": ELIGIBLE_START,
            "eligible_end": ELIGIBLE_END,
            "eligible_date_count": selection["eligible_date_count"],
            "eligible_dates_sha256": selection["eligible_dates_sha256"],
            "seed": SEED,
            "selected_date_count": TARGET_COUNT,
            "selected_dates_sha256": selection["selected_dates_sha256"],
            "required_prior_sessions": PRIOR_SESSIONS,
            "required_session_count": selection["required_session_count"],
            "required_sessions_sha256": selection["required_sessions_sha256"],
            "disjoint_from_every_frozen_exclusion": True,
            "date_substitution_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
        "acquisition_contract": {
            "reference_identity_provider": "Massive dated common-stock snapshots",
            "full_universe_coarse_market_provider": "local exact-contract cache, then whole-session Alpaca SIP",
            "selected_symbol_provider_priority": ["local canonical cache", "IBKR", "Massive", "Alpaca"],
            "whole_provider_fidelity_per_symbol_session": True,
            "full_universe_detail_forbidden": True,
            "selected_symbols_only_minute_tape_quote_detail": True,
            "point_in_time_security_master_required_before_market_freeze": True,
            "split_actions_required_before_market_freeze": True,
            "primary_catalyst_source_rules_required_before_market_freeze": True,
            "retest_trigger_implementation_required_before_market_freeze": True,
            "zero_substitution_policy_required_before_market_freeze": True,
            "historical_store_reserve_enforced": True,
            "capacity_projection": base._capacity_projection(
                required_session_count=required_sessions_count, store=store
            ),
            "target_outcomes_observed_or_derived": False,
        },
    }


def freeze(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    publication._published(Path(__file__))
    publication._published(INSPECTOR)
    _validate_prerequisites()
    store = HistoricalDayStore.from_env(env_path)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise ChallengerTranche2Error("historical store reserve is not ready")
    exclusions = _exclusions()
    selection, required = build_selection(
        calendar_dates=_calendar_dates(), exclusion_snapshot=exclusions
    )
    _write_json(DEFAULT_SELECTION, selection)
    private_path = _private_path(store.root)
    base._write_gzip(private_path, exclusions)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": sorted(selection["selected_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(HYPOTHESIS),
                _repo_path(CALENDAR_STATUS),
                _repo_path(QUALIFICATION_RESULT),
                _repo_path(DEFAULT_SELECTION),
                "CHALLENGER_ORB_RETEST.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        **_expected_contract(
            exclusions=exclusions,
            selection=selection,
            required_sessions_count=len(required),
            store=store,
            private_path=private_path,
        ),
    }
    return freeze_dataset_contract(contract, output_root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("command", choices=("freeze",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, manifest = freeze(env_path=args.env, output_root=DEFAULT_MANIFEST_ROOT)
        selection = _read_json(DEFAULT_SELECTION)
        result = {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "path": _repo_path(path),
            "status": "FROZEN_READY",
            "inspected": False,
            "selected_dates": TARGET_COUNT,
            "eligible_dates": selection["eligible_date_count"],
            "excluded_dates": selection["excluded_date_count"],
            "required_sessions": selection["required_session_count"],
            "date_substitution_allowed": False,
            "target_outcomes_observed_or_derived": False,
        }
        _write_json(DEFAULT_STATUS, result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerTranche2Error,
        prior.ChallengerTrancheError,
        base.DevelopmentTrancheError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
