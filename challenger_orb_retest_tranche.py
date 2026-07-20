"""Freeze an exact disjoint 100-session development tranche for the challenger."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest as retest
import development_non_return_collection as publication
import development_tranche as base
from historical_store import HistoricalDayStore
from learning_data import LearningDataError, freeze_dataset_contract
from learning_registry import REGISTRY_ROOT, RegistryError, current_entities
from scanner_replay import required_sessions


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-challenger-orb-retest-development-2026-07-20-v1"
SEED = 2026072005
TARGET_COUNT = 100
PRIOR_SESSIONS = 15
ELIGIBLE_START = "2023-01-03"
ELIGIBLE_END = "2026-07-17"
CALENDAR = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/session-calendar-2023-01-through-2026-07.json"
CALENDAR_SOURCE = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/session-calendar-source.json"
CALENDAR_STATUS = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/calendar-status.json"
QUALIFICATION_RESULT = PROJECT_ROOT / "research_results/2026-07-20-development-non-return-qualification-v3-v2-inspection.json"
HYPOTHESIS = PROJECT_ROOT / "learning/hypotheses" / f"experiment-catalyst-orb-retest-v1-{retest.HYPOTHESIS_SHA256}.json"
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_tranche_inspection.py"
PRIOR_SELECTIONS = (*base.PRIOR_SELECTIONS, base.DEFAULT_SELECTION)
DEFAULT_SELECTION = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/selection-2026-07-20-100-days.json"
DEFAULT_MANIFEST_ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/selection_manifests"
DEFAULT_STATUS = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/selection-status.json"
PRIVATE_NAMESPACE = "challenger_orb_retest_v1"


class ChallengerTrancheError(RuntimeError):
    """The challenger selection cannot be proven disjoint and immutable."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    return base._sha256_file(path)


def _repo_path(path: Path) -> str:
    return base._repo_path(path)


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _read_json(path: Path) -> dict[str, Any]:
    value = base._read_json(path)
    if not isinstance(value, dict):
        raise ChallengerTrancheError(f"{path} must contain an object")
    return value


def _private_path(store_root: Path) -> Path:
    return store_root / "_derived" / PRIVATE_NAMESPACE / DATASET_ID / "exclusions.json.gz"


def _binding(path: Path) -> dict[str, str]:
    return {"path": _repo_path(path), "sha256": _sha256_file(path)}


def _verify_binding(value: Mapping[str, Any]) -> None:
    raw = value.get("path")
    if not isinstance(raw, str) or not raw:
        raise ChallengerTrancheError("bound path is missing")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise ChallengerTrancheError("bound path is unsafe")
    path = PROJECT_ROOT / relative
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise ChallengerTrancheError(f"bound artifact drifted: {relative}")


def _calendar_dates() -> list[str]:
    try:
        value = json.loads(CALENDAR.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerTrancheError(f"cannot read challenger calendar: {exc}") from exc
    if not isinstance(value, list):
        raise ChallengerTrancheError("challenger calendar must contain an array")
    dates = [date.fromisoformat(str(row["date"])).isoformat() for row in value]
    if len(dates) != 887 or dates != sorted(set(dates)):
        raise ChallengerTrancheError("challenger calendar identity differs")
    return dates


def _validate_prerequisites() -> None:
    calendar_status = _read_json(CALENDAR_STATUS)
    calendar_source = _read_json(CALENDAR_SOURCE)
    qualification = _read_json(QUALIFICATION_RESULT)
    if not (
        calendar_status.get("status") == "COLLECTION_INSPECTED"
        and calendar_status.get("inspected") is True
        and calendar_status.get("sessions") == 887
        and calendar_status.get("calendar_sha256") == _sha256_file(CALENDAR)
        and calendar_status.get("target_outcomes_observed_or_derived") is False
        and calendar_source.get("calendar_sha256") == _sha256_file(CALENDAR)
        and calendar_source.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerTrancheError("calendar prerequisite differs")
    if not (
        qualification.get("status") == "QUALIFICATION_INSPECTED"
        and qualification.get("inspected") is True
        and qualification.get("survivors") == 0
        and qualification.get("outcome_contract_permitted") is False
        and qualification.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerTrancheError("v3 falsification prerequisite differs")
    hypothesis = _read_json(HYPOTHESIS)
    if (
        hypothesis.get("contract_sha256") != retest.HYPOTHESIS_SHA256
        or len(hypothesis.get("trial_family", [])) != 1
    ):
        raise ChallengerTrancheError("challenger hypothesis differs")


def build_selection(
    *,
    calendar_dates: Sequence[str],
    exclusion_snapshot: Mapping[str, Any],
    seed: int = SEED,
    target_count: int = TARGET_COUNT,
) -> tuple[dict[str, Any], list[str]]:
    normalized = [date.fromisoformat(value).isoformat() for value in calendar_dates]
    if normalized != sorted(set(normalized)):
        raise ChallengerTrancheError("calendar dates must be unique and chronological")
    excluded = set(exclusion_snapshot.get("excluded_dates", []))
    eligible = [
        value
        for index, value in enumerate(normalized)
        if ELIGIBLE_START <= value <= ELIGIBLE_END
        and value not in excluded
        and index >= PRIOR_SESSIONS
    ]
    if len(eligible) < target_count:
        raise ChallengerTrancheError(
            f"eligible disjoint pool has {len(eligible)} dates, needs {target_count}"
        )
    selected = random.Random(seed).sample(eligible, target_count)
    if set(selected) & excluded:
        raise ChallengerTrancheError("selected tranche overlaps excluded dates")
    required = required_sessions(
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


def _exclusions() -> dict[str, Any]:
    return base.build_exclusion_snapshot(
        prior_selection_paths=PRIOR_SELECTIONS,
        inspected_evidence=_registered_inspected_exclusions(),
    )


def _registered_inspected_exclusions(
    *, registry_root: Path = REGISTRY_ROOT
) -> tuple[set[str], dict[str, Any]]:
    """Bind inspected JSON evidence, including public array artifacts."""

    try:
        entities = current_entities("datasets", registry_root)
    except RegistryError as exc:
        raise ChallengerTrancheError(f"dataset registry is invalid: {exc}") from exc
    evidence_paths: set[str] = set()
    inspected_events: list[dict[str, Any]] = []
    for event in entities.values():
        payload = event["payload"]
        if payload.get("inspected") is not True:
            continue
        inspected_events.append(event)
        evidence_paths.update(
            item
            for item in payload.get("evidence_paths", [])
            if Path(item).suffix == ".json"
        )
    dates: set[str] = set()
    artifacts: list[dict[str, Any]] = []
    for relative in sorted(evidence_paths):
        path = PROJECT_ROOT / relative
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ChallengerTrancheError(f"cannot read inspected evidence: {path}") from exc
        artifact_dates = sorted(base._extract_target_dates(value))
        dates.update(artifact_dates)
        artifacts.append(
            {
                "path": relative,
                "sha256": _sha256_file(path),
                "target_dates": len(artifact_dates),
                "target_dates_sha256": _sha256_json(artifact_dates),
            }
        )
    registry_path = registry_root / "DATASETS.jsonl"
    return dates, {
        "registry_path": _repo_path(registry_path),
        "registry_sha256": _sha256_file(registry_path),
        "inspected_entity_set_sha256": _sha256_json(
            sorted(inspected_events, key=lambda item: str(item["entity_id"]))
        ),
        "inspected_entities": len(inspected_events),
        "json_evidence_artifacts": len(artifacts),
        "artifact_set_sha256": _sha256_json(artifacts),
        "artifacts": artifacts,
    }


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
        raise ChallengerTrancheError("historical store reserve is not ready")
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
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze")
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
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
        ChallengerTrancheError,
        LearningDataError,
        base.DevelopmentTrancheError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
