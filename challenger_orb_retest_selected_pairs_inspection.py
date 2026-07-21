"""Independently inspect the frozen ORB-retest selected-pair boundary.

The inspector reconstructs every private date-security row directly from the
inspected scanner summary and private scanner detail.  It does not call the
freezer or the shared selection primitive, and it reads no catalyst, trigger,
quote, post-entry, return, or outcome data.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract
from learning_experiment import LearningExperimentError, load_hypothesis_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-selected-candidate-contract-2026-07-21-challenger-orb-retest-v1"
PREENTRY_DATASET_ID = "dataset-challenger-orb-retest-preentry-collection-2026-07-21-v1"
SCANNER_DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-21-challenger-orb-retest-v2"
)
EXPECTED_SELECTED_PAIRS = 1826
EXPECTED_DATES = 100
MINIMUM_FREE_BYTES = 20 * 1024**3
SCANNER_MANIFEST_SHA256 = (
    "77dc80b560b67aaf3ab3991e7a67ffef0cb344ab79ece4e79e868d4b50b96640"
)
OUTER_MANIFEST_SHA256 = (
    "b580483bc8150725018edb74534a2884d248108737fcd83c5af6e63269f9d8e9"
)
HYPOTHESIS_SHA256 = "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105"
PRIMARY_TRIAL_ID = "trial-8a93c0ab15c85248"
DEFAULT_SUMMARY = (
    PROJECT_ROOT / "research_results/2026-07-21-challenger-orb-retest-scanner-v2.json"
)
DEFAULT_INSPECTION = (
    PROJECT_ROOT
    / "research_results/2026-07-21-challenger-orb-retest-scanner-v2-inspection.json"
)
DEFAULT_SCANNER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/scanner_manifests"
    / (
        "dataset-production-scanner-replay-2026-07-21-challenger-orb-retest-v2-"
        "77dc80b560b67aaf3ab3991e7a67ffef0cb344ab79ece4e79e868d4b50b96640.json"
    )
)
DEFAULT_OUTER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/acquisition_manifests"
    / (
        "dataset-challenger-orb-retest-acquisition-2026-07-21-v2-"
        "b580483bc8150725018edb74534a2884d248108737fcd83c5af6e63269f9d8e9.json"
    )
)
DEFAULT_HYPOTHESIS = (
    PROJECT_ROOT
    / "learning/hypotheses"
    / (
        "experiment-catalyst-orb-retest-v1-"
        "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105.json"
    )
)
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/selected_pair_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/selected-pair-contract-status.json"
)
SCANNER_FIELDS = (
    "open_price",
    "opening_high",
    "opening_low",
    "opening_close",
    "opening_volume",
    "opening_relative_volume",
    "opening_return",
    "average_daily_volume_14",
    "daily_atr_14",
    "prior_close",
)


class ChallengerSelectedPairsInspectionError(RuntimeError):
    """The frozen selected-pair boundary does not independently rebuild."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerSelectedPairsInspectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ChallengerSelectedPairsInspectionError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerSelectedPairsInspectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ChallengerSelectedPairsInspectionError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ChallengerSelectedPairsInspectionError(
            f"path is outside the public repository: {path}"
        ) from exc


def _resolve_repo_path(value: Any) -> Path:
    raw = str(value or "")
    relative = Path(raw)
    if not raw or relative.is_absolute() or ".." in relative.parts:
        raise ChallengerSelectedPairsInspectionError("repository path is unsafe")
    path = (PROJECT_ROOT / relative).resolve()
    if not path.is_relative_to(PROJECT_ROOT.resolve()):
        raise ChallengerSelectedPairsInspectionError("repository path escapes root")
    return path


def _verify_binding(value: Any, expected_path: Path) -> None:
    if not isinstance(value, Mapping):
        raise ChallengerSelectedPairsInspectionError("file binding is malformed")
    path = _resolve_repo_path(value.get("path"))
    if (
        path != expected_path.resolve()
        or not path.is_file()
        or _sha256_file(path) != value.get("sha256")
    ):
        raise ChallengerSelectedPairsInspectionError(f"bound file drifted: {path}")


def _detail_path(summary: Mapping[str, Any]) -> Path:
    artifact = summary.get("detailed_artifact")
    if not isinstance(artifact, Mapping) or artifact.get("public") is not False:
        raise ChallengerSelectedPairsInspectionError(
            "scanner private-detail contract is missing"
        )
    path = _resolve_repo_path(artifact.get("local_path"))
    if not path.is_file() or _sha256_file(path) != artifact.get("sha256"):
        raise ChallengerSelectedPairsInspectionError("scanner private detail drifted")
    return path


def rebuild_selection(
    summary_path: Path, *, dataset_id: str
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Rebuild exact rows without importing either selection implementation."""

    summary = _read_json(summary_path)
    expected = {
        "dataset_id": SCANNER_DATASET_ID,
        "status": "READY",
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "complete_universe": True,
        "selection_is_dynamic": True,
    }
    if any(summary.get(field) != value for field, value in expected.items()):
        raise ChallengerSelectedPairsInspectionError("scanner summary contract differs")
    detail_path = _detail_path(summary)
    detail = _read_json(detail_path)
    if detail.get("dataset_id") != SCANNER_DATASET_ID:
        raise ChallengerSelectedPairsInspectionError("scanner detail identity differs")
    for field in (
        "scanner_rules_sha256",
        "security_master_sha256",
        "split_actions_sha256",
    ):
        if detail.get(field) != summary.get(field):
            raise ChallengerSelectedPairsInspectionError(
                f"scanner detail {field} differs"
            )

    public_dates = summary.get("dates")
    detail_dates = detail.get("dates")
    if not isinstance(public_dates, list) or not isinstance(detail_dates, Mapping):
        raise ChallengerSelectedPairsInspectionError("scanner dates are malformed")
    pairs: list[dict[str, Any]] = []
    daily_shortlists: list[dict[str, Any]] = []
    for public_day in public_dates:
        if not isinstance(public_day, Mapping):
            raise ChallengerSelectedPairsInspectionError(
                "scanner public date is malformed"
            )
        day = str(public_day.get("date") or "")
        try:
            datetime.fromisoformat(day)
        except ValueError as exc:
            raise ChallengerSelectedPairsInspectionError(
                f"scanner date is malformed: {day}"
            ) from exc
        raw_day = detail_dates.get(day)
        if not isinstance(raw_day, Mapping):
            raise ChallengerSelectedPairsInspectionError(f"scanner detail lacks {day}")
        symbols = raw_day.get("selected_symbols")
        evaluations = raw_day.get("evaluations")
        if not isinstance(symbols, list) or not isinstance(evaluations, list):
            raise ChallengerSelectedPairsInspectionError(
                f"scanner selected rows are malformed for {day}"
            )
        if any(not isinstance(symbol, str) or not symbol for symbol in symbols) or len(
            symbols
        ) != len(set(symbols)):
            raise ChallengerSelectedPairsInspectionError(
                f"scanner selected identities repeat for {day}"
            )
        eligible = [
            row
            for row in evaluations
            if isinstance(row, Mapping)
            and isinstance(row.get("symbol"), str)
            and row.get("disposition") == "eligible"
        ]
        if len({str(row["symbol"]) for row in eligible}) != len(eligible):
            raise ChallengerSelectedPairsInspectionError(
                f"scanner eligible identities repeat for {day}"
            )
        by_symbol = {str(row["symbol"]): row for row in eligible}
        shortlist: list[dict[str, Any]] = []
        for rank, symbol in enumerate(symbols, 1):
            row = by_symbol.get(symbol)
            if row is None or int(row.get("opening_rvol_rank", -1)) != rank:
                raise ChallengerSelectedPairsInspectionError(
                    f"scanner rank or eligibility differs for {day}"
                )
            try:
                scanner_fields = {field: row[field] for field in SCANNER_FIELDS}
                selected = {
                    "date": day,
                    "symbol": symbol,
                    "instrument_id": str(row["instrument_id"]),
                    "primary_exchange": str(row["primary_exchange"]),
                    "rank": rank,
                    "scanner_fields": scanner_fields,
                }
            except KeyError as exc:
                raise ChallengerSelectedPairsInspectionError(
                    f"selected scanner row lacks {exc.args[0]} for {day}"
                ) from exc
            if not selected["instrument_id"] or not selected["primary_exchange"]:
                raise ChallengerSelectedPairsInspectionError(
                    f"selected scanner identity is incomplete for {day}"
                )
            pairs.append(selected)
            shortlist.append(
                {
                    "symbol": symbol,
                    "instrument_id": selected["instrument_id"],
                    "opening_relative_volume": scanner_fields[
                        "opening_relative_volume"
                    ],
                    "opening_return": scanner_fields["opening_return"],
                    "rank": rank,
                }
            )
        shortlist_hash = _sha256_json(shortlist)
        if len(shortlist) != int(
            public_day.get("shortlist_count", -1)
        ) or shortlist_hash != public_day.get("shortlist_sha256"):
            raise ChallengerSelectedPairsInspectionError(
                f"scanner shortlist does not rebuild for {day}"
            )
        daily_shortlists.append(
            {
                "date": day,
                "shortlist_count": len(shortlist),
                "shortlist_sha256": shortlist_hash,
            }
        )

    requested = [str(value) for value in summary.get("requested_dates", [])]
    rebuilt_dates = [row["date"] for row in daily_shortlists]
    if requested != rebuilt_dates or len(requested) != len(set(requested)):
        raise ChallengerSelectedPairsInspectionError(
            "scanner requested-date identity differs"
        )
    private = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "source_dataset_id": SCANNER_DATASET_ID,
        "source_summary_sha256": _sha256_file(summary_path),
        "source_detail_sha256": _sha256_file(detail_path),
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "selected_pair_count": len(pairs),
        "selected_pairs": pairs,
    }
    public = {
        "source_dataset_id": SCANNER_DATASET_ID,
        "source_manifest_sha256": str(
            summary.get("source", {}).get("contract_sha256") or ""
        ),
        "source_summary_sha256": _sha256_file(summary_path),
        "source_detail_sha256": _sha256_file(detail_path),
        "requested_dates": requested,
        "selected_pair_count": len(pairs),
        "daily_shortlists": daily_shortlists,
        "private_selection_content_sha256": _sha256_json(private),
    }
    return private, public


def _expected_source_paths() -> dict[str, Path]:
    return {
        "scanner_summary": DEFAULT_SUMMARY,
        "scanner_inspection": DEFAULT_INSPECTION,
        "scanner_manifest": DEFAULT_SCANNER_MANIFEST,
        "outer_manifest": DEFAULT_OUTER_MANIFEST,
        "hypothesis": DEFAULT_HYPOTHESIS,
    }


def _expected_implementation_paths() -> dict[str, Path]:
    return {
        "freezer": PROJECT_ROOT / "challenger_orb_retest_selected_pairs.py",
        "inspector": Path(__file__),
        "selection_primitive": PROJECT_ROOT / "scanner_selected_pairs.py",
        "trigger": PROJECT_ROOT / "challenger_orb_retest.py",
    }


def _verify_contract_bindings(
    manifest: Mapping[str, Any], *, require_published: bool
) -> None:
    source_bindings = manifest.get("source_bindings")
    implementations = manifest.get("implementation_contract")
    publication = manifest.get("publication_contract")
    expected_sources = _expected_source_paths()
    expected_implementations = _expected_implementation_paths()
    if (
        not isinstance(source_bindings, Mapping)
        or set(source_bindings) != set(expected_sources)
        or not isinstance(implementations, Mapping)
        or set(implementations) != set(expected_implementations)
        or not isinstance(publication, Mapping)
    ):
        raise ChallengerSelectedPairsInspectionError("contract bindings are incomplete")
    for name, path in expected_sources.items():
        _verify_binding(source_bindings[name], path)
    for name, path in expected_implementations.items():
        _verify_binding(implementations[name], path)

    all_bindings = {**source_bindings, **implementations}
    if not require_published:
        if publication:
            raise ChallengerSelectedPairsInspectionError(
                "test contract unexpectedly claims publication"
            )
        return
    if set(publication) != set(all_bindings):
        raise ChallengerSelectedPairsInspectionError(
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
        raise ChallengerSelectedPairsInspectionError(
            "inspection requires HEAD to equal its pushed upstream"
        )
    for name, binding in all_bindings.items():
        value = publication[name]
        if (
            not isinstance(value, Mapping)
            or value.get("commit") != head
            or value.get("path") != binding.get("path")
            or value.get("sha256") != binding.get("sha256")
        ):
            raise ChallengerSelectedPairsInspectionError(
                f"publication binding differs for {name}"
            )
        status = subprocess.run(
            ["git", "status", "--porcelain", "--", str(value["path"])],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        if status.strip():
            raise ChallengerSelectedPairsInspectionError(
                f"published input is dirty: {value['path']}"
            )


def _private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived/scanner_selected_pairs"
        / dataset_id
        / "selected-pairs.json.gz"
    )


def _downstream_artifacts(store_root: Path) -> list[Path]:
    root = store_root / "_derived/challenger_orb_retest_preentry" / PREENTRY_DATASET_ID
    return (
        sorted(path for path in root.rglob("*") if path.is_file())
        if root.exists()
        else []
    )


def _assert_private_not_public(
    manifest: Mapping[str, Any], private: Mapping[str, Any]
) -> None:
    forbidden_keys = {
        "symbol",
        "symbols",
        "instrument_id",
        "primary_exchange",
        "scanner_fields",
        "selected_pairs",
    }

    def walk(value: Any) -> None:
        if isinstance(value, Mapping):
            if forbidden_keys & {str(key) for key in value}:
                raise ChallengerSelectedPairsInspectionError(
                    "public manifest contains private identity fields"
                )
            for child in value.values():
                walk(child)
        elif isinstance(value, list):
            for child in value:
                walk(child)

    walk(manifest)
    identities = {
        str(row[field])
        for row in private["selected_pairs"]
        for field in ("symbol", "instrument_id")
    }
    public_strings: set[str] = set()

    def strings(value: Any) -> None:
        if isinstance(value, Mapping):
            for child in value.values():
                strings(child)
        elif isinstance(value, list):
            for child in value:
                strings(child)
        elif isinstance(value, str):
            public_strings.add(value)

    strings(manifest)
    if identities & public_strings:
        raise ChallengerSelectedPairsInspectionError(
            "public manifest contains a private selected identity"
        )


def inspect_selected_pairs(
    *,
    manifest_path: Path,
    env_path: Path,
    status_path: Path,
    require_published: bool = True,
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChallengerSelectedPairsInspectionError(
            "selected-pair dataset identity differs"
        )
    _verify_contract_bindings(manifest, require_published=require_published)

    scanner_manifest = load_frozen_dataset_contract(DEFAULT_SCANNER_MANIFEST)
    outer_manifest = load_frozen_dataset_contract(DEFAULT_OUTER_MANIFEST)
    scanner_inspection = _read_json(DEFAULT_INSPECTION)
    summary = _read_json(DEFAULT_SUMMARY)
    try:
        hypothesis = load_hypothesis_contract(DEFAULT_HYPOTHESIS)
    except LearningExperimentError as exc:
        raise ChallengerSelectedPairsInspectionError(str(exc)) from exc
    detail = summary.get("detailed_artifact")
    if not isinstance(detail, Mapping):
        raise ChallengerSelectedPairsInspectionError(
            "scanner detail binding is missing"
        )
    if any(
        (
            scanner_manifest.get("dataset_id") != SCANNER_DATASET_ID,
            scanner_manifest.get("manifest_sha256") != SCANNER_MANIFEST_SHA256,
            outer_manifest.get("manifest_sha256") != OUTER_MANIFEST_SHA256,
            outer_manifest.get("full_universe_market_contract", {}).get(
                "scanner_manifest_sha256"
            )
            != SCANNER_MANIFEST_SHA256,
            summary.get("source", {}).get("contract_sha256") != SCANNER_MANIFEST_SHA256,
            scanner_inspection.get("dataset_id") != SCANNER_DATASET_ID,
            scanner_inspection.get("status") != "INSPECTED",
            scanner_inspection.get("valid") is not True,
            scanner_inspection.get("manifest_sha256") != SCANNER_MANIFEST_SHA256,
            scanner_inspection.get("summary_sha256") != _sha256_file(DEFAULT_SUMMARY),
            scanner_inspection.get("detail_sha256") != detail.get("sha256"),
            scanner_inspection.get("completed_dates") != EXPECTED_DATES,
            scanner_inspection.get("total_selected") != EXPECTED_SELECTED_PAIRS,
            hypothesis.get("contract_sha256") != HYPOTHESIS_SHA256,
            hypothesis.get("primary_trial_id") != PRIMARY_TRIAL_ID,
            hypothesis.get("falsification_criteria", {}).get("minimum_closed_signals")
            != 50,
        )
    ):
        raise ChallengerSelectedPairsInspectionError("upstream lineage differs")

    private, public = rebuild_selection(DEFAULT_SUMMARY, dataset_id=DATASET_ID)
    if (
        private.get("selected_pair_count") != EXPECTED_SELECTED_PAIRS
        or len(public.get("requested_dates", [])) != EXPECTED_DATES
        or len(
            {
                (str(row["date"]), str(row["instrument_id"]))
                for row in private["selected_pairs"]
            }
        )
        != EXPECTED_SELECTED_PAIRS
    ):
        raise ChallengerSelectedPairsInspectionError(
            "selected-pair graph is incomplete or ambiguous"
        )
    expected_public = {
        **public,
        "source_inspection_sha256": _sha256_file(DEFAULT_INSPECTION),
        "source_outer_manifest_sha256": _sha256_file(DEFAULT_OUTER_MANIFEST),
        "hypothesis_contract_sha256": HYPOTHESIS_SHA256,
        "primary_trial_id": PRIMARY_TRIAL_ID,
    }
    if manifest.get("selection_contract") != expected_public:
        raise ChallengerSelectedPairsInspectionError(
            "public selection aggregate does not independently rebuild"
        )

    config = HistoricalStoreConfig.from_env(env_path)
    reserve_bytes = max(MINIMUM_FREE_BYTES, config.min_free_bytes)
    free_bytes = shutil.disk_usage(config.root).free
    if free_bytes < reserve_bytes:
        raise ChallengerSelectedPairsInspectionError(
            "historical store reserve is not available"
        )
    private_path = _private_path(config.root, DATASET_ID)
    stored_private = _read_gzip(private_path)
    if stored_private != private or _sha256_json(stored_private) != expected_public.get(
        "private_selection_content_sha256"
    ):
        raise ChallengerSelectedPairsInspectionError(
            "private selected-pair graph does not rebuild"
        )
    downstream = _downstream_artifacts(config.root)
    if downstream:
        raise ChallengerSelectedPairsInspectionError(
            "downstream pre-entry artifacts exist before contract inspection"
        )

    mechanism = {
        "rule_version": "catalyst-orb-retest-v1",
        "hypothesis_contract_sha256": HYPOTHESIS_SHA256,
        "primary_trial_id": PRIMARY_TRIAL_ID,
        "initial_break_entry_allowed": False,
        "retest_rule": (
            "first completed later bar touches opening high and closes at or above"
        ),
        "entry_trigger": "first condition-valid rebreak of held retest-bar high",
        "decision_snapshot_seconds_after_rebreak": 10,
        "entry_cutoff_et": "10:30:00",
        "one_trial_no_parameter_selection": True,
    }
    permitted = {
        "selected_symbols_only": True,
        "point_in_time_primary_source_evidence": True,
        "condition_valid_sip_trades_through_terminal_boundary": True,
        "completed_noninterpolated_retest_bars": True,
        "quote_book_and_tape_through_final_decision": True,
        "preentry_structure_benchmark_halt_and_liquidity": True,
        "full_universe_selected_detail": False,
        "post_entry_rows": False,
        "returns_or_outcomes": False,
    }
    outcome_lock = {
        "post_entry_data_access_allowed": False,
        "return_fields_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "outcome_contract_must_be_separately_frozen": True,
        "date_symbol_provider_or_missing_input_substitution_allowed": False,
    }
    downstream_zero = {
        "dataset_id": PREENTRY_DATASET_ID,
        "pre_freeze_artifact_count": 0,
        "private_location": (
            "LOCAL_HISTORICAL_DATA_ROOT/_derived/"
            f"challenger_orb_retest_preentry/{PREENTRY_DATASET_ID}"
        ),
    }
    capacity = manifest.get("capacity_contract")
    payload = manifest.get("dataset_payload")
    registered_at = str(manifest.get("registered_at") or "")
    try:
        parsed_registered_at = datetime.fromisoformat(registered_at)
    except ValueError as exc:
        raise ChallengerSelectedPairsInspectionError(
            "manifest registration timestamp is malformed"
        ) from exc
    if parsed_registered_at.tzinfo is None:
        raise ChallengerSelectedPairsInspectionError(
            "manifest registration timestamp lacks timezone"
        )
    if any(
        (
            manifest.get("requested_dates") != public["requested_dates"],
            manifest.get("mechanism_contract") != mechanism,
            manifest.get("permitted_next_inputs") != permitted,
            manifest.get("outcome_lock") != outcome_lock,
            manifest.get("downstream_zero_state") != downstream_zero,
            not isinstance(payload, Mapping),
            payload.get("lane") != "development",
            payload.get("claim_scope") != "DEVELOPMENT_ONLY",
            payload.get("status") != "FROZEN",
            payload.get("inspected") is not False,
            not isinstance(capacity, Mapping),
            capacity.get("minimum_free_bytes") != MINIMUM_FREE_BYTES,
            capacity.get("configured_reserve_bytes") != config.min_free_bytes,
            capacity.get("enforced_reserve_bytes") != reserve_bytes,
            int(capacity.get("free_bytes_at_freeze", -1)) < reserve_bytes,
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
        )
    ):
        raise ChallengerSelectedPairsInspectionError(
            "mechanism, access, outcome, or capacity contract differs"
        )
    _assert_private_not_public(manifest, private)

    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_path": _repo_path(manifest_path),
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "requested_dates": EXPECTED_DATES,
        "requested_dates_sha256": _sha256_json(public["requested_dates"]),
        "selected_pair_count": EXPECTED_SELECTED_PAIRS,
        "selected_pair_graph_sha256": expected_public[
            "private_selection_content_sha256"
        ],
        "daily_shortlist_set_sha256": _sha256_json(public["daily_shortlists"]),
        "source_scanner_detail_sha256": public["source_detail_sha256"],
        "pre_freeze_downstream_artifacts": 0,
        "capacity_ready": True,
        "private_rows_outside_repository": True,
        "private_identifiers_public": False,
        "date_symbol_provider_or_missing_input_substitution_allowed": False,
        "outcome_contract_permitted": False,
        "target_outcomes_observed_or_derived": False,
        "next_required_stage": (
            "commit and push this inspected selected-pair boundary, then freeze and "
            "inspect the exact no-outcome causal/source request graph before collection"
        ),
        "claim_boundary": (
            "Exact inspected 09:35 scanner selection and permitted pre-entry input "
            "classes only. No catalyst, trigger, quote, fill, return, outcome, alpha, "
            "maturity, or production-rule claim is established."
        ),
        "valid": True,
    }
    _write_json(status_path, result)
    return result


def _default_manifest() -> Path:
    matches = sorted(DEFAULT_MANIFEST_ROOT.glob(f"{DATASET_ID}-*.json"))
    if len(matches) != 1:
        raise ChallengerSelectedPairsInspectionError(
            "exactly one selected-pair manifest is required"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, nargs="?")
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_selected_pairs(
            manifest_path=args.manifest or _default_manifest(),
            env_path=args.env,
            status_path=args.status,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerSelectedPairsInspectionError,
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
