"""Freeze and inspect the causal inventory for gap-protection discovery.

The source files contain a compact scanner index: five real opening minutes
plus a deterministic end-of-day residual used only for prior-session
lookbacks.  The frozen scanner implementation is the information wall.  On a
target session it consumes only the five opening rows; no post-09:35 field is
carried into the research inventory.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import outcome_exposure
from equity_gap_continuation_validation import _candidates
from historical_store import HistoricalDayStore, sha256_file
from learning_data import security_master_sha256
from scanner_replay import (
    build_scanner_replay,
    load_calendar,
    load_frozen_scanner_contract,
    required_sessions,
    validate_rules,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "equity-gap-protection-continuation-search"
DATASET_ID = "dataset-equity-gap-protection-continuation-2026-07-24-v2"
SCHEMA_VERSION = 1
DEVELOPMENT_FRACTION = 0.60
EMBARGO_SESSIONS = 5

SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/scanner_manifests/"
    "dataset-production-scanner-replay-2026-07-21-challenger-orb-retest-v2-"
    "77dc80b560b67aaf3ab3991e7a67ffef0cb344ab79ece4e79e868d4b50b96640.json"
)
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/"
    "session-calendar-2023-01-through-2026-07.json"
)
RULES_PATH = (
    PROJECT_ROOT / "historical_batches/scanner_replay/scanner-rules-v2.json"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor"
    / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "preentry-contract"
SUMMARY_ROOT = OUTPUT_ROOT / "preentry-summary"
INSPECTION_ROOT = OUTPUT_ROOT / "preentry-inspection"
RECOVERY_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v1/"
    "preentry-failure/"
    "gap-protection-preentry-failure-"
    "3e61512f6353248ac3d65995dfbab171a50305d76bf4b84df908ef9313a4504a.json"
)

PARAMETER_GRID = {
    "breakout_volume_multiple": [1.5, 2.5],
    "maximum_structural_stop_fraction": [0.03, 0.04],
    "minimum_gap_fraction": [0.02, 0.04],
    "opening_range_minutes": [5],
    "signal_cutoff_minutes": [60, 120],
    "target_r": [1.5, 2.0],
}


class GapProtectionError(RuntimeError):
    """The successor evidence graph is incomplete or has drifted."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GapProtectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GapProtectionError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise GapProtectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GapProtectionError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise GapProtectionError(f"path escaped repository: {path}") from exc


def _publish(
    root: Path,
    prefix: str,
    content: Mapping[str, Any],
    *,
    identity_field: str,
) -> tuple[Path, dict[str, Any]]:
    value = dict(content)
    identity = _hash(value)
    value[identity_field] = identity
    path = root / f"{prefix}-{identity}.json"
    if path.exists() and _load_json(path) != value:
        raise GapProtectionError(f"hash-addressed artifact drifted: {path}")
    if not path.exists():
        _write_json(path, value)
    return path, value


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "gap_protection_successor" / DATASET_ID


def _run_root() -> Path:
    return PROJECT_ROOT / "learning_runs" / "gap_protection_successor" / DATASET_ID


def _detail_path(_store: HistoricalDayStore) -> Path:
    return _run_root() / "scanner-replay-detail.json"


def _private_summary_path(_store: HistoricalDayStore) -> Path:
    return _run_root() / "scanner-replay-summary.json"


def _inventory_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "frozen-inventory.json.gz"


def _source_paths(
    store: HistoricalDayStore,
    manifest: Mapping[str, Any],
) -> tuple[Path, Path]:
    source_root = (
        store.root
        / "_derived"
        / "scanner_replay"
        / str(manifest["dataset_id"])
    )
    return source_root / "minute_aggs", source_root / "attestations"


def _source_inventory(
    store: HistoricalDayStore,
    manifest: Mapping[str, Any],
) -> dict[str, Any]:
    minute_root, attestation_root = _source_paths(store, manifest)
    dates = list(manifest["collection_contract"]["required_session_dates"])
    rows: list[dict[str, str]] = []
    for day in dates:
        minute_path = minute_root / day[:4] / f"{day}.csv.gz"
        attestation_path = attestation_root / day[:4] / f"{day}.json"
        if not minute_path.is_file() or not attestation_path.is_file():
            raise GapProtectionError(f"source inventory is incomplete on {day}")
        attestation = _load_json(attestation_path)
        if (
            attestation.get("status") != "READY"
            or attestation.get("date") != day
            or attestation.get("dataset_id") != manifest["dataset_id"]
        ):
            raise GapProtectionError(f"source attestation is not READY on {day}")
        rows.append(
            {
                "date": day,
                "minute_file_sha256": sha256_file(minute_path),
                "attestation_file_sha256": sha256_file(attestation_path),
            }
        )
    return {
        "dates": len(dates),
        "first_date": dates[0],
        "last_date": dates[-1],
        "inventory_sha256": _hash(rows),
    }


def _source_graph(
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], list[str], dict[str, Any]]:
    manifest = load_frozen_scanner_contract(SOURCE_MANIFEST)
    required = list(manifest["collection_contract"]["required_session_dates"])
    if len(required) != 474:
        raise GapProtectionError("source must retain exactly 474 causal sessions")
    calendar = load_calendar(CALENDAR_PATH)
    positions = {day: index for index, day in enumerate(calendar)}
    required_set = set(required)
    targets = [
        day
        for day in required
        if positions[day] >= 15
        and set(calendar[positions[day] - 15 : positions[day] + 1])
        <= required_set
    ]
    if required_sessions(targets, calendar) != required:
        raise GapProtectionError("expanded target dates no longer bind the source")
    universe = manifest["dataset_payload"]["universe_contract"]
    rules = validate_rules(RULES_PATH)
    if _hash(rules) != universe["scanner_rules_sha256"]:
        raise GapProtectionError("scanner rules drifted from the source contract")
    security_path = PROJECT_ROOT / str(universe["security_master_path"])
    split_path = PROJECT_ROOT / str(universe["split_actions_path"])
    if security_master_sha256(security_path) != universe["security_master_sha256"]:
        raise GapProtectionError("security master drifted from the source contract")
    if sha256_file(split_path) != universe["split_actions_sha256"]:
        raise GapProtectionError("split actions drifted from the source contract")
    return manifest, targets, _source_inventory(store, manifest)


def _trial_count() -> int:
    result = 1
    for values in PARAMETER_GRID.values():
        result *= len(values)
    return result


def build_contract(
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    manifest, targets, source_inventory = _source_graph(source)
    universe = manifest["dataset_payload"]["universe_contract"]
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "gap_protection_preentry_contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "OUTCOME_BLIND_PREENTRY_FROZEN",
        "storage_recovery_binding": {
            "failure_path": _repo_path(RECOVERY_FAILURE),
            "failure_sha256": sha256_file(RECOVERY_FAILURE),
            "changed_semantics": False,
            "changed_dates_symbols_or_grid": False,
            "recovery_scope": (
                "write the ignored scanner detail below learning_runs so the "
                "shared scanner can serialize a repository-relative private path"
            ),
        },
        "source_binding": {
            "manifest_path": _repo_path(SOURCE_MANIFEST),
            "manifest_sha256": sha256_file(SOURCE_MANIFEST),
            "source_dataset_id": manifest["dataset_id"],
            "source_inventory": source_inventory,
            "calendar_path": _repo_path(CALENDAR_PATH),
            "calendar_sha256": sha256_file(CALENDAR_PATH),
            "rules_path": _repo_path(RULES_PATH),
            "rules_semantic_sha256": universe["scanner_rules_sha256"],
            "security_master_path": universe["security_master_path"],
            "security_master_sha256": universe["security_master_sha256"],
            "split_actions_path": universe["split_actions_path"],
            "split_actions_sha256": universe["split_actions_sha256"],
        },
        "target_dates": targets,
        "target_dates_sha256": _hash(targets),
        "selection_contract": {
            "information_cutoff": "TARGET_SESSION_09:35_ET",
            "candidate_rule": {
                "security_type": "COMMON",
                "opening_price_strictly_above": 5.0,
                "minimum_gap_fraction_inclusive": 0.02,
                "maximum_gap_fraction_inclusive": 0.08,
            },
            "prior_outcome_filter": (
                "discard an entire date before allocation when any frozen "
                "candidate date/symbol pair overlaps the global exposure index"
            ),
            "empty_candidate_dates": "discard_before_allocation",
            "allocation": {
                "ordering": "chronological",
                "development_fraction_after_embargo": DEVELOPMENT_FRACTION,
                "development_count_formula": (
                    "floor((eligible_date_count - embargo_sessions) * "
                    "development_fraction_after_embargo)"
                ),
                "embargo_sessions": EMBARGO_SESSIONS,
                "confirmation": "all remaining eligible dates",
                "substitutions_allowed": False,
            },
        },
        "search_contract": {
            "selection_mode": "development_search",
            "parameter_grid": PARAMETER_GRID,
            "trial_count": _trial_count(),
            "winner_selection": "PORTFOLIO_VALIDATION_V2_SELECTION_AWARE_V1",
            "maximum_hold_sessions": 1,
            "primary_cost_bps_per_side": 5,
            "stress_cost_bps_per_side": [10, 20],
        },
        "information_wall": {
            "composite_source_layout": (
                "five real opening minute rows plus a deterministic 15:59 "
                "residual used only for completed prior-session lookbacks"
            ),
            "target_session_fields_permitted": [
                "09:30-09:34 OHLCV",
                "point-in-time security identity",
            ],
            "target_session_post_09_35_fields_permitted": False,
            "target_outcomes_observed_or_derived": False,
            "confirmation_outcomes_permitted": False,
            "broker_actions_permitted": False,
        },
        "implementation_binding": {
            "controller_path": _repo_path(Path(__file__)),
            "controller_sha256": sha256_file(Path(__file__)),
            "scanner_path": _repo_path(PROJECT_ROOT / "scanner_replay.py"),
            "scanner_sha256": sha256_file(PROJECT_ROOT / "scanner_replay.py"),
        },
    }
    return _publish(
        CONTRACT_ROOT,
        "gap-protection-preentry-contract",
        content,
        identity_field="contract_sha256",
    )


def _load_contract(path: Path) -> dict[str, Any]:
    value = _load_json(path)
    supplied = value.pop("contract_sha256", None)
    expected = _hash(value)
    value["contract_sha256"] = supplied
    if (
        supplied != expected
        or path.name != f"gap-protection-preentry-contract-{expected}.json"
    ):
        raise GapProtectionError("preentry contract hash is invalid")
    return value


def _allocation(
    detail: Mapping[str, Any],
    *,
    exposure_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    eligible: list[str] = []
    candidates_by_date: dict[str, list[dict[str, Any]]] = {}
    excluded: list[dict[str, Any]] = []
    for day in sorted(detail["dates"]):
        candidates = _candidates(day, detail["dates"][day])
        if not candidates:
            excluded.append({"date": day, "reason": "no_frozen_candidates"})
            continue
        symbols = sorted(str(row["symbol"]) for row in candidates)
        overlaps = outcome_exposure.find_overlaps(
            {"dates": [day], "symbols": symbols},
            exposure_records,
        )
        if overlaps:
            excluded.append(
                {
                    "date": day,
                    "reason": "prior_outcome_exposure",
                    "overlap_count": len(overlaps),
                }
            )
            continue
        eligible.append(day)
        candidates_by_date[day] = candidates
    usable = len(eligible) - EMBARGO_SESSIONS
    development_count = math.floor(usable * DEVELOPMENT_FRACTION)
    if development_count < 50 or usable - development_count < 20:
        raise GapProtectionError("clean inventory lacks minimum partition capacity")
    development = eligible[:development_count]
    embargo = eligible[development_count : development_count + EMBARGO_SESSIONS]
    confirmation = eligible[development_count + EMBARGO_SESSIONS :]
    return {
        "eligible_dates": eligible,
        "candidates_by_date": candidates_by_date,
        "excluded_dates": excluded,
        "partitions": {
            "development": development,
            "embargo": embargo,
            "confirmation": confirmation,
        },
    }


def build_preentry(
    contract_path: Path,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    contract = _load_contract(contract_path)
    if contract["implementation_binding"]["controller_sha256"] != sha256_file(
        Path(__file__)
    ):
        raise GapProtectionError("controller drifted after preentry freeze")
    if contract["implementation_binding"]["scanner_sha256"] != sha256_file(
        PROJECT_ROOT / "scanner_replay.py"
    ):
        raise GapProtectionError("scanner implementation drifted after preentry freeze")
    manifest, targets, source_inventory = _source_graph(source)
    if source_inventory != contract["source_binding"]["source_inventory"]:
        raise GapProtectionError("source inventory drifted after preentry freeze")
    if targets != contract["target_dates"]:
        raise GapProtectionError("target dates drifted after preentry freeze")
    universe = manifest["dataset_payload"]["universe_contract"]
    minute_root, _ = _source_paths(source, manifest)
    detail_path = _detail_path(source)
    scanner_summary_path = _private_summary_path(source)
    scanner_summary = build_scanner_replay(
        selected_dates=targets,
        calendar=load_calendar(CALENDAR_PATH),
        rules=validate_rules(RULES_PATH),
        security_path=PROJECT_ROOT / str(universe["security_master_path"]),
        minute_root=minute_root,
        split_path=PROJECT_ROOT / str(universe["split_actions_path"]),
        detailed_output=detail_path,
        summary_output=scanner_summary_path,
        dataset_id=DATASET_ID,
        summary_extension={
            "source_contract_sha256": contract["contract_sha256"],
            "target_outcomes_observed_or_derived": False,
            "broker_actions_permitted": False,
        },
    )
    detail = _load_json(detail_path)
    allocation = _allocation(
        detail,
        exposure_records=outcome_exposure.read_index(),
    )
    inventory = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "contract_sha256": contract["contract_sha256"],
        "detail_sha256": sha256_file(detail_path),
        "global_outcome_index_sha256": outcome_exposure.audit()["index_sha256"],
        **allocation,
        "target_outcomes_observed_or_derived": False,
        "confirmation_outcomes_permitted": False,
    }
    inventory["content_sha256"] = _hash(inventory)
    inventory_path = _inventory_path(source)
    _write_gzip(inventory_path, inventory)
    partition = inventory["partitions"]
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "gap_protection_preentry_summary",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "PREENTRY_BUILT_AWAITING_INSPECTION",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "scanner_summary": {
            "completed_dates": scanner_summary["completed_dates"],
            "selection_time_et": scanner_summary["selection_time_et"],
            "information_cutoff": scanner_summary["information_cutoff"],
            "detail_sha256": sha256_file(detail_path),
        },
        "inventory": {
            "content_sha256": inventory["content_sha256"],
            "file_sha256": sha256_file(inventory_path),
            "eligible_dates": len(inventory["eligible_dates"]),
            "excluded_dates": len(inventory["excluded_dates"]),
            "candidate_pairs": sum(
                len(rows) for rows in inventory["candidates_by_date"].values()
            ),
            "development_dates": len(partition["development"]),
            "embargo_dates": len(partition["embargo"]),
            "confirmation_dates": len(partition["confirmation"]),
            "development_candidate_pairs": sum(
                len(inventory["candidates_by_date"][day])
                for day in partition["development"]
            ),
            "confirmation_candidate_pairs": sum(
                len(inventory["candidates_by_date"][day])
                for day in partition["confirmation"]
            ),
            "global_outcome_index_sha256": inventory[
                "global_outcome_index_sha256"
            ],
        },
        "target_outcomes_observed_or_derived": False,
        "confirmation_outcomes_permitted": False,
        "broker_actions_permitted": False,
    }
    return _publish(
        SUMMARY_ROOT,
        "gap-protection-preentry-summary",
        content,
        identity_field="summary_sha256",
    )


def inspect_preentry(
    contract_path: Path,
    summary_path: Path,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    contract = _load_contract(contract_path)
    summary = _load_json(summary_path)
    supplied_summary_hash = summary.pop("summary_sha256", None)
    if supplied_summary_hash != _hash(summary):
        raise GapProtectionError("preentry summary hash is invalid")
    summary["summary_sha256"] = supplied_summary_hash
    if summary.get("contract_sha256") != contract["contract_sha256"]:
        raise GapProtectionError("summary is not bound to the frozen contract")
    detail_path = _detail_path(source)
    inventory_path = _inventory_path(source)
    detail = _load_json(detail_path)
    inventory = _load_gzip(inventory_path)
    rebuilt = _allocation(
        detail,
        exposure_records=outcome_exposure.read_index(),
    )
    expected_inventory = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "contract_sha256": contract["contract_sha256"],
        "detail_sha256": sha256_file(detail_path),
        "global_outcome_index_sha256": outcome_exposure.audit()["index_sha256"],
        **rebuilt,
        "target_outcomes_observed_or_derived": False,
        "confirmation_outcomes_permitted": False,
    }
    expected_inventory["content_sha256"] = _hash(expected_inventory)
    if inventory != expected_inventory:
        raise GapProtectionError("private preentry inventory failed independent rebuild")
    partition = inventory["partitions"]
    expected_counts = {
        "content_sha256": inventory["content_sha256"],
        "file_sha256": sha256_file(inventory_path),
        "eligible_dates": len(inventory["eligible_dates"]),
        "excluded_dates": len(inventory["excluded_dates"]),
        "candidate_pairs": sum(
            len(rows) for rows in inventory["candidates_by_date"].values()
        ),
        "development_dates": len(partition["development"]),
        "embargo_dates": len(partition["embargo"]),
        "confirmation_dates": len(partition["confirmation"]),
        "development_candidate_pairs": sum(
            len(inventory["candidates_by_date"][day])
            for day in partition["development"]
        ),
        "confirmation_candidate_pairs": sum(
            len(inventory["candidates_by_date"][day])
            for day in partition["confirmation"]
        ),
        "global_outcome_index_sha256": inventory["global_outcome_index_sha256"],
    }
    if summary.get("inventory") != expected_counts:
        raise GapProtectionError("public preentry accounting failed independent rebuild")
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "gap_protection_preentry_inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "PREENTRY_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "summary_path": _repo_path(summary_path),
        "summary_sha256": summary["summary_sha256"],
        "inventory": expected_counts,
        "trial_count": _trial_count(),
        "confirmation_outcomes_permitted": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions_permitted": False,
        "valid": True,
    }
    return _publish(
        INSPECTION_ROOT,
        "gap-protection-preentry-inspection",
        content,
        identity_field="inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("freeze-preentry")
    build = subparsers.add_parser("build-preentry")
    build.add_argument("contract", type=Path)
    inspect = subparsers.add_parser("inspect-preentry")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("summary", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-preentry":
            path, value = build_contract()
        elif args.command == "build-preentry":
            path, value = build_preentry(args.contract)
        else:
            path, value = inspect_preentry(args.contract, args.summary)
        print(
            json.dumps(
                {
                    "path": str(path),
                    "state": value["state"],
                    "sha256": value[
                        next(
                            field
                            for field in (
                                "contract_sha256",
                                "summary_sha256",
                                "inspection_sha256",
                            )
                            if field in value
                        )
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (GapProtectionError, KeyError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
