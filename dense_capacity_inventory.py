"""Allocate the W31 dense batch on contiguous, globally untouched sessions."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dense_family_contracts
import next_week_discovery_batch as batch
import outcome_exposure
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CALENDAR = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/"
    "session-calendar-2023-01-through-2026-07.json"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/next_batch/capacity"
DEVELOPMENT_SESSIONS = 120
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 35
SESSIONS_PER_FAMILY = (
    DEVELOPMENT_SESSIONS + EMBARGO_SESSIONS + CONFIRMATION_SESSIONS
)


class DenseCapacityInventoryError(RuntimeError):
    """The calendar, weekly gate, or untouched capacity is insufficient."""


def _read(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DenseCapacityInventoryError(f"cannot read {path}: {exc}") from exc


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError:
        return str(path.resolve())


def _calendar(path: Path) -> list[str]:
    raw = _read(path)
    if not isinstance(raw, list) or not raw:
        raise DenseCapacityInventoryError("calendar must be a non-empty array")
    dates: list[str] = []
    for index, row in enumerate(raw):
        if not isinstance(row, Mapping):
            raise DenseCapacityInventoryError(f"calendar row {index} is invalid")
        day = row.get("date")
        if not isinstance(day, str):
            raise DenseCapacityInventoryError("calendar date is invalid")
        try:
            date.fromisoformat(day)
        except ValueError as exc:
            raise DenseCapacityInventoryError("calendar date is invalid") from exc
        if row.get("open_et") != "09:30" or row.get("close_et") != "16:00":
            continue
        dates.append(day)
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise DenseCapacityInventoryError(
            "calendar sessions must be unique and chronological"
        )
    return dates


def _globally_exposed_dates(
    records: Sequence[Mapping[str, Any]],
) -> set[str]:
    return {
        day
        for record in records
        for day, _symbol in outcome_exposure.scope_pairs(record["scope"])
    }


def _untouched_runs(
    calendar: Sequence[str], exposed_dates: set[str]
) -> list[list[str]]:
    runs: list[list[str]] = []
    current: list[str] = []
    for day in calendar:
        if day in exposed_dates:
            if current:
                runs.append(current)
                current = []
            continue
        current.append(day)
    if current:
        runs.append(current)
    return runs


def _allocate(calendar: Sequence[str], exposed_dates: set[str]) -> list[list[str]]:
    required = SESSIONS_PER_FAMILY * 3
    candidates = [run for run in _untouched_runs(calendar, exposed_dates) if len(run) >= required]
    if not candidates:
        raise DenseCapacityInventoryError(
            f"no contiguous untouched calendar run has {required} full sessions"
        )
    selected = candidates[-1][-required:]
    return [
        selected[index : index + SESSIONS_PER_FAMILY]
        for index in range(0, required, SESSIONS_PER_FAMILY)
    ]


def _capacity_count(family: Mapping[str, Any]) -> int:
    universe = family["universe"]
    if "symbols" in universe:
        instruments = len(universe["symbols"])
    else:
        instruments = 250
    return DEVELOPMENT_SESSIONS * instruments


def _scope(family: Mapping[str, Any], dates: Sequence[str]) -> dict[str, Any]:
    universe = family["universe"]
    symbols = sorted(universe.get("symbols", ["*"]))
    return {"dates": list(dates), "symbols": symbols}


def _capacity_manifest(
    family: Mapping[str, Any],
    dates: Sequence[str],
    *,
    created_at: str,
    calendar_path: Path,
    output_root: Path,
) -> Path:
    family_id = str(family["family_id"])
    path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{family_id}-w31-capacity",
            "registered_at": created_at,
            "requested_dates": list(dates),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    _repo_path(calendar_path),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                    "STRATEGY_DISCOVERY_V2.md",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": family_id,
                    "formal_capacity": _capacity_count(family),
                    "capacity_unit": "frozen instrument-session observations",
                    "development_sessions": DEVELOPMENT_SESSIONS,
                    "embargo_sessions": EMBARGO_SESSIONS,
                    "confirmation_sessions": CONFIRMATION_SESSIONS,
                    "calendar_sha256": _file_hash(calendar_path),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        output_root / family_id / "manifests",
    )
    return path


def build_inventory(
    *,
    as_of: date,
    created_at: str,
    calendar_path: Path = DEFAULT_CALENDAR,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    if as_of < batch.ACTIVATION_NOT_BEFORE:
        raise DenseCapacityInventoryError(
            f"capacity allocation is closed until {batch.ACTIVATION_NOT_BEFORE}"
        )
    try:
        observed_at = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DenseCapacityInventoryError("created_at is invalid") from exc
    if observed_at.tzinfo is None or observed_at.date() != as_of:
        raise DenseCapacityInventoryError(
            "created_at must be timezone-aware and match as_of"
        )
    records = outcome_exposure.read_index(index_path)
    exposure_audit = outcome_exposure.audit(index_path)
    blocks = _allocate(_calendar(calendar_path), _globally_exposed_dates(records))
    plan = batch.build_plan()
    families: list[dict[str, Any]] = []
    for family, block in zip(plan["families"], blocks, strict=True):
        development = block[:DEVELOPMENT_SESSIONS]
        embargo = block[
            DEVELOPMENT_SESSIONS : DEVELOPMENT_SESSIONS + EMBARGO_SESSIONS
        ]
        confirmation = block[-CONFIRMATION_SESSIONS:]
        capacity_manifest = _capacity_manifest(
            family,
            block,
            created_at=created_at,
            calendar_path=calendar_path,
            output_root=output_root,
        )
        families.append(
            {
                "family_id": family["family_id"],
                "capacity_manifest": _repo_path(capacity_manifest),
                "development_dates": development,
                "embargo_dates": embargo,
                "confirmation_dates": confirmation,
                "development_scope": _scope(family, development),
                "confirmation_scope": _scope(family, confirmation),
            }
        )
    outcome_exposure.assert_disjoint(
        [family["development_scope"] for family in families]
    )
    outcome_exposure.assert_disjoint(
        [family["confirmation_scope"] for family in families]
    )
    inventory = {
        "schema_version": 1,
        "campaign_id": batch.CAMPAIGN_ID,
        "target_iso_week": batch.TARGET_ISO_WEEK,
        "created_at": created_at,
        "calendar_path": _repo_path(calendar_path),
        "calendar_sha256": _file_hash(calendar_path),
        "outcome_exposure_index_sha256": exposure_audit["index_sha256"],
        "families": families,
        "outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }
    inventory["inventory_sha256"] = dense_family_contracts._hash(inventory)
    path = output_root / f"w31-capacity-inventory-{inventory['inventory_sha256']}.json"
    dense_family_contracts._write(inventory, path)
    return path, inventory


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=date.fromisoformat, required=True)
    parser.add_argument("--created-at", required=True)
    parser.add_argument("--calendar", type=Path, default=DEFAULT_CALENDAR)
    parser.add_argument("--index", type=Path, default=outcome_exposure.DEFAULT_INDEX)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    path, inventory = build_inventory(
        as_of=args.as_of,
        created_at=args.created_at,
        calendar_path=args.calendar,
        index_path=args.index,
        output_root=args.output_root,
    )
    print(
        json.dumps(
            {
                "path": str(path),
                "inventory_sha256": inventory["inventory_sha256"],
                "families": len(inventory["families"]),
                "provider_requests": 0,
                "outcomes_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
