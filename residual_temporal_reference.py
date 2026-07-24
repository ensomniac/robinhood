"""Freeze and collect outcome-blind reference identities for residual v7.

This phase deliberately opens no market prices or forward returns.  It freezes
200 deterministic 2021-2022 decision dates, then collects the complete dated
Massive common-stock reference snapshot for each date into the ignored
historical store.  A separate inspector must rebuild the usable listing
identities before any price collection contract can be frozen.
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import outcome_exposure
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_json_bytes,
    canonical_sha256,
    sha256_file,
)
from scanner_replay import (
    MASSIVE_REFERENCE_URL,
    MassiveReferenceCollector,
    MassiveReferenceConfig,
    ScannerReplayError,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "liquid-equity-market-residual-reversal-temporal-expansion-v7"
MECHANISM_FAMILY = "two-to-three-day-cross-sectional-reversal"
SUCCESSOR_ID = (
    "two-to-three-day-cross-sectional-reversal-v7-temporal-expansion"
)
ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/continuous_v2/"
    "session-calendar-2014-01-through-2022-12.json"
)
V6_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "two-to-three-day-cross-sectional-reversal-v6-disjoint-long-history/"
    "data-contract/residual-replication-data-contract-"
    "fa1bff02df9d68f0e4d13095db233233f221434813d09f91e4d1123663519fa9.json"
)
V6_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "liquid-equity-market-residual-reversal-replication/"
    "development-inspection/"
    "liquid-equity-market-residual-reversal-replication-development-inspection-"
    "5af65a86d919ed857c6d36adfc0df7e29b1ec0781b8099027595f5131aa275a9.json"
)
REFERENCE_DATE_COUNT = 200
SELECTION_START = "2021-01-04"
SELECTION_END = "2022-12-30"
CONTRACT_KIND = "residual-temporal-reference-contract"
CONTRACT_STATE = "REFERENCE_CONTRACT_FROZEN"
COLLECTION_KIND = "residual-temporal-reference-collection"
COLLECTION_STATE = "REFERENCE_COLLECTED_UNINSPECTED"


class ResidualTemporalReferenceError(RuntimeError):
    """The v7 reference boundary or provider result is invalid."""


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualTemporalReferenceError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ResidualTemporalReferenceError(f"{path} must contain an object")
    return value


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualTemporalReferenceError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ResidualTemporalReferenceError(f"{field} needs a timezone")
    return parsed.astimezone(timezone.utc)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualTemporalReferenceError(
            f"path escaped repository: {path}"
        ) from exc


def _calendar() -> list[str]:
    try:
        raw = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualTemporalReferenceError(
            "frozen early exchange calendar is unavailable"
        ) from exc
    if not isinstance(raw, list):
        raise ResidualTemporalReferenceError("frozen early calendar is malformed")
    dates = [
        str(row.get("date"))
        for row in raw
        if isinstance(row, Mapping) and isinstance(row.get("date"), str)
    ]
    if dates != sorted(set(dates)):
        raise ResidualTemporalReferenceError(
            "frozen early calendar is not chronological"
        )
    return dates


def selected_dates() -> list[str]:
    """Return 200 stable quantiles spanning every 2021-2022 session."""

    eligible = [
        day for day in _calendar() if SELECTION_START <= day <= SELECTION_END
    ]
    if len(eligible) < REFERENCE_DATE_COUNT:
        raise ResidualTemporalReferenceError(
            "early calendar has insufficient signal capacity"
        )
    selected = [
        eligible[(index * (len(eligible) - 1)) // (REFERENCE_DATE_COUNT - 1)]
        for index in range(REFERENCE_DATE_COUNT)
    ]
    if (
        len(selected) != REFERENCE_DATE_COUNT
        or len(set(selected)) != REFERENCE_DATE_COUNT
        or selected != sorted(selected)
        or selected[0] != SELECTION_START
        or selected[-1] != SELECTION_END
    ):
        raise ResidualTemporalReferenceError(
            "deterministic early-date selection is invalid"
        )
    return selected


def _v6_bindings() -> tuple[dict[str, Any], dict[str, Any]]:
    contract = strategy_discovery.load_artifact(
        V6_CONTRACT, expected_kind="residual-replication-data-contract"
    )
    inspection = strategy_discovery.load_artifact(
        V6_INSPECTION, expected_kind="development-search-inspection"
    )
    if not (
        contract.get("state") == "DATA_CONTRACT_FROZEN"
        and len(contract.get("development_signal_dates", [])) == 200
        and len(contract.get("confirmation_signal_dates", [])) == 93
        and contract.get("confirmation_prices_accessed") is False
        and inspection.get("state") == "REJECTED"
        and inspection.get("family_id")
        == "liquid-equity-market-residual-reversal-replication"
    ):
        raise ResidualTemporalReferenceError(
            "v6 adverse evidence or untouched reserve drifted"
        )
    return contract, inspection


def _pre_reference_exposure_check(
    dates: Sequence[str], records: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    """Reject wildcard exposure; exact symbol collisions are checked later."""

    selected = set(dates)
    explicit: list[dict[str, Any]] = []
    for raw in records:
        record = outcome_exposure.validate_record(raw)
        scope = record["scope"]
        overlapping_dates = sorted(selected & set(scope["dates"]))
        if not overlapping_dates:
            continue
        symbols = scope.get("symbols")
        if symbols == ["*"]:
            raise ResidualTemporalReferenceError(
                "early-date selection overlaps prior wildcard outcome exposure"
            )
        explicit.append(
            {
                "exposure_id": record["exposure_id"],
                "dates": overlapping_dates,
                "symbols": list(symbols or []),
            }
        )
    return explicit


def freeze_contract(
    *,
    created_at: str,
    root: Path = ROOT,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at, "created_at")
    dates = selected_dates()
    v6_contract, v6_inspection = _v6_bindings()
    records = outcome_exposure.read_index()
    explicit_exposures = _pre_reference_exposure_check(dates, records)
    confirmation_scope = dict(v6_contract["confirmation_scope"])
    outcome_exposure.assert_untouched(confirmation_scope, records)
    implementation_files = (
        "residual_temporal_reference.py",
        "residual_temporal_reference_inspection.py",
        "scanner_replay.py",
        "outcome_exposure.py",
        "strategy_discovery.py",
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": CONTRACT_KIND,
        "state": CONTRACT_STATE,
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "successor_id": SUCCESSOR_ID,
        "created_at": created_at,
        "selection_contract": {
            "calendar_path": _repo_path(CALENDAR_PATH),
            "calendar_sha256": sha256_file(CALENDAR_PATH),
            "start_inclusive": SELECTION_START,
            "end_inclusive": SELECTION_END,
            "eligible_session_count": len(
                [
                    day
                    for day in _calendar()
                    if SELECTION_START <= day <= SELECTION_END
                ]
            ),
            "selection_rule": (
                "floor(i*(N-1)/(K-1)) for i=0..K-1 over chronological "
                "eligible sessions"
            ),
            "requested_date_count": REFERENCE_DATE_COUNT,
            "requested_dates": dates,
            "requested_dates_sha256": canonical_sha256(dates),
            "substitutions_allowed": False,
        },
        "reference_contract": {
            "provider": "Massive",
            "endpoint": MASSIVE_REFERENCE_URL,
            "market": "stocks",
            "locale": "us",
            "security_type": "CS",
            "active_as_of_date": True,
            "sort": "ticker",
            "order": "asc",
            "page_limit": 1000,
            "maximum_pages_per_date": 50,
            "prices_or_returns_requested": False,
        },
        "v6_adverse_evidence": {
            "contract_path": _repo_path(V6_CONTRACT),
            "contract_file_sha256": sha256_file(V6_CONTRACT),
            "contract_artifact_sha256": v6_contract["artifact_sha256"],
            "development_signal_count": len(
                v6_contract["development_signal_dates"]
            ),
            "inspection_path": _repo_path(V6_INSPECTION),
            "inspection_file_sha256": sha256_file(V6_INSPECTION),
            "inspection_artifact_sha256": v6_inspection["artifact_sha256"],
            "inspection_state": v6_inspection["state"],
            "rules_or_grid_changed": False,
        },
        "preserved_confirmation": {
            "scope": confirmation_scope,
            "signal_count": len(v6_contract["confirmation_signal_dates"]),
            "embargo_dates": list(v6_contract["embargo_dates"]),
            "prices_accessed": False,
        },
        "prior_explicit_symbol_exposures": explicit_exposures,
        "explicit_symbol_collisions_must_fail_inspection": True,
        "implementation_hashes": {
            name: sha256_file(PROJECT_ROOT / name)
            for name in implementation_files
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "market_prices_accessed": False,
        "strategy_outcomes_accessed": False,
        "provider_access_before_freeze": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "reference-contract",
        "residual-temporal-reference-contract",
    )


def load_contract(
    path: Path, *, enforce_commit: bool = True
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    contract = strategy_discovery.load_artifact(
        path, expected_kind=CONTRACT_KIND
    )
    if not (
        contract.get("state") == CONTRACT_STATE
        and contract.get("market_prices_accessed") is False
        and contract.get("strategy_outcomes_accessed") is False
        and contract.get("provider_access_before_freeze") is False
        and contract.get("broker_actions") == 0
        and contract.get("selection_contract", {}).get("requested_dates")
        == selected_dates()
    ):
        raise ResidualTemporalReferenceError(
            "reference contract is not collection-ready"
        )
    for name, expected in contract.get("implementation_hashes", {}).items():
        implementation = PROJECT_ROOT / str(name)
        if (
            not implementation.is_file()
            or sha256_file(implementation) != expected
        ):
            raise ResidualTemporalReferenceError(
                f"frozen reference implementation drifted: {name}"
            )
        if enforce_commit:
            strategy_discovery.require_committed(implementation)
    records = outcome_exposure.read_index()
    _pre_reference_exposure_check(
        contract["selection_contract"]["requested_dates"], records
    )
    outcome_exposure.assert_untouched(
        contract["preserved_confirmation"]["scope"], records
    )
    return contract


class _TelemetryCollector(MassiveReferenceCollector):
    def __init__(self, config: MassiveReferenceConfig):
        super().__init__(config)
        self.requests = 0
        self.request_seconds = 0.0
        self.pacing_wait_seconds = 0.0

    def _throttle(self) -> None:
        started = time.monotonic()
        super()._throttle()
        self.pacing_wait_seconds += time.monotonic() - started

    def _get(self, url: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        started = time.monotonic()
        try:
            return super()._get(url, params)
        finally:
            self.requests += 1
            self.request_seconds += time.monotonic() - started


def _write_snapshot(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with gzip.GzipFile(
            filename="", mode="wb", fileobj=temporary.open("wb"), mtime=0
        ) as target:
            target.write(canonical_json_bytes(list(rows)) + b"\n")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_snapshot(path: Path) -> list[dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualTemporalReferenceError(
            f"cannot read cached reference snapshot {path}: {exc}"
        ) from exc
    if not isinstance(value, list) or not value:
        raise ResidualTemporalReferenceError(
            f"cached reference snapshot is empty: {path}"
        )
    rows = [dict(item) for item in value if isinstance(item, Mapping)]
    if len(rows) != len(value):
        raise ResidualTemporalReferenceError(
            f"cached reference snapshot has malformed rows: {path}"
        )
    return rows


def collect(
    contract_path: Path,
    *,
    completed_at: str,
    root: Path = ROOT,
    store_config: HistoricalStoreConfig | None = None,
    provider_config: MassiveReferenceConfig | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(completed_at, "completed_at")
    contract = load_contract(contract_path, enforce_commit=enforce_commit)
    store = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    provider = provider_config or MassiveReferenceConfig.from_env(
        DEFAULT_ENV_PATH
    )
    relative = Path("_derived") / SUCCESSOR_ID / "reference"
    output_root = store.root / relative
    dates = contract["selection_contract"]["requested_dates"]
    snapshots: list[dict[str, Any]] = []
    cache_hits = 0
    failures = 0
    started = time.monotonic()
    with _TelemetryCollector(provider) as collector:
        for index, day in enumerate(dates, 1):
            output = output_root / f"{day}.json.gz"
            if output.is_file():
                rows = _read_snapshot(output)
                disposition = "cached"
                cache_hits += 1
            else:
                try:
                    rows = collector.fetch(day)
                except Exception:
                    failures += 1
                    raise
                _write_snapshot(output, rows)
                disposition = "collected"
            symbols = [str(row.get("ticker") or "") for row in rows]
            if (
                not symbols
                or any(not symbol for symbol in symbols)
                or symbols != sorted(symbols)
                or len(symbols) != len(set(symbols))
            ):
                raise ResidualTemporalReferenceError(
                    f"{day}: reference symbols are incomplete or ambiguous"
                )
            snapshots.append(
                {
                    "date": day,
                    "rows": len(rows),
                    "sha256": sha256_file(output),
                    "disposition": disposition,
                }
            )
            print(
                f"reference {index}/{len(dates)} {day}: "
                f"{len(rows)} rows ({disposition})",
                flush=True,
            )
        telemetry = {
            "provider_requests": collector.requests,
            "provider_request_seconds": round(
                collector.request_seconds, 6
            ),
            "pacing_wait_seconds": round(
                collector.pacing_wait_seconds, 6
            ),
            "cache_hits": cache_hits,
            "failures": failures,
            "elapsed_seconds": round(time.monotonic() - started, 6),
        }
    payload = {
        "schema_version": 1,
        "artifact_kind": COLLECTION_KIND,
        "state": COLLECTION_STATE,
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "collection_completed_at": completed_at,
        "external_relative_path": str(relative),
        "snapshots": snapshots,
        "snapshot_count": len(snapshots),
        "snapshot_manifest_sha256": canonical_sha256(snapshots),
        "provider_telemetry": telemetry,
        "prices_or_returns_accessed": False,
        "strategy_outcomes_accessed": False,
        "substitutions": 0,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "reference-collection",
        "residual-temporal-reference-collection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    collection = subparsers.add_parser("collect")
    collection.add_argument("contract", type=Path)
    collection.add_argument("--completed-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, artifact = freeze_contract(
                created_at=args.created_at, root=args.root
            )
        else:
            path, artifact = collect(
                args.contract,
                completed_at=args.completed_at,
                root=args.root,
            )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "broker_actions": artifact["broker_actions"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualTemporalReferenceError,
        ScannerReplayError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
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
