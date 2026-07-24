"""Collect completed 2026 earnings metadata for a dense PEAD successor.

This stage accesses only report metadata.  It freezes exact market-wide
calendar requests through the last completed session before provider access,
normalizes responses under the already inspected ambiguity policy, and keeps
all price and forward-return access forbidden.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from contextlib import contextmanager
from datetime import date
from pathlib import Path
from typing import Any

import earnings_gap_continuation as base
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = base.CAMPAIGN_ID
FAMILY_ID = "earnings-gap-continuation"
SUCCESSOR_ID = "earnings-gap-continuation-v2-dense-pead"
EVENT_START = "2026-01-01"
EVENT_END = "2026-07-23"
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / SUCCESSOR_ID
)
V1_COLLECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-gap-continuation-v1-development-search/event-collection/"
    "earnings-gap-event-collection-"
    "5c51a8f58f326bc1fe3c745a8dfc421ff9de51150ed6b933c0bab9b67b9cfbf6.json"
)
V1_CAPACITY_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-gap-continuation-v1-development-search/"
    "event-capacity-inspection/"
    "earnings-gap-event-capacity-inspection-"
    "d3ba91c913cc3a21d1faeb5fdb410935ea24deb5484b62fdf229282134df8e3f.json"
)


class EarningsGapContinuationV2Error(RuntimeError):
    """The completed-event metadata boundary is incomplete or drifted."""


def _requests() -> list[dict[str, Any]]:
    return [
        {"start_date": "2026-01-01", "days": 31, "filter": None},
        {"start_date": "2026-02-01", "days": 28, "filter": None},
        {"start_date": "2026-03-01", "days": 31, "filter": None},
        {"start_date": "2026-04-01", "days": 30, "filter": None},
        {"start_date": "2026-05-01", "days": 31, "filter": None},
        {"start_date": "2026-06-01", "days": 30, "filter": None},
        {"start_date": "2026-07-01", "days": 23, "filter": None},
    ]


def _read(path: Path) -> dict[str, Any]:
    value = base._read(path)
    return dict(value)


def _repo_path(path: Path) -> str:
    return base._repo_path(path)


def _source_graph(*, enforce_commit: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    for path in (V1_COLLECTION, V1_CAPACITY_INSPECTION):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    collection = _read(V1_COLLECTION)
    inspection = _read(V1_CAPACITY_INSPECTION)
    if not (
        collection.get("state") == "EVENT_METADATA_COLLECTED_UNINSPECTED"
        and collection.get("verified_positive_surprises") == 9_516
        and collection.get("market_prices_accessed") is False
        and collection.get("forward_returns_accessed") is False
        and inspection.get("state") == "INSUFFICIENT_POWER_CAPACITY"
        and inspection.get("development_signal_dates") == 57
        and inspection.get("untouched_confirmation_signal_dates") == 14
        and inspection.get("strategy_returns_computed") == 0
        and inspection.get("forward_returns_accessed") is False
    ):
        raise EarningsGapContinuationV2Error(
            "v1 metadata-only capacity disposition drifted"
        )
    return collection, inspection


def build_event_contract(
    *,
    created_at: str,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    base._timestamp(created_at, "created_at")
    if date.fromisoformat(EVENT_END) >= date.today():
        raise EarningsGapContinuationV2Error(
            "event end must precede the current Eastern date"
        )
    collection, inspection = _source_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-gap-v2-event-collection-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": created_at,
        "provider": "Robinhood read-only market-wide earnings calendar",
        "provider_method": "get_earnings_calendar",
        "event_start": EVENT_START,
        "event_end": EVENT_END,
        "requests": _requests(),
        "logical_provider_requests": 7,
        "selection_fields": [
            "symbol",
            "report_date",
            "timing",
            "verified",
            "actual_eps",
            "estimated_eps",
        ],
        "selection_rule": (
            "Retain unambiguous company-verified reports with numeric actual "
            "EPS strictly above estimated EPS. No price, gap, return, or "
            "strategy field may influence event inclusion."
        ),
        "v1_collection_path": _repo_path(V1_COLLECTION),
        "v1_collection_file_sha256": sha256_file(V1_COLLECTION),
        "v1_collection_sha256": collection["collection_sha256"],
        "v1_capacity_inspection_path": _repo_path(
            V1_CAPACITY_INSPECTION
        ),
        "v1_capacity_inspection_file_sha256": sha256_file(
            V1_CAPACITY_INSPECTION
        ),
        "v1_capacity_inspection_sha256": inspection[
            "inspection_sha256"
        ],
        "material_difference_rationale": (
            "V1 was never evaluated and failed only its narrow gap-universe "
            "confirmation capacity check. V2 collects later completed event "
            "metadata so candidate liquidity and reaction rules can be "
            "preregistered on a dense event denominator before any target "
            "price or forward-return access."
        ),
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "implementation_sha256": sha256_file(
            Path(__file__).resolve()
        ),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    value["contract_sha256"] = base._self_hash(
        value, "contract_sha256"
    )
    return value


def freeze_event_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    value = build_event_contract(
        created_at=created_at,
        enforce_commit=enforce_commit,
    )
    path = (
        root
        / "event-contract"
        / f"earnings-gap-v2-event-contract-"
        f"{value['contract_sha256']}.json"
    )
    base._write_json(path, value)
    return path, value


def inspect_event_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    base._timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = _read(contract_path)
    supplied = contract.get("contract_sha256")
    if supplied != base._self_hash(contract, "contract_sha256"):
        raise EarningsGapContinuationV2Error(
            "event contract hash is invalid"
        )
    rebuilt = build_event_contract(
        created_at=str(contract["created_at"]),
        enforce_commit=enforce_commit,
    )
    checks = {
        "exact_rebuild": rebuilt == contract,
        "seven_exact_windows": contract["requests"] == _requests(),
        "completed_boundary": contract["event_end"] == EVENT_END,
        "no_prices": contract["market_prices_accessed"] is False,
        "no_forward_returns": (
            contract["forward_returns_accessed"] is False
        ),
        "no_strategy_metrics": contract["strategy_metrics_computed"] == 0,
        "no_broker_actions": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsGapContinuationV2Error(
            "event contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-gap-v2-event-contract-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "EVENT_CONTRACT_INSPECTED_READY",
        "inspected_at": inspected_at,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": supplied,
        "checks": checks,
        "collection_authorized": True,
        "provider_access_authorized": True,
        "authorized_provider_requests": 7,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = base._self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "event-contract-inspection"
        / f"earnings-gap-v2-event-contract-inspection-"
        f"{value['inspection_sha256']}.json"
    )
    base._write_json(path, value)
    return path, value


@contextmanager
def _base_event_scope():
    original = (
        base.EVENT_START,
        base.EVENT_END,
        base.PRIOR_DISCARDED_PROVIDER_REQUESTS,
    )
    try:
        base.EVENT_START = EVENT_START
        base.EVENT_END = EVENT_END
        base.PRIOR_DISCARDED_PROVIDER_REQUESTS = 0
        yield
    finally:
        (
            base.EVENT_START,
            base.EVENT_END,
            base.PRIOR_DISCARDED_PROVIDER_REQUESTS,
        ) = original


def ingest_event_responses(
    contract_path: Path,
    inspection_path: Path,
    lines: Sequence[str],
    *,
    collected_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    inspection = _read(inspection_path)
    if not (
        inspection.get("state") == "EVENT_CONTRACT_INSPECTED_READY"
        and inspection.get("provider_access_authorized") is True
        and inspection.get("contract_path") == _repo_path(contract_path)
    ):
        raise EarningsGapContinuationV2Error(
            "event provider access is not independently authorized"
        )
    with _base_event_scope():
        path, value = base.ingest_event_calendars(
            contract_path,
            inspection_path,
            lines,
            collected_at=collected_at,
            root=root,
            store=store,
        )
    value["artifact_kind"] = "earnings-gap-v2-event-collection"
    value["successor_id"] = SUCCESSOR_ID
    value["provider_requests"] = 7
    value["effective_provider_requests"] = 7
    value["discarded_provider_requests"] = 0
    value["collection_sha256"] = base._self_hash(
        value, "collection_sha256"
    )
    replacement = (
        root
        / "event-collection"
        / f"earnings-gap-v2-event-collection-"
        f"{value['collection_sha256']}.json"
    )
    path.unlink(missing_ok=True)
    base._write_json(replacement, value)
    return replacement, value


def inspect_event_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    base._timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(collection_path)
    collection = _read(collection_path)
    if collection.get("collection_sha256") != base._self_hash(
        collection, "collection_sha256"
    ):
        raise EarningsGapContinuationV2Error(
            "event collection hash is invalid"
        )
    source = store or HistoricalDayStore.from_env()
    relative = str(collection["private_payload"]).replace(
        "LOCAL_HISTORICAL_DATA_ROOT/", ""
    )
    private = source.root / relative
    payload = base._load_gzip(private)
    positive = [
        row
        for row in payload["events"]
        if row["verified"]
        and row["actual_eps"] is not None
        and row["estimated_eps"] is not None
        and row["actual_eps"] > row["estimated_eps"]
    ]
    checks = {
        "private_file_hash": (
            sha256_file(private)
            == collection["private_payload_file_sha256"]
        ),
        "private_content_hash": (
            canonical_sha256(payload)
            == collection["private_payload_content_sha256"]
        ),
        "request_count": payload["effective_provider_requests"] == 7,
        "event_dates_bounded": all(
            EVENT_START <= row["report_date"] <= EVENT_END
            for row in payload["events"]
        ),
        "normalized_count": (
            len(payload["events"]) == collection["normalized_events"]
        ),
        "positive_count": (
            len(positive) == collection["verified_positive_surprises"]
        ),
        "no_prices": payload["market_prices_accessed"] is False,
        "no_forward_returns": (
            payload["forward_returns_accessed"] is False
        ),
        "no_broker_actions": payload["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsGapContinuationV2Error(
            "event collection inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-gap-v2-event-collection-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "EVENT_COLLECTION_INSPECTED_READY",
        "inspected_at": inspected_at,
        "collection_path": _repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "normalized_events": len(payload["events"]),
        "verified_positive_surprises": len(positive),
        "checks": checks,
        "provider_requests_during_inspection": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = base._self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "event-collection-inspection"
        / f"earnings-gap-v2-event-collection-inspection-"
        f"{value['inspection_sha256']}.json"
    )
    base._write_json(path, value)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze-event-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = commands.add_parser("inspect-event-contract")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    ingest = commands.add_parser("ingest-event-responses")
    ingest.add_argument("contract", type=Path)
    ingest.add_argument("inspection", type=Path)
    ingest.add_argument("--collected-at", required=True)
    inspect_collection = commands.add_parser("inspect-event-collection")
    inspect_collection.add_argument("collection", type=Path)
    inspect_collection.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-event-contract":
            path, value = freeze_event_contract(
                created_at=args.created_at
            )
        elif args.command == "inspect-event-contract":
            path, value = inspect_event_contract(
                args.contract,
                inspected_at=args.inspected_at,
            )
        elif args.command == "ingest-event-responses":
            lines = list(__import__("sys").stdin)
            path, value = ingest_event_responses(
                args.contract,
                args.inspection,
                lines,
                collected_at=args.collected_at,
            )
        else:
            path, value = inspect_event_collection(
                args.collection,
                inspected_at=args.inspected_at,
            )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": value.get(
                        "state", value.get("artifact_kind")
                    ),
                    "sha256": value.get(
                        "inspection_sha256",
                        value.get(
                            "collection_sha256",
                            value.get("contract_sha256"),
                        ),
                    ),
                    "provider_requests": value.get(
                        "provider_requests", 0
                    ),
                    "market_prices_accessed": False,
                    "forward_returns_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        EarningsGapContinuationV2Error,
        base.EarningsGapContinuationError,
        OSError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
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
