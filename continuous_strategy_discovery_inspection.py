"""Independently inspect continuous existing-family calendar artifacts."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import continuous_strategy_discovery as continuous
import outcome_exposure
import strategy_discovery


class ContinuousDiscoveryInspectionError(RuntimeError):
    """The independently rebuilt calendar artifact is invalid."""


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContinuousDiscoveryInspectionError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ContinuousDiscoveryInspectionError(
            f"{field} must include a timezone"
        )
    return parsed


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = continuous.CALENDAR_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    inspected = _timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = strategy_discovery.load_artifact(
        contract_path, expected_kind=continuous.CALENDAR_CONTRACT_KIND
    )
    continuous._predecessor(enforce_commit=enforce_commit)
    if not (
        inspected
        > _timestamp(str(contract["created_at"]), "contract.created_at")
        and contract.get("campaign_id") == continuous.CAMPAIGN_ID
        and contract.get("research_generation")
        == continuous.RESEARCH_GENERATION
        and contract.get("successor_id") == continuous.SUCCESSOR_ID
        and contract.get("mechanism_family") == continuous.MECHANISM_FAMILY
        and contract.get("implementation_hashes")
        == continuous._implementation_hashes()
        and contract.get("query")
        == {
            "start": continuous.CALENDAR_START,
            "end": continuous.CALENDAR_END,
        }
        and contract.get("minimum_sessions")
        == continuous.MINIMUM_CALENDAR_SESSIONS
        and contract.get("date_substitutions_allowed") is False
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("provider_requests") == 0
        and contract.get("market_prices_accessed") is False
        and contract.get("target_outcomes_accessed") is False
        and contract.get("broker_actions") == 0
    ):
        raise ContinuousDiscoveryInspectionError(
            "successor calendar contract does not independently rebuild"
        )
    if (
        continuous.PROJECT_ROOT / str(contract["calendar_path"])
    ).exists() or (
        continuous.PROJECT_ROOT / str(contract["source_path"])
    ).exists():
        raise ContinuousDiscoveryInspectionError(
            "calendar output appeared before contract inspection"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": continuous.CALENDAR_CONTRACT_INSPECTION_KIND,
        "campaign_id": continuous.CAMPAIGN_ID,
        "state": "CALENDAR_CONTRACT_INSPECTED_READY",
        "research_generation": continuous.RESEARCH_GENERATION,
        "successor_id": continuous.SUCCESSOR_ID,
        "contract_path": continuous._repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": {
            "existing_family_identity_rebuilt": True,
            "predecessor_retirement_rebuilt": True,
            "provider_query_exact": True,
            "implementation_hashes_exact": True,
            "outputs_absent": True,
            "outcome_boundary_closed": True,
            "substitutions_forbidden": True,
            "weekly_new_family_slot_not_consumed": True,
        },
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "contract-inspection",
        "continuous-successor-calendar-contract-inspection",
    )


def inspect_calendar(
    status_path: Path,
    *,
    inspected_at: str,
    root: Path = continuous.CALENDAR_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    inspected = _timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(status_path)
    status = strategy_discovery.load_artifact(
        status_path, expected_kind=continuous.CALENDAR_COLLECTION_KIND
    )
    contract_path = continuous.PROJECT_ROOT / str(status["contract_path"])
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = strategy_discovery.load_artifact(
        contract_path, expected_kind=continuous.CALENDAR_CONTRACT_KIND
    )
    calendar_path = continuous.PROJECT_ROOT / str(status["calendar_path"])
    source_path = continuous.PROJECT_ROOT / str(status["source_path"])
    if enforce_commit:
        strategy_discovery.require_committed(calendar_path)
        strategy_discovery.require_committed(source_path)
    source = continuous._read(source_path)
    rows = json.loads(calendar_path.read_text(encoding="utf-8"))
    normalized = continuous.normalize_calendar_rows(rows)
    collected = _timestamp(str(status["collected_at"]), "collected_at")
    if not (
        inspected > collected
        and status.get("state") == "CALENDAR_COLLECTED_UNINSPECTED"
        and status.get("contract_sha256") == contract["artifact_sha256"]
        and status.get("calendar_sha256")
        == strategy_discovery._file_hash(calendar_path)
        and source.get("calendar_sha256") == status["calendar_sha256"]
        and source.get("contract_sha256") == contract["artifact_sha256"]
        and source.get("provider_requests") == 1
        and source.get("market_prices_accessed") is False
        and source.get("target_outcomes_accessed") is False
        and source.get("broker_actions") == 0
        and len(normalized) >= continuous.MINIMUM_CALENDAR_SESSIONS
        and normalized[0]["date"] >= continuous.CALENDAR_START
        and normalized[-1]["date"] <= continuous.CALENDAR_END
    ):
        raise ContinuousDiscoveryInspectionError(
            "successor calendar collection does not independently rebuild"
        )
    full_sessions = [
        row
        for row in normalized
        if row["open_et"] == "09:30" and row["close_et"] == "16:00"
    ]
    if len(full_sessions) < continuous.TOTAL_SESSIONS:
        raise ContinuousDiscoveryInspectionError(
            "calendar lacks the frozen successor evidence capacity"
        )
    exposure = outcome_exposure.audit()
    payload = {
        "schema_version": 1,
        "artifact_kind": continuous.CALENDAR_DATA_INSPECTION_KIND,
        "campaign_id": continuous.CAMPAIGN_ID,
        "state": "CALENDAR_INSPECTED_READY",
        "research_generation": continuous.RESEARCH_GENERATION,
        "successor_id": continuous.SUCCESSOR_ID,
        "status_path": continuous._repo_path(status_path),
        "status_sha256": status["artifact_sha256"],
        "contract_sha256": contract["artifact_sha256"],
        "calendar_path": status["calendar_path"],
        "calendar_sha256": status["calendar_sha256"],
        "sessions": len(normalized),
        "full_sessions": len(full_sessions),
        "first_session": normalized[0]["date"],
        "last_session": normalized[-1]["date"],
        "outcome_exposure_index_sha256": exposure["index_sha256"],
        "checks": {
            "provider_scope_exact": True,
            "chronology_exact": True,
            "session_count_sufficient": True,
            "price_and_return_fields_absent": True,
            "predecessor_corpus_not_selected": True,
            "broker_actions_zero": True,
        },
        "provider_requests": 1,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "inspected_at": inspected_at,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "data-inspection",
        "continuous-successor-calendar-data-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=continuous.CALENDAR_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    contract = sub.add_parser("inspect-contract")
    contract.add_argument("artifact", type=Path)
    contract.add_argument("--inspected-at", required=True)
    calendar = sub.add_parser("inspect-calendar")
    calendar.add_argument("artifact", type=Path)
    calendar.add_argument("--inspected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "inspect-contract":
            path, artifact = inspect_contract(
                args.artifact,
                inspected_at=args.inspected_at,
                root=args.root,
            )
        else:
            path, artifact = inspect_calendar(
                args.artifact,
                inspected_at=args.inspected_at,
                root=args.root,
            )
        print(
            json.dumps(
                {
                    "path": continuous._repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ContinuousDiscoveryInspectionError,
        continuous.ContinuousDiscoveryError,
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
