"""Independently inspect the pre-2016 ETF pullback replication calendar."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import etf_pullback_replication as replication
import outcome_exposure
import strategy_discovery
from historical_store import sha256_file


class EtfPullbackReplicationInspectionError(RuntimeError):
    """The calendar contract or provider output failed reconstruction."""


def inspect_contract(
    contract_path: Path, *, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    replication._timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(contract_path)
    contract = strategy_discovery.load_artifact(
        contract_path,
        expected_kind=replication.CALENDAR_CONTRACT_KIND,
    )
    checks = {
        "implementation_exact": contract.get("implementation_hashes")
        == replication._implementation_hashes(),
        "provider_query_exact": contract.get("query")
        == {
            "start": replication.CALENDAR_START,
            "end": replication.CALENDAR_END,
        },
        "minimum_capacity": contract.get("minimum_sessions")
        == replication.MINIMUM_CALENDAR_SESSIONS,
        "outputs_absent": not replication.CALENDAR_PATH.exists()
        and not replication.CALENDAR_SOURCE_PATH.exists(),
        "substitutions_forbidden": contract.get(
            "date_substitutions_allowed"
        )
        is False,
        "outcomes_closed": contract.get("market_prices_accessed") is False
        and contract.get("target_outcomes_accessed") is False,
        "broker_actions_zero": contract.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EtfPullbackReplicationInspectionError(
            "calendar contract inspection failed"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": (
            replication.CALENDAR_CONTRACT_INSPECTION_KIND
        ),
        "campaign_id": replication.CAMPAIGN_ID,
        "state": "CALENDAR_CONTRACT_INSPECTED_READY",
        "successor_id": replication.SUCCESSOR_ID,
        "contract_path": replication._repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": checks,
        "provider_access_permitted": True,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        replication.CALENDAR_ROOT / "contract-inspection",
        "etf-pullback-replication-calendar-contract-inspection",
    )


def inspect_data(
    status_path: Path, *, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    inspected = replication._timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(status_path)
    status = strategy_discovery.load_artifact(
        status_path,
        expected_kind=replication.CALENDAR_COLLECTION_KIND,
    )
    try:
        collected = datetime.fromisoformat(
            str(status["collected_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise EtfPullbackReplicationInspectionError(
            "calendar collection timestamp is invalid"
        ) from exc
    if inspected <= collected:
        raise EtfPullbackReplicationInspectionError(
            "calendar inspection must follow collection"
        )
    strategy_discovery.require_committed(replication.CALENDAR_PATH)
    strategy_discovery.require_committed(
        replication.CALENDAR_SOURCE_PATH
    )
    rows = replication.normalize_calendar_rows(
        json.loads(replication.CALENDAR_PATH.read_text(encoding="utf-8"))
    )
    source = json.loads(
        replication.CALENDAR_SOURCE_PATH.read_text(encoding="utf-8")
    )
    dates = [
        row["date"]
        for row in rows
        if row["open_et"] == "09:30" and row["close_et"] == "16:00"
    ]
    selected = dates[-replication.TOTAL_SESSIONS :]
    development = selected[
        replication.DEVELOPMENT_WARMUP_SESSIONS:
        replication.DEVELOPMENT_WARMUP_SESSIONS
        + replication.DEVELOPMENT_SESSIONS
    ]
    confirmation = selected[-replication.CONFIRMATION_SESSIONS :]
    checks = {
        "calendar_hash": sha256_file(replication.CALENDAR_PATH)
        == status.get("calendar_sha256")
        == source.get("calendar_sha256"),
        "source_binding": source.get("contract_sha256")
        == status.get("contract_sha256"),
        "session_count": len(rows) == status.get("sessions")
        and len(rows) >= replication.MINIMUM_CALENDAR_SESSIONS,
        "full_session_capacity": len(dates)
        >= replication.TOTAL_SESSIONS,
        "development_untouched": not outcome_exposure.find_overlaps(
            replication._scope(development),
            outcome_exposure.read_index(),
        ),
        "confirmation_untouched": not outcome_exposure.find_overlaps(
            replication._scope(confirmation),
            outcome_exposure.read_index(),
        ),
        "provider_request_exact": status.get("provider_requests") == 1,
        "market_prices_absent": status.get("market_prices_accessed")
        is False,
        "target_outcomes_absent": status.get("target_outcomes_accessed")
        is False,
        "broker_actions_zero": status.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EtfPullbackReplicationInspectionError(
            "calendar data inspection failed"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": replication.CALENDAR_DATA_INSPECTION_KIND,
        "campaign_id": replication.CAMPAIGN_ID,
        "state": "CALENDAR_INSPECTED_READY",
        "successor_id": replication.SUCCESSOR_ID,
        "status_path": replication._repo_path(status_path),
        "status_sha256": status["artifact_sha256"],
        "calendar_path": replication._repo_path(
            replication.CALENDAR_PATH
        ),
        "calendar_sha256": sha256_file(replication.CALENDAR_PATH),
        "source_path": replication._repo_path(
            replication.CALENDAR_SOURCE_PATH
        ),
        "sessions": len(rows),
        "full_sessions": len(dates),
        "development_sessions": len(development),
        "confirmation_sessions": len(confirmation),
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "checks": checks,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "inspected_at": inspected_at,
    }
    return strategy_discovery._write_artifact(
        payload,
        replication.CALENDAR_ROOT / "data-inspection",
        "etf-pullback-replication-calendar-data-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    contract = sub.add_parser("inspect-contract")
    contract.add_argument("artifact", type=Path)
    contract.add_argument("--inspected-at", required=True)
    data = sub.add_parser("inspect-data")
    data.add_argument("artifact", type=Path)
    data.add_argument("--inspected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "inspect-contract":
            path, value = inspect_contract(
                args.artifact, inspected_at=args.inspected_at
            )
        else:
            path, value = inspect_data(
                args.artifact, inspected_at=args.inspected_at
            )
        print(
            json.dumps(
                {
                    "path": replication._repo_path(path),
                    "state": value["state"],
                    "sha256": value["artifact_sha256"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        EtfPullbackReplicationInspectionError,
        replication.EtfPullbackReplicationError,
        OSError,
        ValueError,
        strategy_discovery.StrategyDiscoveryError,
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
