"""Independently inspect and close the v10 HTTP 400 source failure."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as metadata
import earnings_sec_reaction_failure as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecReactionFailureInspectionError(RuntimeError):
    """The v10 source-policy failure could not be independently closed."""


def inspect_failure(
    failure_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(failure_path)
    failure = metadata._read(failure_path)
    search_path = metadata.PROJECT_ROOT / failure["search_path"]
    inspection_path = metadata.PROJECT_ROOT / failure["inspection_path"]
    rebuilt = source.build_failure(
        search_path,
        inspection_path,
        observed_at=failure["observed_at"],
        store=store,
    )
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    checks = {
        "failure_hash_valid": failure["failure_sha256"]
        == v5.self_hash(failure, "failure_sha256"),
        "failure_exactly_rebuilt": failure == rebuilt,
        "first_request_bound": failure["failed_request"]["ordinal"] == 1
        and failure["failed_request"]["symbol"] == source.FAILED_SYMBOL,
        "http_400_exact": failure["error"]
        == {
            "category": "unregistered_http_status",
            "http_status": 400,
            "sanitized_message": (
                "Yahoo development request returned HTTP 400"
            ),
        },
        "zero_tasks_or_rows": failure["failure_boundary"][
            "tasks_checkpointed"
        ]
        == 0
        and failure["failure_boundary"]["rows_retained"] == 0
        and failure["failure_boundary"]["development_prices_retained"]
        is False,
        "development_scope_still_untouched": not outcome_exposure.find_overlaps(
            search["family_contract"]["development_scope"],
            outcome_exposure.read_index(),
        ),
        "zero_metrics_or_winner": failure["failure_boundary"][
            "strategy_metrics_computed"
        ]
        == 0
        and failure["failure_boundary"]["winner_selection_executed"] is False,
        "confirmation_closed": failure["failure_boundary"][
            "confirmation_prices_accessed"
        ]
        is False
        and failure["disposition"]["confirmation_access_permitted"] is False,
        "v10_terminal": failure["disposition"][
            "v10_promotion_eligible"
        ]
        is False
        and failure["disposition"]["same_search_resume_permitted"] is False,
        "failed_symbol_closed": failure["disposition"][
            "failed_symbol_retry_permitted"
        ]
        is False
        and failure["disposition"][
            "failed_symbol_successor_reuse_permitted"
        ]
        is False,
        "successor_bounded": failure["disposition"][
            "remaining_108_symbols_successor_permitted"
        ]
        is True
        and failure["disposition"][
            "successor_search_freeze_required_before_access"
        ]
        is True,
        "broker_actions_zero": failure["failure_boundary"]["broker_actions"]
        == 0,
    }
    if not all(checks.values()):
        raise EarningsSecReactionFailureInspectionError(
            "v10 development source failure inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v10-source-policy-failure-inspection"
        ),
        "campaign_id": source.search_source.CAMPAIGN_ID,
        "family_id": source.search_source.FAMILY_ID,
        "successor_id": source.search_source.SUCCESSOR_ID,
        "state": "REACTION_V10_SOURCE_FAILURE_INSPECTED_TERMINAL",
        "inspected_at": metadata._timestamp(inspected_at, "inspected_at"),
        "failure_path": metadata._repo_path(failure_path),
        "failure_file_sha256": sha256_file(failure_path),
        "failure_sha256": failure["failure_sha256"],
        "checks": checks,
        "v10_promotion_eligible": False,
        "remaining_108_symbols_successor_permitted": True,
        "successor_search_freeze_required_before_access": True,
        "successor_http_400_missing_policy_permitted": True,
        "excluded_successor_symbol": source.FAILED_SYMBOL,
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = v5.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "development-source-failure-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    metadata._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("failure", type=Path)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value = inspect_failure(
        args.failure, inspected_at=args.inspected_at
    )
    print(
        json.dumps(
            {
                "path": metadata._repo_path(path),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
