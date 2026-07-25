"""Independently inspect the SEC PEAD Massive permission failure."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as market
import earnings_sec_market_failure as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecMarketFailureInspectionError(RuntimeError):
    """The permission failure could not be independently closed."""


def inspect_failure(
    failure_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(failure_path)
    failure = market._read(failure_path)
    contract_path = market.PROJECT_ROOT / failure["contract_path"]
    inspection_path = market.PROJECT_ROOT / failure["inspection_path"]
    rebuilt = source.build_failure(
        contract_path,
        inspection_path,
        observed_at=failure["observed_at"],
        store=store,
    )
    contract = market._read(contract_path)
    checks = {
        "failure_hash_valid": failure["failure_sha256"]
        == v5.self_hash(failure, "failure_sha256"),
        "failure_exactly_rebuilt": failure == rebuilt,
        "first_frozen_request_bound": failure["failed_request"][
            "request_sha256"
        ]
        == contract["requests"][0]["request_sha256"],
        "permission_status_exact": failure["error"]["category"]
        == "permanent_permission"
        and failure["error"]["http_status"] == 403,
        "one_request_zero_rows": failure["failure_boundary"][
            "provider_requests"
        ]
        == 1
        and failure["failure_boundary"]["rows_returned"] == 0,
        "zero_tasks_or_exposure": failure["failure_boundary"][
            "tasks_checkpointed"
        ]
        == 0
        and failure["failure_boundary"]["outcome_exposure_index_changed"]
        is False
        and not outcome_exposure.find_overlaps(
            contract["development_scope"], outcome_exposure.read_index()
        ),
        "confirmation_closed": failure["failure_boundary"][
            "confirmation_prices_accessed"
        ]
        is False,
        "zero_metrics_or_broker": failure["failure_boundary"][
            "strategy_metrics_computed"
        ]
        == 0
        and failure["failure_boundary"]["broker_actions"] == 0,
        "same_source_closed": failure["disposition"][
            "same_source_retry_permitted"
        ]
        is False,
        "purchase_closed": failure["disposition"][
            "provider_purchase_permitted"
        ]
        is False,
        "exact_fallback_only": failure["disposition"][
            "exact_no_purchase_source_fallback_permitted"
        ]
        is True
        and failure["disposition"]["request_graph_change_permitted"] is False
        and failure["disposition"]["strategy_rule_change_permitted"] is False,
    }
    if not all(checks.values()):
        raise EarningsSecMarketFailureInspectionError(
            "Massive permission failure inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-development-market-data-source-failure-inspection"
        ),
        "campaign_id": market.CAMPAIGN_ID,
        "family_id": market.FAMILY_ID,
        "successor_id": market.SUCCESSOR_ID,
        "state": "DEVELOPMENT_SOURCE_PERMISSION_FAILURE_INSPECTED",
        "inspected_at": market._timestamp(inspected_at, "inspected_at"),
        "failure_path": market._repo_path(failure_path),
        "failure_file_sha256": sha256_file(failure_path),
        "failure_sha256": failure["failure_sha256"],
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "same_source_retry_authorized": False,
        "provider_purchase_authorized": False,
        "exact_no_purchase_fallback_authorized": True,
        "development_prices_retained": False,
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
        / "market-data-source-failure-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    market._write(path, value)
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
                "path": market._repo_path(path),
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
