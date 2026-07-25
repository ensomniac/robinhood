"""Record the fail-closed Massive permission result for SEC PEAD data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


DEFAULT_ROOT = source.DEFAULT_ROOT


class EarningsSecMarketFailureError(RuntimeError):
    """The exact failed provider boundary cannot be proven."""


def build_failure(
    contract_path: Path,
    inspection_path: Path,
    *,
    observed_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = source._read(contract_path)
    inspection = source._read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == v5.self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "MARKET_DATA_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and contract["implementation_hashes"][
            "earnings_sec_market_data.py"
        ]
        == sha256_file(source.PROJECT_ROOT / "earnings_sec_market_data.py")
    ):
        raise EarningsSecMarketFailureError(
            "failed Massive contract lineage differs"
        )
    historical_store = store or HistoricalDayStore.from_env()
    task_root = (
        historical_store.root
        / source.PRIVATE_NAMESPACE
        / contract["contract_sha256"]
        / "tasks"
    )
    task_files = sorted(task_root.glob("*.json.gz"))
    if task_files:
        raise EarningsSecMarketFailureError(
            "permission failure boundary contains retained tasks"
        )
    if outcome_exposure.find_overlaps(
        contract["development_scope"], outcome_exposure.read_index()
    ):
        raise EarningsSecMarketFailureError(
            "development scope was outcome exposed despite permission failure"
        )
    first = contract["requests"][0]
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-development-market-data-source-failure"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "DEVELOPMENT_SOURCE_PERMISSION_FAILURE",
        "observed_at": source._timestamp(observed_at, "observed_at"),
        "contract_path": source._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": source._repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "provider": "Massive SIP adjusted daily aggregates",
        "failed_request": {
            "ordinal": 1,
            "request_sha256": first["request_sha256"],
            "symbol": first["symbol"],
            "path": first["path"],
        },
        "error": {
            "category": "permanent_permission",
            "http_status": 403,
            "sanitized_message": "Massive HTTP 403",
        },
        "failure_boundary": {
            "provider_requests": 1,
            "provider_responses": 1,
            "rows_returned": 0,
            "tasks_checkpointed": 0,
            "symbols_opened": 0,
            "development_prices_retained": False,
            "strategy_metrics_computed": 0,
            "confirmation_prices_accessed": False,
            "outcome_exposure_index_changed": False,
            "broker_actions": 0,
        },
        "disposition": {
            "same_source_retry_permitted": False,
            "provider_purchase_permitted": False,
            "exact_no_purchase_source_fallback_permitted": True,
            "request_graph_change_permitted": False,
            "strategy_rule_change_permitted": False,
        },
    }
    value["failure_sha256"] = v5.self_hash(value, "failure_sha256")
    return value


def record_failure(
    contract_path: Path,
    inspection_path: Path,
    *,
    observed_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_failure(
        contract_path,
        inspection_path,
        observed_at=observed_at,
        store=store,
    )
    path = (
        root
        / "market-data-source-failure"
        / f"failure-{value['failure_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    parser.add_argument("inspection", type=Path)
    parser.add_argument("--observed-at", required=True)
    args = parser.parse_args(argv)
    path, value = record_failure(
        args.contract,
        args.inspection,
        observed_at=args.observed_at,
    )
    print(
        json.dumps(
            {
                "path": source._repo_path(path),
                "sha256": value["failure_sha256"],
                "state": value["state"],
                "failure_boundary": value["failure_boundary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
