"""Record the fail-closed HTTP 400 boundary for v10 development data."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as metadata
import earnings_sec_reaction_collection as collection
import earnings_sec_reaction_search as search_source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


DEFAULT_ROOT = search_source.DEFAULT_ROOT
FAILED_SYMBOL = "APC"
HTTP_STATUS = 400


class EarningsSecReactionFailureError(RuntimeError):
    """The exact v10 source-policy failure boundary cannot be proven."""


def build_failure(
    search_path: Path,
    inspection_path: Path,
    *,
    observed_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(search_path)
    strategy_discovery.require_committed(inspection_path)
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    inspection = metadata._read(inspection_path)
    contract = search["family_contract"]
    if not (
        search["artifact_sha256"]
        == "0fc621564e338e96b6f3641a145cce3d5fd4fc4da35256c3dce9887da3399f1e"
        and inspection.get("state")
        == "REACTION_SEARCH_INSPECTED_READY_FOR_COLLECTION"
        and inspection.get("valid") is True
        and inspection.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("development_collection_authorized") is True
        and contract["implementation_hashes"][
            "earnings_sec_reaction_collection.py"
        ]
        == sha256_file(
            search_source.PROJECT_ROOT
            / "earnings_sec_reaction_collection.py"
        )
    ):
        raise EarningsSecReactionFailureError(
            "v10 failed collection lineage differs"
        )
    historical_store = store or HistoricalDayStore.from_env()
    observed = metadata._timestamp(observed_at, "observed_at")
    task_root = (
        historical_store.root
        / collection.PRIVATE_NAMESPACE
        / search["artifact_sha256"]
        / "tasks"
    )
    task_files = sorted(task_root.glob("*.json.gz"))
    if task_files:
        raise EarningsSecReactionFailureError(
            "v10 HTTP 400 boundary contains checkpointed tasks"
        )
    observed_timestamp = datetime.fromisoformat(
        observed.replace("Z", "+00:00")
    )
    records_at_failure = [
        record
        for record in outcome_exposure.read_index()
        if datetime.fromisoformat(
            str(record["recorded_at"]).replace("Z", "+00:00")
        )
        <= observed_timestamp
    ]
    if outcome_exposure.find_overlaps(
        contract["development_scope"], records_at_failure
    ):
        raise EarningsSecReactionFailureError(
            "v10 scope was indexed despite retaining no price outcomes"
        )
    first = contract["development_data_requests"][0]
    if first["symbol"] != FAILED_SYMBOL:
        raise EarningsSecReactionFailureError(
            "v10 failed request ordinal differs"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-reaction-v10-source-policy-failure",
        "campaign_id": search_source.CAMPAIGN_ID,
        "family_id": search_source.FAMILY_ID,
        "successor_id": search_source.SUCCESSOR_ID,
        "state": "REACTION_V10_HTTP_400_SOURCE_POLICY_FAILURE",
        "observed_at": observed,
        "search_path": metadata._repo_path(search_path),
        "search_file_sha256": sha256_file(search_path),
        "search_sha256": search["artifact_sha256"],
        "inspection_path": metadata._repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "failed_request": {
            "ordinal": 1,
            "symbol": first["symbol"],
            "request_sha256": first["request_sha256"],
            "endpoint": first["endpoint"],
        },
        "error": {
            "category": "unregistered_http_status",
            "http_status": HTTP_STATUS,
            "sanitized_message": "Yahoo development request returned HTTP 400",
        },
        "failure_boundary": {
            "provider_requests": 1,
            "provider_responses": 1,
            "tasks_checkpointed": 0,
            "rows_retained": 0,
            "symbols_with_retained_rows": [],
            "development_prices_retained": False,
            "outcome_exposure_index_changed": False,
            "strategy_metrics_computed": 0,
            "winner_selection_executed": False,
            "confirmation_prices_accessed": False,
            "broker_actions": 0,
        },
        "disposition": {
            "v10_promotion_eligible": False,
            "same_search_resume_permitted": False,
            "failed_symbol_retry_permitted": False,
            "failed_symbol_successor_reuse_permitted": False,
            "remaining_108_symbols_successor_permitted": True,
            "successor_search_freeze_required_before_access": True,
            "successor_may_register_http_400_as_permanent_missing": True,
            "confirmation_access_permitted": False,
        },
    }
    value["failure_sha256"] = v5.self_hash(
        value, "failure_sha256"
    )
    return value


def record_failure(
    search_path: Path,
    inspection_path: Path,
    *,
    observed_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_failure(
        search_path,
        inspection_path,
        observed_at=observed_at,
        store=store,
    )
    path = (
        root
        / "development-source-failure"
        / f"failure-{value['failure_sha256']}.json"
    )
    metadata._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("search", type=Path)
    parser.add_argument("inspection", type=Path)
    parser.add_argument("--observed-at", required=True)
    args = parser.parse_args(argv)
    path, value = record_failure(
        args.search,
        args.inspection,
        observed_at=args.observed_at,
    )
    print(
        json.dumps(
            {
                "path": metadata._repo_path(path),
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
