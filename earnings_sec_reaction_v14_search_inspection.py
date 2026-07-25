"""Independently inspect the frozen v14 untouched-remainder search."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_corrected_expansion as capacity
import earnings_sec_reaction_v14_search as source
import outcome_exposure
import strategy_discovery
from historical_store import sha256_file
from learning_data import load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


class EarningsSecReactionV14SearchInspectionError(RuntimeError):
    """The v14 pre-outcome search failed independent reconstruction."""


def inspect_search(
    search_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(search_path)
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    contract = search["family_contract"]
    capacity_path = source.PROJECT_ROOT / contract["capacity_manifest"]
    strategy_discovery.require_committed(capacity_path)
    capacity_manifest = load_frozen_dataset_contract(capacity_path)
    rebuilt = source.build_contract(
        created_at=contract["created_at"],
        capacity_manifest=capacity_path,
    )
    preflight_path = source.PROJECT_ROOT / search["preflight_path"]
    strategy_discovery.require_committed(preflight_path)
    preflight = strategy_discovery.load_artifact(
        preflight_path,
        expected_kind="discovery-preflight-inspection",
    )
    prior = source.v13.prior_statistics()
    failure = source.failure_inspection()
    trial_ids = {
        str(row["trial_id"]) for row in contract["trial_family"]
    }
    request_rows = contract["development_data_requests"]
    excluded = set(failure["exposure_scope"]["symbols"])
    checks = {
        "search_hash_valid": search["artifact_sha256"]
        == strategy_discovery._hash(
            {
                key: value
                for key, value in search.items()
                if key != "artifact_sha256"
            }
        ),
        "search_state_frozen": search["state"] == "SEARCH_FROZEN",
        "contract_exactly_rebuilt": contract == rebuilt,
        "preflight_capacity_ready": preflight["state"]
        == "CAPACITY_READY"
        and preflight["verified_capacity"]
        == source.EXPECTED_DEVELOPMENT_EVENTS,
        "complete_current_32_trials": search["trial_count"] == 32
        and len(contract["trial_family"]) == 32,
        "complete_cumulative_64_trials": contract[
            "selection_accounting"
        ]["cumulative_trial_count"]
        == 64
        and len(contract["prior_trial_sharpes"]) == 32
        and len(contract["prior_trial_p_values"]) == 32,
        "prior_paths_exact": contract[
            "prior_trial_daily_returns_by_id"
        ]
        == prior["prior_trial_daily_returns_by_id"]
        and set(contract["prior_trial_daily_returns_by_id"])
        == trial_ids,
        "prior_statistics_exact": contract["prior_trial_sharpes"]
        == prior["prior_trial_sharpes"]
        and contract["prior_trial_p_values"]
        == prior["prior_trial_p_values"],
        "complete_608_requests": len(request_rows)
        == source.EXPECTED_DEVELOPMENT_SYMBOLS
        and {
            str(request["symbol"]) for request in request_rows
        }
        == set(contract["universe"]["symbols"]),
        "request_hashes_valid": all(
            row.get("request_sha256")
            == capacity.self_hash(row, "request_sha256")
            for row in request_rows
        ),
        "opened_prefix_fully_excluded": set(
            contract["universe"]["excluded_symbols"]
        )
        == excluded
        and len(excluded) == source.EXPECTED_EXCLUDED_SYMBOLS
        and not excluded.intersection(contract["universe"]["symbols"])
        and contract["universe"]["v13_opened_prefix_reused"] is False,
        "source_failure_exact": contract[
            "prior_source_failure_lineage"
        ]["inspection_sha256"]
        == failure["inspection_sha256"]
        and contract["prior_source_failure_lineage"][
            "retained_tasks_reused"
        ]
        is False,
        "invalid_ohlcv_policy_frozen": contract[
            "development_data_policy"
        ]["invalid_ohlcv"]
        == "whole_symbol_permanent_missing_zero_credit"
        and contract["development_data_policy"]["malformed_ohlcv"]
        == "fail_closed",
        "selection_aware_rule_frozen": contract["selection_mode"]
        == "development_search"
        and contract["winner_selection"] == DEVELOPMENT_SEARCH_RULE,
        "confirmation_symbol_disjoint": not set(
            contract["universe"]["symbols"]
        ).intersection(contract["universe"]["confirmation_symbols"])
        and contract["confirmation_data_reserve"][
            "symbol_disjoint_from_development"
        ]
        is True,
        "confirmation_closed": search[
            "confirmation_access_permitted"
        ]
        is False
        and contract["confirmation_data_reserve"][
            "authorized_only_after_frozen_winner"
        ]
        is True,
        "outcomes_absent": search["outcomes_accessed"] is False,
        "broker_actions_zero": search["broker_actions_permitted"]
        is False,
        "capacity_metadata_only": capacity_manifest[
            "dataset_payload"
        ]["dense_capacity"]["external_dataset_opened"]
        is False,
    }
    index = outcome_exposure.read_index()
    try:
        outcome_exposure.assert_untouched(
            contract["development_scope"], index
        )
        checks["development_scope_untouched"] = True
    except outcome_exposure.OutcomeExposureError:
        checks["development_scope_untouched"] = False
    try:
        outcome_exposure.assert_untouched(
            contract["confirmation_scope"], index
        )
        checks["confirmation_scope_untouched"] = True
    except outcome_exposure.OutcomeExposureError:
        checks["confirmation_scope_untouched"] = False
    if not all(checks.values()):
        raise EarningsSecReactionV14SearchInspectionError(
            "v14 frozen search inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v14-search-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "REACTION_V14_SEARCH_INSPECTED_READY_FOR_COLLECTION",
        "inspected_at": capacity._timestamp(
            inspected_at, "inspected_at"
        ),
        "search_path": capacity._repo_path(search_path),
        "search_file_sha256": sha256_file(search_path),
        "search_sha256": search["artifact_sha256"],
        "checks": checks,
        "development_collection_authorized": True,
        "authorized_provider_requests": len(request_rows),
        "excluded_v13_symbols": len(excluded),
        "cumulative_trial_count": 64,
        "confirmation_provider_access_authorized": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "search-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    capacity._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("search", type=Path)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value = inspect_search(
        args.search, inspected_at=args.inspected_at
    )
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
                "authorized_provider_requests": value[
                    "authorized_provider_requests"
                ],
                "excluded_v13_symbols": value[
                    "excluded_v13_symbols"
                ],
                "confirmation_provider_access_authorized": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
