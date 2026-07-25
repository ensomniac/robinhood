"""Independently inspect the frozen v11 SEC reaction search."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as metadata
import earnings_sec_reaction_v11_search as source
import outcome_exposure
import strategy_discovery
from historical_store import sha256_file
from learning_data import load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


class EarningsSecReactionV11SearchInspectionError(RuntimeError):
    """The v11 pre-outcome search could not be reconstructed."""


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
    capacity_path = metadata.PROJECT_ROOT / contract["capacity_manifest"]
    strategy_discovery.require_committed(capacity_path)
    capacity = load_frozen_dataset_contract(capacity_path)
    rebuilt = source.build_contract(
        created_at=contract["created_at"],
        capacity_manifest=capacity_path,
    )
    preflight_path = metadata.PROJECT_ROOT / search["preflight_path"]
    strategy_discovery.require_committed(preflight_path)
    preflight = strategy_discovery.load_artifact(
        preflight_path,
        expected_kind="discovery-preflight-inspection",
    )
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
        and preflight["verified_capacity"] == 147,
        "complete_32_trials": search["trial_count"] == 32
        and len(contract["trial_family"]) == 32,
        "six_predecessor_symbols_excluded": contract["universe"][
            "excluded_symbols"
        ]
        == source.EXCLUDED_SYMBOLS
        and not set(source.EXCLUDED_SYMBOLS).intersection(
            contract["universe"]["symbols"]
        ),
        "exact_108_requests": len(
            contract["development_data_requests"]
        )
        == 108
        and {
            request["symbol"]
            for request in contract["development_data_requests"]
        }
        == set(contract["universe"]["symbols"]),
        "http_400_policy_frozen": contract[
            "development_data_policy"
        ]["http_400"]
        == "permanent_missing_zero_credit",
        "selection_aware_rule_frozen": contract["selection_mode"]
        == "development_search"
        and contract["winner_selection"]
        == DEVELOPMENT_SEARCH_RULE,
        "confirmation_closed": search["confirmation_access_permitted"]
        is False,
        "outcomes_absent": search["outcomes_accessed"] is False,
        "broker_actions_zero": search["broker_actions_permitted"] is False,
        "capacity_metadata_only": capacity["dataset_payload"][
            "dense_capacity"
        ]["external_dataset_opened"]
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
        raise EarningsSecReactionV11SearchInspectionError(
            "v11 frozen search inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v11-search-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "REACTION_V11_SEARCH_INSPECTED_READY_FOR_COLLECTION",
        "inspected_at": metadata._timestamp(
            inspected_at, "inspected_at"
        ),
        "search_path": metadata._repo_path(search_path),
        "search_file_sha256": sha256_file(search_path),
        "search_sha256": search["artifact_sha256"],
        "checks": checks,
        "development_collection_authorized": True,
        "authorized_provider_requests": 108,
        "confirmation_provider_access_authorized": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = v5.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "search-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    metadata._write(path, value)
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
