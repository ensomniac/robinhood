"""Freeze the v11 SEC reaction search after the terminal v10 HTTP 400."""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as metadata
import earnings_sec_reaction_search as v10
import earnings_sec_yahoo_failure as v9_failure
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v10.CAMPAIGN_ID
FAMILY_ID = v10.FAMILY_ID
SUCCESSOR_ID = "earnings-positive-surprise-drift-v11-sec-reaction-search"
STRATEGY_ID = v10.STRATEGY_ID
MECHANISM_FAMILY = v10.MECHANISM_FAMILY
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
V10_TERMINAL_INSPECTION = (
    v10.DEFAULT_ROOT
    / "development-source-failure-inspection"
    / "inspection-167e56c232c1cb4d22a0fc09764e892111b4ff412812b79631249d6df92a57b4.json"
)
FAILED_V10_SYMBOL = "APC"
EXCLUDED_SYMBOLS = sorted(
    [*v9_failure.EXPOSED_SYMBOLS, FAILED_V10_SYMBOL]
)
V10_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / FAMILY_ID
    / "search"
    / (
        f"{FAMILY_ID}-search-"
        "0fc621564e338e96b6f3641a145cce3d5fd4fc4da35256c3dce9887da3399f1e"
        ".json"
    )
)


class EarningsSecReactionV11SearchError(RuntimeError):
    """The bounded v11 successor search boundary drifted."""


def selection(
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    strategy_discovery.require_committed(V10_TERMINAL_INSPECTION)
    terminal = metadata._read(V10_TERMINAL_INSPECTION)
    if not (
        terminal.get("state")
        == "REACTION_V10_SOURCE_FAILURE_INSPECTED_TERMINAL"
        and terminal.get("valid") is True
        and terminal.get("v10_promotion_eligible") is False
        and terminal.get("remaining_108_symbols_successor_permitted")
        is True
        and terminal.get("successor_search_freeze_required_before_access")
        is True
        and terminal.get("successor_http_400_missing_policy_permitted")
        is True
        and terminal.get("excluded_successor_symbol")
        == FAILED_V10_SYMBOL
    ):
        raise EarningsSecReactionV11SearchError(
            "v10 source-policy failure is not independently terminal"
        )
    base = v10.selection(store)
    development_metadata = {
        day: [
            row
            for row in rows
            if row["symbol"] != FAILED_V10_SYMBOL
        ]
        for day, rows in base["development_metadata_by_date"].items()
    }
    development_symbols = sorted(
        {
            row["symbol"]
            for rows in development_metadata.values()
            for row in rows
        }
    )
    development_requests = [
        request
        for request in base["development_requests"]
        if request["symbol"] != FAILED_V10_SYMBOL
    ]
    development_signal_dates = [
        day for day, rows in development_metadata.items() if rows
    ]
    result = {
        **base,
        "successor_id": SUCCESSOR_ID,
        "development_signal_dates": development_signal_dates,
        "development_event_count": sum(
            len(rows) for rows in development_metadata.values()
        ),
        "development_symbols": development_symbols,
        "development_metadata_by_date": development_metadata,
        "development_requests": development_requests,
        "excluded_pre_search_symbols": EXCLUDED_SYMBOLS,
        "v10_terminal_inspection_sha256": terminal[
            "inspection_sha256"
        ],
    }
    if not (
        result["development_event_count"] == 147
        and len(development_signal_dates) == 82
        and len(development_symbols) == 108
        and len(development_requests) == 108
        and FAILED_V10_SYMBOL not in development_symbols
        and FAILED_V10_SYMBOL not in result["confirmation_symbols"]
        and result["confirmation_event_count"] == 229
        and len(result["confirmation_signal_dates"]) == 116
        and len(result["confirmation_symbols"]) == 184
    ):
        raise EarningsSecReactionV11SearchError(
            "v11 capacity or untouched request graph differs"
        )
    return result


def build_contract(
    *,
    created_at: str,
    capacity_manifest: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    selected = selection(store)
    strategy_discovery.require_committed(V10_SEARCH)
    frozen_v10 = strategy_discovery.load_artifact(
        V10_SEARCH, expected_kind="frozen-development-search"
    )
    contract = copy.deepcopy(frozen_v10["family_contract"])
    for field in (
        "implementation_hashes",
        "trial_family",
        "primary_trial_id",
        "rolling_origin_plan",
    ):
        contract.pop(field, None)
    development_scope = {
        "dates": selected["development_opened_dates"],
        "symbols": selected["development_symbols"],
    }
    confirmation_scope = {
        "dates": selected["confirmation_dates"],
        "symbols": selected["confirmation_symbols"],
    }
    index = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, index)
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    contract.update(
        {
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "parent_experiment_id": frozen_v10["family_contract"][
                "experiment_id"
            ],
            "created_at": metadata._timestamp(created_at, "created_at"),
            "status": "INVENTED",
            "material_difference_rationale": (
                "V10 is independently terminal after the first APC request "
                "returned an unregistered HTTP 400 with zero retained rows. "
                "V11 permanently excludes APC, preserves the exact strategy "
                "grid, and prospectively classifies HTTP 400 as permanent "
                "missing before accessing any of the remaining 108 symbols."
            ),
            "contamination_risks": [
                "Five v9 symbols are globally exposed and permanently excluded.",
                "APC had a v10 HTTP 400 and is permanently excluded without retry.",
                "The remaining 108 symbols are untouched before this freeze.",
                "Confirmation prices remain globally untouched and inaccessible.",
            ],
            "development_dates": selected["development_dates"],
            "development_signal_dates": selected[
                "development_signal_dates"
            ],
            "embargo_dates": selected["embargo_dates"],
            "confirmation_dates": selected["confirmation_dates"],
            "confirmation_signal_dates": selected[
                "confirmation_signal_dates"
            ],
            "confirmation_signal_capacity": len(
                selected["confirmation_signal_dates"]
            ),
            "development_scope": development_scope,
            "confirmation_scope": confirmation_scope,
            "outcome_exposure_index_sha256": outcome_exposure.audit()[
                "index_sha256"
            ],
            "universe": {
                "point_in_time": True,
                "security_type": (
                    "SEC same-accession verified common equity"
                ),
                "excluded_symbols": EXCLUDED_SYMBOLS,
                "symbols": selected["development_symbols"],
                "selection_sha256": canonical_sha256(selected),
                "development_event_count": selected[
                    "development_event_count"
                ],
                "confirmation_event_count": selected[
                    "confirmation_event_count"
                ],
            },
            "development_data_requests": selected[
                "development_requests"
            ],
            "development_data_policy": {
                "source": "Yahoo Finance historical chart JSON",
                "authorized_requests": 108,
                "pacing_seconds": 0.20,
                "retries": 0,
                "substitutions": 0,
                "http_400": "permanent_missing_zero_credit",
                "http_404": "permanent_missing_zero_credit",
                "identity_schema_mismatch": (
                    "permanent_missing_zero_credit"
                ),
                "other_http_or_schema_error": "fail_closed",
                "confirmation_requests": 0,
            },
            "implementation_files": [
                "earnings_sec_reaction_v11_search.py",
                "earnings_sec_reaction_v11_search_inspection.py",
                "earnings_sec_reaction_v11_collection.py",
                "earnings_sec_reaction_v11_collection_inspection.py",
                "dense_strategy_runtime.py",
                "dense_strategy_plugin.py",
                "learning_statistics.py",
                "learning_experiment.py",
                "strategy_discovery.py",
                "outcome_exposure.py",
                "portfolio_maturity.py",
                "portfolio_config.toml",
            ],
            "capacity_manifest": metadata._repo_path(
                capacity_manifest
            ),
        }
    )
    contract["universe_requirements"] = {
        **contract["universe_requirements"],
        "excluded_symbols": EXCLUDED_SYMBOLS,
    }
    contract["falsifiers"] = [
        item
        for item in contract["falsifiers"]
        if "five pre-search exposed symbols" not in item
    ] + ["any use of the six quarantined predecessor symbols"]
    return strategy_discovery._validate_family_contract(contract)


def freeze_family(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any], Path]:
    for raw in (
        "earnings_sec_reaction_v11_search.py",
        "earnings_sec_reaction_v11_search_inspection.py",
        "earnings_sec_reaction_v11_collection.py",
        "earnings_sec_reaction_v11_collection_inspection.py",
        "dense_strategy_runtime.py",
        "dense_strategy_plugin.py",
    ):
        strategy_discovery.require_committed(PROJECT_ROOT / raw)
    selected = selection(store)
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": metadata._timestamp(
                created_at, "created_at"
            ),
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    metadata._repo_path(V10_TERMINAL_INSPECTION),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": False,
                "confirmation_access_permitted": False,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": selected[
                        "development_event_count"
                    ],
                    "signal_dates": len(
                        selected["development_signal_dates"]
                    ),
                    "confirmation_signal_dates": len(
                        selected["confirmation_signal_dates"]
                    ),
                    "external_dataset_opened": False,
                },
            },
        },
        root / "capacity",
    )
    contract = build_contract(
        created_at=created_at,
        capacity_manifest=capacity_path,
        store=store,
    )
    digest = hashlib.sha256(v5.canonical_bytes(contract)).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    metadata._write(path, contract)
    return path, contract, capacity_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "freeze-family"))
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    if args.command == "status":
        value = {
            "state": "READY_TO_FREEZE",
            "successor_id": SUCCESSOR_ID,
            "calendar_wait_required": False,
            "provider_requests_permitted": 0,
        }
    else:
        if not args.created_at:
            raise EarningsSecReactionV11SearchError(
                "--created-at is required"
            )
        path, contract, capacity = freeze_family(
            created_at=args.created_at
        )
        value = {
            "state": "FAMILY_FROZEN",
            "path": metadata._repo_path(path),
            "capacity_path": metadata._repo_path(capacity),
            "trial_count": len(contract["trial_family"]),
            "development_events": contract["universe"][
                "development_event_count"
            ],
            "provider_requests_permitted": 0,
        }
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
