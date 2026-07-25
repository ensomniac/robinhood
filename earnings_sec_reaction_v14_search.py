"""Freeze the v14 SEC reaction search on the untouched v13 remainder.

V13 terminated after a provider schema failure on its twenty-seventh symbol.
This successor permanently excludes the complete opened prefix and preserves
the original trial family, cumulative selection correction, and sealed
confirmation reserve.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_corrected_expansion as capacity
import earnings_sec_market_data as market
import earnings_sec_reaction_v13_search as v13
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v13.CAMPAIGN_ID
FAMILY_ID = v13.FAMILY_ID
SUCCESSOR_ID = (
    "earnings-positive-surprise-drift-v14-sec-2012-2015-"
    "invalid-ohlcv-policy"
)
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
FAILURE_INSPECTION = (
    v13.DEFAULT_ROOT
    / "development-source-failure-inspection"
    / "inspection-"
    "565528f8164b8cfcc9cdb40c50c14cda44bcea5e25e4818272a4ce20311db901"
    ".json"
)
EXPECTED_EXCLUDED_SYMBOLS = 27
EXPECTED_DEVELOPMENT_EVENTS = 1019
EXPECTED_DEVELOPMENT_SIGNAL_DATES = 499
EXPECTED_DEVELOPMENT_SYMBOLS = 608
EXPECTED_CONFIRMATION_EVENTS = v13.EXPECTED_CONFIRMATION_EVENTS
EXPECTED_CONFIRMATION_SIGNAL_DATES = v13.EXPECTED_CONFIRMATION_SIGNAL_DATES
EXPECTED_CONFIRMATION_SYMBOLS = v13.EXPECTED_CONFIRMATION_SYMBOLS


class EarningsSecReactionV14SearchError(RuntimeError):
    """The v14 untouched-remainder search boundary drifted."""


def failure_inspection() -> dict[str, Any]:
    strategy_discovery.require_committed(FAILURE_INSPECTION)
    value = capacity._read(FAILURE_INSPECTION)
    exposure_id = value.get("exposure_id")
    if not (
        value.get("inspection_sha256")
        == capacity.self_hash(value, "inspection_sha256")
        and value.get("inspection_sha256")
        == "565528f8164b8cfcc9cdb40c50c14cda44bcea5e25e4818272a4ce20311db901"
        and value.get("state")
        == "REACTION_V13_INVALID_OHLCV_INSPECTED_TERMINAL"
        and value.get("valid") is True
        and value.get("remaining_608_symbols_successor_permitted") is True
        and value.get("successor_search_freeze_required_before_access") is True
        and value.get("invalid_ohlcv_permanent_missing_policy_permitted") is True
        and value.get("confirmation_access_authorized") is False
        and len(value.get("exposure_scope", {}).get("symbols", []))
        == EXPECTED_EXCLUDED_SYMBOLS
        and any(
            record.get("exposure_id") == exposure_id
            for record in outcome_exposure.read_index()
        )
    ):
        raise EarningsSecReactionV14SearchError(
            "v13 source failure is not independently closed and indexed"
        )
    return value


def selection(
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    failure = failure_inspection()
    original = v13.selection(store)
    excluded = sorted(
        str(symbol) for symbol in failure["exposure_scope"]["symbols"]
    )
    excluded_set = set(excluded)
    selected = copy.deepcopy(original)
    selected["successor_id"] = SUCCESSOR_ID
    selected["development_symbols"] = sorted(
        set(original["development_symbols"]) - excluded_set
    )
    selected["development_requests"] = [
        row
        for row in original["development_requests"]
        if str(row["symbol"]) not in excluded_set
    ]
    selected["development_metadata_by_date"] = {
        day: [
            row
            for row in rows
            if str(row["symbol"]) not in excluded_set
        ]
        for day, rows in original["development_metadata_by_date"].items()
    }
    selected["development_signal_dates"] = [
        day
        for day, rows in selected["development_metadata_by_date"].items()
        if rows
    ]
    selected["development_event_count"] = sum(
        len(rows)
        for rows in selected["development_metadata_by_date"].values()
    )
    selected["permanently_excluded_symbols"] = excluded
    selected["v13_failure_inspection_sha256"] = failure[
        "inspection_sha256"
    ]
    selected["v13_opened_prefix_reused"] = False
    if not (
        len(excluded) == EXPECTED_EXCLUDED_SYMBOLS
        and excluded_set <= set(original["development_symbols"])
        and len(selected["development_symbols"])
        == EXPECTED_DEVELOPMENT_SYMBOLS
        and len(selected["development_requests"])
        == EXPECTED_DEVELOPMENT_SYMBOLS
        and selected["development_event_count"]
        == EXPECTED_DEVELOPMENT_EVENTS
        and len(selected["development_signal_dates"])
        == EXPECTED_DEVELOPMENT_SIGNAL_DATES
        and selected["confirmation_event_count"]
        == EXPECTED_CONFIRMATION_EVENTS
        and len(selected["confirmation_signal_dates"])
        == EXPECTED_CONFIRMATION_SIGNAL_DATES
        and len(selected["confirmation_symbols"])
        == EXPECTED_CONFIRMATION_SYMBOLS
        and not set(selected["development_symbols"]).intersection(
            selected["confirmation_symbols"]
        )
        and not excluded_set.intersection(selected["development_symbols"])
        and selected["market_prices_accessed"] is False
        and selected["confirmation_prices_accessed"] is False
    ):
        raise EarningsSecReactionV14SearchError(
            "v14 event selection or exclusion boundary differs"
        )
    return selected


def build_contract(
    *,
    created_at: str,
    capacity_manifest: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    selected = selection(store)
    failure = failure_inspection()
    contract = v13.build_contract(
        created_at=created_at,
        capacity_manifest=capacity_manifest,
        store=store,
        selected_override=selected,
    )
    contract.update(
        {
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "parent_experiment_id": f"experiment-{v13.SUCCESSOR_ID}",
            "material_difference_rationale": (
                "V13 terminated before strategy evaluation when Yahoo returned "
                "structurally invalid OHLCV on request ordinal 26. V14 makes "
                "all 27 opened symbols permanently ineligible, uses only the "
                "remaining 608 globally untouched symbols, and prospectively "
                "classifies the same invalid-OHLCV condition as whole-symbol "
                "permanent missing with zero signal credit."
            ),
            "contamination_risks": [
                "The prior 32 evaluated trials remain adverse history and enter every cumulative correction.",
                "All 27 symbols opened by v13 are permanently excluded from v14.",
                "No retained v13 task or price row is reused by this successor.",
                "Development uses only globally untouched 2011-2014 date-symbol price scope.",
                "Confirmation symbols are absent from development and remain sealed before a winner freeze.",
            ],
            "universe": {
                **contract["universe"],
                "excluded_symbols": selected[
                    "permanently_excluded_symbols"
                ],
                "v13_opened_prefix_reused": False,
            },
            "development_data_policy": {
                **contract["development_data_policy"],
                "invalid_ohlcv": (
                    "whole_symbol_permanent_missing_zero_credit"
                ),
                "malformed_ohlcv": "fail_closed",
                "range_escape": "fail_closed",
                "other_http_or_schema_error": "fail_closed",
            },
            "prior_source_failure_lineage": {
                "inspection_path": market._repo_path(
                    FAILURE_INSPECTION
                ),
                "inspection_file_sha256": sha256_file(
                    FAILURE_INSPECTION
                ),
                "inspection_sha256": failure["inspection_sha256"],
                "exposure_id": failure["exposure_id"],
                "excluded_symbols": selected[
                    "permanently_excluded_symbols"
                ],
                "remaining_untouched_symbols": (
                    EXPECTED_DEVELOPMENT_SYMBOLS
                ),
                "same_version_resume_permitted": False,
                "retained_tasks_reused": False,
            },
            "falsifiers": [
                *contract["falsifiers"],
                "any reuse of a v13 task, row, or opened symbol",
                "any invalid-OHLCV response not retained as whole-symbol zero credit",
                "any change to the prospectively frozen source-error policy",
            ],
            "implementation_files": [
                "earnings_sec_reaction_v14_search.py",
                "earnings_sec_reaction_v14_search_inspection.py",
                "earnings_sec_reaction_v14_collection.py",
                "earnings_sec_reaction_v14_collection_inspection.py",
                "earnings_sec_reaction_v13_search.py",
                "earnings_sec_corrected_expansion.py",
                "earnings_sec_corrected_expansion_inspection.py",
                "dense_strategy_runtime.py",
                "dense_strategy_plugin.py",
                "learning_statistics.py",
                "learning_experiment.py",
                "strategy_discovery.py",
                "outcome_exposure.py",
                "portfolio_maturity.py",
                "portfolio_config.toml",
            ],
            "capacity_manifest": market._repo_path(
                capacity_manifest
            ),
        }
    )
    return strategy_discovery._validate_family_contract(contract)


def freeze_family(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any], Path]:
    for raw in (
        "earnings_sec_reaction_v14_search.py",
        "earnings_sec_reaction_v14_search_inspection.py",
        "earnings_sec_reaction_v14_collection.py",
        "earnings_sec_reaction_v14_collection_inspection.py",
        "earnings_sec_reaction_v13_search.py",
        "dense_strategy_runtime.py",
        "dense_strategy_plugin.py",
        "learning_experiment.py",
    ):
        strategy_discovery.require_committed(PROJECT_ROOT / raw)
    selected = selection(store)
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": market._timestamp(
                created_at, "created_at"
            ),
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    market._repo_path(v13.CAPACITY_INSPECTION),
                    market._repo_path(FAILURE_INSPECTION),
                    market._repo_path(v13.PRIOR_RESULT),
                    market._repo_path(v13.PRIOR_INSPECTION),
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
                    "excluded_v13_symbols": (
                        EXPECTED_EXCLUDED_SYMBOLS
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
    digest = hashlib.sha256(
        capacity.canonical_bytes(contract)
    ).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    market._write(path, contract)
    return path, contract, capacity_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "freeze-family"))
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    if args.command == "status":
        selected = selection()
        value = {
            "state": "READY_TO_FREEZE",
            "successor_id": SUCCESSOR_ID,
            "calendar_wait_required": False,
            "development_events": selected["development_event_count"],
            "development_symbols": len(
                selected["development_symbols"]
            ),
            "excluded_symbols": len(
                selected["permanently_excluded_symbols"]
            ),
            "confirmation_events": selected[
                "confirmation_event_count"
            ],
            "provider_requests_permitted": 0,
        }
    else:
        if not args.created_at:
            raise EarningsSecReactionV14SearchError(
                "--created-at is required"
            )
        path, contract, capacity_path = freeze_family(
            created_at=args.created_at
        )
        value = {
            "state": "FAMILY_FROZEN",
            "path": market._repo_path(path),
            "capacity_path": market._repo_path(capacity_path),
            "trial_count": len(contract["trial_family"]),
            "cumulative_trial_count": contract[
                "selection_accounting"
            ]["cumulative_trial_count"],
            "development_events": contract["universe"][
                "development_event_count"
            ],
            "provider_requests_permitted": 0,
        }
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
