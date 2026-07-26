"""Freeze the unchanged-grid Form 4 replication on the untouched v1 reserve.

The prior 32-trial family remains rejected adverse history.  This controller
uses only its never-opened 2023-2024 reserve, discards the sparse 2023 segment
outcome-blind, creates chronological development/embargo/confirmation slices,
and carries all prior trial paths into the cumulative 64-trial corrections.
"""

from __future__ import annotations

import argparse
import copy
import gzip
import hashlib
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from statistics import NormalDist
from typing import Any

import insider_purchase_discovery as v1
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract
from learning_statistics import (
    annualized_sharpe,
    probability_of_backtest_overfitting,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v1.CAMPAIGN_ID
FAMILY_ID = (
    "clustered-form4-open-market-purchase-continuation-replication-v2"
)
MECHANISM_FAMILY = v1.MECHANISM_FAMILY
VERSION_ID = FAMILY_ID
EXPERIMENT_ID = f"{VERSION_ID}-development-search"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/discovery" / FAMILY_ID
)
CAPACITY_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/form4_insider_purchase/replication-v2/capacity"
)
PRIVATE_NAMESPACE = Path("_derived/form4_insider_purchase_replication_v2")
PRIOR_SEARCH = next(
    (
        PROJECT_ROOT
        / "strategy_tournament/v2/discovery"
        / v1.FAMILY_ID
        / "search"
    ).glob("*.json")
)
PRIOR_RESULT = next(
    (
        PROJECT_ROOT
        / "strategy_tournament/v2/discovery"
        / v1.FAMILY_ID
        / "development"
    ).glob("*.json")
)
PRIOR_INSPECTION = next(
    (
        PROJECT_ROOT
        / "strategy_tournament/v2/discovery"
        / v1.FAMILY_ID
        / "development-inspection"
    ).glob("*.json")
)
PRIOR_CAPACITY = next(v1.MANIFEST_ROOT.glob("*.json"))
SOURCE_HISTORY_START = "2024-01-02"
DEVELOPMENT_START = "2024-03-07"
DEVELOPMENT_SIGNAL_END = "2024-09-19"
DEVELOPMENT_END = "2024-09-25"
EMBARGO_START = "2024-09-26"
EMBARGO_END = "2024-10-02"
CONFIRMATION_START = "2024-10-03"
CONFIRMATION_END = "2024-12-31"


class InsiderPurchaseReplicationError(RuntimeError):
    """The replication lineage, evidence partition, or prior paths drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _calendar_slice(start: str, end: str) -> list[str]:
    return [day for day in v1._load_calendar() if start <= day <= end]


def _scope(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    symbols_by_date: dict[str, set[str]] = defaultdict(set)
    for event in events:
        for day in event["scope_dates"]:
            symbols_by_date[str(day)].add(str(event["symbol"]))
    dates = sorted(symbols_by_date)
    if not dates:
        raise InsiderPurchaseReplicationError("replication event scope is empty")
    return outcome_exposure.validate_scope(
        {
            "dates": dates,
            "symbols_by_date": {
                day: sorted(symbols_by_date[day]) for day in dates
            },
        }
    )


def _prior_capacity_inventory() -> tuple[dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(PRIOR_CAPACITY)
    manifest = load_frozen_dataset_contract(PRIOR_CAPACITY)
    capacity = manifest["dataset_payload"].get("form4_purchase_capacity")
    if not (
        isinstance(capacity, Mapping)
        and capacity.get("family_id") == v1.FAMILY_ID
        and capacity.get("experiment_id") == v1.EXPERIMENT_ID
        and capacity.get("confirmation_access_permitted") is False
        and capacity.get("market_prices_accessed") is False
        and capacity.get("provider_requests") == 0
    ):
        raise InsiderPurchaseReplicationError(
            "prior Form 4 capacity is not a sealed zero-price reserve"
        )
    binding = capacity.get("private_inventory")
    if not isinstance(binding, Mapping):
        raise InsiderPurchaseReplicationError(
            "prior private inventory binding is missing"
        )
    relative = Path(str(binding.get("relative_path", "")))
    if (
        binding.get("storage") != "LOCAL_HISTORICAL_DATA_ROOT"
        or relative.is_absolute()
        or ".." in relative.parts
    ):
        raise InsiderPurchaseReplicationError(
            "prior private inventory binding is unsafe"
        )
    path = HistoricalDayStore.from_env().root / relative
    if not path.is_file() or sha256_file(path) != binding.get("file_sha256"):
        raise InsiderPurchaseReplicationError(
            "prior private inventory file drifted"
        )
    with gzip.open(path, "rt", encoding="utf-8") as source:
        inventory = json.load(source)
    if (
        not isinstance(inventory, dict)
        or canonical_sha256(inventory) != binding.get("content_sha256")
        or inventory.get("family_id") != v1.FAMILY_ID
        or len(inventory.get("confirmation_events", [])) != 668
    ):
        raise InsiderPurchaseReplicationError(
            "prior private inventory content drifted"
        )
    return manifest, inventory


def _pair_clean(
    events: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    exposed: set[tuple[str, str]] = set()
    wildcard_dates: set[str] = set()
    for record in records:
        for day, symbol in outcome_exposure.scope_pairs(record["scope"]):
            if symbol == "*":
                wildcard_dates.add(day)
            else:
                exposed.add((day, symbol))
    clean: list[dict[str, Any]] = []
    contaminated: list[dict[str, Any]] = []
    for raw in events:
        event = dict(raw)
        target = (
            contaminated
            if any(
                day in wildcard_dates
                or (day, str(event["symbol"])) in exposed
                for day in event["scope_dates"]
            )
            else clean
        )
        target.append(event)
    return clean, contaminated


def selection() -> dict[str, Any]:
    _manifest, inventory = _prior_capacity_inventory()
    all_records = outcome_exposure.read_index()
    self_development_prefixes = (
        f"development-{FAMILY_ID}-",
        f"development-partial-{FAMILY_ID}-",
        f"development-source-{FAMILY_ID}-",
    )
    records = [
        record
        for record in all_records
        if not str(record["exposure_id"]).startswith(
            self_development_prefixes
        )
    ]
    clean, contaminated = _pair_clean(
        inventory["confirmation_events"], records
    )
    development_events = [
        event
        for event in clean
        if DEVELOPMENT_START
        <= str(event["entry_date"])
        <= DEVELOPMENT_SIGNAL_END
    ]
    confirmation_events = [
        event
        for event in clean
        if CONFIRMATION_START
        <= str(event["entry_date"])
        <= CONFIRMATION_END
    ]
    development_dates = _calendar_slice(
        DEVELOPMENT_START, DEVELOPMENT_END
    )
    embargo_dates = _calendar_slice(EMBARGO_START, EMBARGO_END)
    confirmation_dates = _calendar_slice(
        CONFIRMATION_START, CONFIRMATION_END
    )
    development_scope = _scope(development_events)
    confirmation_scope = _scope(confirmation_events)
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    if not (
        len(contaminated) == 0
        and len(development_events) == 424
        and len({row["entry_date"] for row in development_events}) == 94
        and len(confirmation_events) == 209
        and len({row["entry_date"] for row in confirmation_events}) == 46
        and len(embargo_dates) == 5
        and development_dates[-1] < embargo_dates[0]
        and embargo_dates[-1] < confirmation_dates[0]
        and all(
            max(event["scope_dates"]) <= DEVELOPMENT_END
            for event in development_events
        )
        and all(
            min(event["scope_dates"]) >= CONFIRMATION_START
            for event in confirmation_events
        )
    ):
        raise InsiderPurchaseReplicationError(
            "frozen replication partition capacity drifted"
        )
    return {
        "development_dates": development_dates,
        "development_signal_dates": sorted(
            {row["entry_date"] for row in development_events}
        ),
        "development_events": development_events,
        "development_scope": development_scope,
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": sorted(
            {row["entry_date"] for row in confirmation_events}
        ),
        "confirmation_events": confirmation_events,
        "confirmation_scope": confirmation_scope,
        "contaminated_events_removed_without_replacement": len(contaminated),
        "outcome_exposure_index_sha256": sha256_file(
            outcome_exposure.DEFAULT_INDEX
        ),
    }


def prior_statistics() -> dict[str, Any]:
    for path in (PRIOR_SEARCH, PRIOR_RESULT, PRIOR_INSPECTION):
        strategy_discovery.require_committed(path)
    search = strategy_discovery.load_artifact(
        PRIOR_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        PRIOR_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        PRIOR_INSPECTION, expected_kind="development-search-inspection"
    )
    if not (
        search.get("trial_count") == 32
        and search.get("artifact_sha256") == result.get("search_sha256")
        and result.get("artifact_sha256") == inspection.get("result_sha256")
        and inspection.get("state") == "REJECTED"
        and inspection.get("selection", {}).get("status") == "REJECTED"
    ):
        raise InsiderPurchaseReplicationError(
            "prior rejected 32-trial lineage is invalid"
        )
    evaluation = strategy_discovery._load_development_evaluation(
        result, root=strategy_discovery.DEFAULT_ROOT
    )
    trials = sorted(evaluation["trials"], key=lambda row: row["trial_id"])
    paths: dict[str, list[float]] = {}
    sharpes: list[float] = []
    p_values: list[float] = []
    for trial in trials:
        trial_id = str(trial["trial_id"])
        values = [
            float(value)
            for value in trial["metrics"]["oof_daily_account_returns"]
        ]
        mean = statistics.fmean(values)
        deviation = statistics.stdev(values) if len(values) > 1 else 0.0
        statistic = (
            mean / (deviation / math.sqrt(len(values)))
            if deviation
            else 0.0
        )
        paths[trial_id] = values
        sharpes.append(float(annualized_sharpe(values) or 0.0))
        p_values.append(1 - NormalDist().cdf(statistic))
    pbo = probability_of_backtest_overfitting(paths)["probability"]
    frozen_ids = {
        str(row["trial_id"])
        for row in search["family_contract"]["trial_family"]
    }
    if (
        len(paths) != 32
        or set(paths) != frozen_ids
        or pbo is None
    ):
        raise InsiderPurchaseReplicationError(
            "prior trial return paths are incomplete"
        )
    return {
        "prior_search_path": _relative(PRIOR_SEARCH),
        "prior_search_file_sha256": sha256_file(PRIOR_SEARCH),
        "prior_search_sha256": search["artifact_sha256"],
        "prior_result_path": _relative(PRIOR_RESULT),
        "prior_result_file_sha256": sha256_file(PRIOR_RESULT),
        "prior_result_sha256": result["artifact_sha256"],
        "prior_inspection_path": _relative(PRIOR_INSPECTION),
        "prior_inspection_file_sha256": sha256_file(PRIOR_INSPECTION),
        "prior_inspection_sha256": inspection["artifact_sha256"],
        "prior_evaluation_binding": dict(result["evaluation_binding"]),
        "prior_trial_sharpes": sharpes,
        "prior_trial_p_values": p_values,
        "prior_trial_daily_returns_by_id": paths,
        "prior_standalone_pbo_probability": float(pbo),
        "cumulative_pbo_method": (
            "concatenate matching unchanged-grid OOF paths across "
            "chronologically disjoint corpora"
        ),
    }


def _write_private_inventory(value: Mapping[str, Any]) -> dict[str, Any]:
    content_sha256 = canonical_sha256(value)
    relative = (
        PRIVATE_NAMESPACE
        / content_sha256
        / f"inventory-{content_sha256}.json.gz"
    )
    path = HistoricalDayStore.from_env().root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = path.with_suffix(".json.gz.tmp")
        with gzip.open(temporary, "wt", encoding="utf-8") as destination:
            json.dump(value, destination, sort_keys=True, separators=(",", ":"))
        temporary.replace(path)
    with gzip.open(path, "rt", encoding="utf-8") as source:
        rebuilt = json.load(source)
    if canonical_sha256(rebuilt) != content_sha256:
        raise InsiderPurchaseReplicationError(
            "private replication inventory drifted"
        )
    return {
        "storage": "LOCAL_HISTORICAL_DATA_ROOT",
        "relative_path": str(relative),
        "content_sha256": content_sha256,
        "file_sha256": sha256_file(path),
        "compression": "gzip",
        "format": "canonical-json",
    }


def _inventory(selected: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "version_id": VERSION_ID,
        "parent_family_id": v1.FAMILY_ID,
        "parent_capacity_manifest": _relative(PRIOR_CAPACITY),
        "parent_capacity_manifest_sha256": sha256_file(PRIOR_CAPACITY),
        "outcome_exposure_index_sha256": selected[
            "outcome_exposure_index_sha256"
        ],
        "selection": {
            "grid": "identical_to_parent_32_trial_grid",
            "sparse_2023_segment_disposition": (
                "discarded_outcome_blind_before_any_market_price_access"
            ),
            "development_entry_range": [
                DEVELOPMENT_START,
                DEVELOPMENT_SIGNAL_END,
            ],
            "development_exit_calendar_end": DEVELOPMENT_END,
            "embargo_range": [EMBARGO_START, EMBARGO_END],
            "confirmation_entry_range": [
                CONFIRMATION_START,
                CONFIRMATION_END,
            ],
            "contaminated_events": (
                "exclude_without_replacement_before_freeze"
            ),
        },
        "development_events": selected["development_events"],
        "confirmation_events": selected["confirmation_events"],
        "counts": {
            "development_events": len(selected["development_events"]),
            "development_entry_dates": len(
                selected["development_signal_dates"]
            ),
            "development_symbols": len(
                {row["symbol"] for row in selected["development_events"]}
            ),
            "confirmation_events": len(selected["confirmation_events"]),
            "confirmation_entry_dates": len(
                selected["confirmation_signal_dates"]
            ),
            "confirmation_symbols": len(
                {row["symbol"] for row in selected["confirmation_events"]}
            ),
            "contaminated_events_removed_without_replacement": selected[
                "contaminated_events_removed_without_replacement"
            ],
        },
    }


def build_contract(
    *,
    created_at: str,
    capacity_manifest: Path,
    selected: Mapping[str, Any],
) -> dict[str, Any]:
    prior = prior_statistics()
    parent = strategy_discovery.load_artifact(
        PRIOR_SEARCH, expected_kind="frozen-development-search"
    )
    contract = copy.deepcopy(parent["family_contract"])
    for field in (
        "implementation_hashes",
        "trial_family",
        "primary_trial_id",
        "rolling_origin_plan",
        "prior_trial_sharpes",
        "prior_trial_p_values",
        "prior_trial_daily_returns_by_id",
    ):
        contract.pop(field, None)
    contract.update(
        {
            "experiment_id": EXPERIMENT_ID,
            "parent_experiment_id": v1.EXPERIMENT_ID,
            "family_id": FAMILY_ID,
            "strategy_id": FAMILY_ID,
            "created_at": created_at,
            "status": "INVENTED",
            "material_difference_rationale": (
                "This is a chronologically disjoint replication of the exact "
                "rejected v1 32-trial grid. It does not tune or repair any "
                "parameter; it tests whether the prior positive stressed paths "
                "replicate in the never-opened 2024 reserve while retaining "
                "all prior trials in cumulative selection correction."
            ),
            "selection_rule": (
                "Evaluate the unchanged 32-trial grid and apply DSR and Holm "
                "across all 64 current-plus-prior attempts, cumulative PBO "
                "from matching parameter paths concatenated across the two "
                "disjoint OOF corpora, plus every frozen account, stress, "
                "rolling-fold, and neighbor gate."
            ),
            "development_dates": selected["development_dates"],
            "development_signal_dates": selected[
                "development_signal_dates"
            ],
            "development_scope": selected["development_scope"],
            "embargo_dates": selected["embargo_dates"],
            "confirmation_dates": selected["confirmation_dates"],
            "confirmation_signal_dates": selected[
                "confirmation_signal_dates"
            ],
            "confirmation_signal_capacity": len(
                selected["confirmation_signal_dates"]
            ),
            "confirmation_scope": selected["confirmation_scope"],
            "source_history_start": SOURCE_HISTORY_START,
            "source_history_end": DEVELOPMENT_END,
            "outcome_exposure_index_sha256": selected[
                "outcome_exposure_index_sha256"
            ],
            "universe": {
                "development_symbol_count": len(
                    {
                        row["symbol"]
                        for row in selected["development_events"]
                    }
                ),
                "confirmation_symbol_count": len(
                    {
                        row["symbol"]
                        for row in selected["confirmation_events"]
                    }
                ),
                "development_event_count": len(
                    selected["development_events"]
                ),
                "confirmation_event_count": len(
                    selected["confirmation_events"]
                ),
                "identity": "as-filed SEC issuer CIK and symbol",
            },
            "capacity_manifest": _relative(capacity_manifest),
            "capacity_policy": {
                "retire_below": 50,
                "fast_lane_at": 100,
                "replication_development_events": 424,
                "replication_confirmation_signal_dates": 46,
            },
            "selection_accounting": {
                "current_trial_count": 32,
                "prior_evaluated_trial_count": 32,
                "cumulative_trial_count": 64,
                "deflated_sharpe_trial_count": 64,
                "holm_p_value_count": 64,
                "cumulative_pbo_method": prior[
                    "cumulative_pbo_method"
                ],
                "prior_standalone_pbo_is_not_irreversible_veto": True,
            },
            "prior_selection_lineage": {
                key: value
                for key, value in prior.items()
                if key
                not in {
                    "prior_trial_sharpes",
                    "prior_trial_p_values",
                    "prior_trial_daily_returns_by_id",
                }
            },
            "prior_trial_sharpes": prior["prior_trial_sharpes"],
            "prior_trial_p_values": prior["prior_trial_p_values"],
            "prior_trial_daily_returns_by_id": prior[
                "prior_trial_daily_returns_by_id"
            ],
            "prior_pbo_probability": 0.0,
            "research_generation": "existing_family_successor",
            "new_mechanism_family_slot_consumed": False,
            "implementation_files": [
                "insider_purchase_replication_search.py",
                "insider_purchase_data.py",
                "insider_purchase_plugin.py",
                "dense_strategy_runtime.py",
                "learning_statistics.py",
                "learning_experiment.py",
                "strategy_discovery.py",
                "outcome_exposure.py",
                "portfolio_maturity.py",
                "portfolio_config.toml",
            ],
            "plugin": {
                "module": "insider_purchase_plugin",
                "preflight": "preflight",
                "evaluate_development": "evaluate_development",
                "evaluate_confirmation": "evaluate_confirmation",
                "evaluate_production": "evaluate_production",
            },
            "contamination_risks": [
                (
                    "The prior 32 outcomes are adverse history and enter every "
                    "cumulative selection correction."
                ),
                (
                    "Only globally untouched reserve pairs enter development "
                    "or confirmation; contaminated events are dropped without "
                    "replacement before freeze."
                ),
                (
                    "Confirmation prices remain inaccessible before an exact "
                    "winner is frozen."
                ),
            ],
            "falsifiers": [
                "nonpositive stressed log growth",
                "unstable parameter neighbors",
                "selection-aware statistical rejection across all 64 attempts",
                "incomplete account or candidate accounting",
                "insufficient frozen confirmation power capacity",
                "any alteration or omission of a prior selection path",
                "any development-confirmation pair overlap",
            ],
            "partitions": {
                "rolling_origin": True,
                "confirmation_untouched": True,
                "embargo_sessions": 5,
                "development_contamination": "none_at_freeze",
                "sparse_2023_reserve": "discarded_outcome_blind",
            },
        }
    )
    validated = strategy_discovery._validate_family_contract(contract)
    if (
        validated["parameter_grid"]
        != parent["family_contract"]["parameter_grid"]
        or {
            row["trial_id"] for row in validated["trial_family"]
        }
        != {
            row["trial_id"]
            for row in parent["family_contract"]["trial_family"]
        }
    ):
        raise InsiderPurchaseReplicationError(
            "replication grid or matching trial IDs drifted"
        )
    return validated


def freeze_family(
    *, created_at: str
) -> tuple[Path, Path, dict[str, Any]]:
    for path in (
        PRIOR_SEARCH,
        PRIOR_RESULT,
        PRIOR_INSPECTION,
        PRIOR_CAPACITY,
        outcome_exposure.DEFAULT_INDEX,
    ):
        strategy_discovery.require_committed(path)
    for relative in (
        "insider_purchase_replication_search.py",
        "insider_purchase_data.py",
        "insider_purchase_plugin.py",
        "dense_strategy_runtime.py",
        "learning_experiment.py",
        "strategy_discovery.py",
    ):
        strategy_discovery.require_committed(PROJECT_ROOT / relative)
    selected = selection()
    inventory = _inventory(selected)
    private_binding = _write_private_inventory(inventory)
    counts = inventory["counts"]
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{EXPERIMENT_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    _relative(PRIOR_CAPACITY),
                    _relative(PRIOR_RESULT),
                    _relative(PRIOR_INSPECTION),
                    _relative(outcome_exposure.DEFAULT_INDEX),
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "form4_purchase_capacity": {
                    "family_id": FAMILY_ID,
                    "experiment_id": EXPERIMENT_ID,
                    "parent_family_id": v1.FAMILY_ID,
                    "private_inventory": private_binding,
                    "counts": counts,
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "confirmation_access_permitted": False,
                    "unchanged_grid": True,
                    "cumulative_trial_count": 64,
                    "new_mechanism_family_slot_consumed": False,
                },
            },
        },
        CAPACITY_ROOT,
    )
    contract = build_contract(
        created_at=created_at,
        capacity_manifest=capacity_path,
        selected=selected,
    )
    digest = _hash(contract)
    directory = DEFAULT_ROOT / "family-contract"
    directory.mkdir(parents=True, exist_ok=True)
    contract_path = directory / f"contract-{digest}.json"
    rendered = json.dumps(contract, indent=2, sort_keys=True) + "\n"
    if (
        contract_path.exists()
        and contract_path.read_text(encoding="utf-8") != rendered
    ):
        raise InsiderPurchaseReplicationError(
            "content-addressed family contract collision"
        )
    if not contract_path.exists():
        temporary = contract_path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(contract_path)
    return capacity_path, contract_path, counts


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "freeze-family"))
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    if args.command == "status":
        selected = selection()
        value = {
            "state": "READY_TO_FREEZE",
            "family_id": FAMILY_ID,
            "calendar_wait_required": False,
            "development_events": len(selected["development_events"]),
            "development_signal_dates": len(
                selected["development_signal_dates"]
            ),
            "confirmation_events": len(selected["confirmation_events"]),
            "confirmation_signal_dates": len(
                selected["confirmation_signal_dates"]
            ),
            "cumulative_trial_count": 64,
            "provider_requests_permitted": 0,
            "broker_actions": 0,
        }
    else:
        if not args.created_at:
            raise InsiderPurchaseReplicationError(
                "--created-at is required"
            )
        capacity, contract, counts = freeze_family(
            created_at=args.created_at
        )
        value = {
            "state": "FAMILY_FROZEN",
            "capacity_manifest": _relative(capacity),
            "family_contract": _relative(contract),
            "counts": counts,
            "cumulative_trial_count": 64,
            "market_prices_accessed": False,
            "confirmation_accessed": False,
            "broker_actions": 0,
        }
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
