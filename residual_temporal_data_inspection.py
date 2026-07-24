"""Independently inspect the residual-reversal v7 data contract and dataset."""

from __future__ import annotations

import argparse
import gzip
import json
import statistics
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import outcome_exposure
import residual_temporal_data as data
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent


class ResidualTemporalDataInspectionError(RuntimeError):
    """An independently rebuilt v7 data assertion failed."""


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualTemporalDataInspectionError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise ResidualTemporalDataInspectionError(
            f"{field} needs a timezone"
        )
    return parsed.astimezone(timezone.utc)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualTemporalDataInspectionError(
            f"path escaped repository: {path}"
        ) from exc


def _artifact_hash(value: Mapping[str, Any]) -> str:
    return canonical_sha256(
        {
            key: item
            for key, item in value.items()
            if key != "artifact_sha256"
        }
    )


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = data.ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    observed_at = _timestamp(inspected_at, "inspected_at")
    contract = data.load_contract(
        contract_path, enforce_commit=enforce_commit
    )
    frozen_at = _timestamp(str(contract["created_at"]), "created_at")
    if observed_at <= frozen_at:
        raise ResidualTemporalDataInspectionError(
            "data contract inspection must follow freeze"
        )
    reference_path = PROJECT_ROOT / str(
        contract["reference_inspection_path"]
    )
    reference_inspection, early = data._load_reference_inspection(
        reference_path, enforce_commit=enforce_commit
    )
    v6_contract, later = data._v6_development_identities()
    partitions = data._partitions(sorted(early), v6_contract)
    (
        development_dates,
        development_signals,
        embargo,
        confirmation_dates,
        confirmation_signals,
    ) = partitions
    combined = {**early, **later}
    symbols = sorted(
        {
            "SPY",
            *{
                symbol
                for values in combined.values()
                for symbol in values
            },
        }
    )
    records = outcome_exposure.read_index()
    contaminated = data._contaminated_record(records)
    fresh_overlaps = outcome_exposure.find_overlaps(
        contract["fresh_development_scope"], records
    )
    confirmation_overlaps = outcome_exposure.find_overlaps(
        contract["confirmation_scope"], records
    )
    checks = {
        "artifact_hash_valid": contract.get("artifact_sha256")
        == _artifact_hash(contract),
        "reference_binding_rebuilt": reference_inspection[
            "artifact_sha256"
        ]
        == contract["reference_inspection_sha256"],
        "v6_binding_rebuilt": v6_contract["artifact_sha256"]
        == contract["v6_contract_sha256"],
        "contaminated_exposure_rebuilt": contaminated
        == contract["contaminated_training_exposure"],
        "combined_identity_graph_rebuilt": canonical_sha256(combined)
        == contract["identity_graph_sha256"],
        "development_partition_rebuilt": contract["development_dates"]
        == development_dates
        and contract["development_signal_dates"] == development_signals,
        "embargo_rebuilt": contract["embargo_dates"] == embargo
        and len(embargo) == 5,
        "confirmation_partition_rebuilt": contract["confirmation_dates"]
        == confirmation_dates
        and contract["confirmation_signal_dates"]
        == confirmation_signals,
        "symbol_union_rebuilt": contract["development_symbol_count"]
        == len(symbols)
        and contract["development_symbols_sha256"]
        == canonical_sha256(symbols),
        "fresh_scope_untouched": not fresh_overlaps,
        "confirmation_scope_untouched": not confirmation_overlaps,
        "rules_and_grid_unchanged": contract[
            "rules_or_parameter_grid_changed"
        ]
        is False,
        "confirmation_prices_locked": contract[
            "confirmation_prices_accessed"
        ]
        is False,
        "strategy_metrics_absent": contract[
            "strategy_metrics_accessed_before_freeze"
        ]
        is False,
        "substitutions_zero": contract["substitutions"] == 0,
        "broker_actions_zero": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        failed = sorted(key for key, passed in checks.items() if not passed)
        raise ResidualTemporalDataInspectionError(
            "data contract inspection failed: " + ", ".join(failed)
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": data.CONTRACT_INSPECTION_KIND,
        "state": data.CONTRACT_INSPECTION_STATE,
        "campaign_id": data.CAMPAIGN_ID,
        "family_id": data.FAMILY_ID,
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": checks,
        "development_signal_capacity": len(development_signals),
        "fresh_development_signal_capacity": len(early),
        "contaminated_training_signal_count": len(later),
        "confirmation_signal_capacity": len(confirmation_signals),
        "market_prices_accessed": False,
        "strategy_metrics_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "data-contract-inspection",
        "residual-temporal-data-contract-inspection",
    )


def _load_dataset(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualTemporalDataInspectionError(
            f"cannot read development dataset: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ResidualTemporalDataInspectionError(
            "development dataset must contain an object"
        )
    return value


def _liquid_capacity(
    dataset: Mapping[str, Any],
) -> tuple[int, int, int]:
    bars = dataset.get("daily_bars")
    identities = dataset.get("reference_identities_by_date")
    decision_dates = dataset.get("decision_dates")
    if not (
        isinstance(bars, Mapping)
        and isinstance(identities, Mapping)
        and isinstance(decision_dates, list)
    ):
        raise ResidualTemporalDataInspectionError(
            "development dataset topology is incomplete"
        )
    by_symbol = {
        str(symbol): {
            str(row["date"]): row
            for row in rows
            if isinstance(row, Mapping)
            and isinstance(row.get("date"), str)
        }
        for symbol, rows in bars.items()
        if isinstance(rows, list)
    }
    spy_rows = bars.get("SPY")
    if not isinstance(spy_rows, list):
        raise ResidualTemporalDataInspectionError(
            "development SPY calendar is missing"
        )
    calendar = [
        str(row["date"])
        for row in spy_rows
        if isinstance(row, Mapping)
        and isinstance(row.get("date"), str)
    ]
    if calendar != sorted(set(calendar)):
        raise ResidualTemporalDataInspectionError(
            "development SPY calendar is malformed"
        )
    positions = {day: index for index, day in enumerate(calendar)}
    complete_dates = 0
    minimum_qualified = 10**9
    for day in decision_dates:
        index = positions.get(str(day), -1)
        if index < 59:
            minimum_qualified = min(minimum_qualified, 0)
            continue
        prior = calendar[index - 59 : index + 1]
        day_identities = identities.get(day)
        if not isinstance(day_identities, Mapping):
            raise ResidualTemporalDataInspectionError(
                f"{day}: identity denominator is missing"
            )
        qualified = 0
        for symbol in day_identities:
            series = by_symbol.get(str(symbol), {})
            history = [series.get(prior_day) for prior_day in prior]
            if any(row is None for row in history):
                continue
            complete = [row for row in history if row is not None]
            dollar = [
                float(row["close"]) * float(row["volume"])
                for row in complete
            ]
            if (
                float(complete[-1]["close"]) >= 10.0
                and statistics.median(dollar[-20:]) >= 50_000_000.0
            ):
                qualified += 1
        minimum_qualified = min(minimum_qualified, qualified)
        if qualified >= 250:
            complete_dates += 1
    return complete_dates, minimum_qualified, len(decision_dates)


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = data.ROOT,
    store_config: HistoricalStoreConfig | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    observed_at = _timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(collection_path)
    collection = strategy_discovery.load_artifact(
        collection_path, expected_kind=data.COLLECTION_KIND
    )
    completed_at = _timestamp(
        str(collection["collection_completed_at"]),
        "collection_completed_at",
    )
    if observed_at <= completed_at:
        raise ResidualTemporalDataInspectionError(
            "dataset inspection must follow collection"
        )
    contract_path = PROJECT_ROOT / str(collection["contract_path"])
    contract = data.load_contract(
        contract_path, enforce_commit=enforce_commit
    )
    store = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    dataset_path = (
        store.root / str(collection["external_relative_path"])
    ).resolve()
    if (
        store.root.resolve() not in dataset_path.parents
        or not dataset_path.is_file()
        or sha256_file(dataset_path)
        != collection["external_file_sha256"]
    ):
        raise ResidualTemporalDataInspectionError(
            "development dataset binding drifted"
        )
    dataset = _load_dataset(dataset_path)
    complete_dates, minimum_qualified, decision_dates = _liquid_capacity(
        dataset
    )
    bars = dataset.get("daily_bars", {})
    rows = sum(
        len(value)
        for value in bars.values()
        if isinstance(value, list)
    )
    symbols_with_rows = sum(
        bool(value)
        for value in bars.values()
        if isinstance(value, list)
    )
    empty_series = sum(
        not value
        for value in bars.values()
        if isinstance(value, list)
    )
    checks = {
        "artifact_hash_valid": collection.get("artifact_sha256")
        == _artifact_hash(collection),
        "contract_binding_valid": collection["contract_sha256"]
        == contract["artifact_sha256"],
        "dataset_file_hash_valid": collection["external_file_sha256"]
        == sha256_file(dataset_path),
        "dataset_content_hash_valid": collection["dataset_sha256"]
        == canonical_sha256(dataset),
        "family_bound": dataset.get("family_id") == data.FAMILY_ID,
        "account_dates_complete": dataset.get("evaluation_dates")
        == contract["development_dates"],
        "decision_dates_complete": dataset.get("decision_dates")
        == contract["development_signal_dates"],
        "identity_denominator_complete": set(
            dataset.get("reference_identities_by_date", {})
        )
        == set(contract["development_signal_dates"]),
        "symbol_count_rebuilt": collection["symbols_requested"]
        == len(bars),
        "symbols_with_rows_rebuilt": collection["symbols_with_rows"]
        == symbols_with_rows,
        "empty_series_rebuilt": collection["empty_series"] == empty_series,
        "daily_rows_rebuilt": collection["daily_rows"] == rows,
        "liquid_universe_capacity": complete_dates == decision_dates
        and minimum_qualified >= 250,
        "strategy_metrics_absent": collection[
            "strategy_metrics_computed"
        ]
        is False,
        "confirmation_prices_locked": collection[
            "confirmation_prices_accessed"
        ]
        is False,
        "substitutions_zero": collection["substitutions"] == 0,
        "broker_actions_zero": collection["broker_actions"] == 0,
    }
    if not all(checks.values()):
        failed = sorted(key for key, passed in checks.items() if not passed)
        raise ResidualTemporalDataInspectionError(
            "development data inspection failed: " + ", ".join(failed)
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": data.COLLECTION_INSPECTION_KIND,
        "state": data.COLLECTION_INSPECTION_STATE,
        "campaign_id": data.CAMPAIGN_ID,
        "family_id": data.FAMILY_ID,
        "collection_path": _repo_path(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": checks,
        "decision_dates": decision_dates,
        "decision_dates_with_liquid_capacity": complete_dates,
        "minimum_qualified_names": minimum_qualified,
        "empty_series": empty_series,
        "strategy_metrics_accessed": False,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    inspection_path, inspection = strategy_discovery._write_artifact(
        payload,
        root / "development-data-inspection",
        "residual-temporal-development-data-inspection",
    )
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                "dataset-residual-reversal-v7-temporal-expansion-development"
            ),
            "registered_at": inspected_at,
            "requested_dates": list(contract["development_dates"]),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "confirmation_access_permitted": False,
                "collection_inspection_path": _repo_path(inspection_path),
                "collection_inspection_sha256": inspection[
                    "artifact_sha256"
                ],
                "dense_runtime": {
                    "family_id": data.FAMILY_ID,
                    "external_relative_path": collection[
                        "external_relative_path"
                    ],
                    "external_file_sha256": collection[
                        "external_file_sha256"
                    ],
                    "dataset_sha256": collection["dataset_sha256"],
                    "format": "json.gz",
                    "formal_capacity": len(
                        contract["development_signal_dates"]
                    ),
                    "expected_empty_series": empty_series,
                },
            },
        },
        root / "development-dataset",
    )
    return inspection_path, inspection, manifest_path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=data.ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("inspect-contract")
    contract.add_argument("artifact", type=Path)
    contract.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-collection")
    collection.add_argument("artifact", type=Path)
    collection.add_argument("--inspected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "inspect-contract":
            path, artifact = inspect_contract(
                args.artifact,
                inspected_at=args.inspected_at,
                root=args.root,
            )
            result: dict[str, Any] = {
                "written": _repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
            }
        else:
            path, artifact, manifest_path, manifest = inspect_collection(
                args.artifact,
                inspected_at=args.inspected_at,
                root=args.root,
            )
            result = {
                "written": _repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "dataset_manifest": _repo_path(manifest_path),
                "dataset_manifest_sha256": manifest["manifest_sha256"],
            }
        print(
            json.dumps(
                {**result, "broker_actions": 0},
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualTemporalDataInspectionError,
        data.ResidualTemporalDataError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
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
