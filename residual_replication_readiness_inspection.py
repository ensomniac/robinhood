"""Inspect residual-replication readiness with explicit warm-up zero days.

The frozen collector correctly starts at the first available exchange session.
The original collection inspector incorrectly required its first 59 sessions
to have a 60-session liquidity history.  This independent successor preserves
those dates as explicit zero-return warm-up days and measures capacity only
where the frozen top-250 rule is structurally observable.  It never computes a
strategy return or opens confirmation prices.
"""

from __future__ import annotations

import argparse
import json
import statistics
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import residual_replication_data as data
import residual_replication_inspection as original
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent


class ResidualReplicationReadinessError(RuntimeError):
    """The collected development denominator is not evaluation-ready."""


def _liquid_capacity(
    dataset: Mapping[str, Any],
) -> tuple[int, int, int, int]:
    bars = dataset.get("daily_bars")
    identities = dataset.get("reference_identities_by_date")
    decision_dates = dataset.get("decision_dates")
    if not (
        isinstance(bars, Mapping)
        and isinstance(identities, Mapping)
        and isinstance(decision_dates, list)
    ):
        raise ResidualReplicationReadinessError(
            "development dataset topology is incomplete"
        )
    by_symbol = {
        str(symbol): {
            str(row["date"]): row
            for row in rows
            if isinstance(row, Mapping) and isinstance(row.get("date"), str)
        }
        for symbol, rows in bars.items()
        if isinstance(rows, list)
    }
    spy_rows = bars.get("SPY")
    if not isinstance(spy_rows, list):
        raise ResidualReplicationReadinessError(
            "development SPY session calendar is missing"
        )
    calendar = [
        str(row["date"])
        for row in spy_rows
        if isinstance(row, Mapping) and isinstance(row.get("date"), str)
    ]
    if len(calendar) != len(set(calendar)) or calendar != sorted(calendar):
        raise ResidualReplicationReadinessError(
            "development SPY session calendar is malformed"
        )
    positions = {day: index for index, day in enumerate(calendar)}
    complete_dates = 0
    warmup_zero_days = 0
    qualified_counts: list[int] = []
    for raw_day in decision_dates:
        day = str(raw_day)
        index = positions.get(day, -1)
        if index < 59:
            warmup_zero_days += 1
            continue
        prior = calendar[index - 59 : index + 1]
        day_identities = identities.get(day)
        if not isinstance(day_identities, Mapping):
            raise ResidualReplicationReadinessError(
                f"{day}: point-in-time identity denominator is missing"
            )
        qualified = 0
        for raw_symbol in day_identities:
            series = by_symbol.get(str(raw_symbol), {})
            history = [series.get(prior_day) for prior_day in prior]
            if any(row is None for row in history):
                continue
            complete = [row for row in history if row is not None]
            dollar = [
                float(row["close"]) * float(row["volume"]) for row in complete
            ]
            if (
                float(complete[-1]["close"]) >= 10
                and statistics.median(dollar[-20:]) >= 50_000_000
            ):
                qualified += 1
        qualified_counts.append(qualified)
        if qualified >= 250:
            complete_dates += 1
    if not qualified_counts:
        raise ResidualReplicationReadinessError(
            "no decision date has a complete 60-session liquidity lookback"
        )
    return (
        complete_dates,
        min(qualified_counts),
        len(decision_dates),
        warmup_zero_days,
    )


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = data.ROOT,
    store_config: HistoricalStoreConfig | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    observed_at = original._timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(collection_path)
    collection = strategy_discovery.load_artifact(
        collection_path, expected_kind=data.COLLECTION_KIND
    )
    contract_path = PROJECT_ROOT / str(collection.get("contract_path", ""))
    contract = data._load_contract(
        contract_path, enforce_commit=enforce_commit
    )
    completed_at = original._timestamp(
        str(collection["collection_completed_at"]), "collection_completed_at"
    )
    if observed_at <= completed_at:
        raise ResidualReplicationReadinessError(
            "collection inspection must follow completion"
        )
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    dataset_path = (
        config.root / str(collection["external_relative_path"])
    ).resolve()
    if config.root.resolve() not in dataset_path.parents:
        raise ResidualReplicationReadinessError(
            "development dataset escaped the historical store"
        )
    if (
        not dataset_path.is_file()
        or sha256_file(dataset_path) != collection["external_file_sha256"]
    ):
        raise ResidualReplicationReadinessError(
            "development dataset bytes drifted"
        )
    dataset = original._load_dataset(dataset_path)
    complete_dates, minimum_qualified, decision_dates, warmup_zero_days = (
        _liquid_capacity(dataset)
    )
    rows = sum(
        len(value)
        for value in dataset.get("daily_bars", {}).values()
        if isinstance(value, list)
    )
    symbols_with_rows = sum(
        bool(value)
        for value in dataset.get("daily_bars", {}).values()
        if isinstance(value, list)
    )
    checks = {
        "artifact_hash_valid": collection.get("artifact_sha256")
        == original._artifact_hash(collection),
        "contract_binding_valid": collection.get("contract_sha256")
        == contract["artifact_sha256"],
        "dataset_file_hash_valid": collection.get("external_file_sha256")
        == sha256_file(dataset_path),
        "dataset_content_hash_valid": collection.get("dataset_sha256")
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
        "symbol_count_rebuilt": collection.get("symbols_requested")
        == len(dataset.get("daily_bars", {})),
        "symbols_with_rows_rebuilt": collection.get("symbols_with_rows")
        == symbols_with_rows,
        "daily_rows_rebuilt": collection.get("daily_rows") == rows,
        "warmup_zero_days_explicit": 0 < warmup_zero_days < decision_dates,
        "liquid_universe_capacity": complete_dates >= 100
        and minimum_qualified >= 250,
        "strategy_metrics_absent": collection.get(
            "strategy_metrics_computed"
        )
        is False,
        "confirmation_prices_locked": collection.get(
            "confirmation_prices_accessed"
        )
        is False,
        "substitutions_zero": collection.get("substitutions") == 0,
        "broker_actions_zero": collection.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        failed = sorted(key for key, value in checks.items() if not value)
        raise ResidualReplicationReadinessError(
            "development data inspection failed: " + ", ".join(failed)
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": data.COLLECTION_INSPECTION_KIND,
        "inspection_implementation": (
            "residual_replication_readiness_inspection.py"
        ),
        "inspection_implementation_sha256": sha256_file(Path(__file__)),
        "supersedes_failed_inspector": {
            "implementation": "residual_replication_inspection.py",
            "failure": "warmup_dates_counted_as_capacity_failures",
            "strategy_metrics_accessed": False,
        },
        "state": data.COLLECTION_INSPECTION_STATE,
        "campaign_id": data.CAMPAIGN_ID,
        "family_id": data.FAMILY_ID,
        "collection_path": original._repo_path(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "contract_path": original._repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": checks,
        "decision_dates": decision_dates,
        "decision_dates_with_liquid_capacity": complete_dates,
        "warmup_zero_return_days": warmup_zero_days,
        "minimum_qualified_names_after_warmup": minimum_qualified,
        "strategy_metrics_accessed": False,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    inspection_path, inspection = strategy_discovery._write_artifact(
        payload,
        root / "development-data-inspection",
        "residual-replication-development-data-inspection",
    )
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                "dataset-residual-reversal-v6-long-history-development"
            ),
            "registered_at": inspected_at,
            "requested_dates": list(contract["development_dates"]),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    original._repo_path(contract_path),
                    original._repo_path(collection_path),
                    original._repo_path(inspection_path),
                    "STRATEGY_DISCOVERY_V2.md",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "confirmation_access_permitted": False,
                "collection_inspection_path": original._repo_path(
                    inspection_path
                ),
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
                    "warmup_zero_return_days": warmup_zero_days,
                },
            },
        },
        root / "development-dataset",
    )
    return inspection_path, inspection, manifest_path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--inspected-at", required=True)
    parser.add_argument("--root", type=Path, default=data.ROOT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, artifact, manifest_path, manifest = inspect_collection(
            args.artifact,
            inspected_at=args.inspected_at,
            root=args.root,
        )
        print(
            json.dumps(
                {
                    "written": original._repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "warmup_zero_return_days": artifact[
                        "warmup_zero_return_days"
                    ],
                    "decision_dates_with_liquid_capacity": artifact[
                        "decision_dates_with_liquid_capacity"
                    ],
                    "minimum_qualified_names_after_warmup": artifact[
                        "minimum_qualified_names_after_warmup"
                    ],
                    "dataset_manifest": original._repo_path(manifest_path),
                    "dataset_manifest_sha256": manifest["manifest_sha256"],
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualReplicationReadinessError,
        data.ResidualReplicationDataError,
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
