"""Independently inspect the Yahoo fallback for fixed SPY RSI(2)."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import outcome_exposure
import spy_rsi2_data as shared
import spy_rsi2_yahoo_data as source
import strategy_discovery
from historical_store import (
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract


class SpyRsi2YahooInspectionError(RuntimeError):
    """A rebuilt Yahoo source artifact failed closed."""


def inspect_source_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = shared._read(contract_path)
    failure_inspection_path = (
        source.PROJECT_ROOT
        / str(contract["prior_source_failure"]["inspection_path"])
    )
    rebuilt = source.build_source_contract(
        created_at=contract["created_at"],
        massive_failure_inspection_path=failure_inspection_path,
    )
    split = shared.partitions()
    checks = {
        "contract_hash_valid": contract.get("contract_sha256")
        == shared.self_hash(contract, "contract_sha256"),
        "contract_exactly_rebuilt": contract == rebuilt,
        "calendar_exactly_rebuilt": all(
            contract.get(field) == split[field]
            for field in (
                "warmup_dates",
                "development_dates",
                "embargo_dates",
                "confirmation_dates",
            )
        ),
        "prior_source_failure_closed": contract.get(
            "prior_source_failure", {}
        ).get("retry_permitted")
        is False,
        "one_no_purchase_request": contract.get("request_policy", {}).get(
            "authorized_provider_requests"
        )
        == 1
        and contract.get("request_policy", {}).get("no_purchase_required") is True,
        "zero_retry_or_substitution": contract.get("request_policy", {}).get(
            "retries_permitted"
        )
        == 0
        and contract.get("request_policy", {}).get("substitutions_permitted") == 0,
        "split_adjusted_raw_ohlc": contract.get("development_request", {}).get(
            "response_semantics", {}
        ).get("raw_close_adjustment")
        == "splits_only"
        and contract["development_request"]["response_semantics"].get(
            "dividend_adjusted_close_used"
        )
        is False,
        "one_fixed_trial": contract.get("frozen_rule", {}).get("trial_count") == 1
        and contract.get("frozen_rule", {}).get("parameters")
        == shared.PARAMETERS,
        "confirmation_access_locked": contract.get("request_policy", {}).get(
            "confirmation_request_permitted"
        )
        is False,
        "zero_outcomes": contract.get("market_prices_accessed") is False
        and contract.get("strategy_metrics_computed") == 0
        and contract.get("confirmation_prices_accessed") is False,
        "broker_actions_zero": contract.get("broker_actions") == 0,
    }
    index = outcome_exposure.read_index()
    try:
        outcome_exposure.assert_untouched(contract["development_scope"], index)
        checks["development_scope_untouched"] = True
    except outcome_exposure.OutcomeExposureError:
        checks["development_scope_untouched"] = False
    try:
        outcome_exposure.assert_untouched(contract["confirmation_scope"], index)
        checks["confirmation_scope_untouched"] = True
    except outcome_exposure.OutcomeExposureError:
        checks["confirmation_scope_untouched"] = False
    if not all(checks.values()):
        raise SpyRsi2YahooInspectionError(
            "Yahoo source contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-yahoo-source-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "source_id": source.SOURCE_ID,
        "state": "SOURCE_CONTRACT_INSPECTED_READY",
        "inspected_at": shared._timestamp(inspected_at, "inspected_at"),
        "contract_path": shared._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "development_provider_access_authorized": True,
        "confirmation_provider_access_authorized": False,
        "authorized_provider_requests": 1,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = shared.self_hash(value, "inspection_sha256")
    path = (
        root
        / "yahoo-source-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    shared._write(path, value)
    return path, value


def inspect_development_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
    store_config: HistoricalStoreConfig | None = None,
) -> tuple[Path, dict[str, Any], Path]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = shared._read(collection_path)
    contract_path = source.PROJECT_ROOT / str(collection["contract_path"])
    source_inspection_path = source.PROJECT_ROOT / str(collection["inspection_path"])
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(source_inspection_path)
    contract = shared._read(contract_path)
    source_inspection = shared._read(source_inspection_path)
    store = store_config or HistoricalStoreConfig.from_env()
    relative = Path(str(collection["external_relative_path"]))
    private_path = (store.root / relative).resolve()
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or store.root.resolve() not in private_path.parents
        or not private_path.is_file()
        or sha256_file(private_path) != collection["external_file_sha256"]
    ):
        raise SpyRsi2YahooInspectionError(
            "private Yahoo SPY dataset is missing, unsafe, or drifted"
        )
    try:
        with gzip.open(private_path, "rt", encoding="utf-8") as handle:
            dataset = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise SpyRsi2YahooInspectionError(
            "private Yahoo SPY dataset is unreadable"
        ) from exc
    expected_dates = [
        *contract["warmup_dates"],
        *contract["development_dates"],
    ]
    rows = dataset.get("daily_bars", {}).get("SPY", [])
    try:
        runtime.prepare_dataset(dataset)
        runtime_valid = True
    except runtime.DenseStrategyRuntimeError:
        runtime_valid = False
    source_exposure_id = (
        f"source-{source.FAMILY_ID}-yahoo-"
        f"{collection['collection_sha256'][:16]}"
    )
    checks = {
        "collection_hash_valid": collection.get("collection_sha256")
        == shared.self_hash(collection, "collection_sha256"),
        "contract_binding_valid": collection.get("contract_sha256")
        == contract.get("contract_sha256"),
        "source_id_bound": collection.get("source_id") == source.SOURCE_ID,
        "source_inspection_valid": source_inspection.get("state")
        == "SOURCE_CONTRACT_INSPECTED_READY"
        and source_inspection.get("valid") is True,
        "private_hash_valid": sha256_file(private_path)
        == collection.get("external_file_sha256")
        and canonical_sha256(dataset) == collection.get("dataset_sha256"),
        "dataset_scope_exact": dataset.get("family_id") == source.FAMILY_ID
        and dataset.get("symbols") == ["SPY"]
        and dataset.get("evaluation_dates") == contract["development_dates"]
        and isinstance(rows, list)
        and [row.get("date") for row in rows] == expected_dates,
        "runtime_validation_passed": runtime_valid,
        "one_request_only": collection.get("provider_requests") == 1
        and collection.get("provider_telemetry", {}).get("requests") == 1,
        "zero_retry_or_substitution": collection.get("retries") == 0
        and collection.get("substitutions") == 0,
        "development_exposure_indexed": any(
            record["exposure_id"] == source_exposure_id
            for record in outcome_exposure.read_index()
        ),
        "confirmation_still_untouched": not outcome_exposure.find_overlaps(
            contract["confirmation_scope"],
            outcome_exposure.read_index(),
        ),
        "zero_metrics_or_broker_actions": collection.get(
            "strategy_metrics_computed"
        )
        == 0
        and collection.get("confirmation_prices_accessed") is False
        and collection.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise SpyRsi2YahooInspectionError(
            "Yahoo development collection inspection failed"
        )
    observed = shared._timestamp(inspected_at, "inspected_at")
    evidence_paths = [
        shared._repo_path(contract_path),
        shared._repo_path(source_inspection_path),
        shared._repo_path(collection_path),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{source.SUCCESSOR_ID}-yahoo-development",
            "registered_at": observed,
            "requested_dates": list(contract["development_dates"]),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "dense_runtime": {
                    "family_id": source.FAMILY_ID,
                    "external_relative_path": collection["external_relative_path"],
                    "external_file_sha256": collection["external_file_sha256"],
                    "dataset_sha256": collection["dataset_sha256"],
                    "format": "json.gz",
                    "formal_capacity": len(contract["development_dates"]),
                },
            },
        },
        root / "yahoo-development-dataset",
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-yahoo-development-collection-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "source_id": source.SOURCE_ID,
        "state": "DEVELOPMENT_DATASET_INSPECTED_READY",
        "inspected_at": observed,
        "collection_path": shared._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "dataset_manifest_path": shared._repo_path(manifest_path),
        "dataset_manifest_file_sha256": sha256_file(manifest_path),
        "dataset_manifest_sha256": manifest["manifest_sha256"],
        "checks": checks,
        "development_session_count": len(contract["development_dates"]),
        "warmup_session_count": len(contract["warmup_dates"]),
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = shared.self_hash(value, "inspection_sha256")
    path = (
        root
        / "yahoo-development-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    shared._write(path, value)
    return path, value, manifest_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    source_parser = subparsers.add_parser("inspect-source")
    source_parser.add_argument("contract", type=Path)
    source_parser.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-development")
    collection.add_argument("collection", type=Path)
    collection.add_argument("--inspected-at", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-source":
            path, value = inspect_source_contract(
                args.contract,
                inspected_at=args.inspected_at,
            )
            result = {
                "path": str(path),
                "state": value["state"],
                "sha256": value["inspection_sha256"],
            }
        else:
            path, value, manifest_path = inspect_development_collection(
                args.collection,
                inspected_at=args.inspected_at,
            )
            result = {
                "path": str(path),
                "manifest_path": str(manifest_path),
                "state": value["state"],
                "sha256": value["inspection_sha256"],
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        SpyRsi2YahooInspectionError,
        source.SpyRsi2YahooError,
        shared.SpyRsi2DataError,
        OSError,
        outcome_exposure.OutcomeExposureError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
