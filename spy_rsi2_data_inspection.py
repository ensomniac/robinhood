"""Independently rebuild SPY RSI(2) source and development collection artifacts."""

from __future__ import annotations

import argparse
import gzip
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import outcome_exposure
import spy_rsi2_data as source
import strategy_discovery
from historical_store import (
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract


class SpyRsi2InspectionError(RuntimeError):
    """An independently rebuilt SPY source artifact failed closed."""


def inspect_source_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = source._read(contract_path)
    rebuilt = source.build_source_contract(created_at=contract["created_at"])
    split = source.partitions()
    checks = {
        "contract_hash_valid": contract.get("contract_sha256")
        == source.self_hash(contract, "contract_sha256"),
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
        "five_session_embargo": len(contract.get("embargo_dates", [])) == 5,
        "one_trial_rule": contract.get("frozen_rule", {}).get("trial_count") == 1
        and contract.get("frozen_rule", {}).get("parameters")
        == source.PARAMETERS,
        "one_development_request": contract.get("request_policy", {}).get(
            "authorized_provider_requests"
        )
        == 1,
        "no_retry_or_substitution": contract.get("request_policy", {}).get(
            "retries_permitted"
        )
        == 0
        and contract.get("request_policy", {}).get("substitutions_permitted") == 0,
        "confirmation_access_locked": contract.get("request_policy", {}).get(
            "confirmation_request_permitted"
        )
        is False,
        "development_outcomes_absent": contract.get("market_prices_accessed") is False
        and contract.get("strategy_metrics_computed") == 0,
        "confirmation_outcomes_absent": contract.get(
            "confirmation_prices_accessed"
        )
        is False,
        "broker_actions_zero": contract.get("broker_actions") == 0,
    }
    index = outcome_exposure.read_index()
    try:
        outcome_exposure.assert_untouched(contract["development_scope"], index)
        development_untouched = True
    except outcome_exposure.OutcomeExposureError:
        development_untouched = False
    try:
        outcome_exposure.assert_untouched(contract["confirmation_scope"], index)
        confirmation_untouched = True
    except outcome_exposure.OutcomeExposureError:
        confirmation_untouched = False
    checks["development_scope_untouched"] = development_untouched
    checks["confirmation_scope_untouched"] = confirmation_untouched
    if not all(checks.values()):
        raise SpyRsi2InspectionError("SPY RSI(2) source contract inspection failed")
    observed = source._timestamp(inspected_at, "inspected_at")
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-development-source-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "SOURCE_CONTRACT_INSPECTED_READY",
        "inspected_at": observed,
        "contract_path": source._repo_path(contract_path),
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
    value["inspection_sha256"] = source.self_hash(value, "inspection_sha256")
    path = (
        root
        / "source-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def _load_private(
    collection: Mapping[str, Any],
    store: HistoricalStoreConfig,
) -> tuple[dict[str, Any], Path]:
    relative = Path(str(collection["external_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise SpyRsi2InspectionError("private SPY dataset path is unsafe")
    path = (store.root / relative).resolve()
    if store.root.resolve() not in path.parents:
        raise SpyRsi2InspectionError("private SPY dataset escaped its store")
    if not path.is_file() or sha256_file(path) != collection["external_file_sha256"]:
        raise SpyRsi2InspectionError("private SPY dataset is missing or drifted")
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise SpyRsi2InspectionError("private SPY dataset is unreadable") from exc
    if not isinstance(value, dict):
        raise SpyRsi2InspectionError("private SPY dataset must be an object")
    return value, path


def inspect_development_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
    store_config: HistoricalStoreConfig | None = None,
) -> tuple[Path, dict[str, Any], Path]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = source._read(collection_path)
    contract_path = source.PROJECT_ROOT / str(collection["contract_path"])
    source_inspection_path = source.PROJECT_ROOT / str(collection["inspection_path"])
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(source_inspection_path)
    contract = source._read(contract_path)
    source_inspection = source._read(source_inspection_path)
    store = store_config or HistoricalStoreConfig.from_env()
    dataset, private_path = _load_private(collection, store)
    expected_dates = [
        *contract["warmup_dates"],
        *contract["development_dates"],
    ]
    rows = dataset.get("daily_bars", {}).get("SPY", [])
    try:
        prepared = runtime.prepare_dataset(dataset)
        runtime_valid = prepared.get("family_id") == source.FAMILY_ID
    except runtime.DenseStrategyRuntimeError:
        runtime_valid = False
    expected_source_record = (
        f"source-{source.FAMILY_ID}-{collection['collection_sha256'][:16]}"
    )
    source_records = [
        record
        for record in outcome_exposure.read_index()
        if record["exposure_id"] == expected_source_record
    ]
    checks = {
        "collection_hash_valid": collection.get("collection_sha256")
        == source.self_hash(collection, "collection_sha256"),
        "contract_binding_valid": collection.get("contract_sha256")
        == contract.get("contract_sha256"),
        "source_inspection_valid": source_inspection.get("state")
        == "SOURCE_CONTRACT_INSPECTED_READY"
        and source_inspection.get("valid") is True,
        "private_file_hash_valid": sha256_file(private_path)
        == collection.get("external_file_sha256"),
        "private_content_hash_valid": canonical_sha256(dataset)
        == collection.get("dataset_sha256"),
        "dataset_family_bound": dataset.get("family_id") == source.FAMILY_ID
        and dataset.get("symbols") == ["SPY"],
        "evaluation_dates_exact": dataset.get("evaluation_dates")
        == contract.get("development_dates"),
        "complete_xnys_rows": isinstance(rows, list)
        and [row.get("date") for row in rows] == expected_dates,
        "runtime_validation_passed": runtime_valid,
        "one_request_only": collection.get("provider_requests") == 1
        and collection.get("provider_telemetry", {}).get("requests") == 1,
        "no_retry_or_substitution": collection.get("retries") == 0
        and collection.get("substitutions") == 0,
        "strategy_metrics_absent": collection.get("strategy_metrics_computed") == 0,
        "confirmation_outcomes_absent": collection.get(
            "confirmation_prices_accessed"
        )
        is False,
        "development_exposure_indexed": len(source_records) == 1,
        "confirmation_still_untouched": not outcome_exposure.find_overlaps(
            contract["confirmation_scope"],
            outcome_exposure.read_index(),
        ),
        "broker_actions_zero": collection.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise SpyRsi2InspectionError(
            "SPY RSI(2) development collection inspection failed"
        )
    observed = source._timestamp(inspected_at, "inspected_at")
    evidence_paths = [
        source._repo_path(contract_path),
        source._repo_path(source_inspection_path),
        source._repo_path(collection_path),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{source.SUCCESSOR_ID}-development",
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
        root / "development-dataset",
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-development-collection-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "DEVELOPMENT_DATASET_INSPECTED_READY",
        "inspected_at": observed,
        "collection_path": source._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "dataset_manifest_path": source._repo_path(manifest_path),
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
    value["inspection_sha256"] = source.self_hash(value, "inspection_sha256")
    path = (
        root
        / "development-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
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


def main(argv: Sequence[str] | None = None) -> int:
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
        SpyRsi2InspectionError,
        source.SpyRsi2DataError,
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
