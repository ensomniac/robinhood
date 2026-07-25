"""Independently inspect SEC PEAD Yahoo source and development data."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as market
import earnings_sec_yahoo_data as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract


class EarningsSecYahooInspectionError(RuntimeError):
    """The exact Yahoo contract or collection failed reconstruction."""


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = market._read(contract_path)
    rebuilt = source.build_contract(created_at=contract["created_at"])
    massive = market._read(source.MASSIVE_CONTRACT)
    checks = {
        "contract_hash_valid": contract["contract_sha256"]
        == v5.self_hash(contract, "contract_sha256"),
        "contract_exactly_rebuilt": contract == rebuilt,
        "permission_failure_bound": contract["prior_source"][
            "failure_inspection_sha256"
        ]
        == "c0fe59ed6228e20edeedfbad294ef7034334b2ed2277e04c6507ecf4954fd60c",
        "selection_unchanged": contract["selection_sha256"]
        == massive["selection_sha256"],
        "scopes_unchanged": contract["development_scope"]
        == massive["development_scope"]
        and contract["confirmation_scope"] == massive["confirmation_scope"],
        "request_graph_unchanged": [
            (
                item["symbol"],
                item["start"],
                item["end"],
                item["predecessor_request_sha256"],
            )
            for item in contract["requests"]
        ]
        == [
            (
                item["symbol"],
                item["start"],
                item["end"],
                item["request_sha256"],
            )
            for item in massive["requests"]
        ],
        "no_purchase": contract["provider"]["no_purchase_required"] is True,
        "raw_not_dividend_adjusted": contract["data_semantics"][
            "raw_ohlcv_used"
        ]
        is True
        and contract["data_semantics"]["dividend_adjusted_close_used"] is False,
        "zero_retry_or_substitution": contract["request_policy"][
            "retries_permitted"
        ]
        == 0
        and contract["request_policy"]["substitutions_permitted"] == 0,
        "confirmation_closed": contract["request_policy"][
            "confirmation_requests_permitted"
        ]
        == 0,
        "outcomes_absent": contract["market_prices_accessed"] is False
        and contract["strategy_metrics_computed"] == 0
        and contract["confirmation_prices_accessed"] is False,
        "broker_actions_zero": contract["broker_actions"] == 0,
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
        raise EarningsSecYahooInspectionError(
            "Yahoo source contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-yahoo-development-source-contract-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "source_id": source.SOURCE_ID,
        "state": "YAHOO_CONTRACT_INSPECTED_READY",
        "inspected_at": market._timestamp(inspected_at, "inspected_at"),
        "contract_path": market._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "development_provider_access_authorized": True,
        "confirmation_provider_access_authorized": False,
        "authorized_provider_requests": len(contract["requests"]),
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = v5.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "yahoo-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    market._write(path, value)
    return path, value


def _dataset(
    collection: dict[str, Any],
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], Path]:
    relative = Path(collection["external_relative_path"])
    path = (store.root / relative).resolve()
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or store.root.resolve() not in path.parents
        or not path.is_file()
        or sha256_file(path) != collection["external_file_sha256"]
    ):
        raise EarningsSecYahooInspectionError(
            "private Yahoo dataset is missing or drifted"
        )
    try:
        value = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsSecYahooInspectionError(
            "private Yahoo dataset is unreadable"
        ) from exc
    return value, path


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any], Path]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = market._read(collection_path)
    contract_path = market.PROJECT_ROOT / collection["contract_path"]
    inspection_path = market.PROJECT_ROOT / collection["inspection_path"]
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = market._read(contract_path)
    contract_inspection = market._read(inspection_path)
    historical_store = store or HistoricalDayStore.from_env()
    dataset, private_path = _dataset(collection, historical_store)
    selection = market._development_selection(historical_store)
    allowed_dates = set(contract["development_scope"]["dates"])
    allowed_symbols = set(contract["development_scope"]["symbols"])
    row_count = 0
    rows_valid = True
    for symbol, rows in dataset.get("daily_bars", {}).items():
        dates = [row.get("date") for row in rows]
        row_count += len(rows)
        if (
            symbol not in allowed_symbols
            or dates != sorted(dates)
            or len(dates) != len(set(dates))
            or not set(dates) <= allowed_dates
        ):
            rows_valid = False
        for row in rows:
            try:
                prices = [
                    float(row[field])
                    for field in ("open", "high", "low", "close")
                ]
                volume = int(row["volume"])
            except (KeyError, TypeError, ValueError):
                rows_valid = False
                continue
            if (
                min(prices) <= 0
                or volume < 0
                or prices[2] > min(prices[0], prices[3])
                or prices[1] < max(prices[0], prices[3])
            ):
                rows_valid = False
    exposure_id = (
        f"source-{source.FAMILY_ID}-yahoo-"
        f"{collection['collection_sha256'][:16]}"
    )
    checks = {
        "collection_hash_valid": collection["collection_sha256"]
        == v5.self_hash(collection, "collection_sha256"),
        "contract_chain_valid": collection["contract_sha256"]
        == contract["contract_sha256"]
        == contract_inspection["contract_sha256"],
        "contract_inspected": contract_inspection["state"]
        == "YAHOO_CONTRACT_INSPECTED_READY"
        and contract_inspection["valid"] is True,
        "private_hash_valid": sha256_file(private_path)
        == collection["external_file_sha256"]
        and canonical_sha256(dataset) == collection["dataset_sha256"],
        "selection_exact": dataset["event_metadata_by_date"]
        == selection["event_metadata_by_date"]
        and dataset["evaluation_dates"] == selection["development_dates"],
        "family_bound": dataset["family_id"] == source.FAMILY_ID,
        "rows_valid": rows_valid and row_count == collection["row_count"],
        "symbol_accounting_complete": (
            collection["symbols_complete"]
            + collection["symbols_permanently_missing"]
            == collection["symbols_requested"]
            == len(contract["requests"])
        ),
        "request_accounting_complete": (
            collection["provider_telemetry"]["requests"]
            + collection["provider_telemetry"]["cache_hits"]
            == len(contract["requests"])
        ),
        "zero_retry_or_substitution": collection["retries"] == 0
        and collection["substitutions"] == 0,
        "development_exposure_indexed": any(
            item["exposure_id"] == exposure_id
            for item in outcome_exposure.read_index()
        ),
        "confirmation_untouched": not outcome_exposure.find_overlaps(
            contract["confirmation_scope"],
            outcome_exposure.read_index(),
        ),
        "zero_metrics_or_broker": collection["strategy_metrics_computed"] == 0
        and collection["confirmation_prices_accessed"] is False
        and collection["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsSecYahooInspectionError(
            "Yahoo development collection inspection failed"
        )
    observed = market._timestamp(inspected_at, "inspected_at")
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{source.SUCCESSOR_ID}-development",
            "registered_at": observed,
            "requested_dates": selection["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    market._repo_path(contract_path),
                    market._repo_path(inspection_path),
                    market._repo_path(collection_path),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "dense_runtime": {
                    "family_id": source.FAMILY_ID,
                    "external_relative_path": collection[
                        "external_relative_path"
                    ],
                    "external_file_sha256": collection[
                        "external_file_sha256"
                    ],
                    "dataset_sha256": collection["dataset_sha256"],
                    "format": "json.gz",
                    "formal_capacity": selection[
                        "development_event_count"
                    ],
                },
            },
        },
        root / "development-dataset",
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-yahoo-development-collection-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "source_id": source.SOURCE_ID,
        "state": "YAHOO_DEVELOPMENT_DATASET_INSPECTED_READY",
        "inspected_at": observed,
        "collection_path": market._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "dataset_manifest_path": market._repo_path(manifest_path),
        "dataset_manifest_file_sha256": sha256_file(manifest_path),
        "dataset_manifest_sha256": manifest["manifest_sha256"],
        "checks": checks,
        "symbols_complete": collection["symbols_complete"],
        "symbols_permanently_missing": collection[
            "symbols_permanently_missing"
        ],
        "row_count": row_count,
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = v5.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "yahoo-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    market._write(path, value)
    return path, value, manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("inspect-contract")
    contract.add_argument("artifact", type=Path)
    contract.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-development")
    collection.add_argument("artifact", type=Path)
    collection.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "inspect-contract":
        path, value = inspect_contract(
            args.artifact, inspected_at=args.inspected_at
        )
        manifest = None
    else:
        path, value, manifest = inspect_collection(
            args.artifact, inspected_at=args.inspected_at
        )
    print(
        json.dumps(
            {
                "path": market._repo_path(path),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
                "manifest": (
                    market._repo_path(manifest) if manifest else None
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
