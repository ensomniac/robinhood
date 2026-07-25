"""Inspect v14 development data and bind it to the frozen search."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any

import earnings_sec_corrected_expansion as capacity
import earnings_sec_reaction_v14_collection as source
import earnings_sec_reaction_v14_search as search_source
import outcome_exposure
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract


DATASET_ROOT = (
    search_source.PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / search_source.FAMILY_ID
    / "development-dataset"
)


class EarningsSecReactionV14CollectionInspectionError(RuntimeError):
    """The exact v14 development dataset failed validation."""


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any], Path]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = capacity._read(collection_path)
    search_path = search_source.PROJECT_ROOT / collection["search_path"]
    inspection_path = (
        search_source.PROJECT_ROOT / collection["inspection_path"]
    )
    strategy_discovery.require_committed(search_path)
    strategy_discovery.require_committed(inspection_path)
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    search_inspection = capacity._read(inspection_path)
    contract = search["family_contract"]
    historical_store = store or HistoricalDayStore.from_env()
    relative = Path(collection["external_relative_path"])
    private_path = (historical_store.root / relative).resolve()
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or historical_store.root.resolve() not in private_path.parents
        or not private_path.is_file()
        or sha256_file(private_path)
        != collection["external_file_sha256"]
    ):
        raise EarningsSecReactionV14CollectionInspectionError(
            "private v14 dataset is missing or drifted"
        )
    try:
        dataset = json.loads(gzip.decompress(private_path.read_bytes()))
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsSecReactionV14CollectionInspectionError(
            "private v14 dataset is unreadable"
        ) from exc
    selected = search_source.selection(historical_store)
    allowed_dates = set(contract["development_scope"]["dates"])
    allowed_symbols = set(contract["universe"]["symbols"])
    excluded_symbols = set(contract["universe"]["excluded_symbols"])
    bars = dataset.get("daily_bars", {})
    missing = dataset.get("missing_symbols", {})
    rows_valid = isinstance(bars, dict) and isinstance(missing, dict)
    row_count = 0
    if rows_valid:
        for symbol, rows in bars.items():
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
        if (
            not set(missing) <= allowed_symbols
            or set(missing).intersection(bars)
            or set(bars).union(missing) != allowed_symbols
        ):
            rows_valid = False
    invalid_ohlcv_symbols = sorted(
        symbol
        for symbol, reason in missing.items()
        if reason == source.INVALID_OHLCV_REASON
    )
    exposure_id = (
        f"source-{source.FAMILY_ID}-v14-"
        f"{collection['collection_sha256'][:16]}"
    )
    event_symbols = {
        str(row["symbol"])
        for rows in dataset.get("event_metadata_by_date", {}).values()
        for row in rows
    }
    checks = {
        "collection_hash_valid": collection["collection_sha256"]
        == capacity.self_hash(collection, "collection_sha256"),
        "search_chain_valid": collection["search_sha256"]
        == search["artifact_sha256"]
        == search_inspection["search_sha256"],
        "search_inspected": search_inspection["state"]
        == "REACTION_V14_SEARCH_INSPECTED_READY_FOR_COLLECTION"
        and search_inspection["valid"] is True,
        "private_hash_valid": sha256_file(private_path)
        == collection["external_file_sha256"]
        and canonical_sha256(dataset) == collection["dataset_sha256"],
        "selection_exact": dataset["evaluation_dates"]
        == selected["development_dates"]
        and dataset["event_metadata_by_date"]
        == selected["development_metadata_by_date"],
        "family_bound": dataset["family_id"] == source.FAMILY_ID,
        "rows_valid": rows_valid
        and row_count == collection["row_count"],
        "symbol_accounting_complete": (
            collection["symbols_complete"]
            + collection["symbols_permanently_missing"]
            == collection["symbols_requested"]
            == search_source.EXPECTED_DEVELOPMENT_SYMBOLS
            and len(bars) == collection["symbols_complete"]
            and len(missing)
            == collection["symbols_permanently_missing"]
        ),
        "request_accounting_complete": (
            collection["provider_telemetry"]["requests"]
            + collection["provider_telemetry"]["cache_hits"]
            == search_source.EXPECTED_DEVELOPMENT_SYMBOLS
        ),
        "invalid_ohlcv_accounting_exact": invalid_ohlcv_symbols
        == collection["invalid_ohlcv_symbols"]
        and collection["provider_telemetry"][
            "permanent_missing_responses"
        ]
        >= len(invalid_ohlcv_symbols),
        "v13_prefix_absent": not excluded_symbols.intersection(
            set(bars).union(missing).union(event_symbols)
        )
        and collection["v13_tasks_reused"] == 0
        and dataset["source_semantics"]["v13_tasks_reused"] is False,
        "source_policy_exact": dataset["source_semantics"][
            "invalid_ohlcv_is_whole_symbol_permanent_missing"
        ]
        is True,
        "zero_retry_or_substitution": collection["retries"] == 0
        and collection["substitutions"] == 0,
        "development_exposure_indexed": any(
            record["exposure_id"] == exposure_id
            for record in outcome_exposure.read_index()
        ),
        "confirmation_untouched": not outcome_exposure.find_overlaps(
            contract["confirmation_scope"],
            outcome_exposure.read_index(),
        ),
        "cumulative_selection_bound": len(
            contract["prior_trial_daily_returns_by_id"]
        )
        == 32
        and contract["selection_accounting"]["cumulative_trial_count"]
        == 64,
        "zero_metrics_or_broker": collection[
            "strategy_metrics_computed"
        ]
        == 0
        and collection["confirmation_prices_accessed"] is False
        and collection["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsSecReactionV14CollectionInspectionError(
            "v14 development collection inspection failed"
        )
    observed = capacity._timestamp(inspected_at, "inspected_at")
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{search_source.SUCCESSOR_ID}-development"
            ),
            "registered_at": observed,
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    capacity._repo_path(search_path),
                    capacity._repo_path(inspection_path),
                    capacity._repo_path(collection_path),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "development_search_sha256": search[
                    "artifact_sha256"
                ],
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
                    "formal_capacity": selected[
                        "development_event_count"
                    ],
                    "excluded_v13_symbols": len(excluded_symbols),
                    "invalid_ohlcv_symbols": len(
                        invalid_ohlcv_symbols
                    ),
                },
            },
        },
        DATASET_ROOT,
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v14-collection-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "REACTION_V14_DEVELOPMENT_DATASET_INSPECTED_READY",
        "inspected_at": observed,
        "collection_path": capacity._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "search_sha256": search["artifact_sha256"],
        "dataset_manifest_path": capacity._repo_path(manifest_path),
        "dataset_manifest_file_sha256": sha256_file(manifest_path),
        "dataset_manifest_sha256": manifest["manifest_sha256"],
        "checks": checks,
        "symbols_complete": collection["symbols_complete"],
        "symbols_permanently_missing": collection[
            "symbols_permanently_missing"
        ],
        "invalid_ohlcv_symbols": invalid_ohlcv_symbols,
        "row_count": row_count,
        "excluded_v13_symbols": len(excluded_symbols),
        "cumulative_trial_count": 64,
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "development-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    capacity._write(path, value)
    return path, value, manifest_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", type=Path)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value, manifest = inspect_collection(
        args.collection, inspected_at=args.inspected_at
    )
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "manifest": capacity._repo_path(manifest),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
                "invalid_ohlcv_symbols": value[
                    "invalid_ohlcv_symbols"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
