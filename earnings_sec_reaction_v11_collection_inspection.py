"""Inspect v11 data and bind it to the frozen development search."""

from __future__ import annotations

import argparse
import gzip
import json
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as metadata
import earnings_sec_reaction_v11_collection as source
import earnings_sec_reaction_v11_search as search_source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract


DATASET_ROOT = (
    metadata.PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / search_source.FAMILY_ID
    / "development-dataset"
)


class EarningsSecReactionV11CollectionInspectionError(RuntimeError):
    """The exact v11 dataset failed independent validation."""


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any], Path]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = metadata._read(collection_path)
    search_path = metadata.PROJECT_ROOT / collection["search_path"]
    inspection_path = (
        metadata.PROJECT_ROOT / collection["inspection_path"]
    )
    strategy_discovery.require_committed(search_path)
    strategy_discovery.require_committed(inspection_path)
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    search_inspection = metadata._read(inspection_path)
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
        raise EarningsSecReactionV11CollectionInspectionError(
            "private v11 dataset is missing or drifted"
        )
    try:
        dataset = json.loads(gzip.decompress(private_path.read_bytes()))
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsSecReactionV11CollectionInspectionError(
            "private v11 dataset is unreadable"
        ) from exc
    selected = search_source.selection(historical_store)
    allowed_dates = set(contract["development_scope"]["dates"])
    allowed_symbols = set(contract["universe"]["symbols"])
    rows_valid = True
    row_count = 0
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
        f"source-{source.FAMILY_ID}-v11-"
        f"{collection['collection_sha256'][:16]}"
    )
    checks = {
        "collection_hash_valid": collection["collection_sha256"]
        == v5.self_hash(collection, "collection_sha256"),
        "search_chain_valid": collection["search_sha256"]
        == search["artifact_sha256"]
        == search_inspection["search_sha256"],
        "search_inspected": search_inspection["state"]
        == "REACTION_V11_SEARCH_INSPECTED_READY_FOR_COLLECTION"
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
            == 108
        ),
        "request_accounting_complete": (
            collection["provider_telemetry"]["requests"]
            + collection["provider_telemetry"]["cache_hits"]
            == 108
        ),
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
        "zero_metrics_or_broker": collection[
            "strategy_metrics_computed"
        ]
        == 0
        and collection["confirmation_prices_accessed"] is False
        and collection["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsSecReactionV11CollectionInspectionError(
            "v11 development collection inspection failed"
        )
    observed = metadata._timestamp(inspected_at, "inspected_at")
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
                    metadata._repo_path(search_path),
                    metadata._repo_path(inspection_path),
                    metadata._repo_path(collection_path),
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
                },
            },
        },
        DATASET_ROOT,
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v11-collection-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "REACTION_V11_DEVELOPMENT_DATASET_INSPECTED_READY",
        "inspected_at": observed,
        "collection_path": metadata._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "search_sha256": search["artifact_sha256"],
        "dataset_manifest_path": metadata._repo_path(manifest_path),
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
        / "development-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    metadata._write(path, value)
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
                "path": metadata._repo_path(path),
                "manifest": metadata._repo_path(manifest),
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
