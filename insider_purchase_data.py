"""Freeze, collect, and inspect Form 4 development-only daily market data."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import subprocess
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

import activist_earnings_data as yahoo
import insider_purchase_discovery as family
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = family.FAMILY_ID
CAMPAIGN_ID = family.CAMPAIGN_ID
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery" / FAMILY_ID
PRIVATE_NAMESPACE = Path("_derived/form4_insider_purchase_data")
ENDPOINT_TEMPLATE = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
SOURCE_ID = "yahoo-chart-no-purchase-v1"
REQUEST_START = "2017-11-01"
REQUEST_END = family.DEVELOPMENT_END
PACE_SECONDS = 0.20
CONTROLLER_FILES = (
    "insider_purchase_data.py",
    "activist_earnings_data.py",
)


class InsiderPurchaseDataError(RuntimeError):
    """A frozen request, response, or private runtime dataset drifted."""


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


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise InsiderPurchaseDataError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise InsiderPurchaseDataError(f"{field} needs a timezone")
    return value


def _period(day: str) -> int:
    return int(datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp())


def _exclusive_period(day: str) -> int:
    return int(
        (
            datetime.fromisoformat(f"{day}T00:00:00+00:00")
            + timedelta(days=1)
        ).timestamp()
    )


def _write(
    value: Mapping[str, Any], directory: Path, stem: str
) -> tuple[Path, dict[str, Any]]:
    content = dict(value)
    content.pop("artifact_sha256", None)
    digest = _hash(content)
    artifact = {**content, "artifact_sha256": digest}
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}-{digest}.json"
    rendered = json.dumps(artifact, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise InsiderPurchaseDataError("content-addressed artifact collision")
    if not path.exists():
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
    return path, artifact


def _load(path: Path, kind: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise InsiderPurchaseDataError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise InsiderPurchaseDataError("artifact must contain an object")
    supplied = value.get("artifact_sha256")
    content = {key: item for key, item in value.items() if key != "artifact_sha256"}
    expected = _hash(content)
    if (
        supplied != expected
        or not path.name.endswith(f"-{expected}.json")
        or value.get("artifact_kind") != kind
    ):
        raise InsiderPurchaseDataError("artifact hash, name, or kind drifted")
    return value


def _require_pushed_head() -> str:
    try:
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        upstream = subprocess.run(
            ["git", "rev-parse", "@{upstream}"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
    except subprocess.CalledProcessError as exc:
        raise InsiderPurchaseDataError(
            "cannot verify pushed Git authority"
        ) from exc
    if not head or head != upstream:
        raise InsiderPurchaseDataError(
            "provider access requires committed inputs and pushed HEAD"
        )
    return head


def _private_inventory(contract: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = PROJECT_ROOT / str(contract["capacity_manifest"])
    strategy_discovery.require_committed(manifest_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    capacity = manifest["dataset_payload"]["form4_purchase_capacity"]
    binding = capacity["private_inventory"]
    relative = Path(str(binding.get("relative_path", "")))
    if (
        binding.get("storage") != "LOCAL_HISTORICAL_DATA_ROOT"
        or relative.is_absolute()
        or ".." in relative.parts
    ):
        raise InsiderPurchaseDataError("private inventory binding is unsafe")
    path = HistoricalDayStore.from_env().root / relative
    if not path.is_file() or sha256_file(path) != binding.get("file_sha256"):
        raise InsiderPurchaseDataError("private family inventory file drifted")
    with gzip.open(path, "rt", encoding="utf-8") as source:
        value = json.load(source)
    if (
        not isinstance(value, dict)
        or canonical_sha256(value) != binding.get("content_sha256")
        or value.get("family_id") != FAMILY_ID
    ):
        raise InsiderPurchaseDataError("private family inventory content drifted")
    return value


def _request(symbol: str) -> dict[str, Any]:
    value = {
        "method": "GET",
        "endpoint": ENDPOINT_TEMPLATE.format(symbol=quote(symbol, safe="")),
        "parameters": {
            "period1": _period(REQUEST_START),
            "period2": _exclusive_period(REQUEST_END),
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        },
        "symbol": symbol,
        "start": REQUEST_START,
        "end": REQUEST_END,
    }
    return {**value, "request_sha256": _hash(value)}


def _source_scope(symbols: Sequence[str]) -> dict[str, Any]:
    dates = [
        day
        for day in family._load_calendar()
        if REQUEST_START <= day <= REQUEST_END
    ]
    return outcome_exposure.validate_scope(
        {"dates": dates, "symbols": list(symbols)}
    )


def _controller_hashes() -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in CONTROLLER_FILES:
        path = PROJECT_ROOT / relative
        strategy_discovery.require_committed(path)
        result[relative] = sha256_file(path)
    return result


def build_contract(search_path: Path, created_at: str) -> dict[str, Any]:
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    if search.get("state") != "SEARCH_FROZEN":
        raise InsiderPurchaseDataError("development search is not frozen")
    contract = search["family_contract"]
    inventory = _private_inventory(contract)
    events = inventory["development_events"]
    symbols = sorted({str(row["symbol"]) for row in events})
    if not (
        contract["family_id"] == FAMILY_ID
        and len(contract["trial_family"]) == 32
        and len(events) == 7049
        and len(symbols) == 2589
    ):
        raise InsiderPurchaseDataError(
            "frozen search or development inventory drifted"
        )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], outcome_exposure.read_index()
    )
    requests_ = [_request(symbol) for symbol in symbols]
    return {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-source-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "created_at": _timestamp(created_at, "created_at"),
        "state": "SOURCE_CONTRACT_FROZEN",
        "search_path": _relative(search_path),
        "search_sha256": search["artifact_sha256"],
        "development_dates": contract["development_dates"],
        "development_signal_dates": contract["development_signal_dates"],
        "development_scope": contract["development_scope"],
        "source_scope": _source_scope(symbols),
        "confirmation_scope": contract["confirmation_scope"],
        "event_inventory_content_sha256": canonical_sha256(inventory),
        "development_event_count": len(events),
        "symbols": symbols,
        "controller_hashes": _controller_hashes(),
        "provider": {
            "source_id": SOURCE_ID,
            "name": "Yahoo Finance historical chart JSON",
            "no_purchase_required": True,
            "exchange_timezone_required": "America/New_York",
        },
        "requests": requests_,
        "request_policy": {
            "authorized_provider_requests": len(requests_),
            "pacing_seconds_between_requests": PACE_SECONDS,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "resume_from_hash_valid_tasks": True,
            "permanent_missing": "blocks_entry_date_no_substitution",
            "confirmation_requests_permitted": 0,
        },
        "data_semantics": {
            "raw_ohlcv_used": True,
            "dividend_adjusted_close_used": False,
            "null_rows": "omitted_as_missing_sessions",
            "missing_symbol_or_session": "missed_trade_never_substitute",
            "development_prices_only": True,
            "warmup_start": REQUEST_START,
            "evaluation_end": REQUEST_END,
        },
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }


def freeze_contract(
    search_path: Path, created_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(search_path)
    return _write(
        build_contract(search_path, created_at),
        DEFAULT_ROOT / "development-source-contract",
        "source-contract",
    )


def inspect_contract(
    contract_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    contract = _load(
        contract_path, "form4-purchase-development-source-contract"
    )
    search_path = PROJECT_ROOT / str(contract["search_path"])
    strategy_discovery.require_committed(search_path)
    rebuilt = build_contract(search_path, str(contract["created_at"]))
    if {
        key: item for key, item in contract.items() if key != "artifact_sha256"
    } != rebuilt:
        raise InsiderPurchaseDataError(
            "source contract does not match an independent rebuild"
        )
    task_root = (
        HistoricalDayStore.from_env().root
        / PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / "tasks"
    )
    if task_root.exists() and any(task_root.iterdir()):
        raise InsiderPurchaseDataError(
            "provider task namespace is not empty before inspection"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-source-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "state": "SOURCE_CONTRACT_INSPECTED_READY",
        "contract_path": _relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "search_path": contract["search_path"],
        "search_sha256": contract["search_sha256"],
        "authorized_provider_requests": len(contract["requests"]),
        "confirmation_requests_permitted": 0,
        "inspection": {
            "contract_hash_rebuilt": True,
            "search_binding_rebuilt": True,
            "event_inventory_rebuilt": True,
            "controller_hashes_rebuilt": True,
            "request_hashes_rebuilt": True,
            "full_source_scope_rebuilt": True,
            "confirmation_untouched_rebuilt": True,
            "empty_task_namespace_rebuilt": True,
            "valid": True,
        },
        "development_provider_access_authorized": True,
        "confirmation_provider_access_authorized": False,
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    return _write(
        payload,
        DEFAULT_ROOT / "development-source-contract-inspection",
        "source-inspection",
    )


def _event_metadata(
    inventory: Mapping[str, Any], evaluation_dates: Sequence[str]
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {
        day: [] for day in evaluation_dates
    }
    for event in inventory["development_events"]:
        entry_date = str(event["entry_date"])
        if entry_date not in result:
            raise InsiderPurchaseDataError(
                "development event escaped the account calendar"
            )
        result[entry_date].append(
            {
                "symbol": event["symbol"],
                "issuer_cik": event["issuer_cik"],
                "filing_date": event["filing_date"],
                "filing_dates": event["filing_dates"],
                "entry_date": entry_date,
                "purchase_notional": event["purchase_notional"],
                "distinct_reporting_owners": event[
                    "distinct_reporting_owners"
                ],
                "transaction_count": event["transaction_count"],
                "event_semantics": event["event_semantics"],
            }
        )
    for day in result:
        result[day].sort(
            key=lambda row: (
                -int(row["distinct_reporting_owners"]),
                -float(row["purchase_notional"]),
                str(row["symbol"]),
                str(row["issuer_cik"]),
            )
        )
    return result


def collect(
    contract_path: Path,
    inspection_path: Path,
    collected_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    published_commit = _require_pushed_head()
    contract = _load(
        contract_path, "form4-purchase-development-source-contract"
    )
    inspection = _load(
        inspection_path, "form4-purchase-development-source-inspection"
    )
    if not (
        inspection["state"] == "SOURCE_CONTRACT_INSPECTED_READY"
        and inspection["contract_sha256"] == contract["artifact_sha256"]
        and inspection["development_provider_access_authorized"] is True
        and inspection["confirmation_provider_access_authorized"] is False
        and contract["controller_hashes"] == _controller_hashes()
    ):
        raise InsiderPurchaseDataError("provider authorization is invalid")
    store = HistoricalDayStore.from_env()
    task_root = (
        store.root
        / PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / "tasks"
    )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
        "permanent_missing_responses": 0,
    }
    tasks: list[dict[str, Any]] = []
    http = requests.Session()
    try:
        for index, request in enumerate(contract["requests"]):
            task_path = task_root / f"{request['request_sha256']}.json.gz"
            if task_path.is_file():
                task = yahoo._read_private(task_path)
                telemetry["cache_hits"] += 1
            else:
                if index and telemetry["requests"]:
                    time.sleep(PACE_SECONDS)
                    telemetry["pacing_wait_seconds"] += PACE_SECONDS
                task = yahoo._fetch(request, http, telemetry)
                yahoo._write_private(task_path, task)
            content = {
                key: item for key, item in task.items() if key != "task_sha256"
            }
            if (
                task.get("request_sha256") != request["request_sha256"]
                or task.get("task_sha256") != _hash(content)
            ):
                raise InsiderPurchaseDataError("Yahoo task binding drifted")
            tasks.append(task)
    finally:
        http.close()
    if telemetry["requests"] + telemetry["cache_hits"] != len(
        contract["requests"]
    ):
        raise InsiderPurchaseDataError("request accounting is incomplete")
    search_path = PROJECT_ROOT / str(contract["search_path"])
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    inventory = _private_inventory(search["family_contract"])
    daily_bars = {
        str(task["symbol"]): list(task["rows"])
        for task in tasks
        if task["status"] == "COMPLETE"
    }
    missing = {
        str(task["symbol"]): str(task["missing_reason"])
        for task in tasks
        if task["status"] == "PERMANENT_MISSING"
    }
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "sample_phase": "development",
        "evaluation_dates": contract["development_dates"],
        "signal_dates": contract["development_signal_dates"],
        "event_metadata_by_date": _event_metadata(
            inventory, contract["development_dates"]
        ),
        "daily_bars": daily_bars,
        "missing_symbols": missing,
        "source_semantics": {
            "provider": SOURCE_ID,
            "contract_sha256": contract["artifact_sha256"],
            "raw_ohlcv_used": True,
            "dividend_adjusted_close_used": False,
            "confirmation_prices_accessed": False,
        },
    }
    content_sha256 = canonical_sha256(dataset)
    relative = (
        PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / content_sha256
        / "development.json.gz"
    )
    private_path = store.root / relative
    yahoo._write_private(private_path, dataset)
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "collected_at": _timestamp(collected_at, "collected_at"),
        "state": "DEVELOPMENT_COLLECTED_UNINSPECTED",
        "published_commit": published_commit,
        "contract_path": _relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspection_path": _relative(inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "search_path": contract["search_path"],
        "search_sha256": contract["search_sha256"],
        "private_dataset": {
            "storage": "LOCAL_HISTORICAL_DATA_ROOT",
            "relative_path": str(relative),
            "content_sha256": content_sha256,
            "file_sha256": sha256_file(private_path),
            "compression": "gzip",
            "format": "canonical-json",
        },
        "symbols_requested": len(tasks),
        "symbols_complete": len(daily_bars),
        "symbols_permanently_missing": len(missing),
        "row_count": sum(len(rows) for rows in daily_bars.values()),
        "event_count": sum(
            len(rows) for rows in dataset["event_metadata_by_date"].values()
        ),
        "provider_telemetry": telemetry,
        "substitutions": 0,
        "retries": 0,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    path, artifact = _write(
        payload,
        DEFAULT_ROOT / "development-collection",
        "development-collection",
    )
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"development-source-{FAMILY_ID}-"
                f"{artifact['artifact_sha256'][:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=str(artifact["collected_at"]),
            source_path=_relative(path),
            source_sha256=sha256_file(path),
            scope=contract["source_scope"],
        )
    )
    return path, artifact


def inspect_collection(
    collection_path: Path, inspected_at: str
) -> tuple[Path, Path, dict[str, Any]]:
    strategy_discovery.require_committed(collection_path)
    strategy_discovery.require_committed(outcome_exposure.DEFAULT_INDEX)
    collection = _load(
        collection_path, "form4-purchase-development-collection"
    )
    private = collection["private_dataset"]
    store = HistoricalDayStore.from_env()
    private_path = store.root / str(private["relative_path"])
    if (
        not private_path.is_file()
        or sha256_file(private_path) != private["file_sha256"]
    ):
        raise InsiderPurchaseDataError("private development file drifted")
    dataset = yahoo._read_private(private_path)
    if canonical_sha256(dataset) != private["content_sha256"]:
        raise InsiderPurchaseDataError("private development content drifted")
    search_path = PROJECT_ROOT / str(collection["search_path"])
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    contract = search["family_contract"]
    inventory = _private_inventory(contract)
    expected_metadata = _event_metadata(
        inventory, contract["development_dates"]
    )
    bars = dataset.get("daily_bars")
    if not (
        dataset.get("family_id") == FAMILY_ID
        and dataset.get("sample_phase") == "development"
        and dataset.get("evaluation_dates") == contract["development_dates"]
        and dataset.get("signal_dates") == contract["development_signal_dates"]
        and dataset.get("event_metadata_by_date") == expected_metadata
        and isinstance(bars, Mapping)
        and collection["event_count"] == sum(map(len, expected_metadata.values()))
        and collection["row_count"]
        == sum(len(rows) for rows in bars.values())
        and collection["symbols_complete"] == len(bars)
        and collection["symbols_complete"]
        + collection["symbols_permanently_missing"]
        == collection["symbols_requested"]
    ):
        raise InsiderPurchaseDataError("development dataset rebuild failed")
    for symbol, rows in bars.items():
        if not isinstance(rows, list) or [
            row.get("date") for row in rows
        ] != sorted({str(row.get("date")) for row in rows}):
            raise InsiderPurchaseDataError(f"daily rows are invalid for {symbol}")
    source_contract = _load(
        PROJECT_ROOT / str(collection["contract_path"]),
        "form4-purchase-development-source-contract",
    )
    exposure_matches = [
        record
        for record in outcome_exposure.read_index()
        if record["source_path"] == _relative(collection_path)
    ]
    if not (
        len(exposure_matches) == 1
        and exposure_matches[0]["scope"] == source_contract["source_scope"]
        and exposure_matches[0]["lane"] == "development"
    ):
        raise InsiderPurchaseDataError(
            "development source exposure was not indexed exactly"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-purchase-development-collection-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "state": "DATASET_INSPECTED_READY",
        "collection_path": _relative(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "search_path": collection["search_path"],
        "search_sha256": collection["search_sha256"],
        "private_dataset": private,
        "symbols_complete": collection["symbols_complete"],
        "symbols_permanently_missing": collection[
            "symbols_permanently_missing"
        ],
        "row_count": collection["row_count"],
        "event_count": collection["event_count"],
        "provider_telemetry": collection["provider_telemetry"],
        "inspection": {
            "collection_hash_rebuilt": True,
            "private_file_hash_rebuilt": True,
            "private_content_hash_rebuilt": True,
            "search_binding_rebuilt": True,
            "event_metadata_rebuilt": True,
            "daily_row_accounting_rebuilt": True,
            "complete_source_exposure_record_rebuilt": True,
            "confirmation_prices_absent": True,
            "valid": True,
        },
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    inspection_path, inspection = _write(
        payload,
        DEFAULT_ROOT / "development-collection-inspection",
        "development-inspection",
    )
    manifest = {
        "schema_version": 1,
        "dataset_id": (
            f"dataset-{FAMILY_ID}-development-"
            f"{collection['search_sha256'][:16]}"
        ),
        "registered_at": inspected_at,
        "requested_dates": contract["development_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "evidence_paths": [
                collection["search_path"],
                _relative(collection_path),
                _relative(inspection_path),
                "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
            ],
            "inspected": True,
            "point_in_time_evidence": True,
            "development_search_sha256": collection["search_sha256"],
            "form4_purchase_runtime": {
                "family_id": FAMILY_ID,
                "sample_phase": "development",
                "private_dataset": private,
                "collection_inspection_sha256": inspection[
                    "artifact_sha256"
                ],
                "confirmation_prices_accessed": False,
            },
        },
    }
    manifest_path, _manifest = freeze_dataset_contract(
        manifest, DEFAULT_ROOT / "development-dataset"
    )
    return inspection_path, manifest_path, inspection


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("search", type=Path)
    freeze.add_argument("--created-at", required=True)
    inspect_source = subparsers.add_parser("inspect-contract")
    inspect_source.add_argument("contract", type=Path)
    inspect_source.add_argument("--inspected-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    inspect_data = subparsers.add_parser("inspect-collection")
    inspect_data.add_argument("collection", type=Path)
    inspect_data.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(args.search, args.created_at)
        result: Any = {
            "path": _relative(path),
            "state": value["state"],
            "requests": len(value["requests"]),
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "inspect-contract":
        path, value = inspect_contract(args.contract, args.inspected_at)
        result = {
            "path": _relative(path),
            "state": value["state"],
            "authorized_provider_requests": value[
                "authorized_provider_requests"
            ],
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "collect":
        path, value = collect(
            args.contract, args.inspection, args.collected_at
        )
        result = {
            "path": _relative(path),
            "state": value["state"],
            "symbols_complete": value["symbols_complete"],
            "symbols_permanently_missing": value[
                "symbols_permanently_missing"
            ],
            "row_count": value["row_count"],
            "provider_telemetry": value["provider_telemetry"],
            "artifact_sha256": value["artifact_sha256"],
        }
    else:
        inspection, manifest, value = inspect_collection(
            args.collection, args.inspected_at
        )
        result = {
            "inspection": _relative(inspection),
            "manifest": _relative(manifest),
            "state": value["state"],
            "row_count": value["row_count"],
            "artifact_sha256": value["artifact_sha256"],
        }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
