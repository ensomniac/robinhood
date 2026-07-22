"""Freeze and collect the exact Schedule 13D quarterly-index request graph."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
from historical_concurrency import ordered_bounded_results
from historical_discovery import HistoricalDiscoveryError, SecClient, SecConfig
from historical_store import (
    DEFAULT_MIN_FREE_BYTES,
    HistoricalStoreConfig,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
DATASET_ID = "dataset-schedule-13d-capacity-2026-07-22-v1"
CONTRACT_SHA256 = (
    "dbd63296047961fd5d0546627c79c66607ef3498a69fba15f05f974a14527fab"
)
CONTRACT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/manifests/"
    f"{capacity.CANDIDATE_ID}-{CONTRACT_SHA256}.json"
)
CONTRACT_STATUS_PATH = capacity.DEFAULT_STATUS
GRAPH_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/indexes/manifests"
GRAPH_STATUS_PATH = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/indexes/request-status.json"
)
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/indexes/collection-status.json"
)
COLLECTION_INSPECTION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/indexes/inspections"
)
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_index_collection_inspection.py"
ENV_PATH = PROJECT_ROOT / ".env"
SHARED_CACHE_NAMESPACE = "_sources/sec/schedule13d-quarterly-indexes-v1"
PRIVATE_NAMESPACE = f"_derived/schedule13d_capacity/{DATASET_ID}/indexes"
PRIVATE_INDEX_NAME = "initial-sc13d-index.json.gz"
START_YEAR = 2022
END_YEAR = 2025
WORKERS = 4
MINIMUM_SPACING_SECONDS = 0.15
TIMEOUT_SECONDS = 30.0


class Schedule13dIndexCollectionError(RuntimeError):
    """The frozen SEC quarterly-index graph or collection is invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dIndexCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dIndexCollectionError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dIndexCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dIndexCollectionError(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_gzip_json(value: Mapping[str, Any], path: Path) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(_canonical_bytes(value))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_contract_and_inspection() -> tuple[dict[str, Any], dict[str, Any]]:
    frozen = capacity.load_contract(CONTRACT_PATH)
    inspected = _read_object(CONTRACT_STATUS_PATH)
    if not (
        frozen == capacity.build_contract()
        and frozen.get("contract_sha256") == CONTRACT_SHA256
        and inspected.get("contract_sha256") == CONTRACT_SHA256
        and inspected.get("inspection_sha256")
        == capacity.successor._self_hash(inspected, "inspection_sha256")
        and inspected.get("status") == "CAPACITY_CONTRACT_INSPECTED"
        and inspected.get("sec_source_access_permitted") is True
        and inspected.get("capacity_classification_permitted") is True
        and inspected.get("market_price_access_permitted") is False
        and inspected.get("outcome_access_permitted") is False
        and inspected.get("broker_actions_permitted") is False
        and inspected.get("valid") is True
    ):
        raise Schedule13dIndexCollectionError(
            "Schedule 13D capacity contract is not inspected for SEC access"
        )
    return frozen, inspected


def _store_config() -> HistoricalStoreConfig:
    config = HistoricalStoreConfig.from_env(ENV_PATH)
    minimum = max(DEFAULT_MIN_FREE_BYTES, config.min_free_bytes)
    if (
        config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
        or shutil.disk_usage(config.root).free < minimum
    ):
        raise Schedule13dIndexCollectionError(
            "private historical storage is unsafe or lacks its frozen reserve"
        )
    return config


def _sec_config(store_root: Path) -> SecConfig:
    config = SecConfig.from_env(
        ENV_PATH, store_root / SHARED_CACHE_NAMESPACE, workers=WORKERS
    )
    if not (
        config.minimum_spacing_seconds == MINIMUM_SPACING_SECONDS
        and config.timeout_seconds == TIMEOUT_SECONDS
    ):
        raise Schedule13dIndexCollectionError("SEC client pacing contract drifted")
    return config


def _request(year: int, quarter: int) -> dict[str, Any]:
    url = (
        "https://www.sec.gov/Archives/edgar/full-index/"
        f"{year}/QTR{quarter}/master.idx"
    )
    value = {
        "year": year,
        "quarter": quarter,
        "url": url,
        "shared_cache_relative_path": f"{year}/QTR{quarter}/master.idx",
    }
    value["request_sha256"] = _sha256_json(value)
    return value


def build_request_graph() -> dict[str, Any]:
    """Build the exact zero-count request graph without provider access."""

    frozen, inspected = _load_contract_and_inspection()
    store = _store_config()
    sec = _sec_config(store.root)
    requests = [
        _request(year, quarter)
        for year in range(START_YEAR, END_YEAR + 1)
        for quarter in range(1, 5)
    ]
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-sec-quarterly-index-request-graph",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "dataset_id": DATASET_ID,
        "contract_sha256": frozen["contract_sha256"],
        "contract_inspection_sha256": inspected["inspection_sha256"],
        "request_contract": {
            "provider": "SEC_EDGAR",
            "endpoint_kind": "quarterly master index",
            "request_count": len(requests),
            "collection_start": capacity.COLLECTION_START,
            "collection_end_inclusive": capacity.COLLECTION_END,
            "covered_years": list(range(START_YEAR, END_YEAR + 1)),
            "covered_quarters_per_year": [1, 2, 3, 4],
            "requests": requests,
            "workers": WORKERS,
            "global_minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "maximum_requests_per_second": 1 / MINIMUM_SPACING_SECONDS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "maximum_attempts": 4,
            "provider_substitution_permitted": False,
            "user_agent_sha256": hashlib.sha256(sec.user_agent.encode()).hexdigest(),
        },
        "storage_contract": {
            "shared_cache_namespace": SHARED_CACHE_NAMESPACE,
            "private_derived_namespace": PRIVATE_NAMESPACE,
            "minimum_free_bytes_after_reserve": max(
                DEFAULT_MIN_FREE_BYTES, store.min_free_bytes
            ),
            "raw_source_retention_required": True,
            "historical_deletion_permitted": False,
            "repository_storage_permitted": False,
        },
        "denominator_contract": {
            "retain_every_exact_form_sc13d_row": True,
            "forms_included": ["SC 13D"],
            "forms_excluded": ["SC 13D/A", "SC 13G", "SC 13G/A"],
            "quarter_boundary_rows_retained_until_acceptance_time_classification": True,
            "terminal_reason_required_later_for_every_retained_row": True,
            "filing_count": None,
            "verified_event_count": None,
        },
        "access_contract": {
            "provider_access_before_graph_inspection_permitted": False,
            "provider_access_after_graph_inspection_permitted": True,
            "accession_document_access_permitted": False,
            "market_price_access_permitted": False,
            "entry_fill_access_permitted": False,
            "exit_or_stop_access_permitted": False,
            "forward_return_computation_permitted": False,
            "stage0_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            str(path.relative_to(PROJECT_ROOT)): sha256_file(path)
            for path in (Path(__file__).resolve(), INSPECTOR_PATH)
        },
        "filing_count": None,
        "verified_event_count": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "claim_limit": (
            "This graph freezes only 16 quarterly SEC index requests. It contains "
            "no filing count, document content, price, fill, return, or maturity evidence."
        ),
    }
    graph["request_graph_sha256"] = capacity.successor._self_hash(
        graph, "request_graph_sha256"
    )
    return graph


def graph_path(graph: Mapping[str, Any], root: Path = GRAPH_ROOT) -> Path:
    return root / f"{capacity.CANDIDATE_ID}-{graph['request_graph_sha256']}.json"


def load_request_graph(path: Path) -> dict[str, Any]:
    graph = _read_object(path)
    digest = graph.get("request_graph_sha256")
    if not (
        isinstance(digest, str)
        and digest == capacity.successor._self_hash(graph, "request_graph_sha256")
        and path.name == f"{capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise Schedule13dIndexCollectionError("request graph was mutated or renamed")
    return graph


def freeze_request_graph(
    *, root: Path = GRAPH_ROOT, status_path: Path = GRAPH_STATUS_PATH
) -> tuple[Path, dict[str, Any]]:
    graph = build_request_graph()
    path = graph_path(graph, root)
    if path.exists() and _read_object(path) != graph:
        raise Schedule13dIndexCollectionError(
            "content-addressed request graph has other content"
        )
    _write_json(graph, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": capacity.CANDIDATE_ID,
            "dataset_id": DATASET_ID,
            "contract_sha256": CONTRACT_SHA256,
            "request_graph_sha256": graph["request_graph_sha256"],
            "status": "INDEX_REQUEST_GRAPH_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "filing_count": None,
            "verified_event_count": None,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, graph


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise Schedule13dIndexCollectionError(
            f"publication source is outside repository: {path}"
        ) from exc


def _published_source(path: Path) -> dict[str, str]:
    relative = _repo_relative(path)
    status = subprocess.run(
        ["git", "status", "--porcelain", "--", relative],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise Schedule13dIndexCollectionError(
            f"provider input is not committed: {relative}"
        )
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
    if head != upstream:
        raise Schedule13dIndexCollectionError(
            "provider access requires HEAD to equal its pushed upstream"
        )
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    digest = sha256_file(path)
    if hashlib.sha256(committed).hexdigest() != digest:
        raise Schedule13dIndexCollectionError(f"committed bytes differ: {relative}")
    return {"commit": head, "path": relative, "sha256": digest}


def _resolve_cache_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise Schedule13dIndexCollectionError("SEC cache path is unsafe")
    return store_root / SHARED_CACHE_NAMESPACE / relative


def _private_index_path(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / PRIVATE_INDEX_NAME


def _parse_master_index(text: str, request: Mapping[str, Any]) -> dict[str, Any]:
    header = "CIK|Company Name|Form Type|Date Filed|Filename"
    if header not in text:
        raise Schedule13dIndexCollectionError(
            f"quarterly master index lacks its header: {request['url']}"
        )
    started = False
    all_rows = 0
    retained: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not started:
            if line.startswith("-----"):
                started = True
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company, form, filed_on, filename = (part.strip() for part in parts)
        all_rows += 1
        if form != "SC 13D":
            continue
        try:
            date.fromisoformat(filed_on)
        except ValueError as exc:
            raise Schedule13dIndexCollectionError(
                f"SC 13D index row has invalid filing date: {filed_on}"
            ) from exc
        expected_prefix = f"edgar/data/{int(cik)}/" if cik.isdigit() else ""
        if not expected_prefix or not filename.startswith(expected_prefix):
            raise Schedule13dIndexCollectionError(
                f"SC 13D index row has invalid CIK or filename: {filename}"
            )
        retained.append(
            {
                "year": int(request["year"]),
                "quarter": int(request["quarter"]),
                "cik": cik,
                "company": company,
                "form": form,
                "filed_on": filed_on,
                "filename": filename,
                "complete_submission_url": (
                    "https://www.sec.gov/Archives/" + filename
                ),
                "classification_status": "PENDING_ACCESSION_DOCUMENT",
                "terminal_reason": None,
            }
        )
    if not started or all_rows == 0:
        raise Schedule13dIndexCollectionError(
            f"quarterly master index has no parseable denominator: {request['url']}"
        )
    return {"all_index_rows": all_rows, "initial_sc13d_rows": retained}


def _load_graph_for_collection(graph_path_value: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    graph = load_request_graph(graph_path_value)
    expected = build_request_graph()
    status = _read_object(GRAPH_STATUS_PATH)
    for relative, digest in graph["implementation_hashes"].items():
        if sha256_file(PROJECT_ROOT / relative) != digest:
            raise Schedule13dIndexCollectionError(
                f"request-graph implementation drifted: {relative}"
            )
    if not (
        graph == expected
        and status.get("status") == "INDEX_REQUEST_GRAPH_INSPECTED"
        and status.get("request_graph_sha256") == graph["request_graph_sha256"]
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("accession_document_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise Schedule13dIndexCollectionError(
            "quarterly-index graph is not independently inspected"
        )
    return graph, status


def _collect_one(
    client: SecClient, store_root: Path, request: Mapping[str, Any]
) -> dict[str, Any]:
    cache_path = _resolve_cache_path(store_root, request)
    existed = cache_path.is_file()
    text = client.text(str(request["url"]), cache_path)
    raw = cache_path.read_bytes()
    parsed = _parse_master_index(text, request)
    return {
        "year": request["year"],
        "quarter": request["quarter"],
        "request_sha256": request["request_sha256"],
        "source_origin": "SHARED_CACHE" if existed else "SEC_DOWNLOAD",
        "source_bytes": len(raw),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        **parsed,
    }


def _build_private_index(
    *,
    graph: Mapping[str, Any],
    quarter_results: Sequence[Mapping[str, Any]],
    captured_at: str | None = None,
) -> dict[str, Any]:
    rows = [
        dict(row)
        for quarter in quarter_results
        for row in quarter["initial_sc13d_rows"]
    ]
    filenames = [str(row["filename"]) for row in rows]
    if len(filenames) != len(set(filenames)):
        raise Schedule13dIndexCollectionError(
            "quarterly indexes contain duplicate SC 13D accession rows"
        )
    within_filed_date_window = sum(
        capacity.COLLECTION_START <= str(row["filed_on"]) <= capacity.COLLECTION_END
        for row in rows
    )
    private: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "captured_at": captured_at or datetime.now(UTC).isoformat(),
        "quarter_sources": [
            {
                key: quarter[key]
                for key in (
                    "year",
                    "quarter",
                    "request_sha256",
                    "source_origin",
                    "source_bytes",
                    "source_sha256",
                    "all_index_rows",
                )
            }
            for quarter in quarter_results
        ],
        "all_index_rows": sum(int(row["all_index_rows"]) for row in quarter_results),
        "indexed_initial_sc13d_rows": rows,
        "indexed_initial_sc13d_count": len(rows),
        "filed_date_window_provisional_count": within_filed_date_window,
        "acceptance_time_classified_count": 0,
        "terminal_reason_count": 0,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["private_index_sha256"] = capacity.successor._self_hash(
        private, "private_index_sha256"
    )
    return private


def collect_indexes(graph_path_value: Path) -> dict[str, Any]:
    """Collect exactly the published quarterly indexes, never market outcomes."""

    graph, graph_inspection = _load_graph_for_collection(graph_path_value)
    publication = {
        "collector": _published_source(Path(__file__).resolve()),
        "request_graph": _published_source(graph_path_value),
        "request_graph_inspection": _published_source(GRAPH_STATUS_PATH),
    }
    store = _store_config()
    sec_config = _sec_config(store.root)
    if hashlib.sha256(sec_config.user_agent.encode()).hexdigest() != graph[
        "request_contract"
    ]["user_agent_sha256"]:
        raise Schedule13dIndexCollectionError(
            "SEC private user-agent identity differs from the frozen graph"
        )
    client = SecClient(sec_config)

    def collect(request: Mapping[str, Any]) -> dict[str, Any]:
        return _collect_one(client, store.root, request)

    quarter_results: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for outcome in ordered_bounded_results(
        list(graph["request_contract"]["requests"]),
        collect,
        max_workers=WORKERS,
    ):
        request = outcome.item
        if outcome.error is not None:
            failures.append(
                {
                    "year": request["year"],
                    "quarter": request["quarter"],
                    "request_sha256": request["request_sha256"],
                    "error_type": type(outcome.error).__name__,
                    "error": str(outcome.error),
                }
            )
        elif isinstance(outcome.value, dict):
            quarter_results.append(outcome.value)
    quarter_results.sort(key=lambda row: (int(row["year"]), int(row["quarter"])))
    if failures or len(quarter_results) != 16:
        raise Schedule13dIndexCollectionError(
            f"quarterly-index collection incomplete: {len(failures)} failures"
        )
    private = _build_private_index(graph=graph, quarter_results=quarter_results)
    private_path = _private_index_path(store.root)
    _write_gzip_json(private, private_path)
    if _read_gzip_object(private_path) != private:
        raise Schedule13dIndexCollectionError("private index did not round trip")
    telemetry = client.stats()
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "outcome-blind-sec-quarterly-index-collection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": DATASET_ID,
        "contract_sha256": CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "publication": publication,
        "quarter_request_count": 16,
        "quarter_success_count": len(quarter_results),
        "quarter_failure_count": 0,
        "quarter_sources": [
            {
                key: quarter[key]
                for key in (
                    "year",
                    "quarter",
                    "request_sha256",
                    "source_origin",
                    "source_bytes",
                    "source_sha256",
                    "all_index_rows",
                )
            }
            for quarter in quarter_results
        ],
        "all_index_rows": private["all_index_rows"],
        "indexed_initial_sc13d_count": private["indexed_initial_sc13d_count"],
        "filed_date_window_provisional_count": private[
            "filed_date_window_provisional_count"
        ],
        "private_index_sha256": private["private_index_sha256"],
        "private_index_file_sha256": sha256_file(private_path),
        "acceptance_time_classified_count": 0,
        "terminal_reason_count": 0,
        "provider_telemetry": telemetry,
        "accession_document_access_permitted": False,
        "capacity_classification_complete": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["collection_sha256"] = capacity.successor._self_hash(
        result, "collection_sha256"
    )
    _write_json(result, COLLECTION_STATUS_PATH)
    return result


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dIndexCollectionError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze_request_graph()
            result: dict[str, Any] = {
                "written": _repo_relative(path),
                "request_graph_sha256": value["request_graph_sha256"],
                "request_count": value["request_contract"]["request_count"],
                "filing_count": None,
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        elif args.command == "collect":
            path = _one(
                "strategy_tournament/v2/schedule13d/indexes/manifests/"
                f"{capacity.CANDIDATE_ID}-*.json",
                "quarterly-index request graph",
            )
            result = collect_indexes(path)
        else:
            result = _read_object(COLLECTION_STATUS_PATH)
    except (
        Schedule13dIndexCollectionError,
        HistoricalDiscoveryError,
        OSError,
        ValueError,
        subprocess.CalledProcessError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
