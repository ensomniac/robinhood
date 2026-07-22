"""Freeze and collect required historical SEC submissions files."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_semantic_capacity as semantic
import schedule13d_symbol_submissions as submissions
from historical_concurrency import ordered_bounded_results
from historical_discovery import (
    HistoricalDiscoveryError,
    SecClient,
    SecConfig,
    _submission_recent,
)
from historical_store import DEFAULT_MIN_FREE_BYTES, HistoricalStoreConfig, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
MAIN_INSPECTION_SHA256 = (
    "ced65dcd2ce7a536a55d3189ea50c58093af4ca3a5244b9a56b1994cbebce518"
)
MAIN_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/submissions/inspections/"
    f"{capacity.CANDIDATE_ID}-submissions-{MAIN_INSPECTION_SHA256}.json"
)
GRAPH_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/supplemental/manifests"
)
GRAPH_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/supplemental/request-status.json"
)
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/supplemental/collection-status.json"
)
COLLECTION_INSPECTION_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/supplemental/inspections"
)
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_symbol_supplemental_inspection.py"
ENV_PATH = PROJECT_ROOT / ".env"
SHARED_CACHE_NAMESPACE = "_sources/sec/schedule13d-issuer-submissions-v1"
PRIVATE_NAMESPACE = (
    f"_derived/schedule13d_capacity/{semantic.documents.DATASET_ID}/symbols/supplemental"
)
PRIVATE_COLLECTION_NAME = "historical-submissions-collection.json"
WORKERS = 3
MINIMUM_SPACING_SECONDS = 0.15
TIMEOUT_SECONDS = 30.0


class Schedule13dSymbolSupplementalError(RuntimeError):
    """The historical submissions graph or collection is invalid."""


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
        raise Schedule13dSymbolSupplementalError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dSymbolSupplementalError(f"{path} must contain an object")
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


def _store_config() -> HistoricalStoreConfig:
    config = HistoricalStoreConfig.from_env(ENV_PATH)
    minimum = max(DEFAULT_MIN_FREE_BYTES, config.min_free_bytes)
    if (
        config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
        or shutil.disk_usage(config.root).free < minimum
    ):
        raise Schedule13dSymbolSupplementalError(
            "private historical storage is unsafe or lacks its frozen reserve"
        )
    return config


def _sec_config(store_root: Path) -> SecConfig:
    value = SecConfig.from_env(
        ENV_PATH, store_root / SHARED_CACHE_NAMESPACE, workers=WORKERS
    )
    if not (
        value.minimum_spacing_seconds == MINIMUM_SPACING_SECONDS
        and value.timeout_seconds == TIMEOUT_SECONDS
    ):
        raise Schedule13dSymbolSupplementalError("SEC client pacing contract drifted")
    return value


def _private_collection_path(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / PRIVATE_COLLECTION_NAME


def _load_lineage() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    main = _read_object(submissions.COLLECTION_STATUS_PATH)
    inspection = _read_object(MAIN_INSPECTION_PATH)
    state = semantic._read_gzip_object(semantic._private_result_path())
    if not (
        main.get("collection_sha256")
        == capacity.successor._self_hash(main, "collection_sha256")
        and main.get("success_count") == 312
        and main.get("failure_count") == 0
        and main.get("valid") is True
        and inspection.get("inspection_sha256") == MAIN_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == capacity.successor._self_hash(inspection, "inspection_sha256")
        and inspection.get("collection_sha256") == main["collection_sha256"]
        and inspection.get("supplemental_submission_request_freeze_permitted")
        is True
        and inspection.get("supplemental_submission_file_access_permitted") is False
        and inspection.get("valid") is True
        and state.get("pending_causal_symbol_supplemental") == 317
        and state.get("market_outcomes_accessed") is False
    ):
        raise Schedule13dSymbolSupplementalError(
            "inspected main submissions lineage is invalid"
        )
    return main, inspection, state


def _main_graph() -> dict[str, Any]:
    matches = sorted(
        PROJECT_ROOT.glob(
            "strategy_tournament/v2/schedule13d/symbols/submissions/manifests/"
            f"{capacity.CANDIDATE_ID}-*.json"
        )
    )
    if len(matches) != 1:
        raise Schedule13dSymbolSupplementalError(
            f"expected one main submissions graph; found {len(matches)}"
        )
    return submissions.load_request_graph(matches[0])


def _event_map(state: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in state["records"]:
        if row.get("status") == "PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL":
            events[str(row["subject_cik"])].append(dict(row))
    if sum(len(rows) for rows in events.values()) != 317:
        raise Schedule13dSymbolSupplementalError("pending event map drifted")
    return events


def _has_prior_main(payload: Mapping[str, Any], accepted_at: str) -> bool:
    allowed = set(submissions.ALLOWED_FORMS)
    return any(
        row.get("form") in allowed
        and str(row.get("acceptanceDateTime") or "") <= accepted_at
        for row in _submission_recent(payload).values()
    )


def _request(
    *, cik: str, descriptor: Mapping[str, Any], ordinal: int, event_count: int
) -> dict[str, Any]:
    name = str(descriptor.get("name") or "").strip()
    if not name or Path(name).name != name or not name.endswith(".json"):
        raise Schedule13dSymbolSupplementalError(
            f"unsafe supplemental descriptor: {name}"
        )
    value = {
        "ordinal": ordinal,
        "subject_cik": cik,
        "event_count": event_count,
        "name": name,
        "filing_from": descriptor.get("filingFrom"),
        "filing_to": descriptor.get("filingTo"),
        "url": f"https://data.sec.gov/submissions/{name}",
        "shared_cache_relative_path": f"submissions/files/{name}",
    }
    value["request_sha256"] = _sha256_json(value)
    return value


def build_request_graph() -> dict[str, Any]:
    """Derive exact older-metadata requests from inspected main submissions."""

    main, inspection, state = _load_lineage()
    graph = _main_graph()
    store = _store_config()
    events = _event_map(state)
    descriptors: dict[tuple[str, str], dict[str, Any]] = {}
    main_covered = 0
    missing_without_descriptor = 0
    for request in graph["request_contract"]["requests"]:
        cik = str(request["subject_cik"])
        payload = json.loads(
            submissions._resolve_cache_path(store.root, request).read_text(
                encoding="utf-8"
            )
        )
        missing_events = [
            event
            for event in events[cik]
            if not _has_prior_main(payload, str(event["accepted_at"]))
        ]
        main_covered += len(events[cik]) - len(missing_events)
        if not missing_events:
            continue
        values = payload.get("filings", {}).get("files", [])
        valid_descriptors = [value for value in values if isinstance(value, Mapping)]
        if not valid_descriptors:
            missing_without_descriptor += len(missing_events)
            continue
        for descriptor in valid_descriptors:
            name = str(descriptor.get("name") or "")
            descriptors[(cik, name)] = _request(
                cik=cik,
                descriptor=descriptor,
                ordinal=0,
                event_count=len(missing_events),
            )
    requests = []
    for ordinal, key in enumerate(
        sorted(descriptors, key=lambda value: (int(value[0]), value[1]))
    ):
        request = dict(descriptors[key])
        request["ordinal"] = ordinal
        request["request_sha256"] = _sha256_json(
            {name: value for name, value in request.items() if name != "request_sha256"}
        )
        requests.append(request)
    if not (
        main_covered == 279
        and missing_without_descriptor == 36
        and len(requests) == 3
        and len({row["subject_cik"] for row in requests}) == 2
    ):
        raise Schedule13dSymbolSupplementalError(
            "historical metadata request denominator differs"
        )
    sec = _sec_config(store.root)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-sec-historical-submissions-request-graph",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "dataset_id": semantic.documents.DATASET_ID,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "main_collection_sha256": main["collection_sha256"],
        "main_inspection_sha256": inspection["inspection_sha256"],
        "request_contract": {
            "pending_event_count": 317,
            "events_with_prior_in_main_metadata": main_covered,
            "events_without_prior_and_without_historical_descriptor": (
                missing_without_descriptor
            ),
            "events_requiring_historical_metadata": 2,
            "request_count": len(requests),
            "requests": requests,
            "workers": WORKERS,
            "global_minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "maximum_requests_per_second": 1 / MINIMUM_SPACING_SECONDS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "maximum_attempts": 4,
            "provider_substitution_permitted": False,
            "user_agent_sha256": hashlib.sha256(sec.user_agent.encode()).hexdigest(),
        },
        "selection_contract": {
            "all_descriptors_for_a_subject_with_an_uncovered_event_are_frozen": True,
            "allowed_forms": list(submissions.ALLOWED_FORMS),
            "latest_prior_selection_deferred_until_all_frozen_metadata_inspected": True,
            "current_or_future_symbol_mapping_permitted": False,
        },
        "access_contract": {
            "provider_access_before_graph_inspection_permitted": False,
            "provider_access_after_graph_inspection_permitted": True,
            "issuer_primary_document_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_computation_permitted": False,
            "stage0_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            str(path.relative_to(PROJECT_ROOT)): sha256_file(path)
            for path in (Path(__file__).resolve(), INSPECTOR_PATH)
        },
        "verified_event_count": 1,
        "capacity_passed": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
    }
    value["request_graph_sha256"] = capacity.successor._self_hash(
        value, "request_graph_sha256"
    )
    return value


def graph_path(value: Mapping[str, Any], root: Path = GRAPH_ROOT) -> Path:
    return root / f"{capacity.CANDIDATE_ID}-{value['request_graph_sha256']}.json"


def load_request_graph(path: Path) -> dict[str, Any]:
    value = _read_object(path)
    digest = value.get("request_graph_sha256")
    if not (
        isinstance(digest, str)
        and digest == capacity.successor._self_hash(value, "request_graph_sha256")
        and path.name == f"{capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise Schedule13dSymbolSupplementalError(
            "historical submissions graph was mutated or renamed"
        )
    return value


def freeze_request_graph(
    *, root: Path = GRAPH_ROOT, status_path: Path = GRAPH_STATUS_PATH
) -> tuple[Path, dict[str, Any]]:
    value = build_request_graph()
    path = graph_path(value, root)
    if path.exists() and _read_object(path) != value:
        raise Schedule13dSymbolSupplementalError(
            "content-addressed supplemental graph has other content"
        )
    _write_json(value, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": capacity.CANDIDATE_ID,
            "request_graph_sha256": value["request_graph_sha256"],
            "request_count": 3,
            "events_requiring_historical_metadata": 2,
            "status": "SYMBOL_SUPPLEMENTAL_GRAPH_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "issuer_primary_document_access_permitted": False,
            "capacity_passed": None,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, value


def _repo_relative(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT))


def _published_source(path: Path) -> None:
    relative = _repo_relative(path)
    status = subprocess.run(
        ["git", "status", "--porcelain", "--", relative],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
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
    if status.strip() or head != upstream:
        raise Schedule13dSymbolSupplementalError(
            f"provider input is not committed and pushed: {relative}"
        )


def _resolve_cache_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise Schedule13dSymbolSupplementalError("supplemental cache path is unsafe")
    return store_root / SHARED_CACHE_NAMESPACE / relative


def _load_graph_for_collection(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    graph = load_request_graph(path)
    status = _read_object(GRAPH_STATUS_PATH)
    if not (
        graph == build_request_graph()
        and status.get("status") == "SYMBOL_SUPPLEMENTAL_GRAPH_INSPECTED"
        and status.get("request_graph_sha256") == graph["request_graph_sha256"]
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("issuer_primary_document_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("valid") is True
    ):
        raise Schedule13dSymbolSupplementalError(
            "historical submissions graph is not independently inspected"
        )
    return graph, status


def collect_supplemental(path: Path) -> dict[str, Any]:
    graph, inspection = _load_graph_for_collection(path)
    for published in (Path(__file__).resolve(), path, GRAPH_STATUS_PATH):
        _published_source(published)
    store = _store_config()
    sec_config = _sec_config(store.root)
    if hashlib.sha256(sec_config.user_agent.encode()).hexdigest() != graph[
        "request_contract"
    ]["user_agent_sha256"]:
        raise Schedule13dSymbolSupplementalError("SEC user-agent identity drifted")
    client = SecClient(sec_config)

    def collect(request: Mapping[str, Any]) -> dict[str, Any]:
        cache = _resolve_cache_path(store.root, request)
        existed = cache.is_file()
        payload = client.json(str(request["url"]), cache)
        raw = cache.read_bytes()
        if not any(isinstance(value, list) for value in payload.values()):
            raise Schedule13dSymbolSupplementalError(
                f"historical submissions payload is not columnar: {request['url']}"
            )
        return {
            "ordinal": request["ordinal"],
            "request_sha256": request["request_sha256"],
            "source_origin": "SHARED_CACHE" if existed else "SEC_DOWNLOAD",
            "source_bytes": len(raw),
            "source_sha256": hashlib.sha256(raw).hexdigest(),
            "status": "SUCCESS",
        }

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for outcome in ordered_bounded_results(
        list(graph["request_contract"]["requests"]), collect, max_workers=WORKERS
    ):
        if outcome.error is not None:
            failures.append(
                {
                    "ordinal": outcome.item["ordinal"],
                    "request_sha256": outcome.item["request_sha256"],
                    "error_type": type(outcome.error).__name__,
                    "error": str(outcome.error),
                }
            )
        elif isinstance(outcome.value, dict):
            records.append(outcome.value)
    records.sort(key=lambda row: int(row["ordinal"]))
    private: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "request_graph_sha256": graph["request_graph_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "request_count": 3,
        "success_count": len(records),
        "failure_count": len(failures),
        "records": records,
        "failures": failures,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["private_collection_sha256"] = capacity.successor._self_hash(
        private, "private_collection_sha256"
    )
    private_path = _private_collection_path(store.root)
    _write_json(private, private_path)
    valid = not failures and len(records) == 3
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "outcome-blind-sec-historical-submissions-collection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": inspection["inspection_sha256"],
        "request_count": 3,
        "success_count": len(records),
        "failure_count": len(failures),
        "source_bytes": sum(int(row["source_bytes"]) for row in records),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "provider_telemetry": client.stats(),
        "issuer_primary_document_request_freeze_permitted": valid,
        "issuer_primary_document_access_permitted": False,
        "verified_event_count": 1,
        "capacity_passed": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": valid,
    }
    result["collection_sha256"] = capacity.successor._self_hash(
        result, "collection_sha256"
    )
    _write_json(result, COLLECTION_STATUS_PATH)
    if not valid:
        raise Schedule13dSymbolSupplementalError(
            f"historical submissions collection has {len(failures)} failures"
        )
    return result


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSymbolSupplementalError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "status"))
    args = parser.parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze_request_graph()
            result: dict[str, Any] = {
                "written": _repo_relative(path),
                "request_graph_sha256": value["request_graph_sha256"],
                "request_count": 3,
                "provider_access_permitted": False,
                "capacity_passed": None,
            }
        elif args.command == "collect":
            path = _one(
                "strategy_tournament/v2/schedule13d/symbols/supplemental/manifests/"
                f"{capacity.CANDIDATE_ID}-*.json",
                "historical submissions graph",
            )
            result = collect_supplemental(path)
        else:
            result = _read_object(COLLECTION_STATUS_PATH)
    except (
        Schedule13dSymbolSupplementalError,
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
