"""Collect the frozen ASR SEC full-text-search denominator without outcomes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

import asr_capacity as capacity
from historical_discovery import SecConfig
from historical_store import DEFAULT_MIN_FREE_BYTES, HistoricalStoreConfig


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CONTRACT_SHA256 = (
    "afb19cf7e78db4afb008cbbce5c7ad462911a113efc1c2d36a4dfa80252f6cb7"
)
CONTRACT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/asr/manifests/"
    f"{capacity.CANDIDATE_ID}-{CONTRACT_SHA256}.json"
)
STATUS_PATH = PROJECT_ROOT / "strategy_tournament/v2/asr/search/collection-status.json"
ENV_PATH = PROJECT_ROOT / ".env"
PRIVATE_NAMESPACE = f"_sources/sec/asr-capacity/{CONTRACT_SHA256}"
MINIMUM_SPACING_SECONDS = 1 / 6
TIMEOUT_SECONDS = 30.0
UNRETAINED_SCHEMA_PROBE_REQUESTS = 1


class AsrCapacityCollectionError(RuntimeError):
    """The frozen ASR source scope is unpublished, incomplete, or drifted."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _store() -> HistoricalStoreConfig:
    config = HistoricalStoreConfig.from_env(ENV_PATH)
    minimum = max(DEFAULT_MIN_FREE_BYTES, config.min_free_bytes)
    if (
        config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
        or shutil.disk_usage(config.root).free < minimum
    ):
        raise AsrCapacityCollectionError(
            "private historical storage is unsafe or lacks reserve"
        )
    return config


def _load_authority() -> tuple[dict[str, Any], dict[str, Any]]:
    contract = capacity.load_contract(CONTRACT_PATH)
    inspection = capacity._read_object(capacity.DEFAULT_STATUS)
    if not (
        contract == capacity.build_contract()
        and contract.get("contract_sha256") == CONTRACT_SHA256
        and inspection.get("contract_sha256") == CONTRACT_SHA256
        and inspection.get("inspection_sha256")
        == capacity._self_hash(inspection, "inspection_sha256")
        and inspection.get("status") == "CAPACITY_CONTRACT_INSPECTED"
        and inspection.get("sec_source_access_permitted") is True
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("outcome_access_permitted") is False
        and inspection.get("broker_actions_permitted") is False
        and inspection.get("valid") is True
    ):
        raise AsrCapacityCollectionError("ASR capacity contract is not inspected")
    return contract, inspection


def _published(path: Path) -> dict[str, str]:
    relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
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
        raise AsrCapacityCollectionError(
            "SEC access requires committed inputs and pushed HEAD"
        )
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    current = path.read_bytes()
    if committed != current:
        raise AsrCapacityCollectionError("committed collector bytes differ")
    return {
        "path": relative,
        "commit": head,
        "sha256": hashlib.sha256(current).hexdigest(),
    }


def _query_params(phrase: str, offset: int) -> dict[str, Any]:
    return {
        "q": phrase,
        "dateRange": "custom",
        "startdt": capacity.COLLECTION_START,
        "enddt": capacity.COLLECTION_END,
        "from": offset,
        "size": 100,
    }


def _request_url(phrase: str, offset: int) -> str:
    return "https://efts.sec.gov/LATEST/search-index?" + urlencode(
        _query_params(phrase, offset)
    )


def _cache_path(root: Path, phrase: str, offset: int) -> Path:
    key = _sha256(_query_params(phrase, offset))
    return root / PRIVATE_NAMESPACE / "search" / f"{key}.json"


def _get_json(
    session: requests.Session,
    root: Path,
    phrase: str,
    offset: int,
    *,
    pacing: Callable[[], None],
    telemetry: dict[str, Any],
) -> tuple[dict[str, Any], Path, str]:
    path = _cache_path(root, phrase, offset)
    if path.exists():
        telemetry["cache_hits"] += 1
        raw = path.read_bytes()
        origin = "CACHE"
    else:
        pacing()
        started = time.monotonic()
        response = session.get(_request_url(phrase, offset), timeout=TIMEOUT_SECONDS)
        telemetry["request_seconds"] += time.monotonic() - started
        telemetry["requests"] += 1
        response.raise_for_status()
        raw = response.content
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        try:
            temporary.write_bytes(raw)
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)
        origin = "SEC_DOWNLOAD"
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise AsrCapacityCollectionError("EFTS returned invalid JSON") from exc
    if not isinstance(value, dict):
        raise AsrCapacityCollectionError("EFTS response is not an object")
    return value, path, origin


def _total(response: Mapping[str, Any]) -> tuple[int, str]:
    total = response.get("hits", {}).get("total", {})
    value = total.get("value") if isinstance(total, Mapping) else None
    relation = total.get("relation") if isinstance(total, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AsrCapacityCollectionError("EFTS total is invalid")
    if relation not in {"eq", "gte"}:
        raise AsrCapacityCollectionError("EFTS total relation is invalid")
    return value, str(relation)


def _page_summary(
    phrase: str,
    offset: int,
    response: Mapping[str, Any],
    path: Path,
    origin: str,
    cache_root: Path,
) -> dict[str, Any]:
    value, relation = _total(response)
    hits = response.get("hits", {}).get("hits", [])
    if not isinstance(hits, list):
        raise AsrCapacityCollectionError("EFTS hits are invalid")
    return {
        "phrase_sha256": hashlib.sha256(phrase.encode()).hexdigest(),
        "offset": offset,
        "reported_total": value,
        "total_relation": relation,
        "returned_hits": len(hits),
        "raw_bytes": path.stat().st_size,
        "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "source_origin": origin,
        "cache_relative_path": str(path.relative_to(cache_root)),
    }


def collect_search_denominator(
    *,
    status_path: Path = STATUS_PATH,
    require_published: bool = True,
    session: requests.Session | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    contract, inspection = _load_authority()
    publication = (
        {
            "collector": _published(Path(__file__).resolve()),
            "contract": _published(CONTRACT_PATH),
            "contract_inspection": _published(capacity.DEFAULT_STATUS),
        }
        if require_published
        else {}
    )
    root = store_root or _store().root
    config = SecConfig.from_env(ENV_PATH, root / PRIVATE_NAMESPACE, workers=1)
    client = session or requests.Session()
    client.headers.update(
        {"User-Agent": config.user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "unretained_schema_probe_requests": UNRETAINED_SCHEMA_PROBE_REQUESTS,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }
    next_at = 0.0

    def pace() -> None:
        nonlocal next_at
        now = time.monotonic()
        delay = max(0.0, next_at - now)
        if delay:
            time.sleep(delay)
            telemetry["pacing_wait_seconds"] += delay
        next_at = max(now, next_at) + MINIMUM_SPACING_SECONDS

    first_pages: list[tuple[str, dict[str, Any], Path, str]] = []
    summaries: list[dict[str, Any]] = []
    for phrase in contract["source_contract"]["search_phrases"]:
        response, path, origin = _get_json(
            client, root, phrase, 0, pacing=pace, telemetry=telemetry
        )
        first_pages.append((phrase, response, path, origin))
        summaries.append(_page_summary(phrase, 0, response, path, origin, root))
    inexact = [item for item in summaries if item["total_relation"] != "eq"]
    if inexact:
        state = "BLOCKED_INEXACT_EFTS_DENOMINATOR"
        filing_hit_count: int | None = None
        unique_hit_count: int | None = None
        paginated = False
    else:
        all_ids: set[str] = set()
        for phrase, first, first_path, first_origin in first_pages:
            del first_path, first_origin
            total, _ = _total(first)
            responses = [first]
            for offset in range(100, total, 100):
                response, path, origin = _get_json(
                    client, root, phrase, offset, pacing=pace, telemetry=telemetry
                )
                if _total(response) != (total, "eq"):
                    raise AsrCapacityCollectionError("EFTS total changed during pagination")
                responses.append(response)
                summaries.append(
                    _page_summary(phrase, offset, response, path, origin, root)
                )
            phrase_ids = [
                str(hit.get("_id"))
                for response in responses
                for hit in response.get("hits", {}).get("hits", [])
            ]
            if len(phrase_ids) != total or len(set(phrase_ids)) != total:
                raise AsrCapacityCollectionError(
                    "EFTS phrase pagination is incomplete or duplicated"
                )
            all_ids.update(phrase_ids)
        state = "SEARCH_DENOMINATOR_COMPLETE"
        filing_hit_count = sum(item["reported_total"] for item in summaries if item["offset"] == 0)
        unique_hit_count = len(all_ids)
        paginated = True
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "outcome-blind-asr-efts-search-denominator",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": CONTRACT_SHA256,
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "state": state,
        "publication": publication,
        "query_count": len(first_pages),
        "page_summaries": summaries,
        "all_reported_totals_exact": not inexact,
        "pagination_complete": paginated,
        "filing_hit_count": filing_hit_count,
        "unique_hit_count": unique_hit_count,
        "matched_document_access_performed": False,
        "capacity_classification_complete": False,
        "verified_event_count": None,
        "provider_telemetry": telemetry,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["collection_sha256"] = capacity._self_hash(result, "collection_sha256")
    _write_object(result, status_path)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("collect", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = (
            collect_search_denominator()
            if args.command == "collect"
            else capacity._read_object(STATUS_PATH)
        )
    except (
        AsrCapacityCollectionError,
        capacity.AsrCapacityError,
        OSError,
        requests.RequestException,
        subprocess.CalledProcessError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
