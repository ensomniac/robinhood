"""Collect the ASR v2 recursive exact-total EFTS denominator without outcomes."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests

import asr_capacity_recovery as recovery
from historical_discovery import SecConfig
from historical_store import DEFAULT_MIN_FREE_BYTES, HistoricalStoreConfig


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
STATUS_PATH = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/recovery/search/collection-status.json"
)
ENV_PATH = PROJECT_ROOT / ".env"
MINIMUM_SPACING_SECONDS = 1 / 6
TIMEOUT_SECONDS = 30.0


class AsrCapacityRecoveryCollectionError(RuntimeError):
    """The ASR recovery denominator is unpublished, incomplete, or drifted."""


def _store() -> HistoricalStoreConfig:
    config = HistoricalStoreConfig.from_env(ENV_PATH)
    minimum = max(DEFAULT_MIN_FREE_BYTES, config.min_free_bytes)
    if (
        config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
        or shutil.disk_usage(config.root).free < minimum
    ):
        raise AsrCapacityRecoveryCollectionError(
            "private historical storage is unsafe or lacks reserve"
        )
    return config


def _contract_path() -> Path:
    matches = sorted(recovery.DEFAULT_ROOT.glob(f"{recovery.CANDIDATE_ID}-*.json"))
    if len(matches) != 1:
        raise AsrCapacityRecoveryCollectionError(
            "expected exactly one frozen ASR recovery contract"
        )
    return matches[0]


def _load_authority() -> tuple[Path, dict[str, Any], dict[str, Any]]:
    path = _contract_path()
    contract = recovery.load_contract(path)
    inspection = recovery.read_object(recovery.DEFAULT_STATUS)
    if not (
        contract == recovery.build_contract()
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("inspection_sha256")
        == recovery.self_hash(inspection, "inspection_sha256")
        and inspection.get("status") == "RECOVERY_CONTRACT_INSPECTED"
        and inspection.get("sec_search_access_permitted") is True
        and inspection.get("matched_document_access_permitted") is False
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("outcome_access_permitted") is False
        and inspection.get("broker_actions_permitted") is False
        and inspection.get("valid") is True
    ):
        raise AsrCapacityRecoveryCollectionError(
            "ASR recovery contract is not independently inspected"
        )
    return path, contract, inspection


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
        raise AsrCapacityRecoveryCollectionError(
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
        raise AsrCapacityRecoveryCollectionError("committed collector bytes differ")
    return {
        "path": relative,
        "commit": head,
        "sha256": hashlib.sha256(current).hexdigest(),
    }


def _parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise AsrCapacityRecoveryCollectionError(
            f"invalid collection date {value}"
        ) from exc


def _split_window(start: str, end: str) -> tuple[tuple[str, str], tuple[str, str]]:
    first = _parse_date(start)
    last = _parse_date(end)
    if first >= last:
        raise AsrCapacityRecoveryCollectionError(
            "an inexact single-date window cannot be split"
        )
    midpoint = first + timedelta(days=(last - first).days // 2)
    newer_start = midpoint + timedelta(days=1)
    return (
        (first.isoformat(), midpoint.isoformat()),
        (newer_start.isoformat(), last.isoformat()),
    )


def _query_params(
    phrase: str, start: str, end: str, offset: int, page_size: int
) -> dict[str, Any]:
    return {
        "q": phrase,
        "dateRange": "custom",
        "startdt": start,
        "enddt": end,
        "from": offset,
        "size": page_size,
    }


def _request_url(
    phrase: str, start: str, end: str, offset: int, page_size: int
) -> str:
    return "https://efts.sec.gov/LATEST/search-index?" + urlencode(
        _query_params(phrase, start, end, offset, page_size)
    )


def _namespace(contract_sha256: str) -> str:
    return f"_sources/sec/asr-capacity-recovery/{contract_sha256}"


def _cache_path(
    root: Path,
    contract_sha256: str,
    phrase: str,
    start: str,
    end: str,
    offset: int,
    page_size: int,
) -> Path:
    key = hashlib.sha256(
        recovery.canonical_bytes(
            _query_params(phrase, start, end, offset, page_size)
        )
    ).hexdigest()
    return root / _namespace(contract_sha256) / "search" / f"{key}.json"


def _get_json(
    session: requests.Session,
    root: Path,
    contract_sha256: str,
    phrase: str,
    start: str,
    end: str,
    offset: int,
    page_size: int,
    *,
    pacing: Callable[[], None],
    telemetry: dict[str, Any],
) -> tuple[dict[str, Any], Path, str]:
    path = _cache_path(
        root, contract_sha256, phrase, start, end, offset, page_size
    )
    if path.exists():
        telemetry["cache_hits"] += 1
        raw = path.read_bytes()
        origin = "CACHE"
    else:
        pacing()
        started = time.monotonic()
        try:
            response = session.get(
                _request_url(phrase, start, end, offset, page_size),
                timeout=TIMEOUT_SECONDS,
            )
            telemetry["request_seconds"] += time.monotonic() - started
            telemetry["requests"] += 1
            response.raise_for_status()
            raw = response.content
        except requests.RequestException:
            telemetry["failures"] += 1
            raise
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
        raise AsrCapacityRecoveryCollectionError(
            "EFTS returned invalid JSON"
        ) from exc
    if not isinstance(value, dict):
        raise AsrCapacityRecoveryCollectionError(
            "EFTS response must contain an object"
        )
    return value, path, origin


def _total(response: Mapping[str, Any]) -> tuple[int, str]:
    total = response.get("hits", {}).get("total", {})
    value = total.get("value") if isinstance(total, Mapping) else None
    relation = total.get("relation") if isinstance(total, Mapping) else None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise AsrCapacityRecoveryCollectionError("EFTS total is invalid")
    if relation not in {"eq", "gte"}:
        raise AsrCapacityRecoveryCollectionError(
            "EFTS total relation is invalid"
        )
    return value, str(relation)


def _hits(response: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = response.get("hits", {}).get("hits", [])
    if not isinstance(raw, list) or any(not isinstance(item, dict) for item in raw):
        raise AsrCapacityRecoveryCollectionError("EFTS hits are invalid")
    result = [dict(item) for item in raw]
    for item in result:
        identifier = item.get("_id")
        if not isinstance(identifier, str) or not identifier:
            raise AsrCapacityRecoveryCollectionError(
                "every EFTS hit requires a nonempty _id"
            )
    return result


def _page_summary(
    phrase: str,
    start: str,
    end: str,
    offset: int,
    depth: int,
    response: Mapping[str, Any],
    path: Path,
    origin: str,
    cache_root: Path,
) -> dict[str, Any]:
    total, relation = _total(response)
    return {
        "phrase_sha256": hashlib.sha256(phrase.encode()).hexdigest(),
        "window_start": start,
        "window_end": end,
        "split_depth": depth,
        "offset": offset,
        "reported_total": total,
        "total_relation": relation,
        "returned_hits": len(_hits(response)),
        "raw_bytes": path.stat().st_size,
        "raw_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "source_origin": origin,
        "cache_relative_path": str(path.relative_to(cache_root)),
    }


def _write_private_hit_index(
    root: Path,
    contract_sha256: str,
    hits_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[Path, str, int]:
    payload = recovery.canonical_bytes(
        {
            "schema_version": 1,
            "contract_sha256": contract_sha256,
            "hits": [hits_by_id[key] for key in sorted(hits_by_id)],
        }
    )
    compressed = gzip.compress(payload, mtime=0)
    digest = hashlib.sha256(compressed).hexdigest()
    path = (
        root / _namespace(contract_sha256) / "hit-index" / f"{digest}.json.gz"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() != compressed:
        raise AsrCapacityRecoveryCollectionError(
            "content-addressed private hit index differs"
        )
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(compressed)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return path, digest, len(compressed)


def collect_search_denominator(
    *,
    status_path: Path = STATUS_PATH,
    require_published: bool = True,
    session: requests.Session | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    contract_path, contract, inspection = _load_authority()
    publication = (
        {
            "collector": _published(Path(__file__).resolve()),
            "contract": _published(contract_path),
            "contract_inspection": _published(recovery.DEFAULT_STATUS),
        }
        if require_published
        else {}
    )
    root = store_root or _store().root
    contract_sha256 = str(contract["contract_sha256"])
    config = SecConfig.from_env(
        ENV_PATH, root / _namespace(contract_sha256), workers=1
    )
    client = session or requests.Session()
    client.headers.update(
        {"User-Agent": config.user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
        "prior_unretained_schema_probe_requests": 1,
        "prior_retained_v1_requests": 4,
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

    summaries: list[dict[str, Any]] = []
    retained_by_id: dict[str, dict[str, Any]] = {}
    phrase_hit_counts: dict[str, int] = {}
    exact_leaf_windows = 0
    inexact_parent_windows = 0
    maximum_observed_depth = 0
    blocked_single_date: dict[str, Any] | None = None
    page_size = int(contract["source_contract"]["page_size"])
    maximum_depth = int(
        contract["source_contract"]["window_algorithm"]["maximum_split_depth"]
    )

    def request_page(
        phrase: str, start: str, end: str, offset: int, depth: int
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        response, raw_path, origin = _get_json(
            client,
            root,
            contract_sha256,
            phrase,
            start,
            end,
            offset,
            page_size,
            pacing=pace,
            telemetry=telemetry,
        )
        summaries.append(
            _page_summary(
                phrase,
                start,
                end,
                offset,
                depth,
                response,
                raw_path,
                origin,
                root,
            )
        )
        return response, _hits(response)

    def collect_window(
        phrase: str, start: str, end: str, depth: int
    ) -> list[dict[str, Any]]:
        nonlocal exact_leaf_windows
        nonlocal inexact_parent_windows
        nonlocal maximum_observed_depth
        nonlocal blocked_single_date
        maximum_observed_depth = max(maximum_observed_depth, depth)
        if depth > maximum_depth:
            raise AsrCapacityRecoveryCollectionError(
                "recursive EFTS split depth exceeded its frozen maximum"
            )
        first, first_hits = request_page(phrase, start, end, 0, depth)
        total, relation = _total(first)
        if relation == "gte":
            inexact_parent_windows += 1
            if start == end:
                blocked_single_date = {
                    "phrase_sha256": hashlib.sha256(phrase.encode()).hexdigest(),
                    "date": start,
                    "reported_lower_bound": total,
                    "split_depth": depth,
                }
                return []
            older, newer = _split_window(start, end)
            return collect_window(phrase, *older, depth + 1) + collect_window(
                phrase, *newer, depth + 1
            )
        exact_leaf_windows += 1
        pages = list(first_hits)
        for offset in range(page_size, total, page_size):
            response, hits = request_page(phrase, start, end, offset, depth)
            if _total(response) != (total, "eq"):
                raise AsrCapacityRecoveryCollectionError(
                    "EFTS exact leaf total changed during pagination"
                )
            pages.extend(hits)
        identifiers = [str(item["_id"]) for item in pages]
        if len(identifiers) != total or len(set(identifiers)) != total:
            raise AsrCapacityRecoveryCollectionError(
                "EFTS exact leaf pagination is incomplete or duplicated"
            )
        return pages

    for phrase in contract["source_contract"]["search_phrases"]:
        phrase_hits = collect_window(
            str(phrase), recovery.COLLECTION_START, recovery.COLLECTION_END, 0
        )
        if blocked_single_date is not None:
            break
        phrase_hit_counts[hashlib.sha256(str(phrase).encode()).hexdigest()] = len(
            phrase_hits
        )
        for hit in phrase_hits:
            identifier = str(hit["_id"])
            prior = retained_by_id.get(identifier)
            if prior is not None and prior != hit:
                raise AsrCapacityRecoveryCollectionError(
                    "duplicate EFTS hit identity has conflicting source fields"
                )
            retained_by_id[identifier] = hit

    if blocked_single_date is not None:
        state = "BLOCKED_INEXACT_SINGLE_DATE"
        complete = False
        filing_hit_count: int | None = None
        unique_hit_count: int | None = None
        private_index: dict[str, Any] | None = None
    else:
        state = "SEARCH_DENOMINATOR_COMPLETE"
        complete = True
        filing_hit_count = sum(phrase_hit_counts.values())
        unique_hit_count = len(retained_by_id)
        index_path, index_sha256, index_bytes = _write_private_hit_index(
            root, contract_sha256, retained_by_id
        )
        private_index = {
            "cache_relative_path": str(index_path.relative_to(root)),
            "sha256": index_sha256,
            "bytes": index_bytes,
            "unique_hit_count": unique_hit_count,
        }
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "outcome-blind-asr-recursive-efts-denominator",
        "campaign_id": recovery.CAMPAIGN_ID,
        "candidate_id": recovery.CANDIDATE_ID,
        "contract_sha256": contract_sha256,
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "state": state,
        "publication": publication,
        "page_summaries": summaries,
        "exact_leaf_window_count": exact_leaf_windows,
        "inexact_parent_window_count": inexact_parent_windows,
        "maximum_observed_split_depth": maximum_observed_depth,
        "blocked_single_date": blocked_single_date,
        "phrase_hit_counts_by_hash": phrase_hit_counts,
        "all_leaf_totals_exact": complete,
        "pagination_complete": complete,
        "filing_hit_count": filing_hit_count,
        "unique_hit_count": unique_hit_count,
        "private_hit_index": private_index,
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
    result["collection_sha256"] = recovery.self_hash(
        result, "collection_sha256"
    )
    recovery.write_object(result, status_path)
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
            else recovery.read_object(STATUS_PATH)
        )
    except (
        AsrCapacityRecoveryCollectionError,
        recovery.AsrCapacityRecoveryError,
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
