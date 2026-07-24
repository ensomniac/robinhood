"""Collect the ASR v3 single-page EFTS denominator without outcomes."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import asr_capacity_no_pagination as capacity
import asr_capacity_recovery_collection as shared
from historical_discovery import SecConfig


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/asr/no-pagination/search/collection-status.json"
)
ENV_PATH = PROJECT_ROOT / ".env"


class AsrCapacityNoPaginationCollectionError(RuntimeError):
    """The no-pagination denominator is unpublished, incomplete, or drifted."""


def _contract_path() -> Path:
    matches = sorted(capacity.DEFAULT_ROOT.glob(f"{capacity.CANDIDATE_ID}-*.json"))
    if len(matches) != 1:
        raise AsrCapacityNoPaginationCollectionError(
            "expected exactly one frozen no-pagination contract"
        )
    return matches[0]


def _load_authority() -> tuple[Path, dict[str, Any], dict[str, Any]]:
    path = _contract_path()
    contract = capacity.load_contract(path)
    inspection = capacity.read_object(capacity.DEFAULT_STATUS)
    if not (
        contract == capacity.build_contract()
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("inspection_sha256")
        == capacity.self_hash(inspection, "inspection_sha256")
        and inspection.get("status") == "NO_PAGINATION_CONTRACT_INSPECTED"
        and inspection.get("sec_search_access_permitted") is True
        and inspection.get("matched_document_access_permitted") is False
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("outcome_access_permitted") is False
        and inspection.get("broker_actions_permitted") is False
        and inspection.get("valid") is True
    ):
        raise AsrCapacityNoPaginationCollectionError(
            "no-pagination contract is not independently inspected"
        )
    return path, contract, inspection


def _canonical_hit(hit: Mapping[str, Any]) -> dict[str, Any]:
    identifier = hit.get("_id")
    source = hit.get("_source")
    if not isinstance(identifier, str) or not identifier:
        raise AsrCapacityNoPaginationCollectionError(
            "every EFTS hit requires a nonempty identity"
        )
    if not isinstance(source, Mapping):
        raise AsrCapacityNoPaginationCollectionError(
            "every EFTS hit requires source fields"
        )
    return {"_id": identifier, "_source": dict(source)}


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
            "collector": shared._published(Path(__file__).resolve()),
            "contract": shared._published(contract_path),
            "contract_inspection": shared._published(capacity.DEFAULT_STATUS),
        }
        if require_published
        else {}
    )
    root = store_root or shared._store().root
    contract_sha256 = str(contract["contract_sha256"])
    config = SecConfig.from_env(
        ENV_PATH, root / shared._namespace(contract_sha256), workers=1
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
        "retry_attempts": 0,
        "retry_wait_seconds": 0.0,
        "prior_unretained_schema_probe_requests": 1,
        "prior_retained_v1_requests": 4,
        "prior_v2_source_requests_excluded": True,
    }
    next_at = 0.0

    def pace() -> None:
        nonlocal next_at
        now = time.monotonic()
        delay = max(0.0, next_at - now)
        if delay:
            time.sleep(delay)
            telemetry["pacing_wait_seconds"] += delay
        next_at = max(now, next_at) + shared.MINIMUM_SPACING_SECONDS

    summaries: list[dict[str, Any]] = []
    retained_by_id: dict[str, dict[str, Any]] = {}
    phrase_hit_counts: dict[str, int] = {}
    terminal_leaf_windows = 0
    split_parent_windows = 0
    maximum_observed_depth = 0
    blocked_single_date: dict[str, Any] | None = None
    page_size = int(contract["source_contract"]["page_size"])
    maximum_depth = int(
        contract["source_contract"]["window_algorithm"]["maximum_split_depth"]
    )
    retry = contract["source_contract"]["retry_contract"]
    maximum_attempts = int(retry["maximum_attempts_per_exact_request"])
    retry_statuses = set(int(value) for value in retry["retryable_http_statuses"])
    retry_waits = [float(value) for value in retry["backoff_seconds_by_retry"]]

    def request_page(
        phrase: str, start: str, end: str, depth: int
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        last_error: requests.RequestException | None = None
        for attempt in range(maximum_attempts):
            try:
                response, raw_path, origin = shared._get_json(
                    client,
                    root,
                    contract_sha256,
                    phrase,
                    start,
                    end,
                    0,
                    page_size,
                    pacing=pace,
                    telemetry=telemetry,
                )
                summaries.append(
                    shared._page_summary(
                        phrase,
                        start,
                        end,
                        0,
                        depth,
                        response,
                        raw_path,
                        origin,
                        root,
                    )
                )
                return response, shared._hits(response)
            except requests.RequestException as exc:
                last_error = exc
                status = exc.response.status_code if exc.response is not None else None
                retryable = status is None or status in retry_statuses
                if not retryable or attempt + 1 >= maximum_attempts:
                    raise
                wait_seconds = retry_waits[attempt]
                time.sleep(wait_seconds)
                telemetry["retry_attempts"] += 1
                telemetry["retry_wait_seconds"] += wait_seconds
        if last_error is not None:
            raise last_error
        raise AsrCapacityNoPaginationCollectionError(
            "request retry loop ended without a response"
        )

    def collect_window(
        phrase: str, start: str, end: str, depth: int
    ) -> list[dict[str, Any]]:
        nonlocal terminal_leaf_windows
        nonlocal split_parent_windows
        nonlocal maximum_observed_depth
        nonlocal blocked_single_date
        maximum_observed_depth = max(maximum_observed_depth, depth)
        if depth > maximum_depth:
            raise AsrCapacityNoPaginationCollectionError(
                "recursive split depth exceeded its frozen maximum"
            )
        response, hits = request_page(phrase, start, end, depth)
        total, relation = shared._total(response)
        must_split = relation == "gte" or total > page_size
        if must_split:
            split_parent_windows += 1
            if start == end:
                blocked_single_date = {
                    "phrase_sha256": hashlib.sha256(phrase.encode()).hexdigest(),
                    "date": start,
                    "reported_total": total,
                    "total_relation": relation,
                    "split_depth": depth,
                }
                return []
            older, newer = shared._split_window(start, end)
            older_hits = collect_window(phrase, *older, depth + 1)
            if blocked_single_date is not None:
                return []
            return older_hits + collect_window(phrase, *newer, depth + 1)
        terminal_leaf_windows += 1
        if len(hits) != total:
            raise AsrCapacityNoPaginationCollectionError(
                "single-page terminal row count differs from exact total"
            )
        identifiers = [str(item["_id"]) for item in hits]
        if len(identifiers) != len(set(identifiers)):
            raise AsrCapacityNoPaginationCollectionError(
                "single-page terminal contains duplicate hit identities"
            )
        return hits

    for phrase_value in contract["source_contract"]["search_phrases"]:
        phrase = str(phrase_value)
        phrase_hits = collect_window(
            phrase, capacity.COLLECTION_START, capacity.COLLECTION_END, 0
        )
        if blocked_single_date is not None:
            break
        phrase_ids = [str(item["_id"]) for item in phrase_hits]
        if len(phrase_ids) != len(set(phrase_ids)):
            raise AsrCapacityNoPaginationCollectionError(
                "disjoint terminal windows repeated a filing identity"
            )
        phrase_hit_counts[hashlib.sha256(phrase.encode()).hexdigest()] = len(
            phrase_hits
        )
        for hit in phrase_hits:
            canonical = _canonical_hit(hit)
            identifier = str(canonical["_id"])
            prior = retained_by_id.get(identifier)
            if prior is not None and prior != canonical:
                raise AsrCapacityNoPaginationCollectionError(
                    "cross-phrase filing identity has conflicting source fields"
                )
            retained_by_id[identifier] = canonical

    if blocked_single_date is not None:
        state = "BLOCKED_SINGLE_DATE_OVER_PAGE"
        complete = False
        filing_hit_count: int | None = None
        unique_hit_count: int | None = None
        private_index: dict[str, Any] | None = None
    else:
        state = "SEARCH_DENOMINATOR_COMPLETE"
        complete = True
        filing_hit_count = sum(phrase_hit_counts.values())
        unique_hit_count = len(retained_by_id)
        index_path, index_sha256, index_bytes = shared._write_private_hit_index(
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
        "collection_kind": "outcome-blind-asr-single-page-efts-denominator",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": contract_sha256,
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "state": state,
        "publication": publication,
        "page_summaries": summaries,
        "terminal_leaf_window_count": terminal_leaf_windows,
        "split_parent_window_count": split_parent_windows,
        "maximum_observed_split_depth": maximum_observed_depth,
        "blocked_single_date": blocked_single_date,
        "phrase_hit_counts_by_hash": phrase_hit_counts,
        "all_terminal_totals_exact_and_at_most_page": complete,
        "offset_pagination_requests": 0,
        "denominator_complete": complete,
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
    result["collection_sha256"] = capacity.self_hash(
        result, "collection_sha256"
    )
    capacity.write_object(result, status_path)
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
            else capacity.read_object(STATUS_PATH)
        )
    except (
        AsrCapacityNoPaginationCollectionError,
        capacity.AsrCapacityNoPaginationError,
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
