"""Collect the frozen ASR high-precision complete-submission tier."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import asr_submission_tier as tier
from historical_discovery import SecConfig


PROJECT_ROOT = Path(__file__).resolve().parent
STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/asr/submissions/tier1/collection-status.json"
)
ENV_PATH = PROJECT_ROOT / ".env"
PRIVATE_COLLECTION_NAMESPACE = "_derived/asr-submission-tier1-collection"
RAW_NAMESPACE = "_sources/sec/asr-submission-tier1"
ACCEPTANCE_PATTERN = re.compile(
    rb"<ACCEPTANCE-DATETIME>\s*(\d{14})",
    re.IGNORECASE,
)


class AsrSubmissionCollectionError(RuntimeError):
    """The ASR submission collection is unauthorized, incomplete, or drifted."""


def _contract_path() -> Path:
    matches = sorted(tier.DEFAULT_ROOT.glob("*.json"))
    if len(matches) != 1:
        raise AsrSubmissionCollectionError(
            "expected exactly one frozen submission-tier contract"
        )
    return matches[0]


def _private_graph(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    path = root / str(contract["private_graph"]["cache_relative_path"])
    raw = path.read_bytes() if path.is_file() else b""
    if (
        hashlib.sha256(raw).hexdigest()
        != contract["private_graph"]["private_graph_file_sha256"]
        or len(raw) != contract["private_graph"]["private_graph_bytes"]
    ):
        raise AsrSubmissionCollectionError("private request graph drifted")
    graph = tier._read_gzip(path)
    if graph.get("private_graph_sha256") != contract["private_graph"][
        "private_graph_sha256"
    ]:
        raise AsrSubmissionCollectionError("private graph content hash differs")
    return graph


def _load_authority() -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any], Path]:
    path = _contract_path()
    contract = tier.load_contract(path)
    root = tier.shared._store().root
    graph = _private_graph(root, contract)
    status = tier.read_object(tier.DEFAULT_STATUS)
    if not (
        status.get("status") == "SUBMISSION_TIER_CONTRACT_INSPECTED"
        and status.get("contract_sha256") == contract["contract_sha256"]
        and status.get("inspection_sha256")
        == tier.self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("filing_semantic_classification_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
        and len(graph.get("requests", [])) == 201
        and graph.get("market_outcomes_accessed") is False
    ):
        raise AsrSubmissionCollectionError(
            "submission tier is not independently inspected"
        )
    return path, contract, status, graph, root


def _cache_path(root: Path, url: str) -> Path:
    digest = hashlib.sha256(url.encode()).hexdigest()
    return root / RAW_NAMESPACE / f"{digest}.txt"


def _private_collection_path(root: Path, contract_sha256: str) -> Path:
    return (
        root
        / PRIVATE_COLLECTION_NAMESPACE
        / f"{contract_sha256}-collection.json.gz"
    )


def _write_private(value: Mapping[str, Any], path: Path) -> None:
    raw = gzip.compress(tier.canonical_bytes(value), mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _request_candidate(
    session: requests.Session,
    url: str,
    path: Path,
    *,
    contract: Mapping[str, Any],
    pace: Any,
    telemetry: dict[str, Any],
) -> tuple[str, bytes | None, int | None]:
    if path.is_file():
        telemetry["cache_hits"] += 1
        return "SUCCESS", path.read_bytes(), 200
    request_contract = contract["request_contract"]
    retryable = set(request_contract["retryable_statuses"])
    maximum_attempts = int(request_contract["maximum_attempts_per_candidate"])
    for attempt in range(maximum_attempts):
        pace()
        started = time.monotonic()
        try:
            response = session.get(
                url, timeout=float(request_contract["timeout_seconds"])
            )
            telemetry["requests"] += 1
            telemetry["request_seconds"] += time.monotonic() - started
        except requests.RequestException:
            telemetry["transport_failures"] += 1
            if attempt + 1 >= maximum_attempts:
                return "TRANSPORT_FAILURE", None, None
            wait = float(2**attempt)
            time.sleep(wait)
            telemetry["retry_attempts"] += 1
            telemetry["retry_wait_seconds"] += wait
            continue
        status = int(response.status_code)
        if status == 200:
            raw = response.content
            if (
                not raw
                or len(raw) > int(request_contract["maximum_source_bytes"])
                or b"<SEC-DOCUMENT" not in raw[:4096].upper()
                or ACCEPTANCE_PATTERN.search(raw) is None
            ):
                return "MALFORMED_RESPONSE", None, status
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
            try:
                temporary.write_bytes(raw)
                os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
            telemetry["downloads"] += 1
            telemetry["download_bytes"] += len(raw)
            return "SUCCESS", raw, status
        if status in retryable:
            telemetry["retryable_http_failures"] += 1
            if attempt + 1 >= maximum_attempts:
                return "RETRY_EXHAUSTED", None, status
            wait = float(2**attempt)
            time.sleep(wait)
            telemetry["retry_attempts"] += 1
            telemetry["retry_wait_seconds"] += wait
            continue
        telemetry["terminal_http_candidates"] += 1
        return f"HTTP_{status}", None, status
    return "RETRY_EXHAUSTED", None, None


def collect(
    *,
    status_path: Path = STATUS_PATH,
    require_published: bool = True,
    session: requests.Session | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    contract_path, contract, inspection, graph, configured_root = _load_authority()
    root = store_root or configured_root
    publication = (
        {
            "collector": tier.shared._published(Path(__file__).resolve()),
            "contract": tier.shared._published(contract_path),
            "contract_inspection": tier.shared._published(tier.DEFAULT_STATUS),
        }
        if require_published
        else {}
    )
    config = SecConfig.from_env(
        ENV_PATH, root / RAW_NAMESPACE, workers=1
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
        "downloads": 0,
        "download_bytes": 0,
        "transport_failures": 0,
        "retryable_http_failures": 0,
        "terminal_http_candidates": 0,
        "retry_attempts": 0,
        "retry_wait_seconds": 0.0,
    }
    next_at = 0.0

    def pace() -> None:
        nonlocal next_at
        now = time.monotonic()
        delay = max(0.0, next_at - now)
        if delay:
            time.sleep(delay)
            telemetry["pacing_wait_seconds"] += delay
        next_at = max(now, next_at) + float(
            contract["request_contract"]["minimum_spacing_seconds"]
        )

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    attempted_candidate_count = 0
    for request in graph["requests"]:
        attempts: list[dict[str, Any]] = []
        selected: dict[str, Any] | None = None
        for candidate in request["candidates"]:
            attempted_candidate_count += 1
            url = str(candidate["url"])
            path = _cache_path(root, url)
            state, raw, status = _request_candidate(
                client,
                url,
                path,
                contract=contract,
                pace=pace,
                telemetry=telemetry,
            )
            attempts.append(
                {
                    "candidate_ordinal": candidate["candidate_ordinal"],
                    "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "state": state,
                    "http_status": status,
                }
            )
            if state == "SUCCESS" and raw is not None:
                match = ACCEPTANCE_PATTERN.search(raw)
                if match is None:
                    raise AsrSubmissionCollectionError(
                        "accepted submission lacks acceptance datetime"
                    )
                selected = {
                    "ordinal": request["ordinal"],
                    "request_sha256": request["request_sha256"],
                    "accession": request["accession"],
                    "file_date": request["file_date"],
                    "selected_candidate_ordinal": candidate["candidate_ordinal"],
                    "source_cache_relative_path": str(path.relative_to(root)),
                    "source_sha256": hashlib.sha256(raw).hexdigest(),
                    "source_bytes": len(raw),
                    "acceptance_datetime_raw": match.group(1).decode(),
                    "candidate_attempts": attempts,
                }
                break
        if selected is None:
            failures.append(
                {
                    "ordinal": request["ordinal"],
                    "request_sha256": request["request_sha256"],
                    "accession": request["accession"],
                    "candidate_attempts": attempts,
                    "terminal_reason": "NO_VALID_COMPLETE_SUBMISSION_CANDIDATE",
                }
            )
        else:
            records.append(selected)
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-complete-submission-collection",
        "campaign_id": tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier.capacity.CANDIDATE_ID,
        "tier_id": tier.TIER_ID,
        "contract_sha256": contract["contract_sha256"],
        "private_graph_sha256": graph["private_graph_sha256"],
        "request_count": len(graph["requests"]),
        "success_count": len(records),
        "failure_count": len(failures),
        "attempted_candidate_count": attempted_candidate_count,
        "records": records,
        "failures": failures,
        "filing_semantic_classified_count": 0,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["private_collection_sha256"] = tier.self_hash(
        private, "private_collection_sha256"
    )
    private_path = _private_collection_path(root, contract["contract_sha256"])
    _write_private(private, private_path)
    private_raw = private_path.read_bytes()
    valid = not failures and len(records) == len(graph["requests"])
    result: dict[str, Any] = {
        "schema_version": 1,
        "collection_kind": "outcome-blind-asr-complete-submission-tier",
        "campaign_id": tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier.capacity.CANDIDATE_ID,
        "tier_id": tier.TIER_ID,
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "publication": publication,
        "request_count": len(graph["requests"]),
        "success_count": len(records),
        "failure_count": len(failures),
        "attempted_candidate_count": attempted_candidate_count,
        "source_bytes": sum(int(row["source_bytes"]) for row in records),
        "private_collection": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_collection_sha256": private["private_collection_sha256"],
            "file_sha256": hashlib.sha256(private_raw).hexdigest(),
            "bytes": len(private_raw),
        },
        "provider_telemetry": telemetry,
        "state": (
            "SUBMISSIONS_COLLECTED_READY_FOR_INSPECTION"
            if valid
            else "SUBMISSIONS_COLLECTED_WITH_FAILURES"
        ),
        "filing_semantic_classification_permitted": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": valid,
    }
    result["collection_sha256"] = tier.self_hash(result, "collection_sha256")
    tier.write_object(result, status_path)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("collect", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = collect() if args.command == "collect" else tier.read_object(STATUS_PATH)
    except (
        AsrSubmissionCollectionError,
        tier.AsrSubmissionTierError,
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
