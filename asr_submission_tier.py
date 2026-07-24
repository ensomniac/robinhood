"""Freeze the first high-precision ASR complete-submission collection tier."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asr_capacity as base
import asr_capacity_no_pagination as capacity
import asr_capacity_no_pagination_collection as denominator
import asr_capacity_recovery_collection as shared


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
TIER_ID = "exact-accelerated-repurchase-agreement-tier-1"
HIGH_PRECISION_PHRASE = '"accelerated repurchase agreement"'
HIGH_PRECISION_PHRASE_SHA256 = hashlib.sha256(
    HIGH_PRECISION_PHRASE.encode()
).hexdigest()
DENOMINATOR_INSPECTION_SHA256 = (
    "500443324fa67b74f1d38d54cdca31e6169435a97a21a71dea3393e536d805fe"
)
DENOMINATOR_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/asr/no-pagination/search/inspections/"
    f"{capacity.CANDIDATE_ID}-search-{DENOMINATOR_INSPECTION_SHA256}.json"
)
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/submissions/tier1/contracts"
)
DEFAULT_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/submissions/tier1/contract-status.json"
)
PRIVATE_NAMESPACE = "_derived/asr-submission-tier1"
EXPECTED_SELECTED_HITS = 204
EXPECTED_ACCESSIONS = 201
EXPECTED_CANDIDATE_URLS = 248
EXPECTED_UNSELECTED_UNIQUE_HITS = 17_278
ACCESSION_PATTERN = re.compile(r"^\d{10}-\d{2}-\d{6}$")


class AsrSubmissionTierError(RuntimeError):
    """The high-precision ASR submission tier is invalid or drifted."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        canonical_bytes({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrSubmissionTierError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrSubmissionTierError(f"{path} must contain an object")
    return value


def write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_gzip(value: Mapping[str, Any], path: Path) -> None:
    raw = gzip.compress(canonical_bytes(value), mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as exc:
        raise AsrSubmissionTierError(f"cannot read private graph {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrSubmissionTierError("private request graph must be an object")
    return value


def _lineage() -> tuple[dict[str, Any], dict[str, Any]]:
    collected = capacity.read_object(denominator.STATUS_PATH)
    inspected = read_object(DENOMINATOR_INSPECTION_PATH)
    if not (
        collected.get("collection_sha256")
        == capacity.self_hash(collected, "collection_sha256")
        and collected.get("collection_sha256") == inspected.get("collection_sha256")
        and collected.get("state") == "SEARCH_DENOMINATOR_COMPLETE"
        and collected.get("unique_hit_count") == 17_482
        and collected.get("market_outcomes_accessed") is False
        and inspected.get("inspection_sha256") == DENOMINATOR_INSPECTION_SHA256
        and inspected.get("inspection_sha256")
        == capacity.self_hash(inspected, "inspection_sha256")
        and inspected.get("denominator_complete") is True
        and inspected.get("matched_document_manifest_freeze_permitted") is True
        and inspected.get("matched_document_access_permitted") is False
        and inspected.get("market_outcomes_accessed") is False
    ):
        raise AsrSubmissionTierError("inspected denominator lineage differs")
    return collected, inspected


def _selected_hits(
    collected: Mapping[str, Any], store_root: Path
) -> list[dict[str, Any]]:
    result: dict[str, dict[str, Any]] = {}
    for summary in collected["page_summaries"]:
        if not (
            summary["phrase_sha256"] == HIGH_PRECISION_PHRASE_SHA256
            and summary["total_relation"] == "eq"
            and int(summary["reported_total"]) <= 100
        ):
            continue
        raw_path = store_root / str(summary["cache_relative_path"])
        response = json.loads(raw_path.read_text(encoding="utf-8"))
        for hit in shared._hits(response):
            canonical = denominator._canonical_hit(hit)
            identifier = str(canonical["_id"])
            if identifier in result and result[identifier] != canonical:
                raise AsrSubmissionTierError(
                    "high-precision hit has conflicting source fields"
                )
            result[identifier] = canonical
    values = [result[key] for key in sorted(result)]
    if len(values) != EXPECTED_SELECTED_HITS:
        raise AsrSubmissionTierError("high-precision hit count differs")
    return values


def _submission_url(cik: str, accession: str) -> str:
    if not cik.isdigit() or not ACCESSION_PATTERN.fullmatch(accession):
        raise AsrSubmissionTierError("submission identity is malformed")
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/{accession}.txt"
    )


def build_private_graph(
    *, store_root: Path | None = None
) -> tuple[dict[str, Any], Path]:
    collected, inspected = _lineage()
    root = store_root or shared._store().root
    hits = _selected_hits(collected, root)
    accessions: dict[str, dict[str, Any]] = {}
    for hit in hits:
        source = hit["_source"]
        accession = str(source.get("adsh") or "")
        file_date = str(source.get("file_date") or "")
        ciks = source.get("ciks")
        if (
            not ACCESSION_PATTERN.fullmatch(accession)
            or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", file_date)
            or not isinstance(ciks, list)
            or not ciks
        ):
            raise AsrSubmissionTierError(
                "selected hit lacks accession, date, or CIK candidates"
            )
        row = accessions.setdefault(
            accession,
            {
                "accession": accession,
                "file_date": file_date,
                "selected_hit_ids": [],
                "candidate_ciks": [],
            },
        )
        if row["file_date"] != file_date:
            raise AsrSubmissionTierError("accession file date conflicts")
        row["selected_hit_ids"].append(str(hit["_id"]))
        for cik_value in ciks:
            cik = str(cik_value)
            if cik not in row["candidate_ciks"]:
                row["candidate_ciks"].append(cik)
    requests: list[dict[str, Any]] = []
    for ordinal, accession in enumerate(sorted(accessions)):
        source = accessions[accession]
        candidates = [
            {
                "candidate_ordinal": index,
                "cik": cik,
                "url": _submission_url(cik, accession),
            }
            for index, cik in enumerate(source["candidate_ciks"])
        ]
        request = {
            "ordinal": ordinal,
            "accession": accession,
            "file_date": source["file_date"],
            "selected_hit_ids": sorted(source["selected_hit_ids"]),
            "candidates": candidates,
        }
        request["request_sha256"] = hashlib.sha256(
            canonical_bytes(request)
        ).hexdigest()
        requests.append(request)
    if not (
        len(requests) == EXPECTED_ACCESSIONS
        and sum(len(row["candidates"]) for row in requests)
        == EXPECTED_CANDIDATE_URLS
        and len(
            {
                candidate["url"]
                for row in requests
                for candidate in row["candidates"]
            }
        )
        == EXPECTED_CANDIDATE_URLS
    ):
        raise AsrSubmissionTierError("complete-submission request counts differ")
    graph: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-complete-submission-request-graph",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "tier_id": TIER_ID,
        "denominator_collection_sha256": collected["collection_sha256"],
        "denominator_inspection_sha256": inspected["inspection_sha256"],
        "selection_phrase_sha256": HIGH_PRECISION_PHRASE_SHA256,
        "selected_hit_count": EXPECTED_SELECTED_HITS,
        "selected_accession_count": EXPECTED_ACCESSIONS,
        "candidate_url_count": EXPECTED_CANDIDATE_URLS,
        "unselected_unique_hit_count": EXPECTED_UNSELECTED_UNIQUE_HITS,
        "requests": requests,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    graph["private_graph_sha256"] = self_hash(graph, "private_graph_sha256")
    path = (
        root
        / PRIVATE_NAMESPACE
        / f"{graph['private_graph_sha256']}.json.gz"
    )
    return graph, path


def build_contract(
    *, store_root: Path | None = None
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    graph, graph_path = build_private_graph(store_root=store_root)
    implementation_paths = (
        "asr_submission_tier.py",
        "asr_submission_tier_inspection.py",
        "asr_submission_collection.py",
        "asr_submission_collection_inspection.py",
    )
    contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-asr-complete-submission-tier-contract",
        "campaign_id": capacity.CAMPAIGN_ID,
        "theme_id": capacity.THEME_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "tier_id": TIER_ID,
        "selection_contract": {
            "source": "complete inspected four-phrase EFTS denominator",
            "selected_phrase": HIGH_PRECISION_PHRASE,
            "selected_phrase_sha256": HIGH_PRECISION_PHRASE_SHA256,
            "selected_hit_count": EXPECTED_SELECTED_HITS,
            "selected_accession_count": EXPECTED_ACCESSIONS,
            "candidate_url_count": EXPECTED_CANDIDATE_URLS,
            "unselected_unique_hit_count": EXPECTED_UNSELECTED_UNIQUE_HITS,
            "unselected_terminal_reason": (
                "OUTSIDE_FROZEN_HIGH_PRECISION_TIER_UNRESOLVED"
            ),
            "selection_uses_market_outcomes": False,
            "date_form_issuer_or_event_substitution_permitted": False,
        },
        "staged_capacity_contract": {
            "tier_1_semantics_must_be_independently_inspected_before_tier_2": True,
            "tier_2_may_open_only_if_verified_unique_events_below": 100,
            "tier_2_scope": (
                "already indexed broader 8-K and 6-K accession candidates only"
            ),
            "capacity_threshold_unchanged": True,
        },
        "request_contract": {
            "provider": "SEC_EDGAR_ARCHIVES",
            "endpoint_kind": "accession-bound complete submission text",
            "one_successful_submission_per_accession": True,
            "candidate_order": "EFTS source CIK order",
            "candidate_404_action": "try_next_frozen_candidate",
            "maximum_attempts_per_candidate": 3,
            "retryable_statuses": [429, 500, 502, 503, 504],
            "minimum_spacing_seconds": 0.20,
            "timeout_seconds": 30.0,
            "maximum_source_bytes": 50_000_000,
            "provider_substitution_permitted": False,
        },
        "private_graph": {
            "cache_relative_path": str(graph_path.relative_to(
                store_root or shared._store().root
            )),
            "private_graph_sha256": graph["private_graph_sha256"],
            "private_graph_file_sha256": None,
            "private_graph_bytes": None,
        },
        "classification_handoff": {
            "acceptance_datetime_source": "ACCEPTANCE-DATETIME in submission text",
            "asr_patterns": base.build_contract()["event_contract"][
                "pattern_contract"
            ],
            "semantic_classification_before_collection_inspection_permitted": False,
            "event_deduplication": base.build_contract()["event_contract"][
                "deduplication_key"
            ],
        },
        "access_contract": {
            "provider_access_before_independent_inspection_permitted": False,
            "provider_access_after_independent_inspection_permitted": True,
            "filing_semantic_classification_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            path: file_hash(PROJECT_ROOT / path) for path in implementation_paths
        },
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    return contract, graph, graph_path


def load_contract(path: Path) -> dict[str, Any]:
    value = read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == self_hash(value, "contract_sha256")
        and path.name == f"{capacity.CANDIDATE_ID}-{TIER_ID}-{digest}.json"
    ):
        raise AsrSubmissionTierError("submission-tier contract was mutated or renamed")
    return value


def freeze_contract(
    *,
    output_root: Path = DEFAULT_ROOT,
    status_path: Path = DEFAULT_STATUS,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = store_root or shared._store().root
    contract, graph, graph_path = build_contract(store_root=root)
    _write_gzip(graph, graph_path)
    raw = graph_path.read_bytes()
    contract["private_graph"]["private_graph_file_sha256"] = hashlib.sha256(
        raw
    ).hexdigest()
    contract["private_graph"]["private_graph_bytes"] = len(raw)
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    path = output_root / (
        f"{capacity.CANDIDATE_ID}-{TIER_ID}-{contract['contract_sha256']}.json"
    )
    if path.exists() and read_object(path) != contract:
        raise AsrSubmissionTierError("content-addressed tier contract differs")
    write_object(contract, path)
    write_object(
        {
            "schema_version": 1,
            "campaign_id": capacity.CAMPAIGN_ID,
            "candidate_id": capacity.CANDIDATE_ID,
            "tier_id": TIER_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "SUBMISSION_TIER_CONTRACT_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "filing_semantic_classification_permitted": False,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, contract


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, contract = freeze_contract()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": contract["contract_sha256"],
                "selected_hit_count": EXPECTED_SELECTED_HITS,
                "selected_accession_count": EXPECTED_ACCESSIONS,
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        else:
            result = read_object(DEFAULT_STATUS)
    except (AsrSubmissionTierError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
