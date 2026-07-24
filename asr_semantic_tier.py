"""Freeze and evaluate ASR tier-1 submission semantics without market outcomes."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import asr_capacity as base
import asr_submission_collection as submissions
import asr_submission_tier as tier


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
COLLECTION_INSPECTION_SHA256 = (
    "62aa966f9f8ba7dec4408ad5acf580d90a3628c615bdce874d467d7468dbaa33"
)
COLLECTION_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/asr/submissions/tier1/collection-inspections/"
    f"{tier.capacity.CANDIDATE_ID}-{tier.TIER_ID}-"
    f"{COLLECTION_INSPECTION_SHA256}.json"
)
DEFAULT_CONTRACT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/semantics/tier1/contracts"
)
DEFAULT_CONTRACT_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/semantics/tier1/contract-status.json"
)
DEFAULT_RESULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/semantics/tier1/results"
)
DEFAULT_RESULT_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/semantics/tier1/result-status.json"
)
PRIVATE_RESULT_NAMESPACE = "_derived/asr-semantic-tier1"
WINDOW_BEFORE_CHARACTERS = 1_500
WINDOW_AFTER_CHARACTERS = 6_500
RESOLUTION_RADIUS_CHARACTERS = 1_200
MONTH_NAMES = (
    "January|February|March|April|May|June|July|August|September|October|"
    "November|December"
)
DATE_PATTERN = re.compile(
    rf"\b(?:{MONTH_NAMES})\s+\d{{1,2}},\s+20\d{{2}}\b",
    re.IGNORECASE,
)
NOTIONAL_PATTERN = re.compile(
    r"\$\s*(\d+(?:,\d{3})*(?:\.\d+)?)\s*(million|billion)?\b",
    re.IGNORECASE,
)
HTML_TAG_PATTERN = re.compile(r"<[^>]{1,1000}>")
SPACE_PATTERN = re.compile(r"\s+")


class AsrSemanticTierError(RuntimeError):
    """The semantic contract or inspected source lineage is invalid."""


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
        raise AsrSemanticTierError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrSemanticTierError(f"{path} must contain an object")
    return value


def write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_private_collection(root: Path) -> dict[str, Any]:
    status = tier.read_object(submissions.STATUS_PATH)
    info = status["private_collection"]
    path = root / str(info["cache_relative_path"])
    raw = path.read_bytes() if path.is_file() else b""
    if (
        hashlib.sha256(raw).hexdigest() != info["file_sha256"]
        or len(raw) != info["bytes"]
    ):
        raise AsrSemanticTierError("private submission collection drifted")
    value = json.loads(gzip.decompress(raw))
    if not isinstance(value, dict) or value.get(
        "private_collection_sha256"
    ) != tier.self_hash(value, "private_collection_sha256"):
        raise AsrSemanticTierError("private submission collection hash is invalid")
    return value


def _lineage(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    collection = tier.read_object(submissions.STATUS_PATH)
    inspection = read_object(COLLECTION_INSPECTION_PATH)
    contract = tier.load_contract(submissions._contract_path())
    graph = submissions._private_graph(root, contract)
    private = _read_private_collection(root)
    if not (
        collection.get("collection_sha256")
        == tier.self_hash(collection, "collection_sha256")
        and collection.get("collection_sha256") == inspection.get("collection_sha256")
        and collection.get("success_count") == 196
        and collection.get("failure_count") == 5
        and collection.get("market_outcomes_accessed") is False
        and inspection.get("inspection_sha256") == COLLECTION_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == tier.self_hash(inspection, "inspection_sha256")
        and inspection.get("private_collection_rehashed") is True
        and inspection.get("success_count") == 196
        and inspection.get("failure_count") == 5
        and inspection.get("filing_semantic_classification_permitted") is False
        and inspection.get("market_outcomes_accessed") is False
        and private.get("success_count") == 196
        and private.get("failure_count") == 5
        and graph.get("selected_accession_count") == 201
    ):
        raise AsrSemanticTierError("inspected submission lineage differs")
    return collection, inspection, graph, private


def build_contract(*, store_root: Path | None = None) -> dict[str, Any]:
    root = store_root or tier.shared._store().root
    collection, inspection, graph, private = _lineage(root)
    implementation_paths = (
        "asr_semantic_tier.py",
        "asr_semantic_tier_inspection.py",
    )
    patterns = base.build_contract()["event_contract"]["pattern_contract"]
    contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-asr-tier1-semantic-contract",
        "campaign_id": tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier.capacity.CANDIDATE_ID,
        "strategy_version": tier.capacity.STRATEGY_VERSION,
        "tier_id": tier.TIER_ID,
        "source_lineage": {
            "collection_sha256": collection["collection_sha256"],
            "collection_inspection_sha256": inspection["inspection_sha256"],
            "private_graph_sha256": graph["private_graph_sha256"],
            "private_collection_sha256": private["private_collection_sha256"],
            "selected_accession_count": 201,
            "inspected_success_count": 196,
            "source_failure_count": 5,
            "unselected_unique_hit_count": 17_278,
        },
        "classification_contract": {
            "text_normalization": (
                "utf8 replacement decode, HTML entity unescape, bounded tag removal, "
                "and whitespace collapse"
            ),
            "window_before_characters": WINDOW_BEFORE_CHARACTERS,
            "window_after_characters": WINDOW_AFTER_CHARACTERS,
            "resolution_radius_characters": RESOLUTION_RADIUS_CHARACTERS,
            "all_positive_groups_same_window": True,
            "pattern_contract": patterns,
            "agreement_date_pattern": DATE_PATTERN.pattern,
            "committed_notional_pattern": NOTIONAL_PATTERN.pattern,
            "agreement_date_selection": "nearest parsed date to ASR phrase",
            "notional_selection": "nearest parsed dollar amount to ASR phrase",
            "agreement_date_after_acceptance": "ineligible",
            "missing_or_unparseable_date": "ineligible",
            "missing_or_unparseable_notional": "ineligible",
            "source_failure": "unresolved_zero_credit",
            "unselected_denominator_hit": "unresolved_zero_credit",
        },
        "deduplication_contract": {
            "key": "selected source CIK plus agreement date plus committed dollars",
            "winner": "earliest acceptance datetime then lexical accession",
            "one_accession_may_expose_multiple_distinct_events": True,
        },
        "capacity_contract": {
            "semantic_fast_lane_threshold": 100,
            "tier_2_opens_if_semantically_qualified_unique_events_below": 100,
            "formal_verified_event_count_requires_security_identity": True,
            "market_outcome_access_permitted": False,
        },
        "access_contract": {
            "local_inspected_submission_text_access_permitted_after_inspection": True,
            "new_provider_access_permitted": False,
            "security_identity_provider_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            path: file_hash(PROJECT_ROOT / path) for path in implementation_paths
        },
        "semantically_qualified_unique_event_count": None,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    return contract


def load_contract(path: Path) -> dict[str, Any]:
    value = read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == self_hash(value, "contract_sha256")
        and path.name == f"{tier.capacity.CANDIDATE_ID}-semantic-tier1-{digest}.json"
    ):
        raise AsrSemanticTierError("semantic contract was mutated or renamed")
    return value


def freeze_contract(
    *,
    output_root: Path = DEFAULT_CONTRACT_ROOT,
    status_path: Path = DEFAULT_CONTRACT_STATUS,
) -> tuple[Path, dict[str, Any]]:
    contract = build_contract()
    path = output_root / (
        f"{tier.capacity.CANDIDATE_ID}-semantic-tier1-"
        f"{contract['contract_sha256']}.json"
    )
    if path.exists() and read_object(path) != contract:
        raise AsrSemanticTierError("content-addressed semantic contract differs")
    write_object(contract, path)
    write_object(
        {
            "schema_version": 1,
            "campaign_id": tier.capacity.CAMPAIGN_ID,
            "candidate_id": tier.capacity.CANDIDATE_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "SEMANTIC_CONTRACT_PENDING_INSPECTION",
            "local_source_text_access_permitted": False,
            "provider_access_permitted": False,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, contract


def _normalized_text(raw: bytes) -> str:
    text = raw.decode("utf-8", errors="replace")
    text = html.unescape(text)
    text = HTML_TAG_PATTERN.sub(" ", text)
    return SPACE_PATTERN.sub(" ", text)


def _parse_date(value: str) -> str | None:
    try:
        return datetime.strptime(value.title(), "%B %d, %Y").date().isoformat()
    except ValueError:
        return None


def _parse_notional(match: re.Match[str]) -> int | None:
    try:
        number = float(match.group(1).replace(",", ""))
    except ValueError:
        return None
    unit = (match.group(2) or "").lower()
    multiplier = 1_000_000 if unit == "million" else 1_000_000_000 if unit == "billion" else 1
    value = number * multiplier
    if value <= 0 or value > 1_000_000_000_000 or not value.is_integer():
        return None
    return int(value)


def _nearest_date(text: str, phrase_offset: int) -> str | None:
    candidates = [
        (abs(match.start() - phrase_offset), _parse_date(match.group(0)))
        for match in DATE_PATTERN.finditer(text)
        if abs(match.start() - phrase_offset) <= RESOLUTION_RADIUS_CHARACTERS
    ]
    valid = [(distance, value) for distance, value in candidates if value is not None]
    return min(valid)[1] if valid else None


def _nearest_notional(text: str, phrase_offset: int) -> int | None:
    candidates = [
        (abs(match.start() - phrase_offset), _parse_notional(match))
        for match in NOTIONAL_PATTERN.finditer(text)
        if abs(match.start() - phrase_offset) <= RESOLUTION_RADIUS_CHARACTERS
    ]
    valid = [(distance, value) for distance, value in candidates if value is not None]
    return min(valid)[1] if valid else None


def classify_submission(
    raw: bytes,
    *,
    acceptance_datetime: str,
    cik: str,
    accession: str,
) -> tuple[list[dict[str, Any]], str]:
    text = _normalized_text(raw)
    patterns = base.build_contract()["event_contract"]["pattern_contract"]
    asr_patterns = [
        re.compile(value, re.IGNORECASE | re.DOTALL)
        for value in patterns["asr_phrase"]
    ]
    executed_patterns = [
        re.compile(value, re.IGNORECASE | re.DOTALL)
        for value in patterns["executed_agreement"]
    ]
    notional_patterns = [
        re.compile(value, re.IGNORECASE | re.DOTALL)
        for value in patterns["committed_notional"]
    ]
    mechanics_patterns = [
        re.compile(value, re.IGNORECASE | re.DOTALL)
        for value in patterns["continuing_delivery_or_settlement"]
    ]
    phrase_matches = [
        match for pattern in asr_patterns for match in pattern.finditer(text)
    ]
    if not phrase_matches:
        return [], "NO_ASR_PHRASE"
    acceptance_day = date(
        int(acceptance_datetime[0:4]),
        int(acceptance_datetime[4:6]),
        int(acceptance_datetime[6:8]),
    )
    candidates: dict[tuple[str, str, int], dict[str, Any]] = {}
    failure_reasons: Counter[str] = Counter()
    for phrase in sorted(phrase_matches, key=lambda value: value.start()):
        start = max(0, phrase.start() - WINDOW_BEFORE_CHARACTERS)
        end = min(len(text), phrase.end() + WINDOW_AFTER_CHARACTERS)
        window = text[start:end]
        local_phrase_offset = phrase.start() - start
        if not any(pattern.search(window) for pattern in executed_patterns):
            failure_reasons["NO_EXECUTED_AGREEMENT_SAME_WINDOW"] += 1
            continue
        if not any(pattern.search(window) for pattern in notional_patterns):
            failure_reasons["NO_COMMITTED_NOTIONAL_SAME_WINDOW"] += 1
            continue
        if not any(pattern.search(window) for pattern in mechanics_patterns):
            failure_reasons["NO_CONTINUING_MECHANICS_SAME_WINDOW"] += 1
            continue
        agreement_date = _nearest_date(window, local_phrase_offset)
        if agreement_date is None:
            failure_reasons["UNRESOLVED_AGREEMENT_DATE"] += 1
            continue
        if date.fromisoformat(agreement_date) > acceptance_day:
            failure_reasons["AGREEMENT_DATE_AFTER_ACCEPTANCE"] += 1
            continue
        notional = _nearest_notional(window, local_phrase_offset)
        if notional is None:
            failure_reasons["UNRESOLVED_COMMITTED_NOTIONAL"] += 1
            continue
        key = (cik, agreement_date, notional)
        candidates[key] = {
            "issuer_cik": cik,
            "agreement_date": agreement_date,
            "committed_notional_dollars": notional,
            "acceptance_datetime_raw": acceptance_datetime,
            "accession": accession,
            "event_key_sha256": hashlib.sha256(
                canonical_bytes(list(key))
            ).hexdigest(),
        }
    if candidates:
        return [candidates[key] for key in sorted(candidates)], "QUALIFIED_EVENT_CANDIDATE"
    terminal = (
        failure_reasons.most_common(1)[0][0]
        if failure_reasons
        else "NO_COMPLETE_SAME_WINDOW_EVENT"
    )
    return [], terminal


def _contract_path() -> Path:
    matches = sorted(DEFAULT_CONTRACT_ROOT.glob("*.json"))
    if len(matches) != 1:
        raise AsrSemanticTierError("expected exactly one semantic contract")
    return matches[0]


def _load_evaluation_authority(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contract = load_contract(_contract_path())
    inspection = read_object(DEFAULT_CONTRACT_STATUS)
    _collection, _source_inspection, graph, private = _lineage(root)
    if not (
        contract == build_contract(store_root=root)
        and inspection.get("status") == "SEMANTIC_CONTRACT_INSPECTED"
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("inspection_sha256")
        == self_hash(inspection, "inspection_sha256")
        and inspection.get("local_source_text_access_permitted") is True
        and inspection.get("provider_access_permitted") is False
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("outcome_access_permitted") is False
        and inspection.get("broker_actions_permitted") is False
        and inspection.get("valid") is True
    ):
        raise AsrSemanticTierError("semantic contract is not independently inspected")
    return contract, inspection, graph, private


def evaluate(
    *,
    result_root: Path = DEFAULT_RESULT_ROOT,
    status_path: Path = DEFAULT_RESULT_STATUS,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = store_root or tier.shared._store().root
    contract, inspection, graph, private = _load_evaluation_authority(root)
    requests_by_ordinal = {
        int(row["ordinal"]): row for row in graph["requests"]
    }
    accession_rows: list[dict[str, Any]] = []
    all_events: list[dict[str, Any]] = []
    for record in private["records"]:
        request = requests_by_ordinal[int(record["ordinal"])]
        selected_ordinal = int(record["selected_candidate_ordinal"])
        candidate = next(
            row
            for row in request["candidates"]
            if int(row["candidate_ordinal"]) == selected_ordinal
        )
        raw = (root / record["source_cache_relative_path"]).read_bytes()
        events, terminal = classify_submission(
            raw,
            acceptance_datetime=str(record["acceptance_datetime_raw"]),
            cik=str(candidate["cik"]),
            accession=str(record["accession"]),
        )
        accession_rows.append(
            {
                "ordinal": int(record["ordinal"]),
                "accession": record["accession"],
                "terminal_reason": terminal,
                "qualified_event_count": len(events),
            }
        )
        all_events.extend(events)
    for failure in private["failures"]:
        accession_rows.append(
            {
                "ordinal": int(failure["ordinal"]),
                "accession": failure["accession"],
                "terminal_reason": "SOURCE_FAILURE_UNRESOLVED_ZERO_CREDIT",
                "qualified_event_count": 0,
            }
        )
    winners: dict[tuple[str, str, int], dict[str, Any]] = {}
    for event in all_events:
        key = (
            str(event["issuer_cik"]),
            str(event["agreement_date"]),
            int(event["committed_notional_dollars"]),
        )
        prior = winners.get(key)
        rank = (event["acceptance_datetime_raw"], event["accession"])
        prior_rank = (
            (prior["acceptance_datetime_raw"], prior["accession"])
            if prior is not None
            else None
        )
        if prior_rank is None or rank < prior_rank:
            winners[key] = event
    unique_events = [winners[key] for key in sorted(winners)]
    terminal_counts = Counter(
        row["terminal_reason"] for row in accession_rows
    )
    private_result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier1-semantic-result",
        "campaign_id": tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier.capacity.CANDIDATE_ID,
        "contract_sha256": contract["contract_sha256"],
        "accession_rows": sorted(accession_rows, key=lambda row: row["ordinal"]),
        "unique_events": unique_events,
        "terminal_counts": dict(sorted(terminal_counts.items())),
        "semantically_qualified_unique_event_count": len(unique_events),
        "security_identity_resolution_complete": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private_result["private_result_sha256"] = self_hash(
        private_result, "private_result_sha256"
    )
    private_path = (
        root
        / PRIVATE_RESULT_NAMESPACE
        / f"{private_result['private_result_sha256']}.json.gz"
    )
    raw = gzip.compress(canonical_bytes(private_result), mtime=0)
    private_path.parent.mkdir(parents=True, exist_ok=True)
    private_path.write_bytes(raw)
    result: dict[str, Any] = {
        "schema_version": 1,
        "result_kind": "outcome-blind-asr-tier1-semantic-result",
        "campaign_id": tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier.capacity.CANDIDATE_ID,
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "inspected_source_success_count": 196,
        "source_failure_count": 5,
        "unselected_unique_hit_count": 17_278,
        "terminal_counts": dict(sorted(terminal_counts.items())),
        "semantically_qualified_unique_event_count": len(unique_events),
        "private_result": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_result_sha256": private_result["private_result_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
        },
        "tier_2_open_candidate": len(unique_events) < 100,
        "security_identity_resolution_required": bool(unique_events),
        "security_identity_resolution_complete": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "state": "SEMANTIC_RESULT_PENDING_INSPECTION",
        "valid": True,
    }
    result["result_sha256"] = self_hash(result, "result_sha256")
    output = result_root / (
        f"{tier.capacity.CANDIDATE_ID}-semantic-tier1-"
        f"{result['result_sha256']}.json"
    )
    write_object(result, output)
    write_object(result, status_path)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "evaluate", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, contract = freeze_contract()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": contract["contract_sha256"],
                "local_source_text_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        elif args.command == "evaluate":
            path, value = evaluate()
            result = {
                **value,
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            result = read_object(DEFAULT_RESULT_STATUS)
    except (AsrSemanticTierError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
