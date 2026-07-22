"""Freeze and run outcome-blind Schedule 13D filing-semantic capacity."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import io
import json
import os
import re
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_document_collection as documents
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
DOCUMENT_INSPECTION_SHA256 = (
    "6cbc8067c4b7afd802e6c215c41626df4c17cabf7152e78f686b04afdf85a5fb"
)
DOCUMENT_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/documents/inspections/"
    f"{capacity.CANDIDATE_ID}-document-collection-{DOCUMENT_INSPECTION_SHA256}.json"
)
ACTIVATION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/semantic/activations"
)
ACTIVATION_STATUS_PATH = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/semantic/activation-status.json"
)
RESULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/semantic/results"
RESULT_INSPECTION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/semantic/inspections"
)
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_semantic_capacity_inspection.py"
PRIVATE_NAMESPACE = (
    f"_derived/schedule13d_capacity/{documents.DATASET_ID}/semantic"
)
PRIVATE_RESULT_NAME = "semantic-capacity.json.gz"

ACCEPTANCE_PATTERN = re.compile(
    r"<ACCEPTANCE-DATETIME>\s*(\d{14})", re.IGNORECASE
)
SUBJECT_COMPANY_PATTERN = re.compile(
    r"(?:<SUBJECT-COMPANY>.*?<CIK>\s*|"
    r"SUBJECT\s+COMPANY:.*?CENTRAL\s+INDEX\s+KEY:\s*)(\d+)",
    re.IGNORECASE | re.DOTALL,
)
DOCUMENT_PATTERN = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.IGNORECASE | re.DOTALL)
TYPE_PATTERN = re.compile(r"<TYPE>\s*([^\r\n<]+)", re.IGNORECASE)
TEXT_PATTERN = re.compile(r"<TEXT>(.*)</TEXT>", re.IGNORECASE | re.DOTALL)
TITLE_CLASS_TAG_PATTERN = re.compile(
    r"<TITLEOCLASS>\s*([^\r\n<]+)", re.IGNORECASE
)
ISSUER_SYMBOL_TAG_PATTERN = re.compile(
    r"<[^>]*issuertradingsymbol[^>]*>\s*([^<\r\n]+)", re.IGNORECASE
)
ITEM4_HEADING_PATTERN = re.compile(r"^item\s*4\b", re.IGNORECASE)
ITEM5_HEADING_PATTERN = re.compile(r"^item\s*5\b", re.IGNORECASE)
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,31}$")
SYMBOL_BLACKLIST = {
    "EACH",
    "EXCHANGE",
    "NAME",
    "NONE",
    "N/A",
    "REGISTERED",
    "SYMBOL",
    "TRADING",
    "WHICH",
}


class Schedule13dSemanticCapacityError(RuntimeError):
    """The frozen filing-semantic capacity activation or result is invalid."""


class _ParagraphParser(HTMLParser):
    BLOCK_TAGS = {
        "address",
        "article",
        "blockquote",
        "br",
        "dd",
        "div",
        "dl",
        "dt",
        "h1",
        "h2",
        "h3",
        "h4",
        "h5",
        "h6",
        "hr",
        "li",
        "p",
        "pre",
        "section",
        "table",
        "td",
        "th",
        "tr",
    }

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.parts: list[str] = []
        self.suppressed = 0

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag.lower() in {"script", "style"}:
            self.suppressed += 1
        elif not self.suppressed and tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"script", "style"} and self.suppressed:
            self.suppressed -= 1
        elif not self.suppressed and tag.lower() in self.BLOCK_TAGS:
            self.parts.append("\n")

    def handle_data(self, data: str) -> None:
        if not self.suppressed:
            self.parts.append(data)


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dSemanticCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dSemanticCapacityError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dSemanticCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dSemanticCapacityError(f"{path} must contain an object")
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


def _private_result_path() -> Path:
    store = documents._store_config()
    return store.root / PRIVATE_NAMESPACE / PRIVATE_RESULT_NAME


def _load_document_lineage() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    collection = _read_object(documents.COLLECTION_STATUS_PATH)
    inspection = _read_object(DOCUMENT_INSPECTION_PATH)
    private_path = documents._private_collection_path(documents._store_config().root)
    private = documents._read_gzip_object(private_path)
    if not (
        collection.get("collection_sha256")
        == capacity.successor._self_hash(collection, "collection_sha256")
        and collection.get("success_count") == 6852
        and collection.get("failure_count") == 0
        and collection.get("valid") is True
        and inspection.get("inspection_sha256") == DOCUMENT_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == capacity.successor._self_hash(inspection, "inspection_sha256")
        and inspection.get("collection_sha256") == collection["collection_sha256"]
        and inspection.get("filing_semantic_classification_permitted") is True
        and inspection.get("issuer_symbol_supplemental_access_permitted") is False
        and inspection.get("market_outcomes_accessed") is False
        and inspection.get("valid") is True
        and private.get("private_collection_sha256")
        == collection.get("private_collection_sha256")
        and sha256_file(private_path)
        == collection.get("private_collection_file_sha256")
        and private.get("success_count") == 6852
        and private.get("failure_count") == 0
        and private.get("market_outcomes_accessed") is False
    ):
        raise Schedule13dSemanticCapacityError(
            "inspected complete-submission lineage is invalid"
        )
    return collection, inspection, private


def build_activation() -> dict[str, Any]:
    """Freeze semantic parser identities before classifying filing content."""

    collection, inspection, private = _load_document_lineage()
    activation: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-filing-semantic-capacity-activation",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "dataset_id": documents.DATASET_ID,
        "contract_sha256": documents.indexes.CONTRACT_SHA256,
        "document_collection_sha256": collection["collection_sha256"],
        "document_collection_inspection_sha256": inspection["inspection_sha256"],
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": collection[
            "private_collection_file_sha256"
        ],
        "denominator": {
            "indexed_initial_sc13d_filings": 6852,
            "terminal_or_pending_reason_required_for_every_filing": True,
            "verified_event_count": None,
        },
        "parser_contract": {
            "acceptance_pattern": ACCEPTANCE_PATTERN.pattern,
            "subject_company_pattern": SUBJECT_COMPANY_PATTERN.pattern,
            "document_pattern": DOCUMENT_PATTERN.pattern,
            "type_pattern": TYPE_PATTERN.pattern,
            "text_pattern": TEXT_PATTERN.pattern,
            "title_class_tag_pattern": TITLE_CLASS_TAG_PATTERN.pattern,
            "issuer_symbol_tag_pattern": ISSUER_SYMBOL_TAG_PATTERN.pattern,
            "item4_heading_pattern": ITEM4_HEADING_PATTERN.pattern,
            "item5_heading_pattern": ITEM5_HEADING_PATTERN.pattern,
            "symbol_pattern": SYMBOL_PATTERN.pattern,
            "symbol_blacklist": sorted(SYMBOL_BLACKLIST),
            "html_normalization": (
                "standard-library HTMLParser; script/style suppressed; block tags "
                "become paragraph boundaries; entities decoded; whitespace collapsed"
            ),
            "same_paragraph_intent_required": True,
            "control_patterns_sha256": hashlib.sha256(
                _canonical_bytes(capacity._pattern_contract())
            ).hexdigest(),
            "same_issuer_date_rule": (
                "earliest acceptance then lexical accession among source-and-semantic candidates"
            ),
            "cooldown_calendar_days": capacity.ISSUER_COOLDOWN_CALENDAR_DAYS,
            "unresolved_symbol_conservative_cooldown_anchor": True,
        },
        "access_contract": {
            "classification_before_activation_inspection_permitted": False,
            "classification_after_activation_inspection_permitted": True,
            "issuer_symbol_supplemental_access_permitted": False,
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
        "verified_event_count": None,
        "capacity_passed": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "claim_limit": (
            "This activation freezes only filing-semantic parsing. It contains no "
            "eligibility counts, price, fill, return, or maturity evidence."
        ),
    }
    activation["activation_sha256"] = capacity.successor._self_hash(
        activation, "activation_sha256"
    )
    return activation


def activation_path(value: Mapping[str, Any], root: Path = ACTIVATION_ROOT) -> Path:
    return root / f"{capacity.CANDIDATE_ID}-{value['activation_sha256']}.json"


def load_activation(path: Path) -> dict[str, Any]:
    value = _read_object(path)
    digest = value.get("activation_sha256")
    if not (
        isinstance(digest, str)
        and digest == capacity.successor._self_hash(value, "activation_sha256")
        and path.name == f"{capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise Schedule13dSemanticCapacityError(
            "semantic activation was mutated or renamed"
        )
    return value


def freeze_activation(
    *, root: Path = ACTIVATION_ROOT, status_path: Path = ACTIVATION_STATUS_PATH
) -> tuple[Path, dict[str, Any]]:
    value = build_activation()
    path = activation_path(value, root)
    if path.exists() and _read_object(path) != value:
        raise Schedule13dSemanticCapacityError(
            "content-addressed semantic activation has other content"
        )
    _write_json(value, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": capacity.CANDIDATE_ID,
            "dataset_id": documents.DATASET_ID,
            "contract_sha256": documents.indexes.CONTRACT_SHA256,
            "activation_sha256": value["activation_sha256"],
            "status": "SEMANTIC_ACTIVATION_PENDING_INSPECTION",
            "classification_permitted": False,
            "issuer_symbol_supplemental_access_permitted": False,
            "verified_event_count": None,
            "capacity_passed": None,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, value


def _paragraphs(value: str) -> list[str]:
    parser = _ParagraphParser()
    parser.feed(value)
    parser.close()
    text = html.unescape("".join(parser.parts))
    return [
        normalized
        for part in re.split(r"[\r\n]+", text)
        if (normalized := re.sub(r"\s+", " ", part).strip())
    ]


def _selected_document(complete: str) -> str | None:
    for match in DOCUMENT_PATTERN.finditer(complete):
        block = match.group(1)
        type_match = TYPE_PATTERN.search(block)
        if type_match is None:
            continue
        normalized_type = re.sub(r"\s+", " ", type_match.group(1)).strip().upper()
        if normalized_type != "SC 13D":
            continue
        text_match = TEXT_PATTERN.search(block)
        return text_match.group(1) if text_match else None
    return None


def _item4_paragraphs(paragraphs: Sequence[str]) -> list[str]:
    start: int | None = None
    for index, paragraph in enumerate(paragraphs):
        normalized = re.sub(r"[^a-z0-9]+", " ", paragraph.lower()).strip()
        if start is None and ITEM4_HEADING_PATTERN.search(normalized):
            start = index
            continue
        if start is not None and ITEM5_HEADING_PATTERN.search(normalized):
            return list(paragraphs[start:index])
    return list(paragraphs[start:]) if start is not None else []


def _security_class(complete: str, paragraphs: Sequence[str]) -> str | None:
    tagged = TITLE_CLASS_TAG_PATTERN.search(complete)
    if tagged:
        return html.unescape(tagged.group(1)).strip()
    label = re.compile(r"\(?\s*title of class of securities\s*\)?", re.IGNORECASE)
    for index, paragraph in enumerate(paragraphs):
        match = label.search(paragraph)
        if match:
            before = paragraph[: match.start()].strip(" :-()")
            if before:
                return before[-160:]
            if index:
                previous = paragraphs[index - 1].strip(" :-()")
                if previous:
                    return previous[-160:]
    return None


def _normalized_symbol(value: str) -> str | None:
    symbol = html.unescape(value).strip().upper().replace(" ", "")
    if symbol in SYMBOL_BLACKLIST or not SYMBOL_PATTERN.fullmatch(symbol):
        return None
    return symbol


def _event_symbol(document: str, paragraphs: Sequence[str]) -> str | None:
    tagged = ISSUER_SYMBOL_TAG_PATTERN.search(document)
    if tagged:
        symbol = _normalized_symbol(tagged.group(1))
        if symbol:
            return symbol
    label = re.compile(r"trading\s+symbol(?:\(s\))?", re.IGNORECASE)
    token = re.compile(r"\b[A-Z0-9][A-Z0-9.-]{0,9}\b")
    for paragraph in paragraphs:
        match = label.search(paragraph)
        if not match:
            continue
        tail = paragraph[match.end() : match.end() + 80]
        for candidate in token.findall(tail.upper()):
            symbol = _normalized_symbol(candidate)
            if symbol:
                return symbol
    return None


def _base_classification(request: Mapping[str, Any], raw: bytes) -> dict[str, Any]:
    complete = raw.decode("utf-8", errors="replace")
    accession = Path(str(request["filename"])).stem
    result: dict[str, Any] = {
        "ordinal": int(request["ordinal"]),
        "request_sha256": request["request_sha256"],
        "accession": accession,
        "index_cik": str(request["cik"]),
        "subject_cik": None,
        "accepted_at": None,
        "accepted_date": None,
        "security_class": None,
        "event_symbol": None,
        "control_category": None,
        "base_semantic_candidate": False,
        "verified_event": False,
        "status": None,
    }
    acceptance = ACCEPTANCE_PATTERN.search(complete)
    if acceptance is None:
        result["status"] = "MISSING_ACCEPTANCE_DATETIME"
        return result
    try:
        accepted = datetime.strptime(acceptance.group(1), "%Y%m%d%H%M%S")
    except ValueError:
        result["status"] = "INVALID_ACCEPTANCE_DATETIME"
        return result
    result["accepted_at"] = accepted.isoformat()
    result["accepted_date"] = accepted.date().isoformat()
    if not (
        capacity.COLLECTION_START
        <= result["accepted_date"]
        <= capacity.COLLECTION_END
    ):
        result["status"] = "ACCEPTANCE_OUTSIDE_FROZEN_WINDOW"
        return result
    subject = SUBJECT_COMPANY_PATTERN.search(complete)
    if subject is None:
        result["status"] = "MISSING_SUBJECT_COMPANY_CIK"
        return result
    result["subject_cik"] = subject.group(1).lstrip("0") or "0"
    selected = _selected_document(complete)
    if selected is None:
        result["status"] = "MISSING_ACCESSION_BOUND_SC13D_DOCUMENT"
        return result
    paragraphs = _paragraphs(selected)
    item4 = _item4_paragraphs(paragraphs)
    if not item4:
        result["status"] = "MISSING_OR_AMBIGUOUS_ITEM4"
        return result
    actor_patterns = [
        re.compile(value, re.IGNORECASE | re.DOTALL)
        for value in capacity.ITEM4_ACTOR_INTENT_PATTERNS
    ]
    category_patterns = {
        category: [re.compile(value, re.IGNORECASE | re.DOTALL) for value in values]
        for category, values in capacity.ITEM4_CONTROL_CATEGORY_PATTERNS.items()
    }
    actor_found = False
    category: str | None = None
    for paragraph in item4:
        actor_here = any(pattern.search(paragraph) for pattern in actor_patterns)
        actor_found = actor_found or actor_here
        if not actor_here:
            continue
        for name, patterns in category_patterns.items():
            if any(pattern.search(paragraph) for pattern in patterns):
                category = name
                break
        if category:
            break
    if not actor_found:
        result["status"] = "ITEM4_NO_ACTOR_INTENT"
        return result
    if category is None:
        result["status"] = "ITEM4_NO_SAME_PARAGRAPH_CONTROL_CATEGORY"
        return result
    result["control_category"] = category
    security_class = _security_class(complete, paragraphs)
    result["security_class"] = security_class
    if not security_class:
        result["status"] = "MISSING_SECURITY_CLASS"
        return result
    excluded = any(
        re.search(pattern, security_class, re.IGNORECASE | re.DOTALL)
        for pattern in capacity.EXCLUDED_CLASS_PATTERNS
    )
    common = any(
        re.search(pattern, security_class, re.IGNORECASE | re.DOTALL)
        for pattern in capacity.COMMON_SHARE_CLASS_PATTERNS
    )
    if excluded or not common:
        result["status"] = "INELIGIBLE_SECURITY_CLASS"
        return result
    result["event_symbol"] = _event_symbol(selected, paragraphs)
    result["base_semantic_candidate"] = True
    result["status"] = (
        "POTENTIAL_EVENT_SYMBOL_ELIGIBLE"
        if result["event_symbol"]
        else "PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL"
    )
    return result


def _apply_event_rules(records: list[dict[str, Any]]) -> None:
    candidates = [row for row in records if row["base_semantic_candidate"]]
    same_day: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in candidates:
        same_day[(str(row["subject_cik"]), str(row["accepted_date"]))].append(row)
    retained: list[dict[str, Any]] = []
    for rows in same_day.values():
        ordered = sorted(rows, key=lambda row: (str(row["accepted_at"]), row["accession"]))
        retained.append(ordered[0])
        for duplicate in ordered[1:]:
            duplicate["base_semantic_candidate"] = False
            duplicate["status"] = "SAME_ISSUER_ACCEPTANCE_DATE_DUPLICATE"
    by_issuer: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in retained:
        by_issuer[str(row["subject_cik"])].append(row)
    for rows in by_issuer.values():
        last_date: date | None = None
        for row in sorted(rows, key=lambda item: (str(item["accepted_at"]), item["accession"])):
            current = date.fromisoformat(str(row["accepted_date"]))
            if last_date is not None and (current - last_date).days < capacity.ISSUER_COOLDOWN_CALENDAR_DAYS:
                row["base_semantic_candidate"] = False
                row["status"] = "ISSUER_COOLDOWN"
                continue
            last_date = current
            if row["event_symbol"]:
                row["status"] = "VERIFIED_EVENT_EVENT_FILING_SYMBOL"
                row["verified_event"] = True


def _published(path: Path) -> None:
    relative = documents._repo_relative(path)
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
        raise Schedule13dSemanticCapacityError(
            f"classification input is not committed and pushed: {relative}"
        )


def _load_inspected_activation(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    activation = load_activation(path)
    expected = build_activation()
    status = _read_object(ACTIVATION_STATUS_PATH)
    for relative, digest in activation["implementation_hashes"].items():
        if sha256_file(PROJECT_ROOT / relative) != digest:
            raise Schedule13dSemanticCapacityError(
                f"semantic implementation drifted: {relative}"
            )
    if not (
        activation == expected
        and status.get("status") == "SEMANTIC_ACTIVATION_INSPECTED"
        and status.get("activation_sha256") == activation["activation_sha256"]
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
        and status.get("classification_permitted") is True
        and status.get("issuer_symbol_supplemental_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise Schedule13dSemanticCapacityError(
            "semantic activation is not independently inspected"
        )
    return activation, status


def _classification_state(activation: Mapping[str, Any]) -> dict[str, Any]:
    graph_path = _one(
        "strategy_tournament/v2/schedule13d/documents/manifests/"
        f"{capacity.CANDIDATE_ID}-*.json",
        "document request graph",
    )
    graph = documents.load_request_graph(graph_path)
    store = documents._store_config()
    records: list[dict[str, Any]] = []
    for request in graph["request_contract"]["requests"]:
        raw = documents._resolve_cache_path(store.root, request).read_bytes()
        records.append(_base_classification(request, raw))
    _apply_event_rules(records)
    counts = Counter(str(row["status"]) for row in records)
    verified = sum(bool(row["verified_event"]) for row in records)
    pending = counts["PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL"]
    state: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": documents.DATASET_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": documents.indexes.CONTRACT_SHA256,
        "activation_sha256": activation["activation_sha256"],
        "records": records,
        "attrition_counts": dict(sorted(counts.items())),
        "denominator": len(records),
        "verified_event_lower_bound": verified,
        "pending_causal_symbol_supplemental": pending,
        "minimum_required_verified_events": capacity.MINIMUM_VERIFIED_EVENTS,
        "minimum_capacity_proven_without_supplemental": (
            verified >= capacity.MINIMUM_VERIFIED_EVENTS
        ),
        "capacity_classification_complete": pending == 0,
        "capacity_passed": (
            verified >= capacity.MINIMUM_VERIFIED_EVENTS if pending == 0 else None
        ),
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    state["classification_sha256"] = capacity.successor._self_hash(
        state, "classification_sha256"
    )
    return state


def build_result(
    activation_path_value: Path,
    *,
    require_published: bool = True,
    write_private: bool = False,
) -> dict[str, Any]:
    activation, activation_inspection = _load_inspected_activation(
        activation_path_value
    )
    if require_published:
        _published(Path(__file__).resolve())
        _published(activation_path_value)
        _published(ACTIVATION_STATUS_PATH)
    state = _classification_state(activation)
    private_path = _private_result_path()
    if write_private:
        _write_gzip_json(state, private_path)
    if not private_path.is_file() or _read_gzip_object(private_path) != state:
        raise Schedule13dSemanticCapacityError(
            "private semantic classification is missing or differs"
        )
    complete = bool(state["capacity_classification_complete"])
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "outcome-blind-filing-semantic-capacity-result",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "dataset_id": documents.DATASET_ID,
        "contract_sha256": documents.indexes.CONTRACT_SHA256,
        "activation_sha256": activation["activation_sha256"],
        "activation_inspection_sha256": activation_inspection["inspection_sha256"],
        "classification_sha256": state["classification_sha256"],
        "private_result_file_sha256": sha256_file(private_path),
        "denominator": state["denominator"],
        "attrition_counts": state["attrition_counts"],
        "verified_event_lower_bound": state["verified_event_lower_bound"],
        "pending_causal_symbol_supplemental": state[
            "pending_causal_symbol_supplemental"
        ],
        "minimum_required_verified_events": capacity.MINIMUM_VERIFIED_EVENTS,
        "minimum_capacity_proven_without_supplemental": state[
            "minimum_capacity_proven_without_supplemental"
        ],
        "capacity_classification_complete": complete,
        "capacity_passed": state["capacity_passed"],
        "candidate_disposition": (
            "CAPACITY_CLASSIFICATION_COMPLETE"
            if complete
            else "CAUSAL_SYMBOL_SUPPLEMENTAL_FREEZE_REQUIRED"
        ),
        "issuer_symbol_supplemental_access_permitted": False,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "maturity_effect": "NONE",
    }
    result["result_sha256"] = capacity.successor._self_hash(
        result, "result_sha256"
    )
    return result


def result_path(value: Mapping[str, Any], root: Path = RESULT_ROOT) -> Path:
    return root / f"{capacity.CANDIDATE_ID}-{value['result_sha256']}.json"


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSemanticCapacityError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "classify", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze_activation()
            result: dict[str, Any] = {
                "written": documents._repo_relative(path),
                "activation_sha256": value["activation_sha256"],
                "classification_permitted": False,
                "verified_event_count": None,
                "market_outcomes_accessed": False,
            }
        elif args.command == "classify":
            activation = _one(
                "strategy_tournament/v2/schedule13d/semantic/activations/"
                f"{capacity.CANDIDATE_ID}-*.json",
                "semantic activation",
            )
            value = build_result(activation, write_private=True)
            path = result_path(value)
            _write_json(value, path)
            result = {**value, "written": documents._repo_relative(path)}
        else:
            result = _read_object(ACTIVATION_STATUS_PATH)
    except (
        Schedule13dSemanticCapacityError,
        documents.Schedule13dDocumentCollectionError,
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
