"""Resolve point-in-time common-equity identity for precise ASR events."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import os
import re
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asr_tier2a_submission_resolution as resolution


PROJECT_ROOT = Path(__file__).resolve().parent
IDENTITY_ID = "filing-cover-common-equity-identity-v1"
PRECISE_RESULT_SHA256 = (
    "3663d111fae4ed6d1d142650983db4fae0293365b34a460634b4b36050514ec4"
)
PRECISE_RESULT_PATH = (
    resolution.DEFAULT_RESULT_ROOT
    / f"{resolution.tier2a.capacity.CANDIDATE_ID}-{PRECISE_RESULT_SHA256}.json"
)
PRECISE_INSPECTION_SHA256 = (
    "e1de9acd4d97037a9ffc8c7ce4b82bf631b06fc3b90c4d8b2c6e7adc4f0414ba"
)
PRECISE_INSPECTION_PATH = (
    resolution.DEFAULT_RESULT_ROOT
    / "inspections"
    / f"precise-{PRECISE_INSPECTION_SHA256}.json"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/security-identity"
DEFAULT_CONTRACT_ROOT = DEFAULT_ROOT / "contracts"
DEFAULT_CONTRACT_STATUS = DEFAULT_ROOT / "contract-status.json"
DEFAULT_RESULT_ROOT = DEFAULT_ROOT / "results"
PRIVATE_RESULT_NAMESPACE = "_derived/asr-security-identity-result"
DOCUMENT_PATTERN = re.compile(rb"<DOCUMENT>(.*?)</DOCUMENT>", re.IGNORECASE | re.DOTALL)
TYPE_PATTERN = re.compile(rb"<TYPE>\s*([^\r\n<]+)", re.IGNORECASE)
ROW_PATTERN = re.compile(r"<tr\b[^>]*>(.*?)</tr\s*>", re.IGNORECASE | re.DOTALL)
CELL_PATTERN = re.compile(r"<t[dh]\b[^>]*>(.*?)</t[dh]\s*>", re.IGNORECASE | re.DOTALL)
TAG_PATTERN = re.compile(r"<[^>]+>", re.DOTALL)
SPACE_PATTERN = re.compile(r"\s+")
COMMON_TITLE_PATTERN = re.compile(r"\bcommon (?:stock|shares)\b", re.IGNORECASE)
DISALLOWED_TITLE_PATTERN = re.compile(
    r"\b(?:preferred|preference|depositary|warrant|right|unit|note|bond|debt)\b",
    re.IGNORECASE,
)
TICKER_PATTERN = re.compile(r"[A-Z][A-Z0-9.-]{0,9}")
EXCHANGE_PATTERNS = (
    ("NASDAQ", re.compile(r"\bNASDAQ\b", re.IGNORECASE)),
    (
        "NYSE_AMERICAN",
        re.compile(r"\b(?:NYSE\s+AMERICAN|NYSE\s+MKT)\b", re.IGNORECASE),
    ),
    (
        "NYSE",
        re.compile(r"\b(?:NYSE|NEW YORK STOCK EXCHANGE)\b", re.IGNORECASE),
    ),
    ("CBOE_BZX", re.compile(r"\bCBOE\s+BZX\b", re.IGNORECASE)),
)
FAST_LANE_THRESHOLD = 100


class AsrSecurityIdentityError(RuntimeError):
    """The ASR security-identity contract or result is invalid."""


def canonical_bytes(value: Any) -> bytes:
    return resolution.canonical_bytes(value)


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return resolution.self_hash(value, field)


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrSecurityIdentityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrSecurityIdentityError(f"{path} must contain an object")
    return value


def write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_gzip(value: Mapping[str, Any], path: Path) -> bytes:
    raw = gzip.compress(canonical_bytes(value), mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return raw


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as exc:
        raise AsrSecurityIdentityError(
            f"cannot read private identity artifact {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise AsrSecurityIdentityError(
            "private identity artifact must contain an object"
        )
    return value


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _lineage(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    result = read_object(PRECISE_RESULT_PATH)
    inspection = read_object(PRECISE_INSPECTION_PATH)
    collection_status, collection = resolution.load_private_collection(root)
    info = result["private_result"]
    private_path = root / str(info["cache_relative_path"])
    compressed = private_path.read_bytes() if private_path.is_file() else b""
    private = _read_gzip(private_path)
    if not (
        result.get("result_sha256") == PRECISE_RESULT_SHA256
        and result.get("result_sha256") == self_hash(result, "result_sha256")
        and result.get("precise_semantic_event_count") == 507
        and result.get("security_identity_resolution_complete") is False
        and result.get("market_outcomes_accessed") is False
        and inspection.get("inspection_sha256") == PRECISE_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == self_hash(inspection, "inspection_sha256")
        and inspection.get("security_identity_manifest_freeze_permitted") is True
        and inspection.get("security_identity_access_permitted") is False
        and inspection.get("market_outcomes_accessed") is False
        and hashlib.sha256(compressed).hexdigest() == info["file_sha256"]
        and len(compressed) == info["bytes"]
        and private.get("private_result_sha256") == info["private_result_sha256"]
        and private.get("private_result_sha256")
        == self_hash(private, "private_result_sha256")
        and len(private.get("events", [])) == 507
        and collection_status.get("collection_sha256") == result["collection_sha256"]
        and collection.get("market_outcomes_accessed") is False
    ):
        raise AsrSecurityIdentityError("precise semantic or submission lineage differs")
    return result, inspection, private, collection


def _normalize_cell(value: str) -> str:
    return SPACE_PATTERN.sub(" ", html.unescape(TAG_PATTERN.sub(" ", value))).strip()


def _filing_document(raw: bytes) -> bytes | None:
    for match in DOCUMENT_PATTERN.finditer(raw):
        document = match.group(1)
        type_match = TYPE_PATTERN.search(document[:4096])
        if type_match is None:
            continue
        form = type_match.group(1).strip().upper()
        if form in {b"8-K", b"6-K"}:
            return document
    return None


def _canonical_exchange(value: str) -> str | None:
    for canonical, pattern in EXCHANGE_PATTERNS:
        if pattern.search(value):
            return canonical
    return None


def classify_security_identity(raw: bytes) -> tuple[list[dict[str, str]], str]:
    """Return one or more eligible cover-table identities before ambiguity gating."""
    document = _filing_document(raw)
    if document is None:
        return [], "NO_PRIMARY_CURRENT_REPORT_DOCUMENT"
    text = document.decode("utf-8", errors="replace")
    candidates: dict[tuple[str, str, str], dict[str, str]] = {}
    saw_common_title = False
    saw_disallowed_listing = False
    for row_match in ROW_PATTERN.finditer(text):
        cells = [
            _normalize_cell(cell) for cell in CELL_PATTERN.findall(row_match.group(1))
        ]
        cells = [cell for cell in cells if cell]
        if len(cells) < 3:
            continue
        title, ticker_value, exchange_value = cells[:3]
        if COMMON_TITLE_PATTERN.search(title) is None:
            continue
        saw_common_title = True
        if DISALLOWED_TITLE_PATTERN.search(title):
            saw_disallowed_listing = True
            continue
        ticker = ticker_value.strip().upper()
        exchange = _canonical_exchange(exchange_value)
        if TICKER_PATTERN.fullmatch(ticker) is None or exchange is None:
            saw_disallowed_listing = True
            continue
        identity = {
            "security_title": title,
            "ticker": ticker,
            "exchange": exchange,
        }
        candidates[(title.casefold(), ticker, exchange)] = identity
    identities = [candidates[key] for key in sorted(candidates)]
    if len(identities) == 1:
        return identities, "QUALIFIED_UNAMBIGUOUS_COMMON_EQUITY"
    if len(identities) > 1:
        return identities, "AMBIGUOUS_MULTIPLE_COMMON_EQUITY_IDENTITIES"
    if saw_disallowed_listing:
        return [], "COMMON_TITLE_WITHOUT_ELIGIBLE_US_LISTING"
    if saw_common_title:
        return [], "COMMON_TITLE_UNRESOLVED"
    return [], "NO_COMMON_EQUITY_COVER_ROW"


def build_contract(*, store_root: Path | None = None) -> dict[str, Any]:
    root = store_root or resolution.tier1.shared._store().root
    result, inspection, private, collection = _lineage(root)
    implementation_paths = (
        "asr_security_identity.py",
        "asr_security_identity_inspection.py",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-security-identity-contract",
        "campaign_id": resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": resolution.tier2a.capacity.CANDIDATE_ID,
        "strategy_version": resolution.tier2a.capacity.STRATEGY_VERSION,
        "identity_id": IDENTITY_ID,
        "source_lineage": {
            "precise_result_sha256": result["result_sha256"],
            "precise_inspection_sha256": inspection["inspection_sha256"],
            "private_precise_result_sha256": private["private_result_sha256"],
            "submission_collection_sha256": result["collection_sha256"],
            "inspected_submission_count": collection["success_count"],
            "source_failure_count": collection["failure_count"],
            "precise_semantic_event_count": 507,
        },
        "identity_contract": {
            "source": "same accepted complete submission cover table",
            "primary_document_types": ["6-K", "8-K"],
            "required_title_pattern": COMMON_TITLE_PATTERN.pattern,
            "disallowed_title_pattern": DISALLOWED_TITLE_PATTERN.pattern,
            "ticker_pattern": TICKER_PATTERN.pattern,
            "eligible_exchanges": [
                canonical for canonical, _pattern in EXCHANGE_PATTERNS
            ],
            "exactly_one_eligible_identity_required_per_accession": True,
            "multiple_eligible_common_classes": "ambiguous_zero_credit",
            "missing_or_unparseable_cover_row": "unresolved_zero_credit",
            "identity_timestamp": "complete submission acceptance datetime",
            "external_identity_substitution_permitted": False,
        },
        "capacity_contract": {
            "formal_verified_event_key": (
                "issuer CIK, agreement date, committed dollars, ticker, exchange"
            ),
            "deduplication_winner": (
                "earliest exact acceptance datetime then lexical accession"
            ),
            "fast_lane_threshold": FAST_LANE_THRESHOLD,
            "below_50": "RETIRED_INSUFFICIENT_FORMAL_CAPACITY",
            "from_50_through_99": "PRESERVED_LATER_SINGLE_RULE_RESEARCH",
            "at_least_100": "ADMITTED_TO_DEVELOPMENT_SEARCH_PIPELINE",
        },
        "access_contract": {
            "local_identity_access_before_independent_inspection_permitted": False,
            "local_identity_access_after_independent_inspection_permitted": True,
            "new_provider_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            relative: file_hash(PROJECT_ROOT / relative)
            for relative in implementation_paths
        },
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    return contract


def _contract_path() -> Path:
    matches = sorted(DEFAULT_CONTRACT_ROOT.glob("*.json"))
    if len(matches) != 1:
        raise AsrSecurityIdentityError(
            "expected exactly one security-identity contract"
        )
    return matches[0]


def load_contract(path: Path) -> dict[str, Any]:
    value = read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == self_hash(value, "contract_sha256")
        and path.name == f"{resolution.tier2a.capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise AsrSecurityIdentityError(
            "security-identity contract was mutated or renamed"
        )
    return value


def freeze(
    *,
    output_root: Path = DEFAULT_CONTRACT_ROOT,
    status_path: Path = DEFAULT_CONTRACT_STATUS,
) -> tuple[Path, dict[str, Any]]:
    contract = build_contract()
    path = output_root / (
        f"{resolution.tier2a.capacity.CANDIDATE_ID}-{contract['contract_sha256']}.json"
    )
    write_object(contract, path)
    write_object(
        {
            "schema_version": 1,
            "campaign_id": resolution.tier2a.capacity.CAMPAIGN_ID,
            "candidate_id": resolution.tier2a.capacity.CANDIDATE_ID,
            "identity_id": IDENTITY_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "SECURITY_IDENTITY_CONTRACT_PENDING_INSPECTION",
            "local_identity_access_permitted": False,
            "provider_access_permitted": False,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, contract


def _load_authority(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    contract = load_contract(_contract_path())
    status = read_object(DEFAULT_CONTRACT_STATUS)
    _result, _inspection, private, collection = _lineage(root)
    if not (
        status.get("status") == "SECURITY_IDENTITY_CONTRACT_INSPECTED"
        and status.get("contract_sha256") == contract["contract_sha256"]
        and status.get("inspection_sha256") == self_hash(status, "inspection_sha256")
        and status.get("local_identity_access_permitted") is True
        and status.get("provider_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise AsrSecurityIdentityError(
            "security-identity contract is not independently inspected"
        )
    return contract, status, private, collection


def rebuild_result(
    private: Mapping[str, Any],
    collection: Mapping[str, Any],
    root: Path,
) -> dict[str, Any]:
    records = {str(record["accession"]): record for record in collection["records"]}
    identity_by_accession: dict[str, dict[str, str]] = {}
    accession_reasons: dict[str, str] = {}
    for accession in sorted({str(event["accession"]) for event in private["events"]}):
        record = records.get(accession)
        if record is None:
            accession_reasons[accession] = "SOURCE_FAILURE_UNRESOLVED_ZERO_CREDIT"
            continue
        raw = (root / str(record["source_cache_relative_path"])).read_bytes()
        if (
            hashlib.sha256(raw).hexdigest() != record["source_sha256"]
            or len(raw) != record["source_bytes"]
        ):
            raise AsrSecurityIdentityError(
                "submission drifted during security identity classification"
            )
        identities, reason = classify_security_identity(raw)
        accession_reasons[accession] = reason
        if reason == "QUALIFIED_UNAMBIGUOUS_COMMON_EQUITY":
            identity_by_accession[accession] = identities[0]
    verified: dict[tuple[str, str, int, str, str], dict[str, Any]] = {}
    event_reasons: Counter[str] = Counter()
    for event in private["events"]:
        accession = str(event["accession"])
        identity = identity_by_accession.get(accession)
        if identity is None:
            event_reasons[accession_reasons[accession]] += 1
            continue
        combined = {
            **event,
            **identity,
            "identity_timestamp_raw": event["acceptance_datetime_raw"],
        }
        key = (
            str(event["issuer_cik"]),
            str(event["agreement_date"]),
            int(event["committed_notional_dollars"]),
            identity["ticker"],
            identity["exchange"],
        )
        if key not in verified or (
            str(combined["acceptance_datetime_raw"]),
            str(combined["accession"]),
        ) < (
            str(verified[key]["acceptance_datetime_raw"]),
            str(verified[key]["accession"]),
        ):
            verified[key] = combined
        event_reasons["QUALIFIED_VERIFIED_COMMON_EQUITY_EVENT"] += 1
    verified_events = [verified[key] for key in sorted(verified)]
    verified_count = len(verified_events)
    if verified_count < 50:
        disposition = "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"
    elif verified_count < FAST_LANE_THRESHOLD:
        disposition = "PRESERVED_LATER_SINGLE_RULE_RESEARCH"
    else:
        disposition = "ADMITTED_TO_DEVELOPMENT_SEARCH_PIPELINE"
    result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-security-identity-result",
        "accession_terminal_counts": dict(
            sorted(Counter(accession_reasons.values()).items())
        ),
        "event_terminal_counts": dict(sorted(event_reasons.items())),
        "verified_events": verified_events,
        "verified_event_count": verified_count,
        "capacity_disposition": disposition,
        "security_identity_resolution_complete": True,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    result["private_result_sha256"] = self_hash(result, "private_result_sha256")
    return result


def evaluate(
    *,
    result_root: Path = DEFAULT_RESULT_ROOT,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = store_root or resolution.tier1.shared._store().root
    contract, inspection, private, collection = _load_authority(root)
    private_result = rebuild_result(private, collection, root)
    private_path = (
        root
        / PRIVATE_RESULT_NAMESPACE
        / f"{private_result['private_result_sha256']}.json.gz"
    )
    compressed = _write_gzip(private_result, private_path)
    result: dict[str, Any] = {
        "schema_version": 1,
        "result_kind": "outcome-blind-asr-security-identity-result",
        "campaign_id": resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": resolution.tier2a.capacity.CANDIDATE_ID,
        "identity_id": IDENTITY_ID,
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "precise_semantic_event_count": 507,
        "accession_terminal_counts": private_result["accession_terminal_counts"],
        "event_terminal_counts": private_result["event_terminal_counts"],
        "verified_event_count": private_result["verified_event_count"],
        "capacity_disposition": private_result["capacity_disposition"],
        "security_identity_resolution_complete": True,
        "private_result": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_result_sha256": private_result["private_result_sha256"],
            "file_sha256": hashlib.sha256(compressed).hexdigest(),
            "bytes": len(compressed),
        },
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "state": "SECURITY_IDENTITY_RESULT_PENDING_INSPECTION",
        "valid": True,
    }
    result["result_sha256"] = self_hash(result, "result_sha256")
    path = result_root / (
        f"{resolution.tier2a.capacity.CANDIDATE_ID}-{result['result_sha256']}.json"
    )
    write_object(result, path)
    return path, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "evaluate"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": value["contract_sha256"],
                "local_identity_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        else:
            path, value = evaluate()
            result = {**value, "written": str(path.relative_to(PROJECT_ROOT))}
    except (AsrSecurityIdentityError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
