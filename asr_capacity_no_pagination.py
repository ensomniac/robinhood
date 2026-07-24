"""Freeze ASR v3 capacity recovery with no EFTS offset pagination."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asr_capacity as v1
import asr_capacity_recovery as v2


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = v1.CAMPAIGN_ID
THEME_ID = v1.THEME_ID
CANDIDATE_ID = "accelerated-share-repurchase-continuation-v3-no-pagination"
STRATEGY_VERSION = "2026-07-24-accelerated-share-repurchase-continuation-v3"
COLLECTION_START = v1.COLLECTION_START
COLLECTION_END = v1.COLLECTION_END
PAGE_SIZE = 100
MAXIMUM_SPLIT_DEPTH = 16
MAXIMUM_REQUEST_ATTEMPTS = 3
RETRYABLE_HTTP_STATUSES = [429, 500, 502, 503, 504]
V2_DISPOSITION_SHA256 = (
    "cb79a78c6ab8fed994a7dc4085590570ee3734dec23d1168cf655d5c50caad22"
)
V2_DISPOSITION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/asr/recovery/dispositions/"
    f"{v2.CANDIDATE_ID}-{V2_DISPOSITION_SHA256}.json"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/no-pagination/contracts"
DEFAULT_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/no-pagination/contract-status.json"
)


class AsrCapacityNoPaginationError(RuntimeError):
    """The ASR no-pagination contract or predecessor is invalid."""


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
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise AsrCapacityNoPaginationError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrCapacityNoPaginationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrCapacityNoPaginationError(f"{path} must contain an object")
    return value


def write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _predecessor() -> dict[str, Any]:
    disposition = read_object(V2_DISPOSITION_PATH)
    if not (
        disposition.get("disposition_sha256") == V2_DISPOSITION_SHA256
        and disposition.get("disposition_sha256")
        == self_hash(disposition, "disposition_sha256")
        and disposition.get("disposition")
        == "RETIRED_UNSTABLE_EFTS_PAGINATION"
        and disposition.get("same_contract_resume_or_repair_permitted") is False
        and disposition.get("separately_frozen_no_pagination_version_permitted")
        is True
        and disposition.get("verified_event_count") is None
        and disposition.get("market_outcomes_accessed") is False
        and disposition.get("broker_actions") == 0
    ):
        raise AsrCapacityNoPaginationError("ASR v2 retirement differs")
    return disposition


def build_contract() -> dict[str, Any]:
    predecessor = _predecessor()
    implementation_paths = (
        "asr_capacity_no_pagination.py",
        "asr_capacity_no_pagination_inspection.py",
        "asr_capacity_no_pagination_collection.py",
        "asr_capacity_no_pagination_collection_inspection.py",
        "asr_capacity_recovery_collection.py",
    )
    contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-asr-no-pagination-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "theme_id": THEME_ID,
        "candidate_id": CANDIDATE_ID,
        "strategy_version": STRATEGY_VERSION,
        "iso_week": "2026-W30",
        "family_budget_contract": {
            "mechanism_family_inherited_from": v1.CANDIDATE_ID,
            "inherited_new_mechanism_family_slot": 3,
            "new_mechanism_family_slot_consumed": False,
            "mechanism_rule_changed": False,
            "v1_or_v2_source_contract_reopened": False,
        },
        "preoutcome_authorization": {
            "source": "EXPLICIT_USER_DIRECTION_PLUS_AGENTIC_OWNERSHIP_MANDATE",
            "text_sha256": hashlib.sha256(v2.AUTHORIZATION_TEXT.encode()).hexdigest(),
            "scope": (
                "continue historical strategy discovery without calendar waiting "
                "while preserving weekly-family and outcome-integrity gates"
            ),
        },
        "preserved_predecessor": {
            "candidate_id": predecessor["candidate_id"],
            "strategy_version": predecessor["strategy_version"],
            "contract_sha256": predecessor["contract_sha256"],
            "contract_inspection_sha256": predecessor[
                "contract_inspection_sha256"
            ],
            "disposition_sha256": predecessor["disposition_sha256"],
            "disposition": predecessor["disposition"],
            "reported_exact_total": predecessor["reported_exact_total"],
            "duplicated_hit_id_count": predecessor["duplicated_hit_id_count"],
            "duplicate_excess_rows": predecessor["duplicate_excess_rows"],
            "remains_immutable_adverse_source_history": True,
            "same_contract_repair_permitted": False,
            "evidence_credit_inherited": False,
        },
        "mechanism": (
            "executed accelerated-repurchase agreements can create continuing "
            "dealer delivery, valuation-period, or settlement flow after disclosure"
        ),
        "source_contract": {
            "provider": "SEC_EDGAR_EFTS_AND_ARCHIVES",
            "search_endpoint": "https://efts.sec.gov/LATEST/search-index",
            "collection_start": COLLECTION_START,
            "collection_end_inclusive": COLLECTION_END,
            "search_phrases": list(v1.SEARCH_PHRASES),
            "page_size": PAGE_SIZE,
            "window_algorithm": {
                "initial_window": [COLLECTION_START, COLLECTION_END],
                "traversal": "depth_first_oldest_half_first",
                "split_when_total_relation_is": "gte",
                "split_when_exact_total_exceeds": PAGE_SIZE,
                "split_action": (
                    "bisect the inclusive calendar-date window at its deterministic "
                    "floor midpoint and recurse on the older half then newer half"
                ),
                "terminal_leaf": (
                    "offset zero only, relation eq, exact total at most 100, "
                    "returned rows equal exact total, and unique hit identities"
                ),
                "offset_pagination_permitted": False,
                "single_date_inexact_or_over_page_action": "fail_closed",
                "maximum_split_depth": MAXIMUM_SPLIT_DEPTH,
                "date_or_phrase_substitution_permitted": False,
            },
            "retry_contract": {
                "maximum_attempts_per_exact_request": MAXIMUM_REQUEST_ATTEMPTS,
                "retryable_http_statuses": RETRYABLE_HTTP_STATUSES,
                "backoff_seconds_by_retry": [1.0, 2.0],
                "non_http_transport_retry_permitted": True,
                "terminal_failure_action": "fail_closed_preserve_cache",
            },
            "matched_document_source": (
                "exact SEC Archives document and complete accession submission "
                "identified by each retained EFTS hit"
            ),
            "acceptance_time_source": "SEC complete-submission ACCEPTANCE-DATETIME",
            "shared_cache_reuse": "same contract, exact query, retained SHA-256 only",
            "sec_user_agent_required_from_private_configuration": True,
            "maximum_requests_per_second": 6,
            "provider_substitution_permitted": False,
        },
        "event_contract": v1.build_contract()["event_contract"],
        "denominator_contract": {
            "retain_every_parent_and_terminal_page": True,
            "retain_every_unique_terminal_hit": True,
            "retain_exact_hit_identity_and_source_fields_privately": True,
            "one_terminal_reason_required_for_every_unique_hit": True,
            "complete_query_and_semantic_attrition_required": True,
            "no_hit_event_date_or_source_substitution": True,
        },
        "capacity_gate": v1.build_contract()["capacity_gate"],
        "transition_contract": {
            "archive_document_access_requires_inspected_complete_denominator": True,
            "semantic_classification_requires_separately_frozen_exact_hit_manifest": True,
            "market_price_access_requires_inspected_capacity_at_least_100": True,
            "forward_return_access_before_exact_strategy_freeze": False,
        },
        "access_contract": {
            "sec_search_access_before_independent_inspection_permitted": False,
            "sec_search_access_after_independent_inspection_permitted": True,
            "matched_document_access_permitted_by_this_contract": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            path: file_hash(PROJECT_ROOT / path) for path in implementation_paths
        },
        "filing_hit_count": None,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "claim_limit": (
            "This contract repairs only source enumeration and contains no filing "
            "result, event count, price, return, outcome, or maturity evidence."
        ),
    }
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    return contract


def load_contract(path: Path) -> dict[str, Any]:
    value = read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == self_hash(value, "contract_sha256")
        and path.name == f"{CANDIDATE_ID}-{digest}.json"
    ):
        raise AsrCapacityNoPaginationError(
            "ASR no-pagination contract was mutated or renamed"
        )
    return value


def freeze_contract(
    *, output_root: Path = DEFAULT_ROOT, status_path: Path = DEFAULT_STATUS
) -> tuple[Path, dict[str, Any]]:
    contract = build_contract()
    path = output_root / f"{CANDIDATE_ID}-{contract['contract_sha256']}.json"
    if path.exists() and read_object(path) != contract:
        raise AsrCapacityNoPaginationError(
            "content-addressed no-pagination contract differs"
        )
    write_object(contract, path)
    write_object(
        {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "candidate_id": CANDIDATE_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "NO_PAGINATION_CONTRACT_PENDING_INSPECTION",
            "sec_search_access_permitted": False,
            "matched_document_access_permitted": False,
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
                "status": "NO_PAGINATION_CONTRACT_PENDING_INSPECTION",
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        else:
            result = read_object(DEFAULT_STATUS)
    except (AsrCapacityNoPaginationError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
