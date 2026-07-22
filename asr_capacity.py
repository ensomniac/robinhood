"""Freeze the outcome-blind accelerated-share-repurchase capacity contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import portfolio_successor_activation as successor


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = successor.CAMPAIGN_ID
THEME_ID = "accelerated-share-repurchase-continuation"
CANDIDATE_ID = "accelerated-share-repurchase-continuation-v1"
STRATEGY_VERSION = "2026-07-22-accelerated-share-repurchase-continuation-v1"
ISO_WEEK = "2026-W30"
FAMILY_SLOT = 3
COLLECTION_START = "2010-01-01"
COLLECTION_END = "2025-12-31"
MINIMUM_FORMAL_CAPACITY = 50
FAST_LANE_CAPACITY = 100
AUTHORIZATION_SHA256 = (
    "6a7e27481ff80cd3e457d972c10347ea831801d6532fdc34b9e9fb0453c0fd6e"
)
AUTHORIZATION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/successor_proposal/authorizations/"
    f"{CAMPAIGN_ID}-authorization-{AUTHORIZATION_SHA256}.json"
)
FAMILY_TWO_RETIREMENT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/validation/retirements/"
    "schedule-13d-activist-continuation-v1-development-retirement.json"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/manifests"
DEFAULT_STATUS = PROJECT_ROOT / "strategy_tournament/v2/asr/capacity-status.json"

SEARCH_PHRASES = (
    '"accelerated share repurchase"',
    '"accelerated stock repurchase"',
    '"accelerated repurchase agreement"',
    '"ASR agreement"',
)
ASR_PHRASE_PATTERNS = (
    r"\baccelerated\s+(?:share|stock)\s+repurchase\b",
    r"\baccelerated\s+repurchase\s+agreement\b",
    r"\bASR\s+agreement\b",
)
EXECUTION_PATTERNS = (
    r"\b(?:entered|entering)\s+into\b.{0,400}\b(?:accelerated|ASR)\b",
    r"\b(?:executed|commenced)\b.{0,400}\b(?:accelerated|ASR)\b",
    r"\b(?:accelerated|ASR)\b.{0,400}\b(?:executed|commenced)\b",
)
COMMITTED_NOTIONAL_PATTERNS = (
    r"\$\s?\d[\d,.]*(?:\s?(?:million|billion))?\b.{0,160}\b(?:accelerated|ASR|agreement)\b",
    r"\b(?:accelerated|ASR|agreement)\b.{0,160}\$\s?\d[\d,.]*(?:\s?(?:million|billion))?\b",
)
CONTINUING_MECHANICS_PATTERNS = (
    r"\binitial\s+(?:delivery|deliver)\b",
    r"\bfinal\s+(?:delivery|settlement)\b",
    r"\bvaluation\s+period\b",
    r"\bremaining\s+(?:shares|delivery)\b",
    r"\bsettlement\b.{0,240}\b(?:shares|cash|delivery|VWAP|average)\b",
)
GENERIC_ONLY_PATTERNS = (
    r"\bboard\s+(?:has\s+)?authorized\b",
    r"\bmay\s+repurchase\b",
    r"\bopen[- ]market\s+(?:repurchase|program)\b",
    r"\bintends?\s+to\s+repurchase\b",
)


class AsrCapacityError(RuntimeError):
    """The ASR capacity contract or its authority chain is invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrCapacityError(f"{path} must contain an object")
    return value


def _write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        _canonical_bytes({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _authority() -> tuple[dict[str, Any], dict[str, Any]]:
    authorization = successor.load_authorization(AUTHORIZATION_PATH)
    retirement = _read_object(FAMILY_TWO_RETIREMENT_PATH)
    if not (
        authorization.get("authorization_sha256") == AUTHORIZATION_SHA256
        and authorization.get("scope", {}).get(
            "maximum_new_mechanism_families_per_iso_week"
        )
        == 3
        and retirement.get("candidate_id")
        == "schedule-13d-activist-continuation-v1"
        and retirement.get("disposition") == "RETIRED_DEVELOPMENT"
        and retirement.get("confirmation_outcomes_accessed") is False
        and retirement.get("parameter_repair_on_evaluation_corpus_permitted")
        is False
        and retirement.get("broker_actions") == 0
    ):
        raise AsrCapacityError("v2 authorization or family-two retirement is invalid")
    return authorization, retirement


def _pattern_contract() -> dict[str, Any]:
    groups = {
        "asr_phrase": ASR_PHRASE_PATTERNS,
        "executed_agreement": EXECUTION_PATTERNS,
        "committed_notional": COMMITTED_NOTIONAL_PATTERNS,
        "continuing_delivery_or_settlement": CONTINUING_MECHANICS_PATTERNS,
        "generic_only_exclusions": GENERIC_ONLY_PATTERNS,
    }
    for values in groups.values():
        for pattern in values:
            re.compile(pattern, re.IGNORECASE | re.DOTALL)
    return {key: list(value) for key, value in groups.items()}


def build_contract() -> dict[str, Any]:
    """Build the exact zero-outcome SEC search and semantic capacity contract."""
    authorization, retirement = _authority()
    authority_paths = (
        "PORTFOLIO_VALIDATION_V2.md",
        "PORTFOLIO_THESIS_V2.md",
        "portfolio_config.toml",
        str(AUTHORIZATION_PATH.relative_to(PROJECT_ROOT)),
        str(FAMILY_TWO_RETIREMENT_PATH.relative_to(PROJECT_ROOT)),
    )
    contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-asr-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "authorization_sha256": authorization["authorization_sha256"],
        "family_two_retirement_inspection_sha256": retirement[
            "development_inspection_sha256"
        ],
        "iso_week": ISO_WEEK,
        "new_mechanism_family_slot": FAMILY_SLOT,
        "maximum_new_mechanism_families_this_iso_week": 3,
        "theme_id": THEME_ID,
        "candidate_id": CANDIDATE_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism": (
            "executed accelerated-repurchase agreements can create continuing "
            "dealer delivery, valuation-period, or settlement flow after disclosure"
        ),
        "distinctness_contract": {
            "executed_corporate_dealer_agreement": True,
            "price_only_signal": False,
            "ordinary_repurchase_authorization": False,
            "activist_ownership_event": False,
            "failed_family_rules_reused_or_repaired": False,
        },
        "source_contract": {
            "provider": "SEC_EDGAR_EFTS_AND_ARCHIVES",
            "collection_start": COLLECTION_START,
            "collection_end_inclusive": COLLECTION_END,
            "search_endpoint": "https://efts.sec.gov/LATEST/search-index",
            "search_phrases": list(SEARCH_PHRASES),
            "forms": "all SEC filing forms",
            "date_range_mode": "custom",
            "page_size": 100,
            "pagination": (
                "for each phrase request from=0,100,... until the exact reported "
                "total is exhausted; fail if total is inexact or changes on resume"
            ),
            "matched_document_source": (
                "exact SEC Archives document identified by each EFTS hit"
            ),
            "complete_submission_source": (
                "exact accession .txt submission derived from the same EFTS hit"
            ),
            "acceptance_time_source": "SEC complete-submission ACCEPTANCE-DATETIME",
            "provider_substitution_permitted": False,
            "shared_cache_reuse": "exact URL and retained SHA-256 only",
            "sec_user_agent_required_from_private_configuration": True,
            "maximum_requests_per_second": 6,
        },
        "event_contract": {
            "required_evidence": [
                "executed accelerated repurchase agreement",
                "committed dollar notional",
                "point-in-time SEC acceptance timestamp",
                "post-disclosure delivery valuation or settlement mechanics",
            ],
            "pattern_contract": _pattern_contract(),
            "all_required_positive_groups_must_match": True,
            "generic_only_match": "ineligible_preserve_denominator",
            "board_authorization_only": "ineligible_preserve_denominator",
            "ordinary_open_market_program": "ineligible_preserve_denominator",
            "generic_repurchase_intent": "ineligible_preserve_denominator",
            "deduplication_key": (
                "issuer CIK plus normalized agreement date plus committed notional; "
                "retain first public SEC acceptance then lexical accession"
            ),
            "unresolved_agreement_date": "manual-verification-required-not-verified",
            "unresolved_notional": "ineligible_preserve_denominator",
            "unresolved_mechanics": "ineligible_preserve_denominator",
            "long_common_equity_or_etf_only": True,
        },
        "denominator_contract": {
            "retain_every_unique_efts_hit": True,
            "retain_every_matched_document_attempt": True,
            "terminal_reason_required_for_every_hit": True,
            "complete_query_and_semantic_attrition_required": True,
            "no_hit_or_event_substitution": True,
        },
        "capacity_gate": {
            "retire_below_verified_events": MINIMUM_FORMAL_CAPACITY,
            "preserve_later_single_rule_minimum": MINIMUM_FORMAL_CAPACITY,
            "fast_lane_minimum_verified_events": FAST_LANE_CAPACITY,
            "below_50_disposition": "RETIRED_INSUFFICIENT_FORMAL_CAPACITY",
            "50_through_99_disposition": "PRESERVED_LATER_SINGLE_RULE",
            "100_plus_disposition": "CAPACITY_READY",
        },
        "access_contract": {
            "sec_access_before_contract_inspection_permitted": False,
            "sec_search_and_exact_matched_document_access_after_inspection_permitted": True,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "authority_hashes": {
            path: _file_hash(PROJECT_ROOT / path) for path in authority_paths
        },
        "filing_hit_count": None,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "claim_limit": (
            "This artifact freezes capacity collection only. It contains no filing "
            "hit count, event count, price, return, strategy outcome, or maturity evidence."
        ),
    }
    contract["contract_sha256"] = _self_hash(contract, "contract_sha256")
    return contract


def load_contract(path: Path) -> dict[str, Any]:
    value = _read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == _self_hash(value, "contract_sha256")
        and path.name == f"{CANDIDATE_ID}-{digest}.json"
    ):
        raise AsrCapacityError("ASR capacity contract was mutated or renamed")
    return value


def freeze_contract(
    *, output_root: Path = DEFAULT_ROOT, status_path: Path = DEFAULT_STATUS
) -> tuple[Path, dict[str, Any]]:
    contract = build_contract()
    path = output_root / f"{CANDIDATE_ID}-{contract['contract_sha256']}.json"
    if path.exists() and _read_object(path) != contract:
        raise AsrCapacityError("content-addressed ASR contract has other content")
    _write_object(contract, path)
    _write_object(
        {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "candidate_id": CANDIDATE_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "CAPACITY_CONTRACT_PENDING_INSPECTION",
            "sec_source_access_permitted": False,
            "capacity_classification_permitted": False,
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
                "status": "CAPACITY_CONTRACT_PENDING_INSPECTION",
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        else:
            result = _read_object(DEFAULT_STATUS)
    except (AsrCapacityError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
