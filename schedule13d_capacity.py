"""Freeze the v2 Schedule 13D outcome-blind capacity contract."""

from __future__ import annotations

import argparse
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
AUTHORIZATION_SHA256 = (
    "6a7e27481ff80cd3e457d972c10347ea831801d6532fdc34b9e9fb0453c0fd6e"
)
AUTHORIZATION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/successor_proposal/authorizations/"
    f"{CAMPAIGN_ID}-authorization-{AUTHORIZATION_SHA256}.json"
)
ACTIVATION_STATUS_PATH = successor.DEFAULT_STATUS
FAMILY_ONE_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/inspections/"
    "multi-asset-etf-tsmom-v1-result-"
    "3cabba283e1b9a94cae818c3c57d6c216ad42ecd57e289d4468ea220bd2319ee.json"
)
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_capacity_inspection.py"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/manifests"
DEFAULT_STATUS = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/capacity-status.json"

THEME_ID = "schedule-13d-activist-continuation"
CANDIDATE_ID = "schedule-13d-activist-continuation-v1"
STRATEGY_VERSION = "2026-07-22-schedule-13d-activist-continuation-v1"
ISO_WEEK = "2026-W30"
FAMILY_SLOT = 2
COLLECTION_START = "2022-01-03"
COLLECTION_END = "2025-12-19"
ISSUER_COOLDOWN_CALENDAR_DAYS = 63
MINIMUM_STAGE0_SIGNALS = 30
MINIMUM_MATURITY_SIGNALS = 50
MINIMUM_VERIFIED_EVENTS = MINIMUM_STAGE0_SIGNALS + MINIMUM_MATURITY_SIGNALS

ITEM4_ACTOR_INTENT_PATTERNS = (
    r"reporting\s+persons?.{0,240}\b(?:intend|plan|seek|propose|expect|will|has\s+nominated|have\s+nominated)\b",
    r"\b(?:intend|plan|seek|propose)\b.{0,240}\b(?:issuer|company|board|management|stockholders?)\b",
)
ITEM4_CONTROL_CATEGORY_PATTERNS = {
    "board_representation": (
        r"\bboard\s+representation\b",
        r"\bseat(?:s)?\s+on\s+the\s+board\b",
        r"\bnomina(?:te|ted|ting|tion).{0,120}\bdirectors?\b",
    ),
    "extraordinary_transaction": (
        r"\bmerger\b",
        r"\bbusiness\s+combination\b",
        r"\brecapitalization\b",
        r"\brestructuring\b",
        r"\bstrategic\s+alternatives\b",
        r"\bsale\s+of\s+(?:the\s+)?(?:issuer|company)\b",
    ),
    "management_or_governance_change": (
        r"\bchange.{0,80}\bmanagement\b",
        r"\bchange.{0,80}\bboard\b",
        r"\bcorporate\s+governance\b",
        r"\bcapital\s+allocation\b",
    ),
    "proxy_or_holder_campaign": (
        r"\bsolicit.{0,80}\bprox(?:y|ies)\b",
        r"\bproxy\s+solicitation\b",
        r"\bcommunicat.{0,120}\b(?:stockholders?|shareholders?)\b",
    ),
}
COMMON_SHARE_CLASS_PATTERNS = (
    r"\bcommon\s+(?:stock|shares?)\b",
    r"\bordinary\s+shares?\b",
)
EXCLUDED_CLASS_PATTERNS = (
    r"\bpreferred\b",
    r"\bwarrants?\b",
    r"\boptions?\b",
    r"\bnotes?\b",
    r"\bdebentures?\b",
    r"\bpartnership\s+units?\b",
)


class Schedule13dCapacityError(RuntimeError):
    """The Schedule 13D capacity contract is missing or inconsistent."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dCapacityError(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _assert_campaign_and_family_one() -> tuple[dict[str, Any], dict[str, Any]]:
    authorization = successor.load_authorization(AUTHORIZATION_PATH)
    status = _read_json(ACTIVATION_STATUS_PATH)
    family_one = _read_json(FAMILY_ONE_INSPECTION_PATH)
    if not (
        authorization.get("authorization_sha256") == AUTHORIZATION_SHA256
        and authorization.get("scope", {}).get(
            "maximum_new_mechanism_families_per_iso_week"
        )
        == 3
        and status.get("status") == "AUTHORIZED_READY"
        and status.get("candidate_preregistration_permitted") is True
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and family_one.get("inspection_sha256")
        == "3cabba283e1b9a94cae818c3c57d6c216ad42ecd57e289d4468ea220bd2319ee"
        and family_one.get("stage0_survived") is False
        and family_one.get("stage0_blockers")
        == ["20 bps-per-side total R is not positive"]
        and family_one.get("maturity_effect") == "NONE"
        and family_one.get("valid") is True
    ):
        raise Schedule13dCapacityError(
            "v2 authorization or family-one retirement is invalid"
        )
    return authorization, family_one


def _pattern_contract() -> dict[str, Any]:
    for pattern in (
        *ITEM4_ACTOR_INTENT_PATTERNS,
        *[value for values in ITEM4_CONTROL_CATEGORY_PATTERNS.values() for value in values],
        *COMMON_SHARE_CLASS_PATTERNS,
        *EXCLUDED_CLASS_PATTERNS,
    ):
        re.compile(pattern, re.IGNORECASE | re.DOTALL)
    return {
        "actor_intent_patterns": list(ITEM4_ACTOR_INTENT_PATTERNS),
        "control_category_patterns": {
            key: list(value) for key, value in ITEM4_CONTROL_CATEGORY_PATTERNS.items()
        },
        "common_share_class_patterns": list(COMMON_SHARE_CLASS_PATTERNS),
        "excluded_class_patterns": list(EXCLUDED_CLASS_PATTERNS),
        "flags": ["IGNORECASE", "DOTALL"],
    }


def build_contract() -> dict[str, Any]:
    """Build the exact filing-capacity contract without reading filing counts."""

    authorization, family_one = _assert_campaign_and_family_one()
    bound_files = (
        Path(__file__).resolve(),
        INSPECTOR_PATH,
        AUTHORIZATION_PATH,
        FAMILY_ONE_INSPECTION_PATH,
        PROJECT_ROOT / "PORTFOLIO_VALIDATION_V2.md",
        PROJECT_ROOT / "portfolio_config.toml",
        PROJECT_ROOT / "portfolio_maturity.py",
    )
    contract: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "authorization_sha256": authorization["authorization_sha256"],
        "family_one_retirement_inspection_sha256": family_one[
            "inspection_sha256"
        ],
        "objective": authorization["objective"],
        "iso_week": ISO_WEEK,
        "new_mechanism_family_slot": FAMILY_SLOT,
        "maximum_new_mechanism_families_this_iso_week": 3,
        "theme_id": THEME_ID,
        "candidate_id": CANDIDATE_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": THEME_ID,
        "mechanism": (
            "delayed price discovery after the first public initial Schedule 13D "
            "with an affirmative control-intent Item 4"
        ),
        "distinctness_contract": {
            "ownership_control_event": True,
            "price_only_signal": False,
            "earnings_or_generic_news_event": False,
            "opening_range_or_pullback_signal": False,
            "failed_family_one_rules_reused_or_repaired": False,
        },
        "source_contract": {
            "provider": "SEC_EDGAR",
            "collection_start": COLLECTION_START,
            "collection_end_inclusive": COLLECTION_END,
            "index_source": (
                "https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.idx"
            ),
            "filing_source": (
                "https://www.sec.gov/Archives/edgar/data/{cik}/{accession-path}"
            ),
            "forms_included": ["SC 13D"],
            "forms_excluded": ["SC 13D/A", "SC 13G", "SC 13G/A"],
            "acceptance_time_source": "SEC complete-submission ACCEPTANCE-DATETIME",
            "primary_document_selection": (
                "first accession-bound document whose normalized TYPE is SC 13D"
            ),
            "point_in_time_only": True,
            "provider_substitution_permitted": False,
            "shared_cache_reuse": (
                "permitted only for an exact SEC URL with retained content hash"
            ),
            "maximum_requests_per_second": 8,
            "sec_user_agent_required_from_private_configuration": True,
        },
        "document_contract": {
            "item4_start": "normalized Item 4 heading in the selected SC 13D document",
            "item4_end": "next normalized Item 5 heading",
            "missing_or_ambiguous_item4": "ineligible_preserve_denominator",
            "structured_and_html_text_normalization": (
                "decode entities, remove tags, collapse whitespace, preserve headings"
            ),
            "control_intent_match": (
                "at least one actor-intent pattern and one control-category pattern "
                "must match within the same normalized Item 4 paragraph"
            ),
            "boilerplate_only": "ineligible_preserve_denominator",
            "pattern_contract": _pattern_contract(),
        },
        "security_contract": {
            "eligible_class": (
                "title of class matches a common-share pattern and no excluded-class pattern"
            ),
            "symbol_source_order": [
                "issuerTradingSymbol in the event filing",
                "Trading Symbol in the event filing",
                "dei:TradingSymbol in the latest issuer 10-K, 10-Q, 8-K, 20-F, or 6-K accepted no later than the event",
            ],
            "current_or_future_symbol_mapping_permitted": False,
            "missing_causal_symbol": "ineligible_preserve_denominator",
            "symbol_normalization": "uppercase SEC-reported symbol; no issuer-name inference",
            "long_common_equity_only": True,
        },
        "event_contract": {
            "event_identity": "target issuer CIK plus accession number",
            "initial_schedule_only": True,
            "acceptance_cutoff": "event accepted within the frozen date window",
            "same_issuer_same_acceptance_date": (
                "retain earliest acceptance timestamp then lexical accession"
            ),
            "issuer_cooldown_calendar_days": ISSUER_COOLDOWN_CALENDAR_DAYS,
            "cooldown_rule": (
                "after one eligible event, later events for the same issuer inside "
                "63 calendar days are ineligible and retained"
            ),
            "amendment_promotion_permitted": False,
            "missing_source_substitution_permitted": False,
        },
        "timing_contract": {
            "decision_time": "public SEC acceptance timestamp",
            "candidate_entry_session": (
                "first complete regular trading session strictly after the SEC acceptance date"
            ),
            "same_day_entry_permitted": False,
            "maximum_holding_sessions": 5,
            "price_or_return_access_during_capacity_permitted": False,
        },
        "capacity_gate": {
            "minimum_stage0_signals": MINIMUM_STAGE0_SIGNALS,
            "minimum_disjoint_maturity_signals": MINIMUM_MATURITY_SIGNALS,
            "minimum_verified_events": MINIMUM_VERIFIED_EVENTS,
            "capacity_pass_rule": (
                "deduplicated events satisfying every source, Item 4, class, symbol, "
                "timing, and cooldown rule are at least minimum_verified_events"
            ),
            "negative_disposition": (
                "retire this exact candidate without source, pattern, date, or cooldown "
                "repair on the inspected corpus"
            ),
        },
        "denominator_contract": {
            "retain_every_indexed_initial_sc13d": True,
            "terminal_reason_required_for_every_index_row": True,
            "deduplicate_only_after_source_and_semantic_classification": True,
            "complete_attrition_counts_required": True,
        },
        "access_contract": {
            "sec_index_or_document_access_before_inspection_permitted": False,
            "sec_index_and_document_access_after_inspection_permitted": True,
            "capacity_classification_after_inspection_permitted": True,
            "market_price_access_permitted": False,
            "entry_fill_access_permitted": False,
            "exit_or_stop_access_permitted": False,
            "forward_return_computation_permitted": False,
            "stage0_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "stage0_handoff": {
            "full_rules_and_outcome_contract_frozen_after_capacity_pass": True,
            "independent_zero-result_inspection_required": True,
            "maximum_holding_sessions": 5,
            "cost_grid_bps_per_side": [5, 10, 20],
            "capacity_result_eligible_for_maturity": False,
        },
        "implementation_and_authority_hashes": {
            str(path.relative_to(PROJECT_ROOT)): successor._hash_file(path)
            for path in bound_files
        },
        "filing_count": None,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "claim_limit": (
            "This artifact freezes only the point-in-time filing-capacity test. "
            "It contains no filing count, event count, price, fill, exit, return, "
            "profit factor, or maturity evidence."
        ),
    }
    contract["contract_sha256"] = successor._self_hash(contract, "contract_sha256")
    return contract


def default_contract_path(contract: Mapping[str, Any]) -> Path:
    return DEFAULT_OUTPUT_ROOT / f"{CANDIDATE_ID}-{contract['contract_sha256']}.json"


def load_contract(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == successor._self_hash(value, "contract_sha256")
        and path.name == f"{CANDIDATE_ID}-{digest}.json"
    ):
        raise Schedule13dCapacityError("Schedule 13D contract was mutated or renamed")
    return value


def write_contract(
    *, output_root: Path = DEFAULT_OUTPUT_ROOT, status_path: Path = DEFAULT_STATUS
) -> tuple[Path, dict[str, Any]]:
    value = build_contract()
    path = output_root / f"{CANDIDATE_ID}-{value['contract_sha256']}.json"
    if path.exists() and _read_json(path) != value:
        raise Schedule13dCapacityError("content-addressed contract has other content")
    _write_json(value, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "campaign_id": CAMPAIGN_ID,
            "candidate_id": CANDIDATE_ID,
            "contract_sha256": value["contract_sha256"],
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
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = write_contract()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": value["contract_sha256"],
                "sec_source_access_permitted": False,
                "market_price_access_permitted": False,
                "outcome_access_permitted": False,
            }
        else:
            result = _read_json(DEFAULT_STATUS)
    except (Schedule13dCapacityError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
