from __future__ import annotations

import json
from pathlib import Path

import pytest

import asr_capacity_recovery as recovery
import asr_capacity_recovery_inspection as inspection


def test_recovery_contract_preserves_family_and_opens_no_outcomes():
    contract = recovery.build_contract()
    assert contract["family_budget_contract"]["new_mechanism_family_slot_consumed"] is False
    assert contract["family_budget_contract"]["mechanism_rule_changed"] is False
    assert contract["preserved_predecessor"]["same_contract_repair_permitted"] is False
    assert contract["preserved_predecessor"]["evidence_credit_inherited"] is False
    assert contract["source_contract"]["search_phrases"] == list(
        recovery.v1.SEARCH_PHRASES
    )
    assert contract["event_contract"] == recovery.v1.build_contract()["event_contract"]
    assert contract["capacity_gate"] == recovery.v1.build_contract()["capacity_gate"]
    assert contract["filing_hit_count"] is None
    assert contract["verified_event_count"] is None
    assert contract["access_contract"]["matched_document_access_permitted_by_this_contract"] is False
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["market_outcomes_accessed"] is False
    assert contract["broker_actions"] == 0


def test_recovery_algorithm_is_deterministic_and_fails_on_one_day():
    window = recovery.build_contract()["source_contract"]["window_algorithm"]
    assert window["traversal"] == "depth_first_oldest_half_first"
    assert window["inexact_action"].startswith("bisect the inclusive")
    assert window["single_date_inexact_action"] == "fail_closed"
    assert window["date_or_phrase_substitution_permitted"] is False


def test_inspection_opens_only_recursive_search(tmp_path: Path):
    path, contract = recovery.freeze_contract(
        output_root=tmp_path / "contracts",
        status_path=tmp_path / "status.json",
    )
    result = inspection.inspect_contract(
        path, status_path=tmp_path / "status.json"
    )
    assert result["contract_sha256"] == contract["contract_sha256"]
    assert result["zero_new_family_slot_rebuilt"] is True
    assert result["recursive_window_algorithm_rebuilt"] is True
    assert result["sec_search_access_permitted"] is True
    assert result["matched_document_access_permitted"] is False
    assert result["market_price_access_permitted"] is False
    assert result["outcome_access_permitted"] is False


def test_inspection_rejects_tampered_recovery_contract(tmp_path: Path):
    path, contract = recovery.freeze_contract(
        output_root=tmp_path / "contracts",
        status_path=tmp_path / "status.json",
    )
    contract["source_contract"]["window_algorithm"]["maximum_split_depth"] = 1
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(recovery.AsrCapacityRecoveryError):
        inspection.inspect_contract(path, status_path=tmp_path / "status.json")
