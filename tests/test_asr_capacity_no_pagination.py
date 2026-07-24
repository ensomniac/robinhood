from __future__ import annotations

import json
from pathlib import Path

import pytest

import asr_capacity_no_pagination as capacity
import asr_capacity_no_pagination_inspection as inspection


def test_contract_eliminates_offset_pagination_without_new_family():
    contract = capacity.build_contract()
    family = contract["family_budget_contract"]
    window = contract["source_contract"]["window_algorithm"]
    assert family["new_mechanism_family_slot_consumed"] is False
    assert family["mechanism_rule_changed"] is False
    assert contract["preserved_predecessor"]["same_contract_repair_permitted"] is False
    assert contract["preserved_predecessor"]["evidence_credit_inherited"] is False
    assert window["split_when_total_relation_is"] == "gte"
    assert window["split_when_exact_total_exceeds"] == 100
    assert window["offset_pagination_permitted"] is False
    assert window["single_date_inexact_or_over_page_action"] == "fail_closed"
    assert contract["source_contract"]["search_phrases"] == list(
        capacity.v1.SEARCH_PHRASES
    )
    assert contract["filing_hit_count"] is None
    assert contract["verified_event_count"] is None
    assert contract["market_outcomes_accessed"] is False
    assert contract["broker_actions"] == 0


def test_contract_freezes_bounded_transient_retries():
    retry = capacity.build_contract()["source_contract"]["retry_contract"]
    assert retry["maximum_attempts_per_exact_request"] == 3
    assert retry["retryable_http_statuses"] == [429, 500, 502, 503, 504]
    assert retry["backoff_seconds_by_retry"] == [1.0, 2.0]
    assert retry["terminal_failure_action"] == "fail_closed_preserve_cache"


def test_inspection_opens_only_single_page_search(tmp_path: Path):
    path, contract = capacity.freeze_contract(
        output_root=tmp_path / "contracts",
        status_path=tmp_path / "status.json",
    )
    result = inspection.inspect_contract(
        path, status_path=tmp_path / "status.json"
    )
    assert result["contract_sha256"] == contract["contract_sha256"]
    assert result["zero_new_family_slot_rebuilt"] is True
    assert result["single_page_leaf_algorithm_rebuilt"] is True
    assert result["offset_pagination_forbidden_rebuilt"] is True
    assert result["bounded_retries_rebuilt"] is True
    assert result["sec_search_access_permitted"] is True
    assert result["matched_document_access_permitted"] is False
    assert result["market_price_access_permitted"] is False
    assert result["outcome_access_permitted"] is False


def test_inspection_rejects_tampered_no_pagination_contract(tmp_path: Path):
    path, contract = capacity.freeze_contract(
        output_root=tmp_path / "contracts",
        status_path=tmp_path / "status.json",
    )
    contract["source_contract"]["window_algorithm"]["offset_pagination_permitted"] = True
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(capacity.AsrCapacityNoPaginationError):
        inspection.inspect_contract(path, status_path=tmp_path / "status.json")
