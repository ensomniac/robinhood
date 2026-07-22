from __future__ import annotations

import json
from pathlib import Path

import pytest

import asr_capacity as capacity
import asr_capacity_inspection as inspection


def test_contract_freezes_third_family_without_counts_or_outcomes():
    contract = capacity.build_contract()
    assert contract["new_mechanism_family_slot"] == 3
    assert contract["source_contract"]["collection_start"] == "2010-01-01"
    assert contract["source_contract"]["collection_end_inclusive"] == "2025-12-31"
    assert len(contract["source_contract"]["search_phrases"]) == 4
    assert contract["capacity_gate"]["retire_below_verified_events"] == 50
    assert contract["capacity_gate"]["fast_lane_minimum_verified_events"] == 100
    assert contract["filing_hit_count"] is None
    assert contract["verified_event_count"] is None
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["access_contract"]["forward_return_access_permitted"] is False
    assert contract["broker_actions"] == 0


def test_contract_requires_executed_notional_and_continuing_mechanics():
    event = capacity.build_contract()["event_contract"]
    patterns = event["pattern_contract"]
    assert event["all_required_positive_groups_must_match"] is True
    assert patterns["executed_agreement"]
    assert patterns["committed_notional"]
    assert patterns["continuing_delivery_or_settlement"]
    assert patterns["generic_only_exclusions"]
    assert event["board_authorization_only"] == "ineligible_preserve_denominator"


def test_inspection_opens_only_sec_capacity_sources(tmp_path: Path):
    path, contract = capacity.freeze_contract(
        output_root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    result = inspection.inspect_contract(path, status_path=tmp_path / "status.json")
    assert result["contract_sha256"] == contract["contract_sha256"]
    assert result["sec_source_access_permitted"] is True
    assert result["capacity_classification_permitted"] is True
    assert result["market_price_access_permitted"] is False
    assert result["outcome_access_permitted"] is False
    assert result["filing_hit_count"] is None
    assert result["verified_event_count"] is None


def test_inspection_rejects_tampered_contract(tmp_path: Path):
    path, contract = capacity.freeze_contract(
        output_root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    contract["capacity_gate"]["fast_lane_minimum_verified_events"] = 10
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(capacity.AsrCapacityError):
        inspection.inspect_contract(path, status_path=tmp_path / "status.json")
