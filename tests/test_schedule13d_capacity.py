from __future__ import annotations

import json
from pathlib import Path

import pytest

import schedule13d_capacity as capacity
import schedule13d_capacity_inspection as inspection


def test_contract_freezes_second_family_without_counts_or_market_outcomes():
    contract = capacity.build_contract()
    assert contract["new_mechanism_family_slot"] == 2
    assert contract["maximum_new_mechanism_families_this_iso_week"] == 3
    assert contract["theme_id"] == "schedule-13d-activist-continuation"
    assert contract["source_contract"]["forms_included"] == ["SC 13D"]
    assert "SC 13D/A" in contract["source_contract"]["forms_excluded"]
    assert contract["event_contract"]["issuer_cooldown_calendar_days"] == 63
    assert contract["capacity_gate"] == {
        "minimum_stage0_signals": 30,
        "minimum_disjoint_maturity_signals": 50,
        "minimum_verified_events": 80,
        "capacity_pass_rule": (
            "deduplicated events satisfying every source, Item 4, class, symbol, "
            "timing, and cooldown rule are at least minimum_verified_events"
        ),
        "negative_disposition": (
            "retire this exact candidate without source, pattern, date, or cooldown "
            "repair on the inspected corpus"
        ),
    }
    assert contract["filing_count"] is None
    assert contract["verified_event_count"] is None
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["access_contract"]["forward_return_computation_permitted"] is False
    assert contract["access_contract"]["broker_actions_permitted"] is False
    assert contract["contract_sha256"] == capacity.successor._self_hash(
        contract, "contract_sha256"
    )


def test_contract_requires_filing_bound_control_intent_and_causal_symbol():
    contract = capacity.build_contract()
    document = contract["document_contract"]
    security = contract["security_contract"]
    assert document["control_intent_match"].startswith(
        "at least one actor-intent pattern"
    )
    assert len(document["pattern_contract"]["control_category_patterns"]) == 4
    assert security["current_or_future_symbol_mapping_permitted"] is False
    assert security["missing_causal_symbol"] == "ineligible_preserve_denominator"
    assert security["long_common_equity_only"] is True
    assert contract["denominator_contract"][
        "terminal_reason_required_for_every_index_row"
    ] is True


def test_inspection_opens_only_exact_sec_capacity_sources(tmp_path: Path):
    path, contract = capacity.write_contract(
        output_root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    result = inspection.inspect_contract(path, status_path=tmp_path / "status.json")
    assert result["contract_sha256"] == contract["contract_sha256"]
    assert result["sec_source_access_permitted"] is True
    assert result["capacity_classification_permitted"] is True
    assert result["market_price_access_permitted"] is False
    assert result["outcome_access_permitted"] is False
    assert result["broker_actions_permitted"] is False
    assert result["filing_count"] is None
    assert result["verified_event_count"] is None
    assert result["returns_computed"] == 0
    assert result["maturity_effect"] == "NONE"


def test_inspection_rejects_tampered_contract(tmp_path: Path):
    path, contract = capacity.write_contract(
        output_root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    contract["capacity_gate"]["minimum_verified_events"] = 10
    path.write_text(json.dumps(contract, sort_keys=True), encoding="utf-8")
    with pytest.raises(capacity.Schedule13dCapacityError):
        inspection.inspect_contract(path, status_path=tmp_path / "status.json")
