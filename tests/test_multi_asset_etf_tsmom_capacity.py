from __future__ import annotations

import json
from pathlib import Path

import pytest

import multi_asset_etf_tsmom_capacity as capacity
import multi_asset_etf_tsmom_capacity_inspection as inspection


def test_contract_freezes_distinct_outcome_blind_capacity_candidate():
    contract = capacity.build_contract()
    assert contract["campaign_id"] == "multi-strategy-portfolio-validation-v2"
    assert contract["theme_id"] == "multi-asset-etf-time-series-momentum"
    assert contract["new_mechanism_family_slot"] == 1
    assert contract["maximum_new_mechanism_families_this_iso_week"] == 3
    assert [item["symbol"] for item in contract["universe"]] == [
        "SPY",
        "EFA",
        "EEM",
        "IEF",
        "GLD",
        "DBC",
        "UUP",
    ]
    assert contract["distinctness_contract"] == {
        "uses_own_price_history_only": True,
        "cross_sectional_ranking_used": False,
        "sector_rotation_used": False,
        "opening_range_or_intraday_pullback_used": False,
        "failed_v1_parameters_reused_or_repaired": False,
    }
    assert contract["signal_contract"]["lookback_sessions"] == 252
    assert contract["capacity_gate"]["minimum_stage0_signals"] == 30
    assert contract["capacity_gate"]["minimum_total_historical_signals"] == 50
    assert contract["access_contract"]["provider_collection_before_inspection_permitted"] is False
    assert contract["access_contract"]["forward_return_computation_permitted"] is False
    assert contract["access_contract"]["stage0_outcome_access_permitted"] is False
    assert contract["access_contract"]["broker_actions_permitted"] is False
    assert "signal_count" not in contract
    assert "return" not in contract
    assert contract["contract_sha256"] == capacity.successor._self_hash(
        contract, "contract_sha256"
    )


def test_inspection_opens_only_exact_collection_and_capacity_count(tmp_path: Path):
    path, contract = capacity.write_contract(
        output_root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    pending = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert pending["provider_access_permitted"] is False
    assert pending["capacity_signal_count_permitted"] is False
    result = inspection.inspect_contract(path, status_path=tmp_path / "status.json")
    assert result["contract_sha256"] == contract["contract_sha256"]
    assert result["provider_access_permitted"] is True
    assert result["capacity_signal_count_permitted"] is True
    assert result["outcome_access_permitted"] is False
    assert result["broker_actions_permitted"] is False
    assert result["signal_count"] is None
    assert result["returns_computed"] == 0
    assert result["market_outcomes_accessed"] is False
    assert result["maturity_effect"] == "NONE"


def test_inspection_rejects_tampered_content_addressed_contract(tmp_path: Path):
    path, contract = capacity.write_contract(
        output_root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    contract["signal_contract"]["lookback_sessions"] = 21
    path.write_text(json.dumps(contract, sort_keys=True), encoding="utf-8")
    with pytest.raises(capacity.MultiAssetEtfTsmomCapacityError):
        inspection.inspect_contract(path, status_path=tmp_path / "status.json")


def test_contract_does_not_open_historical_store_or_bind_outcomes(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("historical or provider access occurred")

    monkeypatch.setattr(Path, "glob", forbidden)
    contract = capacity.build_contract()
    assert contract["claim_limit"].startswith(
        "This artifact freezes only a causal capacity test."
    )
    assert contract["stage0_handoff"]["capacity_result_eligible_for_maturity"] is False
