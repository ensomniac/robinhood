from __future__ import annotations

from datetime import date

import next_week_discovery_batch as batch


def test_exact_three_family_grids_use_released_rolling_slots():
    plan = batch.build_plan()
    assert plan["state"] == "READY_FOR_DISJOINT_EVIDENCE_FREEZE"
    assert plan["research_batch_id"] == "rolling-batch-1"
    assert plan["activation_policy"] == "ROLLING_TERMINAL_REPLACEMENT"
    assert [item["trial_count"] for item in plan["families"]] == [48, 32, 32]
    assert len(plan["families"]) == 3
    assert plan["family_contracts_frozen"] == 0
    assert plan["provider_requests"] == 0
    assert plan["market_outcomes_accessed"] is False
    assert plan["broker_actions"] == 0
    pullback = plan["families"][2]
    assert len(pullback["universe"]["symbols"]) == 19
    assert "XLC" not in pullback["universe"]["symbols"]
    assert "XLRE" not in pullback["universe"]["symbols"]
    assert plan["supersedes_plan_sha256"] == batch.SUPERSEDED_PLAN_SHA256


def test_rolling_authorization_opens_evidence_freeze_without_calendar_wait(tmp_path):
    batch.prepare(root=tmp_path / "plans", status_path=tmp_path / "status.json")
    before = batch.activation_status(
        today=date(2026, 7, 22), status_path=tmp_path / "status.json"
    )
    after = batch.activation_status(
        today=date(2026, 7, 23), status_path=tmp_path / "status.json"
    )
    assert before["activation_permitted"] is False
    assert "not authorized before" in before["blockers"][0]
    assert after["activation_permitted"] is True
    assert after["state"] == "READY_FOR_DISJOINT_EVIDENCE_FREEZE"
    assert after["activation_policy"] == "ROLLING_TERMINAL_REPLACEMENT"
    assert after["available_slot_count"] == 3
    assert after["provider_access_permitted"] is False
    assert after["outcome_access_permitted"] is False
    assert after["family_contracts_frozen"] == 0


def test_common_contract_preserves_cost_lookahead_and_hold_gates():
    common = batch.build_plan()["common_contract"]
    assert common["costs_bps_per_side"] == [5, 10, 20]
    assert common["maximum_holding_trading_days"] == 5
    assert common["same_interval_stop_target_ambiguity"] == "stop_first"
    assert common["minimum_expected_gross_move_to_primary_round_trip_cost"] == 5.0
    assert common["maximum_trials_per_family"] == 64
