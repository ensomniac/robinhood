from __future__ import annotations

import json
from pathlib import Path

import cross_sectional_capacity_stage0 as stage0


ROOT = Path(__file__).resolve().parents[1]


def test_both_frozen_cross_sectional_variants_are_capacity_infeasible():
    for variant_id, ordinal in stage0.VARIANT_ORDINALS.items():
        activation = stage0.build_activation(variant_id)
        assert activation["variant_ordinal"] == ordinal
        assert activation["implementation_sha256"] == stage0.sha256_file(
            stage0.Path(stage0.__file__).resolve()
        )
        capacity = activation["capacity_contract"]
        assert capacity["target_date_count"] == 24
        assert capacity["maximum_new_entries_per_target_date"] == 1
        assert capacity["maximum_possible_closed_signals"] == 24
        assert capacity["minimum_required_closed_signals"] == 30
        assert capacity["closed_signal_shortfall"] == 6
        assert capacity["stage0_capacity_possible"] is False
        assert activation["source_selection"]["membership_dates"] == 24
        assert activation["source_selection"]["market_outcomes_accessed"] is False
        assert activation["provider_requests_authorized"] is False
        assert activation["maturity_effect"] == "NONE"


def test_capacity_result_is_a_standard_failed_stage0_disposition(tmp_path):
    variant_id = "two-to-three-day-cross-sectional-reversal-v1"
    activation = stage0.build_activation(variant_id)
    activation_path = tmp_path / "activation.json"
    stage0.daily._write_json(activation, activation_path)
    inspection = stage0.inspect_activation(activation_path)
    inspection_path = tmp_path / "inspection.json"
    stage0.daily._write_json(inspection, inspection_path)
    result = stage0.build_result(
        activation_path, inspection_path, require_published=False
    )
    assert result["denominator"]["closed_signals"] == 0
    assert result["denominator"]["maximum_possible_closed_signals"] == 24
    assert result["primary_5bps"]["expectancy_r"] is None
    assert result["stress"]["20"]["total_r"] == 0
    assert result["stage0_blockers"] == [
        "closed signals are below the Stage 0 minimum",
        "primary expectancy is not positive",
        "primary profit factor is below the Stage 0 minimum",
        "20 bps-per-side total R is not positive",
    ]
    assert result["stage0_survived"] is False
    assert result["market_outcomes_accessed"] is False
    assert result["maturity_effect"] == "NONE"


def test_capacity_inspection_never_authorizes_return_access(tmp_path):
    activation = stage0.build_activation("five-day-52-week-high-continuation-v1")
    path = tmp_path / "activation.json"
    stage0.daily._write_json(activation, path)
    inspection = stage0.inspect_activation(path)
    assert inspection["capacity_evaluation_authorized"] is True
    assert inspection["return_evaluation_authorized"] is False
    assert inspection["provider_requests"] == 0
    assert inspection["returns_computed"] == 0
    assert inspection["market_outcomes_accessed"] is False
    assert inspection["valid"] is True


def test_published_reversal_activation_binds_the_structural_ceiling():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/activations").glob(
            "two-to-three-day-cross-sectional-reversal-v1-*.json"
        )
    )
    assert len(matches) == 1
    activation = json.loads(matches[0].read_text(encoding="utf-8"))
    assert activation == stage0.build_activation(activation["variant_id"])
    assert activation["manifest_sha256"] == stage0.common._self_hash(
        activation, "manifest_sha256"
    )
    assert activation["variant_ordinal"] == 4
    assert activation["base_rules_hash"] == (
        "4795592f6053c73bfced4521b94dfd3a6f4e0c6ea5064b76dde48afb6c63e0d6"
    )
    assert activation["capacity_contract"]["maximum_possible_closed_signals"] == 24
    assert activation["capacity_contract"]["minimum_required_closed_signals"] == 30
    assert activation["provider_requests_authorized"] is False
    assert activation["return_evaluation_authorized_before_inspection"] is False
    assert activation["maturity_effect"] == "NONE"


def test_published_reversal_capacity_inspection_authorizes_no_returns():
    activation_path = next(
        (ROOT / "strategy_tournament/second_wave/activations").glob(
            "two-to-three-day-cross-sectional-reversal-v1-*.json"
        )
    )
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "two-to-three-day-cross-sectional-reversal-v1-input-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection == stage0.inspect_activation(activation_path)
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["maximum_possible_closed_signals"] == 24
    assert inspection["minimum_required_closed_signals"] == 30
    assert inspection["closed_signal_shortfall"] == 6
    assert inspection["capacity_evaluation_authorized"] is True
    assert inspection["return_evaluation_authorized"] is False
    assert inspection["provider_requests"] == 0
    assert inspection["returns_computed"] == 0
    assert inspection["maturity_effect"] == "NONE"


def test_published_reversal_result_fails_without_outcome_access():
    matches = sorted(
        (ROOT / "research_results").glob(
            "2026-07-21-two-to-three-day-cross-sectional-reversal-stage0-*.json"
        )
    )
    assert len(matches) == 1
    result = json.loads(matches[0].read_text(encoding="utf-8"))
    assert result["result_sha256"] == stage0.common._self_hash(result, "result_sha256")
    assert result["denominator"]["decision_dates"] == 24
    assert result["denominator"]["closed_signals"] == 0
    assert result["denominator"]["maximum_possible_closed_signals"] == 24
    assert result["denominator"]["closed_signal_shortfall"] == 6
    assert result["stage0_survived"] is False
    assert result["structural_falsification"]["provider_collection_skipped"] is True
    assert result["structural_falsification"]["outcome_evaluation_skipped"] is True
    assert result["market_outcomes_accessed"] is False
    assert result["records"] == []
    assert result["maturity_effect"] == "NONE"


def test_published_reversal_result_inspection_rebuilds_retirement():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "two-to-three-day-cross-sectional-reversal-v1-result-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["result_sha256"] == (
        "3b54a1c87305fe1e808f3852779900728d741d9062a42fe56d8677108bcca057"
    )
    assert inspection["closed_signals"] == 0
    assert inspection["maximum_possible_closed_signals"] == 24
    assert inspection["minimum_required_closed_signals"] == 30
    assert inspection["stage0_survived"] is False
    assert inspection["provider_requests"] == 0
    assert inspection["returns_computed"] == 0
    assert inspection["market_outcomes_accessed"] is False
    assert inspection["maturity_effect"] == "NONE"
    assert inspection["valid"] is True
