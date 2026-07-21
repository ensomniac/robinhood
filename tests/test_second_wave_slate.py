from __future__ import annotations

import json
from pathlib import Path

import second_wave_slate as slate


ROOT = Path(__file__).resolve().parents[1]


def _manifest_path() -> Path:
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/manifests").glob(
            "portfolio-stage0-second-wave-slate-*.json"
        )
    )
    assert len(matches) == 1
    return matches[0]


def test_published_second_wave_slate_rebuilds_with_six_ordered_variants():
    manifest = json.loads(_manifest_path().read_text(encoding="utf-8"))
    assert manifest == slate.build_manifest()
    slate.validate_manifest(manifest)
    assert manifest["ordered_variant_ids"] == [item[0] for item in slate.SECOND_WAVE]
    assert manifest["stage0_falsification"]["minimum_closed_signals"] == 30
    assert manifest["stage0_falsification"]["minimum_profit_factor"] == 1.10
    assert manifest["stage0_falsification"]["maximum_drawdown_r"] == 8
    assert manifest["maturity_effect"] == "NONE"
    assert manifest["return_evaluation_authorized_before_inspection"] is False
    assert manifest["provider_requests_authorized_before_inspection"] is False


def test_every_variant_is_new_falsification_only_and_inside_portfolio_envelope():
    manifest = slate.build_manifest()
    assert len({item["rules_hash"] for item in manifest["variants"]}) == 6
    for item in manifest["variants"]:
        assert item["claim_scope"] == "FALSIFICATION_ONLY"
        assert item["development_evidence_eligible"] is False
        assert item["confirmation_evidence_eligible"] is False
        assert item["maturity_effect"] == "NONE"
        assert 1 <= item["maximum_holding_trading_days"] <= 5
        assert item["execution"]["maximum_concurrent_positions"] == 1
        assert item["execution"]["maximum_new_entries_per_day"] == 1


def test_cross_sectional_variants_share_exact_frozen_point_in_time_dates():
    manifest = slate.build_manifest()
    variants = {item["variant_id"]: item for item in manifest["variants"]}
    for variant_id in (
        "two-to-three-day-cross-sectional-reversal-v1",
        "five-day-52-week-high-continuation-v1",
    ):
        contract = variants[variant_id]["data_contract"]
        assert contract["target_dates"] == list(slate.EQUITY_TARGET_DATES)
        assert contract["membership_outcomes_observed_or_derived"] is False
        assert contract["provider_substitution"] is False


def test_slate_inspection_is_absent_before_independent_review():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "portfolio-stage0-second-wave-slate-*.json"
        )
    )
    assert matches == []
