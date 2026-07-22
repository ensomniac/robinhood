from __future__ import annotations

import json

import pytest

import outcome_exposure as exposure


def _record(exposure_id: str, scope: dict) -> dict:
    return exposure.build_record(
        exposure_id=exposure_id,
        campaign_id="campaign-test",
        lane="development",
        recorded_at="2026-07-22T19:00:00-04:00",
        source_path="tests/test_outcome_exposure.py",
        source_sha256="a" * 64,
        scope=scope,
    )


def test_exact_pair_overlap_blocks_confirmation_but_other_symbol_is_untouched():
    record = _record(
        "one",
        {"dates": ["2025-01-02"], "symbols": ["SPY"]},
    )

    with pytest.raises(exposure.OutcomeExposureError, match="prior outcome"):
        exposure.assert_untouched(
            {"dates": ["2025-01-02"], "symbols": ["SPY"]}, [record]
        )
    exposure.assert_untouched(
        {"dates": ["2025-01-02"], "symbols": ["QQQ"]}, [record]
    )


def test_wildcard_legacy_date_blocks_every_symbol():
    record = _record(
        "legacy",
        {"dates": ["2025-01-02"], "symbols": ["*"]},
    )
    with pytest.raises(exposure.OutcomeExposureError, match="prior outcome"):
        exposure.assert_untouched(
            {"dates": ["2025-01-02"], "symbols": ["IWM"]}, [record]
        )


def test_new_family_scopes_must_be_pairwise_disjoint():
    exposure.assert_disjoint(
        [
            {"dates": ["2025-01-02"], "symbols": ["SPY"]},
            {"dates": ["2025-01-03"], "symbols": ["SPY"]},
        ]
    )
    with pytest.raises(exposure.OutcomeExposureError, match="new family"):
        exposure.assert_disjoint(
            [
                {"dates": ["2025-01-02"], "symbols": ["SPY"]},
                {"dates": ["2025-01-02"], "symbols": ["SPY"]},
            ]
        )


def test_append_only_index_detects_mutation_and_duplicates(tmp_path):
    path = tmp_path / "index.jsonl"
    record = _record(
        "one",
        {"dates": ["2025-01-02"], "symbols": ["SPY"]},
    )
    exposure.append_record(record, path)
    assert exposure.audit(path)["records"] == 1
    assert exposure.ensure_record(record, path) is False
    with pytest.raises(exposure.OutcomeExposureError, match="already indexed"):
        exposure.append_record(record, path)

    value = json.loads(path.read_text(encoding="utf-8"))
    value["lane"] = "confirmation"
    path.write_text(json.dumps(value) + "\n", encoding="utf-8")
    with pytest.raises(exposure.OutcomeExposureError, match="hash is invalid"):
        exposure.audit(path)
