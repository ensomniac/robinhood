from __future__ import annotations

from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import catalyst_orb_retest_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")


def _bars(day: str = "2026-01-14") -> list[dict[str, object]]:
    session_day = date.fromisoformat(day)
    rows: list[dict[str, object]] = []
    for index in range(390):
        observed = datetime.combine(session_day, time(9, 30), EASTERN) + timedelta(
            minutes=index
        )
        rows.append(
            {
                "time_et": observed.time().isoformat(),
                "open": 100.0,
                "high": 100.5,
                "low": 99.5,
                "close": 100.0,
                "volume": 100,
                "bar_vwap": 100.0,
                "interpolated": False,
            }
        )
    for row in rows[:5]:
        row["high"] = 101.0
    rows[10].update(open=101.0, high=102.2, low=100.8, close=102.0, bar_vwap=101.5)
    rows[11].update(open=101.4, high=101.6, low=100.9, close=101.1, bar_vwap=101.2)
    rows[12].update(open=101.1, high=101.8, low=101.0, close=101.6, bar_vwap=101.4)
    rows[13].update(open=101.7, high=102.0, low=101.5, close=101.8, bar_vwap=101.7)
    return rows


def test_manifest_binds_exhausted_legacy_capacity_outside_maturity():
    manifest = stage0.build_manifest()
    stage0._validate_manifest(manifest)
    assert manifest["base_rules_hash"] == (
        "1d43ea7d5a43fcad8354d789d77d32267599ba46ffa140bcbbdbd51adc4aed86"
    )
    assert manifest["denominator"]["source_verified_positive_pairs"] == 8
    assert manifest["denominator"]["maximum_closed_signals"] == 8
    assert manifest["stage0_gate"]["minimum_closed_signals"] == 30
    assert manifest["outcomes_previously_accessed"] is True
    assert manifest["development_evidence_eligible"] is False
    assert manifest["confirmation_evidence_eligible"] is False
    assert manifest["provider_requests_authorized"] is False


def test_published_activation_is_content_addressed_and_outcome_locked():
    root = Path(__file__).resolve().parents[1]
    path = (
        root
        / "strategy_tournament/activations/"
        "catalyst-orb-retest-v1-"
        "c1c7004952ae77ee579b3f33496239783905caccf4aa29637a64528f7805d630.json"
    )
    manifest = json.loads(path.read_text(encoding="utf-8"))
    assert manifest["manifest_sha256"] == stage0.common._self_hash(
        manifest, "manifest_sha256"
    )
    assert manifest["implementation_sha256"] == stage0.sha256_file(
        root / "catalyst_orb_retest_stage0.py"
    )
    assert manifest["denominator"]["maximum_closed_signals"] == 8
    assert "inputs" not in manifest
    assert manifest["private_inputs"]["contains_private_source_identities"] is True
    assert manifest["return_evaluation_authorized_before_inspection"] is False


def test_trigger_requires_break_then_retest_then_later_bullish_rebreak():
    candidate = stage0._candidate(
        {"date": "2026-01-14", "symbol": "TEST", "rank": 1, "bars": _bars()}
    )
    assert candidate["status"] == "executable"
    assert candidate["first_trigger_time_et"] == "09:40:00"
    assert candidate["retest_time_et"] == "09:41:00"
    assert candidate["trigger_time_et"] == "09:42:00"
    assert candidate["entry_time_et"] == "09:43:00"
    assert candidate["stop"] == 100.9


def test_retest_outside_tolerance_cannot_signal():
    bars = _bars()
    for row in bars[11:]:
        row["low"] = min(float(row["low"]), 100.0)
    candidate = stage0._candidate(
        {"date": "2026-01-14", "symbol": "TEST", "rank": 1, "bars": bars}
    )
    assert candidate["status"] == "no_retest"
