from __future__ import annotations

from unittest import mock

import portfolio_maturity as maturity
import schedule13d_validation as validation


def _records(values: list[float]) -> list[dict[str, object]]:
    return [
        {
            "schema_version": 1,
            "record_type": "signal",
            "recorded_at": "2026-07-22T00:00:00-04:00",
            "strategy_id": validation.STRATEGY_ID,
            "strategy_version": validation.STRATEGY_VERSION,
            "mechanism_family": validation.MECHANISM_FAMILY,
            "rules_hash": "a" * 64,
            "date": f"2024-01-{index + 1:02d}",
            "sample_phase": "development",
            "mode": "historical",
            "signal_id": f"2024-01-{index + 1:02d}-signal",
            "closed": True,
            "eligible": True,
            "net_r": value,
            "stress_10bps_r": value - 0.01,
            "stress_20bps_r": value - 0.02,
            "stop_executed": value < 0,
            "session_capture_complete": True,
            "rule_violations": [],
        }
        for index, value in enumerate(values)
    ]


def test_serializable_metrics_marks_infinite_profit_factor():
    metrics = maturity._robustness_metrics(_records([1.0] * 10), 0.90)
    result = validation._serializable_metrics(metrics)
    assert result["profit_factor"] is None
    assert result["profit_factor_infinite"] is True


def test_development_gate_rejects_top_five_dependence():
    metrics = maturity._robustness_metrics(
        _records([2.0] * 5 + [-0.1] * 25), 0.90
    )
    blockers = validation._phase_blockers(metrics, "development")
    assert any("without five best" in blocker for blocker in blockers)


def test_contract_preserves_stage0_rules_and_locks_confirmation():
    stage0_contract = {
        "rules_hash": "b" * 64,
        "contract_sha256": validation.STAGE0_CONTRACT_SHA256,
        "partition_sha256": "c" * 64,
        "frozen_partitions": {
            "development": [{"event_ordinal": index} for index in range(30)],
            "confirmation": [{"event_ordinal": index} for index in range(20)],
        },
        "execution_contract": {},
        "exit_contract": {},
        "cost_contract": {},
    }
    stage0_result = {"result_sha256": validation.STAGE0_RESULT_SHA256}
    stage0_inspection = {"inspection_sha256": validation.STAGE0_INSPECTION_SHA256}
    requests = iter(
        {"request_sha256": f"{index:064x}"}
        for index in range(50)
    )
    with (
        mock.patch.object(
            validation,
            "_stage0_lineage",
            return_value=(stage0_contract, stage0_result, stage0_inspection),
        ),
        mock.patch.object(validation.stage0, "_xnys_sessions", return_value=[]),
        mock.patch.object(validation.stage0, "_request_contract", side_effect=lambda *_: next(requests)),
        mock.patch.object(validation, "sha256_file", return_value="d" * 64),
    ):
        contract = validation.build_contract(require_published=False)
    assert contract["rules_hash"] == stage0_contract["rules_hash"]
    assert contract["phase_denominator"] == {"development": 30, "confirmation": 20}
    assert contract["access_contract"]["confirmation_input_access_before_inspected_development_pass_permitted"] is False
