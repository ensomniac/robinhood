import copy
from pathlib import Path

import development_non_return_qualification_v3 as qualification
import development_non_return_qualification_v3_inspection as inspection


def _rules():
    return {
        "maximum_quote_age_seconds": 5.0,
        "maximum_a_plus_median_spread_fraction": 0.0008,
        "maximum_median_spread_fraction": 0.001,
        "maximum_single_spread_fraction": 0.0015,
    }


def _snapshots(spread=0.04):
    return [
        {
            "age_seconds": age,
            "bid": 50.0,
            "ask": 50.0 + spread,
            "bid_size": 10_000,
            "ask_size": 10_000,
        }
        for age in (1.0, 1.5, 2.0)
    ]


def _opening_state():
    rows = [
        {
            "time_et": f"2026-01-02T09:{30 + index:02d}:00-05:00",
            "open": 49.5 if index == 0 else 49.7,
            "high": 50.0 if index == 4 else 49.9,
            "low": 49.4,
            "close": 49.9 if index == 4 else 49.7,
            "volume": 100,
            "interpolated": False,
        }
        for index in range(5)
    ]
    return {
        "scanner_fields": {
            "open_price": 49.5,
            "opening_high": 50.0,
            "opening_low": 49.4,
            "opening_close": 49.9,
            "opening_volume": 500,
        },
        "requests": {"opening_bars": {"observations": rows}},
    }


def test_quote_metrics_require_a_plus_spread_for_unvalidated_gate():
    narrow = qualification.quote_metrics(_snapshots(0.03), _rules())
    wide = qualification.quote_metrics(_snapshots(0.05), _rules())

    assert narrow["operating_spread_pass"] is True
    assert narrow["a_plus_spread_pass"] is True
    assert wide["operating_spread_pass"] is True
    assert wide["a_plus_spread_pass"] is False


def test_quote_metrics_fail_closed_on_stale_or_crossed_snapshot():
    snapshots = _snapshots()
    snapshots[1]["age_seconds"] = 5.1
    assert qualification.quote_metrics(snapshots, _rules()) == {
        "valid": False,
        "reason": "stale_crossed_or_nonpositive",
    }


def test_opening_prefix_must_rebuild_scanner_fields_exactly():
    state = _opening_state()
    assert qualification._opening_prefix_consistent(state) is True
    state["scanner_fields"]["opening_high"] = 50.01
    assert qualification._opening_prefix_consistent(state) is False


def test_interpolated_opening_bar_fails_prefix_integrity():
    state = _opening_state()
    state["requests"]["opening_bars"]["observations"][2]["interpolated"] = True
    assert qualification._opening_prefix_consistent(state) is False


def test_no_cross_is_terminal_and_never_a_survivor():
    state = {
        "pair_key": "private-key",
        "date": "2026-01-02",
        "symbol": "XYZ",
        "instrument_id": "private-id",
        "terminal_disposition": "NO_CLEAN_CROSS_BEFORE_CUTOFF",
    }

    record = qualification.evaluate_state(state, {})

    assert record["terminal_reason"] == "NO_CLEAN_CROSS_BEFORE_CUTOFF"
    assert record["survivor"] is False
    assert record["target_outcome_observed_or_derived"] is False


def test_independent_no_cross_rebuild_matches_builder():
    state = {
        "pair_key": "private-key",
        "date": "2026-01-02",
        "symbol": "XYZ",
        "instrument_id": "private-id",
        "terminal_disposition": "NO_CLEAN_CROSS_BEFORE_CUTOFF",
    }
    assert inspection._independent_record(state, {}) == qualification.evaluate_state(
        state, {}
    )


def test_aggregate_applies_gate_order_and_preserves_no_cross_denominator():
    gates = {
        "coarse_scanner": True,
        "opening_prefix_consistent": True,
        "quote": True,
        "a_plus_spread": True,
        "chase": True,
        "vwap": True,
        "liquidity": True,
        "halt": True,
        "market": True,
        "relative_strength": True,
        "structure": True,
        "resistance": True,
        "evaluator": True,
    }
    rejected = copy.deepcopy(gates)
    rejected["chase"] = False
    result = qualification._aggregate(
        [
            {"terminal_reason": "NO_CLEAN_CROSS_BEFORE_CUTOFF", "gates": {}},
            {"terminal_reason": "CHASE_GATE_FAILED", "gates": rejected},
            {"terminal_reason": "SURVIVOR", "gates": gates},
        ]
    )

    assert result["terminal_counts"] == {
        "CHASE_GATE_FAILED": 1,
        "NO_CLEAN_CROSS_BEFORE_CUTOFF": 1,
        "SURVIVOR": 1,
    }
    assert result["cascade"]["selected"] == 3
    assert result["cascade"]["after_clean_cross"] == 2
    assert result["cascade"]["after_chase"] == 1
    assert result["survivors"] == 1


def test_private_qualification_namespace_isolated_from_collector():
    root = Path("/historical")
    assert qualification._private_root(root) == (
        root
        / "_derived/development_non_return_qualification_v3"
        / qualification.DATASET_ID
    )
    assert qualification._private_root(root) != qualification._private_source_root(
        root
    )


def test_contract_locks_outcomes_and_substitution(monkeypatch):
    monkeypatch.setattr(
        qualification,
        "_source_paths",
        lambda root: {
            "index": root / "index",
            "splits": root / "splits",
            "pairs": root / "pairs",
            "root": root,
        },
    )
    monkeypatch.setattr(qualification, "_sha256_file", lambda path: "a" * 64)
    monkeypatch.setattr(
        qualification,
        "_implementation_contract",
        lambda: {"file.py": {"path": "file.py", "sha256": "b" * 64}},
    )
    monkeypatch.setattr(
        qualification,
        "_rules_contract",
        lambda: {
            "strategy_version": "v3",
            "rules_hash": "c" * 64,
            "strategy_config_sha256": "d" * 64,
        },
    )
    monkeypatch.setattr(
        qualification,
        "load_frozen_dataset_contract",
        lambda path: {"requested_dates": ["2026-01-02"]},
    )
    contract = qualification._expected_contract(
        index={"pair_files": []},
        store_root=Path("/historical"),
        observed_free_bytes=30 * 1024**3,
    )

    assert contract["qualification_contract"]["substitutions_allowed"] is False
    assert contract["qualification_contract"]["missing_inputs_default_favorable"] is False
    assert contract["outcome_lock"]["post_entry_data_access_allowed"] is False
    assert contract["outcome_lock"]["outcome_contract_permitted"] is False
    assert contract["privacy_contract"]["public_aggregates_and_hashes_only"] is True


def test_dataset_identity_does_not_alias_collection_contract():
    assert qualification.DATASET_ID != qualification.COLLECTION_DATASET_ID
    assert "gate-evaluation" in qualification.DATASET_ID
    assert qualification.DATASET_ID.endswith("-v2")


def test_independent_record_comparison_uses_persisted_json_semantics():
    rebuilt = [{"evaluator": {"warnings": ("one", "two")}}]
    persisted = [{"evaluator": {"warnings": ["one", "two"]}}]

    assert rebuilt != persisted
    assert qualification._canonical_bytes(rebuilt) == qualification._canonical_bytes(
        persisted
    )
