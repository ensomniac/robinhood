from pathlib import Path

import pytest

import earnings_sec_reaction_v14_collection as collection
import earnings_sec_reaction_v14_search as search
import earnings_sec_yahoo_data as yahoo
from learning_data import freeze_dataset_contract


def test_v14_selection_excludes_complete_v13_opened_prefix():
    selected = search.selection()

    assert selected["development_event_count"] == 1019
    assert len(selected["development_signal_dates"]) == 499
    assert len(selected["development_symbols"]) == 608
    assert len(selected["development_requests"]) == 608
    assert len(selected["permanently_excluded_symbols"]) == 27
    assert "AMCF" in selected["permanently_excluded_symbols"]
    assert not set(selected["permanently_excluded_symbols"]).intersection(
        selected["development_symbols"]
    )
    assert selected["confirmation_event_count"] == 201
    assert len(selected["confirmation_signal_dates"]) == 118
    assert len(selected["confirmation_symbols"]) == 178
    assert selected["v13_opened_prefix_reused"] is False
    assert selected["market_prices_accessed"] is False
    assert selected["confirmation_prices_accessed"] is False


def test_v14_contract_freezes_source_policy_and_selection_correction(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        search.market, "_repo_path", lambda path: str(path)
    )
    source_records = [
        record
        for record in search.outcome_exposure.read_index()
        if record["exposure_id"].startswith(
            "source-failure-earnings-sec-reaction-v13-"
        )
    ]
    assert len(source_records) == 1
    monkeypatch.setattr(
        search.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: source_records,
    )
    monkeypatch.setattr(
        search.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "a" * 64},
    )
    selected = search.selection()
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-v14-test-capacity",
            "registered_at": "2026-07-25T09:20:00Z",
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl"
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": search.FAMILY_ID,
                    "formal_capacity": 1019,
                    "external_dataset_opened": False,
                },
            },
        },
        tmp_path,
    )

    contract = search.build_contract(
        created_at="2026-07-25T09:20:00Z",
        capacity_manifest=capacity_path,
    )

    assert len(contract["trial_family"]) == 32
    assert contract["selection_accounting"]["cumulative_trial_count"] == 64
    assert len(contract["prior_trial_daily_returns_by_id"]) == 32
    assert len(contract["development_data_requests"]) == 608
    assert len(contract["universe"]["excluded_symbols"]) == 27
    assert contract["universe"]["v13_opened_prefix_reused"] is False
    assert (
        contract["development_data_policy"]["invalid_ohlcv"]
        == "whole_symbol_permanent_missing_zero_credit"
    )
    assert contract["prior_source_failure_lineage"][
        "retained_tasks_reused"
    ] is False
    assert contract["confirmation_data_reserve"]["request_count"] == 178


def test_v14_invalid_ohlcv_is_whole_symbol_zero_credit(monkeypatch):
    request = {"request_sha256": "a" * 64, "symbol": "EDGE"}

    def fail(*_args, **_kwargs):
        raise yahoo.EarningsSecYahooDataError(
            collection.INVALID_OHLCV_ERROR
        )

    monkeypatch.setattr(collection.v13_collection, "_fetch", fail)
    telemetry = {
        "requests": 1,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
        "permanent_missing_responses": 0,
    }

    task = collection._fetch(request, object(), telemetry)

    assert task["status"] == "PERMANENT_MISSING"
    assert task["rows"] == []
    assert task["missing_reason"] == collection.INVALID_OHLCV_REASON
    assert telemetry["permanent_missing_responses"] == 1


def test_v14_unfrozen_schema_error_still_fails_closed(monkeypatch):
    request = {"request_sha256": "a" * 64, "symbol": "EDGE"}

    def fail(*_args, **_kwargs):
        raise yahoo.EarningsSecYahooDataError(
            "Yahoo chart contains malformed OHLCV"
        )

    monkeypatch.setattr(collection.v13_collection, "_fetch", fail)
    telemetry = {"permanent_missing_responses": 0}

    with pytest.raises(
        yahoo.EarningsSecYahooDataError,
        match="malformed OHLCV",
    ):
        collection._fetch(request, object(), telemetry)
