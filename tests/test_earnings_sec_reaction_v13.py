from pathlib import Path

import earnings_sec_reaction_v13_collection as collection
import earnings_sec_reaction_v13_search as search
import earnings_sec_yahoo_data as yahoo
from learning_data import freeze_dataset_contract


def test_v13_selection_is_large_and_confirmation_symbol_disjoint():
    selected = search.selection()

    assert (
        selected["development_event_count"]
        == search.EXPECTED_DEVELOPMENT_EVENTS
    )
    assert (
        len(selected["development_signal_dates"])
        == search.EXPECTED_DEVELOPMENT_SIGNAL_DATES
    )
    assert (
        len(selected["development_symbols"])
        == search.EXPECTED_DEVELOPMENT_SYMBOLS
    )
    assert (
        selected["confirmation_event_count"]
        == search.EXPECTED_CONFIRMATION_EVENTS
    )
    assert (
        len(selected["confirmation_signal_dates"])
        == search.EXPECTED_CONFIRMATION_SIGNAL_DATES
    )
    assert (
        len(selected["confirmation_symbols"])
        == search.EXPECTED_CONFIRMATION_SYMBOLS
    )
    assert not set(selected["development_symbols"]).intersection(
        selected["confirmation_symbols"]
    )
    assert selected["market_prices_accessed"] is False
    assert selected["confirmation_prices_accessed"] is False


def test_v13_prior_statistics_bind_complete_adverse_family():
    prior = search.prior_statistics()

    assert len(prior["prior_trial_ids"]) == 32
    assert len(prior["prior_trial_sharpes"]) == 32
    assert len(prior["prior_trial_p_values"]) == 32
    assert len(prior["prior_trial_daily_returns_by_id"]) == 32
    assert set(prior["prior_trial_ids"]) == set(
        prior["prior_trial_daily_returns_by_id"]
    )
    assert 0 <= prior["prior_standalone_pbo_probability"] <= 1
    assert prior["prior_result_sha256"].startswith("40b4414c")


def test_v13_contract_freezes_64_trial_corrections_before_prices(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(search.outcome_exposure, "read_index", lambda: [])
    monkeypatch.setattr(
        search.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "a" * 64},
    )
    monkeypatch.setattr(
        search.market, "_repo_path", lambda path: str(path)
    )
    selected = search.selection()
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-v13-test-capacity",
            "registered_at": "2026-07-25T08:00:00Z",
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
                    "formal_capacity": search.EXPECTED_DEVELOPMENT_EVENTS,
                    "external_dataset_opened": False,
                },
            },
        },
        tmp_path,
    )

    contract = search.build_contract(
        created_at="2026-07-25T08:00:00Z",
        capacity_manifest=capacity_path,
    )

    assert len(contract["trial_family"]) == 32
    assert contract["selection_accounting"]["cumulative_trial_count"] == 64
    assert len(contract["prior_trial_sharpes"]) == 32
    assert len(contract["prior_trial_p_values"]) == 32
    assert len(contract["prior_trial_daily_returns_by_id"]) == 32
    assert contract["prior_pbo_probability"] == 0.0
    assert len(contract["development_data_requests"]) == 635
    assert contract["confirmation_data_reserve"]["request_count"] == 178
    assert contract["development_data_policy"]["retries"] == 0
    assert contract["development_data_policy"]["substitutions"] == 0


def test_v13_registered_source_failures_are_zero_credit(monkeypatch):
    request = {"request_sha256": "a" * 64, "symbol": "EDGE"}

    def fail(*_args, **_kwargs):
        raise yahoo.EarningsSecYahooDataError(
            "Yahoo development request returned HTTP 400"
        )

    monkeypatch.setattr(collection.v11_collection.yahoo, "_fetch", fail)
    telemetry = {
        "requests": 1,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 1,
        "permanent_missing_responses": 0,
    }

    task = collection._fetch(request, object(), telemetry)

    assert task["status"] == "PERMANENT_MISSING"
    assert "HTTP 400" in task["missing_reason"]
    assert telemetry["permanent_missing_responses"] == 1
