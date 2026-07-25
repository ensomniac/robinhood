import pytest

import earnings_sec_reaction_v11_collection as collection
import earnings_sec_reaction_v11_search as search
import earnings_sec_yahoo_data as yahoo
from learning_data import freeze_dataset_contract


def test_v11_selection_excludes_apc_and_preserves_capacity() -> None:
    selected = search.selection()

    assert selected["development_event_count"] == 147
    assert len(selected["development_signal_dates"]) == 82
    assert len(selected["development_symbols"]) == 108
    assert len(selected["development_requests"]) == 108
    assert selected["excluded_pre_search_symbols"] == [
        "AAPL",
        "ADSK",
        "ALGN",
        "AMZN",
        "ANN",
        "APC",
    ]
    assert "APC" not in selected["development_symbols"]
    assert "APC" not in selected["confirmation_symbols"]
    assert selected["confirmation_event_count"] == 229


def test_v11_contract_freezes_http_400_before_outcomes(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        search.metadata, "_repo_path", lambda path: str(path)
    )
    selected = search.selection()
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-v11-test-capacity",
            "registered_at": "2026-07-25T04:35:00Z",
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
                    "formal_capacity": 147,
                    "external_dataset_opened": False,
                },
            },
        },
        tmp_path,
    )

    contract = search.build_contract(
        created_at="2026-07-25T04:35:00Z",
        capacity_manifest=capacity_path,
    )

    assert len(contract["trial_family"]) == 32
    assert len(contract["development_data_requests"]) == 108
    assert (
        contract["development_data_policy"]["http_400"]
        == "permanent_missing_zero_credit"
    )
    assert contract["development_data_policy"]["retries"] == 0
    assert contract["development_data_policy"]["substitutions"] == 0


@pytest.mark.parametrize(
    ("message", "expected_reason"),
    [
        (
            "Yahoo development request returned HTTP 400",
            "Yahoo HTTP 400 retained as permanent missing",
        ),
        (
            "Yahoo chart identity, timezone, or quote arrays drifted",
            "Yahoo identity or quote envelope unavailable",
        ),
    ],
)
def test_v11_registered_failures_are_permanent_missing(
    monkeypatch,
    message,
    expected_reason,
) -> None:
    request = {
        "request_sha256": "a" * 64,
        "symbol": "EDGE",
    }

    def fail(*_args, **_kwargs):
        raise yahoo.EarningsSecYahooDataError(message)

    monkeypatch.setattr(collection.yahoo, "_fetch", fail)
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
    assert task["missing_reason"] == expected_reason
    assert telemetry["permanent_missing_responses"] == 1
