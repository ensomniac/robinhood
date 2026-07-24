import earnings_pead_expansion as expansion


def test_expansion_contract_freezes_unique_outcome_blind_requests() -> None:
    contract = expansion.build_contract(
        created_at="2026-07-24T21:00:00Z",
        enforce_commit=False,
    )

    assert len(contract["symbols"]) == 128
    assert len(set(contract["symbols"])) == 128
    assert contract["requests"] == [
        {"symbol": symbol} for symbol in contract["symbols"]
    ]
    assert contract["v2_development_attempts"] == 32
    assert contract["prior_trials_must_enter_selection_correction"] is True
    assert contract["market_prices_accessed"] is False
    assert contract["forward_returns_accessed"] is False
    assert contract["broker_actions"] == 0


def test_expansion_normalization_rejects_unverified_or_future_rows() -> None:
    base = {
        "symbol": "EDGE",
        "report": {
            "date": "2026-04-15",
            "timing": "am",
            "verified": True,
        },
        "eps": {"actual": "1.20", "estimate": "1.00"},
    }
    assert expansion._normalize(base) == {
        "symbol": "EDGE",
        "report_date": "2026-04-15",
        "timing": "am",
        "verified": True,
        "actual_eps": 1.2,
        "estimated_eps": 1.0,
    }
    assert (
        expansion._normalize(
            {**base, "report": {**base["report"], "verified": False}}
        )
        is None
    )
    assert (
        expansion._normalize(
            {**base, "report": {**base["report"], "date": "2026-07-24"}}
        )
        is None
    )
