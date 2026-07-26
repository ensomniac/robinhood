from __future__ import annotations

import sec_broad_pead_temporal_metadata as source


def test_metadata_contract_freezes_exact_rule_and_eight_archives() -> None:
    contract = source.build_contract(
        created_at="2026-07-26T01:40:00-04:00",
        enforce_commit=False,
    )

    assert len(contract["requests"]) == 8
    assert contract["requests"][0]["quarter"] == "2018q1"
    assert contract["requests"][-1]["quarter"] == "2019q4"
    assert contract["archive_root"] == source.corrected.ARCHIVE_ROOT
    assert "-and-" not in contract["archive_root"]
    assert contract["exact_strategy"]["parameters"] == {
        "maximum_hold_sessions": 5,
        "minimum_opening_gap_fraction": -0.02,
        "minimum_yoy_eps_change_ratio": 0.0,
        "security_trend_gate": "price>SMA200",
        "stop_atr14": 1.5,
    }
    assert contract["exact_strategy"]["parameter_alternatives"] == 0
    assert contract["provider_requests_executed"] == 0
    assert contract["market_prices_accessed"] is False
    assert contract["confirmation_outcomes_accessed"] is False
    assert contract["broker_actions"] == 0


def test_metadata_reserve_has_sma200_warmup_and_no_substitution() -> None:
    dates = source.price_scope_dates()
    contract = source.build_contract(
        created_at="2026-07-26T01:40:00-04:00",
        enforce_commit=False,
    )

    first = dates.index(source.RESERVE_START)
    assert first == 205
    assert dates[-1] <= source.RESERVE_SETTLEMENT_END.isoformat()
    assert contract["reserve"]["price_scope_dates"] == dates
    assert contract["request_policy"]["retries_permitted"] == 0
    assert contract["request_policy"]["substitutions_permitted"] == 0
    assert (
        contract["reserve"][
            "exposed_event_or_symbol_substitution_permitted"
        ]
        is False
    )
