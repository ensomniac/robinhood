from __future__ import annotations

import insider_purchase_data as data


def test_request_is_exact_and_content_addressed():
    request = data._request("BRK.B")

    assert request["endpoint"].endswith("/BRK.B")
    assert request["parameters"]["interval"] == "1d"
    assert request["parameters"]["events"] == "history"
    assert request["start"] == "2017-11-01"
    assert request["end"] == "2022-12-23"
    assert request["request_sha256"] == data._hash(
        {
            key: value
            for key, value in request.items()
            if key != "request_sha256"
        }
    )


def test_event_metadata_keeps_only_observable_form4_fields():
    event = {
        "symbol": "TEST",
        "issuer_cik": "0000000001",
        "filing_date": "2020-01-02",
        "filing_dates": ["2020-01-02"],
        "entry_date": "2020-01-03",
        "purchase_notional": 250_000.0,
        "distinct_reporting_owners": 2,
        "transaction_count": 2,
        "event_semantics": "ORIGINAL_FORM4_DIRECT_OPEN_MARKET_PURCHASE",
    }

    result = data._event_metadata(
        {"development_events": [event]},
        ["2020-01-02", "2020-01-03"],
    )

    assert result["2020-01-02"] == []
    assert result["2020-01-03"] == [
        {
            "symbol": "TEST",
            "issuer_cik": "0000000001",
            "filing_date": "2020-01-02",
            "filing_dates": ["2020-01-02"],
            "entry_date": "2020-01-03",
            "purchase_notional": 250_000.0,
            "distinct_reporting_owners": 2,
            "transaction_count": 2,
            "event_semantics": (
                "ORIGINAL_FORM4_DIRECT_OPEN_MARKET_PURCHASE"
            ),
        }
    ]
