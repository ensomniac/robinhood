import earnings_sec_yahoo_data as source


def _request():
    return {
        "request_sha256": "a" * 64,
        "symbol": "EDGE",
        "start": "2010-01-01",
        "end": "2010-01-31",
    }


def test_yahoo_parser_retains_raw_ohlcv_and_omits_null_rows():
    payload = {
        "chart": {
            "error": None,
            "result": [
                {
                    "meta": {
                        "symbol": "EDGE",
                        "exchangeTimezoneName": "America/New_York",
                    },
                    "timestamp": [1262615400, 1262701800],
                    "indicators": {
                        "quote": [
                            {
                                "open": [10.0, None],
                                "high": [11.0, None],
                                "low": [9.0, None],
                                "close": [10.5, None],
                                "volume": [1000, None],
                            }
                        ]
                    },
                }
            ],
        }
    }

    task = source._parse_response(_request(), payload)

    assert task["status"] == "COMPLETE"
    assert len(task["rows"]) == 1
    assert task["rows"][0]["close"] == 10.5
    assert task["task_sha256"] == source.v5.self_hash(
        task, "task_sha256"
    )


def test_yahoo_parser_retains_missing_symbol_without_substitution():
    task = source._parse_response(
        _request(),
        {"chart": {"result": None, "error": {"code": "Not Found"}}},
    )

    assert task["status"] == "PERMANENT_MISSING"
    assert task["rows"] == []
    assert task["symbol"] == "EDGE"
