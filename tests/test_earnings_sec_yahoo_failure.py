import earnings_sec_yahoo_failure as source


def test_exposed_symbols_include_completed_and_failed_request():
    assert source.COMPLETED_SYMBOLS == ["AAPL", "ADSK", "ALGN", "AMZN"]
    assert source.FAILED_SYMBOL == "ANN"
    assert source.EXPOSED_SYMBOLS == ["AAPL", "ADSK", "ALGN", "AMZN", "ANN"]
