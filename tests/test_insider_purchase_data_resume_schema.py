from __future__ import annotations

import activist_earnings_data as yahoo
import insider_purchase_data_resume_schema as schema


def test_structural_response_messages_are_explicit_and_bounded():
    assert schema.STRUCTURAL_RESPONSE_MESSAGES == (
        "Yahoo dates are not unique and chronological",
        "Yahoo identity, timezone, or quote arrays drifted",
        "Yahoo OHLCV arrays are incomplete",
        "Yahoo returned ambiguous chart results",
        "Yahoo returned invalid OHLCV",
    )


def test_only_exact_parser_errors_receive_structural_disposition():
    for message in schema.STRUCTURAL_RESPONSE_MESSAGES:
        assert schema._is_structural_response_error(
            yahoo.ActivistEarningsDataError(message)
        )
    assert not schema._is_structural_response_error(
        yahoo.ActivistEarningsDataError(
            "Yahoo request failed before a response"
        )
    )
    assert not schema._is_structural_response_error(
        RuntimeError(schema.EXACT_FAILURE_MESSAGE)
    )
