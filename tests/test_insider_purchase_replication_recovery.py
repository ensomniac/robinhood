import activist_earnings_data as yahoo

import insider_purchase_replication_recovery as recovery


def test_schema_policy_is_exact_and_fail_closed():
    for message in recovery.STRUCTURAL_RESPONSE_MESSAGES:
        assert recovery._is_structural_response_error(
            yahoo.ActivistEarningsDataError(message)
        )

    assert not recovery._is_structural_response_error(
        yahoo.ActivistEarningsDataError("Yahoo request failed before a response")
    )
    assert not recovery._is_structural_response_error(
        RuntimeError(recovery.EXACT_FAILURE_MESSAGE)
    )


def test_permanent_missing_task_retains_exact_request_identity():
    request = {
        "request_sha256": "a" * 64,
        "symbol": "LANC",
    }

    task = recovery._permanent_missing_task(
        request, "inspected unusable response"
    )

    assert task["request_sha256"] == request["request_sha256"]
    assert task["symbol"] == "LANC"
    assert task["status"] == "PERMANENT_MISSING"
    assert task["rows"] == []
    assert len(task["task_sha256"]) == 64
