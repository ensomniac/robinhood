from __future__ import annotations

import insider_purchase_data as data
import insider_purchase_data_resume as resume


def test_permanent_missing_task_is_exact_and_content_addressed():
    request = data._request("TEST")

    task = resume._permanent_missing_task(request, "structural missing")

    assert task["request_sha256"] == request["request_sha256"]
    assert task["symbol"] == "TEST"
    assert task["status"] == "PERMANENT_MISSING"
    assert task["rows"] == []
    assert task["task_sha256"] == data._hash(
        {key: value for key, value in task.items() if key != "task_sha256"}
    )
