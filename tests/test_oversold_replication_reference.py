from __future__ import annotations

import json

import oversold_replication_reference as reference


def test_reference_contract_is_exact_date_reference_only(monkeypatch) -> None:
    dates = [f"2026-01-{day:02d}" for day in range(1, 10)]
    monkeypatch.setattr(
        reference,
        "_selection",
        lambda: (
            {"selected_dates": dates},
            {"inspection_sha256": "a" * 64},
        ),
    )
    monkeypatch.setattr(reference, "_repo_path", lambda path: str(path))
    monkeypatch.setattr(reference, "sha256_file", lambda _path: "b" * 64)
    monkeypatch.setattr(reference, "PRIVATE_REFERENCE_ROOT", reference.ROOT / "none")

    contract = reference.build_reference_contract()

    assert contract["dates"] == dates
    assert contract["query"]["point_in_time_parameter"] == "date"
    assert contract["market_prices_permitted"] is False
    assert contract["target_outcomes_observed_or_derived"] is False
    assert contract["substitutions_allowed"] is False
    assert contract["broker_actions"] == 0


def test_v1_failure_preserves_recovery_limit(tmp_path, monkeypatch) -> None:
    failure_root = tmp_path / "failures"
    monkeypatch.setattr(reference, "V1_FAILURE_ROOT", failure_root)
    monkeypatch.setattr(reference, "_require_committed", lambda _path: None)
    monkeypatch.setattr(reference, "_repo_path", lambda path: str(path))
    monkeypatch.setattr(reference, "sha256_file", lambda _path: "c" * 64)
    monkeypatch.setattr(
        reference,
        "_selection",
        lambda: (
            {
                "selected_dates": [f"2026-01-{day:02d}" for day in range(1, 136)]
            },
            {"inspection_sha256": "d" * 64},
        ),
    )

    result = reference.record_v1_failure()

    assert result["state"] == "FAILED_IDENTITY_COVERAGE_BEFORE_PROVIDER_ACCESS"
    assert result["scanner_contract_written"] is False
    assert result["provider_requests"] == 0
    assert result["market_prices_accessed"] is False
    assert result["target_outcomes_observed_or_derived"] is False
    assert "preserve all dates" in result["recovery_limit"]
    path = next(failure_root.glob("*.json"))
    assert json.loads(path.read_text()) == result
