from __future__ import annotations

import oversold_replication_discovery as discovery


def test_scope_retains_only_signal_dates() -> None:
    candidates = {
        "2023-01-26": [{"symbol": "B"}, {"symbol": "A"}],
        "2023-01-27": [],
    }
    assert discovery._scope(
        ["2023-01-26", "2023-01-27"],
        candidates,
    ) == {
        "dates": ["2023-01-26"],
        "symbols_by_date": {
            "2023-01-26": ["A", "B"],
        },
    }


def test_status_does_not_require_calendar_wait(
    monkeypatch,
    tmp_path,
) -> None:
    monkeypatch.setattr(discovery, "DEFAULT_ROOT", tmp_path / "continuous")
    monkeypatch.setattr(
        discovery.confirmation,
        "INSPECTION_ROOT",
        tmp_path / "confirmation",
    )
    monkeypatch.setattr(
        discovery,
        "DEVELOPMENT_MANIFEST_INSPECTION",
        tmp_path / "development-inspection.json",
    )
    result = discovery.status(root=discovery.DEFAULT_ROOT)
    assert result["calendar_wait_required"] is False
    assert result["state"] == "AWAITING_CONFIRMATION_INVENTORY"
