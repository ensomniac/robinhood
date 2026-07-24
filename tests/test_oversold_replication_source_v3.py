from __future__ import annotations

import oversold_replication_source_v3 as source


def test_scanner_selection_is_exact_completed_reserve(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        source.base,
        "_require_committed",
        lambda _path: None,
    )
    value = source._selection_value()

    assert value["selected_dates"] == source._selected_dates()
    assert len(value["selected_dates"]) == 40
    assert value["selected_dates"][0] == "2025-12-11"
    assert value["selected_dates"][-1] == "2026-07-17"
    assert value["selection_time_et"] == "09:35:00"
    assert value["substitution_allowed"] is False
    assert value["market_prices_accessed"] is False


def test_configured_restores_base_globals() -> None:
    names = (
        "DATASET_ID",
        "SELECTION_PATH",
        "SELECTION_INSPECTION_PATH",
        "SECURITY_MASTER",
        "_selected_dates",
    )
    before = {name: getattr(source.base, name) for name in names}

    with source.configured():
        assert source.base.DATASET_ID == source.DATASET_ID
        assert source.base.SELECTION_PATH == source.SCANNER_SELECTION_PATH
        assert source.base._selected_dates() == source._selected_dates()

    assert {name: getattr(source.base, name) for name in names} == before
