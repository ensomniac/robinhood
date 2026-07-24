from __future__ import annotations

import oversold_replication_confirmation as confirmation


def _dates() -> list[str]:
    return [f"2026-{index // 28 + 1:02d}-{index % 28 + 1:02d}" for index in range(135)]


def test_build_inventory_preserves_five_session_embargo(
    monkeypatch,
) -> None:
    dates = _dates()
    detail = {
        "dates": {
            day: {
                "evaluations": (
                    [
                        {
                            "symbol": "EDGE",
                            "instrument_id": "id",
                            "primary_exchange": "XNYS",
                            "open_price": 10.2,
                            "prior_close": 10.0,
                        }
                    ]
                    if index >= 5
                    else []
                )
            }
            for index, day in enumerate(dates)
        }
    }
    monkeypatch.setattr(
        confirmation,
        "sha256_file",
        lambda _path: "detail-hash",
    )
    result = confirmation._build_inventory(detail, dates)
    assert result["embargo_dates"] == dates[:5]
    assert result["evaluation_dates"] == dates[5:]
    assert len(result["signal_dates"]) == 130
    assert len(result["zero_signal_dates"]) == 0
    assert sum(map(len, result["candidates_by_date"].values())) == 130


def test_build_inventory_rejects_nonchronological_selection(
    monkeypatch,
) -> None:
    dates = _dates()
    dates[0], dates[1] = dates[1], dates[0]
    monkeypatch.setattr(
        confirmation,
        "sha256_file",
        lambda _path: "detail-hash",
    )
    try:
        confirmation._build_inventory({"dates": {}}, dates)
    except confirmation.OversoldReplicationConfirmationError as exc:
        assert "selection dates drifted" in str(exc)
    else:
        raise AssertionError("nonchronological confirmation must fail")
