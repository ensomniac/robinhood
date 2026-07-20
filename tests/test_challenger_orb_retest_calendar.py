from unittest.mock import patch

import pytest

import challenger_orb_retest_calendar as calendar
import challenger_orb_retest_calendar_inspection as inspection


def _rows(count=900):
    rows = []
    day = __import__("datetime").date.fromisoformat("2023-01-02")
    while len(rows) < count:
        if day.weekday() < 5:
            rows.append({"date": day.isoformat(), "open": "09:30", "close": "16:00"})
        day += __import__("datetime").timedelta(days=1)
    return rows


def test_normalize_rows_is_strict_and_deterministic(monkeypatch):
    monkeypatch.setattr(calendar, "CALENDAR_END", "2026-12-31")
    first = calendar._normalize_rows(_rows())
    second = calendar._normalize_rows(_rows())

    assert first == second
    assert len(first) == 900
    assert first[0] == {"date": "2023-01-02", "open_et": "09:30", "close_et": "16:00"}


def test_duplicate_or_nonchronological_calendar_fails(monkeypatch):
    monkeypatch.setattr(calendar, "CALENDAR_END", "2026-12-31")
    rows = _rows()
    rows[1] = dict(rows[0])
    with pytest.raises(calendar.ChallengerCalendarError, match="duplicate|chronological"):
        calendar._normalize_rows(rows)


def test_weekend_or_invalid_hours_fail(monkeypatch):
    monkeypatch.setattr(calendar, "CALENDAR_END", "2026-12-31")
    rows = _rows()
    rows[0] = {"date": "2023-01-07", "open": "09:30", "close": "16:00"}
    with pytest.raises(calendar.ChallengerCalendarError, match="invalid"):
        calendar._normalize_rows(rows)

    rows = _rows()
    rows[0]["close"] = "09:00"
    with pytest.raises(calendar.ChallengerCalendarError, match="invalid"):
        calendar._normalize_rows(rows)


def test_contract_freezes_zero_output_and_both_implementations(tmp_path, monkeypatch):
    monkeypatch.setattr(calendar, "DEFAULT_CALENDAR", tmp_path / "calendar.json")
    monkeypatch.setattr(calendar, "DEFAULT_SOURCE", tmp_path / "source.json")
    monkeypatch.setattr(calendar, "_published", lambda path: {"sha256": "x"})
    monkeypatch.setattr(calendar, "_repo_path", lambda path: path.name)
    with (
        patch.object(calendar, "_binding", side_effect=lambda path: {"path": path.name, "sha256": "a" * 64}),
        patch.object(calendar, "freeze_dataset_contract", return_value=(tmp_path / "manifest.json", {"manifest_sha256": "b" * 64})) as freeze,
    ):
        calendar.freeze_contract(output_root=tmp_path)
    contract = freeze.call_args.args[0]
    assert contract["requested_dates"] == [calendar.CALENDAR_START, calendar.CALENDAR_END]
    assert contract["output_contract"]["pre_freeze_output_artifacts"] == 0
    assert contract["output_contract"]["requested_dates_semantics"] == (
        "calendar_query_bounds_not_target_sessions"
    )
    assert set(contract["implementation_contract"]) == {"collector", "inspector"}
    assert contract["output_contract"]["target_outcomes_observed_or_derived"] is False


def test_independent_rows_rebuild_persisted_shape(monkeypatch):
    monkeypatch.setattr(calendar, "CALENDAR_END", "2026-12-31")
    persisted = [
        {"date": row["date"], "open_et": row["open"], "close_et": row["close"]}
        for row in _rows()
    ]
    assert inspection._independent_rows(persisted) == persisted
