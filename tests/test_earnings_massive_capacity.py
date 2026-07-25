import json

import earnings_massive_capacity as capacity
import earnings_massive_capacity_inspection as inspection
from historical_store import HistoricalDayStore


class _Response:
    status_code = 200

    def __init__(self, rows):
        self._rows = rows

    def json(self):
        return {"status": "OK", "results": self._rows}


class _Session:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def get(self, url, *, params, timeout):
        self.calls.append((url, params, timeout))
        return _Response(self.rows)


def _commit_bypass(monkeypatch):
    monkeypatch.setattr(
        capacity.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(
        inspection.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )


def test_contract_freezes_annual_metadata_requests_without_outcomes(
    tmp_path, monkeypatch
):
    _commit_bypass(monkeypatch)
    monkeypatch.setattr(capacity, "DEFAULT_ROOT", tmp_path)
    contract = capacity.build_contract(created_at="2026-07-25T01:00:00Z")

    assert len(contract["requests"]) == 15
    assert contract["requests"][0]["parameters"]["date.gte"] == "2010-04-30"
    assert contract["requests"][-1]["parameters"]["date.lte"] == "2024-12-31"
    assert contract["market_prices_accessed"] is False
    assert contract["forward_returns_accessed"] is False
    assert contract["strategy_metrics_computed"] == 0
    assert contract["broker_actions"] == 0


def test_inspection_rejects_duplicate_eligible_events_from_capacity(
    tmp_path, monkeypatch
):
    _commit_bypass(monkeypatch)
    monkeypatch.setattr(capacity, "DEFAULT_ROOT", tmp_path / "public")
    monkeypatch.setattr(inspection.source, "DEFAULT_ROOT", tmp_path / "public")
    monkeypatch.setattr(capacity, "_repo_path", lambda path: str(path))
    monkeypatch.setattr(capacity, "_api_key", lambda: "configured")
    store = HistoricalDayStore(tmp_path / "private")
    contract_path, _contract = capacity.freeze_contract(
        created_at="2026-07-25T01:00:00Z",
        root=tmp_path / "public",
    )
    inspection_path, _ = inspection.inspect_contract(
        contract_path,
        inspected_at="2026-07-25T01:01:00Z",
        root=tmp_path / "public",
    )
    row = {
        "ticker": "EDGE",
        "date": "2020-01-02",
        "time": "08:00:00",
        "fiscal_year": 2019,
        "fiscal_period": "Q4",
        "date_status": "confirmed",
        "actual_eps": 1.25,
        "estimated_eps": 1.0,
    }
    session = _Session([row, row])
    collection_path, collection = capacity.collect(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T01:02:00Z",
        store=store,
        session=session,
        minimum_interval_seconds=0,
    )
    result_path, result = inspection.inspect_collection(
        collection_path,
        inspected_at="2026-07-25T01:03:00Z",
        store=store,
        root=tmp_path / "public",
    )

    assert len(session.calls) == 15
    assert collection["row_count"] == 30
    assert result["formally_eligible_unique_event_count"] == 0
    assert result["duplicate_eligible_row_count_zero_credit"] == 30
    assert result["state"] == "INSUFFICIENT_METADATA_CAPACITY"
    assert json.loads(result_path.read_text())["provider_requests"] == 0


def test_unexpected_pagination_fails_closed(monkeypatch):
    class PaginatedResponse(_Response):
        def json(self):
            return {
                "status": "OK",
                "results": [],
                "next_url": "https://api.massive.com/next",
            }

    class PaginatedSession:
        def get(self, *_args, **_kwargs):
            return PaginatedResponse([])

    request = capacity._requests()[0]
    try:
        capacity._request(
            request,
            api_key="configured",
            session=PaginatedSession(),
            timeout_seconds=1.0,
        )
    except capacity.EarningsMassiveCapacityError as exc:
        assert "exceeded the frozen row limit" in str(exc)
    else:
        raise AssertionError("unexpected pagination must fail closed")
