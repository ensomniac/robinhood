from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pytest

import dense_data_collection as collection
import dense_data_collection_inspection as inspection
import dense_strategy_runtime as runtime
import strategy_discovery
from historical_providers import HistoricalProviderError
from historical_store import HistoricalStoreConfig, canonical_sha256
from learning_data import freeze_dataset_contract


def _collection_clock():
    return datetime(2026, 7, 27, 13, 0, tzinfo=UTC)


class FakeBackend:
    def __init__(self, symbols, *, fail_after=None):
        self.symbols = symbols
        self.fail_after = fail_after
        self.calls = []
        self.telemetry = {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 0,
            "failures": 0,
        }

    def fetch(self, task):
        if self.fail_after is not None and len(self.calls) >= self.fail_after:
            raise collection.DenseDataCollectionError("injected interruption")
        self.calls.append(task["task_id"])
        self.telemetry["requests"] += 1
        if task["kind"] == "split_actions":
            return []
        day = task["date"]
        return [
            {
                "symbol": symbol,
                "date": day,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 2_000_000,
                "count": 100,
                "wap": 100.25,
            }
            for symbol in self.symbols
        ]

    def close(self):
        return None


class RetryBackend(FakeBackend):
    def __init__(self, symbols, failures, *, retryable=True, retry_after=None):
        super().__init__(symbols)
        self.remaining_failures = failures
        self.retryable = retryable
        self.retry_after = retry_after

    def fetch(self, task):
        if self.remaining_failures:
            self.calls.append(task["task_id"])
            self.telemetry["requests"] += 1
            self.remaining_failures -= 1
            raise HistoricalProviderError(
                "injected provider failure",
                category=(
                    "retryable_provider"
                    if self.retryable
                    else "permanent_fidelity"
                ),
                retry_after_seconds=self.retry_after,
            )
        return super().fetch(task)


def _dates(count):
    start = date(2024, 1, 2)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _artifacts(tmp_path, monkeypatch, *, lane="development"):
    monkeypatch.setattr(collection, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(inspection, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(strategy_discovery, "PROJECT_ROOT", tmp_path)
    confirmation = lane == "confirmation"
    authority_path, authority = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": (
                "frozen-strategy-winner"
                if confirmation
                else "frozen-development-search"
            ),
            "state": "WINNER_FROZEN" if confirmation else "SEARCH_FROZEN",
            **(
                {
                    "rules_hash": "a" * 64,
                    "recorded_at": "2026-07-27T08:00:00-04:00",
                    "confirmation_scope": {"dates": _dates(6), "symbols": ["SPY"]},
                }
                if confirmation
                else {}
            ),
        },
        tmp_path / "authority",
        "search",
    )
    calendar = tmp_path / "calendar.json"
    dates = _dates(8)
    calendar.write_text(
        json.dumps(
            [
                {"date": day, "open_et": "09:30", "close_et": "16:00"}
                for day in dates
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    symbols = [
        "SPY",
        "QQQ",
        "IWM",
        "DIA",
        "EFA",
        "EEM",
        "IEF",
        "TLT",
        "GLD",
        "DBC",
        "XLB",
        "XLE",
        "XLF",
        "XLI",
        "XLK",
        "XLP",
        "XLU",
        "XLV",
        "XLY",
    ]
    split = {"kind": "split_actions", "date": dates[-1], "start": dates[0]}
    split["task_id"] = canonical_sha256(split)
    tasks = [split]
    for day in dates:
        task = {"kind": "grouped_daily_bars", "date": day}
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    payload = {
        "schema_version": 1,
        "artifact_kind": collection.PLAN_KIND,
        "campaign_id": "multi-strategy-portfolio-validation-v2",
        "state": "COLLECTION_PLAN_FROZEN",
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "lane": lane,
        "authority_path": str(authority_path.relative_to(tmp_path)),
        "authority_sha256": authority["artifact_sha256"],
        "binding_sha256": (
            authority["rules_hash"] if confirmation else authority["artifact_sha256"]
        ),
        "calendar_path": str(calendar.relative_to(tmp_path)),
        "calendar_sha256": collection._file_hash(calendar),
        "evaluation_dates": dates[-6:],
        "required_dates": dates,
        "warmup_sessions": 2,
        "symbols": symbols,
        "tasks": tasks,
        "task_count": len(tasks),
        "providers": ["fake"],
        "provider_requests_before_plan_freeze": 0,
        "substitutions_allowed": False,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "as_of": "2026-07-27",
        **(
            {"preregistered_at": authority["recorded_at"]}
            if confirmation
            else {}
        ),
    }
    plan_path, plan = strategy_discovery._write_artifact(
        payload, tmp_path / "plans", "plan"
    )
    return plan_path, plan, symbols


def test_plan_and_provider_access_fail_before_week_reset(tmp_path):
    with pytest.raises(collection.DenseDataCollectionError, match="closed until"):
        collection.freeze_plan(
            tmp_path / "missing.json",
            as_of=date(2026, 7, 22),
            enforce_commit=False,
        )
    with pytest.raises(
        collection.DenseDataCollectionError,
        match="cannot be future-dated",
    ):
        collection.freeze_plan(
            tmp_path / "missing.json",
            as_of=date(2026, 7, 27),
            actual_today=date(2026, 7, 22),
            enforce_commit=False,
        )
    with pytest.raises(
        collection.DenseDataCollectionError,
        match="cannot be future-dated",
    ):
        collection.collect(
            tmp_path / "missing.json",
            as_of=date(2026, 7, 27),
            enforce_commit=False,
            clock=lambda: datetime(2026, 7, 22, 12, 0, tzinfo=UTC),
        )


def test_confirmation_authority_reopens_committed_development_chain(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(collection, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(strategy_discovery, "PROJECT_ROOT", tmp_path)
    contract = {
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "development_dates": _dates(6),
        "confirmation_dates": _dates(6),
        "development_scope": {"dates": _dates(6), "symbols": ["SPY"]},
        "confirmation_scope": {"dates": _dates(6), "symbols": ["QQQ"]},
        "implementation_hashes": {"dense_strategy_plugin.py": "a" * 64},
    }
    search_path, search = strategy_discovery._write_artifact(
        {
            "artifact_kind": "frozen-development-search",
            "state": "SEARCH_FROZEN",
            "family_contract": contract,
        },
        tmp_path / "chain",
        "search",
    )
    result_path, result = strategy_discovery._write_artifact(
        {
            "artifact_kind": "development-search-result",
            "search_path": str(search_path.relative_to(tmp_path)),
            "search_sha256": search["artifact_sha256"],
        },
        tmp_path / "chain",
        "result",
    )
    inspection_path, inspected = strategy_discovery._write_artifact(
        {
            "artifact_kind": "development-search-inspection",
            "state": "WINNER_SELECTED",
            "result_path": str(result_path.relative_to(tmp_path)),
            "result_sha256": result["artifact_sha256"],
        },
        tmp_path / "chain",
        "inspection",
    )
    winner_path, winner = strategy_discovery._write_artifact(
        {
            "artifact_kind": "frozen-strategy-winner",
            "state": "WINNER_FROZEN",
            "family_id": contract["family_id"],
            "rules_hash": "b" * 64,
            "development_inspection_path": str(
                inspection_path.relative_to(tmp_path)
            ),
            "development_inspection_sha256": inspected["artifact_sha256"],
            "development_dates": contract["development_dates"],
            "confirmation_dates": contract["confirmation_dates"],
            "development_scope": contract["development_scope"],
            "confirmation_scope": contract["confirmation_scope"],
            "implementation_hashes": contract["implementation_hashes"],
        },
        tmp_path / "chain",
        "winner",
    )
    checked = []
    monkeypatch.setattr(
        strategy_discovery,
        "require_committed",
        lambda path: checked.append(path.resolve()),
    )

    authority, observed_contract, binding = collection._authority(
        winner_path,
        lane="confirmation",
        enforce_commit=True,
    )

    assert authority == winner
    assert observed_contract == contract
    assert binding == winner["rules_hash"]
    assert checked == [
        winner_path.resolve(),
        inspection_path.resolve(),
        result_path.resolve(),
        search_path.resolve(),
    ]


def test_confirmation_authority_rejects_detached_development_chain(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(collection, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(strategy_discovery, "PROJECT_ROOT", tmp_path)
    inspection_path, _inspection = strategy_discovery._write_artifact(
        {
            "artifact_kind": "development-search-inspection",
            "state": "WINNER_SELECTED",
            "result_path": "unused.json",
            "result_sha256": "a" * 64,
        },
        tmp_path / "chain",
        "inspection",
    )
    winner_path, _winner = strategy_discovery._write_artifact(
        {
            "artifact_kind": "frozen-strategy-winner",
            "state": "WINNER_FROZEN",
            "development_inspection_path": str(
                inspection_path.relative_to(tmp_path)
            ),
            "development_inspection_sha256": "0" * 64,
        },
        tmp_path / "chain",
        "winner",
    )

    with pytest.raises(
        collection.DenseDataCollectionError,
        match="inspection binding drifted",
    ):
        collection._authority(
            winner_path,
            lane="confirmation",
            enforce_commit=False,
        )


@pytest.mark.parametrize(
    ("family_id", "symbols", "expected_tasks"),
    [
        (runtime.EQUITY_RESIDUAL_FAMILY, [], 441),
        (
            runtime.INTRADAY_ETF_FAMILY,
            ["SPY", "QQQ", "IWM", "DIA", "XLF", "XLK", "XLE", "XLV"],
            1_440,
        ),
        (
            runtime.ETF_PULLBACK_FAMILY,
            [
                "SPY",
                "QQQ",
                "IWM",
                "DIA",
                "EFA",
                "EEM",
                "IEF",
                "TLT",
                "GLD",
                "DBC",
                "XLB",
                "XLE",
                "XLF",
                "XLI",
                "XLK",
                "XLP",
                "XLU",
                "XLV",
                "XLY",
            ],
            321,
        ),
    ],
)
def test_frozen_search_derives_exact_warmup_and_request_plan(
    tmp_path, monkeypatch, family_id, symbols, expected_tasks
):
    monkeypatch.setattr(collection, "PROJECT_ROOT", tmp_path)
    days = _dates(600)
    calendar = tmp_path / "calendar.json"
    calendar.write_text(
        json.dumps(
            [
                {"date": day, "open_et": "09:30", "close_et": "16:00"}
                for day in days
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{family_id}-capacity",
            "registered_at": "2026-07-27T08:00:00-04:00",
            "requested_dates": days[250:410],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["tests/test_dense_data_collection.py"],
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": family_id,
                    "formal_capacity": 1_000,
                    "calendar_sha256": collection._file_hash(calendar),
                },
            },
        },
        tmp_path / "capacity",
    )
    contract = {
        "family_id": family_id,
        "capacity_manifest": str(capacity_path),
        "development_warmup_dates": days[
            250
            - (60 if family_id == runtime.INTRADAY_ETF_FAMILY else 200): 250
        ],
        "confirmation_warmup_dates": days[
            375
            - (60 if family_id == runtime.INTRADAY_ETF_FAMILY else 200): 375
        ],
        "development_dates": days[250:370],
        "confirmation_dates": days[375:410],
        "universe": {"symbols": symbols} if symbols else {"security_type": "COMMON"},
    }
    search_path, _search = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": "frozen-development-search",
            "state": "SEARCH_FROZEN",
            "family_contract": contract,
        },
        tmp_path / "search",
        "search",
    )

    _path, plan = collection.freeze_plan(
        search_path,
        as_of=date(2026, 7, 27),
        actual_today=date(2026, 7, 27),
        calendar_path=calendar,
        public_root=tmp_path / "public",
        enforce_commit=False,
    )

    assert plan["task_count"] == expected_tasks
    assert plan["required_dates"][0] == days[50 if family_id != runtime.INTRADAY_ETF_FAMILY else 190]
    assert plan["evaluation_dates"] == days[250:370]
    assert plan["provider_requests_before_plan_freeze"] == 0


def test_existing_successor_plan_uses_frozen_alpaca_daily_provider(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(collection, "PROJECT_ROOT", tmp_path)
    days = _dates(1_200)
    calendar = tmp_path / "calendar.json"
    calendar.write_text(
        json.dumps(
            [
                {"date": day, "open_et": "09:30", "close_et": "16:00"}
                for day in days
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    authority_path = tmp_path / "search.json"
    authority_path.write_text("{}\n", encoding="utf-8")
    contract = {
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "research_generation": (
            collection.continuous_strategy_discovery.RESEARCH_GENERATION
        ),
        "development_dates": days[200:],
        "development_warmup_dates": days[:200],
        "universe": {"symbols": ["SPY", "QQQ", "IWM", "DIA"]},
        "historical_data_contract": {
            "daily_provider": "alpaca",
            "daily_endpoint": "/v2/stocks/{symbol}/bars",
            "daily_feed": "sip",
            "daily_adjustment": "raw",
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        },
    }
    authority = {"artifact_sha256": "a" * 64}
    monkeypatch.setattr(
        collection,
        "_authority",
        lambda *_args, **_kwargs: (authority, contract, authority["artifact_sha256"]),
    )
    monkeypatch.setattr(
        collection,
        "_existing_successor_authorized",
        lambda *_args, **_kwargs: True,
    )
    monkeypatch.setattr(
        collection,
        "_capacity_calendar_hash",
        lambda *_args, **_kwargs: collection._file_hash(calendar),
    )

    _path, plan = collection.freeze_plan(
        authority_path,
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        calendar_path=calendar,
        public_root=tmp_path / "public",
        enforce_commit=False,
    )

    assert plan["daily_provider"] == "alpaca"
    assert plan["task_count"] == 5
    assert [task["kind"] for task in plan["tasks"]] == [
        "split_actions",
        "daily_symbol_bars",
        "daily_symbol_bars",
        "daily_symbol_bars",
        "daily_symbol_bars",
    ]


def test_collection_is_resumable_idempotent_and_independently_inspected(
    tmp_path, monkeypatch
):
    plan_path, _plan, symbols = _artifacts(tmp_path, monkeypatch)
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    public = tmp_path / "public"
    interrupted = FakeBackend(symbols, fail_after=3)
    with pytest.raises(collection.DenseDataCollectionError, match="interruption"):
        collection.collect(
            plan_path,
            as_of=date(2026, 7, 27),
            store_config=config,
            public_root=public,
            backend=interrupted,
            enforce_commit=False,
            clock=_collection_clock,
        )
    assert len(interrupted.calls) == 3

    resumed = FakeBackend(symbols)
    status_path, status = collection.collect(
        plan_path,
        as_of=date(2026, 7, 27),
        store_config=config,
        public_root=public,
        backend=resumed,
        enforce_commit=False,
        clock=_collection_clock,
    )
    assert status["completed_tasks"] == status["task_count"] == 9
    assert status["provider_telemetry"]["cache_hits"] == 3
    assert len(resumed.calls) == 6
    assert status["collection_started_at"] == "2026-07-27T13:00:00Z"
    assert status["collection_completed_at"] == "2026-07-27T13:00:00Z"

    warm = FakeBackend(symbols)
    repeated_path, repeated = collection.collect(
        plan_path,
        as_of=date(2026, 7, 27),
        store_config=config,
        public_root=public,
        backend=warm,
        enforce_commit=False,
        clock=_collection_clock,
    )
    assert repeated_path == status_path
    assert repeated == status
    assert warm.calls == []

    with pytest.raises(
        inspection.DenseDataInspectionError,
        match="inspection must follow",
    ):
        inspection.inspect(
            status_path,
            inspected_at="2026-07-27T08:30:00-04:00",
            store_config=config,
            public_root=public,
            enforce_commit=False,
        )

    inspection_path, inspected, manifest_path, manifest = inspection.inspect(
        status_path,
        inspected_at="2026-07-27T12:00:00-04:00",
        store_config=config,
        public_root=public,
        enforce_commit=False,
    )
    assert inspection_path.is_file()
    assert inspected["state"] == "DATASET_INSPECTED_READY"
    assert inspected["formal_capacity"] == 114
    assert manifest_path.is_file()
    assert manifest["dataset_payload"]["development_search_sha256"] == status[
        "binding_sha256"
    ]
    assert manifest["dataset_payload"]["dense_runtime"]["formal_capacity"] == 114


def test_collection_retries_retryable_provider_failures_with_visible_pacing(
    tmp_path, monkeypatch
):
    plan_path, _plan, symbols = _artifacts(tmp_path, monkeypatch)
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    backend = RetryBackend(symbols, 2, retry_after=2.5)
    waits = []

    _status_path, status = collection.collect(
        plan_path,
        as_of=date(2026, 7, 27),
        store_config=config,
        public_root=tmp_path / "public",
        backend=backend,
        enforce_commit=False,
        retry_sleeper=waits.append,
        clock=_collection_clock,
    )

    assert waits == [2.5, 2.5]
    assert status["provider_telemetry"]["failures"] == 2
    assert status["provider_telemetry"]["pacing_wait_seconds"] == 5.0
    assert status["completed_tasks"] == status["task_count"]


def test_collection_does_not_retry_permanent_provider_failure(tmp_path, monkeypatch):
    plan_path, _plan, symbols = _artifacts(tmp_path, monkeypatch)
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    backend = RetryBackend(symbols, 1, retryable=False)
    waits = []

    with pytest.raises(collection.DenseDataCollectionError, match="provider failure"):
        collection.collect(
            plan_path,
            as_of=date(2026, 7, 27),
            store_config=config,
            public_root=tmp_path / "public",
            backend=backend,
            enforce_commit=False,
            retry_sleeper=waits.append,
            clock=_collection_clock,
        )

    assert waits == []
    assert len(backend.calls) == 1
    assert backend.telemetry["failures"] == 1


def test_confirmation_manifest_attests_capture_after_frozen_winner(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        collection.outcome_exposure,
        "assert_untouched",
        lambda *_args, **_kwargs: None,
    )
    plan_path, plan, symbols = _artifacts(
        tmp_path, monkeypatch, lane="confirmation"
    )
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    public = tmp_path / "public"
    status_path, _status = collection.collect(
        plan_path,
        as_of=date(2026, 7, 27),
        store_config=config,
        public_root=public,
        backend=FakeBackend(symbols),
        enforce_commit=False,
        clock=_collection_clock,
    )
    _inspection_path, _inspected, _manifest_path, manifest = inspection.inspect(
        status_path,
        inspected_at="2026-07-27T12:00:00-04:00",
        store_config=config,
        public_root=public,
        enforce_commit=False,
    )
    payload = manifest["dataset_payload"]
    assert payload["preregistration_sha256"] == plan["binding_sha256"]
    assert payload["preregistered_at"] == plan["preregistered_at"]
    assert payload["collection_started_at"] == "2026-07-27T13:00:00Z"
    assert payload["collection_completed_at"] == "2026-07-27T13:00:00Z"
    assert payload["capture_after_preregistration_attested"] is True
    assert "development_search_sha256" not in payload


def test_confirmation_collection_rejects_start_before_winner_freeze(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(
        collection.outcome_exposure,
        "assert_untouched",
        lambda *_args, **_kwargs: None,
    )
    plan_path, _plan, symbols = _artifacts(
        tmp_path, monkeypatch, lane="confirmation"
    )
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)

    with pytest.raises(
        collection.DenseDataCollectionError,
        match="must start after winner preregistration",
    ):
        collection.collect(
            plan_path,
            as_of=date(2026, 7, 27),
            store_config=config,
            public_root=tmp_path / "public",
            backend=FakeBackend(symbols),
            enforce_commit=False,
            clock=lambda: datetime(2026, 7, 27, 11, 59, tzinfo=UTC),
        )


def test_split_adjustment_uses_only_actions_through_frozen_dataset_end():
    adjusted = collection._adjusted_bar(
        {
            "open": 100.0,
            "high": 101.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 1_000,
        },
        "2024-01-02",
        [("2024-01-03", 0.5), ("2023-12-01", 0.25)],
    )
    assert adjusted["close"] == 50.0
    assert adjusted["volume"] == 2_000
