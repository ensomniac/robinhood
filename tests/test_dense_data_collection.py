from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta

import pytest

import dense_collection_recovery as recovery
import dense_collection_recovery_inspection as recovery_inspection
import dense_data_collection as collection
import dense_data_collection_inspection as inspection
import dense_strategy_runtime as runtime
import outcome_exposure
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
    monkeypatch.setattr(recovery, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(recovery_inspection, "PROJECT_ROOT", tmp_path)
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
                else {
                    "family_contract": {
                        "family_id": runtime.ETF_PULLBACK_FAMILY,
                        "frozen_semantics": "unchanged",
                        "implementation_hashes": {
                            "dense_strategy_plugin.py": "a" * 64
                        },
                    }
                }
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


def test_plan_and_provider_access_fail_before_rolling_authorization(tmp_path):
    with pytest.raises(
        collection.DenseDataCollectionError,
        match="not authorized before",
    ):
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


def test_country_intraday_range_plan_uses_one_task_per_symbol(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(collection, "PROJECT_ROOT", tmp_path)
    days = _dates(70)
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
    symbols = ["EWC", "EWG", "EWP", "EWQ", "EWT", "EWU", "EWW", "EWY"]
    contract = {
        "family_id": runtime.COUNTRY_ETF_OPENING_REVERSAL_FAMILY,
        "research_generation": (
            collection.continuous_strategy_discovery.RESEARCH_GENERATION
        ),
        "development_warmup_dates": days[:60],
        "development_dates": days[60:],
        "universe": {"symbols": symbols},
        "historical_data_contract": {
            "minute_request_mode": "symbol_range",
        },
    }
    authority = {"artifact_sha256": "a" * 64}
    monkeypatch.setattr(
        collection,
        "_authority",
        lambda *_args, **_kwargs: (
            authority,
            contract,
            authority["artifact_sha256"],
        ),
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

    assert plan["task_count"] == len(symbols)
    assert [task["symbol"] for task in plan["tasks"]] == symbols
    assert all(
        task["kind"] == "sip_minute_symbol_range"
        and task["start"] == days[0]
        and task["date"] == days[-1]
        for task in plan["tasks"]
    )


def test_intraday_range_dataset_rebuilds_date_symbol_sessions(tmp_path):
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    days = _dates(2)
    symbols = ["EWC", "EWG"]
    tasks = []
    for symbol in symbols:
        task = {
            "kind": "sip_minute_symbol_range",
            "start": days[0],
            "date": days[-1],
            "symbol": symbol,
        }
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    plan = {
        "family_id": runtime.COUNTRY_ETF_OPENING_REVERSAL_FAMILY,
        "evaluation_dates": days,
        "required_dates": days,
        "symbols": symbols,
        "tasks": tasks,
    }
    for task in tasks:
        rows = [
            {
                "date_et": day,
                "time_et": f"{day}T09:30:00-05:00",
                "open": 100.0,
                "high": 100.2,
                "low": 99.8,
                "close": 100.1,
                "volume": 1_000,
                "wap": 100.05,
            }
            for day in days
        ]
        collection._write_external(
            collection._checkpoint_path(tmp_path, task),
            {
                "schema_version": 1,
                "task": task,
                "rows": rows,
                "rows_sha256": canonical_sha256(rows),
            },
            config,
        )

    dataset = collection.build_dataset(tmp_path, plan)

    assert set(dataset["minute_bars"]) == set(days)
    assert all(
        set(dataset["minute_bars"][day]) == set(symbols)
        for day in days
    )


def test_intraday_range_policy_keeps_only_complete_frozen_sessions(
    tmp_path,
):
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    days = _dates(3)
    symbols = ["DIA", "SPY"]
    tasks = []
    for symbol in symbols:
        task = {
            "kind": "sip_minute_symbol_range",
            "start": days[0],
            "date": days[-1],
            "symbol": symbol,
        }
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    plan = {
        "family_id": runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_FAMILY,
        "evaluation_dates": days,
        "required_dates": days,
        "symbols": symbols,
        "tasks": tasks,
        "intraday_missing_session_policy": (
            collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
        ),
    }

    def rows(day, count):
        start = datetime.fromisoformat(f"{day}T09:30:00-05:00")
        return [
            {
                "date_et": day,
                "time_et": (start + timedelta(minutes=index)).isoformat(),
                "open": 100.0,
                "high": 100.2,
                "low": 99.8,
                "close": 100.1,
                "volume": 1_000,
                "wap": 100.05,
            }
            for index in range(count)
        ]

    for task in tasks:
        task_rows = [
            *rows(days[0], 390),
            *rows(
                days[1],
                389 if task["symbol"] == "DIA" else 390,
            ),
            *rows(days[2], 389),
            *rows("2024-01-05", 210),
        ]
        collection._write_external(
            collection._checkpoint_path(tmp_path, task),
            {
                "schema_version": 1,
                "task": task,
                "rows": task_rows,
                "rows_sha256": canonical_sha256(task_rows),
            },
            config,
        )

    dataset = collection.build_dataset(tmp_path, plan)
    prepared = runtime.prepare_dataset(dataset)

    assert set(dataset["minute_bars"]) == set(days)
    assert set(dataset["minute_bars"][days[0]]) == set(symbols)
    assert set(dataset["minute_bars"][days[1]]) == {"SPY"}
    assert dataset["minute_bars"][days[2]] == {}
    assert dataset["missed_data_dates"] == [days[1], days[2]]
    assert dataset["missing_session_evidence"] == [
        {
            "date": days[1],
            "symbol": "DIA",
            "observed_minutes": 389,
            "expected_minutes": 390,
        },
        {
            "date": days[2],
            "symbol": "DIA",
            "observed_minutes": 389,
            "expected_minutes": 390,
        },
        {
            "date": days[2],
            "symbol": "SPY",
            "observed_minutes": 389,
            "expected_minutes": 390,
        },
    ]
    assert prepared["_prepared_missed_data_dates"] == {
        days[1],
        days[2],
    }
    assert (
        dataset["source_semantics"]["missing_data_policy"]
        == collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
    )
    assert dataset["source_semantics"]["interpolation"] == "forbidden"
    assert dataset["source_semantics"]["substitution"] == "forbidden"


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


def test_failed_grouped_daily_task_is_recorded_without_outcome_exposure(
    tmp_path,
    monkeypatch,
):
    plan_path, _plan, symbols = _artifacts(tmp_path, monkeypatch)
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    backend = FakeBackend(symbols, fail_after=1)
    with pytest.raises(collection.DenseDataCollectionError, match="interruption"):
        collection.collect(
            plan_path,
            as_of=date(2026, 7, 27),
            store_config=config,
            public_root=tmp_path / "public",
            backend=backend,
            enforce_commit=False,
            clock=_collection_clock,
        )

    failure_path, failure = recovery.record_failure(
        plan_path,
        recorded_at="2026-07-27T13:01:00+00:00",
        store_config=config,
        public_root=tmp_path / "public",
        enforce_commit=False,
    )

    assert failure_path.is_file()
    assert failure["failure_code"] == recovery.GROUPED_DAILY_FAILURE
    assert failure["completed_tasks"] == 1
    assert failure["market_price_rows_accessed"] == 0
    assert failure["failure_details"]["corporate_action_rows_accessed"] == 0
    assert failure["failure_details"]["failed_task_kind"] == (
        "grouped_daily_bars"
    )
    assert failure["data_outcomes_accessed"] is False
    assert failure["strategy_metrics_accessed"] is False
    assert failure["confirmation_outcomes_accessed"] is False
    inspection_path, inspected = recovery_inspection.inspect(
        failure_path,
        inspected_at="2026-07-27T13:02:00+00:00",
        store_config=config,
        public_root=tmp_path / "public",
        enforce_commit=False,
    )
    assert inspection_path.is_file()
    assert inspected["state"] == recovery_inspection.INSPECTION_STATE
    assert all(inspected["checks"].values())


def test_failed_massive_symbol_task_has_exact_outcome_blind_failure_code(
    tmp_path,
    monkeypatch,
):
    plan_path, plan, symbols = _artifacts(tmp_path, monkeypatch)
    plan_value = json.loads(plan_path.read_text(encoding="utf-8"))
    plan_value.pop("artifact_sha256")
    for task in plan_value["tasks"][1:]:
        task["kind"] = "massive_daily_symbol_bars"
        task["task_id"] = canonical_sha256(
            {key: value for key, value in task.items() if key != "task_id"}
        )
    massive_plan_path, massive_plan = strategy_discovery._write_artifact(
        plan_value,
        tmp_path / "massive-plan",
        "massive-plan",
    )
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    backend = FakeBackend(symbols, fail_after=1)
    with pytest.raises(collection.DenseDataCollectionError, match="interruption"):
        collection.collect(
            massive_plan_path,
            as_of=date(2026, 7, 27),
            store_config=config,
            public_root=tmp_path / "public",
            backend=backend,
            enforce_commit=False,
            clock=_collection_clock,
        )

    failure_path, failure = recovery.record_failure(
        massive_plan_path,
        recorded_at="2026-07-27T13:01:00+00:00",
        store_config=config,
        public_root=tmp_path / "public",
        enforce_commit=False,
    )

    assert failure_path.is_file()
    assert failure["plan_sha256"] == massive_plan["artifact_sha256"]
    assert failure["failure_code"] == (
        recovery.MASSIVE_DAILY_SYMBOL_FAILURE
    )
    assert failure["failure_details"]["failed_task_kind"] == (
        "massive_daily_symbol_bars"
    )
    assert failure["market_price_rows_accessed"] == 0
    assert failure["data_outcomes_accessed"] is False


class DailyRangeBackend:
    def __init__(self, dates):
        self.dates = dates
        self.telemetry = {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 0,
            "failures": 0,
        }

    def fetch(self, task):
        self.telemetry["requests"] += 1
        if task["kind"] == "split_actions":
            return []
        result = []
        for day in self.dates:
            result.append(
                {
                    "symbol": task["symbol"],
                    "date": day,
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "volume": 2_000_000,
                    "count": 100,
                    "wap": 100.25,
                }
            )
        return result

    def close(self):
        return None


def _pullback_recovery_plan(tmp_path, monkeypatch, *, refresh_search=False):
    plan_path, plan, symbols = _artifacts(tmp_path, monkeypatch)
    monkeypatch.setattr(
        recovery,
        "_implementation_hashes",
        lambda **_kwargs: {
            "dense_collection_recovery.py": "c" * 64,
            "dense_collection_recovery_inspection.py": "i" * 64,
            "dense_data_collection.py": "d" * 64,
        },
    )
    failure_path, failure = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": recovery.FAILURE_KIND,
            "campaign_id": plan["campaign_id"],
            "state": recovery.FAILURE_STATE,
            "family_id": plan["family_id"],
            "lane": plan["lane"],
            "plan_path": str(plan_path.relative_to(tmp_path)),
            "plan_sha256": plan["artifact_sha256"],
            "authority_sha256": plan["authority_sha256"],
            "binding_sha256": plan["binding_sha256"],
            "failure_code": recovery.GROUPED_DAILY_FAILURE,
            "completed_tasks": 1,
            "task_count": plan["task_count"],
            "market_price_rows_accessed": 0,
            "evaluation_tasks_completed": 0,
            "data_outcomes_accessed": False,
            "exposure_scope": None,
            "strategy_metrics_accessed": False,
            "confirmation_outcomes_accessed": False,
            "substitutions": 0,
            "broker_actions": 0,
            "recorded_at": "2026-07-27T13:01:00+00:00",
        },
        tmp_path / "failures",
        "failure",
    )
    inspection_path, _inspection = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": recovery_inspection.INSPECTION_KIND,
            "campaign_id": plan["campaign_id"],
            "state": recovery_inspection.INSPECTION_STATE,
            "family_id": plan["family_id"],
            "lane": plan["lane"],
            "failure_path": str(failure_path.relative_to(tmp_path)),
            "failure_sha256": failure["artifact_sha256"],
            "plan_sha256": plan["artifact_sha256"],
            "failure_code": recovery.GROUPED_DAILY_FAILURE,
            "data_outcomes_accessed": False,
            "exposure_scope": None,
            "strategy_metrics_accessed": False,
            "confirmation_outcomes_accessed": False,
            "checks": {"all": True},
        },
        tmp_path / "failure-inspections",
        "inspection",
    )
    search_path = None
    if refresh_search:
        original_search = strategy_discovery.load_artifact(
            tmp_path / plan["authority_path"],
            expected_kind="frozen-development-search",
        )
        refreshed_contract = dict(original_search["family_contract"])
        refreshed_contract["implementation_hashes"] = {
            "dense_strategy_plugin.py": "b" * 64
        }
        search_path, _search = strategy_discovery._write_artifact(
            {
                "schema_version": 1,
                "artifact_kind": "frozen-development-search",
                "state": "SEARCH_FROZEN",
                "family_contract": refreshed_contract,
            },
            tmp_path / "refreshed-search",
            "search",
        )
        monkeypatch.setattr(
            strategy_discovery,
            "_assert_implementation_current",
            lambda *_args, **_kwargs: None,
        )
    recovery_path, recovery_plan = recovery.freeze_pullback_recovery(
        inspection_path,
        search_path=search_path,
        as_of=date(2026, 7, 27),
        actual_today=date(2026, 7, 27),
        public_root=tmp_path / "public",
        enforce_commit=False,
    )
    assert recovery_plan["recovery_failure_sha256"] == failure["artifact_sha256"]
    assert recovery_plan["supersedes_plan_sha256"] == plan["artifact_sha256"]
    assert recovery_plan["task_count"] == len(symbols) + 1
    return recovery_path, recovery_plan


def test_pullback_recovery_refreshes_only_implementation_bound_search(
    tmp_path,
    monkeypatch,
):
    _path, plan = _pullback_recovery_plan(
        tmp_path,
        monkeypatch,
        refresh_search=True,
    )

    refresh = plan["recovery_search_refresh"]
    assert refresh["only_implementation_hashes_changed"] is True
    assert refresh["refreshed_search_sha256"] == plan["authority_sha256"]


def test_pullback_recovery_uses_raw_alpaca_bars_and_frozen_split_actions(
    tmp_path,
    monkeypatch,
):
    recovery_path, plan = _pullback_recovery_plan(tmp_path, monkeypatch)
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)

    _status_path, status = collection.collect(
        recovery_path,
        as_of=date(2026, 7, 27),
        store_config=config,
        public_root=tmp_path / "public",
        backend=DailyRangeBackend(plan["required_dates"]),
        enforce_commit=False,
        clock=_collection_clock,
    )

    assert status["completed_tasks"] == status["task_count"] == 20
    external = (
        config.root / status["external_relative_path"]
    )
    dataset = inspection._load_external(external)
    assert dataset["source_semantics"]["feed"] == (
        "Alpaca SIP daily symbol range"
    )
    assert "frozen Massive split actions" in dataset["source_semantics"][
        "adjustment"
    ]
    assert dataset["daily_bars"]["SPY"][0]["close"] == 100.5


def test_cross_style_massive_failure_freezes_source_only_alpaca_recovery(
    tmp_path,
):
    failure_inspection = (
        collection.PROJECT_ROOT
        / "strategy_tournament/v2/discovery/"
        "cross-style-etf-breadth-continuation/"
        "development-collection-failure-inspection/"
        "cross-style-etf-breadth-continuation-development-collection-"
        "failure-inspection-1812d98042611a45a0a9d58ee6dc953f0a9f2ea"
        "882e4d1b0c33268d7c45660e6.json"
    )
    _path, plan = recovery.freeze_pullback_recovery(
        failure_inspection,
        as_of=date(2026, 7, 25),
        actual_today=date(2026, 7, 25),
        public_root=tmp_path,
        enforce_commit=False,
    )

    assert plan["family_id"] == (
        runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY
    )
    assert plan["adjustment_semantics"] == collection.RECOVERY_ADJUSTMENT
    assert plan["task_count"] == 9
    assert plan["tasks"][0]["kind"] == "split_actions"
    assert {
        task["kind"] for task in plan["tasks"][1:]
    } == {"daily_symbol_bars"}
    assert plan["market_outcomes_accessed"] is False
    assert plan["substitutions_allowed"] is False


def test_pullback_recovery_rejects_implementation_hash_drift(
    tmp_path,
    monkeypatch,
):
    recovery_path, _plan = _pullback_recovery_plan(tmp_path, monkeypatch)
    artifact = json.loads(recovery_path.read_text(encoding="utf-8"))
    artifact["recovery_implementation_hashes"]["dense_data_collection.py"] = (
        "0" * 64
    )
    artifact.pop("artifact_sha256")
    drifted_path, _drifted = strategy_discovery._write_artifact(
        artifact,
        tmp_path / "drifted",
        "recovery",
    )
    monkeypatch.setattr(
        strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(
        strategy_discovery,
        "_file_hash",
        lambda path: (
            "c" * 64
            if path.name == "dense_collection_recovery.py"
            else "i" * 64
            if path.name == "dense_collection_recovery_inspection.py"
            else "d" * 64
        ),
    )
    with pytest.raises(
        collection.DenseDataCollectionError,
        match="recovery implementation drifted",
    ):
        collection._validate_plan(drifted_path, enforce_commit=True)


def test_incomplete_intraday_collection_is_indexed_as_development_exposure(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setattr(collection, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(recovery, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(recovery_inspection, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(strategy_discovery, "PROJECT_ROOT", tmp_path)
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    tasks = []
    for day in ("2024-01-02", "2024-01-03"):
        task = {
            "kind": "sip_minute_bars",
            "date": day,
            "symbol": "SPY",
        }
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    plan = {
        "artifact_sha256": "a" * 64,
        "family_id": runtime.INTRADAY_ETF_FAMILY,
        "lane": "development",
        "evaluation_dates": ["2024-01-03"],
        "symbols": ["SPY"],
        "task_count": 2,
        "tasks": tasks,
    }
    root = (
        config.root
        / "dense-v2"
        / plan["family_id"]
        / plan["lane"]
        / plan["artifact_sha256"]
    )
    for task in tasks:
        rows = [
            {
                "time_et": (
                    f"{task['date']}T09:{30 + index:02d}:00-05:00"
                ),
            }
            for index in range(2)
        ]
        collection._write_external(
            collection._checkpoint_path(root, task),
            {
                "schema_version": 1,
                "task": task,
                "rows": rows,
                "rows_sha256": canonical_sha256(rows),
            },
            config,
        )
    facts = recovery._failure_facts(
        plan,
        root,
        {"failures": 0},
    )
    assert facts["failure_code"] == recovery.INCOMPLETE_INTRADAY
    assert facts["data_outcomes_accessed"] is True
    assert facts["exposure_scope"] == {
        "dates": ["2024-01-03"],
        "symbols": ["SPY"],
    }
    failure_path, _failure = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": recovery.FAILURE_KIND,
            "campaign_id": "multi-strategy-portfolio-validation-v2",
            "state": recovery.FAILURE_STATE,
            "family_id": plan["family_id"],
            "lane": plan["lane"],
            "plan_sha256": "a" * 64,
            "failure_code": recovery.INCOMPLETE_INTRADAY,
            "data_outcomes_accessed": True,
            "exposure_scope": facts["exposure_scope"],
            "strategy_metrics_accessed": False,
            "confirmation_outcomes_accessed": False,
            "substitutions": 0,
            "broker_actions": 0,
            "recorded_at": "2026-07-27T13:01:00+00:00",
        },
        tmp_path / "failures",
        "failure",
    )
    inspection_path, _inspection = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": recovery_inspection.INSPECTION_KIND,
            "campaign_id": "multi-strategy-portfolio-validation-v2",
            "state": recovery_inspection.INSPECTION_STATE,
            "family_id": plan["family_id"],
            "lane": plan["lane"],
            "failure_path": str(failure_path.relative_to(tmp_path)),
            "failure_sha256": _failure["artifact_sha256"],
            "plan_sha256": "a" * 64,
            "failure_code": recovery.INCOMPLETE_INTRADAY,
            "data_outcomes_accessed": True,
            "exposure_scope": facts["exposure_scope"],
            "strategy_metrics_accessed": False,
            "confirmation_outcomes_accessed": False,
            "checks": {"all": True},
        },
        tmp_path / "failure-inspections",
        "inspection",
    )
    index = tmp_path / "OUTCOME_EXPOSURE_INDEX.jsonl"
    record, appended = recovery.index_failure_exposure(
        inspection_path,
        index_path=index,
        enforce_commit=False,
    )
    assert appended is True
    assert record["lane"] == "development"
    assert outcome_exposure.read_index(index) == [record]


def test_incomplete_fixed_daily_range_records_exact_missing_sessions(
    tmp_path,
):
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    required_dates = ["2024-01-02", "2024-01-03"]
    tasks = []
    for symbol in ("IWB", "SPTM"):
        task = {
            "kind": "daily_symbol_bars",
            "start": required_dates[0],
            "date": required_dates[-1],
            "symbol": symbol,
        }
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    plan = {
        "artifact_sha256": "d" * 64,
        "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
        "lane": "development",
        "required_dates": required_dates,
        "evaluation_dates": required_dates,
        "symbols": ["IWB", "SPTM"],
        "task_count": len(tasks),
        "tasks": tasks,
    }
    root = (
        config.root
        / "dense-v2"
        / plan["family_id"]
        / plan["lane"]
        / plan["artifact_sha256"]
    )
    for task in tasks:
        observed_dates = (
            required_dates
            if task["symbol"] == "IWB"
            else required_dates[1:]
        )
        rows = [
            {
                "symbol": task["symbol"],
                "date": day,
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1_000,
            }
            for day in observed_dates
        ]
        collection._write_external(
            collection._checkpoint_path(root, task),
            {
                "schema_version": 1,
                "task": task,
                "rows": rows,
                "rows_sha256": canonical_sha256(rows),
            },
            config,
        )

    facts = recovery._failure_facts(plan, root, {"failures": 0})

    assert facts["failure_code"] == recovery.INCOMPLETE_FIXED_DAILY_RANGE
    assert facts["data_outcomes_accessed"] is True
    assert facts["failure_details"]["missing_required_symbol_sessions"] == 1
    assert facts["failure_details"]["per_symbol"][1][
        "missing_required_sessions"
    ] == ["2024-01-02"]


def test_symbol_range_failure_summarizes_incomplete_regular_sessions(
    tmp_path,
):
    required_dates = ["2024-01-02", "2024-01-03"]
    symbols = ["EWC", "EWG"]
    tasks = []
    for symbol in symbols:
        task = {
            "kind": "sip_minute_symbol_range",
            "start": required_dates[0],
            "date": required_dates[-1],
            "symbol": symbol,
        }
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    plan = {
        "artifact_sha256": "a" * 64,
        "family_id": runtime.COUNTRY_ETF_OPENING_REVERSAL_FAMILY,
        "lane": "development",
        "required_dates": required_dates,
        "evaluation_dates": [required_dates[-1]],
        "symbols": symbols,
        "task_count": len(tasks),
        "tasks": tasks,
    }
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    root = (
        config.root
        / "dense-v2"
        / plan["family_id"]
        / plan["lane"]
        / plan["artifact_sha256"]
    )

    def rows(day, count):
        start = datetime.fromisoformat(f"{day}T09:30:00-05:00")
        return [
            {
                "date_et": day,
                "time_et": (start + timedelta(minutes=index)).isoformat(),
            }
            for index in range(count)
        ]

    for task in tasks:
        task_rows = [
            *rows(required_dates[0], 390),
            *rows(
                required_dates[1],
                389 if task["symbol"] == "EWC" else 390,
            ),
            *rows("2024-01-04", 210),
        ]
        collection._write_external(
            collection._checkpoint_path(root, task),
            {
                "schema_version": 1,
                "task": task,
                "rows": task_rows,
                "rows_sha256": canonical_sha256(task_rows),
            },
            config,
        )

    facts = recovery._failure_facts(plan, root, {"failures": 0})

    assert facts["failure_code"] == recovery.INCOMPLETE_INTRADAY_RANGE
    assert facts["completed_tasks"] == 2
    assert facts["evaluation_tasks_completed"] == 2
    assert facts["data_outcomes_accessed"] is True
    assert facts["failure_details"]["required_symbol_sessions"] == 4
    assert (
        facts["failure_details"]["incomplete_required_symbol_sessions"]
        == 1
    )
    assert facts["failure_details"]["fully_complete_evaluation_dates"] == 0
    assert facts["failure_details"]["provider_extra_session_dates"] == [
        "2024-01-04"
    ]
    assert facts["failure_details"]["per_symbol"] == [
        {
            "symbol": "EWC",
            "required_sessions": 2,
            "complete_required_sessions": 1,
            "incomplete_required_sessions": 1,
            "evaluation_sessions": 1,
            "complete_evaluation_sessions": 0,
            "incomplete_evaluation_sessions": 1,
        },
        {
            "symbol": "EWG",
            "required_sessions": 2,
            "complete_required_sessions": 2,
            "incomplete_required_sessions": 0,
            "evaluation_sessions": 1,
            "complete_evaluation_sessions": 1,
            "incomplete_evaluation_sessions": 0,
        },
    ]


def test_empty_intraday_missed_date_is_an_implementation_recovery_gap(
    tmp_path,
):
    required_dates = ["2024-01-02", "2024-01-03"]
    symbols = ["DIA", "SPY"]
    tasks = []
    for symbol in symbols:
        task = {
            "kind": "sip_minute_symbol_range",
            "start": required_dates[0],
            "date": required_dates[-1],
            "symbol": symbol,
        }
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    plan = {
        "artifact_sha256": "a" * 64,
        "family_id": (
            runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY
        ),
        "lane": "development",
        "required_dates": required_dates,
        "evaluation_dates": required_dates,
        "symbols": symbols,
        "task_count": len(tasks),
        "tasks": tasks,
        "intraday_missing_session_policy": (
            collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
        ),
    }
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    root = (
        config.root
        / "dense-v2"
        / plan["family_id"]
        / plan["lane"]
        / plan["artifact_sha256"]
    )

    def rows(day, count):
        start = datetime.fromisoformat(f"{day}T09:30:00-05:00")
        return [
            {
                "date_et": day,
                "time_et": (start + timedelta(minutes=index)).isoformat(),
            }
            for index in range(count)
        ]

    for task in tasks:
        task_rows = [
            *rows(required_dates[0], 390),
            *rows(required_dates[1], 376),
        ]
        collection._write_external(
            collection._checkpoint_path(root, task),
            {
                "schema_version": 1,
                "task": task,
                "rows": task_rows,
                "rows_sha256": canonical_sha256(task_rows),
            },
            config,
        )

    facts = recovery._failure_facts(plan, root, {"failures": 0})

    assert (
        facts["failure_code"]
        == recovery.EMPTY_MISSED_DATE_REPRESENTATION
    )
    assert facts["data_outcomes_accessed"] is True
    assert facts["failure_details"][
        "fully_incomplete_evaluation_dates"
    ] == [required_dates[1]]
    assert (
        facts["failure_details"]["fully_complete_evaluation_dates"]
        == 1
    )
    assert facts["failure_details"]["substituted_sessions"] == 0
    assert facts["failure_details"]["interpolated_minutes"] == 0


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
