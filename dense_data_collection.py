"""Freeze and run resumable, search-bound dense-family market-data collection."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib
import io
import json
import os
import shutil
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol

import requests

import continuous_strategy_discovery
import dense_capacity_inventory
import dense_strategy_runtime as runtime
import next_week_discovery_batch as batch
import outcome_exposure
import scanner_replay
import strategy_discovery
from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
    MassiveConfig,
    MassiveHistoricalClient,
)
from historical_store import (
    DEFAULT_ENV_PATH,
    EASTERN,
    HistoricalStoreConfig,
    HistoricalStoreError,
    canonical_json_bytes,
    canonical_sha256,
    sha256_file,
)
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"
DEFAULT_CALENDAR = dense_capacity_inventory.DEFAULT_CALENDAR
PLAN_KIND = "dense-data-collection-plan"
STATUS_KIND = "dense-data-collection-status"
RECOVERY_ADJUSTMENT = "raw_alpaca_with_frozen_massive_split_actions"
MASSIVE_SOURCE_RECOVERY_ADJUSTMENT = (
    "raw_massive_with_frozen_massive_split_actions"
)
YAHOO_SOURCE_RECOVERY_ADJUSTMENT = (
    "raw_yahoo_with_frozen_massive_split_actions"
)
YAHOO_CHART_ENDPOINT = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
)
YAHOO_PACE_SECONDS = 0.20
INTRADAY_CHECKPOINT_REUSE_RECOVERY = (
    "empty-missed-date-checkpoint-reuse"
)
FIXED_ETF_CHECKPOINT_REUSE_RECOVERY = (
    "fixed-etf-dataset-registration-checkpoint-reuse"
)
DAILY_WARMUP_SESSIONS = 200
INTRADAY_WARMUP_SESSIONS = runtime.STANDARDIZATION_LOOKBACK
MAX_TASK_ATTEMPTS = 5
INITIAL_RETRY_DELAY_SECONDS = 1.0
MAX_RETRY_DELAY_SECONDS = 30.0
INTRADAY_FIXED_UNIVERSE_MISS_POLICY = (
    "miss-entire-fixed-universe-entry-date"
)
FIXED_DAILY_ETF_FAMILIES = {
    *runtime.ETF_PULLBACK_FAMILIES,
    runtime.SECTOR_ETF_GAP_DRIFT_FAMILY,
    runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY,
    runtime.FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
    runtime.FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
    runtime.FLIGHT_TO_SAFETY_REPLICATION_V3_FAMILY,
    runtime.ETF_RESIDUAL_REPLICATION_FAMILY,
    runtime.ETF_RESIDUAL_REPLICATION_V2_FAMILY,
    runtime.ETF_RESIDUAL_REPLICATION_V3_FAMILY,
    runtime.ETF_RESIDUAL_REPLICATION_V4_FAMILY,
    runtime.BREADTH_CAPITULATION_REBOUND_FAMILY,
    runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY,
    runtime.STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
    *runtime.ETF_OVERSOLD_FAMILIES,
    runtime.ETF_IBS_REVERSAL_FAMILY,
    runtime.ETF_CLOSE_STRENGTH_CONTINUATION_FAMILY,
}


class DenseDataCollectionError(RuntimeError):
    """A collection authority, request plan, checkpoint, or dataset is unsafe."""


class DenseCollectionBackend(Protocol):
    telemetry: dict[str, Any]

    def fetch(self, task: Mapping[str, Any]) -> list[dict[str, Any]]: ...

    def close(self) -> None: ...


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseDataCollectionError(f"path is outside repository: {path}") from exc


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DenseDataCollectionError(f"cannot read checkpoint {path}: {exc}") from exc


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(canonical_json_bytes(value) + b"\n")
    return buffer.getvalue()


def _write_external(path: Path, value: Any, config: HistoricalStoreConfig) -> None:
    encoded = _gzip_bytes(value)
    config.root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(config.root).free
    if free - len(encoded) < config.min_free_bytes:
        raise DenseDataCollectionError(
            "historical store disk reserve would be breached: "
            f"free={free} write={len(encoded)} reserve={config.min_free_bytes}"
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != encoded:
            raise DenseDataCollectionError(f"immutable external artifact drifted: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _calendar(path: Path) -> list[str]:
    return dense_capacity_inventory._calendar(path)


def _authority(
    path: Path, *, lane: str, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], str]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    if lane == "development":
        artifact = strategy_discovery.load_artifact(
            path, expected_kind="frozen-development-search"
        )
        if artifact.get("state") != "SEARCH_FROZEN":
            raise DenseDataCollectionError("development search is not frozen")
        return artifact, dict(artifact["family_contract"]), str(
            artifact["artifact_sha256"]
        )
    if lane != "confirmation":
        raise DenseDataCollectionError("lane must be development or confirmation")
    winner = strategy_discovery.load_artifact(
        path, expected_kind="frozen-strategy-winner"
    )
    if winner.get("state") != "WINNER_FROZEN":
        raise DenseDataCollectionError("confirmation winner is not frozen")
    inspection_path = PROJECT_ROOT / str(winner["development_inspection_path"])
    if enforce_commit:
        strategy_discovery.require_committed(inspection_path)
    inspection = strategy_discovery.load_artifact(
        inspection_path,
        expected_kind="development-search-inspection",
    )
    if not (
        inspection.get("state") == "WINNER_SELECTED"
        and inspection.get("artifact_sha256")
        == winner.get("development_inspection_sha256")
    ):
        raise DenseDataCollectionError(
            "winner development inspection binding drifted"
        )
    result_path = PROJECT_ROOT / str(inspection["result_path"])
    if enforce_commit:
        strategy_discovery.require_committed(result_path)
    result = strategy_discovery.load_artifact(
        result_path,
        expected_kind="development-search-result",
    )
    if result.get("artifact_sha256") != inspection.get("result_sha256"):
        raise DenseDataCollectionError("winner development result binding drifted")
    search_path = PROJECT_ROOT / str(result["search_path"])
    if enforce_commit:
        strategy_discovery.require_committed(search_path)
    search = strategy_discovery.load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    if not (
        search.get("state") == "SEARCH_FROZEN"
        and search.get("artifact_sha256") == result.get("search_sha256")
    ):
        raise DenseDataCollectionError("winner development search binding drifted")
    contract = dict(search["family_contract"])
    if not (
        contract["family_id"] == winner["family_id"]
        and contract["development_dates"] == winner["development_dates"]
        and contract["confirmation_dates"] == winner["confirmation_dates"]
        and contract.get("development_scope") == winner.get("development_scope")
        and contract.get("confirmation_scope") == winner.get("confirmation_scope")
        and contract["implementation_hashes"] == winner["implementation_hashes"]
    ):
        raise DenseDataCollectionError("winner family drifted from its frozen search")
    return winner, contract, str(winner["rules_hash"])


def _capacity_calendar_hash(
    contract: Mapping[str, Any],
    *,
    enforce_commit: bool,
) -> str:
    raw = contract.get("capacity_manifest")
    if not isinstance(raw, str) or not raw:
        raise DenseDataCollectionError("family contract lacks a capacity manifest")
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    if enforce_commit:
        strategy_discovery.require_committed(path)
    manifest = load_frozen_dataset_contract(path)
    capacity = manifest["dataset_payload"].get("dense_capacity")
    if not (
        isinstance(capacity, Mapping)
        and capacity.get("family_id") == contract.get("family_id")
        and isinstance(capacity.get("calendar_sha256"), str)
    ):
        raise DenseDataCollectionError("capacity manifest lacks its calendar hash")
    return str(capacity["calendar_sha256"])


def _required_dates(
    evaluation_dates: Sequence[str], calendar: Sequence[str], warmup: int
) -> list[str]:
    positions = {day: index for index, day in enumerate(calendar)}
    if any(day not in positions for day in evaluation_dates):
        raise DenseDataCollectionError("evaluation date is absent from the calendar")
    indices = [positions[day] for day in evaluation_dates]
    if indices != list(range(indices[0], indices[-1] + 1)):
        raise DenseDataCollectionError("evaluation dates must be a contiguous calendar slice")
    if indices[0] < warmup:
        raise DenseDataCollectionError("calendar lacks the frozen warmup sessions")
    return list(calendar[indices[0] - warmup : indices[-1] + 1])


def _task(kind: str, day: str, symbol: str | None = None) -> dict[str, Any]:
    value: dict[str, Any] = {"kind": kind, "date": day}
    if symbol is not None:
        value["symbol"] = symbol
    value["task_id"] = canonical_sha256(value)
    return value


def _existing_successor_authorized(
    contract: Mapping[str, Any], *, enforce_commit: bool
) -> bool:
    if contract.get("research_generation") != (
        continuous_strategy_discovery.RESEARCH_GENERATION
    ):
        return False
    validator = contract.get("existing_successor_validator")
    try:
        if validator is None:
            continuous_strategy_discovery.validate_existing_successor_contract(
                contract, enforce_commit=enforce_commit
            )
        elif (
            isinstance(validator, Mapping)
            and isinstance(validator.get("module"), str)
            and isinstance(validator.get("function"), str)
        ):
            module = importlib.import_module(str(validator["module"]))
            function = getattr(module, str(validator["function"]))
            function(contract, enforce_commit=enforce_commit)
        else:
            raise DenseDataCollectionError(
                "existing successor validator declaration is invalid"
            )
    except (
        AttributeError,
        ImportError,
        continuous_strategy_discovery.ContinuousDiscoveryError,
        outcome_exposure.OutcomeExposureError,
    ) as exc:
        raise DenseDataCollectionError(str(exc)) from exc
    return True


def freeze_plan(
    authority_path: Path,
    *,
    lane: str = "development",
    as_of: date | None = None,
    actual_today: date | None = None,
    calendar_path: Path = DEFAULT_CALENDAR,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    observed_today = actual_today or date.today()
    if as_of is not None and as_of > observed_today:
        raise DenseDataCollectionError("data-planning as_of cannot be future-dated")
    current = as_of or observed_today
    if current < batch.ACTIVATION_NOT_BEFORE:
        raise DenseDataCollectionError(
            f"rolling discovery was not authorized before "
            f"{batch.ACTIVATION_NOT_BEFORE}"
        )
    authority, contract, binding = _authority(
        authority_path, lane=lane, enforce_commit=enforce_commit
    )
    existing_successor = _existing_successor_authorized(
        contract, enforce_commit=enforce_commit
    )
    historical_data_contract = contract.get("historical_data_contract", {})
    fixed_symbol_range = (
        isinstance(historical_data_contract, Mapping)
        and historical_data_contract.get("daily_request_mode")
        == "symbol_range"
    )
    if not existing_successor:
        try:
            batch.require_rolling_activation(
                as_of=current,
                actual_today=observed_today,
            )
        except batch.NextWeekBatchError as exc:
            raise DenseDataCollectionError(str(exc)) from exc
    if lane == "confirmation":
        try:
            outcome_exposure.assert_untouched(
                contract["confirmation_scope"], outcome_exposure.read_index()
            )
        except outcome_exposure.OutcomeExposureError as exc:
            raise DenseDataCollectionError(str(exc)) from exc
    calendar_hash = _file_hash(calendar_path)
    if calendar_hash != _capacity_calendar_hash(
        contract,
        enforce_commit=enforce_commit,
    ):
        raise DenseDataCollectionError("collection calendar drifted from capacity freeze")
    evaluation_dates = list(contract[f"{lane}_dates"])
    family_id = str(contract["family_id"])
    intraday = family_id in runtime.INTRADAY_ETF_FAMILIES
    warmup = INTRADAY_WARMUP_SESSIONS if intraday else DAILY_WARMUP_SESSIONS
    warmup_dates = list(contract[f"{lane}_warmup_dates"])
    if len(warmup_dates) != warmup:
        raise DenseDataCollectionError("frozen family warmup count drifted")
    required_dates = _required_dates(evaluation_dates, _calendar(calendar_path), warmup)
    if required_dates != [*warmup_dates, *evaluation_dates]:
        raise DenseDataCollectionError("frozen family warmup dates drifted")
    if intraday:
        symbols = sorted(map(str, contract["universe"]["symbols"]))
        range_mode = (
            contract.get("historical_data_contract", {}).get(
                "minute_request_mode"
            )
            == "symbol_range"
        )
        if range_mode:
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
            providers = [
                "Alpaca SIP raw-adjustment minute bars by frozen symbol range"
            ]
        else:
            tasks = [
                _task("sip_minute_bars", day, symbol)
                for day in required_dates
                for symbol in symbols
            ]
            providers = ["Alpaca SIP raw-adjustment minute bars"]
    elif existing_successor or fixed_symbol_range:
        symbols = sorted(map(str, contract["universe"].get("symbols", [])))
        if not symbols:
            raise DenseDataCollectionError(
                "existing ETF successor needs a frozen symbol universe"
            )
        if not isinstance(historical_data_contract, Mapping) or (
            historical_data_contract.get("split_provider") != "massive"
        ):
            raise DenseDataCollectionError(
                "fixed ETF symbol-range historical data contract is invalid"
            )
        daily_provider = historical_data_contract.get("daily_provider")
        if daily_provider == "alpaca":
            if not (
                historical_data_contract.get("daily_feed") == "sip"
                and historical_data_contract.get("daily_adjustment") == "raw"
            ):
                raise DenseDataCollectionError(
                    "fixed ETF symbol-range Alpaca data contract is invalid"
                )
            daily_kind = "daily_symbol_bars"
            daily_provider_label = (
                "Alpaca SIP raw-adjustment daily bars by frozen symbol range"
            )
        elif daily_provider == "massive":
            if historical_data_contract.get("daily_adjusted") is not False:
                raise DenseDataCollectionError(
                    "fixed ETF symbol-range Massive data contract is invalid"
                )
            daily_kind = "massive_daily_symbol_bars"
            daily_provider_label = (
                "Massive SIP unadjusted daily bars by frozen symbol range"
            )
        elif daily_provider == "yahoo":
            if not (
                historical_data_contract.get("daily_adjustment")
                == YAHOO_SOURCE_RECOVERY_ADJUSTMENT
                and historical_data_contract.get("no_purchase_required")
                is True
                and historical_data_contract.get("retries_permitted") == 0
            ):
                raise DenseDataCollectionError(
                    "fixed ETF symbol-range Yahoo data contract is invalid"
                )
            daily_kind = "yahoo_daily_symbol_bars"
            daily_provider_label = (
                "Yahoo Finance historical chart raw daily OHLCV "
                "by frozen symbol range"
            )
        else:
            raise DenseDataCollectionError(
                "fixed ETF symbol-range daily provider is unsupported"
            )
        split_task = _task("split_actions", required_dates[-1])
        split_task["start"] = required_dates[0]
        split_task["task_id"] = canonical_sha256(
            {key: value for key, value in split_task.items() if key != "task_id"}
        )
        tasks = [split_task]
        for symbol in symbols:
            task = {
                "kind": daily_kind,
                "date": required_dates[-1],
                "start": required_dates[0],
                "symbol": symbol,
            }
            task["task_id"] = canonical_sha256(task)
            tasks.append(task)
        providers = [
            daily_provider_label,
            "Massive point-in-time split actions through the final frozen session",
        ]
    else:
        symbols = sorted(map(str, contract["universe"].get("symbols", [])))
        split_task = _task("split_actions", required_dates[-1])
        split_task["start"] = required_dates[0]
        split_task["task_id"] = canonical_sha256(
            {key: value for key, value in split_task.items() if key != "task_id"}
        )
        tasks = [split_task, *[_task("grouped_daily_bars", day) for day in required_dates]]
        providers = [
            "Massive SIP unadjusted grouped daily aggregates",
            "Massive point-in-time split actions through the final frozen session",
        ]
        if family_id == runtime.EQUITY_RESIDUAL_FAMILY:
            tasks.extend(_task("common_stock_reference", day) for day in evaluation_dates)
            providers.append("Massive point-in-time active U.S. common-stock reference")
    payload = {
        "schema_version": 1,
        "artifact_kind": PLAN_KIND,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": "COLLECTION_PLAN_FROZEN",
        "family_id": family_id,
        "lane": lane,
        "authority_path": _repo_path(authority_path),
        "authority_sha256": authority["artifact_sha256"],
        "binding_sha256": binding,
        "calendar_path": _repo_path(calendar_path),
        "calendar_sha256": calendar_hash,
        "evaluation_dates": evaluation_dates,
        "required_dates": required_dates,
        "warmup_sessions": warmup,
        "symbols": symbols,
        "tasks": tasks,
        "task_count": len(tasks),
        "providers": providers,
        "research_generation": contract.get("research_generation", "new_family"),
        "daily_provider": (
            contract.get("historical_data_contract", {}).get("daily_provider")
            if existing_successor or fixed_symbol_range
            else None
        ),
        "daily_request_mode": (
            contract.get("historical_data_contract", {}).get(
                "daily_request_mode"
            )
            if fixed_symbol_range
            else None
        ),
        "source_request_semantics": (
            {
                "endpoint_template": YAHOO_CHART_ENDPOINT,
                "interval": "1d",
                "events": "history",
                "include_adjusted_close": True,
                "raw_ohlc_used": True,
                "dividend_adjusted_close_used": False,
                "exchange_timezone": "America/New_York",
                "requests_per_symbol": 1,
                "pace_seconds": YAHOO_PACE_SECONDS,
                "no_purchase_required": True,
                "retries_permitted": 0,
            }
            if (
                contract.get("historical_data_contract", {}).get(
                    "daily_provider"
                )
                == "yahoo"
            )
            else None
        ),
        "intraday_missing_session_policy": (
            contract.get("historical_data_contract", {}).get(
                "minute_missing_session_policy"
            )
            if intraday
            else None
        ),
        "universe_semantics": (
            {
                "security_type": "point-in-time active U.S. common stock",
                "prior_close_minimum": 10.0,
                "prior_20_session_median_dollar_volume_minimum": 50_000_000,
                "ranking": "top 250 by prior 60-session median close-times-volume",
                "historical_identity": "listing-scoped composite FIGI, share-class FIGI fallback, then deterministic sourced fallback",
            }
            if family_id == runtime.EQUITY_RESIDUAL_FAMILY
            else None
        ),
        "provider_requests_before_plan_freeze": 0,
        "substitutions_allowed": False,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "as_of": current.isoformat(),
    }
    if lane == "confirmation":
        preregistered_at = authority.get("recorded_at")
        try:
            preregistered = datetime.fromisoformat(
                str(preregistered_at).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise DenseDataCollectionError(
                "frozen winner preregistration timestamp is invalid"
            ) from exc
        if preregistered.tzinfo is None:
            raise DenseDataCollectionError(
                "frozen winner preregistration timestamp needs a timezone"
            )
        if preregistered.date() > current:
            raise DenseDataCollectionError(
                "frozen winner preregistration timestamp is future-dated"
            )
        payload["preregistered_at"] = str(preregistered_at)
    return strategy_discovery._write_artifact(
        payload,
        public_root / family_id / f"{lane}-collection-plan",
        f"{family_id}-{lane}-collection-plan",
    )


def _validate_plan(path: Path, *, enforce_commit: bool) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    plan = strategy_discovery.load_artifact(path, expected_kind=PLAN_KIND)
    intraday_recovery = (
        plan.get("recovery_kind")
        == INTRADAY_CHECKPOINT_REUSE_RECOVERY
    )
    fixed_etf_checkpoint_recovery = (
        plan.get("recovery_kind")
        == FIXED_ETF_CHECKPOINT_REUSE_RECOVERY
    )
    checkpoint_recovery = (
        intraday_recovery or fixed_etf_checkpoint_recovery
    )
    if not (
        plan.get("state") == "COLLECTION_PLAN_FROZEN"
        and plan.get("campaign_id") == batch.CAMPAIGN_ID
        and plan.get("provider_requests_before_plan_freeze") == 0
        and plan.get("substitutions_allowed") is False
        and (
            plan.get("market_outcomes_accessed") is False
            or (
                checkpoint_recovery
                and plan.get("market_outcomes_accessed") is True
            )
        )
        and plan.get("broker_actions") == 0
        and plan.get("task_count") == len(plan.get("tasks", []))
        and plan.get("task_count", 0) > 0
    ):
        raise DenseDataCollectionError("collection plan authority drifted")
    recovery_failure_path = plan.get("recovery_failure_path")
    recovery_adjustment = plan.get("adjustment_semantics")
    recovery_declared = (
        recovery_adjustment is not None or checkpoint_recovery
    )
    if (recovery_failure_path is not None) != recovery_declared:
        raise DenseDataCollectionError(
            "collection recovery fields must appear together"
        )
    if recovery_failure_path is not None:
        if not isinstance(recovery_failure_path, str) or not recovery_failure_path:
            raise DenseDataCollectionError("collection recovery failure path is invalid")
        failure_path = PROJECT_ROOT / recovery_failure_path
        if enforce_commit:
            strategy_discovery.require_committed(failure_path)
        failure = strategy_discovery.load_artifact(
            failure_path,
            expected_kind="dense-data-collection-failure",
        )
        if not (
            failure.get("state") == "COLLECTION_FAILED_NO_STRATEGY_METRICS"
            and failure.get("artifact_sha256") == plan.get("recovery_failure_sha256")
            and failure.get("plan_sha256") == plan.get("supersedes_plan_sha256")
            and failure.get("family_id") == plan.get("family_id")
            and failure.get("lane") == plan.get("lane")
            and failure.get("strategy_metrics_accessed") is False
            and failure.get("confirmation_outcomes_accessed") is False
        ):
            raise DenseDataCollectionError("collection recovery binding drifted")
        failure_inspection_path = (
            PROJECT_ROOT
            / str(plan.get("recovery_failure_inspection_path", ""))
        )
        if enforce_commit:
            strategy_discovery.require_committed(failure_inspection_path)
        failure_inspection = strategy_discovery.load_artifact(
            failure_inspection_path,
            expected_kind="dense-data-collection-failure-inspection",
        )
        inspection_checks = failure_inspection.get("checks")
        if not (
            failure_inspection.get("state")
            == "COLLECTION_FAILURE_INSPECTED"
            and failure_inspection.get("artifact_sha256")
            == plan.get("recovery_failure_inspection_sha256")
            and failure_inspection.get("failure_sha256")
            == failure["artifact_sha256"]
            and failure_inspection.get("plan_sha256")
            == failure["plan_sha256"]
            and isinstance(inspection_checks, Mapping)
            and inspection_checks
            and all(inspection_checks.values())
        ):
            raise DenseDataCollectionError(
                "collection recovery failure inspection drifted"
            )
        search_refresh = plan.get("recovery_search_refresh")
        if search_refresh is not None:
            if not isinstance(search_refresh, Mapping):
                raise DenseDataCollectionError(
                    "collection recovery search refresh is invalid"
                )
            original_plan_path = PROJECT_ROOT / str(
                failure["plan_path"]
            )
            if enforce_commit:
                strategy_discovery.require_committed(
                    original_plan_path
                )
            original_plan = _validate_plan(
                original_plan_path,
                enforce_commit=False,
            )
            original_search_path = (
                PROJECT_ROOT / str(original_plan["authority_path"])
            )
            refreshed_search_path = (
                PROJECT_ROOT / str(plan["authority_path"])
            )
            if enforce_commit:
                strategy_discovery.require_committed(original_search_path)
                strategy_discovery.require_committed(refreshed_search_path)
            original_search = strategy_discovery.load_artifact(
                original_search_path,
                expected_kind="frozen-development-search",
            )
            refreshed_search = strategy_discovery.load_artifact(
                refreshed_search_path,
                expected_kind="frozen-development-search",
            )
            original_contract = dict(original_search["family_contract"])
            refreshed_contract = dict(refreshed_search["family_contract"])
            original_contract.pop("implementation_hashes", None)
            refreshed_contract.pop("implementation_hashes", None)
            if not (
                original_contract == refreshed_contract
                and search_refresh.get("original_search_sha256")
                == original_search["artifact_sha256"]
                and search_refresh.get("refreshed_search_path")
                == plan["authority_path"]
                and search_refresh.get("refreshed_search_sha256")
                == refreshed_search["artifact_sha256"]
                and search_refresh.get("semantic_contract_sha256")
                == canonical_sha256(refreshed_contract)
                and search_refresh.get("only_implementation_hashes_changed")
                is True
            ):
                raise DenseDataCollectionError(
                    "collection recovery search semantics drifted"
                )
        source_plan: dict[str, Any] | None = None
        if checkpoint_recovery:
            source_plan_path = (
                PROJECT_ROOT
                / str(plan.get("checkpoint_source_plan_path", ""))
            )
            source_plan = _validate_plan(
                source_plan_path,
                enforce_commit=enforce_commit,
            )
        if intraday_recovery:
            assert source_plan is not None
            if not (
                recovery_adjustment is None
                and plan.get("family_id")
                == runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY
                and plan.get("lane") == "development"
                and failure.get("failure_code")
                == "EMPTY_INTRADAY_MISSED_DATE_REPRESENTATION_GAP"
                and failure.get("data_outcomes_accessed") is True
                and failure.get("strategy_metrics_accessed") is False
                and failure.get("completed_tasks")
                == failure.get("task_count")
                and plan.get("market_outcomes_accessed") is True
                and plan.get("strategy_metrics_accessed_before_recovery")
                is False
                and plan.get("checkpoint_source_plan_sha256")
                == source_plan["artifact_sha256"]
                == failure["plan_sha256"]
                and plan.get("provider_requests_already_completed")
                == failure.get("provider_telemetry", {}).get("requests")
                and plan.get("additional_provider_requests_authorized")
                == 0
                and plan.get("tasks") == source_plan.get("tasks")
                and plan.get("evaluation_dates")
                == source_plan.get("evaluation_dates")
                and plan.get("required_dates")
                == source_plan.get("required_dates")
                and plan.get("symbols") == source_plan.get("symbols")
            ):
                raise DenseDataCollectionError(
                    "intraday checkpoint-reuse recovery drifted"
                )
        elif fixed_etf_checkpoint_recovery:
            assert source_plan is not None
            if not (
                recovery_adjustment is None
                and plan.get("family_id")
                == runtime.STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY
                and plan.get("lane") == "development"
                and failure.get("failure_code")
                == "FIXED_ETF_DATASET_FAMILY_REGISTRATION_GAP"
                and failure.get("data_outcomes_accessed") is True
                and failure.get("strategy_metrics_accessed") is False
                and failure.get("completed_tasks")
                == failure.get("task_count")
                and plan.get("market_outcomes_accessed") is True
                and plan.get("strategy_metrics_accessed_before_recovery")
                is False
                and plan.get("checkpoint_source_plan_sha256")
                == source_plan["artifact_sha256"]
                == failure["plan_sha256"]
                and plan.get("provider_requests_already_completed")
                == failure.get("provider_telemetry", {}).get("requests")
                and plan.get("additional_provider_requests_authorized")
                == 0
                and plan.get("tasks") == source_plan.get("tasks")
                and plan.get("evaluation_dates")
                == source_plan.get("evaluation_dates")
                and plan.get("required_dates")
                == source_plan.get("required_dates")
                and plan.get("symbols") == source_plan.get("symbols")
            ):
                raise DenseDataCollectionError(
                    "fixed-ETF checkpoint-reuse recovery drifted"
                )
        else:
            source_recovery_valid = (
                (
                    recovery_adjustment == RECOVERY_ADJUSTMENT
                    and plan.get("family_id")
                    in {
                        runtime.ETF_PULLBACK_FAMILY,
                        runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY,
                    }
                    and failure.get("completed_tasks") == 1
                )
                or (
                recovery_adjustment
                == MASSIVE_SOURCE_RECOVERY_ADJUSTMENT
                    and plan.get("family_id")
                    == runtime.ETF_RESIDUAL_REPLICATION_V4_FAMILY
                    and failure.get("failure_code")
                    == "INCOMPLETE_FIXED_DAILY_SYMBOL_RANGE"
                    and failure.get("completed_tasks")
                    == failure.get("task_count")
                )
                or (
                    recovery_adjustment
                    == YAHOO_SOURCE_RECOVERY_ADJUSTMENT
                    and plan.get("family_id")
                    == runtime.ETF_RESIDUAL_REPLICATION_V4_FAMILY
                    and failure.get("failure_code")
                    == (
                        "MASSIVE_DAILY_SYMBOL_TASK_FAILED_BEFORE_PRICE_ACCESS"
                    )
                    and failure.get("completed_tasks") == 1
                )
            ) and (
                plan.get("lane") == "development"
                and failure.get("data_outcomes_accessed") is False
                and failure.get("market_price_rows_accessed") == 0
            )
            if not source_recovery_valid:
                raise DenseDataCollectionError(
                    "collection recovery is outside its frozen outcome-blind scope"
                )
        implementation_hashes = plan.get("recovery_implementation_hashes")
        expected_implementation_names = {
            "dense_collection_recovery.py",
            "dense_collection_recovery_inspection.py",
            "dense_data_collection.py",
        }
        if intraday_recovery:
            expected_implementation_names.add(
                "dense_strategy_runtime.py"
            )
        if not (
            isinstance(implementation_hashes, Mapping)
            and set(implementation_hashes)
            == expected_implementation_names
            and all(
                isinstance(value, str) and len(value) == 64
                for value in implementation_hashes.values()
            )
        ):
            raise DenseDataCollectionError(
                "collection recovery implementation binding is invalid"
            )
        if enforce_commit:
            expected_hashes: dict[str, str] = {}
            for name in sorted(implementation_hashes):
                implementation_path = PROJECT_ROOT / name
                strategy_discovery.require_committed(implementation_path)
                expected_hashes[name] = strategy_discovery._file_hash(
                    implementation_path
                )
            if dict(implementation_hashes) != expected_hashes:
                raise DenseDataCollectionError(
                    "collection recovery implementation drifted"
                )
    task_ids = [item.get("task_id") for item in plan["tasks"]]
    if len(task_ids) != len(set(task_ids)) or any(
        not isinstance(item, Mapping)
        or item.get("task_id")
        != canonical_sha256({key: value for key, value in item.items() if key != "task_id"})
        for item in plan["tasks"]
    ):
        raise DenseDataCollectionError("collection task IDs are incomplete or invalid")
    if recovery_adjustment in {
        RECOVERY_ADJUSTMENT,
        MASSIVE_SOURCE_RECOVERY_ADJUSTMENT,
        YAHOO_SOURCE_RECOVERY_ADJUSTMENT,
    }:
        symbols = sorted(map(str, plan.get("symbols", [])))
        expected = [("split_actions", "")]
        daily_kind = (
            "daily_symbol_bars"
            if recovery_adjustment == RECOVERY_ADJUSTMENT
            else "massive_daily_symbol_bars"
            if recovery_adjustment
            == MASSIVE_SOURCE_RECOVERY_ADJUSTMENT
            else "yahoo_daily_symbol_bars"
        )
        expected.extend((daily_kind, symbol) for symbol in symbols)
        observed = [
            (str(task.get("kind")), str(task.get("symbol") or ""))
            for task in plan["tasks"]
        ]
        if (
            not symbols
            or observed != expected
            or any(
                task.get("start") != plan["required_dates"][0]
                or task.get("date") != plan["required_dates"][-1]
                for task in plan["tasks"]
            )
        ):
            raise DenseDataCollectionError(
                "daily-provider recovery task topology drifted"
            )
    intraday_policy = plan.get("intraday_missing_session_policy")
    if intraday_policy is not None:
        if not (
            intraday_policy == INTRADAY_FIXED_UNIVERSE_MISS_POLICY
            and plan.get("family_id") in runtime.INTRADAY_ETF_FAMILIES
            and all(
                task.get("kind") == "sip_minute_symbol_range"
                for task in plan["tasks"]
            )
        ):
            raise DenseDataCollectionError(
                "intraday missing-session policy drifted"
            )
    authority_path = PROJECT_ROOT / str(plan.get("authority_path", ""))
    if enforce_commit:
        strategy_discovery.require_committed(authority_path)
    expected_kind = (
        "frozen-development-search"
        if plan.get("lane") == "development"
        else "frozen-strategy-winner"
    )
    authority = strategy_discovery.load_artifact(
        authority_path, expected_kind=expected_kind
    )
    if authority.get("artifact_sha256") != plan.get("authority_sha256"):
        raise DenseDataCollectionError("collection authority drifted after plan freeze")
    if plan.get("lane") == "confirmation":
        if plan.get("preregistered_at") != authority.get("recorded_at"):
            raise DenseDataCollectionError(
                "confirmation preregistration timestamp drifted"
            )
        try:
            outcome_exposure.assert_untouched(
                authority["confirmation_scope"], outcome_exposure.read_index()
            )
        except outcome_exposure.OutcomeExposureError as exc:
            raise DenseDataCollectionError(str(exc)) from exc
    calendar_path = PROJECT_ROOT / str(plan.get("calendar_path", ""))
    if not calendar_path.is_file() or _file_hash(calendar_path) != plan.get(
        "calendar_sha256"
    ):
        raise DenseDataCollectionError("collection calendar drifted after plan freeze")
    return plan


class _CountingSession:
    def __init__(self, telemetry: dict[str, Any]):
        self.telemetry = telemetry
        self.session = requests.Session()

    def get(self, *args: Any, **kwargs: Any) -> requests.Response:
        started = time.monotonic()
        self.telemetry["requests"] += 1
        try:
            return self.session.get(*args, **kwargs)
        finally:
            self.telemetry["request_seconds"] += time.monotonic() - started

    def close(self) -> None:
        self.session.close()


class ProviderBackend:
    """Read-only Massive/Alpaca implementation for one frozen task plan."""

    def __init__(self, env_path: Path = DEFAULT_ENV_PATH):
        self.telemetry = {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 0,
            "failures": 0,
        }
        massive = MassiveConfig.optional_from_env(env_path)
        alpaca = AlpacaConfig.optional_from_env(env_path)
        if massive is None or alpaca is None:
            raise DenseDataCollectionError(
                "configured Massive and Alpaca read-only data lanes are required"
            )
        self._massive_session = _CountingSession(self.telemetry)
        self._alpaca_session = _CountingSession(self.telemetry)
        self._reference_session = _CountingSession(self.telemetry)
        self._yahoo_session = _CountingSession(self.telemetry)

        def paced_sleep(seconds: float) -> None:
            self.telemetry["pacing_wait_seconds"] += seconds
            time.sleep(seconds)

        self.massive = MassiveHistoricalClient(
            massive, session=self._massive_session  # type: ignore[arg-type]
        )
        self.alpaca = AlpacaHistoricalClient(
            alpaca,
            session=self._alpaca_session,  # type: ignore[arg-type]
            sleeper=paced_sleep,
        )
        self._paced_sleep = paced_sleep
        reference_config = scanner_replay.MassiveReferenceConfig.from_env(env_path)
        self.reference = scanner_replay.MassiveReferenceCollector(
            reference_config,
            session=self._reference_session,  # type: ignore[arg-type]
            sleeper=paced_sleep,
        )

    def fetch(self, task: Mapping[str, Any]) -> list[dict[str, Any]]:
        kind = task["kind"]
        day = str(task["date"])
        if kind == "grouped_daily_bars":
            return self.massive.fetch_grouped_daily(day, adjusted=False)
        if kind == "split_actions":
            return self.reference.fetch_splits(str(task["start"]), day)
        if kind == "common_stock_reference":
            return self.reference.fetch(day)
        if kind == "sip_minute_bars":
            session_day = date.fromisoformat(day)
            start = datetime.combine(session_day, wall_time(9, 30), tzinfo=EASTERN)
            end = datetime.combine(session_day, wall_time(16, 0), tzinfo=EASTERN)
            return self.alpaca.fetch_bars(
                str(task["symbol"]), start, end, bar_size="1 min", use_rth=True
            )
        if kind == "sip_minute_symbol_range":
            start_day = date.fromisoformat(str(task["start"]))
            end_day = date.fromisoformat(day)
            return self.alpaca.fetch_bars(
                str(task["symbol"]),
                datetime.combine(start_day, wall_time(9, 30), tzinfo=EASTERN),
                datetime.combine(end_day, wall_time(16, 0), tzinfo=EASTERN),
                bar_size="1 min",
                use_rth=True,
            )
        if kind == "daily_symbol_bars":
            start_day = date.fromisoformat(str(task["start"]))
            end_day = date.fromisoformat(day) + timedelta(days=1)
            rows = self.alpaca.fetch_bars(
                str(task["symbol"]),
                datetime.combine(start_day, wall_time(0), tzinfo=EASTERN),
                datetime.combine(end_day, wall_time(0), tzinfo=EASTERN),
                bar_size="1 day",
                use_rth=True,
            )
            return [
                {
                    "symbol": str(task["symbol"]),
                    "date": str(row["date_et"]),
                    "open": row["open"],
                    "high": row["high"],
                    "low": row["low"],
                    "close": row["close"],
                    "volume": row["volume"],
                    "count": row.get("count", 0),
                    "wap": row.get("wap", 0),
                }
                for row in rows
            ]
        if kind == "massive_daily_symbol_bars":
            return self.massive.fetch_daily_bars(
                str(task["symbol"]),
                str(task["start"]),
                day,
                adjusted=False,
            )
        if kind == "yahoo_daily_symbol_bars":
            self._paced_sleep(YAHOO_PACE_SECONDS)
            symbol = str(task["symbol"])
            start = datetime.fromisoformat(
                f"{task['start']}T00:00:00+00:00"
            )
            end = datetime.fromisoformat(
                f"{day}T00:00:00+00:00"
            ) + timedelta(days=1)
            response = self._yahoo_session.get(
                YAHOO_CHART_ENDPOINT.format(symbol=symbol),
                params={
                    "period1": int(start.timestamp()),
                    "period2": int(end.timestamp()),
                    "interval": "1d",
                    "events": "history",
                    "includeAdjustedClose": "true",
                },
                headers={
                    "User-Agent": (
                        "robinhood-codex-historical-research/1.0"
                    )
                },
                timeout=30,
            )
            if response.status_code >= 400:
                raise DenseDataCollectionError(
                    f"Yahoo HTTP {response.status_code}"
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise DenseDataCollectionError(
                    "Yahoo response is not JSON"
                ) from exc
            chart = (
                payload.get("chart")
                if isinstance(payload, Mapping)
                else None
            )
            results = (
                chart.get("result")
                if isinstance(chart, Mapping)
                else None
            )
            if (
                not isinstance(chart, Mapping)
                or chart.get("error") is not None
                or not isinstance(results, list)
                or len(results) != 1
            ):
                raise DenseDataCollectionError(
                    "Yahoo chart result is missing or ambiguous"
                )
            result = results[0]
            meta = (
                result.get("meta")
                if isinstance(result, Mapping)
                else None
            )
            timestamps = (
                result.get("timestamp")
                if isinstance(result, Mapping)
                else None
            )
            indicators = (
                result.get("indicators")
                if isinstance(result, Mapping)
                else None
            )
            quotes = (
                indicators.get("quote")
                if isinstance(indicators, Mapping)
                else None
            )
            quote = (
                quotes[0]
                if isinstance(quotes, list) and quotes
                else None
            )
            if not (
                isinstance(meta, Mapping)
                and str(meta.get("symbol", "")).upper() == symbol
                and meta.get("exchangeTimezoneName")
                == "America/New_York"
                and isinstance(timestamps, list)
                and isinstance(quote, Mapping)
            ):
                raise DenseDataCollectionError(
                    "Yahoo identity, timezone, or quote arrays drifted"
                )
            arrays = {
                field: quote.get(field)
                for field in ("open", "high", "low", "close", "volume")
            }
            if any(
                not isinstance(values, list)
                or len(values) != len(timestamps)
                for values in arrays.values()
            ):
                raise DenseDataCollectionError(
                    "Yahoo OHLCV arrays are incomplete"
                )
            rows: list[dict[str, Any]] = []
            for index, raw_timestamp in enumerate(timestamps):
                values = [arrays[field][index] for field in arrays]
                if any(value is None for value in values):
                    continue
                observed_day = (
                    datetime.fromtimestamp(
                        int(raw_timestamp), timezone.utc
                    )
                    .astimezone(EASTERN)
                    .date()
                    .isoformat()
                )
                row = {
                    "symbol": symbol,
                    "date": observed_day,
                    "open": float(arrays["open"][index]),
                    "high": float(arrays["high"][index]),
                    "low": float(arrays["low"][index]),
                    "close": float(arrays["close"][index]),
                    "volume": int(arrays["volume"][index]),
                    "count": 0,
                    "wap": 0,
                }
                if (
                    not str(task["start"]) <= observed_day <= day
                    or min(
                        row[field]
                        for field in ("open", "high", "low", "close")
                    )
                    <= 0
                    or row["volume"] < 0
                    or row["low"] > min(row["open"], row["close"])
                    or row["high"] < max(row["open"], row["close"])
                ):
                    raise DenseDataCollectionError(
                        "Yahoo returned an invalid OHLCV row"
                    )
                rows.append(row)
            observed_dates = [str(row["date"]) for row in rows]
            if (
                observed_dates != sorted(observed_dates)
                or len(observed_dates) != len(set(observed_dates))
            ):
                raise DenseDataCollectionError(
                    "Yahoo dates are not unique and chronological"
                )
            return rows
        raise DenseDataCollectionError(f"unsupported collection task: {kind}")

    def close(self) -> None:
        self.reference.close()
        self.massive.close()
        self.alpaca.close()
        self._reference_session.close()
        self._massive_session.close()
        self._alpaca_session.close()
        self._yahoo_session.close()


def _checkpoint_path(root: Path, task: Mapping[str, Any]) -> Path:
    return root / "tasks" / f"{task['task_id']}.json.gz"


def _timestamp(value: datetime, field: str) -> datetime:
    if value.tzinfo is None:
        raise DenseDataCollectionError(f"{field} must include a timezone")
    return value.astimezone(timezone.utc)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _telemetry_state(path: Path, plan_sha256: str) -> dict[str, Any]:
    empty = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }
    if not path.exists():
        return {
            "collection_started_at": None,
            "provider_telemetry": empty,
        }
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DenseDataCollectionError("private collection telemetry is invalid") from exc
    if not isinstance(value, Mapping) or value.get("plan_sha256") != plan_sha256:
        raise DenseDataCollectionError("private telemetry plan binding drifted")
    telemetry = value.get("provider_telemetry")
    if not isinstance(telemetry, Mapping):
        raise DenseDataCollectionError("private provider telemetry is invalid")
    started_at = value.get("collection_started_at")
    if not isinstance(started_at, str):
        raise DenseDataCollectionError(
            "private collection start timestamp is invalid"
        )
    try:
        _timestamp(
            datetime.fromisoformat(started_at.replace("Z", "+00:00")),
            "collection_started_at",
        )
    except ValueError as exc:
        raise DenseDataCollectionError(
            "private collection start timestamp is invalid"
        ) from exc
    return {
        "collection_started_at": started_at,
        "provider_telemetry": {
            key: telemetry.get(key, 0) for key in empty
        },
    }


def _combined_telemetry(
    baseline: Mapping[str, Any], current: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        key: float(baseline.get(key, 0)) + float(current.get(key, 0))
        if key in {"request_seconds", "pacing_wait_seconds"}
        else int(baseline.get(key, 0)) + int(current.get(key, 0))
        for key in (
            "requests",
            "request_seconds",
            "pacing_wait_seconds",
            "cache_hits",
            "failures",
        )
    }


def _write_telemetry_state(
    path: Path,
    *,
    plan_sha256: str,
    collection_started_at: str,
    telemetry: Mapping[str, Any],
    config: HistoricalStoreConfig,
) -> None:
    rendered = (
        json.dumps(
            {
                "schema_version": 1,
                "plan_sha256": plan_sha256,
                "collection_started_at": collection_started_at,
                "provider_telemetry": dict(telemetry),
            },
            indent=2,
            sort_keys=True,
        )
        + "\n"
    ).encode()
    config.root.mkdir(parents=True, exist_ok=True)
    free = shutil.disk_usage(config.root).free
    if free - len(rendered) < config.min_free_bytes:
        raise DenseDataCollectionError("telemetry write would breach disk reserve")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(rendered)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_checkpoint(path: Path, task: Mapping[str, Any]) -> list[dict[str, Any]]:
    value = _read_gzip(path)
    if not isinstance(value, Mapping) or value.get("task") != dict(task):
        raise DenseDataCollectionError("checkpoint task binding drifted")
    rows = value.get("rows")
    if not isinstance(rows, list) or any(not isinstance(item, Mapping) for item in rows):
        raise DenseDataCollectionError("checkpoint rows are invalid")
    if value.get("rows_sha256") != canonical_sha256(rows):
        raise DenseDataCollectionError("checkpoint row hash drifted")
    return [dict(item) for item in rows]


def _daily_rows(checkpoint_root: Path, plan: Mapping[str, Any]) -> dict[str, dict[str, dict[str, Any]]]:
    result: dict[str, dict[str, dict[str, Any]]] = {}
    for task in plan["tasks"]:
        if task["kind"] not in {
            "grouped_daily_bars",
            "daily_symbol_bars",
            "massive_daily_symbol_bars",
            "yahoo_daily_symbol_bars",
        }:
            continue
        rows = _load_checkpoint(_checkpoint_path(checkpoint_root, task), task)
        if task["kind"] == "grouped_daily_bars":
            day = str(task["date"])
            mapped = {str(item.get("symbol")): dict(item) for item in rows}
            if not mapped or len(mapped) != len(rows) or any(
                item.get("date") != day for item in mapped.values()
            ):
                raise DenseDataCollectionError(
                    f"grouped daily checkpoint is invalid: {day}"
                )
            result[day] = mapped
            continue
        symbol = str(task["symbol"])
        observed_dates: set[str] = set()
        for item in rows:
            day = str(item.get("date"))
            if (
                not day
                or day in observed_dates
                or item.get("symbol") != symbol
            ):
                raise DenseDataCollectionError(
                    f"daily symbol checkpoint is invalid: {symbol}"
                )
            observed_dates.add(day)
            result.setdefault(day, {})[symbol] = dict(item)
    return result


def _identity(row: Mapping[str, Any]) -> str:
    exchange = str(row.get("primary_exchange") or "UNKNOWN").upper()
    for field, prefix in (
        ("composite_figi", "FIGI-COMPOSITE"),
        ("share_class_figi", "FIGI-SHARE"),
    ):
        value = str(row.get(field) or "").strip()
        if value:
            return f"{prefix}:{value}:LISTING:{exchange}"
    basis = "|".join(
        str(row.get(field) or "").strip()
        for field in ("ticker", "primary_exchange", "cik", "name")
    )
    return f"MASSIVE-FALLBACK:{hashlib.sha256(basis.encode()).hexdigest()[:24]}"


def _equity_universe(
    checkpoint_root: Path,
    plan: Mapping[str, Any],
    daily: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> tuple[dict[str, list[str]], dict[str, dict[str, str]], set[str]]:
    dates = list(plan["required_dates"])
    positions = {day: index for index, day in enumerate(dates)}
    reference = {
        str(task["date"]): _load_checkpoint(_checkpoint_path(checkpoint_root, task), task)
        for task in plan["tasks"]
        if task["kind"] == "common_stock_reference"
    }
    universes: dict[str, list[str]] = {}
    identities: dict[str, dict[str, str]] = {}
    selected_union: set[str] = {"SPY"}
    for day in plan["evaluation_dates"]:
        index = positions[day]
        prior_60 = dates[index - 60 : index]
        rows = reference.get(day)
        if not rows:
            raise DenseDataCollectionError(f"reference snapshot is missing: {day}")
        candidates: list[tuple[float, str, str]] = []
        for item in rows:
            symbol = str(item.get("ticker") or "").strip().upper()
            if not symbol or str(item.get("type") or "").upper() != "CS":
                continue
            history = [daily[prior][symbol] for prior in prior_60 if symbol in daily[prior]]
            if len(history) != 60:
                continue
            dollar = [float(row["close"]) * float(row["volume"]) for row in history]
            prior_close = float(history[-1]["close"])
            median_20 = sorted(dollar[-20:])[9:11]
            median_20_value = sum(median_20) / 2
            median_60 = sorted(dollar)[29:31]
            median_60_value = sum(median_60) / 2
            if prior_close < 10 or median_20_value < 50_000_000:
                continue
            candidates.append((median_60_value, symbol, _identity(item)))
        selected = sorted(candidates, key=lambda item: (-item[0], item[1]))[:250]
        if len(selected) != 250:
            raise DenseDataCollectionError(
                f"{day} has only {len(selected)} fully qualified common stocks"
            )
        universes[day] = [symbol for _liquidity, symbol, _identity_id in selected]
        identities[day] = {
            symbol: identity for _liquidity, symbol, identity in selected
        }
        selected_union.update(universes[day])
    return universes, identities, selected_union


def _bar(row: Mapping[str, Any], day: str) -> dict[str, Any]:
    return {
        "date": day,
        "open": row["open"],
        "high": row["high"],
        "low": row["low"],
        "close": row["close"],
        "volume": row["volume"],
    }


def _complete_intraday_session(
    rows: Sequence[Mapping[str, Any]], day: str
) -> bool:
    timestamps: list[datetime] = []
    for row in rows:
        raw = row.get("time_et")
        if not isinstance(raw, str):
            return False
        try:
            observed = datetime.fromisoformat(raw)
        except ValueError:
            return False
        if observed.tzinfo is None or observed.date().isoformat() != day:
            return False
        timestamps.append(observed)
    return (
        len(timestamps) == 390
        and timestamps == sorted(timestamps)
        and len(timestamps) == len(set(timestamps))
        and timestamps[0].timetz().replace(tzinfo=None) == wall_time(9, 30)
        and timestamps[-1].timetz().replace(tzinfo=None) == wall_time(15, 59)
        and all(
            right - left == timedelta(minutes=1)
            for left, right in zip(timestamps, timestamps[1:])
        )
    )


def _split_factors(
    checkpoint_root: Path, plan: Mapping[str, Any]
) -> dict[str, list[tuple[str, float]]]:
    tasks = [task for task in plan["tasks"] if task["kind"] == "split_actions"]
    if len(tasks) != 1:
        raise DenseDataCollectionError("daily collection needs one frozen split task")
    rows = _load_checkpoint(_checkpoint_path(checkpoint_root, tasks[0]), tasks[0])
    factors: dict[str, list[tuple[str, float]]] = {}
    for row in rows:
        try:
            symbol = str(row["ticker"]).strip().upper()
            execution = date.fromisoformat(str(row["execution_date"])).isoformat()
            factor = float(row["split_from"]) / float(row["split_to"])
        except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
            raise DenseDataCollectionError("split action is malformed") from exc
        if not symbol or factor <= 0:
            raise DenseDataCollectionError("split action factor is invalid")
        factors.setdefault(symbol, []).append((execution, factor))
    for events in factors.values():
        events.sort()
    return factors


def _adjusted_bar(
    row: Mapping[str, Any], day: str, events: Sequence[tuple[str, float]]
) -> dict[str, Any]:
    factor = 1.0
    for execution, split_factor in events:
        if execution > day:
            factor *= split_factor
    bar = _bar(row, day)
    for field in ("open", "high", "low", "close"):
        bar[field] = float(bar[field]) * factor
    bar["volume"] = float(bar["volume"]) / factor
    return bar


def build_dataset(checkpoint_root: Path, plan: Mapping[str, Any]) -> dict[str, Any]:
    family_id = str(plan["family_id"])
    if family_id in runtime.INTRADAY_ETF_FAMILIES:
        minute: dict[str, dict[str, list[dict[str, Any]]]] = {
            str(day): {} for day in plan["required_dates"]
        }
        expected_symbols = set(map(str, plan["symbols"]))
        required_dates = set(map(str, plan["required_dates"]))
        missing_policy = plan.get("intraday_missing_session_policy")
        missing_sessions: list[dict[str, Any]] = []
        for task in plan["tasks"]:
            rows = _load_checkpoint(_checkpoint_path(checkpoint_root, task), task)
            symbol = str(task["symbol"])
            grouped: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                day = (
                    str(task["date"])
                    if task["kind"] == "sip_minute_bars"
                    else str(row["date_et"])
                )
                if day not in required_dates and missing_policy is not None:
                    continue
                grouped.setdefault(day, []).append(row)
            for day, day_rows in list(grouped.items()):
                if (
                    missing_policy == INTRADAY_FIXED_UNIVERSE_MISS_POLICY
                    and not _complete_intraday_session(day_rows, day)
                ):
                    missing_sessions.append(
                        {
                            "date": day,
                            "symbol": symbol,
                            "observed_minutes": len(day_rows),
                            "expected_minutes": 390,
                        }
                    )
                    grouped.pop(day)
            for day, day_rows in grouped.items():
                converted = [
                    {
                        "timestamp": row["time_et"],
                        "open": row["open"],
                        "high": row["high"],
                        "low": row["low"],
                        "close": row["close"],
                        "volume": row["volume"],
                        "vwap_numerator": (
                            float(row["wap"]) * float(row["volume"])
                        ),
                        "vwap_denominator": row["volume"],
                    }
                    for row in day_rows
                ]
                minute.setdefault(day, {})[symbol] = converted
        if missing_policy is None:
            if (
                set(minute) != required_dates
                or any(
                    set(symbols) != expected_symbols
                    for symbols in minute.values()
                )
            ):
                raise DenseDataCollectionError(
                    "intraday task coverage is incomplete"
                )
            missed_dates: list[str] = []
        else:
            if (
                missing_policy != INTRADAY_FIXED_UNIVERSE_MISS_POLICY
                or set(minute) != required_dates
            ):
                raise DenseDataCollectionError(
                    "intraday retained coverage is incomplete"
                )
            missed_dates = sorted(
                day
                for day, symbols in minute.items()
                if set(symbols) != expected_symbols
            )
            missing_sessions.sort(
                key=lambda item: (item["date"], item["symbol"])
            )
        dataset = {
            "schema_version": 1,
            "family_id": family_id,
            "evaluation_dates": list(plan["evaluation_dates"]),
            "symbols": list(plan["symbols"]),
            "regular_session_minutes_by_date": {
                day: 390 for day in plan["required_dates"]
            },
            "minute_bars": minute,
            "source_semantics": {
                "feed": "Alpaca SIP",
                "adjustment": "raw",
                "vwap": "provider SIP minute VWAP multiplied by provider qualifying volume",
            },
        }
        if missing_policy is not None:
            dataset["missed_data_dates"] = missed_dates
            dataset["missing_session_evidence"] = missing_sessions
            dataset["source_semantics"]["missing_data_policy"] = (
                INTRADAY_FIXED_UNIVERSE_MISS_POLICY
            )
            dataset["source_semantics"]["interpolation"] = "forbidden"
            dataset["source_semantics"]["substitution"] = "forbidden"
        return dataset
    daily = _daily_rows(checkpoint_root, plan)
    if family_id in FIXED_DAILY_ETF_FAMILIES:
        symbols = set(map(str, plan["symbols"]))
        missing = [
            (day, symbol)
            for day in plan["required_dates"]
            for symbol in symbols
            if symbol not in daily.get(day, {})
        ]
        if missing:
            raise DenseDataCollectionError(
                f"fixed ETF daily collection is incomplete; first={missing[0]}"
            )
        universe = symbols
        identities: dict[str, dict[str, str]] | None = None
        universe_by_date: dict[str, list[str]] | None = None
    else:
        universe_by_date, identities, universe = _equity_universe(
            checkpoint_root, plan, daily
        )
    split_factors = _split_factors(checkpoint_root, plan)
    bars: dict[str, list[dict[str, Any]]] = {}
    for symbol in sorted(universe):
        rows = [
            _adjusted_bar(
                daily[day][symbol],
                day,
                split_factors.get(symbol, []),
            )
            for day in plan["required_dates"]
            if symbol in daily[day]
        ]
        if rows:
            bars[symbol] = rows
    successor_daily = plan.get("daily_provider") in {
        "alpaca",
        "massive",
        "yahoo",
    }
    successor_provider = plan.get("daily_provider")
    successor_feed = (
        "Alpaca SIP daily symbol range"
        if successor_daily and successor_provider == "alpaca"
        else "Massive SIP daily symbol range"
        if successor_daily and successor_provider == "massive"
        else "Yahoo Finance historical chart JSON"
        if successor_daily and successor_provider == "yahoo"
        else "Massive SIP grouped daily"
    )
    successor_adjustment = (
        "raw Alpaca bars adjusted only by frozen split actions through the dataset end"
        if successor_daily and successor_provider == "alpaca"
        else "raw Massive bars adjusted only by frozen split actions through the dataset end"
        if successor_daily and successor_provider == "massive"
        else (
            "raw Yahoo quote OHLC bars adjusted only by frozen Massive "
            "split actions through the dataset end; dividend-adjusted "
            "close ignored"
        )
        if successor_daily and successor_provider == "yahoo"
        else "raw grouped bars adjusted only by frozen split actions through the dataset end"
    )
    if plan.get("adjustment_semantics") == RECOVERY_ADJUSTMENT:
        successor_feed = "Alpaca SIP daily symbol range"
        successor_adjustment = (
            "raw Alpaca bars adjusted only by frozen Massive split actions "
            "through the dataset end"
        )
    elif (
        plan.get("adjustment_semantics")
        == MASSIVE_SOURCE_RECOVERY_ADJUSTMENT
    ):
        successor_feed = "Massive SIP daily symbol range"
        successor_adjustment = (
            "raw Massive bars adjusted only by frozen Massive split actions "
            "through the dataset end"
        )
    elif (
        plan.get("adjustment_semantics")
        == YAHOO_SOURCE_RECOVERY_ADJUSTMENT
    ):
        successor_feed = "Yahoo Finance historical chart JSON"
        successor_adjustment = (
            "raw Yahoo quote OHLC bars adjusted only by frozen Massive "
            "split actions through the dataset end; dividend-adjusted "
            "close ignored"
        )
    dataset: dict[str, Any] = {
        "schema_version": 1,
        "family_id": family_id,
        "evaluation_dates": list(plan["evaluation_dates"]),
        "daily_bars": bars,
        "source_semantics": {
            "feed": successor_feed,
            "adjustment": successor_adjustment,
        },
    }
    if family_id in FIXED_DAILY_ETF_FAMILIES:
        dataset["symbols"] = list(plan["symbols"])
    else:
        dataset["universe_by_date"] = universe_by_date
        dataset["universe_identity_by_date"] = identities
        dataset["liquidity_selection"] = {
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000,
            "ranking": "top 250 by prior 60-session median close-times-volume",
        }
    return dataset


def _existing_status(
    public_root: Path, plan: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    directory = public_root / str(plan["family_id"]) / f"{plan['lane']}-collection"
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in directory.glob("*.json"):
        try:
            status = strategy_discovery.load_artifact(path, expected_kind=STATUS_KIND)
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if status.get("plan_sha256") == plan["artifact_sha256"]:
            matches.append((path, status))
    if len(matches) > 1:
        raise DenseDataCollectionError("multiple completed statuses bind one plan")
    return matches[0] if matches else None


def _materialize_checkpoint_recovery_checkpoints(
    *,
    config: HistoricalStoreConfig,
    plan: Mapping[str, Any],
    private_root: Path,
    enforce_commit: bool,
) -> None:
    if plan.get("recovery_kind") not in {
        INTRADAY_CHECKPOINT_REUSE_RECOVERY,
        FIXED_ETF_CHECKPOINT_REUSE_RECOVERY,
    }:
        return
    source_plan_path = (
        PROJECT_ROOT / str(plan["checkpoint_source_plan_path"])
    )
    source_plan = _validate_plan(
        source_plan_path,
        enforce_commit=enforce_commit,
    )
    source_root = (
        config.root
        / "dense-v2"
        / str(source_plan["family_id"])
        / str(source_plan["lane"])
        / str(source_plan["artifact_sha256"])
    )
    source_telemetry = _telemetry_state(
        source_root / "collection-telemetry.json",
        str(source_plan["artifact_sha256"]),
    )
    for task in plan["tasks"]:
        source_path = _checkpoint_path(source_root, task)
        source_rows = _load_checkpoint(source_path, task)
        destination = _checkpoint_path(private_root, task)
        if destination.exists():
            destination_rows = _load_checkpoint(destination, task)
            if canonical_sha256(destination_rows) != canonical_sha256(
                source_rows
            ):
                raise DenseDataCollectionError(
                    "recovery checkpoint differs from its frozen source"
                )
            continue
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.link(source_path, destination)
        except OSError as exc:
            raise DenseDataCollectionError(
                "recovery checkpoint hard-link failed"
            ) from exc
    telemetry_path = private_root / "collection-telemetry.json"
    if not telemetry_path.exists():
        _write_telemetry_state(
            telemetry_path,
            plan_sha256=str(plan["artifact_sha256"]),
            collection_started_at=str(
                source_telemetry["collection_started_at"]
            ),
            telemetry=source_telemetry["provider_telemetry"],
            config=config,
        )


def collect(
    plan_path: Path,
    *,
    as_of: date | None = None,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    backend: DenseCollectionBackend | None = None,
    enforce_commit: bool = True,
    retry_sleeper: Any = time.sleep,
    clock: Callable[[], datetime] = _utc_now,
) -> tuple[Path, dict[str, Any]]:
    invocation_started = _timestamp(clock(), "collection clock")
    observed_today = invocation_started.date()
    if as_of is not None and as_of > observed_today:
        raise DenseDataCollectionError("provider as_of cannot be future-dated")
    current = as_of or observed_today
    if current < batch.ACTIVATION_NOT_BEFORE:
        raise DenseDataCollectionError(
            f"rolling discovery was not authorized before "
            f"{batch.ACTIVATION_NOT_BEFORE}"
        )
    plan = _validate_plan(plan_path, enforce_commit=enforce_commit)
    if plan.get("research_generation") != (
        continuous_strategy_discovery.RESEARCH_GENERATION
    ):
        try:
            batch.require_rolling_activation(
                as_of=current,
                actual_today=observed_today,
            )
        except batch.NextWeekBatchError as exc:
            raise DenseDataCollectionError(str(exc)) from exc
    existing = _existing_status(public_root, plan)
    if existing is not None:
        return existing
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    private_root = (
        config.root
        / "dense-v2"
        / str(plan["family_id"])
        / str(plan["lane"])
        / str(plan["artifact_sha256"])
    )
    _materialize_checkpoint_recovery_checkpoints(
        config=config,
        plan=plan,
        private_root=private_root,
        enforce_commit=enforce_commit,
    )
    client = backend or ProviderBackend()
    owns_backend = backend is None
    telemetry_path = private_root / "collection-telemetry.json"
    private_state = _telemetry_state(
        telemetry_path, str(plan["artifact_sha256"])
    )
    collection_started_at = private_state["collection_started_at"] or (
        invocation_started.isoformat().replace("+00:00", "Z")
    )
    started = _timestamp(
        datetime.fromisoformat(collection_started_at.replace("Z", "+00:00")),
        "collection_started_at",
    )
    if plan["lane"] == "confirmation":
        preregistered = _timestamp(
            datetime.fromisoformat(
                str(plan["preregistered_at"]).replace("Z", "+00:00")
            ),
            "preregistered_at",
        )
        if started <= preregistered:
            raise DenseDataCollectionError(
                "confirmation collection must start after winner preregistration"
            )
    baseline_telemetry = private_state["provider_telemetry"]
    _write_telemetry_state(
        telemetry_path,
        plan_sha256=str(plan["artifact_sha256"]),
        collection_started_at=collection_started_at,
        telemetry=baseline_telemetry,
        config=config,
    )
    completed = 0
    try:
        for task in plan["tasks"]:
            path = _checkpoint_path(private_root, task)
            if path.exists():
                _load_checkpoint(path, task)
                client.telemetry["cache_hits"] += 1
            else:
                if plan.get("recovery_kind") in {
                    INTRADAY_CHECKPOINT_REUSE_RECOVERY,
                    FIXED_ETF_CHECKPOINT_REUSE_RECOVERY,
                }:
                    raise DenseDataCollectionError(
                        "checkpoint recovery forbids an additional provider request"
                    )
                attempts = 0
                while True:
                    attempts += 1
                    try:
                        rows = client.fetch(task)
                        break
                    except Exception as exc:
                        client.telemetry["failures"] += 1
                        retryable = isinstance(exc, HistoricalProviderError) and bool(
                            exc.retryable
                        )
                        if retryable and attempts < MAX_TASK_ATTEMPTS:
                            exponential = min(
                                INITIAL_RETRY_DELAY_SECONDS * (2 ** (attempts - 1)),
                                MAX_RETRY_DELAY_SECONDS,
                            )
                            delay = max(
                                exponential,
                                float(exc.retry_after_seconds or 0.0),
                            )
                            client.telemetry["pacing_wait_seconds"] += delay
                        _write_telemetry_state(
                            telemetry_path,
                            plan_sha256=str(plan["artifact_sha256"]),
                            collection_started_at=collection_started_at,
                            telemetry=_combined_telemetry(
                                baseline_telemetry, client.telemetry
                            ),
                            config=config,
                        )
                        if not retryable or attempts >= MAX_TASK_ATTEMPTS:
                            raise
                        retry_sleeper(delay)
                if not isinstance(rows, list):
                    raise DenseDataCollectionError("provider task did not return rows")
                _write_external(
                    path,
                    {
                        "schema_version": 1,
                        "task": dict(task),
                        "rows": rows,
                        "rows_sha256": canonical_sha256(rows),
                    },
                    config,
                )
            completed += 1
            _write_telemetry_state(
                telemetry_path,
                plan_sha256=str(plan["artifact_sha256"]),
                collection_started_at=collection_started_at,
                telemetry=_combined_telemetry(baseline_telemetry, client.telemetry),
                config=config,
            )
        dataset = build_dataset(private_root, plan)
        runtime.prepare_dataset(dataset)
        dataset_path = private_root / "dataset.json.gz"
        _write_external(dataset_path, dataset, config)
    except (HistoricalProviderError, HistoricalStoreError, OSError, ValueError) as exc:
        raise DenseDataCollectionError(str(exc)) from exc
    finally:
        if owns_backend:
            client.close()
    relative = str(dataset_path.resolve().relative_to(config.root.resolve()))
    collection_completed = _timestamp(clock(), "collection clock")
    if collection_completed < started:
        raise DenseDataCollectionError(
            "collection completion cannot precede its first provider attempt"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": STATUS_KIND,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": "COLLECTED_UNINSPECTED",
        "family_id": plan["family_id"],
        "lane": plan["lane"],
        "plan_path": _repo_path(plan_path),
        "plan_sha256": plan["artifact_sha256"],
        "binding_sha256": plan["binding_sha256"],
        "evaluation_dates": plan["evaluation_dates"],
        "task_count": plan["task_count"],
        "completed_tasks": completed,
        "external_relative_path": relative,
        "external_file_sha256": sha256_file(dataset_path),
        "dataset_sha256": canonical_sha256(dataset),
        "provider_telemetry": _combined_telemetry(
            baseline_telemetry, client.telemetry
        ),
        "collection_started_at": collection_started_at,
        "collection_completed_at": collection_completed.isoformat().replace(
            "+00:00", "Z"
        ),
        "substitutions": 0,
        "broker_actions": 0,
        "as_of": current.isoformat(),
    }
    return strategy_discovery._write_artifact(
        payload,
        public_root / str(plan["family_id"]) / f"{plan['lane']}-collection",
        f"{plan['family_id']}-{plan['lane']}-collection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--as-of", type=date.fromisoformat)
    parser.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT)
    parser.add_argument("--calendar", type=Path, default=DEFAULT_CALENDAR)
    subparsers = parser.add_subparsers(dest="command", required=True)
    for command in ("freeze-development", "freeze-confirmation", "collect"):
        child = subparsers.add_parser(command)
        child.add_argument("artifact", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command.startswith("freeze-"):
            lane = args.command.removeprefix("freeze-")
            path, artifact = freeze_plan(
                args.artifact,
                lane=lane,
                as_of=args.as_of,
                calendar_path=args.calendar,
                public_root=args.public_root,
            )
        else:
            path, artifact = collect(
                args.artifact,
                as_of=args.as_of,
                public_root=args.public_root,
            )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "provider_telemetry": artifact.get("provider_telemetry", {}),
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        DenseDataCollectionError,
        HistoricalProviderError,
        HistoricalStoreError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
