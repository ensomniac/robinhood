"""Freeze and execute exact event-window S&P addition price collection."""

from __future__ import annotations

import argparse
import gzip
import json
import os
from collections.abc import Mapping
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Protocol

import dense_data_collection as dense_collection
import dense_strategy_runtime as runtime
import outcome_exposure
import sp500_addition_discovery as discovery
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    HistoricalStoreError,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = strategy_discovery.DEFAULT_ROOT
PLAN_KIND = "sp500-addition-data-plan"
PLAN_INSPECTION_KIND = "sp500-addition-data-plan-inspection"
STATUS_KIND = "sp500-addition-data-collection"
PRIVATE_NAMESPACE = "sp500-addition-v2-yahoo"
PERMANENT_MISSING_ERRORS = frozenset(
    {
        "Yahoo HTTP 400",
        "Yahoo HTTP 404",
        "Yahoo chart result is missing or ambiguous",
    }
)
PERMANENT_MISSING_DISPOSITIONS = frozenset(
    {
        "provider_empty_missing",
        "permanent_symbol_unavailable",
    }
)


class Sp500AdditionCollectionError(RuntimeError):
    """A frozen event-window plan or collected response is invalid."""


class CollectionBackend(Protocol):
    telemetry: dict[str, Any]

    def fetch(self, task: Mapping[str, Any]) -> list[dict[str, Any]]: ...

    def close(self) -> None: ...


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Sp500AdditionCollectionError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise Sp500AdditionCollectionError(
            f"{field} needs a timezone"
        )
    return parsed


def _task(value: Mapping[str, Any]) -> dict[str, Any]:
    task = dict(value)
    task["task_id"] = canonical_sha256(task)
    return task


def _load_scope(
    raw_path: str, expected_sha256: str, *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    path = PROJECT_ROOT / raw_path
    if enforce_commit:
        strategy_discovery.require_committed(path)
    scope = strategy_discovery.load_artifact(
        path, expected_kind="sp500-addition-event-scope"
    )
    if (
        scope.get("artifact_sha256") != expected_sha256
        or scope.get("family_id") != discovery.FAMILY_ID
        or scope.get("market_outcomes_accessed") is not False
        or scope.get("provider_requests") != 0
    ):
        raise Sp500AdditionCollectionError(
            "frozen event scope drifted"
        )
    return path, scope


def _authority(
    authority_path: Path,
    *,
    lane: str,
    enforce_commit: bool,
) -> tuple[dict[str, Any], dict[str, Any], str, str | None]:
    if lane not in {"development", "confirmation"}:
        raise Sp500AdditionCollectionError(
            "collection lane is invalid"
        )
    if enforce_commit:
        strategy_discovery.require_committed(authority_path)
    if lane == "development":
        authority = strategy_discovery.load_artifact(
            authority_path,
            expected_kind="frozen-development-search",
        )
        if authority.get("state") != "SEARCH_FROZEN":
            raise Sp500AdditionCollectionError(
                "development search is not frozen"
            )
        contract = dict(authority["family_contract"])
        binding = str(authority["artifact_sha256"])
        preregistered_at = None
    else:
        authority = strategy_discovery.load_artifact(
            authority_path,
            expected_kind="frozen-strategy-winner",
        )
        if authority.get("state") != "WINNER_FROZEN":
            raise Sp500AdditionCollectionError(
                "confirmation winner is not frozen"
            )
        contract = {
            **dict(authority),
            "universe": authority["exact_rules"]["universe"],
            "event_scope_path": authority["exact_rules"]["universe"][
                "event_scope_path"
            ],
            "event_scope_sha256": authority["exact_rules"]["universe"][
                "event_scope_sha256"
            ],
        }
        binding = str(authority["rules_hash"])
        preregistered_at = str(authority["recorded_at"])
        outcome_exposure.assert_untouched(
            authority["confirmation_scope"],
            outcome_exposure.read_index(),
        )
    if contract.get("family_id") != discovery.FAMILY_ID:
        raise Sp500AdditionCollectionError(
            "collection authority family drifted"
        )
    return authority, contract, binding, preregistered_at


def _plan_components(
    contract: Mapping[str, Any],
    *,
    lane: str,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, str]]:
    scope_path, scope = _load_scope(
        str(contract["event_scope_path"]),
        str(contract["event_scope_sha256"]),
        enforce_commit=enforce_commit,
    )
    events = [
        dict(row) for row in scope[f"{lane}_events"]
    ]
    evaluation_dates = list(contract[f"{lane}_dates"])
    if evaluation_dates != scope[f"{lane}_dates"]:
        raise Sp500AdditionCollectionError(
            "authority dates drifted from the event scope"
        )
    required_dates = sorted(
        {
            day
            for event in events
            for day in [
                str(event["reference_date"]),
                *map(str, event["holding_dates"]),
            ]
        }
    )
    split_task = _task(
        {
            "kind": "split_actions",
            "start": required_dates[0],
            "date": required_dates[-1],
        }
    )
    unique_windows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for event in events:
        key = (
            str(event["ticker"]),
            str(event["reference_date"]),
            str(event["holding_dates"][-1]),
        )
        unique_windows.setdefault(
            key,
            _task(
                {
                    "kind": "yahoo_daily_symbol_bars",
                    "symbol": key[0],
                    "start": key[1],
                    "date": key[2],
                }
            ),
        )
    tasks = [
        split_task,
        *sorted(
            unique_windows.values(),
            key=lambda row: (
                str(row["start"]),
                str(row["date"]),
                str(row["symbol"]),
            ),
        ),
    ]
    task_by_window = {
        (
            str(task["symbol"]),
            str(task["start"]),
            str(task["date"]),
        ): str(task["task_id"])
        for task in tasks
        if task["kind"] == "yahoo_daily_symbol_bars"
    }
    event_task_ids = {
        str(event["event_id"]): task_by_window[
            (
                str(event["ticker"]),
                str(event["reference_date"]),
                str(event["holding_dates"][-1]),
            )
        ]
        for event in events
    }
    return scope_path, scope, events, tasks, event_task_ids


def freeze_plan(
    authority_path: Path,
    *,
    lane: str,
    created_at: str,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at, "created_at")
    authority, contract, binding, preregistered_at = _authority(
        authority_path,
        lane=lane,
        enforce_commit=enforce_commit,
    )
    scope_path, scope, events, tasks, event_task_ids = _plan_components(
        contract,
        lane=lane,
        enforce_commit=enforce_commit,
    )
    payload: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": PLAN_KIND,
        "campaign_id": discovery.CAMPAIGN_ID,
        "state": "COLLECTION_PLAN_FROZEN",
        "family_id": discovery.FAMILY_ID,
        "lane": lane,
        "created_at": created_at,
        "authority_path": _repo_path(authority_path),
        "authority_sha256": authority["artifact_sha256"],
        "binding_sha256": binding,
        "event_scope_path": _repo_path(scope_path),
        "event_scope_sha256": scope["artifact_sha256"],
        "evaluation_dates": list(contract[f"{lane}_dates"]),
        "event_count": len(events),
        "signal_date_capacity": len(
            scope[f"{lane}_signal_dates"]
        ),
        "events_sha256": canonical_sha256(events),
        "tasks": tasks,
        "task_count": len(tasks),
        "event_task_ids": event_task_ids,
        "providers": [
            "Yahoo Finance unadjusted daily bars by exact event window",
            "Massive point-in-time split actions through the final frozen event window",
        ],
        "permanent_missing_symbol_response": "missed_trade",
        "source_scope": scope[f"{lane}_scope"],
        "provider_requests_before_plan_freeze": 0,
        "market_outcomes_accessed": False,
        "substitutions_allowed": False,
        "broker_actions": 0,
    }
    if lane == "confirmation":
        payload["preregistered_at"] = preregistered_at
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / discovery.FAMILY_ID
        / f"{lane}-collection-plan",
        f"{discovery.FAMILY_ID}-{lane}-collection-plan",
    )


def _load_plan(
    plan_path: Path, *, enforce_commit: bool
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(plan_path)
    plan = strategy_discovery.load_artifact(
        plan_path, expected_kind=PLAN_KIND
    )
    if not (
        plan.get("state") == "COLLECTION_PLAN_FROZEN"
        and plan.get("family_id") == discovery.FAMILY_ID
        and plan.get("lane") in {"development", "confirmation"}
        and plan.get("task_count") == len(plan.get("tasks", []))
        and plan.get("task_count", 0) > 1
        and plan.get("provider_requests_before_plan_freeze") == 0
        and plan.get("market_outcomes_accessed") is False
        and plan.get("substitutions_allowed") is False
        and plan.get("permanent_missing_symbol_response")
        == "missed_trade"
        and plan.get("broker_actions") == 0
    ):
        raise Sp500AdditionCollectionError(
            "collection plan authority drifted"
        )
    ids = [task.get("task_id") for task in plan["tasks"]]
    if len(ids) != len(set(ids)) or any(
        not isinstance(task, Mapping)
        or task.get("task_id")
        != canonical_sha256(
            {
                key: value
                for key, value in task.items()
                if key != "task_id"
            }
        )
        for task in plan["tasks"]
    ):
        raise Sp500AdditionCollectionError(
            "collection task hashes drifted"
        )
    if plan["lane"] == "confirmation":
        authority = strategy_discovery.load_artifact(
            PROJECT_ROOT / str(plan["authority_path"]),
            expected_kind="frozen-strategy-winner",
        )
        if (
            authority["rules_hash"] != plan["binding_sha256"]
            or authority["recorded_at"] != plan["preregistered_at"]
        ):
            raise Sp500AdditionCollectionError(
                "confirmation winner binding drifted"
            )
        outcome_exposure.assert_untouched(
            authority["confirmation_scope"],
            outcome_exposure.read_index(),
        )
    return plan


def _plan_inspection(
    plan: Mapping[str, Any],
    inspection_path: Path,
    *,
    enforce_commit: bool,
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(inspection_path)
    inspection = strategy_discovery.load_artifact(
        inspection_path,
        expected_kind=PLAN_INSPECTION_KIND,
    )
    checks = inspection.get("checks")
    if not (
        inspection.get("state") == "COLLECTION_PLAN_INSPECTED"
        and inspection.get("plan_sha256") == plan["artifact_sha256"]
        and isinstance(checks, Mapping)
        and checks
        and all(checks.values())
        and inspection.get("provider_requests") == 0
        and inspection.get("market_outcomes_accessed") is False
    ):
        raise Sp500AdditionCollectionError(
            "collection plan lacks a valid independent inspection"
        )
    return inspection


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise Sp500AdditionCollectionError(
            f"private checkpoint is unreadable: {path}"
        ) from exc


def _write_gzip(
    path: Path, value: Any, config: HistoricalStoreConfig
) -> None:
    rendered = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw,
                mtime=0,
            ) as compressed:
                compressed.write(rendered)
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def _checkpoint_path(
    private_root: Path, task: Mapping[str, Any]
) -> Path:
    return private_root / "tasks" / f"{task['task_id']}.json.gz"


def _checkpoint(
    path: Path, task: Mapping[str, Any]
) -> list[dict[str, Any]]:
    value = _read_gzip(path)
    if not (
        isinstance(value, Mapping)
        and value.get("task") == dict(task)
        and isinstance(value.get("rows"), list)
        and value.get("collection_disposition")
        in {
            "provider_rows",
            *PERMANENT_MISSING_DISPOSITIONS,
        }
        and value.get("rows_sha256")
        == canonical_sha256(value["rows"])
        and all(isinstance(row, Mapping) for row in value["rows"])
    ):
        raise Sp500AdditionCollectionError(
            "private checkpoint binding drifted"
        )
    return [dict(row) for row in value["rows"]]


def _provider_rows(
    client: CollectionBackend,
    task: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    try:
        rows = client.fetch(task)
    except dense_collection.DenseDataCollectionError as exc:
        if (
            task.get("kind") != "yahoo_daily_symbol_bars"
            or str(exc) not in PERMANENT_MISSING_ERRORS
        ):
            raise
        client.telemetry["failures"] = (
            int(client.telemetry.get("failures", 0)) + 1
        )
        return [], "permanent_symbol_unavailable"
    if not isinstance(rows, list):
        raise Sp500AdditionCollectionError(
            "provider task did not return rows"
        )
    if task.get("kind") == "yahoo_daily_symbol_bars" and not rows:
        return [], "provider_empty_missing"
    return [dict(row) for row in rows], "provider_rows"


def _split_factors(
    private_root: Path, plan: Mapping[str, Any]
) -> tuple[dict[str, list[tuple[str, float]]], dict[str, list[str]]]:
    split_tasks = [
        task
        for task in plan["tasks"]
        if task["kind"] == "split_actions"
    ]
    if len(split_tasks) != 1:
        raise Sp500AdditionCollectionError(
            "collection needs one split task"
        )
    rows = _checkpoint(
        _checkpoint_path(private_root, split_tasks[0]),
        split_tasks[0],
    )
    factors: dict[str, list[tuple[str, float]]] = {}
    dates: dict[str, list[str]] = {}
    for row in rows:
        try:
            symbol = str(row["ticker"]).strip().upper()
            execution = date.fromisoformat(
                str(row["execution_date"])
            ).isoformat()
            factor = float(row["split_from"]) / float(row["split_to"])
        except (KeyError, TypeError, ValueError, ZeroDivisionError) as exc:
            raise Sp500AdditionCollectionError(
                "split action is malformed"
            ) from exc
        if not symbol or factor <= 0:
            raise Sp500AdditionCollectionError(
                "split action factor is invalid"
            )
        factors.setdefault(symbol, []).append((execution, factor))
        dates.setdefault(symbol, []).append(execution)
    for symbol in factors:
        factors[symbol].sort()
        dates[symbol] = sorted(set(dates[symbol]))
    return factors, dates


def build_dataset(
    private_root: Path, plan: Mapping[str, Any]
) -> dict[str, Any]:
    scope = strategy_discovery.load_artifact(
        PROJECT_ROOT / str(plan["event_scope_path"]),
        expected_kind="sp500-addition-event-scope",
    )
    events = [
        dict(row) for row in scope[f"{plan['lane']}_events"]
    ]
    if canonical_sha256(events) != plan["events_sha256"]:
        raise Sp500AdditionCollectionError(
            "event rows drifted after plan freeze"
        )
    task_by_id = {
        str(task["task_id"]): task for task in plan["tasks"]
    }
    factors, split_dates = _split_factors(private_root, plan)
    bars: dict[str, dict[str, dict[str, Any]]] = {}
    for event in events:
        task = task_by_id[plan["event_task_ids"][event["event_id"]]]
        rows = _checkpoint(
            _checkpoint_path(private_root, task), task
        )
        symbol = str(event["ticker"])
        for row in rows:
            day = str(row.get("date", ""))
            if (
                row.get("symbol") != symbol
                or not str(task["start"]) <= day <= str(task["date"])
            ):
                raise Sp500AdditionCollectionError(
                    "daily provider row escaped its exact event window"
                )
            adjusted = dense_collection._adjusted_bar(
                row, day, factors.get(symbol, [])
            )
            existing = bars.setdefault(symbol, {}).get(day)
            if existing is not None and existing != adjusted:
                raise Sp500AdditionCollectionError(
                    "overlapping event windows disagree"
                )
            bars[symbol][day] = adjusted
    metadata = {
        day: [] for day in plan["evaluation_dates"]
    }
    for event in events:
        entry_date = str(event["entry_date"])
        if entry_date not in metadata:
            raise Sp500AdditionCollectionError(
                "event entry escaped the account calendar"
            )
        metadata[entry_date].append(dict(event))
    for day in metadata:
        metadata[day] = sorted(
            metadata[day],
            key=lambda row: (
                -int(row["sessions_to_effective"]),
                str(row["ticker"]),
                str(row["event_id"]),
            ),
        )
    dataset = {
        "schema_version": 1,
        "family_id": discovery.FAMILY_ID,
        "evaluation_dates": list(plan["evaluation_dates"]),
        "event_metadata_by_entry_date": metadata,
        "daily_bars": {
            symbol: [
                rows[day] for day in sorted(rows)
            ]
            for symbol, rows in sorted(bars.items())
            if rows
        },
        "split_execution_dates_by_symbol": {
            symbol: split_dates.get(symbol, [])
            for symbol in sorted(bars)
        },
        "source_semantics": {
            "feed": "Yahoo Finance unadjusted daily event windows",
            "adjustment": (
                "raw bars adjusted only by frozen point-in-time split "
                "actions through the dataset end"
            ),
            "permanent_missing_symbol_response": "missed_trade",
            "substitution": "forbidden",
        },
    }
    runtime.prepare_dataset(dataset)
    return dataset


def _telemetry_state(
    path: Path, plan_sha256: str
) -> dict[str, Any]:
    empty = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }
    if not path.exists():
        return {
            "started_at": None,
            "provider_telemetry": empty,
        }
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Sp500AdditionCollectionError(
            "private telemetry is invalid"
        ) from exc
    if (
        not isinstance(value, Mapping)
        or value.get("plan_sha256") != plan_sha256
        or not isinstance(value.get("provider_telemetry"), Mapping)
    ):
        raise Sp500AdditionCollectionError(
            "private telemetry binding drifted"
        )
    return {
        "started_at": value.get("started_at"),
        "provider_telemetry": {
            key: value["provider_telemetry"].get(key, 0)
            for key in empty
        },
    }


def _write_telemetry(
    path: Path,
    *,
    plan_sha256: str,
    started_at: str,
    telemetry: Mapping[str, Any],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(
        {
            "schema_version": 1,
            "plan_sha256": plan_sha256,
            "started_at": started_at,
            "provider_telemetry": dict(telemetry),
        },
        indent=2,
        sort_keys=True,
    ) + "\n"
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _combine_telemetry(
    left: Mapping[str, Any], right: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        key: (
            float(left.get(key, 0)) + float(right.get(key, 0))
            if key in {"request_seconds", "pacing_wait_seconds"}
            else int(left.get(key, 0)) + int(right.get(key, 0))
        )
        for key in (
            "requests",
            "request_seconds",
            "pacing_wait_seconds",
            "cache_hits",
            "failures",
        )
    }


def collect(
    plan_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    backend: CollectionBackend | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    invoked = _timestamp(collected_at, "collected_at").astimezone(UTC)
    plan = _load_plan(plan_path, enforce_commit=enforce_commit)
    _plan_inspection(
        plan, inspection_path, enforce_commit=enforce_commit
    )
    if plan["lane"] == "confirmation" and invoked <= _timestamp(
        str(plan["preregistered_at"]), "preregistered_at"
    ).astimezone(UTC):
        raise Sp500AdditionCollectionError(
            "confirmation collection must follow winner freeze"
        )
    config = store_config or HistoricalStoreConfig.from_env(
        DEFAULT_ENV_PATH
    )
    private_root = (
        config.root
        / PRIVATE_NAMESPACE
        / str(plan["lane"])
        / str(plan["artifact_sha256"])
    )
    telemetry_path = private_root / "telemetry.json"
    saved = _telemetry_state(
        telemetry_path, str(plan["artifact_sha256"])
    )
    started_at = saved["started_at"] or invoked.isoformat().replace(
        "+00:00", "Z"
    )
    client = backend or dense_collection.ProviderBackend()
    owns_backend = backend is None
    completed = 0
    try:
        for task in plan["tasks"]:
            checkpoint_path = _checkpoint_path(private_root, task)
            if checkpoint_path.exists():
                _checkpoint(checkpoint_path, task)
                client.telemetry["cache_hits"] += 1
            else:
                rows, disposition = _provider_rows(client, task)
                _write_gzip(
                    checkpoint_path,
                    {
                        "schema_version": 1,
                        "task": dict(task),
                        "rows": rows,
                        "rows_sha256": canonical_sha256(rows),
                        "collection_disposition": disposition,
                    },
                    config,
                )
            completed += 1
            _write_telemetry(
                telemetry_path,
                plan_sha256=str(plan["artifact_sha256"]),
                started_at=started_at,
                telemetry=_combine_telemetry(
                    saved["provider_telemetry"],
                    client.telemetry,
                ),
            )
        dataset = build_dataset(private_root, plan)
        dataset_path = private_root / "dataset.json.gz"
        _write_gzip(dataset_path, dataset, config)
    finally:
        if owns_backend:
            client.close()
    completed_at = datetime.now(UTC).isoformat().replace(
        "+00:00", "Z"
    )
    relative = str(
        dataset_path.resolve().relative_to(config.root.resolve())
    )
    permanent_missing_task_ids = sorted(
        str(task["task_id"])
        for task in plan["tasks"]
        if _read_gzip(
            _checkpoint_path(private_root, task)
        ).get("collection_disposition")
        in PERMANENT_MISSING_DISPOSITIONS
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": STATUS_KIND,
        "campaign_id": discovery.CAMPAIGN_ID,
        "state": "COLLECTED_UNINSPECTED",
        "family_id": discovery.FAMILY_ID,
        "lane": plan["lane"],
        "plan_path": _repo_path(plan_path),
        "plan_sha256": plan["artifact_sha256"],
        "plan_inspection_path": _repo_path(inspection_path),
        "plan_inspection_sha256": sha256_file(inspection_path),
        "binding_sha256": plan["binding_sha256"],
        "evaluation_dates": plan["evaluation_dates"],
        "event_count": plan["event_count"],
        "task_count": plan["task_count"],
        "completed_tasks": completed,
        "external_relative_path": relative,
        "external_file_sha256": sha256_file(dataset_path),
        "dataset_sha256": canonical_sha256(dataset),
        "provider_telemetry": _combine_telemetry(
            saved["provider_telemetry"], client.telemetry
        ),
        "permanent_missing_task_count": len(
            permanent_missing_task_ids
        ),
        "permanent_missing_task_ids": permanent_missing_task_ids,
        "permanent_missing_semantics": "missed_trade",
        "collection_started_at": started_at,
        "collection_completed_at": completed_at,
        "substitutions": 0,
        "market_outcomes_accessed": True,
        "confirmation_accessed": plan["lane"] == "confirmation",
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / discovery.FAMILY_ID
        / f"{plan['lane']}-collection",
        f"{discovery.FAMILY_ID}-{plan['lane']}-collection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT
    )
    sub = parser.add_subparsers(dest="command", required=True)
    for command in ("freeze-development", "freeze-confirmation"):
        child = sub.add_parser(command)
        child.add_argument("authority", type=Path)
        child.add_argument("--created-at", required=True)
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("plan", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command.startswith("freeze-"):
            lane = args.command.removeprefix("freeze-")
            path, artifact = freeze_plan(
                args.authority,
                lane=lane,
                created_at=args.created_at,
                public_root=args.public_root,
            )
        else:
            path, artifact = collect(
                args.plan,
                args.inspection,
                collected_at=args.collected_at,
                public_root=args.public_root,
            )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "provider_telemetry": artifact.get(
                        "provider_telemetry", {}
                    ),
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        Sp500AdditionCollectionError,
        dense_collection.DenseDataCollectionError,
        HistoricalStoreError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
