"""Freeze and execute exact S&P deletion observation-window collection."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

import dense_data_collection as dense_collection
import dense_strategy_runtime as runtime
import outcome_exposure
import sp500_addition_collection as shared
import sp500_deletion_discovery as discovery
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
PLAN_KIND = "sp500-deletion-data-plan"
PLAN_INSPECTION_KIND = "sp500-deletion-data-plan-inspection"
STATUS_KIND = "sp500-deletion-data-collection"
PRIVATE_NAMESPACE = "sp500-deletion-forced-selling-rebound-v1"
PERMANENT_MISSING_DISPOSITIONS = (
    shared.PERMANENT_MISSING_DISPOSITIONS
)


class Sp500DeletionCollectionError(RuntimeError):
    """A frozen deletion plan or collected response is invalid."""


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
        raise Sp500DeletionCollectionError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise Sp500DeletionCollectionError(
            f"{field} needs a timezone"
        )
    return parsed


def _task(value: Mapping[str, Any]) -> dict[str, Any]:
    task = dict(value)
    task["task_id"] = canonical_sha256(task)
    return task


def _authority(
    authority_path: Path,
    *,
    lane: str,
    enforce_commit: bool,
) -> tuple[dict[str, Any], dict[str, Any], str, str | None]:
    if lane not in {"development", "confirmation"}:
        raise Sp500DeletionCollectionError(
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
            raise Sp500DeletionCollectionError(
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
            raise Sp500DeletionCollectionError(
                "confirmation winner is not frozen"
            )
        contract = {
            **dict(authority),
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
        raise Sp500DeletionCollectionError(
            "collection authority family drifted"
        )
    return authority, contract, binding, preregistered_at


def _scope(
    contract: Mapping[str, Any], *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    path = PROJECT_ROOT / str(contract["event_scope_path"])
    if enforce_commit:
        strategy_discovery.require_committed(path)
    scope = strategy_discovery.load_artifact(
        path, expected_kind="sp500-deletion-event-scope"
    )
    if not (
        scope.get("artifact_sha256")
        == contract["event_scope_sha256"]
        and scope.get("family_id") == discovery.FAMILY_ID
        and scope.get("market_outcomes_accessed") is False
        and scope.get("provider_requests") == 0
    ):
        raise Sp500DeletionCollectionError(
            "frozen deletion event scope drifted"
        )
    return path, scope


def _plan_components(
    contract: Mapping[str, Any],
    *,
    lane: str,
    enforce_commit: bool,
) -> tuple[
    Path,
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, str],
]:
    scope_path, scope = _scope(
        contract, enforce_commit=enforce_commit
    )
    events = [dict(row) for row in scope[f"{lane}_events"]]
    evaluation_dates = list(contract[f"{lane}_dates"])
    if evaluation_dates != scope[f"{lane}_dates"]:
        raise Sp500DeletionCollectionError(
            "authority dates drifted from the deletion scope"
        )
    required_dates = sorted(
        {
            day
            for event in events
            for day in map(str, event["observation_dates"])
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
            str(event["observation_dates"][0]),
            str(event["observation_dates"][-1]),
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
                str(event["observation_dates"][0]),
                str(event["observation_dates"][-1]),
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
    scope_path, scope, events, tasks, event_task_ids = (
        _plan_components(
            contract,
            lane=lane,
            enforce_commit=enforce_commit,
        )
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
            "Yahoo Finance unadjusted daily bars by exact observation window",
            "Massive point-in-time split actions through the final frozen window",
        ],
        "permanent_missing_symbol_response": "missed_trade",
        "invalid_daily_response": "missed_trade",
        "source_scope": scope[f"{lane}_scope"],
        "controller_hashes": {
            "sp500_deletion_collection.py": sha256_file(
                PROJECT_ROOT / "sp500_deletion_collection.py"
            ),
            "sp500_deletion_discovery.py": sha256_file(
                PROJECT_ROOT / "sp500_deletion_discovery.py"
            ),
        },
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
        and plan.get("broker_actions") == 0
    ):
        raise Sp500DeletionCollectionError(
            "deletion collection plan authority drifted"
        )
    if plan["controller_hashes"] != {
        "sp500_deletion_collection.py": sha256_file(
            PROJECT_ROOT / "sp500_deletion_collection.py"
        ),
        "sp500_deletion_discovery.py": sha256_file(
            PROJECT_ROOT / "sp500_deletion_discovery.py"
        ),
    }:
        raise Sp500DeletionCollectionError(
            "deletion collection controller drifted"
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
            raise Sp500DeletionCollectionError(
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
        raise Sp500DeletionCollectionError(
            "deletion plan lacks an independent inspection"
        )
    return inspection


def build_dataset(
    private_root: Path, plan: Mapping[str, Any]
) -> dict[str, Any]:
    scope = strategy_discovery.load_artifact(
        PROJECT_ROOT / str(plan["event_scope_path"]),
        expected_kind="sp500-deletion-event-scope",
    )
    events = [dict(row) for row in scope[f"{plan['lane']}_events"]]
    if canonical_sha256(events) != plan["events_sha256"]:
        raise Sp500DeletionCollectionError(
            "deletion event rows drifted after plan freeze"
        )
    task_by_id = {
        str(task["task_id"]): task for task in plan["tasks"]
    }
    factors, split_dates = shared._split_factors(
        private_root, plan
    )
    bars: dict[str, dict[str, dict[str, Any]]] = {}
    for event in events:
        task = task_by_id[
            plan["event_task_ids"][event["event_id"]]
        ]
        rows = shared._checkpoint(
            shared._checkpoint_path(private_root, task), task
        )
        symbol = str(event["ticker"])
        for row in rows:
            day = str(row.get("date", ""))
            if (
                row.get("symbol") != symbol
                or not str(task["start"])
                <= day
                <= str(task["date"])
            ):
                raise Sp500DeletionCollectionError(
                    "daily row escaped its exact observation window"
                )
            adjusted = dense_collection._adjusted_bar(
                row, day, factors.get(symbol, [])
            )
            existing = bars.setdefault(symbol, {}).get(day)
            if existing is not None and existing != adjusted:
                raise Sp500DeletionCollectionError(
                    "overlapping deletion windows disagree"
                )
            bars[symbol][day] = adjusted
    metadata = {day: [] for day in plan["evaluation_dates"]}
    for event in events:
        entry_date = str(event["entry_date"])
        if entry_date not in metadata:
            raise Sp500DeletionCollectionError(
                "deletion entry escaped the account calendar"
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
            symbol: [rows[day] for day in sorted(rows)]
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
            "invalid_daily_response": "missed_trade",
            "substitution": "forbidden",
        },
    }
    runtime.prepare_dataset(dataset)
    return dataset


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
    invoked = _timestamp(
        collected_at, "collected_at"
    ).astimezone(UTC)
    plan = _load_plan(plan_path, enforce_commit=enforce_commit)
    _plan_inspection(
        plan, inspection_path, enforce_commit=enforce_commit
    )
    if plan["lane"] == "confirmation" and invoked <= _timestamp(
        str(plan["preregistered_at"]), "preregistered_at"
    ).astimezone(UTC):
        raise Sp500DeletionCollectionError(
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
    saved = shared._telemetry_state(
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
            checkpoint_path = shared._checkpoint_path(
                private_root, task
            )
            if checkpoint_path.exists():
                shared._checkpoint(checkpoint_path, task)
                client.telemetry["cache_hits"] += 1
            else:
                rows, disposition = shared._provider_rows(
                    client, task
                )
                shared._write_gzip(
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
            shared._write_telemetry(
                telemetry_path,
                plan_sha256=str(plan["artifact_sha256"]),
                started_at=started_at,
                telemetry=shared._combine_telemetry(
                    saved["provider_telemetry"],
                    client.telemetry,
                ),
            )
        dataset = build_dataset(private_root, plan)
        dataset_path = private_root / "dataset.json.gz"
        shared._write_gzip(dataset_path, dataset, config)
    finally:
        if owns_backend:
            client.close()
    completed_at = datetime.now(UTC).isoformat().replace(
        "+00:00", "Z"
    )
    relative = str(
        dataset_path.resolve().relative_to(config.root.resolve())
    )
    missing_ids = sorted(
        str(task["task_id"])
        for task in plan["tasks"]
        if shared._read_gzip(
            shared._checkpoint_path(private_root, task)
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
        "provider_telemetry": shared._combine_telemetry(
            saved["provider_telemetry"], client.telemetry
        ),
        "permanent_missing_task_count": len(missing_ids),
        "permanent_missing_task_ids": missing_ids,
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
        Sp500DeletionCollectionError,
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
