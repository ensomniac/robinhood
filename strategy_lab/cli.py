"""Single operator CLI for the Strategy Lab platform."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from time import monotonic
from typing import Any

from dotenv import load_dotenv

from .bridge import BridgeWorker, SmartSiouxClient
from .config import ConfigError, DEFAULT_CONFIG_PATH, load_config
from .data import HistoricalCatalog
from .database import LabDatabase
from .live import CodexMCPExecutor
from .runner import StrategyLabRunner
from .scheduler import install as install_scheduler
from .scheduler import status as scheduler_status
from .scheduler import uninstall as uninstall_scheduler
from .snapshot import build_and_write_snapshot


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, default=str))


def _symbols(value: str | None) -> list[str] | None:
    if value is None:
        return None
    result = sorted({item.strip().upper() for item in value.split(",") if item.strip()})
    if not result:
        raise argparse.ArgumentTypeError("symbols cannot be empty")
    return result


def _catalog_progress_publisher(
    config: Any,
    database: LabDatabase,
) -> Any:
    client = SmartSiouxClient(config)
    last_publish = 0.0

    def publish(progress: dict[str, Any]) -> None:
        nonlocal last_publish
        now = monotonic()
        terminal = progress["status"] in {"BUILDING_FEATURES", "READY"}
        if client.configured() and (terminal or now - last_publish >= 10):
            client.publish(build_and_write_snapshot(config, database))
            last_publish = now

    return publish


def _research_progress_publisher(
    config: Any,
    database: LabDatabase,
) -> Any:
    client = SmartSiouxClient(config)

    def publish(_run_id: str) -> None:
        if client.configured():
            client.publish(build_and_write_snapshot(config, database))

    return publish


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Observable declarative strategy discovery and validation"
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("init", help="initialize the local DuckDB state")
    subparsers.add_parser("status", help="show the compact platform status")

    data = subparsers.add_parser("data", help="catalog immutable local history")
    data_sub = data.add_subparsers(dest="data_command", required=True)
    sync = data_sub.add_parser(
        "sync", help="incrementally sync and build the feature mart"
    )
    sync.add_argument("--symbols")
    sync.add_argument("--maximum-symbols", type=int)
    sync.add_argument("--no-feature-rebuild", action="store_true")

    run = subparsers.add_parser("run", help="execute or reproduce research")
    run_sub = run.add_subparsers(dest="run_command", required=True)
    daily = run_sub.add_parser("daily", help="test 50-500 new unique configurations")
    daily.add_argument("--date", type=date.fromisoformat)
    daily.add_argument("--target", type=int)
    daily.add_argument("--allow-dirty", action="store_true", help=argparse.SUPPRESS)
    run_sub.add_parser("scheduled", help="after-close local sync and daily run")
    reproduce = run_sub.add_parser("reproduce", help="rebuild an exact prior run")
    reproduce.add_argument("run_id")

    candidate = subparsers.add_parser(
        "candidate", help="candidate evidence transitions"
    )
    candidate_sub = candidate.add_subparsers(dest="candidate_command", required=True)
    promote = candidate_sub.add_parser("promote", help="open locked holdout once")
    promote.add_argument("strategy_id")

    paper = subparsers.add_parser("paper", help="prospective paper evidence")
    paper_sub = paper.add_subparsers(dest="paper_command", required=True)
    signal = paper_sub.add_parser("signal", help="record one prospective paper signal")
    signal.add_argument("strategy_id")
    signal.add_argument("--clean", action=argparse.BooleanOptionalAction, required=True)
    signal.add_argument("--payload-json", default="{}")

    snapshot = subparsers.add_parser(
        "snapshot", help="build or publish dashboard state"
    )
    snapshot_sub = snapshot.add_subparsers(dest="snapshot_command", required=True)
    snapshot_sub.add_parser("build")
    snapshot_sub.add_parser("publish")

    bridge = subparsers.add_parser("bridge", help="signed SmartSioux worker")
    bridge_sub = bridge.add_subparsers(dest="bridge_command", required=True)
    bridge_sub.add_parser("once")

    live = subparsers.add_parser("live", help="run the armed-candidate MCP tick")
    live_sub = live.add_subparsers(dest="live_command", required=True)
    live_sub.add_parser("tick")

    scheduler = subparsers.add_parser("scheduler", help="manage local LaunchAgents")
    scheduler_sub = scheduler.add_subparsers(dest="scheduler_command", required=True)
    scheduler_sub.add_parser("install")
    scheduler_sub.add_parser("status")
    scheduler_sub.add_parser("uninstall")
    return parser


def _status(database: LabDatabase) -> dict[str, Any]:
    latest_run = database.connection.execute(
        """
        SELECT run_id, run_kind, status, started_at, completed_at,
               tested_configurations, accepted_configurations, error
        FROM runs ORDER BY started_at DESC LIMIT 1
        """
    ).fetchone()
    candidate_states = {
        str(state): int(count)
        for state, count in database.connection.execute(
            "SELECT state, count(*) FROM candidates GROUP BY state ORDER BY state"
        ).fetchall()
    }
    return {
        "status": "READY",
        "database": str(database.config.database_path),
        "data": database.latest_data_version(),
        "counts": {
            table: database.table_count(table)
            for table in (
                "raw_files",
                "observations",
                "ideas",
                "specs",
                "runs",
                "experiment_results",
                "trades",
                "candidates",
                "commands",
                "events",
            )
        },
        "candidate_states": candidate_states,
        "latest_run": (
            {
                "run_id": latest_run[0],
                "run_kind": latest_run[1],
                "status": latest_run[2],
                "started_at": latest_run[3],
                "completed_at": latest_run[4],
                "tested_configurations": latest_run[5],
                "accepted_configurations": latest_run[6],
                "error": latest_run[7],
            }
            if latest_run
            else None
        ),
        "research_provider_requests": 0,
        "broker_actions": 0,
    }


def main(argv: list[str] | None = None) -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "scheduler":
            result = {
                "install": install_scheduler,
                "status": lambda _: scheduler_status(),
                "uninstall": lambda _: uninstall_scheduler(),
            }[args.scheduler_command](config)
            _json(result)
            return 0
        bridge_once = args.command == "bridge" and args.bridge_command == "once"
        with config.runtime_lock(blocking=not bridge_once):
            with LabDatabase(config) as database:
                result = execute_database_command(args, config, database, parser)
        _json(result)
        return 0
    except ConfigError as exc:
        if (
            "runtime is busy" in str(exc)
            and "args" in locals()
            and args.command == "bridge"
        ):
            _json({"status": "BUSY", "message": str(exc)})
            return 0
        _json({"status": "ERROR", "error": str(exc), "error_type": type(exc).__name__})
        return 1
    except Exception as exc:
        _json({"status": "ERROR", "error": str(exc), "error_type": type(exc).__name__})
        return 1


def execute_database_command(
    args: argparse.Namespace,
    config: Any,
    database: LabDatabase,
    parser: argparse.ArgumentParser,
) -> dict[str, Any]:
    if args.command == "init":
        result = _status(database)
    elif args.command == "status":
        result = _status(database)
    elif args.command == "data":
        result = HistoricalCatalog(config, database).sync(
            symbols=_symbols(args.symbols),
            maximum_symbols=args.maximum_symbols,
            rebuild_features=not args.no_feature_rebuild,
            progress_callback=_catalog_progress_publisher(config, database),
        )
    elif args.command == "run":
        runner = StrategyLabRunner(config, database)
        if args.run_command == "daily":
            result = runner.run_daily(
                research_date=args.date,
                target=args.target,
                allow_dirty=args.allow_dirty,
                progress_callback=_research_progress_publisher(config, database),
            )
        elif args.run_command == "scheduled":
            if database.get_metadata("scheduler_paused") == "true":
                result = {"status": "PAUSED"}
            else:
                progress = _catalog_progress_publisher(config, database)
                catalog = HistoricalCatalog(config, database).sync(
                    progress_callback=progress
                )
                result = {
                    "status": "COMPLETED",
                    "catalog": catalog,
                    "research": runner.run_daily(
                        progress_callback=_research_progress_publisher(
                            config, database
                        ),
                    ),
                }
        else:
            result = runner.reproduce(args.run_id)
    elif args.command == "candidate":
        result = StrategyLabRunner(config, database).promote_holdout(args.strategy_id)
    elif args.command == "paper":
        payload = json.loads(args.payload_json)
        if not isinstance(payload, dict):
            raise ValueError("--payload-json must contain an object")
        result = StrategyLabRunner(config, database).record_paper_signal(
            args.strategy_id, clean=args.clean, payload=payload
        )
    elif args.command == "snapshot":
        if args.snapshot_command == "build":
            result = build_and_write_snapshot(config, database)
        else:
            result = BridgeWorker(config, database).run_once()
    elif args.command == "bridge":
        worker = BridgeWorker(config, database)
        result = worker.run_once()
    elif args.command == "live":
        result = {
            "status": "READY",
            "results": CodexMCPExecutor(config, database).tick(),
        }
    else:
        parser.error("unhandled command")
        raise RuntimeError("unhandled command")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
