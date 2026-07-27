"""Compact redacted observability snapshot for SmartSioux."""

from __future__ import annotations

import os
import platform
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from .config import LabConfig
from .database import LabDatabase
from .hashing import canonical_json, canonical_sha256


def _rows(
    database: LabDatabase, query: str, parameters: list[Any] | None = None
) -> list[dict[str, Any]]:
    values = database.connection.execute(query, parameters or []).fetchall()
    columns = [description[0] for description in database.connection.description]
    result: list[dict[str, Any]] = []
    for row in values:
        item = dict(zip(columns, row, strict=True))
        for key, value in list(item.items()):
            if hasattr(value, "isoformat"):
                item[key] = value.isoformat()
        result.append(item)
    return result


def build_snapshot(config: LabConfig, database: LabDatabase) -> dict[str, Any]:
    data_version = database.latest_data_version()
    raw_catalog_progress = database.get_metadata("catalog_progress")
    try:
        catalog_progress = (
            json.loads(raw_catalog_progress) if raw_catalog_progress else None
        )
    except json.JSONDecodeError:
        catalog_progress = None
    run_counts = _rows(
        database,
        """
        SELECT
            count(*) AS total_runs,
            count(*) FILTER (WHERE status='COMPLETED') AS completed_runs,
            count(*) FILTER (WHERE status='FAILED') AS failed_runs,
            coalesce(sum(tested_configurations), 0) AS tested_configurations,
            coalesce(sum(accepted_configurations), 0) AS accepted_configurations
        FROM runs
        """,
    )[0]
    states = {
        str(row["state"]): int(row["count"])
        for row in _rows(
            database,
            "SELECT state, count(*) AS count FROM candidates GROUP BY state ORDER BY state",
        )
    }
    latest_runs = _rows(
        database,
        """
        SELECT run_id, run_kind, status, started_at, completed_at,
               target_configurations, generated_configurations,
               tested_configurations, accepted_configurations, error
        FROM runs ORDER BY started_at DESC LIMIT 20
        """,
    )
    leaders = _rows(
        database,
        """
        SELECT
            result.strategy_id,
            result.family_id,
            candidate.state,
            result.trade_count,
            result.stressed_profit_factor,
            result.win_rate,
            result.win_rate_lower_bound,
            result.bootstrap_lower_expectancy,
            result.maximum_drawdown_r,
            result.deflated_sharpe_probability,
            result.probability_backtest_overfit,
            CAST(json_extract(result.metrics_json, '$.stressed_total_log_growth') AS DOUBLE)
                AS stressed_log_growth
        FROM experiment_results AS result
        JOIN candidates AS candidate USING(strategy_id)
        WHERE result.phase='development'
        QUALIFY row_number() OVER (
            PARTITION BY result.strategy_id ORDER BY result.created_at DESC
        ) = 1
        ORDER BY stressed_log_growth DESC, result.strategy_id
        LIMIT 30
        """,
    )
    candidates = _rows(
        database,
        """
        SELECT strategy_id, rules_sha256, family_id, state, state_reason,
               paper_clean_signals, live_executions, natural_stop_executions,
               armed, armed_until, updated_at
        FROM candidates
        WHERE state <> 'REJECTED'
        ORDER BY updated_at DESC
        LIMIT 50
        """,
    )
    events = _rows(
        database,
        """
        SELECT event_id, event_type, severity, entity_type, entity_id, message, created_at
        FROM events ORDER BY created_at DESC LIMIT 50
        """,
    )
    usage = _rows(
        database,
        """
        SELECT research_date, model, sum(input_tokens) AS input_tokens,
               sum(output_tokens) AS output_tokens,
               sum(estimated_cost_usd) AS estimated_cost_usd
        FROM idea_usage
        GROUP BY research_date, model
        ORDER BY research_date DESC LIMIT 14
        """,
    )
    latest_published = datetime.now(UTC).isoformat()
    payload: dict[str, Any] = {
        "schema_version": int(config.section("bridge")["snapshot_schema_version"]),
        "published_at": latest_published,
        "source": {
            "host": platform.node(),
            "platform": platform.platform(),
            "process_id": os.getpid(),
        },
        "data": data_version,
        "summary": {
            **run_counts,
            "candidate_states": states,
            "unique_specs": database.table_count("specs"),
            "mechanism_ideas": database.table_count("ideas"),
            "provider_requests": 0,
            "broker_actions_during_research": 0,
        },
        "runs": latest_runs,
        "leaders": leaders,
        "candidates": candidates,
        "events": events,
        "idea_usage": usage,
        "worker": {
            "paused": database.get_metadata("scheduler_paused") == "true",
            "last_command_poll": database.get_metadata("last_command_poll"),
            "catalog_progress": catalog_progress,
            "live_enabled": os.getenv(
                str(config.section("bridge")["live_enabled_env"]), ""
            )
            == "1",
        },
    }
    payload["snapshot_sha256"] = canonical_sha256(payload)
    return payload


def write_snapshot(config: LabConfig, payload: dict[str, Any]) -> Path:
    config.ensure_state_directories()
    path = config.snapshot_path
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(canonical_json(payload) + "\n", encoding="utf-8")
    os.chmod(temporary, 0o600)
    os.replace(temporary, path)
    return path


def build_and_write_snapshot(
    config: LabConfig, database: LabDatabase
) -> dict[str, Any]:
    payload = build_snapshot(config, database)
    write_snapshot(config, payload)
    return payload
