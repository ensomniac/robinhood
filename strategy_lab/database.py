"""DuckDB state, lineage, experiment, and operation catalog."""

from __future__ import annotations

import json
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Iterator

import duckdb

from .config import LabConfig
from .contracts import CandidateState, StrategySpec
from .hashing import canonical_json


SCHEMA_VERSION = 1


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


class LabDatabase:
    def __init__(self, config: LabConfig, *, read_only: bool = False) -> None:
        self.config = config
        self.config.ensure_state_directories()
        self.connection = duckdb.connect(
            str(config.database_path),
            read_only=read_only,
            config={
                "threads": str(config.section("daily_run")["workers"]),
                "memory_limit": f"{config.section('daily_run')['memory_limit_gb']}GB",
            },
        )
        if not read_only:
            self.initialize()

    def close(self) -> None:
        self.connection.close()

    def __enter__(self) -> "LabDatabase":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self.connection.execute("BEGIN TRANSACTION")
        try:
            yield
        except BaseException:
            self.connection.execute("ROLLBACK")
            raise
        else:
            self.connection.execute("COMMIT")

    def initialize(self) -> None:
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS metadata (
                key VARCHAR PRIMARY KEY,
                value VARCHAR NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raw_files (
                path VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                session_date DATE NOT NULL,
                size_bytes BIGINT NOT NULL,
                modified_ns BIGINT NOT NULL,
                content_identity VARCHAR NOT NULL,
                disposition VARCHAR NOT NULL,
                inspected_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS securities (
                symbol VARCHAR NOT NULL,
                security_type VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                valid_from DATE NOT NULL,
                valid_to DATE,
                instrument_id VARCHAR NOT NULL,
                primary_exchange VARCHAR NOT NULL,
                PRIMARY KEY(symbol, valid_from, instrument_id)
            );
            CREATE TABLE IF NOT EXISTS observations (
                symbol VARCHAR NOT NULL,
                session_date DATE NOT NULL,
                security_type VARCHAR NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume DOUBLE NOT NULL,
                opening_return_30m DOUBLE,
                opening_range_pct_30m DOUBLE,
                intraday_entry_price DOUBLE,
                intraday_future_high DOUBLE,
                intraday_future_low DOUBLE,
                intraday_exit_price DOUBLE,
                daily_complete BOOLEAN NOT NULL,
                intraday_complete BOOLEAN NOT NULL,
                provider VARCHAR NOT NULL,
                source_identity VARCHAR NOT NULL,
                source_path VARCHAR NOT NULL,
                PRIMARY KEY(symbol, session_date)
            );
            CREATE TABLE IF NOT EXISTS data_versions (
                data_version_id VARCHAR PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL,
                dataset_sha256 VARCHAR NOT NULL UNIQUE,
                feature_sha256 VARCHAR NOT NULL,
                observation_count BIGINT NOT NULL,
                symbol_count BIGINT NOT NULL,
                session_count BIGINT NOT NULL,
                first_session DATE,
                last_session DATE,
                development_end DATE,
                embargo_start DATE,
                holdout_start DATE,
                capacity_state VARCHAR NOT NULL,
                manifest_json VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS ideas (
                idea_id VARCHAR PRIMARY KEY,
                family_id VARCHAR NOT NULL,
                source VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                payload_json VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS specs (
                strategy_id VARCHAR PRIMARY KEY,
                rules_sha256 VARCHAR NOT NULL UNIQUE,
                family_id VARCHAR NOT NULL,
                idea_id VARCHAR NOT NULL,
                horizon VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                spec_json VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runs (
                run_id VARCHAR PRIMARY KEY,
                run_kind VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                started_at TIMESTAMPTZ NOT NULL,
                completed_at TIMESTAMPTZ,
                data_version_id VARCHAR,
                code_commit VARCHAR NOT NULL,
                target_configurations INTEGER NOT NULL,
                generated_configurations INTEGER NOT NULL DEFAULT 0,
                tested_configurations INTEGER NOT NULL DEFAULT 0,
                accepted_configurations INTEGER NOT NULL DEFAULT 0,
                error VARCHAR,
                manifest_json VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS experiment_results (
                run_id VARCHAR NOT NULL,
                strategy_id VARCHAR NOT NULL,
                rules_sha256 VARCHAR NOT NULL,
                family_id VARCHAR NOT NULL,
                phase VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                trade_count INTEGER NOT NULL,
                total_log_growth DOUBLE NOT NULL,
                profit_factor DOUBLE NOT NULL,
                stressed_profit_factor DOUBLE NOT NULL,
                win_rate DOUBLE NOT NULL,
                win_rate_lower_bound DOUBLE NOT NULL,
                bootstrap_lower_expectancy DOUBLE NOT NULL,
                maximum_drawdown_r DOUBLE NOT NULL,
                deflated_sharpe_probability DOUBLE NOT NULL,
                probability_backtest_overfit DOUBLE NOT NULL,
                holm_pass BOOLEAN NOT NULL,
                neighbor_stability BOOLEAN NOT NULL,
                gate_failures_json VARCHAR NOT NULL,
                metrics_json VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL,
                PRIMARY KEY(run_id, strategy_id, phase)
            );
            CREATE TABLE IF NOT EXISTS trades (
                run_id VARCHAR NOT NULL,
                strategy_id VARCHAR NOT NULL,
                phase VARCHAR NOT NULL,
                signal_date DATE NOT NULL,
                entry_date DATE NOT NULL,
                exit_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                entry_price DOUBLE NOT NULL,
                exit_price DOUBLE NOT NULL,
                exit_reason VARCHAR NOT NULL,
                gross_return DOUBLE NOT NULL,
                net_return DOUBLE NOT NULL,
                return_r DOUBLE NOT NULL,
                notional_fraction DOUBLE NOT NULL,
                rank_value DOUBLE,
                PRIMARY KEY(run_id, strategy_id, phase, signal_date, symbol)
            );
            CREATE TABLE IF NOT EXISTS candidates (
                strategy_id VARCHAR PRIMARY KEY,
                rules_sha256 VARCHAR NOT NULL,
                family_id VARCHAR NOT NULL,
                state VARCHAR NOT NULL,
                state_reason VARCHAR NOT NULL,
                development_run_id VARCHAR,
                holdout_run_id VARCHAR,
                paper_clean_signals INTEGER NOT NULL DEFAULT 0,
                live_executions INTEGER NOT NULL DEFAULT 0,
                natural_stop_executions INTEGER NOT NULL DEFAULT 0,
                armed BOOLEAN NOT NULL DEFAULT FALSE,
                armed_until TIMESTAMPTZ,
                updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS holdout_access (
                access_id VARCHAR PRIMARY KEY,
                strategy_id VARCHAR NOT NULL,
                rules_sha256 VARCHAR NOT NULL,
                family_id VARCHAR NOT NULL,
                run_id VARCHAR NOT NULL,
                opened_at TIMESTAMPTZ NOT NULL,
                data_version_id VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS paper_events (
                event_id VARCHAR PRIMARY KEY,
                strategy_id VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                clean BOOLEAN NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL,
                payload_json VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS live_events (
                event_id VARCHAR PRIMARY KEY,
                strategy_id VARCHAR NOT NULL,
                event_type VARCHAR NOT NULL,
                observed_at TIMESTAMPTZ NOT NULL,
                payload_json VARCHAR NOT NULL
            );
            CREATE TABLE IF NOT EXISTS commands (
                command_id VARCHAR PRIMARY KEY,
                command_type VARCHAR NOT NULL,
                status VARCHAR NOT NULL,
                strategy_id VARCHAR,
                rules_sha256 VARCHAR,
                requested_by VARCHAR NOT NULL,
                requested_at TIMESTAMPTZ NOT NULL,
                expires_at TIMESTAMPTZ NOT NULL,
                payload_json VARCHAR NOT NULL,
                result_json VARCHAR,
                updated_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR PRIMARY KEY,
                event_type VARCHAR NOT NULL,
                severity VARCHAR NOT NULL,
                entity_type VARCHAR NOT NULL,
                entity_id VARCHAR,
                message VARCHAR NOT NULL,
                payload_json VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS idea_usage (
                usage_id VARCHAR PRIMARY KEY,
                research_date DATE NOT NULL,
                model VARCHAR NOT NULL,
                input_tokens BIGINT NOT NULL,
                output_tokens BIGINT NOT NULL,
                estimated_cost_usd DOUBLE NOT NULL,
                prompt_sha256 VARCHAR NOT NULL,
                response_sha256 VARCHAR NOT NULL,
                created_at TIMESTAMPTZ NOT NULL
            );
            """
        )
        self.set_metadata("schema_version", str(SCHEMA_VERSION))

    def set_metadata(self, key: str, value: str) -> None:
        self.connection.execute(
            """
            INSERT INTO metadata VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
            """,
            [key, value, utc_now()],
        )

    def get_metadata(self, key: str) -> str | None:
        row = self.connection.execute(
            "SELECT value FROM metadata WHERE key = ?", [key]
        ).fetchone()
        return str(row[0]) if row else None

    def register_spec(
        self, spec: StrategySpec, *, created_at: str | None = None
    ) -> bool:
        existing = self.connection.execute(
            "SELECT strategy_id FROM specs WHERE rules_sha256 = ?", [spec.rules_sha256]
        ).fetchone()
        if existing:
            return False
        self.connection.execute(
            "INSERT INTO specs VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                spec.strategy_id,
                spec.rules_sha256,
                spec.family_id,
                spec.idea_id,
                spec.horizon,
                created_at or utc_now(),
                canonical_json(spec.to_dict()),
            ],
        )
        return True

    def load_spec(self, strategy_id: str) -> StrategySpec:
        row = self.connection.execute(
            "SELECT spec_json FROM specs WHERE strategy_id = ?", [strategy_id]
        ).fetchone()
        if not row:
            raise KeyError(f"unknown strategy {strategy_id}")
        return StrategySpec.from_dict(json.loads(str(row[0])))

    def latest_data_version(self) -> dict[str, Any] | None:
        row = self.connection.execute(
            """
            SELECT data_version_id, dataset_sha256, feature_sha256, observation_count,
                   symbol_count, session_count, first_session, last_session,
                   development_end, embargo_start, holdout_start, capacity_state,
                   manifest_json
            FROM data_versions ORDER BY created_at DESC LIMIT 1
            """
        ).fetchone()
        if not row:
            return None
        keys = [
            "data_version_id",
            "dataset_sha256",
            "feature_sha256",
            "observation_count",
            "symbol_count",
            "session_count",
            "first_session",
            "last_session",
            "development_end",
            "embargo_start",
            "holdout_start",
            "capacity_state",
            "manifest_json",
        ]
        result = dict(zip(keys, row, strict=True))
        result["manifest"] = json.loads(str(result.pop("manifest_json")))
        for key in (
            "first_session",
            "last_session",
            "development_end",
            "embargo_start",
            "holdout_start",
        ):
            if result[key] is not None:
                result[key] = result[key].isoformat()
        return result

    def emit_event(
        self,
        *,
        event_id: str,
        event_type: str,
        severity: str,
        entity_type: str,
        entity_id: str | None,
        message: str,
        payload: dict[str, Any] | None = None,
    ) -> None:
        self.connection.execute(
            "INSERT INTO events VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                event_id,
                event_type,
                severity,
                entity_type,
                entity_id,
                message,
                canonical_json(payload or {}),
                utc_now(),
            ],
        )

    def upsert_candidate(
        self,
        spec: StrategySpec,
        *,
        state: CandidateState,
        reason: str,
        development_run_id: str | None = None,
        holdout_run_id: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO candidates(
                strategy_id, rules_sha256, family_id, state, state_reason,
                development_run_id, holdout_run_id, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(strategy_id) DO UPDATE SET
                state=excluded.state,
                state_reason=excluded.state_reason,
                development_run_id=COALESCE(excluded.development_run_id, candidates.development_run_id),
                holdout_run_id=COALESCE(excluded.holdout_run_id, candidates.holdout_run_id),
                updated_at=excluded.updated_at
            """,
            [
                spec.strategy_id,
                spec.rules_sha256,
                spec.family_id,
                str(state),
                reason,
                development_run_id,
                holdout_run_id,
                utc_now(),
            ],
        )

    def table_count(self, table: str) -> int:
        allowed = {
            "raw_files",
            "observations",
            "data_versions",
            "ideas",
            "specs",
            "runs",
            "experiment_results",
            "trades",
            "candidates",
            "commands",
            "events",
        }
        if table not in allowed:
            raise ValueError(f"unsupported table {table!r}")
        return int(
            self.connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        )
