"""Typed configuration loading and relationship validation."""

from __future__ import annotations

import os
import secrets
import tomllib
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "strategy_lab.toml"


class ConfigError(ValueError):
    """Raised when Strategy Lab configuration is internally unsafe."""


@dataclass(frozen=True)
class LabConfig:
    raw: dict[str, Any]
    path: Path

    def section(self, name: str) -> dict[str, Any]:
        value = self.raw.get(name)
        if not isinstance(value, dict):
            raise ConfigError(f"missing configuration section [{name}]")
        return value

    @property
    def historical_data_root(self) -> Path:
        override = os.getenv("LOCAL_HISTORICAL_DATA_ROOT")
        raw = override or str(self.section("paths")["historical_data_root"])
        return Path(raw).expanduser().resolve()

    @property
    def state_root(self) -> Path:
        override = os.getenv("STRATEGY_LAB_STATE_ROOT")
        if override:
            return Path(override).expanduser().resolve()
        name = str(self.section("paths")["state_directory_name"])
        return self.historical_data_root / name

    @property
    def database_path(self) -> Path:
        return self.state_root / "strategy_lab.duckdb"

    @property
    def feature_path(self) -> Path:
        return self.state_root / "marts" / "daily_features.parquet"

    @property
    def snapshot_path(self) -> Path:
        return self.state_root / "snapshots" / "current.json"

    @property
    def command_spool(self) -> Path:
        return self.state_root / "commands"

    @property
    def bridge_secret_path(self) -> Path:
        raw = self.section("bridge").get("hmac_secret_file")
        if raw:
            return Path(str(raw)).expanduser().resolve()
        return self.state_root / "bridge_hmac_secret"

    @property
    def runtime_lock_path(self) -> Path:
        return self.state_root / ".runtime.lock"

    def ensure_state_directories(self) -> None:
        self.state_root.mkdir(parents=True, exist_ok=True)
        self.feature_path.parent.mkdir(parents=True, exist_ok=True)
        self.snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        self.command_spool.mkdir(parents=True, exist_ok=True)

    def ensure_bridge_secret(self) -> Path:
        """Create the local half of the HMAC transport once with mode 0600."""
        path = self.bridge_secret_path
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            value = path.read_text(encoding="utf-8").strip()
            if len(value) < 32:
                raise ConfigError("configured Strategy Lab bridge secret is invalid")
            os.chmod(path, 0o600)
            return path
        with os.fdopen(descriptor, "w", encoding="utf-8") as output:
            output.write(secrets.token_hex(32) + "\n")
        return path

    @contextmanager
    def runtime_lock(self, *, blocking: bool = True):
        """Serialize DuckDB writers across research and bridge processes."""
        import fcntl

        self.ensure_state_directories()
        with self.runtime_lock_path.open("a+", encoding="utf-8") as lock:
            operation = fcntl.LOCK_EX
            if not blocking:
                operation |= fcntl.LOCK_NB
            try:
                fcntl.flock(lock.fileno(), operation)
            except BlockingIOError as error:
                raise ConfigError("Strategy Lab runtime is busy") from error
            try:
                yield
            finally:
                fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _positive_number(section: dict[str, Any], key: str) -> float:
    value = section.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise ConfigError(f"{key} must be a positive number")
    return float(value)


def validate_config(raw: dict[str, Any]) -> None:
    if raw.get("schema_version") != 1:
        raise ConfigError("strategy_lab schema_version must be 1")
    required = {
        "paths",
        "catalog",
        "daily_run",
        "idea_generation",
        "dsl",
        "execution",
        "pilot_risk",
        "validation",
        "bridge",
    }
    missing = sorted(required.difference(raw))
    if missing:
        raise ConfigError(f"missing configuration sections: {missing}")

    daily = raw["daily_run"]
    minimum = int(_positive_number(daily, "minimum_configurations"))
    target = int(_positive_number(daily, "target_configurations"))
    maximum = int(_positive_number(daily, "maximum_configurations"))
    if not 50 <= minimum <= target <= maximum <= 500:
        raise ConfigError(
            "daily configuration counts must satisfy 50 <= min <= target <= max <= 500"
        )
    if int(daily.get("workers", 0)) < 1:
        raise ConfigError("daily_run.workers must be positive")

    catalog = raw["catalog"]
    common = int(_positive_number(catalog, "minimum_common_sessions"))
    development = int(_positive_number(catalog, "minimum_development_sessions"))
    holdout = int(_positive_number(catalog, "minimum_holdout_sessions"))
    embargo = int(_positive_number(catalog, "embargo_sessions"))
    if common < development + holdout:
        raise ConfigError(
            "minimum common sessions cannot cover development and holdout"
        )
    if embargo >= holdout:
        raise ConfigError("embargo must be shorter than the holdout")

    execution = raw["execution"]
    if execution.get("same_interval_ambiguity") != "stop_first":
        raise ConfigError("same_interval_ambiguity must remain stop_first")
    if int(execution.get("maximum_holding_trading_days", 0)) > 5:
        raise ConfigError("maximum holding period cannot exceed five trading days")
    if int(execution.get("maximum_concurrent_positions", 0)) > 3:
        raise ConfigError("maximum concurrent positions cannot exceed three")
    if int(execution.get("maximum_new_entries_per_day", 0)) > 5:
        raise ConfigError("maximum new entries per day cannot exceed five")

    risk = raw["pilot_risk"]
    if float(risk.get("maximum_gross_notional_fraction", 0)) > 1.0:
        raise ConfigError("pilot gross notional cannot exceed 100%")
    if float(risk.get("maximum_planned_loss_fraction_per_position", 0)) > 0.005:
        raise ConfigError("pilot planned loss per position cannot exceed 0.5%")

    idea = raw["idea_generation"]
    if bool(idea.get("store_responses")):
        raise ConfigError("idea-generation responses must use store=false")
    if float(idea.get("daily_budget_usd", 0)) > 5.0:
        raise ConfigError("idea-generation daily budget cannot exceed $5")

    bridge = raw["bridge"]
    if str(bridge.get("operator_email") or "").strip().lower() != "ryan@ensomniac.com":
        raise ConfigError("Strategy Lab operator must remain ryan@ensomniac.com")


def load_config(path: Path | str = DEFAULT_CONFIG_PATH) -> LabConfig:
    resolved = Path(path).expanduser().resolve()
    with resolved.open("rb") as source:
        raw = tomllib.load(source)
    validate_config(raw)
    return LabConfig(raw=raw, path=resolved)
