"""Append-only public signal ledger and reproducible performance reporting."""

from __future__ import annotations

import argparse
import fcntl
import json
import math
import os
import re
import statistics
import sys
from dataclasses import asdict, dataclass
from datetime import date
from pathlib import Path
from typing import Any, Mapping, Sequence

from strategy_engine import StrategyConfig, StrategyInputError, load_config
from strategy_maturity import assess_maturity


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_LEDGER_PATH = PROJECT_ROOT / "SIGNALS.jsonl"
SCHEMA_VERSION = 1
SIGNAL_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}-[A-Z][A-Z0-9.]{0,9}-\d+$")
SESSION_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}-session(?:-\d+)?$")
SYMBOL_PATTERN = re.compile(r"^[A-Z][A-Z0-9.]{0,9}$")
UUID_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
)
FORBIDDEN_KEY_PARTS = (
    "account_number",
    "broker_order",
    "client_ref",
    "confirmation_id",
    "cancellation_id",
    "replacement_id",
    "credential",
    "password",
    "token",
    "mfa",
)


class LedgerError(ValueError):
    """Raised when a ledger record or file violates the public schema."""


@dataclass(frozen=True)
class LedgerAudit:
    path: str
    records: int
    sessions: int
    signals: int
    violations: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.violations


def _finite(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise LedgerError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise LedgerError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise LedgerError(f"{field} must be finite and >= {minimum}")
    return result


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise LedgerError(f"{field} must be an integer >= {minimum}")
    return value


def _boolean(record: Mapping[str, Any], field: str) -> bool:
    value = record.get(field)
    if not isinstance(value, bool):
        raise LedgerError(f"{field} must be true or false")
    return value


def _string(record: Mapping[str, Any], field: str, *, allow_blank: bool = False) -> str:
    value = record.get(field)
    if not isinstance(value, str) or (not allow_blank and not value.strip()):
        raise LedgerError(f"{field} must be a non-empty string")
    return value


def _validate_date(value: str, field: str) -> None:
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise LedgerError(f"{field} must be an ISO calendar date") from exc


def _validate_string_list(record: Mapping[str, Any], field: str) -> None:
    value = record.get(field)
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise LedgerError(f"{field} must be an array of non-empty strings")


def _walk_forbidden(value: Any, path: str = "record") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower()
            if any(part in normalized for part in FORBIDDEN_KEY_PARTS):
                raise LedgerError(
                    f"{path}.{key} is forbidden in the public signal ledger"
                )
            _walk_forbidden(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _walk_forbidden(child, f"{path}[{index}]")
    elif isinstance(value, str) and UUID_PATTERN.search(value):
        raise LedgerError(f"{path} contains a UUID-shaped private identifier")


def validate_record(
    record: Mapping[str, Any],
    config: StrategyConfig | None = None,
    *,
    require_current_rules: bool = True,
) -> None:
    config = config or load_config()
    _walk_forbidden(record)
    if record.get("schema_version") != SCHEMA_VERSION:
        raise LedgerError(f"schema_version must be {SCHEMA_VERSION}")
    record_type = _string(record, "record_type")
    if record_type not in ("session", "signal"):
        raise LedgerError("record_type must be session or signal")
    strategy_version = _string(record, "strategy_version")
    rules_hash = _string(record, "rules_hash")
    if require_current_rules and (
        strategy_version != config.version or rules_hash != config.rules_hash
    ):
        raise LedgerError("record does not match the current version and rules hash")
    record_date = _string(record, "date")
    _validate_date(record_date, "date")
    mode = _string(record, "mode")
    if mode not in ("live", "shadow"):
        raise LedgerError("mode must be live or shadow")
    phase = _string(record, "sample_phase")
    if phase not in ("pilot", "confirmation"):
        raise LedgerError("sample_phase must be pilot or confirmation")
    _validate_string_list(record, "rule_violations")

    if record_type == "session":
        session_id = _string(record, "session_id")
        if not SESSION_ID_PATTERN.fullmatch(session_id):
            raise LedgerError("session_id must use YYYY-MM-DD-session[-N]")
        if not session_id.startswith(record_date):
            raise LedgerError("session_id date must match date")
        _boolean(record, "closed")
        _boolean(record, "session_capture_complete")
        _boolean(record, "trade_taken")
        _integer(record.get("candidate_count"), "candidate_count")
        _integer(record.get("triggered_signal_count"), "triggered_signal_count")
        _string(record, "no_trade_reason", allow_blank=True)
        return

    signal_id = _string(record, "signal_id")
    if not SIGNAL_ID_PATTERN.fullmatch(signal_id):
        raise LedgerError("signal_id must use YYYY-MM-DD-SYMBOL-N")
    if not signal_id.startswith(record_date):
        raise LedgerError("signal_id date must match date")
    session_id = _string(record, "session_id")
    if not SESSION_ID_PATTERN.fullmatch(session_id) or not session_id.startswith(
        record_date
    ):
        raise LedgerError("signal session_id must match its date")
    symbol = _string(record, "symbol")
    if not SYMBOL_PATTERN.fullmatch(symbol) or f"-{symbol}-" not in signal_id:
        raise LedgerError("symbol must be uppercase and match signal_id")
    triggered = _boolean(record, "triggered")
    eligible = _boolean(record, "eligible")
    closed = _boolean(record, "closed")
    _boolean(record, "session_capture_complete")
    decision = _string(record, "decision")
    if decision not in ("live", "shadow", "rejected", "missed"):
        raise LedgerError("decision must be live, shadow, rejected, or missed")
    _validate_string_list(record, "rejection_reasons")
    _boolean(record, "paper_baseline_eligible")
    _boolean(record, "stop_executed")

    if triggered and eligible and decision in ("rejected", "missed"):
        raise LedgerError("an eligible triggered signal must be live or shadow logged")
    if closed and triggered:
        net_r = _finite(record.get("net_r"), "net_r")
        project_r = _finite(record.get("project_exit_net_r"), "project_exit_net_r")
        paper_r = _finite(
            record.get("paper_eod_shadow_net_r"), "paper_eod_shadow_net_r"
        )
        if not math.isclose(net_r, project_r, rel_tol=0, abs_tol=1e-9):
            raise LedgerError("net_r must equal project_exit_net_r")
        _finite(record.get("net_pnl_dollars"), "net_pnl_dollars")
        # Reading the value is sufficient; paired project/paper reporting uses it.
        _ = paper_r
    elif record.get("net_r") is not None:
        raise LedgerError("net_r is allowed only for a closed triggered signal")

    for field in (
        "entry_slippage_bps",
        "unprotected_seconds",
        "stop_slippage_bps",
        "stop_reserve_bps",
    ):
        if record.get(field) is not None:
            _finite(record[field], field, minimum=0)
    if mode == "live" and closed and triggered:
        if (
            record.get("entry_slippage_bps") is None
            or record.get("unprotected_seconds") is None
        ):
            raise LedgerError(
                "closed live signals require entry slippage and unprotected time"
            )
    if record["stop_executed"] and (
        record.get("stop_slippage_bps") is None
        or record.get("stop_reserve_bps") is None
    ):
        raise LedgerError("stop executions require actual slippage and planned reserve")


def prepare_record(
    payload: Mapping[str, Any], config: StrategyConfig | None = None
) -> dict[str, Any]:
    config = config or load_config()
    record = dict(payload)
    supplied_version = record.get("strategy_version", config.version)
    supplied_hash = record.get("rules_hash", config.rules_hash)
    if supplied_version != config.version or supplied_hash != config.rules_hash:
        raise LedgerError("cannot append a record for stale or different rules")
    record["schema_version"] = SCHEMA_VERSION
    record["strategy_version"] = config.version
    record["rules_hash"] = config.rules_hash
    validate_record(record, config)
    return record


def read_records(path: Path = DEFAULT_LEDGER_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(
        path.read_text(encoding="utf-8").splitlines(), 1
    ):
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise LedgerError(f"line {line_number}: invalid JSON: {exc.msg}") from exc
        if not isinstance(value, dict):
            raise LedgerError(f"line {line_number}: record must be an object")
        records.append(value)
    return records


def _public_id(record: Mapping[str, Any]) -> str:
    return str(record.get("signal_id") or record.get("session_id"))


def append_record(
    payload: Mapping[str, Any],
    path: Path = DEFAULT_LEDGER_PATH,
    config: StrategyConfig | None = None,
) -> dict[str, Any]:
    config = config or load_config()
    record = prepare_record(payload, config)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        handle.seek(0)
        existing_ids: set[str] = set()
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                existing = json.loads(line)
            except json.JSONDecodeError as exc:
                raise LedgerError(
                    f"existing ledger line {line_number} is invalid JSON"
                ) from exc
            existing_ids.add(_public_id(existing))
        record_id = _public_id(record)
        if record_id in existing_ids:
            raise LedgerError(f"duplicate public record id: {record_id}")
        handle.seek(0, os.SEEK_END)
        handle.write(json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
    return record


def audit_ledger(
    path: Path = DEFAULT_LEDGER_PATH, config: StrategyConfig | None = None
) -> LedgerAudit:
    config = config or load_config()
    violations: list[str] = []
    try:
        records = read_records(path)
    except LedgerError as exc:
        return LedgerAudit(str(path), 0, 0, 0, (str(exc),))
    ids: set[str] = set()
    sessions = signals = 0
    for index, record in enumerate(records, 1):
        try:
            validate_record(record, config, require_current_rules=False)
        except LedgerError as exc:
            violations.append(f"record {index}: {exc}")
        record_id = _public_id(record)
        if record_id in ids:
            violations.append(f"record {index}: duplicate public record id {record_id}")
        ids.add(record_id)
        if record.get("record_type") == "session":
            sessions += 1
        elif record.get("record_type") == "signal":
            signals += 1
    return LedgerAudit(str(path), len(records), sessions, signals, tuple(violations))


def _paired_exit_metrics(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    pairs = [
        (
            _finite(record["project_exit_net_r"], "project_exit_net_r"),
            _finite(record["paper_eod_shadow_net_r"], "paper_eod_shadow_net_r"),
        )
        for record in records
        if record.get("record_type") == "signal"
        and record.get("closed") is True
        and record.get("triggered") is True
        and record.get("project_exit_net_r") is not None
        and record.get("paper_eod_shadow_net_r") is not None
    ]
    if not pairs:
        return {
            "paired_signals": 0,
            "project_expectancy_r": None,
            "paper_eod_expectancy_r": None,
            "mean_project_minus_paper_r": None,
            "project_outperformance_rate": None,
        }
    differences = [project - paper for project, paper in pairs]
    return {
        "paired_signals": len(pairs),
        "project_expectancy_r": statistics.fmean(project for project, _ in pairs),
        "paper_eod_expectancy_r": statistics.fmean(paper for _, paper in pairs),
        "mean_project_minus_paper_r": statistics.fmean(differences),
        "project_outperformance_rate": sum(value > 0 for value in differences)
        / len(differences),
    }


def build_report(
    records: Sequence[Mapping[str, Any]], config: StrategyConfig | None = None
) -> dict[str, Any]:
    config = config or load_config()
    version_records = [
        record for record in records if record.get("strategy_version") == config.version
    ]
    current = [
        record
        for record in version_records
        if record.get("rules_hash") == config.rules_hash
    ]
    sessions = [record for record in current if record.get("record_type") == "session"]
    closed_sessions = [record for record in sessions if record.get("closed") is True]
    no_trade_sessions = [
        record for record in closed_sessions if record.get("trade_taken") is False
    ]
    assessment = assess_maturity(version_records, config)
    return {
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "session_metrics": {
            "closed_sessions": len(closed_sessions),
            "no_trade_sessions": len(no_trade_sessions),
            "no_trade_frequency": (
                len(no_trade_sessions) / len(closed_sessions)
                if closed_sessions
                else None
            ),
            "incomplete_capture_sessions": sum(
                record.get("session_capture_complete") is not True
                for record in closed_sessions
            ),
        },
        "exit_overlay": _paired_exit_metrics(current),
        "maturity": assessment.to_dict(),
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    parser.add_argument("--config", type=Path, default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)
    record = subparsers.add_parser("record", help="append one JSON record")
    record.add_argument("input", type=Path)
    subparsers.add_parser("audit", help="validate the entire ledger")
    subparsers.add_parser("report", help="calculate metrics and maturity")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    config = load_config(args.config) if args.config else load_config()
    try:
        if args.command == "record":
            payload = json.loads(args.input.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping):
                raise LedgerError("input must contain a JSON object")
            result: Any = append_record(payload, args.ledger, config)
        elif args.command == "audit":
            audit = audit_ledger(args.ledger, config)
            result = {**asdict(audit), "valid": audit.valid}
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0 if audit.valid else 1
        else:
            result = build_report(read_records(args.ledger), config)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (OSError, json.JSONDecodeError, LedgerError, StrategyInputError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
