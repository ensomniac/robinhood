"""Validate, close, and archive public trade context files.

Active context is intentionally ephemeral: it contains only today's live/shadow
work (or the one historical replay currently running).  Closing a context adds a
human-readable review plus an embedded JSON outcome, then moves the file to the
date-partitioned archive.  The embedded record is the durable learning dataset;
keeping it in the context avoids a second index that could drift out of sync.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import tempfile
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from zoneinfo import ZoneInfo

from strategy_engine import StrategyConfig, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ACTIVE_ROOT = PROJECT_ROOT / "trades" / "active"
DEFAULT_ARCHIVE_ROOT = PROJECT_ROOT / "trades" / "archived"
OUTCOME_SCHEMA_VERSION = 1
OUTCOME_JSON_START = "<!-- trade-outcome-json:start"
OUTCOME_JSON_END = "trade-outcome-json:end -->"
OUTCOME_SECTION_START = "<!-- trade-outcome-summary:start -->"
OUTCOME_SECTION_END = "<!-- trade-outcome-summary:end -->"
CONTEXT_ID_PATTERN = re.compile(
    r"^\d{4}-\d{2}-\d{2}-(?:session(?:-\d+)?|[A-Z][A-Z0-9.]{0,9}-\d+)$"
)
UUID_PATTERN = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-"
    r"[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b"
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
ALLOWED_CONTEXT_KINDS = ("session", "trade_idea", "trade")
ALLOWED_RESULTS = (
    "success",
    "failure",
    "flat",
    "rejected",
    "stale",
    "no_trade",
    "completed",
)
ALLOWED_THESIS_RESULTS = ("held", "failed", "mixed", "not_applicable")


class LifecycleError(ValueError):
    """Raised when context lifecycle state is unsafe or inconsistent."""


@dataclass(frozen=True)
class LifecycleAudit:
    active_contexts: int
    archived_contexts: int
    violations: tuple[str, ...]

    @property
    def valid(self) -> bool:
        return not self.violations


def _nonempty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LifecycleError(f"{field} must be a non-empty string")
    return value.strip()


def _string_list(value: Any, field: str, *, allow_empty: bool = True) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise LifecycleError(f"{field} must be an array of non-empty strings")
    if not allow_empty and not value:
        raise LifecycleError(f"{field} must contain at least one item")
    return [item.strip() for item in value]


def _validate_public_value(value: Any, path: str = "outcome") -> None:
    """Reject private identifiers and non-JSON-safe numeric values."""
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).lower()
            if any(part in normalized for part in FORBIDDEN_KEY_PARTS):
                raise LifecycleError(
                    f"{path}.{key} is forbidden in public outcome data"
                )
            _validate_public_value(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_public_value(child, f"{path}[{index}]")
    elif isinstance(value, str):
        if UUID_PATTERN.search(value):
            raise LifecycleError(f"{path} contains a UUID-shaped private identifier")
        if "enc:fernet:" in value:
            raise LifecycleError(f"{path} contains an encrypted broker identifier")
    elif isinstance(value, float) and not math.isfinite(value):
        raise LifecycleError(f"{path} contains a non-finite number")
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise LifecycleError(f"{path} contains a non-JSON value")


def prepare_outcome(
    payload: Mapping[str, Any],
    config: StrategyConfig | None = None,
    *,
    archived_at: datetime | None = None,
    require_current_rules: bool = True,
) -> dict[str, Any]:
    """Return a normalized, current-rules terminal outcome record."""
    config = config or load_config()
    context_id = _nonempty_string(payload.get("context_id"), "context_id")
    if not CONTEXT_ID_PATTERN.fullmatch(context_id):
        raise LifecycleError(
            "context_id must use YYYY-MM-DD-session[-N] or YYYY-MM-DD-SYMBOL-N"
        )
    day_text = _nonempty_string(payload.get("date"), "date")
    try:
        date.fromisoformat(day_text)
    except ValueError as exc:
        raise LifecycleError("date must be an ISO calendar date") from exc
    if not context_id.startswith(day_text):
        raise LifecycleError("context_id date must match date")

    kind = _nonempty_string(payload.get("context_kind"), "context_kind")
    if kind not in ALLOWED_CONTEXT_KINDS:
        raise LifecycleError(f"context_kind must be one of {ALLOWED_CONTEXT_KINDS}")
    result = _nonempty_string(payload.get("result"), "result")
    if result not in ALLOWED_RESULTS:
        raise LifecycleError(f"result must be one of {ALLOWED_RESULTS}")
    thesis_result = _nonempty_string(payload.get("thesis_result"), "thesis_result")
    if thesis_result not in ALLOWED_THESIS_RESULTS:
        raise LifecycleError(f"thesis_result must be one of {ALLOWED_THESIS_RESULTS}")
    allowed_results_by_kind = {
        "trade": {"success", "failure", "flat"},
        "trade_idea": {"rejected", "stale"},
        "session": {"no_trade", "completed"},
    }
    if result not in allowed_results_by_kind[kind]:
        raise LifecycleError(f"{kind} cannot use result {result}")

    version = payload.get("strategy_version", config.version)
    rules_hash = payload.get("rules_hash", config.rules_hash)
    if not isinstance(version, str) or not version.strip():
        raise LifecycleError("strategy_version must be a non-empty string")
    if not isinstance(rules_hash, str) or not rules_hash.strip():
        raise LifecycleError("rules_hash must be a non-empty string")
    if require_current_rules and (
        version != config.version or rules_hash != config.rules_hash
    ):
        raise LifecycleError("outcome does not match the current strategy rules")

    metrics = payload.get("metrics", {})
    if not isinstance(metrics, Mapping):
        raise LifecycleError("metrics must be an object")
    what_worked = _string_list(payload.get("what_worked"), "what_worked")
    what_failed = _string_list(payload.get("what_failed"), "what_failed")
    if result in ("success", "completed") and not what_worked:
        raise LifecycleError(f"{result} outcomes must record what worked")
    if result in ("failure", "rejected", "stale", "no_trade") and not what_failed:
        raise LifecycleError(f"{result} outcomes must record what failed")
    if kind == "trade":
        net_r = _number_from_metrics(metrics, "net_r")
        if result == "success" and net_r <= 0:
            raise LifecycleError("a successful trade must have positive metrics.net_r")
        if result == "failure" and net_r >= 0:
            raise LifecycleError("a failed trade must have negative metrics.net_r")
        if result == "flat" and not math.isclose(net_r, 0.0, abs_tol=1e-9):
            raise LifecycleError("a flat trade must have zero metrics.net_r")

    record = {
        "schema_version": OUTCOME_SCHEMA_VERSION,
        "context_id": context_id,
        "date": day_text,
        "context_kind": kind,
        "mode": _nonempty_string(payload.get("mode"), "mode"),
        "result": result,
        "summary": _nonempty_string(payload.get("summary"), "summary"),
        "primary_reason": _nonempty_string(
            payload.get("primary_reason"), "primary_reason"
        ),
        "thesis_result": thesis_result,
        "what_worked": what_worked,
        "what_failed": what_failed,
        "lessons": _string_list(payload.get("lessons"), "lessons", allow_empty=False),
        "next_time": _string_list(payload.get("next_time"), "next_time"),
        "metrics": dict(metrics),
        "strategy_version": version,
        "rules_hash": rules_hash,
        "archived_at": (archived_at or datetime.now(timezone.utc)).isoformat(),
    }
    if record["mode"] not in ("live", "shadow"):
        raise LifecycleError("mode must be live or shadow")
    _validate_public_value(record)
    return record


def _number_from_metrics(metrics: Mapping[str, Any], field: str) -> float:
    value = metrics.get(field)
    if isinstance(value, bool):
        raise LifecycleError(f"metrics.{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise LifecycleError(f"metrics.{field} must be numeric") from exc
    if not math.isfinite(result):
        raise LifecycleError(f"metrics.{field} must be finite")
    return result


def validate_outcome_record(
    record: Mapping[str, Any], config: StrategyConfig | None = None
) -> dict[str, Any]:
    """Validate an already-normalized embedded outcome without changing time."""
    if record.get("schema_version") != OUTCOME_SCHEMA_VERSION:
        raise LifecycleError(f"outcome schema_version must be {OUTCOME_SCHEMA_VERSION}")
    archived_at = record.get("archived_at")
    if not isinstance(archived_at, str):
        raise LifecycleError("archived_at must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(archived_at)
    except ValueError as exc:
        raise LifecycleError("archived_at must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise LifecycleError("archived_at must include a timezone")
    payload = dict(record)
    payload.pop("schema_version", None)
    payload.pop("archived_at", None)
    return prepare_outcome(
        payload,
        config,
        archived_at=parsed,
        require_current_rules=False,
    )


def render_outcome_section(record: Mapping[str, Any]) -> str:
    """Render readable Markdown and the canonical embedded learning record."""
    worked = record["what_worked"] or ["None recorded."]
    failed = record["what_failed"] or ["None recorded."]
    next_time = record["next_time"] or ["Preserve the frozen rules and gather data."]
    lines = [
        OUTCOME_SECTION_START,
        "## Terminal Outcome Summary",
        "",
        f"- Result: {record['result']}",
        f"- Primary reason: {record['primary_reason']}",
        f"- Thesis result: {record['thesis_result']}",
        f"- Summary: {record['summary']}",
        "",
        "### What worked",
        "",
        *[f"- {item}" for item in worked],
        "",
        "### What failed",
        "",
        *[f"- {item}" for item in failed],
        "",
        "### Lessons",
        "",
        *[f"- {item}" for item in record["lessons"]],
        "",
        "### Next time",
        "",
        *[f"- {item}" for item in next_time],
        "",
        OUTCOME_JSON_START,
        json.dumps(record, indent=2, sort_keys=True),
        OUTCOME_JSON_END,
        OUTCOME_SECTION_END,
    ]
    return "\n".join(lines) + "\n"


def extract_outcome(text: str, config: StrategyConfig | None = None) -> dict[str, Any]:
    start = text.find(OUTCOME_JSON_START)
    if start < 0:
        raise LifecycleError("context has no embedded terminal outcome")
    start += len(OUTCOME_JSON_START)
    end = text.find(OUTCOME_JSON_END, start)
    if end < 0:
        raise LifecycleError("embedded terminal outcome is not closed")
    if text.find(OUTCOME_JSON_START, start) >= 0:
        raise LifecycleError("context contains more than one terminal outcome")
    raw = text[start:end].strip()
    try:
        value = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise LifecycleError(f"embedded terminal outcome is invalid JSON: {exc.msg}")
    if not isinstance(value, Mapping):
        raise LifecycleError("embedded terminal outcome must be an object")
    return validate_outcome_record(value, config)


def _context_files(root: Path) -> Iterable[Path]:
    if not root.exists():
        return ()
    return (
        path
        for path in sorted(root.rglob("*.md"))
        if path.is_file() and not path.name.startswith(".")
    )


def _active_path(path: Path, active_root: Path) -> Path:
    resolved_root = active_root.resolve()
    resolved = path.resolve()
    if resolved.parent != resolved_root:
        raise LifecycleError("context path must be a direct child of trades/active")
    if resolved.suffix.lower() != ".md" or resolved.name.startswith("."):
        raise LifecycleError("active context must be a visible Markdown file")
    if not resolved.exists() or not resolved.is_file():
        raise LifecycleError(f"active context does not exist: {resolved}")
    return resolved


def archive_context(
    path: Path,
    outcome_payload: Mapping[str, Any],
    *,
    active_root: Path = DEFAULT_ACTIVE_ROOT,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    config: StrategyConfig | None = None,
    allow_historical: bool = False,
    today_et: date | None = None,
) -> Path:
    """Attach a terminal outcome and atomically move one active context."""
    config = config or load_config()
    source = _active_path(path, active_root)
    outcome = prepare_outcome(outcome_payload, config)
    day = date.fromisoformat(outcome["date"])
    current_day = today_et or datetime.now(ZoneInfo("America/New_York")).date()
    if day != current_day and not allow_historical:
        raise LifecycleError(
            "only today's context can be archived unless historical replay is explicit"
        )
    if not source.name.startswith(outcome["date"]):
        raise LifecycleError("active filename must start with its ISO context date")

    archive_day = archive_root.resolve() / outcome["date"].replace("-", "_")
    destination = archive_day / source.name
    if destination.exists():
        raise LifecycleError(f"archive destination already exists: {destination}")

    original = source.read_text(encoding="utf-8")
    if OUTCOME_JSON_START in original:
        existing = extract_outcome(original, config)
        comparable_existing = {
            key: value for key, value in existing.items() if key != "archived_at"
        }
        comparable_new = {
            key: value for key, value in outcome.items() if key != "archived_at"
        }
        if comparable_existing != comparable_new:
            raise LifecycleError(
                "active context already has a different terminal outcome"
            )
        updated = original
    else:
        separator = (
            ""
            if original.endswith("\n\n")
            else "\n"
            if original.endswith("\n")
            else "\n\n"
        )
        updated = original + separator + render_outcome_section(outcome)

    temp_name: str | None = None
    try:
        archive_day.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=source.parent,
            prefix=f".{source.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            temp_name = handle.name
            handle.write(updated)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, source)
        temp_name = None
        os.replace(source, destination)
    finally:
        if temp_name is not None:
            Path(temp_name).unlink(missing_ok=True)
    return destination


def load_archived_outcomes(
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    config: StrategyConfig | None = None,
) -> list[dict[str, Any]]:
    config = config or load_config()
    outcomes: list[dict[str, Any]] = []
    for path in _context_files(archive_root):
        record = extract_outcome(path.read_text(encoding="utf-8"), config)
        record["context_path"] = (
            str(path.relative_to(PROJECT_ROOT))
            if path.is_relative_to(PROJECT_ROOT)
            else str(path)
        )
        outcomes.append(record)
    outcomes.sort(key=lambda item: (item["date"], item["context_id"]))
    return outcomes


def audit_lifecycle(
    *,
    active_root: Path = DEFAULT_ACTIVE_ROOT,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    config: StrategyConfig | None = None,
    today_et: date | None = None,
    historical_active_date: date | None = None,
) -> LifecycleAudit:
    """Check active-day cleanliness and archived outcome completeness."""
    config = config or load_config()
    current_day = today_et or datetime.now(ZoneInfo("America/New_York")).date()
    allowed_active_date = historical_active_date or current_day
    violations: list[str] = []
    active_files = list(_context_files(active_root))
    archived_files = list(_context_files(archive_root))

    if active_root.exists():
        for path in sorted(active_root.iterdir()):
            if path.name.startswith("."):
                continue
            if not path.is_file() or path.suffix.lower() != ".md":
                violations.append(
                    f"active/{path.name}: active entries must be direct Markdown files"
                )
    if archive_root.exists():
        for day_path in sorted(archive_root.iterdir()):
            if day_path.name.startswith("."):
                continue
            if not day_path.is_dir() or not re.fullmatch(
                r"\d{4}_\d{2}_\d{2}", day_path.name
            ):
                violations.append(
                    f"{day_path.name}: archive root entries must be YYYY_MM_DD folders"
                )
                continue
            for path in sorted(day_path.iterdir()):
                if path.name.startswith("."):
                    continue
                if not path.is_file() or path.suffix.lower() != ".md":
                    violations.append(
                        f"{day_path.name}/{path.name}: archive entries must be Markdown files"
                    )

    for path in active_files:
        prefix = path.name[:10]
        try:
            file_day = date.fromisoformat(prefix)
        except ValueError:
            violations.append(
                f"active/{path.name}: filename must start with YYYY-MM-DD"
            )
            continue
        if file_day != allowed_active_date:
            violations.append(
                f"active/{path.name}: stale active context; expected {allowed_active_date.isoformat()}"
            )
        if OUTCOME_JSON_START in path.read_text(encoding="utf-8"):
            violations.append(f"active/{path.name}: terminal context must be archived")

    seen_ids: set[str] = set()
    for path in archived_files:
        relative = path.relative_to(archive_root)
        if len(relative.parts) != 2 or not re.fullmatch(
            r"\d{4}_\d{2}_\d{2}", relative.parts[0]
        ):
            violations.append(f"{relative}: archive path must be YYYY_MM_DD/file.md")
            continue
        folder_day = relative.parts[0].replace("_", "-")
        try:
            record = extract_outcome(path.read_text(encoding="utf-8"), config)
        except LifecycleError as exc:
            violations.append(f"{relative}: {exc}")
            continue
        if record["date"] != folder_day or not path.name.startswith(folder_day):
            violations.append(
                f"{relative}: folder, filename, and outcome dates must match"
            )
        context_id = record["context_id"]
        if context_id in seen_ids:
            violations.append(f"{relative}: duplicate context_id {context_id}")
        seen_ids.add(context_id)

    return LifecycleAudit(
        active_contexts=len(active_files),
        archived_contexts=len(archived_files),
        violations=tuple(violations),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-root", type=Path, default=DEFAULT_ACTIVE_ROOT)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    close = subparsers.add_parser("close", help="close and archive one context")
    close.add_argument("context", type=Path)
    close.add_argument("outcome", type=Path, help="public outcome JSON object")
    close.add_argument(
        "--historical-replay",
        action="store_true",
        help="allow a non-current date for an explicit offline replay",
    )
    subparsers.add_parser("audit", help="audit active and archived context state")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "close":
            payload = json.loads(args.outcome.read_text(encoding="utf-8"))
            if not isinstance(payload, Mapping):
                raise LifecycleError("outcome input must contain a JSON object")
            result: Any = {
                "archived_path": str(
                    archive_context(
                        args.context,
                        payload,
                        active_root=args.active_root,
                        archive_root=args.archive_root,
                        allow_historical=args.historical_replay,
                    )
                )
            }
        else:
            audit = audit_lifecycle(
                active_root=args.active_root, archive_root=args.archive_root
            )
            result = {**asdict(audit), "valid": audit.valid}
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0 if audit.valid else 1
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (OSError, json.JSONDecodeError, LifecycleError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
