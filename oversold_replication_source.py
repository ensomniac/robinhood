"""Freeze and operate the outcome-blind scanner source for oversold replication.

The prior oversold family failed selection-aware confidence with only 31 fills.
This controller does not change that family's rule grid.  It freezes a
continuous set of completed 2026 sessions so a later exact-grid replication can
reserve genuinely untouched confirmation evidence without waiting for a
calendar-budget reset.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import scanner_replay
import scanner_replay_alpaca as alpaca
from historical_store import HistoricalDayStore


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-24-"
    "oversold-replication-confirmation-v1"
)
ROOT = PROJECT_ROOT / "historical_batches/oversold_replication_v4"
SELECTION_PATH = ROOT / "selection-2026-oversold-replication.json"
SELECTION_INSPECTION_PATH = ROOT / "selection-inspection.json"
SCANNER_MANIFEST_ROOT = ROOT / "scanner-manifests"
SCANNER_CONTRACT_INSPECTION_PATH = ROOT / "scanner-contract-inspection.json"
SCANNER_STATUS_PATH = ROOT / "scanner-collection-status.json"
RUN_ROOT = PROJECT_ROOT / "learning_runs/oversold_replication_v4/scanner_replay"
DETAIL_PATH = RUN_ROOT / "scanner-replay-detail.json"
SUMMARY_PATH = RUN_ROOT / "scanner-replay-summary.json"

CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/"
    "session-calendar-2023-01-through-2026-07.json"
)
RULES_PATH = PROJECT_ROOT / "historical_batches/scanner_replay/scanner-rules-v2.json"
SOURCE_ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche2"
SECURITY_MASTER = SOURCE_ROOT / "security-master.jsonl"
SECURITY_SOURCE = SOURCE_ROOT / "security-master-source.json"
SPLITS = (
    PROJECT_ROOT
    / "learning_runs/challenger_orb_retest_v1_tranche2/scanner_replay/"
    "splits.json.gz"
)
SPLIT_SOURCE = SOURCE_ROOT / "split-actions-source.json"
STRATEGY_SOURCE = (
    PROJECT_ROOT
    / "historical_batches/scanner_expansion/production-strategy-source.json"
)
REUSE_SCANNER_MANIFEST = (
    SOURCE_ROOT
    / "scanner_manifests/"
    "dataset-production-scanner-replay-2026-07-21-"
    "challenger-orb-retest-tranche2-v1-"
    "5805ca2d6d3e1df680b0793855e3c64867a596b30e920b6f47c372df3c92da99.json"
)

TARGET_START = "2026-01-02"
TARGET_END = "2026-07-17"
FRONT_EMBARGO_SESSIONS = 5
SELECTION_SEED = 2026072401


class OversoldReplicationSourceError(RuntimeError):
    """The outcome-blind replication source is incomplete or drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationSourceError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise OversoldReplicationSourceError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationSourceError(f"path escaped repository: {path}") from exc


def _require_committed(path: Path) -> None:
    relative = _repo_path(path)
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    if tracked.returncode != 0 or clean.returncode != 0:
        raise OversoldReplicationSourceError(
            f"source must be committed and unchanged: {relative}"
        )


def _selected_dates() -> list[str]:
    calendar = scanner_replay.load_calendar(CALENDAR_PATH)
    selected = [day for day in calendar if TARGET_START <= day <= TARGET_END]
    if (
        not selected
        or selected[0] != TARGET_START
        or selected[-1] != TARGET_END
        or len(selected) <= FRONT_EMBARGO_SESSIONS + 20
    ):
        raise OversoldReplicationSourceError(
            "completed-session confirmation calendar is incomplete"
        )
    return selected


def build_selection() -> dict[str, Any]:
    """Return the deterministic zero-outcome date selection."""

    selected = _selected_dates()
    required = scanner_replay.required_sessions(
        selected, scanner_replay.load_calendar(CALENDAR_PATH)
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "seed": SELECTION_SEED,
        "selected_dates": selected,
        "selected_dates_sha256": _hash(selected),
        "selection_algorithm": (
            "all completed regular sessions from 2026-01-02 through "
            "2026-07-17 inclusive"
        ),
        "selection_purpose": (
            "outcome-blind capacity and untouched confirmation reserve for the "
            "unchanged-grid short-horizon oversold-reversal replication"
        ),
        "front_embargo_sessions": FRONT_EMBARGO_SESSIONS,
        "required_prior_sessions": 15,
        "required_session_count": len(required),
        "required_sessions_sha256": _hash(required),
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_sha256": _file_hash(CALENDAR_PATH),
        "implementation_path": _repo_path(Path(__file__)),
        "implementation_sha256": _file_hash(Path(__file__)),
        "substitution_allowed": False,
        "target_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def freeze_selection() -> dict[str, Any]:
    value = build_selection()
    _write(SELECTION_PATH, value)
    return {
        "state": "SELECTION_FROZEN_AWAITING_INSPECTION",
        "path": _repo_path(SELECTION_PATH),
        "dates": len(value["selected_dates"]),
        "first_date": value["selected_dates"][0],
        "last_date": value["selected_dates"][-1],
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def inspect_selection() -> dict[str, Any]:
    _require_committed(SELECTION_PATH)
    frozen = _read(SELECTION_PATH)
    expected = build_selection()
    checks = {
        "exact_rebuild": frozen == expected,
        "chronological": frozen["selected_dates"]
        == sorted(frozen["selected_dates"]),
        "front_embargo_frozen": frozen["front_embargo_sessions"]
        == FRONT_EMBARGO_SESSIONS,
        "no_substitutions": frozen["substitution_allowed"] is False,
        "no_target_access": frozen["target_outcomes_observed_or_derived"] is False,
        "no_broker_actions": frozen["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationSourceError("selection inspection failed")
    result = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_selection_inspection",
        "state": "SELECTION_INSPECTED_READY",
        "selection_path": _repo_path(SELECTION_PATH),
        "selection_file_sha256": _file_hash(SELECTION_PATH),
        "dates": len(frozen["selected_dates"]),
        "first_date": frozen["selected_dates"][0],
        "last_date": frozen["selected_dates"][-1],
        "checks": checks,
        "provider_requests": 0,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    result["inspection_sha256"] = _hash(result)
    _write(SELECTION_INSPECTION_PATH, result)
    return result


def freeze_scanner(env_path: Path) -> dict[str, Any]:
    for path in (
        Path(__file__),
        SELECTION_PATH,
        SELECTION_INSPECTION_PATH,
        CALENDAR_PATH,
        RULES_PATH,
        SECURITY_MASTER,
        SECURITY_SOURCE,
        SPLIT_SOURCE,
        STRATEGY_SOURCE,
        REUSE_SCANNER_MANIFEST,
        PROJECT_ROOT / "scanner_replay.py",
        PROJECT_ROOT / "scanner_replay_alpaca.py",
    ):
        _require_committed(path)
    if _read(SELECTION_INSPECTION_PATH).get("state") != "SELECTION_INSPECTED_READY":
        raise OversoldReplicationSourceError("selection is not independently ready")
    store = HistoricalDayStore.from_env(env_path)
    path, manifest = alpaca.freeze_contract(
        dataset_id=DATASET_ID,
        selection_path=SELECTION_PATH,
        calendar_path=CALENDAR_PATH,
        rules_path=RULES_PATH,
        security_path=SECURITY_MASTER,
        security_source_path=SECURITY_SOURCE,
        split_path=SPLITS,
        split_source_path=SPLIT_SOURCE,
        strategy_source_path=STRATEGY_SOURCE,
        output_root=SCANNER_MANIFEST_ROOT,
        index_root=alpaca.index_root(store, DATASET_ID),
        reuse_manifest_path=REUSE_SCANNER_MANIFEST,
    )
    return {
        "state": "SCANNER_CONTRACT_FROZEN_AWAITING_INSPECTION",
        "path": _repo_path(path),
        "manifest_sha256": manifest["manifest_sha256"],
        "requested_dates": len(manifest["requested_dates"]),
        "required_sessions": manifest["collection_contract"][
            "required_session_count"
        ],
        "reused_sessions": (
            manifest["collection_contract"].get("reusable_source") or {}
        ).get("session_count", 0),
        "provider_requests": 0,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def _scanner_manifest_path() -> Path:
    paths = sorted(SCANNER_MANIFEST_ROOT.glob("*.json"))
    if len(paths) != 1:
        raise OversoldReplicationSourceError(
            "expected exactly one oversold replication scanner contract"
        )
    return paths[0]


def inspect_scanner_contract(env_path: Path) -> dict[str, Any]:
    path = _scanner_manifest_path()
    _require_committed(path)
    manifest = alpaca.load_contract(path)
    store = HistoricalDayStore.from_env(env_path)
    status = alpaca.collection_status(manifest, store=store)
    expected_dates = _selected_dates()
    selection_inspection = _read(SELECTION_INSPECTION_PATH)
    checks = {
        "dataset_bound": manifest["dataset_id"] == DATASET_ID,
        "selection_bound": manifest["requested_dates"] == expected_dates,
        "calendar_bound": manifest["collection_contract"][
            "session_calendar_sha256"
        ]
        == _file_hash(CALENDAR_PATH),
        "selection_inspected": selection_inspection["state"]
        == "SELECTION_INSPECTED_READY",
        "selection_file_bound": manifest["selection"]["file_sha256"]
        == _file_hash(SELECTION_PATH),
        "no_substitutions": manifest["collection_contract"][
            "substitutions_allowed"
        ]
        is False,
        "zero_state": status["session_files"]["ready"] == 0,
        "zero_provider_requests": status["provider_requests"] == 0,
        "zero_retries": status["provider_retries"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationSourceError("scanner contract inspection failed")
    result = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_scanner_contract_inspection",
        "state": "SCANNER_CONTRACT_INSPECTED_PROVIDER_READY",
        "manifest_path": _repo_path(path),
        "manifest_file_sha256": _file_hash(path),
        "manifest_sha256": manifest["manifest_sha256"],
        "checks": checks,
        "requested_dates": len(expected_dates),
        "required_sessions": manifest["collection_contract"][
            "required_session_count"
        ],
        "provider_requests": 0,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    result["inspection_sha256"] = _hash(result)
    _write(SCANNER_CONTRACT_INSPECTION_PATH, result)
    return result


def collect_scanner(env_path: Path, max_days: int | None) -> dict[str, Any]:
    path = _scanner_manifest_path()
    for required in (path, SCANNER_CONTRACT_INSPECTION_PATH):
        _require_committed(required)
    inspection = _read(SCANNER_CONTRACT_INSPECTION_PATH)
    if inspection.get("state") != "SCANNER_CONTRACT_INSPECTED_PROVIDER_READY":
        raise OversoldReplicationSourceError(
            "scanner provider access lacks independent inspection"
        )
    manifest = alpaca.load_contract(path)
    store = HistoricalDayStore.from_env(env_path)
    status = alpaca.collect_contract(
        manifest,
        config=alpaca.AlpacaBulkConfig.from_env(env_path),
        store=store,
        max_days=max_days,
    )
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "state": (
            "SCANNER_INPUTS_COLLECTED"
            if status["complete"]
            else "SCANNER_INPUTS_PARTIAL"
        ),
        "manifest_path": _repo_path(path),
        "manifest_sha256": manifest["manifest_sha256"],
        "session_files": status["session_files"],
        "provider_requests": status["provider_requests"],
        "provider_retries": status["provider_retries"],
        "derived_rows": status["derived_rows"],
        "reused_sessions": status["reused_sessions"],
        "inherited_derived_rows": status["inherited_derived_rows"],
        "delta_symbols_requested": status["delta_symbols_requested"],
        "substitutions": 0,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    _write(SCANNER_STATUS_PATH, public)
    return public


def status(env_path: Path) -> dict[str, Any]:
    path = _scanner_manifest_path()
    manifest = alpaca.load_contract(path)
    store = HistoricalDayStore.from_env(env_path)
    return alpaca.collection_status(manifest, store=store)


def build_scanner(env_path: Path) -> dict[str, Any]:
    path = _scanner_manifest_path()
    for required in (path, SCANNER_STATUS_PATH):
        _require_committed(required)
    manifest = alpaca.load_contract(path)
    store = HistoricalDayStore.from_env(env_path)
    source_status = alpaca.collection_status(manifest, store=store)
    if not source_status["complete"]:
        raise OversoldReplicationSourceError("scanner inputs are not complete")
    rules, security_path = alpaca.verify_contract_inputs(
        manifest, rules_path=RULES_PATH
    )
    calendar = alpaca.verify_calendar_contract(manifest, CALENDAR_PATH)
    source_root = alpaca.index_root(store, DATASET_ID)
    result = scanner_replay.build_scanner_replay(
        selected_dates=manifest["requested_dates"],
        calendar=calendar,
        rules=rules,
        security_path=security_path,
        minute_root=source_root / "minute_aggs",
        split_path=SPLITS,
        detailed_output=DETAIL_PATH,
        summary_output=SUMMARY_PATH,
    )
    return {
        "state": "SCANNER_REPLAY_BUILT_AWAITING_INSPECTION",
        "dates": len(manifest["requested_dates"]),
        "detail_path": _repo_path(DETAIL_PATH),
        "detail_sha256": _file_hash(DETAIL_PATH),
        "summary_path": _repo_path(SUMMARY_PATH),
        "summary_sha256": _file_hash(SUMMARY_PATH),
        "provider_requests": 0,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "scanner_result": result,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze-selection")
    sub.add_parser("inspect-selection")
    sub.add_parser("freeze-scanner")
    sub.add_parser("inspect-scanner-contract")
    collect = sub.add_parser("collect-scanner")
    collect.add_argument("--max-days", type=int)
    sub.add_parser("status")
    sub.add_parser("build-scanner")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-selection":
            result = freeze_selection()
        elif args.command == "inspect-selection":
            result = inspect_selection()
        elif args.command == "freeze-scanner":
            result = freeze_scanner(args.env)
        elif args.command == "inspect-scanner-contract":
            result = inspect_scanner_contract(args.env)
        elif args.command == "collect-scanner":
            result = collect_scanner(args.env, args.max_days)
        elif args.command == "status":
            result = status(args.env)
        else:
            result = build_scanner(args.env)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationSourceError,
        scanner_replay.ScannerReplayError,
    ) as exc:
        print(
            json.dumps(
                {"state": "BLOCKED", "error": str(exc), "broker_actions": 0},
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
